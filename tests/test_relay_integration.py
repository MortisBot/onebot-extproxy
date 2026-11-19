"""本机回环集成测试：假 NapCat ↔ relay ↔ 假远端（127.0.0.1 临时端口，不连外网）。

默认 skip；需要时：``pytest -q --run-integration``（或 ``pytest -q -m integration --run-integration``）。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import serve as ws_serve
from websockets.exceptions import ConnectionClosed

from onebot_extproxy import policy
from onebot_extproxy.relay import RelayServer, load_config

GROUP_ON = 10001
GROUP_OFF = 10002
REMOTE_TOKEN = "dummy" + "-remote-value"
NAPCAT_TOKEN = "dummy" + "-napcat-value"

pytestmark = pytest.mark.integration


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def write_state(path: Path, **overrides: Any) -> Path:
    state: dict[str, Any] = {
        "version": 2,
        "group_default": False,
        "group_defaults": {str(GROUP_ON): True},
        "feature_defaults": {"ws_consumer_a": True},
        "matrix": {"ws_consumer_a": {str(GROUP_OFF): False}},
    }
    state.update(overrides)
    path.write_text(json.dumps(state), "utf-8")
    return path


async def wait_until(predicate: Callable[[], bool], timeout: float, desc: str) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timeout waiting for {desc}")


async def wait_frames(queue: asyncio.Queue[Any], count: int, timeout: float = 5.0) -> list[Any]:
    await wait_until(lambda: queue.qsize() >= count, timeout, f"{count} frames")
    frames: list[Any] = []
    for _ in range(count):
        frames.append(await asyncio.wait_for(queue.get(), timeout=timeout))
    return frames


def json_payload(frame: Any) -> dict[str, Any]:
    if isinstance(frame, bytes):
        frame = frame.decode("utf-8")
    data = json.loads(frame)
    assert isinstance(data, dict)
    return data


class FakeRemote:
    """假远端消费者：记录连接、请求头与收到的帧。"""

    def __init__(self, port: int) -> None:
        self.port = port
        self.frames: asyncio.Queue[Any] = asyncio.Queue()
        self.connections: list[Any] = []
        self.headers: list[dict[str, str]] = []
        self.connected_count = 0
        self.closed_count = 0
        self.server: Any = None

    async def handler(self, conn: Any) -> None:
        self.connections.append(conn)
        self.headers.append({str(k).lower(): str(v) for k, v in dict(conn.request.headers).items()})
        self.connected_count += 1
        try:
            async for frame in conn:
                await self.frames.put(frame)
        except Exception:  # noqa: BLE001 - 客户端断开
            pass
        finally:
            self.closed_count += 1

    async def start(self) -> None:
        self.server = await ws_serve(
            self.handler,
            "127.0.0.1",
            self.port,
            compression=None,
            ping_interval=None,
            max_size=50 * 1024 * 1024,
        )

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            with contextlib.suppress(Exception):
                await self.server.wait_closed()
            self.server = None


async def test_loopback_filter_passthrough_buffer_reconnect(tmp_path: Path) -> None:
    napcat_port = free_port()
    remote_port = free_port()
    state_path = write_state(tmp_path / "state.json")
    cfg_path = tmp_path / "consumers.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "listen": {"host": "127.0.0.1", "port": napcat_port},
                "state_path": str(state_path),
                "buffer_size": 3,
                "reconnect_min": 0.1,
                "reconnect_max": 0.2,
                "stats_interval": 60,
                "consumers": [
                    {
                        "name": "consumer-a",
                        "feature": "ws_consumer_a",
                        "path": "/consumer-a",
                        "url": f"ws://127.0.0.1:{remote_port}/onebot/v11/ws",
                        "token": REMOTE_TOKEN,
                    }
                ],
            }
        ),
        "utf-8",
    )
    cfg = load_config(cfg_path)
    policy_view = policy.FeaturePolicy(state_path)
    server = RelayServer(cfg, policy_view)
    await server.start()

    remote = FakeRemote(remote_port)
    await remote.start()
    napcat: Any = None
    try:
        # ---------------- 0) 未知路径应被 1008 拒绝 ----------------
        bad = await ws_connect(f"ws://127.0.0.1:{napcat_port}/nope", compression=None, ping_interval=None)
        try:
            await asyncio.wait_for(bad.recv(), timeout=5)
            raise AssertionError("unknown path should be closed, not accepted")
        except ConnectionClosed as exc:
            assert exc.rcvd is not None and exc.rcvd.code == 1008
        finally:
            with contextlib.suppress(Exception):
                await bad.close()

        # ---------------- 1) 连接 + 过滤 + 透传 ----------------
        napcat = await ws_connect(
            f"ws://127.0.0.1:{napcat_port}/consumer-a",
            additional_headers={
                "X-Self-ID": "10000",
                "Authorization": f"Bearer {NAPCAT_TOKEN}",
                "x-client-role": "Universal",
            },
            user_agent_header="OneBot/11",
            compression=None,
            ping_interval=None,
        )
        await wait_until(lambda: remote.connected_count >= 1, 5, "relay → remote connection")
        headers = remote.headers[0]
        assert headers.get("x-self-id") == "10000"  # 中继透传 NapCat 的 X-Self-ID
        assert headers.get("authorization") == f"Bearer {REMOTE_TOKEN}"  # 用配置里的远端 token
        assert headers.get("x-client-role") == "Universal"
        assert headers.get("user-agent") == "OneBot/11"

        lifecycle = {
            "post_type": "meta_event",
            "meta_event_type": "lifecycle",
            "sub_type": "connect",
            "time": 1,
            "self_id": 10000,
        }
        msg_on = {
            "post_type": "message",
            "message_type": "group",
            "group_id": GROUP_ON,
            "message_id": 1,
            "raw_message": "on",
        }
        msg_off = {
            "post_type": "message",
            "message_type": "group",
            "group_id": GROUP_OFF,
            "message_id": 2,
            "raw_message": "off",
        }
        msg_private = {
            "post_type": "message",
            "message_type": "private",
            "user_id": 10000,
            "message_id": 3,
            "raw_message": "pm",
        }
        for payload in (lifecycle, msg_on, msg_off, msg_private):
            await napcat.send(json.dumps(payload))
        frames = await wait_frames(remote.frames, 3, 5)
        decoded = [json_payload(f) for f in frames]
        assert decoded[0].get("post_type") == "meta_event"
        assert decoded[0].get("sub_type") == "connect"
        assert decoded[1].get("group_id") == GROUP_ON
        assert decoded[2].get("message_type") == "private"
        assert all(event.get("group_id") != GROUP_OFF for event in decoded)

        session = server.consumers["/consumer-a"].session
        assert session is not None
        assert session.stats.dropped_policy == 1

        # 远端 → NapCat 透传（API 调用）
        api_call = {"action": "get_group_list", "params": {}, "echo": "api-1"}
        await remote.connections[0].send(json.dumps(api_call))
        got = json.loads(await asyncio.wait_for(napcat.recv(), timeout=5))
        assert got == api_call

        api_resp = {"status": "ok", "retcode": 0, "data": [GROUP_ON], "echo": "api-1"}
        await napcat.send(json.dumps(api_resp))
        resp_frames = await wait_frames(remote.frames, 1)
        assert json_payload(resp_frames[0]) == api_resp

        # ---------------- 2) 远端断线：缓冲最近 N 帧 + 重连 + lifecycle 重放 ----------------
        await remote.stop()
        await wait_until(lambda: server.consumers["/consumer-a"].session is not None, 2, "session alive")
        await asyncio.sleep(0.3)  # 让 relay 感知断线并进入重连循环
        for idx in range(1, 6):
            await napcat.send(
                json.dumps(
                    {
                        "post_type": "message",
                        "message_type": "group",
                        "group_id": GROUP_ON,
                        "message_id": 100 + idx,
                        "raw_message": f"buf-{idx}",
                    }
                )
            )
        await asyncio.sleep(0.3)  # 等 relay 完成若干次失败重连 + 缓冲
        await remote.start()
        await wait_until(lambda: remote.connected_count >= 2, 5, "relay reconnect to remote")
        replay_frames = await wait_frames(remote.frames, 4, 5)
        replay_decoded = [json_payload(f) for f in replay_frames]
        assert replay_decoded[0].get("sub_type") == "connect"
        assert [event.get("raw_message") for event in replay_decoded[1:]] == [
            "buf-3",
            "buf-4",
            "buf-5",
        ]
        assert server.consumers["/consumer-a"].stats.remote_reconnects >= 1

        # ---------------- 3) NapCat 断线 → 远端连接被关闭 ----------------
        closed_before = remote.closed_count
        await napcat.close()
        napcat = None
        await wait_until(lambda: remote.closed_count > closed_before, 5, "remote closed")
        await wait_until(lambda: server.consumers["/consumer-a"].session is None, 5, "session cleaned")
    finally:
        if napcat is not None:
            with contextlib.suppress(Exception):
                await napcat.close()
        with contextlib.suppress(Exception):
            await remote.stop()
        with contextlib.suppress(Exception):
            await server.stop()
