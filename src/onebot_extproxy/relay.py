"""onebot-extproxy 的中继核心：WS server（NapCat 侧）+ 每 consumer WS client（远端侧）。

NapCat 以「反向 WS 客户端」方式并行外连多个消费者，而 OneBot v11 / NapCat 没有
「按群订阅」能力，外部消费者会收到全部群消息。本中继作为 NapCat 与远端消费者之间
的一层代理::

    QQ ⇄ NapCat ──(反向 WS 客户端)──▶ extproxy ──(反向 WS 客户端，同款握手)──▶ 远端消费者
                                      │
                                      └─ 读 feature_switch/state.json，按 (ws_<name>, group) 过滤

- NapCat 侧：本进程做 WS **server**，每个 consumer 一个独立路径（如 ``/consumer-a``）；
- 远端侧：本进程做 WS **client**，用与 NapCat 完全相同的握手头外连真实远端；
- 方向 1（NapCat→远端）：逐帧 JSON 解析，``post_type ∈ {message, notice, request}`` 且带
  ``group_id`` 的事件按矩阵过滤；meta/私聊/非 JSON 帧原样转发；
- 方向 2（远端→NapCat）：完全透传（远端发起的 API 调用与 NapCat 的响应都不过滤）。

可靠性语义
---------
- 远端断线：指数退避重连；断线期间 NapCat→远端的帧缓冲最近 N 帧（默认 50），重连后先重放
  缓存的 lifecycle/connect 帧，再补发缓冲帧（**不是持久化补发**，更早的事件直接丢弃）；
- NapCat 断线：立即关闭对应远端连接并停止重连，等待 NapCat 再来；
- 同一 consumer 重复连接：新连接替换旧连接（旧连接以 1012 关闭）；
- 日志不打印 token；URL 打印前脱敏（去掉 userinfo/query/fragment）。

本模块只保留中继本身；CLI 入口（``--config`` / ``--check-config`` / 环境变量覆盖）见
:mod:`onebot_extproxy.__main__`。环境变量：``EXTPROXY_CONFIG`` / ``EXTPROXY_HOST`` /
``EXTPROXY_PORT`` / ``EXTPROXY_STATE_PATH`` / ``EXTPROXY_BUFFER`` / ``EXTPROXY_LOG_LEVEL`` /
``EXTPROXY_PING_INTERVAL`` / ``EXTPROXY_STATS_INTERVAL`` / ``EXTPROXY_PROXY``
（优先级：CLI > 环境变量 > 配置文件 > 默认）。"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from websockets.asyncio.client import connect as ws_connect
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from .policy import FeaturePolicy, decode_json_frame, is_lifecycle_connect, should_forward_frame

LOG = logging.getLogger("extproxy")

DEFAULT_CONFIG_NAME = "consumers.json"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8090
DEFAULT_STATE_PATH = "/state/state.json"
DEFAULT_BUFFER_SIZE = 50
DEFAULT_QUEUE_MAX = 1000
DEFAULT_RECONNECT_MIN = 1.0
DEFAULT_RECONNECT_MAX = 30.0
DEFAULT_MAX_SIZE = 50 * 1024 * 1024  # NapCat 反向 WS 的 maxPayload 是 50MB
DEFAULT_STATS_INTERVAL = 60.0
USER_AGENT = "OneBot/11"
CLIENT_ROLE = "Universal"


class ConfigError(Exception):
    """配置文件不合法。"""


class _RemoteClosed(Exception):
    """内部信号：远端连接在对端关闭（读方向已结束），用于唤醒写循环重连。"""


_REMOTE_CLOSED = object()  # 队列哨兵，避免写循环卡在 queue.get() 上不感知远端断开


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
@dataclass
class ConsumerConfig:
    name: str
    feature: str
    path: str
    url: str
    token: str = ""

    @property
    def token_configured(self) -> bool:
        return bool(self.token)

    @classmethod
    def from_raw(cls, raw: Any, index: int) -> ConsumerConfig:
        where = f"consumers[{index}]"
        if not isinstance(raw, dict):
            raise ConfigError(f"{where} must be an object")
        name = raw.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(f"{where}.name must be a non-empty string")
        name = name.strip()
        url = raw.get("url")
        if not isinstance(url, str) or not url.strip():
            raise ConfigError(f"{where}.url must be a non-empty string")
        url = url.strip()
        scheme = urlsplit(url).scheme.lower()
        if scheme not in ("ws", "wss"):
            raise ConfigError(f"{where}.url must use ws:// or wss:// (got {scheme!r})")
        path = raw.get("path")
        if not isinstance(path, str) or not path.strip():
            path = "/" + name
        path = _normalize_path(path, f"{where}.path")
        feature = raw.get("feature")
        if not isinstance(feature, str) or not feature.strip():
            feature = f"ws_{name}"
        feature = feature.strip()
        token = raw.get("token", "")
        if token is None:
            token = ""
        if not isinstance(token, str):
            raise ConfigError(f"{where}.token must be a string")
        return cls(name=name, feature=feature, path=path, url=url, token=token)


@dataclass
class RelayConfig:
    consumers: list[ConsumerConfig] = field(default_factory=list)
    listen_host: str = DEFAULT_HOST
    listen_port: int = DEFAULT_PORT
    state_path: str = DEFAULT_STATE_PATH
    buffer_size: int = DEFAULT_BUFFER_SIZE
    queue_max: int = DEFAULT_QUEUE_MAX
    reconnect_min: float = DEFAULT_RECONNECT_MIN
    reconnect_max: float = DEFAULT_RECONNECT_MAX
    stats_interval: float = DEFAULT_STATS_INTERVAL
    ping_interval: float | None = None
    proxy: str | None = None
    max_size: int = DEFAULT_MAX_SIZE
    log_level: str = "INFO"


def _normalize_path(path: str, where: str = "path") -> str:
    path = path.strip()
    if not path.startswith("/"):
        path = "/" + path
    if len(path) > 1:
        path = path.rstrip("/")
    if any(ch in path for ch in ("?", "#", " ")):
        raise ConfigError(f"{where} must not contain '?', '#' or spaces: {path!r}")
    return path


def _pick(cli: Any, env_name: str, file_value: Any, default: Any) -> Any:
    if cli is not None:
        return cli
    env_value = os.environ.get(env_name)
    if env_value is not None and env_value != "":
        return env_value
    if file_value is not None:
        return file_value
    return default


def _as_int(value: Any, where: str, minimum: int | None = None) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{where} must be an integer (got {value!r})") from exc
    if minimum is not None and out < minimum:
        raise ConfigError(f"{where} must be >= {minimum} (got {out})")
    return out


def _as_float(value: Any, where: str, minimum: float | None = None) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{where} must be a number (got {value!r})") from exc
    if minimum is not None and out < minimum:
        raise ConfigError(f"{where} must be >= {minimum} (got {out})")
    return out


def _as_optional_float(value: Any, where: str) -> float | None:
    if value is None or value == "":
        return None
    return _as_float(value, where, minimum=0.0)


def _config_file_fallback() -> Path:
    # 先找脚本同目录的 consumers.json，再找当前目录
    here = Path(__file__).resolve().parent / DEFAULT_CONFIG_NAME
    if here.is_file():
        return here
    return Path.cwd() / DEFAULT_CONFIG_NAME


def load_config(
    config_path: str | os.PathLike[str] | None,
    *,
    cli_host: str | None = None,
    cli_port: int | None = None,
    cli_state: str | None = None,
    cli_buffer: int | None = None,
    cli_log_level: str | None = None,
    cli_ping_interval: float | None = None,
    cli_stats_interval: float | None = None,
) -> RelayConfig:
    """读取并校验 consumers.json（支持 CLI/环境变量覆盖）。

    配置文件 schema（version 1）::

        {
          "version": 1,
          "listen": {"host": "0.0.0.0", "port": 8090},
          "state_path": "/state/state.json",
          "buffer_size": 50,
          "reconnect_min": 1.0,
          "reconnect_max": 30.0,
          "stats_interval": 60,
          "ping_interval": null,
          "proxy": null,
          "consumers": [
            {"name": "maimai", "feature": "ws_maimai", "path": "/maimai",
             "url": "ws://host:port/onebot/v11/ws", "token": "..."}
          ]
        }
    """
    raw_path = config_path or os.environ.get("EXTPROXY_CONFIG") or _config_file_fallback()
    path = Path(raw_path)
    try:
        raw_text = path.read_text("utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc
    try:
        file_cfg = json.loads(raw_text)
    except ValueError as exc:
        raise ConfigError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(file_cfg, dict):
        raise ConfigError("config root must be a JSON object")

    listen = file_cfg.get("listen") if isinstance(file_cfg.get("listen"), dict) else {}
    consumers_raw = file_cfg.get("consumers")
    if not isinstance(consumers_raw, list) or not consumers_raw:
        raise ConfigError("'consumers' must be a non-empty array")

    host = _pick(cli_host, "EXTPROXY_HOST", listen.get("host"), DEFAULT_HOST)
    if not isinstance(host, str) or not host:
        raise ConfigError(f"listen.host must be a non-empty string (got {host!r})")
    port = _as_int(_pick(cli_port, "EXTPROXY_PORT", listen.get("port"), DEFAULT_PORT), "listen.port", 1)
    state_path = _pick(cli_state, "EXTPROXY_STATE_PATH", file_cfg.get("state_path"), DEFAULT_STATE_PATH)
    if not isinstance(state_path, str) or not state_path:
        raise ConfigError("state_path must be a non-empty string")
    buffer_size = _as_int(
        _pick(cli_buffer, "EXTPROXY_BUFFER", file_cfg.get("buffer_size"), DEFAULT_BUFFER_SIZE), "buffer_size", 0
    )
    queue_max = _as_int(file_cfg.get("queue_max", DEFAULT_QUEUE_MAX), "queue_max", 1)
    reconnect_min = _as_float(file_cfg.get("reconnect_min", DEFAULT_RECONNECT_MIN), "reconnect_min", 0.05)
    reconnect_max = _as_float(file_cfg.get("reconnect_max", DEFAULT_RECONNECT_MAX), "reconnect_max", reconnect_min)
    stats_interval = _as_float(
        _pick(cli_stats_interval, "EXTPROXY_STATS_INTERVAL", file_cfg.get("stats_interval"), DEFAULT_STATS_INTERVAL),
        "stats_interval",
        1.0,
    )
    ping_interval = _as_optional_float(
        _pick(cli_ping_interval, "EXTPROXY_PING_INTERVAL", file_cfg.get("ping_interval"), None),
        "ping_interval",
    )
    log_level = _pick(cli_log_level, "EXTPROXY_LOG_LEVEL", file_cfg.get("log_level"), "INFO")
    log_level = str(log_level).upper()
    if log_level not in ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"):
        raise ConfigError(f"log_level must be one of CRITICAL/ERROR/WARNING/INFO/DEBUG (got {log_level!r})")
    proxy = file_cfg.get("proxy")
    if proxy is not None and not isinstance(proxy, str):
        raise ConfigError("proxy must be a string (ws proxy url) or null")
    if not proxy:
        proxy = None

    consumers: list[ConsumerConfig] = []
    names: set[str] = set()
    paths: set[str] = set()
    for idx, item in enumerate(consumers_raw):
        consumer = ConsumerConfig.from_raw(item, idx)
        if consumer.name in names:
            raise ConfigError(f"duplicate consumer name: {consumer.name!r}")
        if consumer.path in paths:
            raise ConfigError(f"duplicate consumer path: {consumer.path!r}")
        names.add(consumer.name)
        paths.add(consumer.path)
        consumers.append(consumer)

    return RelayConfig(
        consumers=consumers,
        listen_host=host,
        listen_port=port,
        state_path=state_path,
        buffer_size=buffer_size,
        queue_max=queue_max,
        reconnect_min=reconnect_min,
        reconnect_max=reconnect_max,
        stats_interval=stats_interval,
        ping_interval=ping_interval,
        proxy=proxy,
        log_level=log_level,
    )


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------
def sanitize_url(url: str) -> str:
    """日志用 URL：保留 scheme://host[:port]/path，去掉 userinfo/query/fragment（可能含 token）。"""
    try:
        parts = urlsplit(url)
    except ValueError:
        return "<invalid-url>"
    netloc = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit((parts.scheme, netloc, parts.path, "", ""))


def _group_of(event: dict[str, Any] | None) -> Any:
    if not isinstance(event, dict):
        return None
    return event.get("group_id")


@dataclass
class ConsumerStats:
    napcat_connects: int = 0
    napcat_disconnects: int = 0
    remote_connects: int = 0
    remote_reconnects: int = 0
    remote_connect_failures: int = 0
    forwarded: int = 0
    dropped_policy: int = 0
    dropped_overflow: int = 0


class _Session:
    """一次 NapCat 连接的完整中继会话（含远端重连生命周期）。"""

    def __init__(self, relay: ConsumerRelay, napcat: ServerConnection) -> None:
        self.relay = relay
        self.cfg = relay.cfg
        self.policy = relay.policy
        self.napcat = napcat
        self.stats = relay.stats
        self.self_id = self._read_self_id()
        self.closed = False
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self.relay.app_cfg.queue_max)
        self.buffer: deque[Any] = deque(maxlen=self.relay.app_cfg.buffer_size)
        self.lifecycle_frame: Any = None
        self.remote: Any = None
        self.remote_ready = False
        self.writer: asyncio.Task[None] | None = None
        self._done = asyncio.Event()

    # ------------------------------------------------------------------ util
    def _read_self_id(self) -> str:
        request = getattr(self.napcat, "request", None)
        headers = getattr(request, "headers", None)
        self_id = ""
        if headers is not None:
            try:
                self_id = str(headers.get("X-Self-ID") or "").strip()
            except Exception:  # noqa: BLE001 - 防御性：任何 header 异常都不影响中继
                self_id = ""
        return self_id

    def _peer(self) -> str:
        peer = getattr(self.napcat, "remote_address", None)
        return str(peer)

    def log(self, level: int, msg: str, *args: Any) -> None:
        LOG.log(level, "[%s] " + msg, self.cfg.name, *args)

    # --------------------------------------------------------------- 生命周期
    async def run(self) -> None:
        self.log(
            logging.INFO,
            "napcat connected peer=%s self_id=%s path=%s feature=%s remote=%s token=%s",
            self._peer(),
            self.self_id or "?",
            self.cfg.path,
            self.cfg.feature,
            sanitize_url(self.cfg.url),
            "configured" if self.cfg.token_configured else "none",
        )
        self.writer = asyncio.create_task(self._remote_loop(), name=f"extproxy-remote-{self.cfg.name}")
        self.writer.add_done_callback(self._on_writer_done)
        try:
            await self._napcat_pump()
        finally:
            await self.close(reason="napcat-disconnected")
            self._done.set()

    def _on_writer_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:  # 不应发生：内部异常已在 remote_loop 捕获
            self.log(logging.ERROR, "remote task crashed: %r", exc)

    async def close(self, *, code: int = 1000, reason: str = "") -> None:
        if self.closed:
            return
        self.closed = True
        if self.writer is not None and not self.writer.done():
            self.writer.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.writer
        remote = self.remote
        self.remote = None
        self.remote_ready = False
        if remote is not None:
            with contextlib.suppress(ConnectionClosed, Exception):
                await remote.close(code=code, reason=reason[:120])
        with contextlib.suppress(ConnectionClosed, Exception):
            await self.napcat.close(code=code, reason=reason[:120])

    # --------------------------------------------------------------- NapCat 侧
    async def _napcat_pump(self) -> None:
        try:
            async for frame in self.napcat:
                await self._on_napcat_frame(frame)
        except ConnectionClosed as exc:
            self.log(
                logging.INFO,
                "napcat disconnected: code=%s reason=%r",
                getattr(exc, "code", "?"),
                getattr(exc, "reason", ""),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.log(logging.WARNING, "napcat pump error: %r", exc)

    async def _on_napcat_frame(self, frame: Any) -> None:
        event = decode_json_frame(frame)
        lifecycle = is_lifecycle_connect(event)
        if lifecycle:
            # 唯一需要缓存并在远端每次（重）连后重放的一帧；不放进普通缓冲，避免重复
            self.lifecycle_frame = frame
        forward, reason, parsed = should_forward_frame(frame, self.policy, self.cfg.feature)
        if not forward:
            self.stats.dropped_policy += 1
            if self.stats.dropped_policy == 1 or self.stats.dropped_policy % 100 == 0:
                group_id = _group_of(parsed)
                self.log(
                    logging.INFO,
                    "drop event feature=%s group=%s reason=%s source=%s dropped_total=%d",
                    self.cfg.feature,
                    group_id,
                    reason,
                    self.policy.source_of(self.cfg.feature, group_id),
                    self.stats.dropped_policy,
                )
            return
        self.stats.forwarded += 1
        if lifecycle and not self.remote_ready:
            # 已在 lifecycle_frame 缓存，远端连上时重放即可
            return
        if self.remote_ready:
            self._enqueue(frame)
        else:
            self._buffer(frame)

    def _enqueue(self, frame: Any) -> None:
        if self.queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
                self.stats.dropped_overflow += 1
        with contextlib.suppress(asyncio.QueueFull):
            self.queue.put_nowait(frame)

    def _buffer(self, frame: Any) -> None:
        limit = self.buffer.maxlen or 0
        if limit and len(self.buffer) >= limit:
            self.stats.dropped_overflow += 1
        self.buffer.append(frame)

    # ---------------------------------------------------------------- 远端侧
    async def _remote_loop(self) -> None:
        backoff = self.relay.app_cfg.reconnect_min
        while True:
            if self.closed:
                return
            try:
                remote = await self._connect_remote()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 网络/握手/HTTP 状态码错误都走重试
                self.stats.remote_connect_failures += 1
                self.log(
                    logging.WARNING,
                    "remote connect failed: %s: %s (retry in %.1fs)",
                    type(exc).__name__,
                    exc,
                    backoff,
                )
                await self._sleep(backoff)
                backoff = min(backoff * 2, self.relay.app_cfg.reconnect_max)
                continue

            backoff = self.relay.app_cfg.reconnect_min
            self.remote = remote
            self.remote_ready = True
            self.stats.remote_connects += 1
            self.log(logging.INFO, "remote connected: %s", sanitize_url(self.cfg.url))
            reader = asyncio.create_task(self._remote_to_napcat(remote), name=f"extproxy-r2n-{self.cfg.name}")
            try:
                await self._replay_and_forward(remote)
            except asyncio.CancelledError:
                raise
            except (_RemoteClosed, ConnectionClosed) as exc:
                self.log(
                    logging.INFO,
                    "remote disconnected: %s code=%s reason=%r",
                    type(exc).__name__,
                    getattr(exc, "code", "?"),
                    getattr(exc, "reason", ""),
                )
            except Exception as exc:  # noqa: BLE001
                self.log(logging.WARNING, "remote forward error: %r", exc)
            finally:
                self.remote = None
                self.remote_ready = False
                reader.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await reader
                with contextlib.suppress(ConnectionClosed, Exception):
                    await remote.close()
            if self.closed:
                return
            # 断线期间已从 queue 缓存的帧，重连后补发（只保留最近 N 帧；过滤哨兵）
            pending: list[Any] = []
            while True:
                try:
                    frame = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if frame is not _REMOTE_CLOSED:
                    pending.append(frame)
            if pending:
                for frame in pending[-self.relay.app_cfg.buffer_size :] if self.relay.app_cfg.buffer_size else []:
                    self._buffer(frame)
            self.stats.remote_reconnects += 1
            self.log(
                logging.WARNING,
                "remote reconnect in %.1fs (buffered=%d, dropped_overflow=%d)",
                backoff,
                len(self.buffer),
                self.stats.dropped_overflow,
            )
            await self._sleep(backoff)
            backoff = min(backoff * 2, self.relay.app_cfg.reconnect_max)

    async def _connect_remote(self) -> Any:
        headers = {"x-client-role": CLIENT_ROLE}
        if self.self_id:
            headers["X-Self-ID"] = self.self_id
        if self.cfg.token:
            headers["Authorization"] = f"Bearer {self.cfg.token}"
        kwargs: dict[str, Any] = dict(
            additional_headers=headers,
            user_agent_header=USER_AGENT,
            max_size=self.relay.app_cfg.max_size,
            compression=None,  # NapCat: perMessageDeflate=false
            ping_interval=self.relay.app_cfg.ping_interval,
            open_timeout=10,
            close_timeout=5,
            proxy=self.relay.app_cfg.proxy,
        )
        return await ws_connect(self.cfg.url, **kwargs)

    async def _replay_and_forward(self, remote: Any) -> None:
        if self.lifecycle_frame is not None:
            # 重放不做 forwarded 计数（该帧在首次接收时已计入）
            await remote.send(self.lifecycle_frame)
        while self.buffer:
            await remote.send(self.buffer.popleft())
        while not self.closed:
            frame = await self.queue.get()
            if frame is _REMOTE_CLOSED:
                raise _RemoteClosed()
            await remote.send(frame)

    def _signal_remote_closed(self) -> None:
        """读方向感知到远端关闭时，往队列塞哨兵唤醒写循环（队列满则先丢最旧帧）。"""
        if self.closed:
            return
        if self.queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
                self.stats.dropped_overflow += 1
        with contextlib.suppress(asyncio.QueueFull):
            self.queue.put_nowait(_REMOTE_CLOSED)

    async def _remote_to_napcat(self, remote: Any) -> None:
        try:
            async for frame in remote:
                await self.napcat.send(frame)
        except ConnectionClosed:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self.log(logging.WARNING, "remote→napcat forward error: %r", exc)
        finally:
            self._signal_remote_closed()

    async def _sleep(self, delay: float) -> None:
        try:
            await asyncio.wait_for(self._done.wait(), timeout=delay)
        except TimeoutError:
            pass

    def mark_done(self) -> None:
        self._done.set()

    async def shutdown(self) -> None:
        self.mark_done()
        await self.close(reason="relay-shutdown")


class ConsumerRelay:
    """单个消费者的路由：接 NapCat，管理当前会话。"""

    def __init__(self, cfg: ConsumerConfig, app_cfg: RelayConfig, policy: FeaturePolicy) -> None:
        self.cfg = cfg
        self.app_cfg = app_cfg
        self.policy = policy
        self.stats = ConsumerStats()
        self.session: _Session | None = None
        self._lock = asyncio.Lock()

    async def handle_napcat(self, conn: ServerConnection) -> None:
        async with self._lock:
            old = self.session
            if old is not None:
                self.session = None
                LOG.warning("[%s] new napcat connection replaces existing session", self.cfg.name)
                await old.shutdown()
            session = _Session(self, conn)
            self.session = session
            self.stats.napcat_connects += 1
        try:
            await session.run()
        finally:
            async with self._lock:
                if self.session is session:
                    self.session = None
            self.stats.napcat_disconnects += 1
            LOG.info(
                "[%s] session finished (forwarded=%d dropped_policy=%d dropped_overflow=%d)",
                self.cfg.name,
                self.stats.forwarded,
                self.stats.dropped_policy,
                self.stats.dropped_overflow,
            )

    def status_line(self) -> str:
        session = self.session
        napcat = "up" if session is not None else "down"
        remote = "up" if (session is not None and session.remote_ready) else "down"
        buffered = len(session.buffer) if session is not None else 0
        return (
            f"[{self.cfg.name}] napcat={napcat} remote={remote} buffered={buffered} "
            f"forwarded={self.stats.forwarded} dropped_policy={self.stats.dropped_policy} "
            f"dropped_overflow={self.stats.dropped_overflow} reconnects={self.stats.remote_reconnects} "
            f"connect_failures={self.stats.remote_connect_failures}"
        )

    async def shutdown(self) -> None:
        session = self.session
        self.session = None
        if session is not None:
            await session.shutdown()


# ---------------------------------------------------------------------------
# 服务器
# ---------------------------------------------------------------------------
class RelayServer:
    """一个 WS server 承载所有 consumer 路径；每条路径一个 ConsumerRelay。"""

    def __init__(self, cfg: RelayConfig, policy: FeaturePolicy) -> None:
        self.cfg = cfg
        self.policy = policy
        self.consumers: dict[str, ConsumerRelay] = {c.path: ConsumerRelay(c, cfg, policy) for c in cfg.consumers}
        self.server: Server | None = None
        self._stats_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def handler(self, conn: ServerConnection) -> None:
        request = getattr(conn, "request", None)
        raw_path = getattr(request, "path", "") or ""
        path = raw_path.split("?", 1)[0]
        if len(path) > 1:
            path = path.rstrip("/")
        relay = self.consumers.get(path) or self.consumers.get("/" + path.lstrip("/"))
        if relay is None:
            LOG.warning("reject unknown path %r from peer %s", raw_path, getattr(conn, "remote_address", "?"))
            await conn.close(code=1008, reason="unknown extproxy path")
            return
        await relay.handle_napcat(conn)

    async def start(self) -> None:
        LOG.info(
            "starting extproxy: listen=ws://%s:%d consumers=%s state=%s buffer=%d",
            self.cfg.listen_host,
            self.cfg.listen_port,
            ",".join(f"{c.name}->{c.path}({c.feature})" for c in self.cfg.consumers),
            self.cfg.state_path,
            self.cfg.buffer_size,
        )
        LOG.info(
            "remote targets: %s",
            " | ".join(
                f"{c.name}={sanitize_url(c.url)} token={'configured' if c.token_configured else 'none'}"
                for c in self.cfg.consumers
            ),
        )
        self.server = await serve(
            self.handler,
            self.cfg.listen_host,
            self.cfg.listen_port,
            max_size=self.cfg.max_size,
            compression=None,  # NapCat: perMessageDeflate=false
            ping_interval=self.cfg.ping_interval,
            logger=LOGGER_CHILD,
        )
        self._stats_task = asyncio.create_task(self._stats_loop(), name="extproxy-stats")

    async def stop(self) -> None:
        self._stop.set()
        if self.server is not None:
            self.server.close()
            with contextlib.suppress(Exception):
                await self.server.wait_closed()
            self.server = None
        for relay in self.consumers.values():
            with contextlib.suppress(Exception):
                await relay.shutdown()
        if self._stats_task is not None:
            self._stats_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._stats_task
            self._stats_task = None
        LOG.info("extproxy stopped")

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, self._stop.set)
        await self.start()
        try:
            await self._stop.wait()
        finally:
            await self.stop()

    async def _stats_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.stats_interval)
            for relay in self.consumers.values():
                LOG.info("%s", relay.status_line())
            LOG.debug(
                "policy: reloads=%d parse_errors=%d last_error=%r",
                self.policy.reload_count,
                self.policy.parse_error_count,
                self.policy.last_error,
            )


LOGGER_CHILD = logging.getLogger("extproxy.websockets")
