"""relay 配置解析 / 校验 / CLI（--check-config）单测：不建任何网络连接。"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest

from onebot_extproxy.__main__ import main
from onebot_extproxy.relay import ConfigError, load_config, sanitize_url

ENV_KEYS = (
    "EXTPROXY_CONFIG",
    "EXTPROXY_HOST",
    "EXTPROXY_PORT",
    "EXTPROXY_STATE_PATH",
    "EXTPROXY_BUFFER",
    "EXTPROXY_LOG_LEVEL",
    "EXTPROXY_PING_INTERVAL",
    "EXTPROXY_STATS_INTERVAL",
    "EXTPROXY_PROXY",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


def write_config(
    root: Path,
    *,
    url: str = "ws://127.0.0.1:18799/onebot/v11/ws",
    **overrides: Any,
) -> Path:
    cfg: dict[str, Any] = {
        "version": 1,
        "listen": {"host": "127.0.0.1", "port": 18090},
        "state_path": str(root / "state.json"),
        "buffer_size": 3,
        "reconnect_min": 0.1,
        "reconnect_max": 0.2,
        "stats_interval": 60,
        "consumers": [
            {
                "name": "consumer-a",
                "feature": "ws_consumer_a",
                "path": "/consumer-a",
                "url": url,
                "token": "unit-test-token",
            },
            {
                "name": "consumer-b",
                "feature": "ws_consumer_b",
                "path": "/consumer-b/",
                "url": url,
                "token": "",
            },
        ],
    }
    cfg.update(overrides)
    path = root / "consumers.json"
    path.write_text(json.dumps(cfg), "utf-8")
    return path


def test_config_parsing(tmp_path: Path) -> None:
    path = write_config(tmp_path)
    cfg = load_config(path)
    assert cfg.listen_port == 18090
    assert len(cfg.consumers) == 2
    assert cfg.consumers[0].feature == "ws_consumer_a"
    assert cfg.consumers[1].feature == "ws_consumer_b"
    assert cfg.consumers[1].path == "/consumer-b"  # 尾部斜杠被规整
    assert cfg.consumers[0].token == "unit-test-token"
    assert cfg.consumers[1].token_configured is False
    assert cfg.buffer_size == 3
    assert cfg.state_path == str(tmp_path / "state.json")
    assert cfg.reconnect_min == 0.1
    assert cfg.reconnect_max == 0.2


def test_consumer_defaults(tmp_path: Path) -> None:
    raw = {
        "consumers": [
            {"name": "alpha", "url": "ws://127.0.0.1:1/x"},
            {"name": "beta", "url": "wss://example.invalid/y", "path": "beta", "token": None},
        ]
    }
    path = tmp_path / "consumers.json"
    path.write_text(json.dumps(raw), "utf-8")
    cfg = load_config(path)
    assert cfg.consumers[0].feature == "ws_alpha"
    assert cfg.consumers[0].path == "/alpha"
    assert cfg.consumers[0].token == ""
    assert cfg.consumers[1].path == "/beta"
    assert cfg.consumers[1].token == ""
    # 未提供时使用默认值
    assert cfg.listen_host == "0.0.0.0"
    assert cfg.listen_port == 8090
    assert cfg.log_level == "INFO"


def test_env_and_cli_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write_config(tmp_path)
    monkeypatch.setenv("EXTPROXY_PORT", "18091")
    monkeypatch.setenv("EXTPROXY_BUFFER", "7")
    cfg = load_config(path)
    assert cfg.listen_port == 18091
    assert cfg.buffer_size == 7

    cfg2 = load_config(path, cli_port=18092, cli_buffer=9)
    assert cfg2.listen_port == 18092  # CLI 优先于环境变量
    assert cfg2.buffer_size == 9

    monkeypatch.setenv("EXTPROXY_HOST", "127.0.0.1")
    monkeypatch.setenv("EXTPROXY_STATE_PATH", str(tmp_path / "other-state.json"))
    monkeypatch.setenv("EXTPROXY_LOG_LEVEL", "debug")
    monkeypatch.setenv("EXTPROXY_PING_INTERVAL", "20")
    monkeypatch.setenv("EXTPROXY_STATS_INTERVAL", "30")
    cfg3 = load_config(path)
    assert cfg3.listen_host == "127.0.0.1"
    assert cfg3.state_path == str(tmp_path / "other-state.json")
    assert cfg3.log_level == "DEBUG"
    assert cfg3.ping_interval == 20.0
    assert cfg3.stats_interval == 30.0

    monkeypatch.setenv("EXTPROXY_CONFIG", str(path))
    assert load_config(None).listen_port == 18091


def test_validation_errors(tmp_path: Path) -> None:
    bad_path = tmp_path / "bad.json"

    def expect_error(raw: Any, needle: str) -> None:
        bad_path.write_text(json.dumps(raw), "utf-8")
        with pytest.raises(ConfigError, match=needle):
            load_config(bad_path)

    expect_error({"consumers": []}, "non-empty")
    expect_error({"consumers": [{"name": "", "url": "ws://127.0.0.1/x"}]}, "name")
    expect_error({"consumers": [{"name": "a", "url": "http://127.0.0.1/y"}]}, "ws://")
    expect_error(
        {
            "consumers": [
                {"name": "a", "url": "ws://127.0.0.1/x", "path": "/a"},
                {"name": "b", "url": "ws://127.0.0.1/x", "path": "/a"},
            ]
        },
        "duplicate",
    )
    expect_error(
        {
            "consumers": [
                {"name": "a", "url": "ws://127.0.0.1/x"},
                {"name": "a", "url": "ws://127.0.0.1/z"},
            ]
        },
        "duplicate",
    )
    expect_error(
        {"consumers": [{"name": "a", "url": "ws://127.0.0.1/x"}], "listen": {"port": 0}},
        "port",
    )
    expect_error(
        {"consumers": [{"name": "a", "url": "ws://127.0.0.1/x"}], "log_level": "LOUD"},
        "log_level",
    )
    expect_error({"consumers": [{"name": "a", "url": "ws://127.0.0.1/x"}], "proxy": 5}, "proxy")

    # 文件不存在 / JSON 非法
    missing = tmp_path / "missing.json"
    with pytest.raises(ConfigError, match="cannot read config"):
        load_config(missing)
    bad_path.write_text("{ not json", "utf-8")
    with pytest.raises(ConfigError, match="invalid JSON"):
        load_config(bad_path)


def test_sanitize_url() -> None:
    assert sanitize_url("ws://user:secret@host:1234/path?token=abc#frag") == ("ws://host:1234/path")
    assert sanitize_url("wss://example.invalid:13999/onebot/v11/ws") == ("wss://example.invalid:13999/onebot/v11/ws")


def test_check_config_cli_output_is_sanitized(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = write_config(tmp_path)
    code = main(["--config", str(path), "--check-config"])
    out = capsys.readouterr().out
    assert code == 0
    assert "unit-test-token" not in out
    assert "127.0.0.1:18799" in out
    assert '"token": "configured"' in out
    assert '"state_file_present"' in out
    logging.getLogger().setLevel(logging.WARNING)


def test_check_config_reports_missing_state(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = write_config(tmp_path)
    code = main(["--config", str(path), "--check-config"])
    out = capsys.readouterr().out
    assert code == 0
    assert '"state_file_present": false' in out
    logging.getLogger().setLevel(logging.WARNING)


def test_main_bad_config_exit_code(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{}", "utf-8")
    code = main(["--config", str(bad), "--check-config"])
    assert code == 2
    logging.getLogger().setLevel(logging.WARNING)
