"""pytest 公共配置：集成测试默认跳过，需显式 ``--run-integration``。"""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="运行本机回环集成测试（127.0.0.1 临时端口，不连接任何外部服务）",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: 本机回环集成测试（默认 skip，--run-integration 才跑）",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(reason="需 --run-integration 才运行（不连外网的本机回环测试）")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def _clean_extproxy_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """避免宿主环境里的 EXTPROXY_* 干扰配置测试。"""
    for key in (
        "EXTPROXY_CONFIG",
        "EXTPROXY_HOST",
        "EXTPROXY_PORT",
        "EXTPROXY_STATE_PATH",
        "EXTPROXY_BUFFER",
        "EXTPROXY_LOG_LEVEL",
        "EXTPROXY_PING_INTERVAL",
        "EXTPROXY_STATS_INTERVAL",
        "EXTPROXY_PROXY",
    ):
        monkeypatch.delenv(key, raising=False)
