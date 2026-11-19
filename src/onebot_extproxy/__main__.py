"""onebot-extproxy CLI 入口。

用法::

    onebot-extproxy --config /config/consumers.json
    onebot-extproxy --check-config

所有配置项均可用环境变量覆盖（优先级：CLI > 环境变量 > 配置文件 > 默认值）::

    EXTPROXY_CONFIG / EXTPROXY_HOST / EXTPROXY_PORT / EXTPROXY_STATE_PATH /
    EXTPROXY_BUFFER / EXTPROXY_LOG_LEVEL / EXTPROXY_PING_INTERVAL /
    EXTPROXY_STATS_INTERVAL / EXTPROXY_PROXY
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from .policy import FeaturePolicy
from .relay import LOG, ConfigError, RelayConfig, RelayServer, load_config, sanitize_url


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="extproxy: OneBot v11 反向 WS 中继（外部消费者按群过滤）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--config", "-c", help="consumers.json 路径（默认脚本同目录或 ./consumers.json）")
    parser.add_argument("--state", help="feature_switch state.json 路径（覆盖配置里的 state_path）")
    parser.add_argument("--host", help="监听地址（默认 0.0.0.0）")
    parser.add_argument("--port", type=int, help="监听端口（默认 8090）")
    parser.add_argument("--buffer", type=int, help="远端断线时缓冲最近 N 帧（默认 50）")
    parser.add_argument("--log-level", help="日志级别 CRITICAL/ERROR/WARNING/INFO/DEBUG")
    parser.add_argument("--ping-interval", type=float, help="WS ping 间隔秒（默认禁用）")
    parser.add_argument("--stats-interval", type=float, help="统计日志间隔秒（默认 60）")
    parser.add_argument("--check-config", action="store_true", help="只校验配置并打印脱敏摘要后退出")
    return parser


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # websockets 的 DEBUG 太吵；只保留 WARNING 以上
    logging.getLogger("extproxy.websockets").setLevel(logging.WARNING)


def _print_config(cfg: RelayConfig) -> None:
    summary = {
        "listen": {"host": cfg.listen_host, "port": cfg.listen_port},
        "state_path": cfg.state_path,
        "buffer_size": cfg.buffer_size,
        "reconnect": [cfg.reconnect_min, cfg.reconnect_max],
        "consumers": [
            {
                "name": c.name,
                "feature": c.feature,
                "path": c.path,
                "url": sanitize_url(c.url),
                "token": "configured" if c.token_configured else "none",
            }
            for c in cfg.consumers
        ],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        cfg = load_config(
            args.config,
            cli_host=args.host,
            cli_port=args.port,
            cli_state=args.state,
            cli_buffer=args.buffer,
            cli_log_level=args.log_level,
            cli_ping_interval=args.ping_interval,
            cli_stats_interval=args.stats_interval,
        )
    except ConfigError as exc:
        setup_logging("ERROR")
        LOG.error("config error: %s", exc)
        return 2
    setup_logging(cfg.log_level)
    if args.check_config:
        _print_config(cfg)
        probe = FeaturePolicy(cfg.state_path, default_when_unmanaged=True)
        probe.reload(force=True)
        print(
            json.dumps(
                {
                    "state_file_present": bool(probe.file_present),
                    "state_reloads": probe.reload_count,
                    "state_parse_errors": probe.parse_error_count,
                    "state_note": (
                        "ok"
                        if probe.file_present and probe.parse_error_count == 0
                        else "missing → external consumers default-on"
                        if not probe.file_present
                        else "parse errors → last-good/默认放行"
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    policy = FeaturePolicy(cfg.state_path, default_when_unmanaged=True)
    policy.reload(force=True)
    if policy.file_present:
        LOG.info(
            "state loaded from %s (reloads=%d parse_errors=%d)",
            cfg.state_path,
            policy.reload_count,
            policy.parse_error_count,
        )
    else:
        LOG.warning(
            "state file %s not found: all external consumers default-on (feature_switch matrix ignored until file exists)",
            cfg.state_path,
        )
    server = RelayServer(cfg, policy)
    try:
        asyncio.run(server.run())
    except KeyboardInterrupt:  # pragma: no cover
        LOG.info("interrupted")
    except OSError as exc:
        LOG.error("failed to start server on %s:%d: %r", cfg.listen_host, cfg.listen_port, exc)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover - 由 entrypoint / python -m 触发
    sys.exit(main())
