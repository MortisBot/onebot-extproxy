"""onebot-extproxy —— OneBot v11 / NapCat 反向 WS 中继（外部消费者按群过滤）。

NapCat 以「反向 WS 客户端」方式外连多个消费者；OneBot v11 / NapCat 本身没有按群订阅
能力。本中继把「按功能 × 群」的过滤放在 NapCat 与远端消费者之间：

- WS server 侧接 NapCat（每个 consumer 一个路径）；
- WS client 侧用与 NapCat 相同的握手头外连远端；
- 只过滤 ``post_type ∈ {message, notice, request}`` 且带 ``group_id`` 的事件帧，
  过滤策略来自 feature_switch 的 ``state.json``（四层回落，热更新）；
- 远端 → NapCat 的 API 调用/响应完全透传。

安装后入口：``onebot-extproxy --config /config/consumers.json``；
模块化用法：``from onebot_extproxy.relay import RelayServer``。
"""

from __future__ import annotations

from .policy import (
    FeaturePolicy,
    decode_json_frame,
    is_lifecycle_connect,
    should_forward_event,
    should_forward_frame,
)
from .relay import ConsumerConfig, RelayConfig, RelayServer, load_config, sanitize_url

__version__ = "0.1.0"

__all__ = [
    "ConsumerConfig",
    "FeaturePolicy",
    "RelayConfig",
    "RelayServer",
    "__version__",
    "decode_json_frame",
    "is_lifecycle_connect",
    "load_config",
    "sanitize_url",
    "should_forward_event",
    "should_forward_frame",
]
