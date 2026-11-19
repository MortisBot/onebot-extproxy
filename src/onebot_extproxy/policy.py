"""extproxy 过滤策略：读取 feature_switch 的 state.json，按「功能 × 群」四层回落决策。

设计约束（与 ``nonebot/plugins/feature_switch/store.py`` 保持同一份状态文件/同一套语义）::

    matrix[feature][group] → feature_defaults[feature] → group_defaults[group] → group_default

与 feature_switch 的唯一差异（本模块的核心需求）：
- 外部 WS 消费者功能 key（``ws_maimai`` / ``ws_pjsk`` / ``ws_misc``）在 state 中**完全缺失**
  （既不在 ``matrix`` 也不在 ``feature_defaults``）时视为「未纳管」，返回
  ``default_when_unmanaged``（默认 True）—— 保持「NapCat 全量转发」的现状；
- 一旦该 feature 出现在 state 里（哪怕只有 matrix 一行），就进入受管模式，严格按四层回落。

过滤语义：只丢弃 ``post_type ∈ {message, notice, request}`` 且带 ``group_id``、且
(feature, group) 被关闭的事件帧；``meta_event``、无 ``group_id`` 的帧（私聊等）一律放行。

坏文件策略（与 feature_switch 的 last-good 一致，但首读失败时 fail-open）：
- 已成功加载过 → JSON 解析失败保留 last-good，只计数；
- 从未成功加载过 → 使用空状态（所有 feature 未纳管 → 默认放行全部），不崩溃；
- 文件被删除 → 回到空状态（未纳管 → 默认放行全部）；
- 其它 OSError（如权限）→ 有 last-good 用 last-good，否则空状态。

本模块**只用标准库**，可脱离 websockets/NoneBot 在任何 Python 3.10+ 上单测。
"""

from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("extproxy.policy")

#: 需要按群过滤的 event 类型（OneBot v11）；其余 post_type 一律放行
FILTERED_POST_TYPES = frozenset(("message", "notice", "request"))

STATE_VERSION = 2


def _as_bool(value: Any) -> bool | None:
    """把 state 里的值规整成 bool；无法安全判断的返回 None（视为「未设置」）。"""
    if isinstance(value, bool):
        return value
    # JSON 里 0/1 也常被当作布尔；其它整数/字符串不做猜测（避免 "false" → True 的坑）
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    return None


def empty_state() -> dict[str, Any]:
    """空状态：所有 feature 都未纳管。"""
    return {
        "version": STATE_VERSION,
        "updated_at": None,
        "group_default": False,
        "group_defaults": {},
        "feature_defaults": {},
        "matrix": {},
    }


def normalize_state(raw: Any) -> dict[str, Any]:
    """容错规整 state.json；非法/缺失字段回落空值（不抛异常，除根不是对象时）。"""
    if not isinstance(raw, dict):
        raise ValueError("state root must be a JSON object")
    st = empty_state()
    try:
        st["version"] = int(raw.get("version", STATE_VERSION) or STATE_VERSION)
    except (TypeError, ValueError):
        st["version"] = STATE_VERSION
    updated = raw.get("updated_at")
    st["updated_at"] = updated if isinstance(updated, str) else None

    gd = _as_bool(raw.get("group_default"))
    st["group_default"] = bool(gd) if gd is not None else False

    gds = raw.get("group_defaults")
    if isinstance(gds, dict):
        out: dict[str, bool] = {}
        for key, value in gds.items():
            b = _as_bool(value)
            if b is not None:
                out[str(key)] = b
        st["group_defaults"] = out

    fds = raw.get("feature_defaults")
    if isinstance(fds, dict):
        out = {}
        for key, value in fds.items():
            b = _as_bool(value)
            if b is not None:
                out[str(key)] = b
        st["feature_defaults"] = out

    mx = raw.get("matrix")
    matrix: dict[str, dict[str, bool]] = {}
    if isinstance(mx, dict):
        for feature, groups in mx.items():
            if not isinstance(groups, dict):
                continue
            row: dict[str, bool] = {}
            for gid, value in groups.items():
                b = _as_bool(value)
                if b is not None:
                    row[str(gid)] = b
            if row:
                matrix[str(feature)] = row
    st["matrix"] = matrix
    return st


class FeaturePolicy:
    """feature_switch state.json 的只读视图（mtime+size 缓存，支持热更新）。"""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        default_when_unmanaged: bool = True,
    ) -> None:
        self.path = Path(path)
        self.default_when_unmanaged = bool(default_when_unmanaged)
        self._lock = threading.RLock()
        self._stat_key: tuple[int, int] | None = None
        self._loaded = False  # 是否成功从文件加载过（决定 last-good 语义）
        self._state: dict[str, Any] = empty_state()
        self.reload_count = 0
        self.parse_error_count = 0
        self.last_error: str | None = None
        self.file_present: bool | None = None

    # ------------------------------------------------------------------ load
    def _load_locked(self) -> dict[str, Any]:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            # 文件不存在：回落到「未纳管」空状态（删除 state = 回落默认）
            if self.file_present is not False or self._loaded:
                self._state = empty_state()
                self._loaded = False
                self._stat_key = None
                self.file_present = False
                self.reload_count += 1
                LOGGER.info("policy: state file %s not found, all external consumers default-on", self.path)
            return self._state
        except OSError as exc:
            self.parse_error_count += 1
            self.last_error = repr(exc)
            self.file_present = True
            if not self._loaded:
                self._state = empty_state()
            LOGGER.warning(
                "policy: cannot stat %s: %r; keep %s",
                self.path,
                exc,
                "last-good" if self._loaded else "empty/default-on",
            )
            return self._state

        key = (stat.st_mtime_ns, stat.st_size)
        if key == self._stat_key:
            return self._state
        self.file_present = True
        try:
            raw = json.loads(self.path.read_text("utf-8"))
            state = normalize_state(raw)
        except (OSError, ValueError, TypeError) as exc:
            self.parse_error_count += 1
            self.last_error = repr(exc)
            self._stat_key = key
            if not self._loaded:
                self._state = empty_state()
                self._loaded = False
            LOGGER.warning(
                "policy: bad state %s: %r; keep %s",
                self.path,
                exc,
                "last-good" if self._loaded else "empty/default-on",
            )
            return self._state
        self._state = state
        self._loaded = True
        self._stat_key = key
        self.reload_count += 1
        LOGGER.debug("policy: loaded %s (reload #%d)", self.path, self.reload_count)
        return self._state

    def reload(self, force: bool = False) -> bool:
        """按需重载；返回是否有变化（force=True 时忽略 stat 缓存）。"""
        with self._lock:
            before = (self._stat_key, self.reload_count)
            if force:
                self._stat_key = None
            self._load_locked()
            return (self._stat_key, self.reload_count) != before

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._load_locked(), ensure_ascii=False))

    # --------------------------------------------------------------- queries
    def _state_ro(self) -> dict[str, Any]:
        with self._lock:
            return self._load_locked()

    def is_managed(self, feature: str) -> bool:
        """该 feature 是否已被写入 state（matrix 或 feature_defaults 出现即算纳管）。"""
        st = self._state_ro()
        return feature in st["matrix"] or feature in st["feature_defaults"]

    def is_enabled(self, feature: str, group_id: str | int | None) -> bool:
        """(功能, 群) 是否放行；group_id=None（私聊/非群）永远放行。"""
        if group_id is None:
            return True
        st = self._state_ro()
        if not (feature in st["matrix"] or feature in st["feature_defaults"]):
            return self.default_when_unmanaged
        gid = str(group_id)
        row = st["matrix"].get(feature) or {}
        if gid in row:
            return row[gid]
        if feature in st["feature_defaults"]:
            return st["feature_defaults"][feature]
        return st["group_defaults"].get(gid, st["group_default"])

    def source_of(self, feature: str, group_id: str | int | None) -> str:
        """调试用：说明当前取值来自哪一层。"""
        if group_id is None:
            return "non-group(always-on)"
        st = self._state_ro()
        if not (feature in st["matrix"] or feature in st["feature_defaults"]):
            return f"unmanaged(default={self.default_when_unmanaged})"
        gid = str(group_id)
        if gid in (st["matrix"].get(feature) or {}):
            return "matrix"
        if feature in st["feature_defaults"]:
            return "feature_default"
        if gid in st["group_defaults"]:
            return "group_default(entry)"
        return "group_default"


# ---------------------------------------------------------------------------
# 事件/帧级过滤（纯函数，供 relay 与 self_check 共用）
# ---------------------------------------------------------------------------
def decode_json_frame(raw: Any) -> dict[str, Any] | None:
    """把 WS 帧解码为 JSON 对象；非 JSON / 非 UTF-8 / 非对象一律返回 None。"""
    if isinstance(raw, bytes):
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    elif isinstance(raw, str):
        text = raw
    else:
        return None
    try:
        data = json.loads(text)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


def is_lifecycle_connect(event: dict[str, Any] | None) -> bool:
    """是否为 OneBot lifecycle connect meta event（远端重连时要重放的那一帧）。"""
    return bool(
        isinstance(event, dict)
        and event.get("post_type") == "meta_event"
        and event.get("meta_event_type") == "lifecycle"
        and event.get("sub_type") == "connect"
    )


def should_forward_event(
    event: dict[str, Any],
    policy: FeaturePolicy,
    feature: str,
) -> tuple[bool, str]:
    """判断一个 OneBot 事件对象是否应转发给远端；返回 (是否转发, 原因)。"""
    post_type = event.get("post_type")
    if post_type not in FILTERED_POST_TYPES:
        return True, "not-filtered-post-type"
    group_id = event.get("group_id")
    if group_id is None:
        return True, "no-group"
    if policy.is_enabled(feature, group_id):
        return True, "enabled"
    return False, "group-disabled"


def should_forward_frame(
    raw: Any,
    policy: FeaturePolicy,
    feature: str,
) -> tuple[bool, str, dict[str, Any] | None]:
    """帧级判断：非 JSON 帧原样放行；JSON 事件按策略过滤。

    返回 ``(forward, reason, parsed_event_or_None)``。
    """
    event = decode_json_frame(raw)
    if event is None:
        return True, "non-json", None
    ok, reason = should_forward_event(event, policy, feature)
    return ok, reason, event
