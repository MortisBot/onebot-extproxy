"""policy 单测：四层回落、过滤语义、坏文件 fail-open/last-good、热更新等。

全部使用标准库 + tmp_path，不建网络、不碰生产 state。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest

from onebot_extproxy import policy

GROUP_ON = 10001
GROUP_OFF = 10002
GROUP_OTHER = 10003

# 与生产 state.json 同构的最小样例（功能 key / 群号均为占位）
PROD_STATE = {
    "version": 2,
    "updated_at": "2026-01-01T00:00:00+08:00",
    "group_default": False,
    "group_defaults": {str(GROUP_ON): True},
    "feature_defaults": {"internal_feature": False},
    "matrix": {"chart": {str(GROUP_OFF): False}},
}

WS_FEATURES = ("ws_consumer_a", "ws_consumer_b", "ws_consumer_c")


def write_state(path: Path, data: dict[str, Any] | str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False, indent=2)
    path.write_text(text, "utf-8")
    # 保证 mtime_ns 变化，避免同一 ns 内连续写被 stat 缓存命中
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    return path


def managed_state(**overrides: Any) -> dict[str, Any]:
    state: dict[str, Any] = {
        "version": 2,
        "group_default": False,
        "group_defaults": {},
        "feature_defaults": {},
        "matrix": {},
    }
    state.update(overrides)
    return state


@pytest.fixture()
def managed_policy(tmp_path: Path) -> policy.FeaturePolicy:
    state = managed_state(
        feature_defaults={"ws_consumer_a": True},
        matrix={"ws_consumer_a": {str(GROUP_OFF): False}},
    )
    return policy.FeaturePolicy(write_state(tmp_path / "state.json", state))


# ---------------------------------------------------------------- 默认 / 四层回落
def test_missing_file_defaults_on(tmp_path: Path) -> None:
    missing = tmp_path / "nope" / "state.json"
    p = policy.FeaturePolicy(missing)
    p.reload(force=True)
    assert p.file_present is False
    for feature in WS_FEATURES:
        assert not p.is_managed(feature)
        for group in (GROUP_ON, GROUP_OFF, GROUP_OTHER, "10004"):
            assert p.is_enabled(feature, group)
    assert p.is_enabled("ws_consumer_a", None) is True
    assert p.source_of("ws_consumer_a", GROUP_OFF).startswith("unmanaged")


def test_current_prod_state_shape(tmp_path: Path) -> None:
    p = policy.FeaturePolicy(write_state(tmp_path / "state.json", PROD_STATE))
    # 外部 feature 完全缺失 → 默认 True，即使 group_default=False
    for feature in WS_FEATURES:
        for group in (GROUP_OFF, GROUP_OTHER):
            assert p.is_enabled(feature, group)
    # 内部 feature（已纳管）走正常四层回落
    assert not p.is_enabled("chart", GROUP_OFF)
    assert p.is_enabled("chart", GROUP_ON)
    assert not p.is_enabled("chart", GROUP_OTHER)
    assert not p.is_enabled("internal_feature", GROUP_ON)
    assert p.reload_count == 1


def test_matrix_feature_group_fallbacks(tmp_path: Path) -> None:
    state = {
        "version": 2,
        "group_default": False,
        "group_defaults": {str(GROUP_ON): True},
        "feature_defaults": {"ws_consumer_b": True, "ws_consumer_c": False},
        "matrix": {
            "ws_consumer_b": {str(GROUP_OFF): False},
            "ws_consumer_c": {str(GROUP_ON): True},
        },
    }
    p = policy.FeaturePolicy(write_state(tmp_path / "state.json", state))
    # ws_consumer_b：matrix 覆盖 → feature_default → group fallback
    assert not p.is_enabled("ws_consumer_b", GROUP_OFF)
    assert p.is_enabled("ws_consumer_b", GROUP_ON)
    assert p.is_enabled("ws_consumer_b", GROUP_OTHER)
    # ws_consumer_c：matrix true → feature_default false
    assert p.is_enabled("ws_consumer_c", GROUP_ON)
    assert not p.is_enabled("ws_consumer_c", GROUP_OTHER)
    assert p.source_of("ws_consumer_b", GROUP_OFF) == "matrix"
    assert p.source_of("ws_consumer_b", GROUP_OTHER) == "feature_default"


def test_group_default_layers(tmp_path: Path) -> None:
    state = {
        "version": 2,
        "group_default": True,
        "group_defaults": {str(GROUP_ON): False},
        "feature_defaults": {},
        "matrix": {"ws_consumer_a": {"10004": False}},
    }
    p = policy.FeaturePolicy(write_state(tmp_path / "state.json", state))
    assert not p.is_enabled("ws_consumer_a", "10004")
    assert not p.is_enabled("ws_consumer_a", GROUP_ON)
    assert p.is_enabled("ws_consumer_a", GROUP_OTHER)
    assert p.source_of("ws_consumer_a", GROUP_ON) == "group_default(entry)"
    assert p.source_of("ws_consumer_a", GROUP_OTHER) == "group_default"

    p2 = policy.FeaturePolicy(
        write_state(
            tmp_path / "state2.json",
            managed_state(group_default=False, matrix={"ws_consumer_a": {"1": True}}),
        )
    )
    assert not p2.is_enabled("ws_consumer_a", GROUP_OTHER)


def test_group_id_str_int_equivalent(tmp_path: Path) -> None:
    state = managed_state(matrix={"ws_consumer_a": {str(GROUP_OFF): False}})
    p = policy.FeaturePolicy(write_state(tmp_path / "state.json", state))
    assert p.is_enabled("ws_consumer_a", GROUP_OFF) == p.is_enabled("ws_consumer_a", str(GROUP_OFF))
    assert not p.is_enabled("ws_consumer_a", GROUP_OFF)


def test_managed_detection(tmp_path: Path) -> None:
    state = managed_state(
        feature_defaults={"ws_consumer_b": True},
        matrix={"ws_consumer_c": {"1": True}},
    )
    p = policy.FeaturePolicy(write_state(tmp_path / "state.json", state))
    assert p.is_managed("ws_consumer_b")
    assert p.is_managed("ws_consumer_c")
    assert not p.is_managed("ws_consumer_a")
    assert not p.is_managed("chart")


def test_unmanaged_default_param(tmp_path: Path) -> None:
    p = policy.FeaturePolicy(tmp_path / "missing.json", default_when_unmanaged=False)
    assert not p.is_enabled("ws_consumer_a", GROUP_ON)
    assert "default=False" in p.source_of("ws_consumer_a", GROUP_ON)


# ---------------------------------------------------------------- 事件 / 帧过滤
def test_filter_event_semantics(managed_policy: policy.FeaturePolicy) -> None:
    p = managed_policy

    def fwd(event: dict[str, Any]) -> bool:
        ok, _reason = policy.should_forward_event(event, p, "ws_consumer_a")
        return ok

    assert not fwd({"post_type": "message", "group_id": GROUP_OFF})
    assert fwd({"post_type": "message", "group_id": GROUP_ON})
    assert not fwd({"post_type": "message", "group_id": str(GROUP_OFF)})
    assert fwd({"post_type": "message"})
    assert fwd({"post_type": "message", "group_id": None})
    assert fwd(
        {
            "post_type": "meta_event",
            "meta_event_type": "lifecycle",
            "sub_type": "connect",
            "group_id": GROUP_OFF,
        }
    )
    assert fwd({"post_type": "meta_event", "meta_event_type": "heartbeat", "group_id": GROUP_OFF})
    assert not fwd({"post_type": "notice", "group_id": GROUP_OFF})
    assert not fwd({"post_type": "request", "group_id": GROUP_OFF})
    assert fwd({"post_type": "notice"})
    assert fwd({"post_type": "unknown_type", "group_id": GROUP_OFF})
    assert fwd({"group_id": GROUP_OFF})


def test_filter_frame_non_json_passthrough(managed_policy: policy.FeaturePolicy) -> None:
    p = managed_policy
    non_event_frames: list[Any] = [
        "plain text",
        b"\x00\xff\xfe",
        "[1,2,3]",
        "42",
        123,
        None,
    ]
    for frame in non_event_frames:
        ok, reason, parsed = policy.should_forward_frame(frame, p, "ws_consumer_a")
        assert ok, f"non-event frame {frame!r} passthrough ({reason})"
        assert parsed is None

    event_cases = [
        (json.dumps({"post_type": "message", "group_id": GROUP_ON}), True),
        (json.dumps({"post_type": "message", "group_id": GROUP_OFF}), False),
        (json.dumps({"post_type": "message", "group_id": GROUP_OFF}).encode(), False),
        (
            json.dumps({"post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "connect"}),
            True,
        ),
    ]
    for frame, expected in event_cases:
        ok, reason, parsed = policy.should_forward_frame(frame, p, "ws_consumer_a")
        assert ok is expected, f"frame {frame!r} ({reason})"
        assert isinstance(parsed, dict)


def test_lifecycle_and_decode_helpers() -> None:
    assert policy.is_lifecycle_connect(
        {"post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "connect"}
    )
    assert not policy.is_lifecycle_connect(
        {"post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "enable"}
    )
    assert not policy.is_lifecycle_connect(None)
    assert not policy.is_lifecycle_connect({"post_type": "message"})

    assert policy.decode_json_frame(b'{"a": 1}') == {"a": 1}
    assert policy.decode_json_frame('{"a": 1}') == {"a": 1}
    assert policy.decode_json_frame(b"[1, 2]") is None
    assert policy.decode_json_frame("not json") is None
    assert policy.decode_json_frame(123) is None
    assert policy.decode_json_frame(None) is None
    assert policy.decode_json_frame(b"\xff") is None


def test_empty_and_normalize_state() -> None:
    st = policy.empty_state()
    assert st["group_default"] is False
    assert st["matrix"] == {} and st["feature_defaults"] == {}
    with pytest.raises(ValueError):
        policy.normalize_state([])
    with pytest.raises(ValueError):
        policy.normalize_state("x")

    norm = policy.normalize_state(
        {
            "version": "abc",
            "updated_at": 5,
            "group_default": 1,
            "group_defaults": {"1": True, "bad": "x"},
            "feature_defaults": {"f": 0},
            "matrix": {"f": {"2": 1}, "bad": "not-a-dict", "empty": {}},
        }
    )
    assert norm["version"] == policy.STATE_VERSION
    assert norm["updated_at"] is None
    assert norm["group_default"] is True
    assert norm["group_defaults"] == {"1": True}
    assert norm["feature_defaults"] == {"f": False}
    assert norm["matrix"] == {"f": {"2": True}}


# ---------------------------------------------------------------- 坏文件 / 热更新
def test_bad_state_first_load_fail_open(tmp_path: Path) -> None:
    path = write_state(tmp_path / "state.json", "{ this is not json")
    p = policy.FeaturePolicy(path)
    assert p.is_enabled("ws_consumer_a", GROUP_OFF)
    assert not p.is_managed("ws_consumer_a")
    assert p.parse_error_count >= 1
    assert p.is_enabled("ws_consumer_b", 123) is True
    assert p.last_error is not None


def test_bad_state_keeps_last_good(tmp_path: Path) -> None:
    path = write_state(
        tmp_path / "state.json",
        managed_state(feature_defaults={"ws_consumer_a": False}),
    )
    p = policy.FeaturePolicy(path)
    assert not p.is_enabled("ws_consumer_a", GROUP_ON)

    write_state(path, "{ broken")
    assert not p.is_enabled("ws_consumer_a", GROUP_ON)  # last-good 仍禁用
    assert p.parse_error_count >= 1

    write_state(
        path,
        managed_state(feature_defaults={"ws_consumer_a": True}),
    )
    assert p.is_enabled("ws_consumer_a", GROUP_ON)  # 修好后恢复


def test_mtime_cache_and_reload(tmp_path: Path) -> None:
    path = write_state(tmp_path / "state.json", managed_state(group_default=True))
    p = policy.FeaturePolicy(path)
    p.reload(force=True)
    count_after_load = p.reload_count
    for _ in range(5):
        p.is_enabled("ws_consumer_a", GROUP_OTHER)
    assert p.reload_count == count_after_load  # stat 未变 → 不重载

    write_state(
        path,
        managed_state(feature_defaults={"ws_consumer_a": False}),
    )
    assert not p.is_enabled("ws_consumer_a", GROUP_OTHER)  # 热更新生效
    assert p.reload_count > count_after_load

    old = p.reload_count
    assert p.reload(force=True) is True
    assert p.reload_count > old


def test_state_deleted_resets_default(tmp_path: Path) -> None:
    path = write_state(
        tmp_path / "state.json",
        managed_state(feature_defaults={"ws_consumer_a": False}),
    )
    p = policy.FeaturePolicy(path)
    assert not p.is_enabled("ws_consumer_a", GROUP_ON)
    path.unlink()
    assert p.is_enabled("ws_consumer_a", GROUP_ON)
    assert not p.is_managed("ws_consumer_a")


def test_non_bool_values_are_ignored(tmp_path: Path) -> None:
    state = managed_state(
        feature_defaults={"ws_consumer_b": 0, "ws_consumer_c": 1, "ws_consumer_a": "false"},
        matrix={"ws_consumer_a": {"1": "no"}},
    )
    p = policy.FeaturePolicy(write_state(tmp_path / "state.json", state))
    assert not p.is_enabled("ws_consumer_b", GROUP_ON)
    assert p.is_enabled("ws_consumer_c", GROUP_ON)
    # 字符串布尔不可信：不参与纳管/匹配 → 视为未纳管 → 默认 True（fail-open）
    assert not p.is_managed("ws_consumer_a")
    assert p.is_enabled("ws_consumer_a", GROUP_ON)
    assert p.snapshot()["feature_defaults"].get("ws_consumer_a") is None


def test_snapshot_isolation(managed_policy: policy.FeaturePolicy) -> None:
    p = managed_policy
    snap = p.snapshot()
    snap["matrix"]["ws_consumer_a"][str(GROUP_ON)] = False
    assert p.is_enabled("ws_consumer_a", GROUP_ON)
    assert "group_default" in snap
