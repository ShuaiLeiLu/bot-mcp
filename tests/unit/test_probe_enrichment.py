from __future__ import annotations

from datetime import UTC, datetime, timedelta

from probe import (
    ChannelHealth,
    GroupAccountCounts,
    ProbeUsageRecord,
    build_channel_probes,
    resolve_channel_group_ids,
)

CHECKED_AT = datetime(2026, 8, 23, 10, 0, tzinfo=UTC)


def _channel(*, name: str = "任意显示名称", latency_ms: int = 2700) -> ChannelHealth:
    return ChannelHealth(
        monitor_id="19",
        name=name,
        provider="openai",
        model="gpt-test",
        status="operational",
        latency_ms=latency_ms,
        availability_7d=99.8,
        last_checked_at=CHECKED_AT.isoformat(),
        enabled=True,
        group_name="",
    )


def _usage(
    log_id: str,
    *,
    api_key_id: str,
    group_id: str,
    duration_ms: int,
    offset_ms: int = 700,
) -> ProbeUsageRecord:
    return ProbeUsageRecord(
        log_id=log_id,
        api_key_id=api_key_id,
        group_id=group_id,
        model="gpt-test",
        created_at=CHECKED_AT + timedelta(milliseconds=offset_ms),
        duration_ms=duration_ms,
        user_agent="Go-http-client/1.1",
    )


def test_resolves_group_from_the_unique_best_probe_api_key_usage() -> None:
    channel = _channel(latency_ms=2682)
    records = [
        _usage("1", api_key_id="78", group_id="36", duration_ms=2653),
        _usage("2", api_key_id="149", group_id="3", duration_ms=2858, offset_ms=2600),
    ]

    bindings = resolve_channel_group_ids([channel], records)

    assert bindings == {"19": "36"}


def test_does_not_guess_when_two_probe_api_keys_have_near_equal_scores() -> None:
    channel = _channel(latency_ms=2700)
    records = [
        _usage("1", api_key_id="78", group_id="36", duration_ms=2680),
        _usage("2", api_key_id="149", group_id="3", duration_ms=2730),
    ]

    bindings = resolve_channel_group_ids([channel], records)

    assert bindings == {}


def test_build_channel_probes_prefers_api_key_group_id_over_display_names() -> None:
    expected = GroupAccountCounts("47", "team 特惠", 2, 2, 0, 0)

    probes = build_channel_probes(
        [_channel(name="以后随便改名")],
        [expected, GroupAccountCounts("3", "其他组", 1, 1, 0, 0)],
        group_ids_by_monitor={"19": "47"},
    )

    assert probes[0].accounts == expected
