from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sub2api_mcp.guardian.contracts import (
    BreakerPolicy,
    ChannelDecisionInput,
    GuardianEventType,
    GuardianHealth,
    ManualControl,
    RecoveryPolicy,
)
from sub2api_mcp.guardian.state_machine import decide_channel_state

NOW = datetime(2026, 8, 23, 10, 0, tzinfo=UTC)


def _input(**overrides: object) -> ChannelDecisionInput:
    values: dict[str, object] = {
        "channel_id": "42",
        "score": 100,
        "recent_events": [GuardianEventType.PERFECT],
        "recent_ttfb_ms": [100],
        "current_health": GuardianHealth.HEALTHY,
        "manual_control": ManualControl.NONE,
        "schedulable": True,
        "group_available_count": 3,
        "now": NOW,
        "breaker": BreakerPolicy(),
        "recovery": RecoveryPolicy(),
    }
    values.update(overrides)
    return ChannelDecisionInput.model_validate(values)


def test_manual_pause_and_exclude_override_all_automatic_rules() -> None:
    paused = decide_channel_state(_input(manual_control="PAUSED", score=100))
    excluded = decide_channel_state(_input(manual_control="EXCLUDED", score=100))

    assert paused.health is GuardianHealth.MANUALLY_PAUSED
    assert paused.should_schedule is False
    assert paused.can_auto_recover is False
    assert excluded.health is GuardianHealth.EXCLUDED
    assert excluded.should_probe is False


def test_fatal_event_fuses_but_minimum_pool_forces_keep() -> None:
    fused = decide_channel_state(_input(score=0, recent_events=[GuardianEventType.FATAL]))
    forced = decide_channel_state(
        _input(
            score=0,
            recent_events=[GuardianEventType.FATAL],
            group_available_count=1,
        )
    )

    assert fused.health is GuardianHealth.FUSED
    assert fused.should_schedule is False
    assert forced.health is GuardianHealth.FORCED_KEEP
    assert forced.should_schedule is True


def test_error_window_requires_failure_count_and_low_score() -> None:
    decision = decide_channel_state(
        _input(
            score=55,
            recent_events=[
                GuardianEventType.GATEWAY_ERROR,
                GuardianEventType.GATEWAY_ERROR,
                GuardianEventType.GATEWAY_ERROR,
                GuardianEventType.PERFECT,
                GuardianEventType.PERFECT,
            ],
        )
    )

    assert decision.health is GuardianHealth.FUSED


def test_recovery_requires_score_streak_and_hold_duration() -> None:
    not_held = decide_channel_state(
        _input(
            current_health="FUSED",
            score=90,
            success_streak=3,
            healthy_since=NOW - timedelta(seconds=30),
            schedulable=False,
        )
    )
    recovered = decide_channel_state(
        _input(
            current_health="FUSED",
            score=90,
            success_streak=3,
            healthy_since=NOW - timedelta(seconds=61),
            schedulable=False,
        )
    )

    assert not_held.health is GuardianHealth.FUSED
    assert recovered.health is GuardianHealth.HEALTHY
    assert recovered.should_schedule is True
