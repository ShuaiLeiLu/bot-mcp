from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sub2api_mcp.guardian.contracts import (
    BreakerPolicy,
    ChannelDecisionInput,
    GuardianEventType,
    GuardianFreshness,
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
    assert paused.should_probe is False
    assert paused.can_auto_recover is False
    assert excluded.health is GuardianHealth.EXCLUDED
    assert excluded.should_probe is False


def test_upstream_disabled_is_never_automatically_reenabled_or_probed() -> None:
    decision = decide_channel_state(_input(schedulable=False, score=100))

    assert decision.health is GuardianHealth.UPSTREAM_DISABLED
    assert decision.should_schedule is False
    assert decision.should_probe is False
    assert decision.can_auto_recover is False
    assert decision.reason == "upstream_disabled"


def test_fatal_event_fuses_only_after_trusted_confirmation() -> None:
    unconfirmed = decide_channel_state(
        _input(score=0, recent_events=[GuardianEventType.FATAL], fatal_confirmed=False)
    )
    fused = decide_channel_state(
        _input(score=0, recent_events=[GuardianEventType.FATAL], fatal_confirmed=True)
    )
    forced = decide_channel_state(
        _input(
            score=0,
            recent_events=[GuardianEventType.FATAL],
            fatal_confirmed=True,
            group_available_count=1,
        )
    )

    assert unconfirmed.health is GuardianHealth.DEGRADED
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


def test_low_confidence_and_stale_evidence_freeze_automatic_actions() -> None:
    low_confidence = decide_channel_state(
        _input(
            score=10,
            confidence=0.59,
            recent_events=[GuardianEventType.PROBE_FAIL] * 5,
        )
    )
    stale = decide_channel_state(
        _input(
            score=10,
            confidence=1,
            freshness=GuardianFreshness.STALE,
            recent_events=[GuardianEventType.PROBE_FAIL] * 5,
        )
    )

    assert low_confidence.health is GuardianHealth.HEALTHY
    assert low_confidence.should_schedule is True
    assert low_confidence.reason == "low_confidence"
    assert stale.health is GuardianHealth.STALE
    assert stale.should_schedule is True
    assert stale.reason == "evidence_stale"


def test_error_window_requires_fuse_confidence() -> None:
    decision = decide_channel_state(
        _input(
            score=10,
            confidence=0.84,
            recent_events=[GuardianEventType.GATEWAY_ERROR] * 5,
        )
    )

    assert decision.health is GuardianHealth.DEGRADED
    assert decision.reason == "score_degraded"


def test_recovery_requires_score_streak_and_hold_duration() -> None:
    not_held = decide_channel_state(
        _input(
            current_health="FUSED",
            score=90,
            success_streak=3,
            healthy_since=NOW - timedelta(seconds=30),
        )
    )
    recovered = decide_channel_state(
        _input(
            current_health="FUSED",
            score=90,
            success_streak=3,
            healthy_since=NOW - timedelta(seconds=61),
        )
    )

    assert not_held.health is GuardianHealth.FUSED
    assert recovered.health is GuardianHealth.HEALTHY
    assert recovered.should_schedule is True


def test_recovery_requires_guardian_ownership_and_recovery_confidence() -> None:
    human_owned = decide_channel_state(
        _input(
            current_health="FUSED",
            guardian_owned_fuse=False,
            score=100,
            confidence=1,
            success_streak=3,
            healthy_since=NOW - timedelta(seconds=90),
        )
    )
    low_confidence = decide_channel_state(
        _input(
            current_health="FUSED",
            guardian_owned_fuse=True,
            score=100,
            confidence=0.84,
            success_streak=3,
            healthy_since=NOW - timedelta(seconds=90),
        )
    )

    assert human_owned.health is GuardianHealth.FUSED
    assert human_owned.should_probe is False
    assert human_owned.reason == "fuse_not_guardian_owned"
    assert low_confidence.health is GuardianHealth.FUSED
    assert low_confidence.reason == "awaiting_recovery_confidence"


def test_fused_channel_cannot_recover_before_cooldown_expires() -> None:
    decision = decide_channel_state(
        _input(
            current_health=GuardianHealth.FUSED,
            score=90,
            success_streak=3,
            healthy_since=NOW - timedelta(seconds=90),
            fused_until=NOW + timedelta(seconds=1),
        )
    )

    assert decision.health is GuardianHealth.FUSED
    assert decision.reason == "fused_cooldown"


def test_fused_slow_channel_with_usable_pool_returns_to_degraded_after_cooldown() -> None:
    decision = decide_channel_state(
        _input(
            current_health=GuardianHealth.FUSED,
            score=20,
            recent_events=[GuardianEventType.SLOW_TTFB],
            group_available_count=3,
            fused_until=NOW - timedelta(seconds=1),
        )
    )

    assert decision.health is GuardianHealth.DEGRADED
    assert decision.should_schedule is True
    assert decision.reason == "usable_pool_slow_response"
