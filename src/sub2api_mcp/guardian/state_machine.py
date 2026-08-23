"""Guardian fuse, degrade, recovery, and manual-control state machine."""

from __future__ import annotations

from .contracts import (
    ChannelDecision,
    ChannelDecisionInput,
    GuardianEventType,
    GuardianHealth,
    ManualControl,
)

_FAILURES = {
    GuardianEventType.GATEWAY_ERROR,
    GuardianEventType.QUOTA_EXHAUSTED,
    GuardianEventType.PROBE_FAIL,
    GuardianEventType.UPSTREAM_UNKNOWN,
    GuardianEventType.FATAL,
}


def _would_break_minimum_pool(value: ChannelDecisionInput) -> bool:
    return value.group_available_count <= value.breaker.min_pool_size


def _fused_or_forced(value: ChannelDecisionInput, reason: str) -> ChannelDecision:
    if _would_break_minimum_pool(value):
        return ChannelDecision(
            health=GuardianHealth.FORCED_KEEP,
            should_schedule=True,
            should_probe=True,
            can_auto_recover=True,
            reason=f"minimum_pool:{reason}",
        )
    return ChannelDecision(
        health=GuardianHealth.FUSED,
        should_schedule=False,
        should_probe=value.recovery.enabled,
        can_auto_recover=value.recovery.enabled,
        reason=reason,
    )


def decide_channel_state(value: ChannelDecisionInput) -> ChannelDecision:
    if value.manual_control is ManualControl.EXCLUDED:
        return ChannelDecision(
            health=GuardianHealth.EXCLUDED,
            should_schedule=False,
            should_probe=False,
            can_auto_recover=False,
            reason="manual_exclusion",
        )
    if value.manual_control is ManualControl.PAUSED:
        return ChannelDecision(
            health=GuardianHealth.MANUALLY_PAUSED,
            should_schedule=False,
            should_probe=True,
            can_auto_recover=False,
            reason="manual_pause",
        )

    if value.current_health is GuardianHealth.FUSED:
        held_long_enough = bool(
            value.healthy_since is not None
            and (value.now - value.healthy_since).total_seconds() >= value.recovery.hold_seconds
        )
        if (
            value.recovery.enabled
            and value.score >= value.recovery.target_score
            and value.success_streak >= value.recovery.success_count
            and held_long_enough
        ):
            return ChannelDecision(
                health=GuardianHealth.HEALTHY,
                should_schedule=True,
                should_probe=True,
                can_auto_recover=True,
                reason="recovery_threshold_met",
            )
        return ChannelDecision(
            health=GuardianHealth.FUSED,
            should_schedule=False,
            should_probe=value.recovery.enabled,
            can_auto_recover=value.recovery.enabled,
            reason="awaiting_recovery",
        )

    if value.breaker.enabled:
        if value.breaker.hard_fatal and GuardianEventType.FATAL in value.recent_events[:1]:
            return _fused_or_forced(value, "fatal_event")
        http_events = value.recent_events[: value.breaker.http_window]
        if (
            sum(event in _FAILURES for event in http_events) >= value.breaker.http_failures
            and value.score < value.breaker.http_score_below
        ):
            return _fused_or_forced(value, "error_window")
        latency_values = value.recent_ttfb_ms[: value.breaker.latency_window]
        if (
            sum(ttfb > value.breaker.latency_ttfb_ms for ttfb in latency_values)
            >= value.breaker.latency_occurrences
        ):
            if not value.breaker.latency_degrade_only:
                return _fused_or_forced(value, "latency_window")
            return ChannelDecision(
                health=GuardianHealth.DEGRADED,
                should_schedule=True,
                should_probe=True,
                can_auto_recover=True,
                reason="latency_degraded",
            )

    if value.degrade.enabled and value.score < value.degrade.score_threshold:
        return ChannelDecision(
            health=GuardianHealth.DEGRADED,
            should_schedule=True,
            should_probe=True,
            can_auto_recover=True,
            reason="score_degraded",
        )
    return ChannelDecision(
        health=GuardianHealth.HEALTHY,
        should_schedule=True,
        should_probe=True,
        can_auto_recover=True,
        reason="healthy",
    )
