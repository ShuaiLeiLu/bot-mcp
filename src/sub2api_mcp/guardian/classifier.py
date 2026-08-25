"""Conservative external result classification."""

from __future__ import annotations

import re

from .contracts import ClassifiedSample, GuardianEventType, ScoringPolicy

_CONTROL = re.compile(r"[\x00-\x1f\x7f]")


def _safe_message(value: str) -> str:
    return _CONTROL.sub(" ", str(value or "")).strip()[:500]


def classify_sample(
    *,
    status_code: int | None,
    message: str,
    ttfb_ms: int | None,
    succeeded: bool,
    policy: ScoringPolicy,
) -> ClassifiedSample:
    safe = _safe_message(message)
    lowered = safe.casefold()
    if status_code in {401, 402, 403}:
        event = GuardianEventType.FATAL
    elif status_code == 429:
        event = GuardianEventType.QUOTA_EXHAUSTED
    elif succeeded:
        event = (
            GuardianEventType.SLOW_TTFB
            if ttfb_ms is not None and ttfb_ms > policy.slow_ttfb_ms
            else GuardianEventType.PERFECT
        )
    elif any(pattern.casefold() in lowered for pattern in policy.fatal_patterns):
        event = GuardianEventType.FATAL
    elif status_code is not None and (
        status_code >= 500 or status_code in policy.gateway_status_codes
    ):
        event = GuardianEventType.GATEWAY_ERROR
    elif "timeout" in lowered or "deadline exceeded" in lowered:
        event = GuardianEventType.PROBE_FAIL
    else:
        event = GuardianEventType.UPSTREAM_UNKNOWN
    return ClassifiedSample(
        event_type=event,
        score=policy.event_scores[event],
        safe_message=safe,
    )


def classify_monitor_status(
    *,
    status: str,
    latency_ms: int | None,
    available_count: int | None,
    policy: ScoringPolicy,
) -> GuardianEventType:
    """Classify one aggregate monitor without confusing slow timeout with pool outage."""
    if status == "operational":
        return (
            GuardianEventType.SLOW_TTFB
            if latency_ms is not None and latency_ms > policy.slow_ttfb_ms
            else GuardianEventType.PERFECT
        )
    if status == "degraded":
        return GuardianEventType.SLOW_TTFB
    if status in {"failed", "error"}:
        if (
            latency_ms is not None
            and latency_ms >= policy.slow_ttfb_ms
            and available_count is not None
            and available_count > 0
        ):
            return GuardianEventType.SLOW_TTFB
        return GuardianEventType.PROBE_FAIL
    return GuardianEventType.UPSTREAM_UNKNOWN
