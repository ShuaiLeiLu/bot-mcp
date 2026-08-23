from __future__ import annotations

from sub2api_mcp.guardian.classifier import classify_sample
from sub2api_mcp.guardian.contracts import GuardianEventType, ScoringPolicy


def test_classifier_distinguishes_fatal_quota_gateway_and_slow_events() -> None:
    policy = ScoringPolicy()

    assert (
        classify_sample(
            status_code=401, message="", ttfb_ms=None, succeeded=False, policy=policy
        ).event_type
        is GuardianEventType.FATAL
    )
    assert (
        classify_sample(
            status_code=429, message="rate limit", ttfb_ms=None, succeeded=False, policy=policy
        ).event_type
        is GuardianEventType.QUOTA_EXHAUSTED
    )
    assert (
        classify_sample(
            status_code=503,
            message="gateway unavailable",
            ttfb_ms=None,
            succeeded=False,
            policy=policy,
        ).event_type
        is GuardianEventType.GATEWAY_ERROR
    )
    assert (
        classify_sample(
            status_code=200, message="ok", ttfb_ms=5001, succeeded=True, policy=policy
        ).event_type
        is GuardianEventType.SLOW_TTFB
    )
    assert (
        classify_sample(
            status_code=200, message="ok", ttfb_ms=100, succeeded=True, policy=policy
        ).event_type
        is GuardianEventType.PERFECT
    )


def test_classifier_uses_fatal_patterns_but_not_unknown_timeouts() -> None:
    policy = ScoringPolicy()

    fatal = classify_sample(
        status_code=None,
        message="Invalid API key",
        ttfb_ms=None,
        succeeded=False,
        policy=policy,
    )
    timeout = classify_sample(
        status_code=None,
        message="context deadline exceeded",
        ttfb_ms=None,
        succeeded=False,
        policy=policy,
    )

    assert fatal.event_type is GuardianEventType.FATAL
    assert timeout.event_type is GuardianEventType.PROBE_FAIL


def test_success_text_cannot_be_misclassified_by_error_keywords() -> None:
    result = classify_sample(
        status_code=200,
        message="load balance completed",
        ttfb_ms=100,
        succeeded=True,
        policy=ScoringPolicy(),
    )

    assert result.event_type is GuardianEventType.PERFECT
