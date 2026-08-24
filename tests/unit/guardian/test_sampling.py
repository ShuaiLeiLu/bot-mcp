from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from sub2api_mcp.guardian.contracts import (
    GuardianEventType,
    GuardianTrafficObservation,
)
from sub2api_mcp.guardian.sampling import build_traffic_buckets


def _request_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _observation(
    request_id: str,
    *,
    channel_id: str | None = "channel-1",
    minute: int = 0,
    score: int = 100,
    event_type: GuardianEventType = GuardianEventType.PERFECT,
    ttfb_ms: int | None = 1000,
    monitor: bool = False,
) -> GuardianTrafficObservation:
    return GuardianTrafficObservation(
        request_id_hash=_request_hash(request_id),
        channel_id=channel_id,
        occurred_at=datetime(2026, 8, 24, 2, minute, 15, tzinfo=UTC),
        event_type=event_type,
        score=score,
        ttfb_ms=ttfb_ms,
        status_code=200 if score == 100 else 502,
        is_monitor_request=monitor,
    )


def test_builds_deterministic_minute_buckets_and_reports_filtered_evidence() -> None:
    first = _observation("request-1")
    result = build_traffic_buckets(
        [
            first,
            first,
            _observation("monitor", monitor=True),
            _observation("unattributed", channel_id=None),
            _observation(
                "request-2",
                minute=1,
                score=25,
                event_type=GuardianEventType.GATEWAY_ERROR,
                ttfb_ms=3000,
            ),
        ]
    )

    assert len(result.buckets) == 2
    assert result.duplicate_count == 1
    assert result.excluded_monitor_count == 1
    assert result.unattributed_count == 1
    assert result.buckets[0].bucket_at == datetime(2026, 8, 24, 2, 0, tzinfo=UTC)
    assert result.buckets[0].score == 100
    assert result.buckets[1].score == 25


def test_high_volume_is_reduced_to_one_bucket_with_capped_quality() -> None:
    observations = [
        _observation(
            f"request-{index}",
            ttfb_ms=index * 100,
        )
        for index in range(1, 101)
    ]

    result = build_traffic_buckets(observations)
    bucket = result.buckets[0]

    assert len(result.buckets) == 1
    assert bucket.event_count == 100
    assert bucket.score == 100
    assert bucket.quality == 1
    assert bucket.ttfb_p95_ms == 9500


def test_conflicting_duplicate_request_hash_is_rejected() -> None:
    original = _observation("same", score=100)
    conflicting = original.model_copy(
        update={
            "occurred_at": original.occurred_at + timedelta(seconds=1),
            "score": 25,
            "event_type": GuardianEventType.GATEWAY_ERROR,
        }
    )

    with pytest.raises(ValueError, match="changed"):
        build_traffic_buckets([original, conflicting])
