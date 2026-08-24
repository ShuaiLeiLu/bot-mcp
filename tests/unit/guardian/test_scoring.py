from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from sub2api_mcp.guardian.contracts import (
    GuardianEventType,
    GuardianEvidenceBucket,
    GuardianFreshness,
    GuardianSample,
    GuardianSampleSource,
    SamplingPolicy,
    ScoringPolicy,
)
from sub2api_mcp.guardian.scoring import (
    calculate_health_score,
    calculate_health_score_v2,
    calculate_short_score,
)


def _sample(score: int, index: int) -> GuardianSample:
    occurred_at = datetime(2026, 8, 23, 10, 59, tzinfo=UTC) - timedelta(minutes=index)
    return GuardianSample(
        channel_id="995",
        event_type=GuardianEventType.PERFECT,
        score=score,
        occurred_at=occurred_at,
        source=GuardianSampleSource.PROBE,
    )


def test_short_score_matches_reference_geometric_decay_exactly() -> None:
    values = [65, 40, 65, 10, 100, 15, 100, 15, 15, 100]
    samples = [_sample(value, index) for index, value in enumerate(values)]

    score = calculate_short_score(samples, latest_weight=0.5, decay=0.5)

    assert score == pytest.approx(55.62133072407043, abs=1e-12)


def test_final_score_combines_short_and_long_windows() -> None:
    values = [65, 40, 65, 10, 100, 15, 100, 15, 15, 100]
    samples = [_sample(value, index) for index, value in enumerate(values)]

    result = calculate_health_score(
        samples,
        short_window=10,
        long_window=60,
        latest_weight=0.5,
        short_ratio=0.7,
        decay=0.5,
    )

    expected = 55.62133072407043 * 0.7 + (sum(values) / len(values)) * 0.3
    assert result.final_score == pytest.approx(expected, abs=1e-12)
    assert result.sample_count == 10


def test_empty_samples_have_zero_score_without_nan() -> None:
    result = calculate_health_score([])

    assert result.final_score == 0
    assert result.short_score == 0
    assert result.long_score == 0


def _bucket(
    score: float,
    quality: float,
    age_minutes: int,
    *,
    now: datetime,
) -> GuardianEvidenceBucket:
    return GuardianEvidenceBucket(
        channel_id="channel-1",
        bucket_at=now - timedelta(minutes=age_minutes),
        score=score,
        quality=quality,
        sources=frozenset({GuardianSampleSource.SHARED_MONITOR}),
        event_count=1,
    )


def test_v2_score_matches_the_published_golden_example() -> None:
    now = datetime(2026, 8, 24, 2, 2, tzinfo=UTC)
    result = calculate_health_score_v2(
        [
            _bucket(100, 0.85, 0, now=now),
            _bucket(65, 1.0, 1, now=now),
            _bucket(25, 0.85, 2, now=now),
        ],
        now=now,
    )

    assert result.short_score == pytest.approx(68.82317750686934, abs=1e-12)
    assert result.long_score == pytest.approx(63.971259697525845, abs=1e-12)
    assert result.health_score == pytest.approx(67.36760216406628, abs=1e-12)
    assert result.confidence == pytest.approx(0.7196488001243996, abs=1e-12)
    assert result.freshness is GuardianFreshness.FRESH
    assert result.warming_up is True


def test_v2_no_evidence_preserves_previous_score_but_removes_confidence() -> None:
    result = calculate_health_score_v2(
        [],
        now=datetime(2026, 8, 24, 2, 2, tzinfo=UTC),
        previous_score=91.5,
    )

    assert result.short_score == 91.5
    assert result.long_score == 91.5
    assert result.health_score == 91.5
    assert result.confidence == 0
    assert result.freshness is GuardianFreshness.EXPIRED
    assert result.evidence_bucket_count == 0
    assert result.warming_up is True


@pytest.mark.parametrize(
    ("age_seconds", "expected"),
    [
        (180, GuardianFreshness.FRESH),
        (181, GuardianFreshness.STALE),
        (600, GuardianFreshness.STALE),
        (601, GuardianFreshness.EXPIRED),
    ],
)
def test_v2_freshness_boundaries_are_deterministic(
    age_seconds: int,
    expected: GuardianFreshness,
) -> None:
    now = datetime(2026, 8, 24, 2, 2, tzinfo=UTC)
    bucket = GuardianEvidenceBucket(
        channel_id="channel-1",
        bucket_at=now - timedelta(seconds=age_seconds),
        score=100,
        quality=0.85,
        sources=frozenset({GuardianSampleSource.SHARED_MONITOR}),
        event_count=1,
    )

    result = calculate_health_score_v2(
        [bucket],
        now=now,
        scoring=ScoringPolicy(),
        sampling=SamplingPolicy(),
    )

    assert result.freshness is expected
