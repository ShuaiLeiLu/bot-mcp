"""Exact Guardian short/long health score calculation."""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime

from .contracts import (
    GuardianEvidenceBucket,
    GuardianFreshness,
    GuardianSample,
    GuardianScore,
    GuardianScoreV2,
    SamplingPolicy,
    ScoringPolicy,
)


def calculate_short_score(
    samples: Sequence[GuardianSample],
    *,
    latest_weight: float = 0.5,
    decay: float = 0.5,
) -> float:
    if not samples:
        return 0.0
    if len(samples) == 1:
        return float(samples[0].score)
    remainder = samples[1:]
    raw_weights = [decay**index for index in range(len(remainder))]
    raw_total = sum(raw_weights)
    historical = sum(
        sample.score * raw_weight / raw_total
        for sample, raw_weight in zip(remainder, raw_weights, strict=True)
    )
    return samples[0].score * latest_weight + historical * (1 - latest_weight)


def calculate_health_score(
    samples: Sequence[GuardianSample],
    *,
    short_window: int = 10,
    long_window: int = 60,
    latest_weight: float = 0.5,
    short_ratio: float = 0.7,
    decay: float = 0.5,
) -> GuardianScore:
    if not samples:
        return GuardianScore(
            short_score=0,
            long_score=0,
            final_score=0,
            sample_count=0,
        )
    ordered = sorted(samples, key=lambda sample: sample.occurred_at, reverse=True)
    short_samples = ordered[:short_window]
    long_samples = ordered[:long_window]
    short_score = calculate_short_score(
        short_samples,
        latest_weight=latest_weight,
        decay=decay,
    )
    long_score = sum(sample.score for sample in long_samples) / len(long_samples)
    final_score = short_score * short_ratio + long_score * (1 - short_ratio)
    return GuardianScore(
        short_score=short_score,
        long_score=long_score,
        final_score=final_score,
        sample_count=len(long_samples),
    )


def _time_weighted_score(
    buckets: Sequence[GuardianEvidenceBucket],
    *,
    now: datetime,
    half_life_minutes: float,
) -> float:
    weighted: list[tuple[float, float]] = []
    for bucket in buckets:
        age_minutes = (now - bucket.bucket_at).total_seconds() / 60
        time_decay = math.pow(2, -age_minutes / half_life_minutes)
        weighted.append((bucket.score, bucket.quality * time_decay))
    total = sum(weight for _, weight in weighted)
    if total <= 0:
        return sum(bucket.score for bucket in buckets) / len(buckets)
    return sum(score * weight for score, weight in weighted) / total


def _bounded_score(value: float) -> float:
    return max(0.0, min(100.0, value))


def calculate_health_score_v2(
    buckets: Sequence[GuardianEvidenceBucket],
    *,
    now: datetime,
    previous_score: float = 0,
    scoring: ScoringPolicy | None = None,
    sampling: SamplingPolicy | None = None,
) -> GuardianScoreV2:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if not 0 <= previous_score <= 100:
        raise ValueError("previous_score must be between 0 and 100")
    scoring_policy = scoring or ScoringPolicy()
    sampling_policy = sampling or SamplingPolicy()
    if not buckets:
        return GuardianScoreV2(
            short_score=previous_score,
            long_score=previous_score,
            health_score=previous_score,
            confidence=0,
            freshness=GuardianFreshness.EXPIRED,
            evidence_bucket_count=0,
            last_evidence_at=None,
            warming_up=True,
        )

    channel_ids = {bucket.channel_id for bucket in buckets}
    if len(channel_ids) != 1:
        raise ValueError("all evidence buckets must belong to one channel")
    bucket_times = [bucket.bucket_at for bucket in buckets]
    if len(set(bucket_times)) != len(bucket_times):
        raise ValueError("duplicate evidence bucket time")
    if any(bucket_at > now for bucket_at in bucket_times):
        raise ValueError("evidence bucket cannot be in the future")

    long_buckets = [
        bucket
        for bucket in buckets
        if (now - bucket.bucket_at).total_seconds()
        <= scoring_policy.long_window_minutes * 60
    ]
    if not long_buckets:
        return GuardianScoreV2(
            short_score=previous_score,
            long_score=previous_score,
            health_score=previous_score,
            confidence=0,
            freshness=GuardianFreshness.EXPIRED,
            evidence_bucket_count=0,
            last_evidence_at=max(bucket_times),
            warming_up=True,
        )
    short_buckets = [
        bucket
        for bucket in long_buckets
        if (now - bucket.bucket_at).total_seconds()
        <= scoring_policy.short_window_minutes * 60
    ]
    long_score = _time_weighted_score(
        long_buckets,
        now=now,
        half_life_minutes=scoring_policy.long_half_life_minutes,
    )
    short_score = (
        _time_weighted_score(
            short_buckets,
            now=now,
            half_life_minutes=scoring_policy.short_half_life_minutes,
        )
        if short_buckets
        else long_score
    )
    health_score = (
        short_score * scoring_policy.short_ratio
        + long_score * (1 - scoring_policy.short_ratio)
    )

    latest_evidence_at = max(bucket_times)
    latest_age_seconds = (now - latest_evidence_at).total_seconds()
    if latest_age_seconds <= sampling_policy.fresh_seconds:
        freshness = GuardianFreshness.FRESH
    elif latest_age_seconds <= sampling_policy.expire_seconds:
        freshness = GuardianFreshness.STALE
    else:
        freshness = GuardianFreshness.EXPIRED

    coverage = min(1.0, len(short_buckets) / sampling_policy.min_warmup_buckets)
    quality_weights = [
        math.pow(
            2,
            -((now - bucket.bucket_at).total_seconds() / 60)
            / scoring_policy.short_half_life_minutes,
        )
        for bucket in short_buckets
    ]
    quality_total = sum(quality_weights)
    quality = (
        sum(
            bucket.quality * weight
            for bucket, weight in zip(short_buckets, quality_weights, strict=True)
        )
        / quality_total
        if quality_total > 0
        else 0
    )
    freshness_factor = math.pow(
        2,
        -latest_age_seconds / sampling_policy.fresh_seconds,
    )
    confidence = max(
        0.0,
        min(1.0, freshness_factor * (coverage * 0.6 + quality * 0.4)),
    )
    return GuardianScoreV2(
        short_score=_bounded_score(short_score),
        long_score=_bounded_score(long_score),
        health_score=_bounded_score(health_score),
        confidence=confidence,
        freshness=freshness,
        evidence_bucket_count=len(long_buckets),
        last_evidence_at=latest_evidence_at,
        warming_up=len(short_buckets) < sampling_policy.min_warmup_buckets,
    )
