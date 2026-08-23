"""Exact Guardian short/long health score calculation."""

from __future__ import annotations

from collections.abc import Sequence

from .contracts import GuardianSample, GuardianScore


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
