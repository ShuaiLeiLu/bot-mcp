"""Pluggable price, speed, and balanced weight allocation."""

from __future__ import annotations

import math
from collections.abc import Sequence

from .contracts import GuardianStrategy, WeightCandidate, WeightsPolicy


def _inverse(value: float | int | None, exponent: float) -> float:
    if value is None or value <= 0:
        return 1.0
    return 1 / math.pow(float(value), exponent)


def allocate_weights(
    candidates: Sequence[WeightCandidate],
    *,
    strategy: GuardianStrategy,
    policy: WeightsPolicy,
) -> dict[str, float]:
    if not candidates:
        return {}
    raw: dict[str, float] = {}
    for candidate in candidates:
        if candidate.score < policy.gate_floor:
            raw[candidate.channel_id] = 0.0
            continue
        price_signal = _inverse(candidate.effective_rate, policy.price_exp)
        speed_signal = _inverse(candidate.ttfb_p95_ms, policy.speed_exp)
        if strategy is GuardianStrategy.PRICE:
            strategy_signal = price_signal
        elif strategy is GuardianStrategy.SPEED:
            strategy_signal = speed_signal
        else:
            strategy_signal = price_signal * policy.balanced_price_ratio + speed_signal * (
                1 - policy.balanced_price_ratio
            )
        health_signal = max(candidate.score, 0.01) / 100
        raw[candidate.channel_id] = strategy_signal * health_signal * candidate.schedule_multiplier
    total = sum(raw.values())
    if total <= 0:
        return {candidate.channel_id: 0.0 for candidate in candidates}
    return {channel_id: value / total * policy.budget for channel_id, value in raw.items()}
