"""Pluggable price, speed, and balanced weight allocation."""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence

from .contracts import (
    GuardianFreshness,
    GuardianStrategy,
    WeightAllocation,
    WeightCandidate,
    WeightsPolicy,
    WritePolicy,
)


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


def _integer_budget(
    raw: dict[str, float],
    *,
    budget: int,
    minimum: int,
    maximum: int,
) -> tuple[dict[str, int], int]:
    positive = {key: value for key, value in raw.items() if value > 0}
    targets = {key: 0 for key in raw}
    if not positive or budget <= 0:
        return targets, max(0, budget)
    total = sum(positive.values())
    floats = {
        key: min(maximum, max(minimum, budget * value / total))
        for key, value in positive.items()
    }
    targets.update({key: int(math.floor(value)) for key, value in floats.items()})
    allocated = sum(targets.values())
    while allocated < budget:
        candidates = [key for key in positive if targets[key] < maximum]
        if not candidates:
            break
        candidates.sort(
            key=lambda key: (floats[key] - math.floor(floats[key]), positive[key], key),
            reverse=True,
        )
        for key in candidates:
            if allocated >= budget:
                break
            targets[key] += 1
            allocated += 1
    while allocated > budget:
        candidates = [key for key in positive if targets[key] > minimum]
        if not candidates:
            break
        candidates.sort(key=lambda key: (positive[key], key))
        for key in candidates:
            if allocated <= budget:
                break
            targets[key] -= 1
            allocated -= 1
    return targets, max(0, budget - allocated)


def allocate_weights_v2(
    candidates: Sequence[WeightCandidate],
    *,
    strategy: GuardianStrategy,
    policy: WeightsPolicy,
    confidence_floor: float,
) -> WeightAllocation:
    frozen = [candidate for candidate in candidates if candidate.confidence < confidence_floor]
    reserved_budget = sum(candidate.current_load_factor for candidate in frozen)
    if reserved_budget > policy.budget:
        return WeightAllocation(
            target_load_factors={
                candidate.channel_id: candidate.current_load_factor for candidate in candidates
            },
            reserved_budget=reserved_budget,
            unallocated_budget=0,
            blocked_reason="reserved_budget_exceeds_group_budget",
        )
    eligible = [candidate for candidate in candidates if candidate not in frozen]
    valid_rates = [
        float(candidate.effective_rate)
        for candidate in eligible
        if candidate.effective_rate is not None and candidate.effective_rate > 0
    ]
    valid_speeds = [
        float(candidate.ttfb_p95_ms)
        for candidate in eligible
        if candidate.ttfb_p95_ms is not None and candidate.ttfb_p95_ms > 0
    ]
    median_rate = statistics.median(valid_rates) if valid_rates else 1.0
    median_speed = statistics.median(valid_speeds) if valid_speeds else 1000.0
    min_rate = min(valid_rates) if valid_rates else median_rate
    min_speed = min(valid_speeds) if valid_speeds else median_speed
    raw: dict[str, float] = {}
    for candidate in eligible:
        if candidate.score < policy.gate_floor:
            raw[candidate.channel_id] = 0
            continue
        rate = float(candidate.effective_rate or median_rate)
        speed = float(candidate.ttfb_p95_ms or median_speed)
        price_signal = math.pow(min_rate / max(rate, 1e-12), policy.price_exp)
        speed_signal = math.pow(min_speed / max(speed, 100), policy.speed_exp)
        if candidate.effective_rate is None:
            price_signal *= 0.8
        if candidate.ttfb_p95_ms is None:
            speed_signal *= 0.8
        if strategy is GuardianStrategy.PRICE:
            strategy_signal = price_signal
        elif strategy is GuardianStrategy.SPEED:
            strategy_signal = speed_signal
        else:
            strategy_signal = math.pow(
                price_signal, policy.balanced_price_ratio
            ) * math.pow(speed_signal, 1 - policy.balanced_price_ratio)
        health_signal = max(
            0.0,
            (candidate.score - policy.gate_floor) / max(1e-12, 100 - policy.gate_floor),
        )
        raw[candidate.channel_id] = (
            strategy_signal
            * health_signal
            * math.pow(candidate.confidence, policy.confidence_exp)
            * candidate.schedule_multiplier
        )
    allocatable = max(0, int(policy.budget) - reserved_budget)
    targets, unallocated = _integer_budget(
        raw,
        budget=allocatable,
        minimum=policy.min_load_factor,
        maximum=policy.max_load_factor,
    )
    for candidate in frozen:
        targets[candidate.channel_id] = candidate.current_load_factor
    return WeightAllocation(
        target_load_factors=targets,
        reserved_budget=reserved_budget,
        unallocated_budget=unallocated,
    )


def bounded_load_factor(current: int, target: int, *, policy: WritePolicy) -> int:
    difference = target - current
    if difference == 0:
        return current
    relative = abs(difference) / max(1, current)
    if (
        abs(difference) < policy.min_absolute_change
        or relative < policy.min_relative_change
    ):
        return current
    max_step = max(1, round(max(1, current) * policy.max_relative_step))
    return current + max(-max_step, min(max_step, difference))


def recommend_priority(
    *,
    baseline: int,
    score: float,
    confidence: float,
    freshness: GuardianFreshness,
    forced_keep: bool = False,
) -> int:
    if forced_keep:
        return 5
    if confidence < 0.75 or freshness is not GuardianFreshness.FRESH:
        return max(1, min(5, baseline))
    if score >= 75:
        offset = 0
    elif score >= 60:
        offset = 1
    else:
        offset = 2
    return max(1, min(5, baseline + offset))
