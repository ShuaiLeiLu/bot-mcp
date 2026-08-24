from __future__ import annotations

import pytest

from sub2api_mcp.guardian.contracts import (
    GuardianFreshness,
    GuardianStrategy,
    WeightCandidate,
    WeightsPolicy,
    WritePolicy,
)
from sub2api_mcp.guardian.weights import (
    allocate_weights,
    allocate_weights_v2,
    bounded_load_factor,
    recommend_priority,
)


def _candidate(
    channel_id: str,
    *,
    score: float = 100,
    rate: float | None = 1,
    ttfb: int | None = 1000,
) -> WeightCandidate:
    return WeightCandidate(
        channel_id=channel_id,
        score=score,
        effective_rate=rate,
        ttfb_p95_ms=ttfb,
    )


def test_price_strategy_favors_lower_cost_and_respects_budget() -> None:
    result = allocate_weights(
        [_candidate("cheap", rate=0.1), _candidate("costly", rate=0.2)],
        strategy=GuardianStrategy.PRICE,
        policy=WeightsPolicy(budget=400),
    )

    assert result["cheap"] > result["costly"]
    assert sum(result.values()) == pytest.approx(400)


def test_speed_strategy_favors_lower_ttfb() -> None:
    result = allocate_weights(
        [_candidate("fast", ttfb=500), _candidate("slow", ttfb=2000)],
        strategy=GuardianStrategy.SPEED,
        policy=WeightsPolicy(budget=400),
    )

    assert result["fast"] > result["slow"]


def test_gate_floor_zeroes_unhealthy_candidate() -> None:
    result = allocate_weights(
        [_candidate("healthy", score=90), _candidate("bad", score=39)],
        strategy=GuardianStrategy.BALANCED,
        policy=WeightsPolicy(budget=400, gate_floor=40),
    )

    assert result["bad"] == 0
    assert result["healthy"] == pytest.approx(400)


def test_missing_price_and_speed_degrade_safely_without_nan() -> None:
    result = allocate_weights(
        [_candidate("unknown", rate=None, ttfb=None)],
        strategy=GuardianStrategy.BALANCED,
        policy=WeightsPolicy(budget=400),
    )

    assert result == {"unknown": 400.0}


def test_v2_reserves_frozen_budget_and_conserves_allocatable_budget() -> None:
    candidates = [
        _candidate("frozen").model_copy(
            update={"confidence": 0.5, "current_load_factor": 50}
        ),
        *[
            _candidate(f"channel-{index}", rate=1 + index / 10).model_copy(
                update={"confidence": 1, "current_load_factor": 80}
            )
            for index in range(4)
        ],
    ]

    result = allocate_weights_v2(
        candidates,
        strategy=GuardianStrategy.PRICE,
        policy=WeightsPolicy(budget=400, min_load_factor=1, max_load_factor=100),
        confidence_floor=0.75,
    )

    assert result.target_load_factors["frozen"] == 50
    assert sum(result.target_load_factors.values()) + result.unallocated_budget == 400
    assert result.reserved_budget == 50
    assert result.blocked_reason is None


def test_v2_missing_signals_receive_a_penalty_instead_of_an_advantage() -> None:
    result = allocate_weights_v2(
        [
            _candidate("known-a", rate=1, ttfb=1000),
            _candidate("known-b", rate=2, ttfb=2000),
            _candidate("missing", rate=None, ttfb=None),
        ],
        strategy=GuardianStrategy.BALANCED,
        policy=WeightsPolicy(budget=300, min_load_factor=1, max_load_factor=300),
        confidence_floor=0.75,
    )

    assert result.target_load_factors["missing"] < result.target_load_factors["known-a"]


def test_load_factor_step_and_priority_tiers_are_stable() -> None:
    writes = WritePolicy(max_relative_step=0.2, min_relative_change=0.15)

    assert bounded_load_factor(100, 200, policy=writes) == 120
    assert bounded_load_factor(100, 110, policy=writes) == 100
    assert recommend_priority(
        baseline=2,
        score=90,
        confidence=0.8,
        freshness=GuardianFreshness.FRESH,
    ) == 2
    assert recommend_priority(
        baseline=2,
        score=55,
        confidence=0.8,
        freshness=GuardianFreshness.FRESH,
    ) == 4
    assert recommend_priority(
        baseline=2,
        score=10,
        confidence=0.2,
        freshness=GuardianFreshness.STALE,
    ) == 2
    assert recommend_priority(
        baseline=2,
        score=100,
        confidence=1,
        freshness=GuardianFreshness.FRESH,
        forced_keep=True,
    ) == 5
