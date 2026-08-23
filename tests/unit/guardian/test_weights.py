from __future__ import annotations

import pytest

from sub2api_mcp.guardian.contracts import GuardianStrategy, WeightCandidate, WeightsPolicy
from sub2api_mcp.guardian.weights import allocate_weights


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
