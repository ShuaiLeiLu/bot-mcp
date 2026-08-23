from __future__ import annotations

import pytest
from pydantic import ValidationError

from sub2api_mcp.guardian.contracts import GuardianPolicy


def test_policy_defaults_to_observe_only_and_safe_limits() -> None:
    policy = GuardianPolicy()

    assert policy.observe_only is True
    assert policy.auto_apply.schedulable is False
    assert policy.auto_apply.priority is False
    assert policy.auto_apply.load_factor is False
    assert policy.breaker.max_switch_per_round == 1
    assert policy.breaker.min_pool_size == 1
    assert policy.probe.concurrency == 4


def test_policy_rejects_self_contradictory_windows_and_load_bounds() -> None:
    with pytest.raises(ValidationError):
        GuardianPolicy.model_validate({"breaker": {"http_window": 3, "http_failures": 4}})
    with pytest.raises(ValidationError):
        GuardianPolicy.model_validate({"weights": {"min_load_factor": 10, "max_load_factor": 5}})
