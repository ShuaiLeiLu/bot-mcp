from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from sub2api_mcp.guardian.contracts import (
    GuardianEventType,
    GuardianEvidence,
    GuardianEvidenceBucket,
    GuardianFieldName,
    GuardianFieldOwner,
    GuardianFieldOwnership,
    GuardianFreshness,
    GuardianPolicy,
    GuardianRolloutStage,
    GuardianSampleSource,
    GuardianScoreV2,
    SamplingMode,
)


def test_policy_defaults_to_observe_only_and_safe_limits() -> None:
    policy = GuardianPolicy()

    assert policy.observe_only is True
    assert policy.auto_apply.schedulable is False
    assert policy.auto_apply.priority is False
    assert policy.auto_apply.load_factor is False
    assert policy.breaker.max_switch_per_round == 1
    assert policy.breaker.min_pool_size == 1
    assert policy.probe.concurrency == 4
    assert policy.probe.enabled is False
    assert policy.sampling.mode is SamplingMode.SHARED
    assert policy.sampling.shared_snapshot_interval_seconds == 60
    assert policy.sampling.fresh_seconds == 180
    assert policy.sampling.expire_seconds == 600
    assert policy.confidence.degrade_min == 0.60
    assert policy.confidence.weight_min == 0.75
    assert policy.confidence.fuse_min == 0.85
    assert policy.rollout.stage is GuardianRolloutStage.OBSERVE
    assert policy.recovery_budget.enabled is False


def test_policy_rejects_self_contradictory_windows_and_load_bounds() -> None:
    with pytest.raises(ValidationError):
        GuardianPolicy.model_validate({"breaker": {"http_window": 3, "http_failures": 4}})
    with pytest.raises(ValidationError):
        GuardianPolicy.model_validate({"weights": {"min_load_factor": 10, "max_load_factor": 5}})
    with pytest.raises(ValidationError):
        GuardianPolicy.model_validate(
            {"sampling": {"fresh_seconds": 600, "expire_seconds": 180}}
        )
    with pytest.raises(ValidationError):
        GuardianPolicy.model_validate(
            {"confidence": {"degrade_min": 0.8, "weight_min": 0.7}}
        )


def test_v1_policy_payload_loads_with_safe_v2_defaults() -> None:
    policy = GuardianPolicy.model_validate(
        {
            "revision": 3,
            "enabled": True,
            "observe_only": True,
            "scan_interval_seconds": 30,
            "probe": {"enabled": True, "interval_seconds": 60},
        }
    )

    assert policy.revision == 3
    assert policy.enabled is True
    assert policy.observe_only is True
    assert policy.probe.enabled is True
    assert policy.sampling.mode is SamplingMode.SHARED
    assert policy.rollout.stage is GuardianRolloutStage.OBSERVE
    assert policy.auto_apply.load_factor is False
    assert policy.auto_apply.priority is False
    assert policy.auto_apply.schedulable is False


def test_v2_evidence_and_field_ownership_contracts_are_strict() -> None:
    observed_at = datetime(2026, 8, 24, 2, 0, tzinfo=UTC)
    evidence = GuardianEvidence(
        source_event_id="snapshot-1:channel-7",
        channel_id="7",
        source=GuardianSampleSource.SHARED_MONITOR,
        event_type=GuardianEventType.PERFECT,
        score=100,
        occurred_at=observed_at,
        reliability=0.85,
        event_count=1,
        ttfb_ms=1200,
    )
    ownership = GuardianFieldOwnership(
        channel_id="7",
        field_name=GuardianFieldName.LOAD_FACTOR,
        owner=GuardianFieldOwner.UPSTREAM,
        baseline_value=100,
        last_guardian_value=None,
        last_write_at=None,
    )
    bucket = GuardianEvidenceBucket(
        channel_id="7",
        bucket_at=observed_at,
        score=100,
        quality=0.85,
        sources=frozenset({GuardianSampleSource.SHARED_MONITOR}),
        event_count=1,
    )
    score = GuardianScoreV2(
        short_score=100,
        long_score=100,
        health_score=100,
        confidence=0.46,
        freshness=GuardianFreshness.FRESH,
        evidence_bucket_count=1,
        last_evidence_at=observed_at,
    )

    assert evidence.source is GuardianSampleSource.SHARED_MONITOR
    assert evidence.reliability == 0.85
    assert ownership.owner is GuardianFieldOwner.UPSTREAM
    assert bucket.sources == frozenset({GuardianSampleSource.SHARED_MONITOR})
    assert score.health_score == 100
    assert GuardianFreshness.FRESH.value == "FRESH"

    with pytest.raises(ValidationError):
        GuardianEvidence.model_validate(
            {
                **evidence.model_dump(mode="json"),
                "reliability": 1.1,
            }
        )
    with pytest.raises(ValidationError):
        GuardianFieldOwnership.model_validate(
            {
                **ownership.model_dump(mode="json"),
                "unexpected": True,
            }
        )
    with pytest.raises(ValidationError):
        GuardianScoreV2(
            short_score=0,
            long_score=0,
            health_score=0,
            confidence=0,
            freshness=GuardianFreshness.EXPIRED,
            evidence_bucket_count=1,
            last_evidence_at=None,
        )
