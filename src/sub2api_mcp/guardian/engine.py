"""One bounded Guardian synchronization and dry-run evaluation cycle."""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from ..contracts import ProbeResult
from .contracts import (
    ChannelDecisionInput,
    GuardianEventType,
    GuardianHealth,
    GuardianPolicy,
    GuardianSample,
    GuardianSampleSource,
    ManualControl,
    UpstreamProbeEntry,
    UpstreamProbeSnapshot,
    WeightCandidate,
)
from .repository import GuardianRepository
from .scoring import calculate_health_score
from .state_machine import decide_channel_state
from .weights import allocate_weights


class GuardianOperations(Protocol):
    async def probe(self) -> ProbeResult: ...


def _event_for(entry: UpstreamProbeEntry, policy: GuardianPolicy) -> GuardianEventType:
    if entry.status == "operational":
        if entry.latency_ms is not None and entry.latency_ms > policy.scoring.slow_ttfb_ms:
            return GuardianEventType.SLOW_TTFB
        return GuardianEventType.PERFECT
    if entry.status == "degraded":
        return GuardianEventType.SLOW_TTFB
    if entry.status in {"failed", "error"}:
        return GuardianEventType.PROBE_FAIL
    return GuardianEventType.UPSTREAM_UNKNOWN


class GuardianEngine:
    def __init__(self, repository: GuardianRepository, operations: GuardianOperations) -> None:
        self.repository = repository
        self._operations = operations

    async def run_once(
        self,
        *,
        dry_run: bool = True,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        run = await self.repository.create_run(dry_run=dry_run, idempotency_key=idempotency_key)
        if not cast(bool, run.pop("created")):
            return run
        run_id = cast(str, run["run_id"])
        owner = f"run:{run_id}:{uuid.uuid4()}"
        leased = False
        try:
            policy = await self.repository.get_policy()
            leased = await self.repository.acquire_lease(
                "engine", owner, seconds=max(policy.scan_interval_seconds * 2, 30)
            )
            if not leased:
                finished = await self.repository.finish_run(
                    run_id,
                    status="FAILED",
                    error_code="GUARDIAN_BUSY",
                    error_message="Another Guardian cycle currently owns the lease",
                )
                return finished
            probe = await self._operations.probe()
            snapshot = UpstreamProbeSnapshot.model_validate(probe.snapshot)
            result = await self._evaluate(snapshot, policy, requested_dry_run=dry_run)
            return await self.repository.finish_run(run_id, status="SUCCEEDED", result=result)
        except Exception:
            await self.repository.finish_run(
                run_id,
                status="FAILED",
                error_code="GUARDIAN_RUN_FAILED",
                error_message="Guardian could not complete this cycle",
            )
            raise
        finally:
            if leased:
                await self.repository.release_lease("engine", owner)

    async def _evaluate(
        self,
        snapshot: UpstreamProbeSnapshot,
        policy: GuardianPolicy,
        *,
        requested_dry_run: bool,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        evaluated: list[tuple[UpstreamProbeEntry, float, GuardianHealth, str]] = []
        fuse_count = 0
        transitions = 0
        expected_changes = 0
        for entry in snapshot.entries:
            existing = await self.repository.get_channel(entry.monitor_id)
            manual_control = ManualControl(
                existing["manual_control"] if existing else ManualControl.NONE.value
            )
            event_type = _event_for(entry, policy)
            sample = GuardianSample(
                channel_id=entry.monitor_id,
                event_type=event_type,
                score=policy.scoring.event_scores[event_type],
                occurred_at=now,
                source=GuardianSampleSource.PROBE,
                ttfb_ms=entry.latency_ms,
                message=entry.status,
            )
            await self.repository.append_sample(sample)
            samples = await self.repository.list_samples(
                entry.monitor_id, limit=policy.scoring.long_window
            )
            score = calculate_health_score(
                samples,
                short_window=policy.scoring.short_window,
                long_window=policy.scoring.long_window,
                latest_weight=policy.scoring.latest_weight,
                short_ratio=policy.scoring.short_ratio,
                decay=policy.scoring.decay,
            )
            recent_events = tuple(item.event_type for item in samples)
            recent_ttfb = tuple(item.ttfb_ms for item in samples if item.ttfb_ms is not None)
            current_health = GuardianHealth(
                existing["health"] if existing else GuardianHealth.PENDING.value
            )
            success_streak = 0
            for recent in recent_events:
                if recent is not GuardianEventType.PERFECT:
                    break
                success_streak += 1
            decision = decide_channel_state(
                ChannelDecisionInput(
                    channel_id=entry.monitor_id,
                    score=score.final_score,
                    recent_events=recent_events,
                    recent_ttfb_ms=recent_ttfb,
                    current_health=current_health,
                    manual_control=manual_control,
                    schedulable=entry.upstream_schedulable,
                    group_available_count=entry.available_count or 1,
                    success_streak=success_streak,
                    now=now,
                    breaker=policy.breaker,
                    degrade=policy.degrade,
                    recovery=policy.recovery,
                )
            )
            if decision.health is GuardianHealth.FUSED:
                if fuse_count >= policy.breaker.max_switch_per_round:
                    decision = decision.model_copy(
                        update={
                            "health": GuardianHealth.DEGRADED,
                            "should_schedule": True,
                            "reason": "round_fuse_limit",
                        }
                    )
                else:
                    fuse_count += 1
            if current_health is not decision.health:
                transitions += 1
                await self.repository.add_event(
                    event_type=f"CHANNEL_{decision.health.value}",
                    severity=(
                        "WARNING"
                        if decision.health
                        in {
                            GuardianHealth.FUSED,
                            GuardianHealth.FORCED_KEEP,
                            GuardianHealth.DEGRADED,
                        }
                        else "INFO"
                    ),
                    channel_id=entry.monitor_id,
                    group_id=entry.group_id,
                    message=f"{entry.name}: {decision.reason}",
                    details={
                        "from": current_health.value,
                        "to": decision.health.value,
                        "score": score.final_score,
                        "reason": decision.reason,
                    },
                )
            expected_action = (
                "NO_CHANGE"
                if entry.upstream_schedulable == decision.should_schedule
                else ("ENABLE" if decision.should_schedule else "DISABLE")
            )
            if expected_action != "NO_CHANGE":
                expected_changes += 1
            await self.repository.upsert_channel(
                channel_id=entry.monitor_id,
                name=entry.name,
                group_id=entry.group_id,
                upstream_status=entry.status,
                upstream_schedulable=entry.upstream_schedulable,
                health=decision.health,
                score=score.final_score,
                latency_ms=entry.latency_ms,
                desired_schedulable=decision.should_schedule,
                manual_control=manual_control,
                details={
                    "reason": decision.reason,
                    "short_score": score.short_score,
                    "long_score": score.long_score,
                    "sample_count": score.sample_count,
                    "event_type": event_type.value,
                    "available_count": entry.available_count,
                    "error_count": entry.error_count,
                    "temporary_unavailable_count": entry.temporary_unavailable_count,
                    "closed_count": entry.closed_count,
                    "expected_action": expected_action,
                },
                seen_at=now,
            )
            evaluated.append((entry, score.final_score, decision.health, decision.reason))

        group_candidates: dict[str, list[WeightCandidate]] = defaultdict(list)
        for entry, score, health, _ in evaluated:
            if health not in {
                GuardianHealth.FUSED,
                GuardianHealth.EXCLUDED,
                GuardianHealth.MANUALLY_PAUSED,
            }:
                group_candidates[entry.group_id or "ungrouped"].append(
                    WeightCandidate(
                        channel_id=entry.monitor_id,
                        score=score,
                        effective_rate=entry.effective_rate,
                        ttfb_p95_ms=entry.latency_ms,
                    )
                )
        weights = {
            group_id: allocate_weights(candidates, strategy=policy.strategy, policy=policy.weights)
            for group_id, candidates in group_candidates.items()
        }
        return {
            "observe_only": policy.observe_only,
            "requested_dry_run": requested_dry_run,
            "channels_evaluated": len(evaluated),
            "state_transitions": transitions,
            "expected_changes": expected_changes,
            "writes_applied": 0,
            "strategy": policy.strategy.value,
            "weight_candidates": weights,
            "writeback_blocked_reason": (
                "observe_only" if policy.observe_only else "writeback_adapter_not_enabled"
            ),
        }
