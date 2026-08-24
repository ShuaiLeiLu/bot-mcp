"""Field-owned, idempotent Guardian writeback boundary with safe defaults."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from .contracts import (
    GuardianFieldName,
    GuardianFieldOwner,
    GuardianPolicy,
    GuardianRolloutStage,
    GuardianWriteDecision,
    GuardianWriteOutcome,
    GuardianWriteProposal,
)
from .ownership import observe_field_value
from .repository import GuardianRepository


class GuardianFieldWriter(Protocol):
    async def write_field(
        self,
        channel_id: str,
        field_name: GuardianFieldName,
        value: object,
    ) -> None: ...


_STAGE_RANK = {
    GuardianRolloutStage.OBSERVE: 0,
    GuardianRolloutStage.LOAD_FACTOR: 1,
    GuardianRolloutStage.PRIORITY: 2,
    GuardianRolloutStage.SCHEDULABLE: 3,
}
_FIELD_RANK = {
    GuardianFieldName.LOAD_FACTOR: 1,
    GuardianFieldName.PRIORITY: 2,
    GuardianFieldName.SCHEDULABLE: 3,
}


class GuardianWritebackService:
    def __init__(
        self,
        repository: GuardianRepository,
        writer: GuardianFieldWriter | None,
    ) -> None:
        self.repository = repository
        self._writer = writer

    async def apply(
        self,
        proposal: GuardianWriteProposal,
        *,
        policy: GuardianPolicy,
    ) -> GuardianWriteDecision:
        saved = await self.repository.get_idempotent_result(
            proposal.idempotency_key,
            "guardian_write_field",
            f"{proposal.channel_id}:{proposal.field_name.value}",
        )
        if saved is not None:
            return GuardianWriteDecision.model_validate(saved)

        ownership = observe_field_value(
            await self.repository.get_field_ownership(
                proposal.channel_id,
                proposal.field_name,
            ),
            current_value=proposal.current_value,
            channel_id=proposal.channel_id,
            field_name=proposal.field_name,
        )
        await self.repository.save_field_ownership(ownership)
        if ownership.owner is GuardianFieldOwner.HUMAN:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.BLOCKED,
                "human_field_takeover",
            )
        if proposal.current_value == proposal.desired_value:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.NO_CHANGE,
                "already_at_target",
            )
        if policy.observe_only:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.DRY_RUN,
                "observe_only",
            )
        if not self._field_is_enabled(proposal.field_name, policy):
            return await self._finish(
                proposal,
                GuardianWriteOutcome.BLOCKED,
                "field_write_not_approved",
            )
        if self._writer is None:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.BLOCKED,
                "writeback_adapter_disabled",
            )
        try:
            await self._writer.write_field(
                proposal.channel_id,
                proposal.field_name,
                proposal.desired_value,
            )
        except Exception:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.FAILED,
                "writeback_failed",
            )
        await self.repository.save_field_ownership(
            ownership.model_copy(
                update={
                    "owner": GuardianFieldOwner.GUARDIAN,
                    "last_guardian_value": proposal.desired_value,
                    "last_write_at": datetime.now(UTC),
                }
            )
        )
        return await self._finish(
            proposal,
            GuardianWriteOutcome.APPLIED,
            proposal.reason,
        )

    @staticmethod
    def _field_is_enabled(field_name: GuardianFieldName, policy: GuardianPolicy) -> bool:
        if _STAGE_RANK[policy.rollout.stage] < _FIELD_RANK[field_name]:
            return False
        return {
            GuardianFieldName.LOAD_FACTOR: policy.auto_apply.load_factor,
            GuardianFieldName.PRIORITY: policy.auto_apply.priority,
            GuardianFieldName.SCHEDULABLE: policy.auto_apply.schedulable,
        }[field_name]

    async def _finish(
        self,
        proposal: GuardianWriteProposal,
        outcome: GuardianWriteOutcome,
        reason: str,
    ) -> GuardianWriteDecision:
        decision = GuardianWriteDecision(
            channel_id=proposal.channel_id,
            field_name=proposal.field_name,
            outcome=outcome,
            current_value=proposal.current_value,
            desired_value=proposal.desired_value,
            reason=reason,
        )
        await self.repository.add_write_audit(
            channel_id=proposal.channel_id,
            action=proposal.field_name.value,
            before=proposal.current_value,
            after=proposal.desired_value,
            reason=reason,
            idempotency_key=proposal.idempotency_key,
            outcome=outcome.value,
        )
        await self.repository.save_idempotent_result(
            proposal.idempotency_key,
            "guardian_write_field",
            proposal.channel_id + ":" + proposal.field_name.value,
            decision.model_dump(mode="json"),
        )
        return decision
