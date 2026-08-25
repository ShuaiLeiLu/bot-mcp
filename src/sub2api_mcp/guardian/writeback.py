"""Field-owned, idempotent Guardian writeback boundary with safe defaults."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from .contracts import (
    GuardianFieldName,
    GuardianFieldOwner,
    GuardianPolicy,
    GuardianWriteDecision,
    GuardianWriteOutcome,
    GuardianWriteProposal,
)
from .ownership import observe_field_value
from .repository import GuardianRepository


class GuardianFieldWriter(Protocol):
    async def write_field(
        self,
        account_id: str,
        field_name: GuardianFieldName,
        value: object,
    ) -> int | bool: ...


class GuardianWritebackService:
    def __init__(
        self,
        repository: GuardianRepository,
        writer: GuardianFieldWriter | None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.repository = repository
        self._writer = writer
        self._clock = clock or (lambda: datetime.now(UTC))

    async def apply(
        self,
        proposal: GuardianWriteProposal,
        *,
        policy: GuardianPolicy,
    ) -> GuardianWriteDecision:
        operation_subject = (
            f"{proposal.channel_id}:{proposal.account_id}:{proposal.field_name.value}"
        )
        saved = await self.repository.get_idempotent_result(
            proposal.idempotency_key,
            "guardian_write_field",
            operation_subject,
        )
        if saved is not None:
            return GuardianWriteDecision.model_validate(saved)

        ownership = observe_field_value(
            await self.repository.get_field_ownership(
                proposal.channel_id,
                proposal.field_name,
                account_id=proposal.account_id,
            ),
            current_value=proposal.current_value,
            channel_id=proposal.channel_id,
            account_id=proposal.account_id,
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
        if not policy.enabled:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.BLOCKED,
                "guardian_disabled",
            )
        cooldown_seconds = {
            GuardianFieldName.LOAD_FACTOR: policy.writes.load_cooldown_seconds,
            GuardianFieldName.PRIORITY: policy.writes.priority_cooldown_seconds,
            GuardianFieldName.SCHEDULABLE: policy.writes.priority_cooldown_seconds,
        }[proposal.field_name]
        if (
            ownership.owner is GuardianFieldOwner.GUARDIAN
            and ownership.last_write_at is not None
            and (self._clock() - ownership.last_write_at).total_seconds()
            < cooldown_seconds
        ):
            return await self._finish(
                proposal,
                GuardianWriteOutcome.BLOCKED,
                "write_cooldown",
            )
        if self._writer is None:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.BLOCKED,
                "writeback_adapter_disabled",
            )
        try:
            verified_value = await self._writer.write_field(
                proposal.account_id,
                proposal.field_name,
                proposal.desired_value,
            )
        except Exception:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.FAILED,
                "writeback_failed",
            )
        if verified_value != proposal.desired_value:
            return await self._finish(
                proposal,
                GuardianWriteOutcome.FAILED,
                "writeback_verification_mismatch",
            )
        await self.repository.save_field_ownership(
            ownership.model_copy(
                update={
                    "owner": GuardianFieldOwner.GUARDIAN,
                    "last_guardian_value": proposal.desired_value,
                    "last_write_at": self._clock().astimezone(UTC),
                }
            )
        )
        return await self._finish(
            proposal,
            GuardianWriteOutcome.APPLIED,
            proposal.reason,
        )

    async def _finish(
        self,
        proposal: GuardianWriteProposal,
        outcome: GuardianWriteOutcome,
        reason: str,
    ) -> GuardianWriteDecision:
        decision = GuardianWriteDecision(
            channel_id=proposal.channel_id,
            account_id=proposal.account_id,
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
            f"{proposal.channel_id}:{proposal.account_id}:{proposal.field_name.value}",
            decision.model_dump(mode="json"),
        )
        return decision
