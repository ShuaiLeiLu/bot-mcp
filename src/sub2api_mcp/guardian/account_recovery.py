"""Pure conditional account-recovery selection for Guardian."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Protocol

from ..errors import ServiceError
from .contracts import (
    AccountMutationResult,
    AccountRecoveryClassification,
    AccountRecoveryOwner,
    AccountRecoveryPolicy,
    AccountRecoveryResult,
    AccountRecoveryRunStatus,
    AccountRecoveryRunTrigger,
    AccountTestExecutionResult,
    GuardianAccountMutationOutcome,
    GuardianAccountObservation,
    GuardianAccountRecoveryDecision,
    GuardianAccountRecoveryRun,
    GuardianAccountRecoverySelection,
    GuardianAccountStatus,
    GuardianAccountTestOutcome,
)
from .repository import GuardianRepository


class AccountRecoveryOperations(Protocol):
    async def guardian_test_account(
        self,
        account_id: str,
    ) -> GuardianAccountTestOutcome: ...

    async def guardian_enable_account(
        self,
        account_id: str,
    ) -> GuardianAccountMutationOutcome: ...

    async def guardian_disable_account(
        self,
        account_id: str,
    ) -> GuardianAccountMutationOutcome: ...


def _classification(
    account: GuardianAccountObservation,
    *,
    quarantined_account_ids: frozenset[str],
) -> tuple[AccountRecoveryClassification, str]:
    if account.expired:
        return AccountRecoveryClassification.EXCLUDED, "expired"
    if account.temporary_unavailable:
        return AccountRecoveryClassification.EXCLUDED, "temporary_unavailable"
    if (
        account.status is GuardianAccountStatus.ACTIVE
        and not account.schedulable
    ):
        return AccountRecoveryClassification.MANUAL_PAUSE, "manual_pause"
    if account.account_id in quarantined_account_ids:
        return AccountRecoveryClassification.SYSTEM_QUARANTINE, "system_quarantine"
    if account.status is GuardianAccountStatus.ERROR:
        return AccountRecoveryClassification.UPSTREAM_ERROR, "upstream_error"
    if account.status in {
        GuardianAccountStatus.DISABLED,
        GuardianAccountStatus.INACTIVE,
    }:
        return AccountRecoveryClassification.DISABLED, "disabled"
    return AccountRecoveryClassification.AVAILABLE, "normal_account"


def select_account_recovery_candidates(
    observations: list[GuardianAccountObservation],
    *,
    trigger: AccountRecoveryRunTrigger,
    policy: AccountRecoveryPolicy,
    group_id: str | None = None,
    quarantined_account_ids: frozenset[str] = frozenset(),
    already_processed_account_ids: frozenset[str] = frozenset(),
) -> GuardianAccountRecoverySelection:
    """Classify a canonical account snapshot without performing any I/O."""
    if trigger is AccountRecoveryRunTrigger.CHANNEL_ERROR and group_id is None:
        raise ValueError("channel-error recovery requires a group ID")
    if group_id is not None and (
        not group_id.isdigit() or int(group_id) <= 0 or len(group_id) > 20
    ):
        raise ValueError("group ID must be a positive decimal identifier")
    account_ids = [item.account_id for item in observations]
    if len(account_ids) != len(set(account_ids)):
        raise ValueError("account observations contain duplicate account IDs")

    channel_scope = trigger is AccountRecoveryRunTrigger.CHANNEL_ERROR or (
        trigger is AccountRecoveryRunTrigger.MANUAL and group_id is not None
    )
    scoped = [
        item
        for item in observations
        if not channel_scope or group_id in item.group_ids
    ]
    scoped.sort(key=lambda item: int(item.account_id))
    decisions: list[GuardianAccountRecoveryDecision] = []
    for account in scoped:
        classification, reason = _classification(
            account,
            quarantined_account_ids=quarantined_account_ids,
        )
        if account.account_id in already_processed_account_ids:
            selected = False
            reason = "already_processed"
        elif classification in {
            AccountRecoveryClassification.MANUAL_PAUSE,
            AccountRecoveryClassification.EXCLUDED,
        }:
            selected = False
        elif channel_scope:
            selected = True
            reason = "channel_error"
        else:
            selected = classification in {
                AccountRecoveryClassification.UPSTREAM_ERROR,
                AccountRecoveryClassification.DISABLED,
                AccountRecoveryClassification.SYSTEM_QUARANTINE,
            }
            if selected:
                reason = "abnormal_state"
        decisions.append(
            GuardianAccountRecoveryDecision(
                account=account,
                classification=classification,
                selected=selected,
                reason=reason,
            )
        )

    global_block_reason = None
    if channel_scope and len(decisions) > policy.max_accounts_per_episode:
        global_block_reason = "account_group_too_large"
        decisions = [
            item.model_copy(
                update={"selected": False, "reason": global_block_reason}
            )
            for item in decisions
        ]
    selected_account_ids = tuple(
        item.account.account_id for item in decisions if item.selected
    )
    return GuardianAccountRecoverySelection(
        trigger=trigger,
        decisions=tuple(decisions),
        selected_account_ids=selected_account_ids,
        global_block_reason=global_block_reason,
    )


class AccountRecoveryExecutor:
    """Execute one durable, sequential Guardian-owned account recovery run."""

    def __init__(
        self,
        repository: GuardianRepository,
        operations: AccountRecoveryOperations,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._repository = repository
        self._operations = operations
        self._clock = clock or (lambda: datetime.now(UTC))

    @staticmethod
    def _dedup_key(
        *,
        snapshot_id: str,
        trigger: AccountRecoveryRunTrigger,
        episode_id: str | None,
        group_id: str | None,
    ) -> str:
        if trigger is AccountRecoveryRunTrigger.CHANNEL_ERROR:
            if episode_id is None:
                raise ValueError("channel-error recovery requires an episode ID")
            return f"episode:{episode_id}:channel-error"
        if trigger is AccountRecoveryRunTrigger.MANUAL:
            return f"snapshot:{snapshot_id}:manual:{group_id or 'all'}"
        return f"snapshot:{snapshot_id}:bad-account-state"

    async def execute(
        self,
        *,
        snapshot_id: str,
        trigger: AccountRecoveryRunTrigger,
        policy: AccountRecoveryPolicy,
        policy_revision: int,
        episode_id: str | None = None,
        channel_id: str | None = None,
        group_id: str | None = None,
        quarantined_account_ids: frozenset[str] = frozenset(),
        already_processed_account_ids: frozenset[str] = frozenset(),
    ) -> GuardianAccountRecoveryRun:
        if not policy.enabled or policy.owner is not AccountRecoveryOwner.GUARDIAN:
            raise ServiceError(
                "ACCOUNT_RECOVERY_NOT_GUARDIAN_OWNED",
                "Guardian account recovery is not enabled and owned by Guardian",
            )
        dedup_key = self._dedup_key(
            snapshot_id=snapshot_id,
            trigger=trigger,
            episode_id=episode_id,
            group_id=group_id,
        )
        owner = f"account-recovery:{uuid.uuid4()}"
        if not await self._repository.acquire_lease(
            "account-recovery",
            owner,
            seconds=120,
        ):
            raise ServiceError(
                "ACCOUNT_RECOVERY_BUSY",
                "Another Guardian account recovery run is active",
            )
        run: GuardianAccountRecoveryRun | None = None
        counts = {
            "selected": 0,
            "tested": 0,
            "enabled": 0,
            "disabled": 0,
            "indeterminate": 0,
            "skipped": 0,
        }
        try:
            started_at = self._now()
            run = await self._repository.create_account_recovery_run(
                dedup_key=dedup_key,
                trigger=trigger,
                snapshot_id=snapshot_id,
                episode_id=episode_id,
                policy_revision=policy_revision,
                started_at=started_at,
            )
            if run.status is not AccountRecoveryRunStatus.RUNNING:
                return run
            stored = await self._repository.list_account_recovery_results(run.run_id)
            stored_account_ids = frozenset(item.account_id for item in stored)
            observations = await self._repository.list_account_observations(snapshot_id)
            selection = select_account_recovery_candidates(
                observations,
                trigger=trigger,
                policy=policy,
                group_id=group_id,
                quarantined_account_ids=quarantined_account_ids,
                already_processed_account_ids=(
                    already_processed_account_ids | stored_account_ids
                ),
            )
            counts = {
                "selected": len(selection.selected_account_ids) + len(stored),
                "tested": sum(item.tested for item in stored),
                "enabled": sum(item.result is AccountRecoveryResult.ENABLED for item in stored),
                "disabled": sum(
                    item.result is AccountRecoveryResult.DISABLED for item in stored
                ),
                "indeterminate": sum(
                    item.result is AccountRecoveryResult.INDETERMINATE for item in stored
                ),
                "skipped": sum(item.result is AccountRecoveryResult.SKIPPED for item in stored),
            }
            if selection.global_block_reason is not None:
                counts["skipped"] += len(selection.decisions)
                return await self._repository.finish_account_recovery_run(
                    run.run_id,
                    status=AccountRecoveryRunStatus.FAILED.value,
                    result=counts,
                    finished_at=self._now(),
                )

            selected = [item for item in selection.decisions if item.selected]
            stop_remaining = False
            for decision in selected:
                account_id = decision.account.account_id
                if stop_remaining:
                    final_result = AccountRecoveryResult.SKIPPED
                    reason = "run_stopped_after_unverified_mutation"
                    tested = False
                else:
                    final_result, reason, tested, stop_remaining = (
                        await self._execute_account(account_id)
                    )
                await self._repository.record_account_recovery_result(
                    run_id=run.run_id,
                    dedup_key=f"{dedup_key}:account:{account_id}",
                    account_id=account_id,
                    channel_id=channel_id,
                    group_id=group_id,
                    classification=decision.classification,
                    result=final_result,
                    reason=(reason or final_result.value.casefold())[:200],
                    tested=tested,
                    occurred_at=self._now(),
                )
                counts["tested"] += int(tested)
                counts[final_result.value.casefold()] += 1
                if not await self._repository.acquire_lease(
                    "account-recovery",
                    owner,
                    seconds=120,
                ):
                    raise RuntimeError("Guardian account recovery lease was lost")
            return await self._repository.finish_account_recovery_run(
                run.run_id,
                status=AccountRecoveryRunStatus.SUCCEEDED.value,
                result=counts,
                finished_at=self._now(),
            )
        except Exception:
            if run is not None and run.status is AccountRecoveryRunStatus.RUNNING:
                with suppress(Exception):
                    await self._repository.finish_account_recovery_run(
                        run.run_id,
                        status=AccountRecoveryRunStatus.FAILED.value,
                        result=counts,
                        finished_at=self._now(),
                    )
            raise
        finally:
            await self._repository.release_lease("account-recovery", owner)

    async def _execute_account(
        self,
        account_id: str,
    ) -> tuple[AccountRecoveryResult, str, bool, bool]:
        try:
            tested = await self._operations.guardian_test_account(account_id)
        except Exception:
            return AccountRecoveryResult.INDETERMINATE, "account_test_failed", True, False
        if tested.account_id != account_id:
            return AccountRecoveryResult.INDETERMINATE, "test_identity_mismatch", True, False
        if tested.result is AccountTestExecutionResult.SKIPPED:
            return AccountRecoveryResult.SKIPPED, tested.reason, tested.attempted, False
        if tested.result is AccountTestExecutionResult.INDETERMINATE:
            return AccountRecoveryResult.INDETERMINATE, tested.reason, tested.attempted, False
        operation = (
            self._operations.guardian_enable_account
            if tested.result is AccountTestExecutionResult.SUCCESS
            else self._operations.guardian_disable_account
        )
        expected = (
            AccountRecoveryResult.ENABLED
            if tested.result is AccountTestExecutionResult.SUCCESS
            else AccountRecoveryResult.DISABLED
        )
        try:
            mutation = await operation(account_id)
        except Exception:
            return (
                AccountRecoveryResult.INDETERMINATE,
                "account_mutation_failed",
                tested.attempted,
                True,
            )
        if mutation.account_id != account_id:
            return (
                AccountRecoveryResult.INDETERMINATE,
                "mutation_identity_mismatch",
                tested.attempted,
                True,
            )
        if mutation.result in {
            AccountMutationResult.APPLIED,
            AccountMutationResult.NO_CHANGE,
        }:
            return expected, mutation.reason, tested.attempted, False
        if not mutation.attempted and mutation.result is AccountMutationResult.BLOCKED:
            return AccountRecoveryResult.SKIPPED, mutation.reason, tested.attempted, False
        return (
            AccountRecoveryResult.INDETERMINATE,
            mutation.reason,
            tested.attempted,
            mutation.attempted,
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            raise ValueError("account recovery clock must be timezone-aware")
        return value.astimezone(UTC)
