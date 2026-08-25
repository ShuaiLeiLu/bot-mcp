from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from sub2api_mcp.contracts import (
    AccountObservation,
    AccountObservationStatus,
    DeliveryPurpose,
    DeliveryTargetCreate,
    JobType,
    MediaPolicy,
    ProbeResult,
    TargetType,
)
from sub2api_mcp.guardian.account_recovery import AccountRecoveryExecutor
from sub2api_mcp.guardian.contracts import (
    AccountMutationResult,
    AccountRecoveryOwner,
    AccountRecoveryResult,
    AccountRecoveryRunStatus,
    AccountRecoveryRunTrigger,
    AccountTestExecutionResult,
    GuardianAccountMutationOutcome,
    GuardianAccountTestOutcome,
)
from sub2api_mcp.guardian.engine import GuardianEngine
from sub2api_mcp.guardian.repository import GuardianRepository
from sub2api_mcp.guardian.service import GuardianService
from sub2api_mcp.repository import SqliteRepository


@dataclass
class ScriptedAccountOperations:
    test_results: dict[str, AccountTestExecutionResult]
    test_calls: list[str] = field(default_factory=lambda: list[str]())
    enable_calls: list[str] = field(default_factory=lambda: list[str]())
    disable_calls: list[str] = field(default_factory=lambda: list[str]())

    async def probe(self) -> ProbeResult:
        raise AssertionError("shared Guardian recovery must not perform another inventory probe")

    async def guardian_test_account(self, account_id: str) -> GuardianAccountTestOutcome:
        self.test_calls.append(account_id)
        result = self.test_results[account_id]
        return GuardianAccountTestOutcome(
            account_id=account_id,
            result=result,
            reason=result.value.casefold(),
            attempted=True,
        )

    async def guardian_enable_account(
        self,
        account_id: str,
    ) -> GuardianAccountMutationOutcome:
        self.enable_calls.append(account_id)
        return GuardianAccountMutationOutcome(
            account_id=account_id,
            result=AccountMutationResult.APPLIED,
            reason="verified_enabled",
            attempted=True,
        )

    async def guardian_disable_account(
        self,
        account_id: str,
    ) -> GuardianAccountMutationOutcome:
        self.disable_calls.append(account_id)
        return GuardianAccountMutationOutcome(
            account_id=account_id,
            result=AccountMutationResult.APPLIED,
            reason="verified_disabled",
            attempted=True,
        )


async def _enable_guardian_account_recovery(
    repository: GuardianRepository,
) -> None:
    policy = await repository.get_policy()
    await repository.update_policy(
        policy.model_copy(
            update={
                "enabled": True,
                "account_recovery": policy.account_recovery.model_copy(
                    update={"enabled": True, "owner": AccountRecoveryOwner.GUARDIAN}
                ),
            }
        ),
        expected_revision=policy.revision,
    )


def _observations() -> list[AccountObservation]:
    return [
        AccountObservation(
            account_id="1",
            group_ids=("36",),
            status=AccountObservationStatus.ERROR,
            schedulable=False,
        ),
        AccountObservation(
            account_id="2",
            group_ids=("36",),
            status=AccountObservationStatus.DISABLED,
            schedulable=False,
        ),
        AccountObservation(
            account_id="3",
            group_ids=("36",),
            status=AccountObservationStatus.INACTIVE,
            schedulable=False,
        ),
        AccountObservation(
            account_id="4",
            group_ids=("36",),
            status=AccountObservationStatus.ACTIVE,
            schedulable=False,
        ),
    ]


@pytest.mark.asyncio
async def test_executor_persists_verified_results_and_restart_never_replays(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    snapshot_id = "a" * 64
    occurred_at = datetime(2026, 8, 25, 3, tzinfo=UTC)
    repository = GuardianRepository(path, clock=lambda: occurred_at)
    await repository.initialize()
    await _enable_guardian_account_recovery(repository)
    await repository.upsert_account_observations(
        snapshot_id=snapshot_id,
        observed_at=occurred_at,
        observations=_observations(),
    )
    operations = ScriptedAccountOperations(
        {
            "1": AccountTestExecutionResult.SUCCESS,
            "2": AccountTestExecutionResult.DEFINITIVE_FAILURE,
            "3": AccountTestExecutionResult.INDETERMINATE,
        }
    )
    policy = await repository.get_policy()
    executor = AccountRecoveryExecutor(repository, operations, clock=lambda: occurred_at)

    first = await executor.execute(
        snapshot_id=snapshot_id,
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
        policy=policy.account_recovery,
        policy_revision=policy.revision,
    )
    repeated = await executor.execute(
        snapshot_id=snapshot_id,
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
        policy=policy.account_recovery,
        policy_revision=policy.revision,
    )
    reopened = GuardianRepository(path, clock=lambda: occurred_at + timedelta(minutes=1))
    await reopened.initialize()
    after_restart = await AccountRecoveryExecutor(
        reopened,
        operations,
        clock=lambda: occurred_at + timedelta(minutes=1),
    ).execute(
        snapshot_id=snapshot_id,
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
        policy=policy.account_recovery,
        policy_revision=policy.revision,
    )
    records = await reopened.list_account_recovery_results(first.run_id)

    assert first.status is AccountRecoveryRunStatus.SUCCEEDED
    assert repeated.run_id == first.run_id == after_restart.run_id
    assert operations.test_calls == ["1", "2", "3"]
    assert operations.enable_calls == ["1"]
    assert operations.disable_calls == ["2"]
    assert {item.account_id: item.result for item in records} == {
        "1": AccountRecoveryResult.ENABLED,
        "2": AccountRecoveryResult.DISABLED,
        "3": AccountRecoveryResult.INDETERMINATE,
    }
    assert "4" not in {item.account_id for item in records}


@pytest.mark.asyncio
async def test_notification_failure_does_not_replay_verified_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state.db"
    snapshot_id = "b" * 64
    occurred_at = datetime(2026, 8, 25, 4, tzinfo=UTC)
    notification_repository = SqliteRepository(path)
    repository = GuardianRepository(path, clock=lambda: occurred_at)
    await notification_repository.initialize()
    await repository.initialize()
    await _enable_guardian_account_recovery(repository)
    await repository.upsert_account_observations(
        snapshot_id=snapshot_id,
        observed_at=occurred_at,
        observations=[_observations()[0]],
    )
    await notification_repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="recovery-admin",
            bot_uuid="bot-1",
            target_type=TargetType.PERSON,
            target_id="admin-1",
            purposes=frozenset({DeliveryPurpose.RECOVERY_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
        )
    )
    operations = ScriptedAccountOperations({"1": AccountTestExecutionResult.SUCCESS})
    service = GuardianService(
        repository,
        GuardianEngine(repository, operations),
        notification_repository=notification_repository,
        account_operations=operations,
    )

    async def fail_enqueue(*_args: object, **_kwargs: object) -> Any:
        raise RuntimeError("simulated outbox failure")

    monkeypatch.setattr(notification_repository, "enqueue_outbox", fail_enqueue)
    first = await service.execute_account_recovery(
        snapshot_id=snapshot_id,
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
    )
    monkeypatch.undo()
    reopened_repository = GuardianRepository(path)
    reopened_notifications = SqliteRepository(path)
    await reopened_repository.initialize()
    await reopened_notifications.initialize()
    restarted_service = GuardianService(
        reopened_repository,
        GuardianEngine(reopened_repository, operations),
        notification_repository=reopened_notifications,
        account_operations=operations,
    )
    second = await restarted_service.execute_account_recovery(
        snapshot_id=snapshot_id,
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
    )
    another_restart = GuardianService(
        reopened_repository,
        GuardianEngine(reopened_repository, operations),
        notification_repository=reopened_notifications,
        account_operations=operations,
    )
    third = await another_restart.execute_account_recovery(
        snapshot_id=snapshot_id,
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
    )

    assert first.run_id == second.run_id == third.run_id
    assert first.status is AccountRecoveryRunStatus.SUCCEEDED
    assert operations.test_calls == ["1"]
    assert operations.enable_calls == ["1"]
    assert await reopened_notifications.outbox_backlog() == 1


@pytest.mark.asyncio
async def test_unverified_mutation_stops_and_persists_remaining_accounts_as_skipped(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    snapshot_id = "c" * 64
    occurred_at = datetime(2026, 8, 25, 4, 30, tzinfo=UTC)
    repository = GuardianRepository(path, clock=lambda: occurred_at)
    await repository.initialize()
    await _enable_guardian_account_recovery(repository)
    await repository.upsert_account_observations(
        snapshot_id=snapshot_id,
        observed_at=occurred_at,
        observations=_observations()[:2],
    )

    class UncertainMutationOperations(ScriptedAccountOperations):
        async def guardian_enable_account(
            self,
            account_id: str,
        ) -> GuardianAccountMutationOutcome:
            self.enable_calls.append(account_id)
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=AccountMutationResult.INDETERMINATE,
                reason="enable_readback_failed",
                attempted=True,
            )

    operations = UncertainMutationOperations(
        {
            "1": AccountTestExecutionResult.SUCCESS,
            "2": AccountTestExecutionResult.SUCCESS,
        }
    )
    policy = await repository.get_policy()

    run = await AccountRecoveryExecutor(
        repository,
        operations,
        clock=lambda: occurred_at,
    ).execute(
        snapshot_id=snapshot_id,
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
        policy=policy.account_recovery,
        policy_revision=policy.revision,
    )
    records = await repository.list_account_recovery_results(run.run_id)

    assert operations.test_calls == ["1"]
    assert operations.enable_calls == ["1"]
    assert {item.account_id: item.result for item in records} == {
        "1": AccountRecoveryResult.INDETERMINATE,
        "2": AccountRecoveryResult.SKIPPED,
    }


@pytest.mark.asyncio
async def test_new_channel_error_broadens_once_then_returns_to_bad_state_only(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    scheduler_repository = SqliteRepository(path)
    repository = GuardianRepository(path)
    await scheduler_repository.initialize()
    await repository.initialize()
    await _enable_guardian_account_recovery(repository)
    payload = {
        "version": 1,
        "entries": [
            {
                "monitor_id": "47",
                "name": "team",
                "status": "failed",
                "group_id": "36",
                "upstream_schedulable": True,
                "available_count": 1,
                "error_count": 1,
                "temporary_unavailable_count": 0,
                "closed_count": 1,
            }
        ],
        "accounts": [
            AccountObservation(
                account_id="1",
                group_ids=("36",),
                status=AccountObservationStatus.ACTIVE,
                schedulable=True,
            ).model_dump(mode="json"),
            AccountObservation(
                account_id="2",
                group_ids=("36",),
                status=AccountObservationStatus.ERROR,
                schedulable=False,
            ).model_dump(mode="json"),
            AccountObservation(
                account_id="3",
                group_ids=("36",),
                status=AccountObservationStatus.ACTIVE,
                schedulable=False,
            ).model_dump(mode="json"),
        ],
    }
    operations = ScriptedAccountOperations(
        {
            "1": AccountTestExecutionResult.SUCCESS,
            "2": AccountTestExecutionResult.DEFINITIVE_FAILURE,
        }
    )
    service = GuardianService(
        repository,
        GuardianEngine(repository, operations),
        account_operations=operations,
    )
    first_at = datetime(2026, 8, 25, 3, tzinfo=UTC)
    second_at = first_at + timedelta(minutes=1)
    await scheduler_repository.publish_guardian_snapshot(payload, captured_at=first_at)

    first = await service.run_once(dry_run=True, idempotency_key="first")
    episode = await repository.get_open_channel_error_episode("47")
    await scheduler_repository.publish_guardian_snapshot(payload, captured_at=second_at)
    second = await service.run_once(dry_run=True, idempotency_key="second")

    assert first["status"] == "SUCCEEDED"
    assert second["status"] == "SUCCEEDED"
    assert episode is not None
    assert episode.opened_snapshot_id == first["result"]["snapshot_id"]
    assert operations.test_calls == ["1", "2", "2"]
    assert "3" not in operations.test_calls


@pytest.mark.asyncio
async def test_guardian_handles_durable_recovery_job_without_testing_normal_accounts(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    snapshot_id = "f" * 64
    root_repository = SqliteRepository(path)
    repository = GuardianRepository(path)
    await root_repository.initialize()
    await repository.initialize()
    await _enable_guardian_account_recovery(repository)
    await repository.upsert_account_observations(
        snapshot_id=snapshot_id,
        observed_at=datetime(2026, 8, 25, 4, tzinfo=UTC),
        observations=[
            AccountObservation(
                account_id="1",
                group_ids=("36",),
                status=AccountObservationStatus.ERROR,
                schedulable=False,
            ),
            AccountObservation(
                account_id="2",
                group_ids=("36",),
                status=AccountObservationStatus.ACTIVE,
                schedulable=True,
            ),
        ],
    )
    await root_repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="recovery-admin",
            bot_uuid="bot-1",
            target_type=TargetType.PERSON,
            target_id="admin-1",
            purposes=frozenset({DeliveryPurpose.RECOVERY_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
        )
    )
    operations = ScriptedAccountOperations({"1": AccountTestExecutionResult.SUCCESS})
    service = GuardianService(
        repository,
        GuardianEngine(repository, operations),
        notification_repository=root_repository,
        account_operations=operations,
    )
    job = await root_repository.create_job(
        JobType.RECOVERY,
        {"snapshot_id": snapshot_id, "trigger": "BAD_ACCOUNT_STATE"},
    )

    result = await service.handle_recovery(job)

    assert result["recovery_run"]["status"] == "SUCCEEDED"
    assert operations.test_calls == ["1"]
    assert operations.enable_calls == ["1"]
    assert "2" not in operations.test_calls
