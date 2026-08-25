from __future__ import annotations

import pytest

from sub2api_mcp.guardian.account_recovery import select_account_recovery_candidates
from sub2api_mcp.guardian.contracts import (
    AccountRecoveryClassification,
    AccountRecoveryPolicy,
    AccountRecoveryRunTrigger,
    GuardianAccountObservation,
    GuardianAccountStatus,
)


def _account(
    account_id: str,
    *,
    status: GuardianAccountStatus = GuardianAccountStatus.ACTIVE,
    schedulable: bool = True,
    groups: tuple[str, ...] = ("36",),
    expired: bool = False,
    temporary: bool = False,
) -> GuardianAccountObservation:
    return GuardianAccountObservation(
        account_id=account_id,
        group_ids=groups,
        status=status,
        schedulable=schedulable,
        expired=expired,
        temporary_unavailable=temporary,
    )


def test_bad_state_selects_only_abnormal_nonpaused_accounts() -> None:
    selection = select_account_recovery_candidates(
        [
            _account("1"),
            _account("2", schedulable=False),
            _account("3", status=GuardianAccountStatus.ERROR, schedulable=False),
            _account("4", status=GuardianAccountStatus.DISABLED, schedulable=False),
            _account("5", status=GuardianAccountStatus.INACTIVE, schedulable=False),
            _account("6", status=GuardianAccountStatus.ERROR, expired=True),
            _account(
                "7",
                status=GuardianAccountStatus.DISABLED,
                schedulable=False,
                temporary=True,
            ),
        ],
        trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
        policy=AccountRecoveryPolicy(),
    )
    by_id = {item.account.account_id: item for item in selection.decisions}

    assert selection.selected_account_ids == ("3", "4", "5")
    assert by_id["1"].classification is AccountRecoveryClassification.AVAILABLE
    assert by_id["1"].reason == "normal_account"
    assert by_id["2"].classification is AccountRecoveryClassification.MANUAL_PAUSE
    assert by_id["2"].reason == "manual_pause"
    assert by_id["3"].classification is AccountRecoveryClassification.UPSTREAM_ERROR
    assert by_id["4"].classification is AccountRecoveryClassification.DISABLED
    assert by_id["6"].classification is AccountRecoveryClassification.EXCLUDED
    assert by_id["7"].classification is AccountRecoveryClassification.EXCLUDED


def test_channel_error_selects_the_whole_group_except_protected_accounts() -> None:
    selection = select_account_recovery_candidates(
        [
            _account("1"),
            _account("2", schedulable=False),
            _account("3", status=GuardianAccountStatus.ERROR, schedulable=False),
            _account("4", status=GuardianAccountStatus.DISABLED, schedulable=False),
            _account("5", groups=("41",)),
            _account("6", expired=True),
        ],
        trigger=AccountRecoveryRunTrigger.CHANNEL_ERROR,
        policy=AccountRecoveryPolicy(),
        group_id="36",
        quarantined_account_ids=frozenset({"4"}),
    )
    by_id = {item.account.account_id: item for item in selection.decisions}

    assert selection.selected_account_ids == ("1", "3", "4")
    assert "5" not in by_id
    assert by_id["1"].classification is AccountRecoveryClassification.AVAILABLE
    assert by_id["2"].classification is AccountRecoveryClassification.MANUAL_PAUSE
    assert by_id["4"].classification is AccountRecoveryClassification.SYSTEM_QUARANTINE
    assert by_id["6"].classification is AccountRecoveryClassification.EXCLUDED


def test_episode_idempotency_and_group_ceiling_fail_closed() -> None:
    observations = [_account("1"), _account("2")]
    repeated = select_account_recovery_candidates(
        observations,
        trigger=AccountRecoveryRunTrigger.CHANNEL_ERROR,
        policy=AccountRecoveryPolicy(),
        group_id="36",
        already_processed_account_ids=frozenset({"1"}),
    )
    blocked = select_account_recovery_candidates(
        observations,
        trigger=AccountRecoveryRunTrigger.CHANNEL_ERROR,
        policy=AccountRecoveryPolicy(max_accounts_per_episode=1),
        group_id="36",
    )

    assert repeated.selected_account_ids == ("2",)
    assert repeated.decisions[0].reason == "already_processed"
    assert blocked.selected_account_ids == ()
    assert blocked.global_block_reason == "account_group_too_large"
    assert all(not item.selected for item in blocked.decisions)


def test_duplicate_observations_and_unmapped_channel_error_are_rejected() -> None:
    duplicate = _account("1")

    with pytest.raises(ValueError, match="duplicate"):
        select_account_recovery_candidates(
            [duplicate, duplicate],
            trigger=AccountRecoveryRunTrigger.BAD_ACCOUNT_STATE,
            policy=AccountRecoveryPolicy(),
        )
    with pytest.raises(ValueError, match="group"):
        select_account_recovery_candidates(
            [duplicate],
            trigger=AccountRecoveryRunTrigger.CHANNEL_ERROR,
            policy=AccountRecoveryPolicy(),
        )
