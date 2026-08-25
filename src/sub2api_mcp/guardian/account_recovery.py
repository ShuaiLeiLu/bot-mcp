"""Pure conditional account-recovery selection for Guardian."""

from __future__ import annotations

from .contracts import (
    AccountRecoveryClassification,
    AccountRecoveryPolicy,
    AccountRecoveryRunTrigger,
    GuardianAccountObservation,
    GuardianAccountRecoveryDecision,
    GuardianAccountRecoverySelection,
    GuardianAccountStatus,
)


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
