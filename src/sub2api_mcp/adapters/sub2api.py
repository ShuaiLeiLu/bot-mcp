"""Adapter around the existing validated Sub2API scheduling domain modules."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from bindings import mask_email
from maintenance import (
    AccountDisableResult,
    AccountDispatchState,
    MaintenanceAdjustment,
    MaintenanceMutationObserver,
    MaintenancePolicy,
    MaintenanceServiceFactory,
)
from monitor import Sub2APIClient
from notification_image import render_status_report_image
from probe import ChannelProbe, ProbeSnapshot, format_status_report
from pydantic import TypeAdapter

from ..actor_bridge import ActorAccount
from ..config import Settings
from ..contracts import (
    AccountObservation,
    AccountObservationStatus,
    AccountQuarantineIntent,
    AccountQuarantineReason,
    AccountQuarantineRecord,
    MaintenanceOutcome,
    MaintenanceOutcomeCode,
    ProbeResult,
    QuarantineProbeAttempt,
    QuarantineProbeResult,
)
from ..guardian.contracts import (
    AccountMutationResult,
    AccountTestExecutionResult,
    GuardianAccountMutationOutcome,
    GuardianAccountSchedulingState,
    GuardianAccountStatus,
    GuardianAccountTestOutcome,
    GuardianFieldName,
    UpstreamProbeSnapshot,
)

_SNAPSHOT_ADAPTER = TypeAdapter(dict[str, Any])

_REASON_MAP = {
    "channel_test_failed": AccountQuarantineReason.CHANNEL_TEST_FAILED,
    "slow_first_token": AccountQuarantineReason.SLOW_FIRST_TOKEN,
}

BeforeQuarantine = Callable[[dict[str, object]], Awaitable[None]]
AfterQuarantine = Callable[[str, bool, bool], Awaitable[None]]
BeforeRestore = Callable[[str], Awaitable[None]]
AfterRestore = Callable[[str, bool, bool], Awaitable[None]]


class _MaintenanceObserver(MaintenanceMutationObserver):
    def __init__(
        self,
        before_quarantine: BeforeQuarantine,
        after_quarantine: AfterQuarantine,
    ) -> None:
        self._before_quarantine = before_quarantine
        self._after_quarantine = after_quarantine

    async def before_disable(self, adjustment: MaintenanceAdjustment) -> None:
        reason = _REASON_MAP.get(adjustment.reason)
        if reason is None:
            raise ValueError("unsupported quarantine reason")
        await self._before_quarantine(
            {
                "account_id": adjustment.account_id,
                "reason": reason.value,
                "group_ids": list(adjustment.group_ids),
                "threshold_ms": adjustment.threshold_ms,
                "observed_count": adjustment.observed_count,
                "previous_status": adjustment.previous_status,
                "previous_schedulable": adjustment.previous_schedulable,
            }
        )

    async def after_disable(
        self,
        adjustment: MaintenanceAdjustment,
        result: AccountDisableResult,
    ) -> None:
        await self._after_quarantine(
            adjustment.account_id,
            result.success,
            result.state_uncertain,
        )


class LegacySub2APIAdapter:
    """Reuse the plugin's hardened API parsing and account-mutation invariants."""

    def __init__(
        self,
        client: Sub2APIClient,
        *,
        maintenance_policy: MaintenancePolicy | None = None,
    ) -> None:
        self._client = client
        self._maintenance_policy = maintenance_policy or MaintenancePolicy()
        self._maintenance = MaintenanceServiceFactory.create(client, self._maintenance_policy)
        self._last_probes: list[ChannelProbe] = []

    async def probe(self) -> ProbeResult:
        triggered_at = datetime.now(UTC)
        probes, accounts = await self._client.fetch_probe_with_accounts()
        self._last_probes = probes
        snapshot = _SNAPSHOT_ADAPTER.validate_json(ProbeSnapshot.from_probes(probes).to_bytes())
        image_base64: str | None = None
        try:
            image_data_uri = render_status_report_image(
                probes,
                triggered_at=triggered_at,
            )
            prefix = "data:image/png;base64,"
            if image_data_uri.startswith(prefix):
                image_base64 = image_data_uri[len(prefix) :]
        except Exception:
            image_base64 = None
        return ProbeResult(
            snapshot=snapshot,
            report=format_status_report(probes, triggered_at=triggered_at),
            image_base64=image_base64,
            guardian_snapshot=self._build_guardian_snapshot(probes),
            account_observations=tuple(
                AccountObservation(
                    account_id=account.account_id,
                    group_ids=account.group_ids,
                    status=AccountObservationStatus(account.status),
                    schedulable=account.schedulable,
                    expired=account.expired,
                    temporary_unavailable=account.bucket == "temporary",
                )
                for account in sorted(accounts, key=lambda item: int(item.account_id))
            ),
            captured_at=triggered_at,
        )

    async def guardian_snapshot(self) -> dict[str, Any]:
        """Return the richer, still-secret-free snapshot used by Guardian."""
        probes = await self._client.fetch_probe()
        self._last_probes = probes
        return self._build_guardian_snapshot(probes)

    @staticmethod
    def _guardian_account_block_reason(state: AccountDispatchState) -> str | None:
        if not state.success:
            return "account_state_unavailable"
        if state.status == "active" and state.schedulable is False:
            return "manual_pause"
        if state.expired:
            return "expired"
        if state.temporary_unavailable:
            return "temporary_unavailable"
        return None

    async def guardian_test_account(
        self,
        account_id: str,
    ) -> GuardianAccountTestOutcome:
        state = await self._client.fetch_account_dispatch_state(account_id)
        observed_status = (
            GuardianAccountStatus(state.status)
            if state.success
            else None
        )
        observed_schedulable = state.schedulable if state.success else None
        blocked = self._guardian_account_block_reason(state)
        if blocked is not None:
            return GuardianAccountTestOutcome(
                account_id=account_id,
                result=(
                    AccountTestExecutionResult.INDETERMINATE
                    if blocked == "account_state_unavailable"
                    else AccountTestExecutionResult.SKIPPED
                ),
                reason=blocked,
                observed_status=observed_status,
                observed_schedulable=observed_schedulable,
            )
        tested = await self._client.test_account_availability(account_id)
        if tested.account_id != account_id:
            return GuardianAccountTestOutcome(
                account_id=account_id,
                result=AccountTestExecutionResult.INDETERMINATE,
                reason="test_identity_mismatch",
            )
        if tested.success:
            result = AccountTestExecutionResult.SUCCESS
        elif tested.definitive_failure:
            result = AccountTestExecutionResult.DEFINITIVE_FAILURE
        else:
            result = AccountTestExecutionResult.INDETERMINATE
        return GuardianAccountTestOutcome(
            account_id=account_id,
            result=result,
            reason=tested.reason,
            first_event_ms=tested.first_event_ms,
            attempted=True,
            observed_status=observed_status,
            observed_schedulable=observed_schedulable,
        )

    async def guardian_enable_account(
        self,
        account_id: str,
        *,
        tested: GuardianAccountTestOutcome,
    ) -> GuardianAccountMutationOutcome:
        if (
            tested.account_id != account_id
            or tested.observed_status is None
            or tested.observed_schedulable is None
        ):
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=AccountMutationResult.INDETERMINATE,
                reason="test_context_invalid",
            )
        if (
            tested.observed_status is GuardianAccountStatus.ACTIVE
            and tested.observed_schedulable is False
        ):
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=AccountMutationResult.BLOCKED,
                reason="manual_pause",
            )
        if tested.result is not AccountTestExecutionResult.SUCCESS:
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=AccountMutationResult.INDETERMINATE,
                reason="test_context_invalid",
            )
        state = await self._client.fetch_account_dispatch_state(account_id)
        blocked = self._guardian_account_block_reason(state)
        if blocked == "manual_pause":
            blocked = None
        if blocked is not None:
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=(
                    AccountMutationResult.INDETERMINATE
                    if blocked == "account_state_unavailable"
                    else AccountMutationResult.BLOCKED
                ),
                reason=blocked,
            )
        if state.status == "active" and state.schedulable is True:
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=AccountMutationResult.NO_CHANGE,
                reason="already_enabled",
            )
        started_at = datetime.now(UTC)
        restored = await self._client.restore_account(
            account_id,
            now=started_at,
            deadline=started_at + timedelta(seconds=30),
        )
        return GuardianAccountMutationOutcome(
            account_id=account_id,
            result=(
                AccountMutationResult.APPLIED
                if restored.success
                else AccountMutationResult.INDETERMINATE
                if restored.state_uncertain
                else AccountMutationResult.BLOCKED
            ),
            reason=restored.reason,
            attempted=True,
        )

    async def guardian_disable_account(
        self,
        account_id: str,
    ) -> GuardianAccountMutationOutcome:
        state = await self._client.fetch_account_dispatch_state(account_id)
        blocked = self._guardian_account_block_reason(state)
        if blocked is not None:
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=(
                    AccountMutationResult.INDETERMINATE
                    if blocked == "account_state_unavailable"
                    else AccountMutationResult.BLOCKED
                ),
                reason=blocked,
            )
        if state.status in {"inactive", "disabled"} and state.schedulable is False:
            return GuardianAccountMutationOutcome(
                account_id=account_id,
                result=AccountMutationResult.NO_CHANGE,
                reason="already_disabled",
            )
        disabled = await self._client.disable_account(account_id)
        return GuardianAccountMutationOutcome(
            account_id=account_id,
            result=(
                AccountMutationResult.APPLIED
                if disabled.success
                else AccountMutationResult.INDETERMINATE
                if disabled.state_uncertain
                else AccountMutationResult.BLOCKED
            ),
            reason=disabled.reason,
            attempted=True,
        )

    async def write_field(
        self,
        account_id: str,
        field_name: GuardianFieldName,
        value: object,
    ) -> int | bool:
        """Write one verified Sub2API account field.

        ``account_id`` is deliberately account-scoped. Callers must resolve Guardian monitor and
        group identities before entering this boundary.
        """
        result = await self._client.write_account_scheduling_field(
            account_id,
            field_name.value.casefold(),
            value,
        )
        if not result.success:
            raise RuntimeError("Guardian account scheduling field write failed")
        verified = result.verified_value
        if isinstance(verified, (bool, int)):
            return verified
        raise RuntimeError("Guardian account scheduling verification is missing")

    async def read_account_scheduling_state(
        self,
        account_id: str,
    ) -> GuardianAccountSchedulingState:
        state = await self._client.fetch_account_scheduling_state(account_id)
        return GuardianAccountSchedulingState(
            account_id=state.account_id,
            success=state.success,
            status=(
                AccountObservationStatus(state.status)
                if state.success
                else None
            ),
            schedulable=state.schedulable,
            priority=state.priority,
            load_factor=state.load_factor,
            concurrency=state.concurrency,
            effective_load_factor=state.effective_load_factor,
            expired=state.expired,
            temporary_unavailable=state.temporary_unavailable,
        )

    @staticmethod
    def _build_guardian_snapshot(probes: list[ChannelProbe]) -> dict[str, Any]:
        entries: list[dict[str, Any]] = []
        for probe in probes:
            channel = probe.channel
            accounts = probe.accounts
            entries.append(
                {
                    "monitor_id": channel.monitor_id,
                    "name": channel.name,
                    "status": channel.status,
                    "group_id": accounts.group_id if accounts is not None else None,
                    "group_name": accounts.name if accounts is not None else None,
                    "available_count": (accounts.available_count if accounts is not None else None),
                    "error_count": accounts.error_count if accounts is not None else None,
                    "temporary_unavailable_count": (
                        accounts.temporary_unavailable_count if accounts is not None else None
                    ),
                    "closed_count": accounts.closed_count if accounts is not None else None,
                    "latency_ms": channel.latency_ms,
                    "upstream_schedulable": channel.enabled,
                }
            )
        entries.sort(key=lambda item: (str(item["monitor_id"]), str(item["name"])))
        snapshot = UpstreamProbeSnapshot.model_validate(
            {"version": 1, "entries": entries}
        )
        return snapshot.model_dump(mode="json")

    async def maintain(
        self,
        probe: ProbeResult,
        *,
        excluded_account_ids: frozenset[str] = frozenset(),
        before_quarantine: BeforeQuarantine | None = None,
        after_quarantine: AfterQuarantine | None = None,
    ) -> list[dict[str, object]]:
        del probe
        if not (
            self._maintenance_policy.channel_account_sweep_enabled
            or self._maintenance_policy.log_account_guard_enabled
        ):
            return []
        probes = self._last_probes or await self._client.fetch_probe()
        observer = (
            _MaintenanceObserver(before_quarantine, after_quarantine)
            if before_quarantine is not None and after_quarantine is not None
            else None
        )
        if (before_quarantine is None) != (after_quarantine is None):
            raise ValueError("both quarantine callbacks are required")
        report = await self._maintenance.run(
            probes,
            now=datetime.now(UTC),
            excluded_account_ids=excluded_account_ids,
            observer=observer,
        )
        outcomes = [
            MaintenanceOutcome(
                outcome=MaintenanceOutcomeCode.QUARANTINED,
                account_id=item.account_id,
                account_name=item.account_name,
                reason=_REASON_MAP[item.reason],
                group_ids=item.group_ids,
                threshold_ms=item.threshold_ms,
                observed_count=item.observed_count,
            )
            for item in report.adjustments
            if item.reason in _REASON_MAP
        ]
        for notice in report.notices:
            outcomes.append(
                MaintenanceOutcome(
                    outcome=MaintenanceOutcomeCode(notice.code),
                    account_id=notice.account_id or None,
                    account_name=notice.account_name or None,
                    reason=_REASON_MAP.get(notice.reason),
                    group_id=notice.group_id or None,
                    group_name=notice.group_name or None,
                    protected_group_ids=notice.group_ids,
                )
            )
        return [
            item.model_dump(mode="json", exclude_none=True, exclude_defaults=True)
            for item in outcomes
        ]

    async def reconcile_quarantine_intent(
        self,
        intent: AccountQuarantineIntent,
    ) -> str:
        state = await self._client.fetch_account_dispatch_state(intent.account_id)
        if not state.success:
            return "KEEP"
        if (
            state.status == intent.previous_status
            and state.schedulable is intent.previous_schedulable
        ):
            return "CLEAR"
        if state.status in {"inactive", "disabled"}:
            accounts = await self._client.fetch_account_group_states(now=datetime.now(UTC))
            current = next(
                (item for item in accounts if item.account_id == intent.account_id),
                None,
            )
            known_group_ids = await self._client.fetch_known_group_ids()
            if (
                current is None
                or current.group_ids != intent.group_ids
                or not set(current.group_ids) <= known_group_ids
            ):
                return "KEEP"
            disabled = await self._client.disable_account(intent.account_id)
            return "PROMOTE" if disabled.success else "KEEP"
        if state.status == "active" and state.schedulable is False:
            return "KEEP"
        return "CLEAR"

    async def probe_quarantined(
        self,
        marker: AccountQuarantineRecord,
        *,
        before_restore: BeforeRestore | None = None,
        after_restore: AfterRestore | None = None,
    ) -> QuarantineProbeAttempt:
        if (before_restore is None) != (after_restore is None):
            raise ValueError("both quarantine restore callbacks are required")
        state = await self._client.fetch_account_dispatch_state(marker.account_id)
        if not state.success:
            return QuarantineProbeAttempt(
                account_id=marker.account_id,
                result=QuarantineProbeResult.INVALID,
            )
        if state.status not in {"inactive", "disabled"} or state.schedulable is not False:
            return QuarantineProbeAttempt(
                account_id=marker.account_id,
                result=QuarantineProbeResult.INVALID,
            )
        tested = await self._client.test_account_availability(marker.account_id)
        if not tested.success:
            return QuarantineProbeAttempt(
                account_id=marker.account_id,
                result=(
                    QuarantineProbeResult.FAILED
                    if tested.definitive_failure
                    else QuarantineProbeResult.INVALID
                ),
                latency_ms=tested.first_event_ms,
            )
        if marker.reason is AccountQuarantineReason.SLOW_FIRST_TOKEN:
            if tested.first_event_ms is None:
                return QuarantineProbeAttempt(
                    account_id=marker.account_id,
                    result=QuarantineProbeResult.INVALID,
                )
            if tested.first_event_ms > marker.threshold_ms:
                return QuarantineProbeAttempt(
                    account_id=marker.account_id,
                    result=QuarantineProbeResult.SLOW,
                    latency_ms=tested.first_event_ms,
                )
        restore_started = datetime.now(UTC)
        if before_restore is not None:
            await before_restore(marker.account_id)
        restored = await self._client.restore_account(
            marker.account_id,
            now=restore_started,
            deadline=restore_started + timedelta(seconds=30),
        )
        if after_restore is not None:
            await after_restore(
                marker.account_id,
                restored.success,
                restored.state_uncertain,
            )
        return QuarantineProbeAttempt(
            account_id=marker.account_id,
            result=(
                QuarantineProbeResult.RECOVERED
                if restored.success
                else QuarantineProbeResult.FAILED
            ),
            latency_ms=tested.first_event_ms,
            recovered=restored.success,
        )

    async def reconcile_quarantine_restore(
        self,
        marker: AccountQuarantineRecord,
    ) -> str:
        account_id = marker.account_id
        state = await self._client.fetch_account_dispatch_state(account_id)
        if not state.success:
            return "KEEP"
        if state.status == "active" and state.schedulable is True:
            return "RECOVERED"
        if state.status in {"inactive", "disabled"} and state.schedulable is False:
            return "CANCEL"
        tested = await self._client.test_account_availability(account_id)
        if not tested.success:
            return "KEEP"
        if marker.reason is AccountQuarantineReason.SLOW_FIRST_TOKEN and (
            tested.first_event_ms is None
            or tested.first_event_ms > marker.threshold_ms
        ):
            return "KEEP"
        restore_started = datetime.now(UTC)
        restored = await self._client.restore_account(
            account_id,
            now=restore_started,
            deadline=restore_started + timedelta(seconds=30),
        )
        return "RECOVERED" if restored.success else "KEEP"

    async def find_active_account(self, email: str) -> ActorAccount | None:
        account = await self._client.find_account_by_email(email)
        if account is None:
            return None
        return ActorAccount(
            user_id=account.user_id,
            email_masked=mask_email(account.email),
            status=account.status,
        )

    async def account_report(self, user_id: str) -> str:
        account, today_usage, month_usage = await asyncio.gather(
            self._client.fetch_account(user_id),
            self._client.fetch_account_usage(user_id, "today"),
            self._client.fetch_account_usage(user_id, "month"),
        )
        if account.user_id != user_id:
            raise ValueError("Sub2API returned a different account")
        status_label = {
            "active": "正常",
            "disabled": "停用",
            "suspended": "冻结",
        }.get(account.status, "未知")
        return "\n".join(
            [
                "智算账户",
                f"邮箱：{mask_email(account.email)}",
                f"状态：{status_label}",
                f"余额：${account.balance:.2f}",
                f"今日使用金额：${today_usage.total_actual_cost:.4f}",
                f"今日请求数量：{today_usage.total_requests:,}",
                f"今日 Token 数量：{today_usage.total_tokens:,}",
                f"本月使用金额：${month_usage.total_actual_cost:.4f}",
            ]
        )


def build_sub2api_adapter(settings: Settings) -> LegacySub2APIAdapter:
    """Build the adapter from validated settings without leaking the admin key."""

    client = Sub2APIClient(
        settings.sub2api_admin_key.get_secret_value(),
        timeout_seconds=settings.sub2api_timeout_seconds,
    )
    policy = MaintenancePolicy(
        channel_account_sweep_enabled=settings.channel_account_sweep_enabled,
        channel_account_sweep_max_accounts=settings.channel_account_sweep_max_accounts,
        log_account_guard_enabled=settings.log_account_guard_enabled,
        log_error_threshold=settings.log_error_threshold,
        log_slow_first_token_threshold=settings.log_slow_first_token_threshold,
        slow_first_token_ms=settings.slow_first_token_ms,
        log_window_minutes=settings.log_window_minutes,
    )
    return LegacySub2APIAdapter(
        client,
        maintenance_policy=policy,
    )
