from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from probe import AccountGroupState, ChannelProbe


@dataclass(frozen=True, slots=True)
class AccountTestResult:
    account_id: str
    success: bool
    definitive_failure: bool
    reason: str = ""
    first_event_ms: int | None = None


@dataclass(frozen=True, slots=True)
class AccountDisableResult:
    account_id: str
    success: bool
    reason: str = ""


@dataclass(frozen=True, slots=True)
class AccountRestoreResult:
    account_id: str
    success: bool
    reason: str = ""


@dataclass(frozen=True, slots=True)
class UsageLogRecord:
    account_id: str
    created_at: datetime
    first_token_ms: int | None
    duration_ms: int | None


@dataclass(frozen=True, slots=True)
class RequestLogRecord:
    account_id: str | None
    created_at: datetime
    kind: str
    status_code: int | None
    phase: str = ""


@dataclass(frozen=True, slots=True)
class MaintenancePolicy:
    channel_account_sweep_enabled: bool = False
    channel_account_sweep_max_accounts: int = 1000
    log_account_guard_enabled: bool = False
    log_error_threshold: int = 3
    log_slow_first_token_threshold: int = 3
    slow_first_token_ms: int = 30_000
    log_window_minutes: int = 30

    def __post_init__(self) -> None:
        if self.channel_account_sweep_max_accounts < 1:
            raise ValueError("channel account sweep limit must be positive")
        if self.log_error_threshold < 1:
            raise ValueError("log error threshold must be positive")
        if self.log_slow_first_token_threshold < 1:
            raise ValueError("slow first token threshold must be positive")
        if self.slow_first_token_ms < 1:
            raise ValueError("slow first token threshold must be positive")
        if self.log_window_minutes < 1:
            raise ValueError("log window must be positive")


@dataclass(frozen=True, slots=True)
class MaintenanceAdjustment:
    account_id: str
    account_name: str
    reason: str
    group_ids: tuple[str, ...] = ()
    threshold_ms: int = 30_000
    observed_count: int = 1


@dataclass(frozen=True, slots=True)
class MaintenanceNotice:
    code: str
    group_id: str = ""
    group_name: str = ""
    account_id: str = ""
    account_name: str = ""
    reason: str = ""


@dataclass(frozen=True, slots=True)
class MaintenanceReport:
    adjustments: tuple[MaintenanceAdjustment, ...] = ()
    notices: tuple[MaintenanceNotice, ...] = ()


class _MinimumUsablePool:
    def __init__(self, accounts: Sequence[AccountGroupState]) -> None:
        self._counts: dict[str, int] = {}
        for account in accounts:
            for group_id in account.group_ids:
                self._counts.setdefault(group_id, 0)
                if self._is_usable(account):
                    self._counts[group_id] += 1

    @staticmethod
    def _is_usable(account: AccountGroupState) -> bool:
        return (
            account.bucket == "available"
            and account.status == "active"
            and account.schedulable
            and not account.expired
        )

    def can_disable(self, account: AccountGroupState) -> bool:
        if not account.group_ids or any(
            group_id not in self._counts for group_id in account.group_ids
        ):
            return False
        decrement = 1 if self._is_usable(account) else 0
        return all(
            self._counts[group_id] - decrement >= 1
            for group_id in account.group_ids
        )

    def record_disable(self, account: AccountGroupState) -> None:
        if not self._is_usable(account):
            return
        for group_id in account.group_ids:
            self._counts[group_id] -= 1


class MaintenanceGateway(Protocol):
    async def fetch_account_group_states(
        self,
        *,
        now: datetime,
    ) -> list[AccountGroupState]: ...

    async def test_account_availability(
        self,
        account_id: str,
    ) -> AccountTestResult: ...

    async def disable_account(self, account_id: str) -> AccountDisableResult: ...

    async def fetch_recent_usage_logs(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[UsageLogRecord]: ...

    async def fetch_recent_request_logs(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[RequestLogRecord]: ...


class _ChannelAccountSweepService:
    def __init__(self, gateway: MaintenanceGateway, policy: MaintenancePolicy):
        self._gateway = gateway
        self._policy = policy

    async def run(
        self,
        probes: Sequence[ChannelProbe],
        accounts: Sequence[AccountGroupState],
        *,
        already_adjusted: set[str],
        pool: _MinimumUsablePool,
    ) -> tuple[list[MaintenanceAdjustment], list[MaintenanceNotice]]:
        if not self._policy.channel_account_sweep_enabled:
            return [], []

        adjustments: list[MaintenanceAdjustment] = []
        notices: list[MaintenanceNotice] = []
        test_results: dict[str, AccountTestResult] = {}
        processed_groups: set[str] = set()
        for probe in probes:
            if probe.channel.status not in {"failed", "error"}:
                continue
            group = probe.accounts
            if group is None:
                notices.append(
                    MaintenanceNotice(
                        code="AMBIGUOUS_GROUP_MAPPING",
                        group_name=probe.channel.name,
                    )
                )
                continue
            if group.group_id in processed_groups:
                continue
            processed_groups.add(group.group_id)
            candidates = [
                account
                for account in accounts
                if group.group_id in account.group_ids
                and not account.expired
                and account.account_id not in already_adjusted
                and account.status not in {"inactive", "disabled"}
                and not (account.status == "active" and not account.schedulable)
            ]
            candidates.sort(key=lambda item: int(item.account_id))
            untested = [
                account
                for account in candidates
                if account.account_id not in test_results
            ]
            remaining_budget = (
                self._policy.channel_account_sweep_max_accounts - len(test_results)
            )
            if len(untested) > remaining_budget:
                notices.append(
                    MaintenanceNotice(
                        code="SWEEP_LIMIT_REACHED",
                        group_id=group.group_id,
                        group_name=group.name,
                    )
                )
                continue
            for account in untested:
                test_results[account.account_id] = (
                    await self._gateway.test_account_availability(account.account_id)
                )
            group_results = [test_results[account.account_id] for account in candidates]
            if not any(result.success for result in group_results):
                notices.append(
                    MaintenanceNotice(
                        code="NO_HEALTHY_ACCOUNT",
                        group_id=group.group_id,
                        group_name=group.name,
                    )
                )
                continue
            for account in candidates:
                result = test_results[account.account_id]
                if (
                    result.success
                    or not result.definitive_failure
                    or account.account_id in already_adjusted
                ):
                    continue
                if not pool.can_disable(account):
                    notices.append(
                        MaintenanceNotice(
                            code="MINIMUM_POOL_PROTECTED",
                            group_id=group.group_id,
                            group_name=group.name,
                            account_id=account.account_id,
                            account_name=account.name or account.account_id,
                            reason="channel_test_failed",
                        )
                    )
                    continue
                disabled = await self._gateway.disable_account(account.account_id)
                if disabled.success:
                    pool.record_disable(account)
                    already_adjusted.add(account.account_id)
                    adjustments.append(
                        MaintenanceAdjustment(
                            account_id=account.account_id,
                            account_name=account.name or account.account_id,
                            reason="channel_test_failed",
                            group_ids=account.group_ids,
                            threshold_ms=self._policy.slow_first_token_ms,
                        )
                    )
        return adjustments, notices


class _AccountLogGuardService:
    def __init__(self, gateway: MaintenanceGateway, policy: MaintenancePolicy):
        self._gateway = gateway
        self._policy = policy

    async def run(
        self,
        accounts: Sequence[AccountGroupState],
        *,
        now: datetime,
        already_adjusted: set[str],
        pool: _MinimumUsablePool,
    ) -> tuple[list[MaintenanceAdjustment], list[MaintenanceNotice]]:
        if not self._policy.log_account_guard_enabled:
            return [], []
        if now.tzinfo is None:
            raise ValueError("maintenance time must be timezone-aware")
        end = now.astimezone(UTC)
        start = end - timedelta(minutes=self._policy.log_window_minutes)
        usage_logs = await self._gateway.fetch_recent_usage_logs(start=start, end=end)
        account_by_id = {
            account.account_id: account
            for account in accounts
            if account.bucket != "closed"
        }
        slow_counts: dict[str, int] = {}
        for record in usage_logs:
            if (
                record.account_id in account_by_id
                and start <= record.created_at.astimezone(UTC) < end
                and record.first_token_ms is not None
                and record.first_token_ms > self._policy.slow_first_token_ms
            ):
                slow_counts[record.account_id] = slow_counts.get(record.account_id, 0) + 1

        adjustments: list[MaintenanceAdjustment] = []
        notices: list[MaintenanceNotice] = []
        for account_id in sorted(slow_counts, key=int):
            if (
                slow_counts[account_id]
                < self._policy.log_slow_first_token_threshold
                or account_id in already_adjusted
            ):
                continue
            reason = "slow_first_token"
            account = account_by_id[account_id]
            if not pool.can_disable(account):
                notices.append(
                    MaintenanceNotice(
                        code="MINIMUM_POOL_PROTECTED",
                        account_id=account_id,
                        account_name=account.name or account_id,
                        reason=reason,
                    )
                )
                continue
            disabled = await self._gateway.disable_account(account_id)
            if disabled.success:
                pool.record_disable(account)
                already_adjusted.add(account_id)
                adjustments.append(
                    MaintenanceAdjustment(
                        account_id=account_id,
                        account_name=account.name or account_id,
                        reason=reason,
                        group_ids=account.group_ids,
                        threshold_ms=self._policy.slow_first_token_ms,
                        observed_count=slow_counts[account_id],
                    )
                )
        return adjustments, notices


class MaintenanceCoordinator:
    def __init__(
        self,
        gateway: MaintenanceGateway,
        policy: MaintenancePolicy,
    ):
        self._gateway = gateway
        self._policy = policy
        self._channel_sweep = _ChannelAccountSweepService(gateway, policy)
        self._log_guard = _AccountLogGuardService(gateway, policy)
        self._lock = asyncio.Lock()
        self._disabled_ids: set[str] = set()

    async def run(
        self,
        probes: Sequence[ChannelProbe],
        *,
        now: datetime | None = None,
    ) -> MaintenanceReport:
        if not (
            self._policy.channel_account_sweep_enabled
            or self._policy.log_account_guard_enabled
        ):
            return MaintenanceReport()
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("maintenance time must be timezone-aware")
        async with self._lock:
            accounts = await self._gateway.fetch_account_group_states(now=current)
            current_closed_ids = {
                account.account_id
                for account in accounts
                if account.bucket == "closed"
            }
            self._disabled_ids.intersection_update(current_closed_ids)
            adjusted_ids = set(self._disabled_ids)
            pool = _MinimumUsablePool(accounts)
            adjustments, notices = await self._channel_sweep.run(
                probes,
                accounts,
                already_adjusted=adjusted_ids,
                pool=pool,
            )
            log_adjustments, log_notices = await self._log_guard.run(
                accounts,
                now=current,
                already_adjusted=adjusted_ids,
                pool=pool,
            )
            adjustments.extend(log_adjustments)
            notices.extend(log_notices)
            self._disabled_ids.update(adjusted_ids)
            return MaintenanceReport(tuple(adjustments), tuple(notices))


class MaintenanceServiceFactory:
    @staticmethod
    def create(
        gateway: MaintenanceGateway,
        policy: MaintenancePolicy,
    ) -> MaintenanceCoordinator:
        return MaintenanceCoordinator(gateway, policy)


def format_maintenance_adjustments(
    adjustments: Sequence[MaintenanceAdjustment],
) -> str:
    labels = {
        "channel_test_failed": "渠道异常测试失败",
        "repeated_errors": "30分钟内重复上游错误",
        "slow_first_token": "首字延迟超过30秒",
    }
    lines = ["账号自动处理"]
    for adjustment in adjustments:
        reason = labels.get(adjustment.reason, "触发账号健康规则")
        lines.append(
            f"{adjustment.account_name} (#{adjustment.account_id})：{reason}，已关闭"
        )
    return "\n".join(lines)
