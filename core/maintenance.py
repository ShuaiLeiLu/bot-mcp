from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol, Sequence

from probe import AccountGroupState, ChannelProbe


@dataclass(frozen=True, slots=True)
class AccountTestResult:
    account_id: str
    success: bool
    definitive_failure: bool
    reason: str = ""


@dataclass(frozen=True, slots=True)
class AccountDisableResult:
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


@dataclass(frozen=True, slots=True)
class MaintenanceReport:
    adjustments: tuple[MaintenanceAdjustment, ...] = ()


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
    ) -> list[MaintenanceAdjustment]:
        if not self._policy.channel_account_sweep_enabled:
            return []

        adjustments: list[MaintenanceAdjustment] = []
        tested_ids: set[str] = set()
        for probe in probes:
            group = probe.accounts
            if group is None or group.error_count <= 0:
                continue
            candidates = [
                account
                for account in accounts
                if group.group_id in account.group_ids
                and account.bucket == "error"
                and account.status == "error"
                and account.schedulable
                and not account.expired
                and account.account_id not in tested_ids
                and account.account_id not in already_adjusted
            ]
            if len(tested_ids) + len(candidates) > self._policy.channel_account_sweep_max_accounts:
                candidates = candidates[
                    : self._policy.channel_account_sweep_max_accounts - len(tested_ids)
                ]
            for account in candidates:
                tested_ids.add(account.account_id)
                result = await self._gateway.test_account_availability(account.account_id)
                if (
                    result.success
                    or not result.definitive_failure
                    or account.account_id in already_adjusted
                ):
                    continue
                disabled = await self._gateway.disable_account(account.account_id)
                if disabled.success:
                    already_adjusted.add(account.account_id)
                    adjustments.append(
                        MaintenanceAdjustment(
                            account_id=account.account_id,
                            account_name=account.name or account.account_id,
                            reason="channel_test_failed",
                        )
                    )
        return adjustments


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
    ) -> list[MaintenanceAdjustment]:
        if not self._policy.log_account_guard_enabled:
            return []
        if now.tzinfo is None:
            raise ValueError("maintenance time must be timezone-aware")
        end = now.astimezone(timezone.utc)
        start = end - timedelta(minutes=self._policy.log_window_minutes)
        usage_logs, request_logs = await asyncio.gather(
            self._gateway.fetch_recent_usage_logs(start=start, end=end),
            self._gateway.fetch_recent_request_logs(start=start, end=end),
        )
        account_by_id = {
            account.account_id: account
            for account in accounts
            if account.bucket != "closed"
        }
        error_counts: dict[str, int] = {}
        slow_counts: dict[str, int] = {}
        for record in request_logs:
            if not self._is_account_error(record, start=start, end=end):
                continue
            if record.account_id in account_by_id:
                error_counts[record.account_id] = error_counts.get(record.account_id, 0) + 1
        for record in usage_logs:
            if (
                record.account_id in account_by_id
                and start <= record.created_at.astimezone(timezone.utc) < end
                and record.first_token_ms is not None
                and record.first_token_ms > self._policy.slow_first_token_ms
            ):
                slow_counts[record.account_id] = slow_counts.get(record.account_id, 0) + 1

        adjustments: list[MaintenanceAdjustment] = []
        for account_id in sorted(set(error_counts) | set(slow_counts), key=int):
            reason: str | None = None
            if error_counts.get(account_id, 0) >= self._policy.log_error_threshold:
                reason = "repeated_errors"
            elif (
                slow_counts.get(account_id, 0)
                >= self._policy.log_slow_first_token_threshold
            ):
                reason = "slow_first_token"
            if reason is None or account_id in already_adjusted:
                continue
            disabled = await self._gateway.disable_account(account_id)
            if disabled.success:
                already_adjusted.add(account_id)
                account = account_by_id[account_id]
                adjustments.append(
                    MaintenanceAdjustment(
                        account_id=account_id,
                        account_name=account.name or account_id,
                        reason=reason,
                    )
                )
        return adjustments

    @staticmethod
    def _is_account_error(
        record: RequestLogRecord,
        *,
        start: datetime,
        end: datetime,
    ) -> bool:
        if (
            record.kind != "error"
            or record.account_id is None
            or not start <= record.created_at.astimezone(timezone.utc) < end
        ):
            return False
        status_code = record.status_code or 0
        if status_code in {429, 529}:
            return False
        if record.phase in {"upstream", "account_auth"}:
            return True
        return status_code >= 500


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
        current = now or datetime.now(timezone.utc)
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
            adjustments = await self._channel_sweep.run(
                probes,
                accounts,
                already_adjusted=adjusted_ids,
            )
            adjustments.extend(
                await self._log_guard.run(
                    accounts,
                    now=current,
                    already_adjusted=adjusted_ids,
                )
            )
            self._disabled_ids.update(adjusted_ids)
            return MaintenanceReport(tuple(adjustments))


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
