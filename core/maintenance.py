from __future__ import annotations

import asyncio
from collections.abc import Sequence, Set
from dataclasses import dataclass, replace
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
    state_uncertain: bool = False


@dataclass(frozen=True, slots=True)
class AccountRestoreResult:
    account_id: str
    success: bool
    reason: str = ""
    state_uncertain: bool = False


@dataclass(frozen=True, slots=True)
class AccountDispatchState:
    account_id: str
    success: bool
    status: str = ""
    schedulable: bool | None = None
    expired: bool = False
    temporary_unavailable: bool = False


@dataclass(frozen=True, slots=True)
class AccountSchedulingState:
    account_id: str
    success: bool
    status: str = ""
    schedulable: bool | None = None
    priority: int | None = None
    load_factor: int | None = None
    concurrency: int | None = None
    effective_load_factor: int | None = None


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
    previous_status: str = "active"
    previous_schedulable: bool = True


@dataclass(frozen=True, slots=True)
class MaintenanceNotice:
    code: str
    group_id: str = ""
    group_name: str = ""
    account_id: str = ""
    account_name: str = ""
    reason: str = ""
    group_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MaintenanceReport:
    adjustments: tuple[MaintenanceAdjustment, ...] = ()
    notices: tuple[MaintenanceNotice, ...] = ()


class _MinimumUsablePool:
    def __init__(
        self,
        accounts: Sequence[AccountGroupState],
        *,
        excluded_account_ids: Set[str] = frozenset(),
    ) -> None:
        self._counts: dict[str, int] = {}
        for account in accounts:
            for group_id in account.group_ids:
                self._counts.setdefault(group_id, 0)
                if (
                    account.account_id not in excluded_account_ids
                    and self._is_usable(account)
                ):
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

    def blocking_group_ids(self, account: AccountGroupState) -> tuple[str, ...]:
        if not account.group_ids:
            return ()
        decrement = 1 if self._is_usable(account) else 0
        return tuple(
            group_id
            for group_id in account.group_ids
            if group_id not in self._counts
            or self._counts[group_id] - decrement < 1
        )

class MaintenanceGateway(Protocol):
    async def fetch_known_group_ids(self) -> frozenset[str]: ...

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


class MaintenanceMutationObserver(Protocol):
    async def before_disable(self, adjustment: MaintenanceAdjustment) -> None: ...

    async def after_disable(
        self,
        adjustment: MaintenanceAdjustment,
        result: AccountDisableResult,
    ) -> None: ...


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
        known_group_ids: frozenset[str],
    ) -> tuple[list[MaintenanceAdjustment], list[MaintenanceNotice]]:
        if not self._policy.channel_account_sweep_enabled:
            return [], []

        notices: list[MaintenanceNotice] = []
        failed_groups: dict[str, str] = {}
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
            if group.group_id not in known_group_ids:
                notices.append(
                    MaintenanceNotice(
                        code="AMBIGUOUS_GROUP_MAPPING",
                        group_id=group.group_id,
                        group_name=group.name,
                    )
                )
                continue
            failed_groups[group.group_id] = group.name
        candidates = sorted(
            (
                account
                for account in accounts
                if set(account.group_ids) & failed_groups.keys()
                and self._eligible(account, already_adjusted=already_adjusted)
            ),
            key=lambda item: int(item.account_id),
        )
        if len(candidates) > self._policy.channel_account_sweep_max_accounts:
            ordered_groups = sorted(
                failed_groups.items(),
                key=lambda item: int(item[0]),
            )
            for group_id, group_name in ordered_groups:
                notices.append(
                    MaintenanceNotice(
                        code="SWEEP_LIMIT_REACHED",
                        group_id=group_id,
                        group_name=group_name,
                    )
                )
            return [], notices

        test_results: dict[str, AccountTestResult] = {}
        for account in candidates:
            result = await self._gateway.test_account_availability(account.account_id)
            if (
                result.account_id != account.account_id
                or (result.success and result.definitive_failure)
            ):
                result = AccountTestResult(
                    account.account_id,
                    success=False,
                    definitive_failure=False,
                    reason="invalid_test_result",
                )
            test_results[account.account_id] = result

        healthy_groups: set[str] = set()
        for group_id, group_name in sorted(failed_groups.items(), key=lambda item: int(item[0])):
            if any(
                test_results[account.account_id].success
                for account in candidates
                if group_id in account.group_ids
            ):
                healthy_groups.add(group_id)
            else:
                notices.append(
                    MaintenanceNotice(
                        code="NO_HEALTHY_ACCOUNT",
                        group_id=group_id,
                        group_name=group_name,
                    )
                )

        adjustments: list[MaintenanceAdjustment] = []
        for original in candidates:
            result = test_results[original.account_id]
            failed_memberships = set(original.group_ids) & failed_groups.keys()
            if (
                result.success
                or not result.definitive_failure
                or not failed_memberships
                or not failed_memberships <= healthy_groups
                or original.account_id in already_adjusted
            ):
                continue
            if not set(original.group_ids) <= known_group_ids:
                notices.append(
                    MaintenanceNotice(
                        code="AMBIGUOUS_GROUP_MAPPING",
                        account_id=original.account_id,
                        account_name=original.name or original.account_id,
                    )
                )
                continue
            adjustments.append(
                MaintenanceAdjustment(
                account_id=original.account_id,
                account_name=original.name or original.account_id,
                reason="channel_test_failed",
                group_ids=original.group_ids,
                threshold_ms=self._policy.slow_first_token_ms,
                previous_status=original.status,
                previous_schedulable=original.schedulable,
                )
            )
        return adjustments, notices

    @staticmethod
    def _eligible(
        account: AccountGroupState,
        *,
        already_adjusted: Set[str],
    ) -> bool:
        return (
            bool(account.group_ids)
            and not account.expired
            and account.account_id not in already_adjusted
            and account.status not in {"inactive", "disabled"}
            and not (account.status == "active" and not account.schedulable)
        )


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
        known_group_ids: frozenset[str],
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
            if _MinimumUsablePool._is_usable(account)
            and account.account_id not in already_adjusted
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
            account = account_by_id[account_id]
            if not account.group_ids or not set(account.group_ids) <= known_group_ids:
                notices.append(
                    MaintenanceNotice(
                        code="AMBIGUOUS_GROUP_MAPPING",
                        account_id=account_id,
                        account_name=account.name or account_id,
                        reason="slow_first_token",
                    )
                )
                continue
            adjustments.append(
                MaintenanceAdjustment(
                    account_id=account_id,
                    account_name=account.name or account_id,
                    reason="slow_first_token",
                    group_ids=account.group_ids,
                    threshold_ms=self._policy.slow_first_token_ms,
                    observed_count=slow_counts[account_id],
                    previous_status=account.status,
                    previous_schedulable=account.schedulable,
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
        excluded_account_ids: Set[str] = frozenset(),
        observer: MaintenanceMutationObserver | None = None,
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
            known_group_ids = await self._gateway.fetch_known_group_ids()
            if not known_group_ids:
                return MaintenanceReport(
                    notices=(MaintenanceNotice(code="AMBIGUOUS_GROUP_MAPPING"),)
                )
            current_closed_ids = {
                account.account_id
                for account in accounts
                if account.bucket == "closed"
            }
            self._disabled_ids.intersection_update(current_closed_ids)
            adjusted_ids = set(self._disabled_ids) | set(excluded_account_ids)
            channel_candidates, notices = await self._channel_sweep.run(
                probes,
                accounts,
                already_adjusted=adjusted_ids,
                known_group_ids=known_group_ids,
            )
            log_candidates, log_notices = await self._log_guard.run(
                accounts,
                now=current,
                already_adjusted=adjusted_ids,
                known_group_ids=known_group_ids,
            )
            notices.extend(log_notices)
            candidate_by_id = {
                candidate.account_id: candidate for candidate in log_candidates
            }
            for candidate in channel_candidates:
                candidate_by_id[candidate.account_id] = candidate

            adjustments: list[MaintenanceAdjustment] = []
            fresh_accounts = await self._gateway.fetch_account_group_states(
                now=datetime.now(UTC)
            )
            for account_id in sorted(candidate_by_id, key=int):
                candidate = candidate_by_id[account_id]
                fresh = next(
                    (item for item in fresh_accounts if item.account_id == account_id),
                    None,
                )
                is_eligible = bool(
                    fresh is not None
                    and (
                        _MinimumUsablePool._is_usable(fresh)
                        if candidate.reason == "slow_first_token"
                        else _ChannelAccountSweepService._eligible(
                            fresh,
                            already_adjusted=adjusted_ids,
                        )
                    )
                )
                if (
                    fresh is None
                    or not is_eligible
                    or fresh.group_ids != candidate.group_ids
                    or not fresh.group_ids
                    or not set(fresh.group_ids) <= known_group_ids
                ):
                    notices.append(
                        MaintenanceNotice(
                            code="AMBIGUOUS_GROUP_MAPPING",
                            account_id=account_id,
                            account_name=candidate.account_name,
                            reason=candidate.reason,
                        )
                    )
                    continue
                pool = _MinimumUsablePool(
                    fresh_accounts,
                    excluded_account_ids=adjusted_ids,
                )
                if not pool.can_disable(fresh):
                    notices.append(
                        MaintenanceNotice(
                            code="MIN_POOL_PROTECTED",
                            account_id=account_id,
                            account_name=fresh.name or candidate.account_name,
                            reason=candidate.reason,
                            group_ids=pool.blocking_group_ids(fresh),
                        )
                    )
                    continue
                adjustment = MaintenanceAdjustment(
                    account_id=account_id,
                    account_name=fresh.name or candidate.account_name,
                    reason=candidate.reason,
                    group_ids=fresh.group_ids,
                    threshold_ms=candidate.threshold_ms,
                    observed_count=candidate.observed_count,
                    previous_status=fresh.status,
                    previous_schedulable=fresh.schedulable,
                )
                if observer is not None:
                    await observer.before_disable(adjustment)
                raw_result = await self._gateway.disable_account(account_id)
                result = raw_result
                if (
                    raw_result.account_id != account_id
                    or (raw_result.success and raw_result.state_uncertain)
                ):
                    result = AccountDisableResult(
                        account_id,
                        success=False,
                        reason="invalid_disable_result",
                        state_uncertain=True,
                    )
                if observer is not None:
                    await observer.after_disable(adjustment, result)
                if result.state_uncertain:
                    notices.append(
                        MaintenanceNotice(
                            code="MUTATION_STATE_UNCERTAIN",
                            account_id=account_id,
                            account_name=adjustment.account_name,
                            reason=candidate.reason,
                        )
                    )
                    break
                if result.success:
                    adjusted_ids.add(account_id)
                    adjustments.append(adjustment)
                    fresh_accounts = [
                        (
                            replace(
                                item,
                                bucket="closed",
                                status="inactive",
                                schedulable=False,
                            )
                            if item.account_id == account_id
                            else item
                        )
                        for item in fresh_accounts
                    ]
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
