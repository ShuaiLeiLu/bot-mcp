from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest
from maintenance import (
    AccountDisableResult,
    AccountTestResult,
    MaintenancePolicy,
    MaintenanceServiceFactory,
    RequestLogRecord,
    UsageLogRecord,
)
from probe import AccountGroupState, ChannelHealth, ChannelProbe, GroupAccountCounts


@dataclass
class Gateway:
    accounts: list[AccountGroupState]
    tests: dict[str, AccountTestResult] = field(
        default_factory=lambda: dict[str, AccountTestResult]()
    )
    usage_logs: list[UsageLogRecord] = field(
        default_factory=lambda: list[UsageLogRecord]()
    )
    request_logs: list[RequestLogRecord] = field(
        default_factory=lambda: list[RequestLogRecord]()
    )
    tested: list[str] = field(default_factory=lambda: list[str]())
    disabled: list[str] = field(default_factory=lambda: list[str]())

    async def fetch_account_group_states(
        self, *, now: datetime
    ) -> list[AccountGroupState]:
        del now
        return self.accounts

    async def test_account_availability(self, account_id: str) -> AccountTestResult:
        self.tested.append(account_id)
        return self.tests[account_id]

    async def disable_account(self, account_id: str) -> AccountDisableResult:
        self.disabled.append(account_id)
        return AccountDisableResult(account_id, success=True)

    async def fetch_recent_usage_logs(
        self, *, start: datetime, end: datetime
    ) -> list[UsageLogRecord]:
        del start, end
        return self.usage_logs

    async def fetch_recent_request_logs(
        self, *, start: datetime, end: datetime
    ) -> list[RequestLogRecord]:
        del start, end
        return self.request_logs


def _account(
    account_id: str,
    *,
    groups: tuple[str, ...] = ("36",),
    bucket: str = "available",
    status: str = "active",
    schedulable: bool = True,
    expired: bool = False,
) -> AccountGroupState:
    return AccountGroupState(
        account_id=account_id,
        group_ids=groups,
        bucket=bucket,
        name=f"账号{account_id}",
        status=status,
        schedulable=schedulable,
        expired=expired,
    )


def _probe(
    *,
    status: str = "failed",
    group_id: str = "36",
    available_count: int = 1,
) -> ChannelProbe:
    return ChannelProbe(
        channel=ChannelHealth(
            monitor_id=f"monitor-{group_id}",
            name=f"渠道{group_id}",
            provider="openai",
            model="gpt-test",
            status=status,
            latency_ms=1000,
            availability_7d=50,
            last_checked_at="2026-08-25T02:00:00Z",
            enabled=True,
            group_name="",
        ),
        accounts=GroupAccountCounts(
            group_id,
            f"渠道{group_id}",
            4,
            available_count,
            0,
            1,
        ),
    )


@pytest.mark.asyncio
async def test_failed_channel_tests_every_eligible_account_before_disabling() -> None:
    gateway = Gateway(
        accounts=[
            _account("1"),
            _account("2", bucket="error", status="error"),
            _account("3", bucket="error", status="error", schedulable=False),
            _account("4", bucket="closed", schedulable=False),
            _account("5", expired=True),
        ],
        tests={
            "1": AccountTestResult("1", success=True, definitive_failure=False),
            "2": AccountTestResult("2", success=False, definitive_failure=True),
            "3": AccountTestResult("3", success=False, definitive_failure=True),
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=1)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.tested == ["1", "2", "3"]
    assert gateway.disabled == ["2", "3"]
    assert [item.account_id for item in report.adjustments] == ["2", "3"]
    assert all(item.group_ids == ("36",) for item in report.adjustments)


@pytest.mark.asyncio
async def test_all_failed_channel_sweep_never_disables_an_account() -> None:
    gateway = Gateway(
        accounts=[_account("1"), _account("2")],
        tests={
            account_id: AccountTestResult(
                account_id,
                success=False,
                definitive_failure=True,
            )
            for account_id in ("1", "2")
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=2)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.tested == ["1", "2"]
    assert gateway.disabled == []
    assert report.adjustments == ()
    assert [notice.code for notice in report.notices] == ["NO_HEALTHY_ACCOUNT"]


@pytest.mark.asyncio
async def test_minimum_pool_protects_the_last_usable_account() -> None:
    gateway = Gateway(
        accounts=[
            _account("1"),
            _account("2", bucket="error", status="error"),
        ],
        tests={
            "1": AccountTestResult("1", success=False, definitive_failure=True),
            "2": AccountTestResult("2", success=True, definitive_failure=False),
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=1)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.disabled == []
    assert report.adjustments == ()
    assert [notice.code for notice in report.notices] == ["MINIMUM_POOL_PROTECTED"]
    assert report.notices[0].account_id == "1"


@pytest.mark.asyncio
async def test_multi_group_candidate_requires_spare_capacity_in_every_group() -> None:
    candidate = _account("1", groups=("36", "41"))
    gateway = Gateway(
        accounts=[
            candidate,
            _account("2", groups=("36",)),
            _account("3", groups=("41",), bucket="error", status="error"),
        ],
        tests={
            "1": AccountTestResult("1", success=False, definitive_failure=True),
            "2": AccountTestResult("2", success=True, definitive_failure=False),
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(group_id="36", available_count=2)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.disabled == []
    assert [notice.code for notice in report.notices] == ["MINIMUM_POOL_PROTECTED"]


@pytest.mark.asyncio
async def test_slow_log_guard_obeys_the_same_minimum_pool() -> None:
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    gateway = Gateway(
        accounts=[_account("1")],
        usage_logs=[
            UsageLogRecord(
                account_id="1",
                created_at=now - timedelta(minutes=minute),
                first_token_ms=45_000,
                duration_ms=50_000,
            )
            for minute in (1, 2, 3)
        ],
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(log_account_guard_enabled=True),
    )

    report = await coordinator.run([_probe()], now=now)

    assert gateway.disabled == []
    assert report.adjustments == ()
    assert [notice.code for notice in report.notices] == ["MINIMUM_POOL_PROTECTED"]


@pytest.mark.asyncio
async def test_repeated_request_errors_do_not_create_an_unowned_disable() -> None:
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    gateway = Gateway(
        accounts=[_account("1"), _account("2")],
        request_logs=[
            RequestLogRecord(
                account_id="1",
                created_at=now - timedelta(minutes=minute),
                kind="error",
                status_code=502,
                phase="upstream",
            )
            for minute in (1, 2, 3)
        ],
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(log_account_guard_enabled=True),
    )

    report = await coordinator.run([_probe()], now=now)

    assert gateway.disabled == []
    assert report.adjustments == ()


@pytest.mark.asyncio
async def test_failed_channel_without_a_unique_group_mapping_fails_closed() -> None:
    gateway = Gateway(accounts=[_account("1")])
    probe = _probe()
    unmapped = ChannelProbe(channel=probe.channel, accounts=None)
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [unmapped],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.tested == []
    assert gateway.disabled == []
    assert [notice.code for notice in report.notices] == ["AMBIGUOUS_GROUP_MAPPING"]
