from __future__ import annotations

from dataclasses import dataclass, field, replace
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
    disable_results: dict[str, AccountDisableResult] = field(
        default_factory=lambda: dict[str, AccountDisableResult]()
    )
    known_groups: frozenset[str] | None = None
    expire_account_id: str | None = None
    expire_after_fetches: int | None = None
    fetch_count: int = 0
    tested: list[str] = field(default_factory=lambda: list[str]())
    disabled: list[str] = field(default_factory=lambda: list[str]())

    async def fetch_known_group_ids(self) -> frozenset[str]:
        if self.known_groups is not None:
            return self.known_groups
        return frozenset(
            group_id for account in self.accounts for group_id in account.group_ids
        )

    async def fetch_account_group_states(
        self, *, now: datetime
    ) -> list[AccountGroupState]:
        self.fetch_count += 1
        if (
            self.expire_account_id is not None
            and self.expire_after_fetches is not None
            and self.fetch_count > self.expire_after_fetches
        ):
            return [
                replace(account, bucket="closed", expired=True, schedulable=False)
                if account.account_id == self.expire_account_id
                else account
                for account in self.accounts
            ]
        return self.accounts

    async def test_account_availability(self, account_id: str) -> AccountTestResult:
        self.tested.append(account_id)
        return self.tests[account_id]

    async def disable_account(self, account_id: str) -> AccountDisableResult:
        self.disabled.append(account_id)
        result = self.disable_results.get(
            account_id,
            AccountDisableResult(account_id, success=True),
        )
        if result.success:
            self.accounts = [
                replace(account, bucket="closed", status="inactive", schedulable=False)
                if account.account_id == account_id
                else account
                for account in self.accounts
            ]
        return result

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
    assert [notice.code for notice in report.notices] == ["MIN_POOL_PROTECTED"]
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
    assert [notice.code for notice in report.notices] == ["MIN_POOL_PROTECTED"]


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
    assert [notice.code for notice in report.notices] == ["MIN_POOL_PROTECTED"]


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


@pytest.mark.asyncio
async def test_unknown_nonempty_account_membership_fails_closed() -> None:
    gateway = Gateway(
        accounts=[
            _account("1", groups=("36", "999")),
            _account("2", groups=("36",)),
        ],
        tests={
            "1": AccountTestResult("1", success=False, definitive_failure=True),
            "2": AccountTestResult("2", success=True, definitive_failure=False),
        },
        known_groups=frozenset({"36"}),
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=2)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.disabled == []
    assert report.adjustments == ()
    assert [notice.code for notice in report.notices] == [
        "AMBIGUOUS_GROUP_MAPPING"
    ]


@pytest.mark.asyncio
async def test_durable_quarantine_ids_are_excluded_even_if_upstream_state_drifts() -> None:
    gateway = Gateway(
        accounts=[
            _account("1"),
            _account("2"),
            _account("3", bucket="error", status="error"),
        ],
        tests={
            "1": AccountTestResult("1", success=False, definitive_failure=True),
            "3": AccountTestResult("3", success=True, definitive_failure=False),
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=2)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
        excluded_account_ids=frozenset({"2"}),
    )

    assert gateway.tested == ["1", "3"]
    assert gateway.disabled == []
    assert report.adjustments == ()
    assert [notice.code for notice in report.notices] == ["MIN_POOL_PROTECTED"]


@pytest.mark.asyncio
async def test_failed_candidates_are_mutated_in_global_account_id_order() -> None:
    gateway = Gateway(
        accounts=[
            _account("1", groups=("41",)),
            _account("9", groups=("36", "41")),
            _account("2", groups=("36",)),
            _account("3", groups=("41",), bucket="error", status="error"),
        ],
        tests={
            "1": AccountTestResult("1", success=False, definitive_failure=True),
            "2": AccountTestResult("2", success=True, definitive_failure=False),
            "3": AccountTestResult("3", success=True, definitive_failure=False),
            "9": AccountTestResult("9", success=False, definitive_failure=True),
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [
            _probe(group_id="36", available_count=2),
            _probe(group_id="41", available_count=2),
        ],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.disabled == ["1"]
    assert [item.account_id for item in report.adjustments] == ["1"]
    assert [notice.code for notice in report.notices] == ["MIN_POOL_PROTECTED"]


@pytest.mark.asyncio
async def test_latency_and_channel_candidates_share_global_account_order() -> None:
    now = datetime(2026, 8, 25, 2, tzinfo=UTC)
    gateway = Gateway(
        accounts=[_account("1"), _account("2")],
        tests={
            "1": AccountTestResult("1", success=True, definitive_failure=False),
            "2": AccountTestResult("2", success=False, definitive_failure=True),
        },
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
        MaintenancePolicy(
            channel_account_sweep_enabled=True,
            log_account_guard_enabled=True,
        ),
    )

    report = await coordinator.run([_probe(available_count=2)], now=now)

    assert gateway.disabled == ["1"]
    assert [(item.account_id, item.reason) for item in report.adjustments] == [
        ("1", "slow_first_token")
    ]
    assert [notice.code for notice in report.notices] == ["MIN_POOL_PROTECTED"]
    assert report.notices[0].account_id == "2"


@pytest.mark.asyncio
async def test_account_expiring_during_sweep_cannot_count_as_spare_capacity() -> None:
    cycle_start = datetime(2026, 8, 25, 0, tzinfo=UTC)
    gateway = Gateway(
        accounts=[_account("1"), _account("2")],
        tests={
            "1": AccountTestResult("1", success=False, definitive_failure=True),
            "2": AccountTestResult("2", success=True, definitive_failure=False),
        },
        expire_account_id="2",
        expire_after_fetches=1,
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=2)],
        now=cycle_start,
    )

    assert gateway.disabled == []
    assert report.adjustments == ()
    assert [notice.code for notice in report.notices] == ["MIN_POOL_PROTECTED"]


@pytest.mark.asyncio
async def test_mismatched_test_identity_is_indeterminate() -> None:
    gateway = Gateway(
        accounts=[_account("1"), _account("2")],
        tests={
            "1": AccountTestResult("999", success=False, definitive_failure=True),
            "2": AccountTestResult("2", success=True, definitive_failure=False),
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

    assert gateway.disabled == []
    assert report.adjustments == ()


@pytest.mark.asyncio
async def test_uncertain_disable_stops_remaining_mutations() -> None:
    gateway = Gateway(
        accounts=[_account("1"), _account("2"), _account("3")],
        tests={
            "1": AccountTestResult("1", success=False, definitive_failure=True),
            "2": AccountTestResult("2", success=False, definitive_failure=True),
            "3": AccountTestResult("3", success=True, definitive_failure=False),
        },
        disable_results={
            "1": AccountDisableResult(
                "1",
                success=False,
                reason="readback_unavailable",
                state_uncertain=True,
            )
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=3)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.disabled == ["1"]
    assert report.adjustments == ()
    assert [notice.code for notice in report.notices] == [
        "MUTATION_STATE_UNCERTAIN"
    ]


@pytest.mark.asyncio
async def test_channel_sweep_refreshes_inventory_once_before_mutations() -> None:
    gateway = Gateway(
        accounts=[_account(str(account_id)) for account_id in range(1, 7)],
        tests={
            str(account_id): AccountTestResult(
                str(account_id),
                success=account_id == 6,
                definitive_failure=account_id != 6,
            )
            for account_id in range(1, 7)
        },
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run(
        [_probe(available_count=3)],
        now=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )

    assert gateway.disabled == ["1", "2", "3", "4", "5"]
    assert [item.account_id for item in report.adjustments] == gateway.disabled
    assert gateway.fetch_count == 2
