from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

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
    tested: list[str] = field(default_factory=lambda: list[str]())
    disabled: list[str] = field(default_factory=lambda: list[str]())

    async def fetch_account_group_states(
        self, *, now: datetime
    ) -> list[AccountGroupState]:
        del now
        return self.accounts

    async def test_account_availability(self, account_id: str) -> AccountTestResult:
        self.tested.append(account_id)
        return AccountTestResult(account_id, success=False, definitive_failure=True)

    async def disable_account(self, account_id: str) -> AccountDisableResult:
        self.disabled.append(account_id)
        return AccountDisableResult(account_id, success=True)

    async def fetch_recent_usage_logs(
        self, *, start: datetime, end: datetime
    ) -> list[UsageLogRecord]:
        del start, end
        return []

    async def fetch_recent_request_logs(
        self, *, start: datetime, end: datetime
    ) -> list[RequestLogRecord]:
        del start, end
        return []


def _account(
    account_id: str,
    *,
    bucket: str,
    status: str,
    schedulable: bool,
) -> AccountGroupState:
    return AccountGroupState(
        account_id=account_id,
        group_ids=("36",),
        bucket=bucket,
        name=f"账号{account_id}",
        status=status,
        schedulable=schedulable,
    )


@pytest.mark.asyncio
async def test_operational_channel_does_not_trigger_an_account_sweep() -> None:
    gateway = Gateway(
        accounts=[
            _account("1", bucket="available", status="active", schedulable=True),
            _account("2", bucket="error", status="error", schedulable=True),
            _account("3", bucket="error", status="error", schedulable=False),
            _account("4", bucket="closed", status="inactive", schedulable=False),
        ]
    )
    probe = ChannelProbe(
        channel=ChannelHealth(
            monitor_id="19",
            name="稳定渠道",
            provider="openai",
            model="gpt-test",
            status="operational",
            latency_ms=1000,
            availability_7d=100,
            last_checked_at="2026-08-23T10:00:00Z",
            enabled=True,
            group_name="",
        ),
        accounts=GroupAccountCounts("36", "稳定渠道", 4, 1, 0, 2),
    )
    coordinator = MaintenanceServiceFactory.create(
        gateway,
        MaintenancePolicy(channel_account_sweep_enabled=True),
    )

    report = await coordinator.run([probe], now=datetime(2026, 8, 23, 10, tzinfo=UTC))

    assert gateway.tested == []
    assert gateway.disabled == []
    assert report.adjustments == ()
