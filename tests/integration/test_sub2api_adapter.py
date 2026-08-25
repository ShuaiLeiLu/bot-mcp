from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

import pytest
from maintenance import (
    AccountDisableResult,
    AccountRestoreResult,
    AccountTestResult,
    MaintenancePolicy,
    RequestLogRecord,
    UsageLogRecord,
)
from monitor import Sub2APIClient
from probe import AccountGroupState, ChannelHealth, ChannelProbe, GroupAccountCounts

from sub2api_mcp.adapters.sub2api import LegacySub2APIAdapter
from sub2api_mcp.contracts import (
    AccountQuarantineReason,
    AccountQuarantineRecord,
    QuarantineProbeResult,
)


class FakeClient:
    def __init__(self) -> None:
        self.latency = 10
        self.calls = 0

    async def fetch_probe(self) -> list[ChannelProbe]:
        self.calls += 1
        channels: list[ChannelProbe] = []
        for index, provider in enumerate(("openai", "anthropic", "gemini", "future-provider"), 1):
            channels.append(
                ChannelProbe(
                    channel=ChannelHealth(
                        monitor_id=str(index),
                        name=f"{provider}-channel",
                        provider=provider,
                        model="model",
                        status="operational",
                        latency_ms=self.latency,
                        availability_7d=99.0,
                        last_checked_at="2026-08-23T00:00:00Z",
                        enabled=True,
                    ),
                    accounts=GroupAccountCounts(
                        group_id=str(index),
                        name=f"{provider}-channel",
                        total_count=2,
                        available_count=2,
                        temporary_unavailable_count=0,
                        error_count=0,
                    ),
                )
            )
        return channels


class MaintenanceFakeClient(FakeClient):
    async def fetch_probe(self) -> list[ChannelProbe]:
        return [
            ChannelProbe(
                channel=ChannelHealth(
                    monitor_id="1",
                    name="特惠渠道",
                    provider="openai",
                    model="model",
                    status="failed",
                    latency_ms=32_000,
                    availability_7d=20.0,
                    last_checked_at="2026-08-25T02:00:00Z",
                    enabled=True,
                ),
                accounts=GroupAccountCounts("7", "特惠渠道", 2, 1, 0, 1),
            )
        ]

    async def fetch_account_group_states(
        self, *, now: datetime
    ) -> list[AccountGroupState]:
        del now
        return [
            AccountGroupState(
                account_id="1",
                group_ids=("7",),
                bucket="available",
                name="健康账号",
                status="active",
                schedulable=True,
            ),
            AccountGroupState(
                account_id="2",
                group_ids=("7",),
                bucket="error",
                name="故障账号",
                status="error",
                schedulable=False,
            ),
        ]

    async def test_account_availability(self, account_id: str) -> AccountTestResult:
        return AccountTestResult(
            account_id,
            success=account_id == "1",
            definitive_failure=account_id == "2",
        )

    async def disable_account(self, account_id: str) -> AccountDisableResult:
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


class QuarantineFakeClient:
    def __init__(
        self,
        *,
        success: bool,
        definitive_failure: bool,
        first_event_ms: int | None,
        restore_success: bool = True,
    ) -> None:
        self.result = AccountTestResult(
            "42",
            success=success,
            definitive_failure=definitive_failure,
            first_event_ms=first_event_ms,
        )
        self.restore_success = restore_success
        self.restore_calls = 0

    async def test_account_availability(self, account_id: str) -> AccountTestResult:
        assert account_id == "42"
        return self.result

    async def restore_account(
        self, account_id: str, *, now: datetime
    ) -> AccountRestoreResult:
        assert account_id == "42"
        assert now.tzinfo is not None
        self.restore_calls += 1
        return AccountRestoreResult(account_id, success=self.restore_success)


def _quarantine(reason: AccountQuarantineReason) -> AccountQuarantineRecord:
    return AccountQuarantineRecord(
        account_id="42",
        reason=reason,
        group_ids=("7",),
        threshold_ms=30_000,
        observed_count=3,
        quarantined_at=datetime(2026, 8, 25, 2, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_adapter_supports_all_provider_channel_values_without_branching() -> None:
    fake = FakeClient()
    adapter = LegacySub2APIAdapter(cast(Sub2APIClient, fake))

    result = await adapter.probe()

    names = [entry["name"] for entry in result.snapshot["entries"]]  # type: ignore[index]
    assert names == [
        "openai-channel",
        "anthropic-channel",
        "gemini-channel",
        "future-provider-channel",
    ]
    assert result.guardian_snapshot is not None
    assert result.guardian_snapshot["entries"][0]["latency_ms"] == 10  # type: ignore[index]
    assert result.captured_at is not None
    assert fake.calls == 1


@pytest.mark.asyncio
async def test_adapter_snapshot_ignores_latency_only_changes() -> None:
    fake = FakeClient()
    adapter = LegacySub2APIAdapter(cast(Sub2APIClient, fake))

    first = await adapter.probe()
    fake.latency = 9999
    second = await adapter.probe()

    assert first.snapshot == second.snapshot
    assert first.report != second.report


@pytest.mark.asyncio
async def test_adapter_emits_strict_verified_quarantine_outcomes() -> None:
    fake = MaintenanceFakeClient()
    adapter = LegacySub2APIAdapter(
        cast(Sub2APIClient, fake),
        maintenance_policy=MaintenancePolicy(channel_account_sweep_enabled=True),
    )
    probe = await adapter.probe()

    outcomes = await adapter.maintain(probe)

    assert outcomes == [
        {
            "outcome": "QUARANTINED",
            "account_id": "2",
            "account_name": "故障账号",
            "reason": "CHANNEL_TEST_FAILED",
            "group_ids": ["7"],
            "threshold_ms": 30_000,
            "observed_count": 1,
        }
    ]


@pytest.mark.asyncio
async def test_slow_quarantine_stays_isolated_until_latency_is_fast() -> None:
    fake = QuarantineFakeClient(
        success=True,
        definitive_failure=False,
        first_event_ms=45_000,
    )
    adapter = LegacySub2APIAdapter(cast(Sub2APIClient, fake))

    outcome = await adapter.probe_quarantined(
        _quarantine(AccountQuarantineReason.SLOW_FIRST_TOKEN)
    )

    assert outcome.result is QuarantineProbeResult.SLOW
    assert outcome.latency_ms == 45_000
    assert outcome.recovered is False
    assert fake.restore_calls == 0


@pytest.mark.asyncio
async def test_fast_slow_quarantine_is_verified_and_restored() -> None:
    fake = QuarantineFakeClient(
        success=True,
        definitive_failure=False,
        first_event_ms=2_000,
    )
    adapter = LegacySub2APIAdapter(cast(Sub2APIClient, fake))

    outcome = await adapter.probe_quarantined(
        _quarantine(AccountQuarantineReason.SLOW_FIRST_TOKEN)
    )

    assert outcome.result is QuarantineProbeResult.RECOVERED
    assert outcome.recovered is True
    assert fake.restore_calls == 1


@pytest.mark.asyncio
async def test_channel_failure_quarantine_does_not_require_latency_to_restore() -> None:
    fake = QuarantineFakeClient(
        success=True,
        definitive_failure=False,
        first_event_ms=None,
    )
    adapter = LegacySub2APIAdapter(cast(Sub2APIClient, fake))

    outcome = await adapter.probe_quarantined(
        _quarantine(AccountQuarantineReason.CHANNEL_TEST_FAILED)
    )

    assert outcome.result is QuarantineProbeResult.RECOVERED
    assert outcome.recovered is True
    assert fake.restore_calls == 1


@pytest.mark.asyncio
async def test_missing_slow_latency_fails_closed_without_restore() -> None:
    fake = QuarantineFakeClient(
        success=True,
        definitive_failure=False,
        first_event_ms=None,
    )
    adapter = LegacySub2APIAdapter(cast(Sub2APIClient, fake))

    outcome = await adapter.probe_quarantined(
        _quarantine(AccountQuarantineReason.SLOW_FIRST_TOKEN)
    )

    assert outcome.result is QuarantineProbeResult.INVALID
    assert outcome.recovered is False
    assert fake.restore_calls == 0


@pytest.mark.asyncio
async def test_restore_verification_failure_stays_quarantined() -> None:
    fake = QuarantineFakeClient(
        success=True,
        definitive_failure=False,
        first_event_ms=2_000,
        restore_success=False,
    )
    adapter = LegacySub2APIAdapter(cast(Sub2APIClient, fake))

    outcome = await adapter.probe_quarantined(
        _quarantine(AccountQuarantineReason.SLOW_FIRST_TOKEN)
    )

    assert outcome.result is QuarantineProbeResult.FAILED
    assert outcome.recovered is False
    assert fake.restore_calls == 1
