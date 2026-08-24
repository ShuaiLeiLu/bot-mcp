from __future__ import annotations

from typing import cast

import pytest
from monitor import Sub2APIClient
from probe import ChannelHealth, ChannelProbe, GroupAccountCounts

from sub2api_mcp.adapters.sub2api import LegacySub2APIAdapter


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
