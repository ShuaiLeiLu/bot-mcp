"""Adapter around the existing validated Sub2API scheduling domain modules."""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from bindings import mask_email
from maintenance import MaintenancePolicy, MaintenanceServiceFactory
from monitor import Sub2APIClient
from notification_image import render_status_report_image
from probe import ChannelProbe, ProbeSnapshot, format_status_report
from pydantic import TypeAdapter
from recovery import active_recovery_window

from ..actor_bridge import ActorAccount
from ..config import Settings
from ..contracts import ProbeResult

_SNAPSHOT_ADAPTER = TypeAdapter(dict[str, Any])


class LegacySub2APIAdapter:
    """Reuse the plugin's hardened API parsing and account-mutation invariants."""

    def __init__(
        self,
        client: Sub2APIClient,
        *,
        recovery_enabled: bool = False,
        recovery_window_start: str = "02:00",
        recovery_window_end: str = "05:00",
        recovery_max_accounts: int = 5,
        maintenance_policy: MaintenancePolicy | None = None,
    ) -> None:
        self._client = client
        self._recovery_enabled = recovery_enabled
        self._recovery_window_start = recovery_window_start
        self._recovery_window_end = recovery_window_end
        self._recovery_max_accounts = recovery_max_accounts
        self._maintenance_policy = maintenance_policy or MaintenancePolicy()
        self._maintenance = MaintenanceServiceFactory.create(client, self._maintenance_policy)
        self._last_probes: list[ChannelProbe] = []
        self._recovery_rotation: list[str] = []

    async def probe(self) -> ProbeResult:
        triggered_at = datetime.now(UTC)
        probes = await self._client.fetch_probe()
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
        )

    async def guardian_snapshot(self) -> dict[str, Any]:
        """Return the richer, still-secret-free snapshot used by Guardian."""
        probes = await self._client.fetch_probe()
        self._last_probes = probes
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
        return {"version": 1, "entries": entries}

    async def recover(self) -> list[dict[str, object]]:
        if not self._recovery_enabled:
            return []
        current = datetime.now(UTC)
        window = active_recovery_window(
            current,
            self._recovery_window_start,
            self._recovery_window_end,
        )
        if window is None:
            return []
        candidates = await self._client.fetch_recovery_candidates(now=current)
        candidates.sort(key=lambda item: int(item.account_id))
        by_id = {item.account_id: item for item in candidates}
        current_ids = set(by_id)
        rotation = [item for item in self._recovery_rotation if item in current_ids]
        rotation.extend(item.account_id for item in candidates if item.account_id not in rotation)
        self._recovery_rotation = rotation
        selected = rotation[: self._recovery_max_accounts]
        outcomes: list[dict[str, object]] = []
        for account_id in selected:
            now = datetime.now(UTC)
            active_window = active_recovery_window(
                now,
                self._recovery_window_start,
                self._recovery_window_end,
            )
            if active_window is None or active_window.window_id != window.window_id:
                break
            outcome = await self._client.test_and_recover_account(
                by_id[account_id], now=now, deadline=window.ends_at
            )
            if outcome.result != "test_failed":
                outcomes.append(asdict(outcome))
            if self._recovery_rotation and self._recovery_rotation[0] == account_id:
                self._recovery_rotation = self._recovery_rotation[1:] + [account_id]
        return outcomes

    async def maintain(self, probe: ProbeResult) -> list[dict[str, object]]:
        del probe
        if not (
            self._maintenance_policy.channel_account_sweep_enabled
            or self._maintenance_policy.log_account_guard_enabled
        ):
            return []
        probes = self._last_probes or await self._client.fetch_probe()
        report = await self._maintenance.run(probes, now=datetime.now(UTC))
        return [asdict(item) for item in report.adjustments]

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
        recovery_enabled=settings.recovery_enabled,
        recovery_window_start=settings.recovery_window_start,
        recovery_window_end=settings.recovery_window_end,
        recovery_max_accounts=settings.recovery_max_accounts_per_run,
        maintenance_policy=policy,
    )
