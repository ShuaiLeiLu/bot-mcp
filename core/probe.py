from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo


class MonitorDataError(ValueError):
    """Raised when Sub2API returns unexpected monitor data."""


_RATE_PREFIX_PATTERN = re.compile(
    r"^\s*\d+(?:\.\d+)?\s*[x×]\s*",
    re.IGNORECASE,
)
_GROUP_DECORATION_PATTERN = re.compile(
    r"【[^】]*】|\[[^\]]*\]|（[^）]*）|\([^)]*\)"
)
_WHITESPACE_PATTERN = re.compile(r"\s+")


def _semantic_group_key(value: str) -> str:
    text = _RATE_PREFIX_PATTERN.sub("", value)
    text = _GROUP_DECORATION_PATTERN.sub("", text)
    text = _WHITESPACE_PATTERN.sub("", text).casefold()
    if text.endswith("渠道"):
        text = text[: -len("渠道")]
    if text.endswith("级"):
        text = text[:-1]
    return text


def _bounded_text(value: Any, field: str, *, required: bool = False) -> str:
    if value is None:
        if required:
            raise MonitorDataError(f"missing {field}")
        return ""
    text = re.sub(r"[\x00-\x1f\x7f]", " ", str(value)).strip()
    if required and not text:
        raise MonitorDataError(f"empty {field}")
    return text[:200]


def _optional_number(value: Any, field: str, number_type: type[int | float]):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MonitorDataError(f"invalid {field}")
    return number_type(value)


def _non_negative_int(value: Any, field: str, *, default: int | None = None) -> int:
    if value is None and default is not None:
        return default
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MonitorDataError(f"invalid {field}")
    return value


def _optional_non_negative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    return _non_negative_int(value, field)


def _positive_id_text(value: Any, field: str) -> str:
    if isinstance(value, bool):
        raise MonitorDataError(f"invalid {field}")
    text = str(value).strip()
    if (
        not text.isdigit()
        or len(text) > 20
        or int(text) <= 0
        or text != str(int(text))
    ):
        raise MonitorDataError(f"invalid {field}")
    return text


@dataclass(frozen=True, slots=True)
class ChannelHealth:
    monitor_id: str
    name: str
    provider: str
    model: str
    status: str
    latency_ms: int | None
    availability_7d: float | None
    last_checked_at: str
    enabled: bool
    group_name: str = ""


@dataclass(frozen=True, slots=True)
class GroupAccountCounts:
    group_id: str
    name: str
    total_count: int
    available_count: int
    temporary_unavailable_count: int
    error_count: int

    def __post_init__(self) -> None:
        _positive_id_text(self.group_id, "group id")
        if not isinstance(self.name, str) or not self.name.strip() or len(self.name) > 200:
            raise MonitorDataError("invalid group name")
        counts = (
            self.total_count,
            self.available_count,
            self.temporary_unavailable_count,
            self.error_count,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in counts
        ):
            raise MonitorDataError("group account counts must be non-negative integers")
        if sum(counts[1:]) > self.total_count:
            raise MonitorDataError("group account counts exceed total")

    @property
    def closed_count(self) -> int:
        return self.total_count - (
            self.available_count
            + self.temporary_unavailable_count
            + self.error_count
        )


@dataclass(frozen=True, slots=True)
class GroupDefinition:
    group_id: str
    name: str


@dataclass(frozen=True, slots=True)
class AccountGroupState:
    account_id: str
    group_ids: tuple[str, ...]
    bucket: str
    name: str = ""
    status: str = ""
    schedulable: bool = False
    expired: bool = False


@dataclass(frozen=True, slots=True)
class ChannelProbe:
    channel: ChannelHealth
    accounts: GroupAccountCounts | None


@dataclass(frozen=True, slots=True)
class ProbeUsageRecord:
    log_id: str
    api_key_id: str
    group_id: str | None
    model: str
    created_at: datetime
    duration_ms: int | None
    user_agent: str


@dataclass(frozen=True, slots=True)
class ProbeSnapshotEntry:
    monitor_id: str
    name: str
    status: str
    group_id: str | None
    available_count: int | None
    error_count: int | None
    temporary_unavailable_count: int | None
    closed_count: int | None


@dataclass(frozen=True, slots=True)
class ProbeSnapshot:
    entries: tuple[ProbeSnapshotEntry, ...]
    VERSION = 1
    MAX_STORAGE_BYTES = 512 * 1024

    @classmethod
    def from_probes(cls, probes: Iterable[ChannelProbe]) -> ProbeSnapshot:
        entries = []
        for probe in probes:
            accounts = probe.accounts
            entries.append(
                ProbeSnapshotEntry(
                    monitor_id=probe.channel.monitor_id,
                    name=probe.channel.name,
                    status=probe.channel.status,
                    group_id=accounts.group_id if accounts is not None else None,
                    available_count=(
                        accounts.available_count if accounts is not None else None
                    ),
                    error_count=accounts.error_count if accounts is not None else None,
                    temporary_unavailable_count=(
                        accounts.temporary_unavailable_count
                        if accounts is not None
                        else None
                    ),
                    closed_count=accounts.closed_count if accounts is not None else None,
                )
            )
        entries.sort(key=lambda entry: (entry.monitor_id, entry.name))
        return cls(tuple(entries))

    def to_bytes(self) -> bytes:
        payload = {
            "version": self.VERSION,
            "entries": [
                {
                    "monitor_id": entry.monitor_id,
                    "name": entry.name,
                    "status": entry.status,
                    "group_id": entry.group_id,
                    "available_count": entry.available_count,
                    "error_count": entry.error_count,
                    "temporary_unavailable_count": entry.temporary_unavailable_count,
                    "closed_count": entry.closed_count,
                }
                for entry in self.entries
            ],
        }
        return json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_bytes(cls, value: bytes) -> ProbeSnapshot:
        if not isinstance(value, bytes) or not value or len(value) > cls.MAX_STORAGE_BYTES:
            raise MonitorDataError("invalid probe snapshot storage")
        try:
            payload = json.loads(value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise MonitorDataError("invalid probe snapshot storage") from exc
        if not isinstance(payload, dict) or payload.get("version") != cls.VERSION:
            raise MonitorDataError("unsupported probe snapshot storage")
        items = payload.get("entries")
        if not isinstance(items, list) or len(items) > 10_000:
            raise MonitorDataError("invalid probe snapshot entries")
        entries = tuple(_parse_probe_snapshot_entry(item) for item in items)
        if tuple(sorted(entries, key=lambda entry: (entry.monitor_id, entry.name))) != entries:
            raise MonitorDataError("probe snapshot entries are not canonical")
        return cls(entries)


def _parse_probe_snapshot_entry(value: Any) -> ProbeSnapshotEntry:
    if not isinstance(value, dict):
        raise MonitorDataError("probe snapshot entry must be an object")
    status = _bounded_text(value.get("status"), "snapshot status", required=True).lower()
    if status not in {"operational", "degraded", "failed", "error", "unknown"}:
        raise MonitorDataError("invalid snapshot status")
    raw_group_id = value.get("group_id")
    group_id = (
        None
        if raw_group_id is None
        else _positive_id_text(raw_group_id, "snapshot group id")
    )
    counts = (
        _optional_non_negative_int(
            value.get("available_count"),
            "snapshot available_count",
        ),
        _optional_non_negative_int(value.get("error_count"), "snapshot error_count"),
        _optional_non_negative_int(
            value.get("temporary_unavailable_count"),
            "snapshot temporary_unavailable_count",
        ),
        _optional_non_negative_int(value.get("closed_count"), "snapshot closed_count"),
    )
    if any(count is None for count in counts) != all(count is None for count in counts):
        raise MonitorDataError("snapshot account counts are incomplete")
    if (group_id is None) != all(count is None for count in counts):
        raise MonitorDataError("snapshot group and account counts do not match")
    return ProbeSnapshotEntry(
        monitor_id=_bounded_text(
            value.get("monitor_id"),
            "snapshot monitor id",
            required=True,
        ),
        name=_bounded_text(
            value.get("name"),
            "snapshot channel name",
            required=True,
        ),
        status=status,
        group_id=group_id,
        available_count=counts[0],
        error_count=counts[1],
        temporary_unavailable_count=counts[2],
        closed_count=counts[3],
    )


def _normalize_status(value: Any) -> str:
    if value == "":
        return "unknown"
    status = _bounded_text(value, "primary_status", required=True).lower()
    if status not in {"operational", "degraded", "failed", "error"}:
        return "unknown"
    return status


def parse_channel_monitors(payload: Any) -> list[ChannelHealth]:
    """Validate the documented ``data.items`` monitor response."""

    if not isinstance(payload, dict):
        raise MonitorDataError("response must be an object")
    if payload.get("code") != 0:
        raise MonitorDataError("Sub2API request failed")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise MonitorDataError("response data.items must be a list")

    channels: list[ChannelHealth] = []
    seen_ids: set[str] = set()
    for item in data["items"]:
        if not isinstance(item, dict):
            raise MonitorDataError("monitor item must be an object")
        monitor_id = _positive_id_text(item.get("id"), "monitor id")
        if monitor_id in seen_ids:
            raise MonitorDataError("duplicate channel monitor")
        seen_ids.add(monitor_id)
        channels.append(
            ChannelHealth(
                monitor_id=monitor_id,
                name=_bounded_text(item.get("name"), "name", required=True),
                provider=_bounded_text(item.get("provider"), "provider"),
                model=_bounded_text(item.get("primary_model"), "primary_model"),
                status=_normalize_status(item.get("primary_status")),
                latency_ms=_optional_number(
                    item.get("primary_latency_ms"),
                    "primary_latency_ms",
                    int,
                ),
                availability_7d=_optional_number(
                    item.get("availability_7d"),
                    "availability_7d",
                    float,
                ),
                last_checked_at=_bounded_text(
                    item.get("last_checked_at"),
                    "last_checked_at",
                ),
                enabled=item.get("enabled") is True,
                group_name=_bounded_text(item.get("group_name"), "group_name"),
            )
        )
    return channels


def _future_timestamp(value: Any, field: str, now: datetime) -> bool:
    if value is None:
        return False
    if not isinstance(value, str) or len(value) > 100:
        raise MonitorDataError(f"invalid {field}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorDataError(f"invalid {field}") from exc
    if parsed.tzinfo is None:
        raise MonitorDataError(f"invalid {field}")
    return parsed > now.astimezone(parsed.tzinfo)


def parse_group_definitions(payload: Any) -> list[GroupDefinition]:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API group request failed")
    items = payload.get("data")
    if not isinstance(items, list):
        raise MonitorDataError("group response data must be a list")

    groups: list[GroupDefinition] = []
    seen_ids: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise MonitorDataError("group item must be an object")
        group_id = _positive_id_text(item.get("id"), "group id")
        if group_id in seen_ids:
            raise MonitorDataError("duplicate group id")
        seen_ids.add(group_id)
        groups.append(
            GroupDefinition(
                group_id=group_id,
                name=_bounded_text(item.get("name"), "group name", required=True),
            )
        )
    return groups


def parse_probe_usage_page(
    payload: Any,
    *,
    expected_page: int,
) -> tuple[list[ProbeUsageRecord], int, int, int]:
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API probe usage request failed")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise MonitorDataError("probe usage data.items must be a list")
    total = _non_negative_int(data.get("total"), "probe usage total")
    page = _non_negative_int(data.get("page"), "probe usage page")
    page_size = _non_negative_int(data.get("page_size"), "probe usage page_size")
    pages = _non_negative_int(data.get("pages"), "probe usage pages")
    expected_pages = max(1, (total + page_size - 1) // page_size) if page_size else 0
    if (
        page != expected_page
        or page < 1
        or page_size < 1
        or pages != expected_pages
        or page > pages
        or len(data["items"]) > page_size
        or len(data["items"]) > total
    ):
        raise MonitorDataError("invalid probe usage pagination")

    records: list[ProbeUsageRecord] = []
    for item in data["items"]:
        if not isinstance(item, dict):
            raise MonitorDataError("probe usage item must be an object")
        raw_group_id = item.get("group_id")
        created_at_raw = item.get("created_at")
        if not isinstance(created_at_raw, str) or len(created_at_raw) > 100:
            raise MonitorDataError("invalid probe usage created_at")
        try:
            created_at = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MonitorDataError("invalid probe usage created_at") from exc
        if created_at.tzinfo is None:
            raise MonitorDataError("invalid probe usage created_at")
        duration_ms = _optional_non_negative_int(
            item.get("duration_ms"),
            "probe usage duration_ms",
        )
        records.append(
            ProbeUsageRecord(
                log_id=_positive_id_text(item.get("id"), "probe usage id"),
                api_key_id=_positive_id_text(
                    item.get("api_key_id"),
                    "probe usage api key id",
                ),
                # API keys carry their stable group_id in Sub2API usage logs.
                # Source: https://github.com/Wei-Shaw/sub2api/blob/main/backend/ent/schema/api_key.go
                group_id=(
                    None
                    if raw_group_id is None
                    else _positive_id_text(raw_group_id, "probe usage group id")
                ),
                model=_bounded_text(item.get("model"), "probe usage model", required=True),
                created_at=created_at,
                duration_ms=duration_ms,
                user_agent=_bounded_text(item.get("user_agent"), "probe usage user_agent"),
            )
        )
    return records, pages, page_size, len(data["items"])


def parse_account_group_state_page(
    payload: Any,
    *,
    expected_page: int,
    now: datetime,
) -> tuple[list[AccountGroupState], int]:
    if now.tzinfo is None:
        raise ValueError("account snapshot time must be timezone-aware")
    if not isinstance(payload, dict) or payload.get("code") != 0:
        raise MonitorDataError("Sub2API account snapshot request failed")
    data = payload.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        raise MonitorDataError("account snapshot data.items must be a list")
    total = _non_negative_int(data.get("total"), "account snapshot total")
    page = _non_negative_int(data.get("page"), "account snapshot page")
    page_size = _non_negative_int(data.get("page_size"), "account snapshot page_size")
    pages = _non_negative_int(data.get("pages"), "account snapshot pages")
    expected_pages = max(1, (total + page_size - 1) // page_size) if page_size else 0
    if (
        page != expected_page
        or page < 1
        or page_size < 1
        or pages != expected_pages
        or page > pages
        or len(data["items"]) > page_size
        or len(data["items"]) > total
    ):
        raise MonitorDataError("invalid account snapshot pagination")

    states: list[AccountGroupState] = []
    seen_ids: set[str] = set()
    for item in data["items"]:
        if not isinstance(item, dict):
            raise MonitorDataError("account snapshot item must be an object")
        account_id = _positive_id_text(item.get("id"), "account snapshot id")
        if account_id in seen_ids:
            raise MonitorDataError("duplicate account snapshot id")
        seen_ids.add(account_id)
        status = item.get("status")
        if status not in {"active", "error", "disabled", "inactive"}:
            raise MonitorDataError("invalid account snapshot status")
        schedulable = item.get("schedulable")
        auto_pause = item.get("auto_pause_on_expired")
        if not isinstance(schedulable, bool) or not isinstance(auto_pause, bool):
            raise MonitorDataError("invalid account snapshot scheduling fields")
        expires_at = item.get("expires_at")
        if expires_at is not None and (
            isinstance(expires_at, bool) or not isinstance(expires_at, (int, float))
        ):
            raise MonitorDataError("invalid account snapshot expires_at")
        raw_group_ids = item.get("group_ids", [])
        if raw_group_ids is None:
            raw_group_ids = []
        if not isinstance(raw_group_ids, list) or len(raw_group_ids) > 100:
            raise MonitorDataError("invalid account snapshot group_ids")
        group_ids = tuple(
            sorted(
                {
                    _positive_id_text(group_id, "account snapshot group id")
                    for group_id in raw_group_ids
                },
                key=int,
            )
        )
        is_expired = bool(
            auto_pause
            and expires_at is not None
            and expires_at <= now.timestamp()
        )
        is_temporary = any(
            _future_timestamp(item.get(field), field, now)
            for field in (
                "rate_limit_reset_at",
                "overload_until",
                "temp_unschedulable_until",
            )
        )
        if status == "error":
            bucket = "error"
        elif status == "active" and schedulable and not is_expired:
            bucket = "temporary" if is_temporary else "available"
        else:
            bucket = "closed"
        states.append(
            AccountGroupState(
                account_id=account_id,
                group_ids=group_ids,
                bucket=bucket,
                name=_bounded_text(item.get("name"), "account snapshot name"),
                status=status,
                schedulable=schedulable,
                expired=is_expired,
            )
        )
    return states, pages


def aggregate_group_account_counts(
    groups: Iterable[GroupDefinition],
    accounts: Iterable[AccountGroupState],
) -> list[GroupAccountCounts]:
    group_list = list(groups)
    mutable: dict[str, list[int]] = {
        group.group_id: [0, 0, 0, 0] for group in group_list
    }
    bucket_index = {"available": 1, "temporary": 2, "error": 3}
    for account in accounts:
        for group_id in account.group_ids:
            counts = mutable.get(group_id)
            if counts is None:
                continue
            counts[0] += 1
            index = bucket_index.get(account.bucket)
            if index is not None:
                counts[index] += 1

    return [
        GroupAccountCounts(
            group_id=group.group_id,
            name=group.name,
            total_count=mutable[group.group_id][0],
            available_count=mutable[group.group_id][1],
            temporary_unavailable_count=mutable[group.group_id][2],
            error_count=mutable[group.group_id][3],
        )
        for group in group_list
    ]


def build_channel_probes(
    channels: Iterable[ChannelHealth],
    groups: Iterable[GroupAccountCounts],
    *,
    group_ids_by_monitor: Mapping[str, str] | None = None,
) -> list[ChannelProbe]:
    group_list = list(groups)
    group_by_id = {group.group_id: group for group in group_list}
    group_lookup: dict[str, GroupAccountCounts | None] = {}
    semantic_lookup: dict[str, GroupAccountCounts | None] = {}
    for group in group_list:
        key = group.name.casefold()
        group_lookup[key] = None if key in group_lookup else group
        semantic_key = _semantic_group_key(group.name)
        if semantic_key:
            semantic_lookup[semantic_key] = (
                None if semantic_key in semantic_lookup else group
            )

    probes: list[ChannelProbe] = []
    for channel in channels:
        if not channel.enabled:
            continue
        resolved_group_id = (group_ids_by_monitor or {}).get(channel.monitor_id)
        if resolved_group_id is not None:
            probes.append(
                ChannelProbe(
                    channel=channel,
                    accounts=group_by_id.get(resolved_group_id),
                )
            )
            continue
        accounts = None
        candidates: list[str] = []
        for raw_name in (channel.group_name, channel.name):
            if not raw_name:
                continue
            candidates.append(raw_name)
            without_rate = _RATE_PREFIX_PATTERN.sub("", raw_name).strip()
            if without_rate and without_rate != raw_name:
                candidates.append(without_rate)
        for candidate in candidates:
            accounts = group_lookup.get(candidate.casefold())
            if accounts is not None:
                break
        if accounts is None:
            for candidate in candidates:
                semantic_key = _semantic_group_key(candidate)
                if not semantic_key:
                    continue
                accounts = semantic_lookup.get(semantic_key)
                if accounts is not None:
                    break
        probes.append(ChannelProbe(channel=channel, accounts=accounts))
    probes.sort(key=lambda probe: (probe.channel.name.casefold(), probe.channel.monitor_id))
    return probes


def resolve_channel_group_ids(
    channels: Iterable[ChannelHealth],
    records: Iterable[ProbeUsageRecord],
) -> dict[str, str]:
    record_list = list(records)
    bindings: dict[str, str] = {}
    for channel in channels:
        if channel.latency_ms is None or channel.latency_ms < 0 or not channel.model:
            continue
        try:
            checked_at = datetime.fromisoformat(
                channel.last_checked_at.replace("Z", "+00:00")
            )
        except ValueError:
            continue
        if checked_at.tzinfo is None:
            continue
        best_by_group: dict[str, int] = {}
        for record in record_list:
            if (
                record.model != channel.model
                or "go-http-client" not in record.user_agent.casefold()
                or record.created_at.tzinfo is None
                or record.group_id is None
                or not record.group_id.isdigit()
                or int(record.group_id) <= 0
                or record.duration_ms is None
            ):
                continue
            offset_ms = int((record.created_at - checked_at).total_seconds() * 1000)
            if not 0 <= offset_ms <= 2000:
                continue
            duration_difference = abs(record.duration_ms - channel.latency_ms)
            threshold = max(1000, channel.latency_ms // 4)
            if duration_difference > threshold:
                continue
            current = best_by_group.get(record.group_id)
            if current is None or duration_difference < current:
                best_by_group[record.group_id] = duration_difference
        ranked = sorted(best_by_group.items(), key=lambda item: (item[1], int(item[0])))
        if not ranked:
            continue
        if len(ranked) > 1 and ranked[1][1] - ranked[0][1] <= 100:
            continue
        bindings[channel.monitor_id] = ranked[0][0]
    return bindings


def parse_channel_monitor_page(
    payload: Any,
    *,
    expected_page: int,
) -> tuple[list[ChannelHealth], int, int]:
    channels = parse_channel_monitors(payload)
    data = payload["data"]
    pagination_fields = ("total", "page", "page_size", "pages")
    if not any(field in data for field in pagination_fields):
        return channels, 1, max(1, len(channels))
    if not all(field in data for field in pagination_fields):
        raise MonitorDataError("incomplete channel monitor pagination")

    total = _non_negative_int(data.get("total"), "channel monitor total")
    page = _non_negative_int(data.get("page"), "channel monitor page")
    page_size = _non_negative_int(data.get("page_size"), "channel monitor page_size")
    pages = _non_negative_int(data.get("pages"), "channel monitor pages")
    expected_pages = max(1, (total + page_size - 1) // page_size) if page_size else 0
    if (
        page != expected_page
        or page < 1
        or page_size < 1
        or pages != expected_pages
        or page > pages
        or len(channels) > page_size
        or len(channels) > total
    ):
        raise MonitorDataError("invalid channel monitor pagination")
    return channels, pages, page_size


def format_trigger_time(triggered_at: datetime | None = None) -> str:
    """Format one trigger timestamp in the operator-facing Beijing timezone."""

    current = triggered_at or datetime.now(UTC)
    if current.tzinfo is None:
        raise ValueError("triggered_at must be timezone-aware")
    local = current.astimezone(ZoneInfo("Asia/Shanghai"))
    return f"触发时间：{local:%Y-%m-%d %H:%M:%S}（北京时间）"


def format_status_report(
    probes: Iterable[ChannelProbe],
    *,
    triggered_at: datetime | None = None,
) -> str:
    probe_list = list(probes)
    trigger_line = format_trigger_time(triggered_at)
    if not probe_list:
        return f"📊 渠道监控\n{trigger_line}\n暂无启用的渠道探测结果。"

    status_labels = {
        "operational": ("✅", "正常"),
        "degraded": ("⚠️", "降级"),
        "failed": ("❌", "失败"),
        "error": ("❌", "异常"),
        "unknown": ("❔", "未知"),
    }
    normal_count = sum(
        probe.channel.status == "operational" for probe in probe_list
    )
    blocks = [
        f"📊 渠道监控｜共 {len(probe_list)} 个｜正常 {normal_count}｜异常 {len(probe_list) - normal_count}\n"
        f"{trigger_line}"
    ]
    for probe in probe_list:
        channel = probe.channel
        latency = "--" if channel.latency_ms is None else f"{channel.latency_ms}ms"
        availability = (
            "--"
            if channel.availability_7d is None
            else f"{channel.availability_7d:.1f}%"
        )
        icon, status_label = status_labels.get(channel.status, ("❔", "未知"))
        if probe.accounts is None:
            group_line = "分组：未关联账号组"
            account_line = "账号：暂无可核对的分组数据"
        else:
            group_line = f"分组：{probe.accounts.name} (#{probe.accounts.group_id})"
            account_line = (
                f"账号：可用 {probe.accounts.available_count}｜错误 {probe.accounts.error_count}｜"
                f"临时不可调度 {probe.accounts.temporary_unavailable_count}｜关闭 {probe.accounts.closed_count}"
            )
        blocks.append(
            f"{icon} {channel.name}\n"
            f"状态：{status_label}｜延迟：{latency}｜7日可用率：{availability}\n"
            f"探测：{channel.provider or '--'}｜{channel.model or '--'}\n"
            f"{group_line}\n"
            f"{account_line}"
        )
    return "\n\n".join(blocks)
