"""Durable Guardian policy, channel, sample, run, and event persistence."""

from __future__ import annotations

import asyncio
import base64
import json
import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from ..errors import ServiceError
from .contracts import (
    ChannelPolicyOverride,
    GuardianHealth,
    GuardianPolicy,
    GuardianSample,
    ManualControl,
)

GUARDIAN_SCHEMA_VERSION = 2

GUARDIAN_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS guardian_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_policy (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    policy_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_group_overrides (
    group_id TEXT PRIMARY KEY,
    policy_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_channel_overrides (
    channel_id TEXT PRIMARY KEY,
    override_json TEXT NOT NULL,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_channels (
    channel_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    group_id TEXT,
    upstream_status TEXT NOT NULL,
    upstream_schedulable INTEGER NOT NULL,
    health TEXT NOT NULL,
    score REAL NOT NULL,
    latency_ms INTEGER,
    desired_schedulable INTEGER NOT NULL,
    manual_control TEXT NOT NULL,
    details_json TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0,
    freshness_state TEXT NOT NULL DEFAULT 'EXPIRED',
    last_evidence_at TEXT,
    warmup_buckets INTEGER NOT NULL DEFAULT 0,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_guardian_channels_group
    ON guardian_channels(group_id, health, channel_id);
CREATE TABLE IF NOT EXISTS guardian_samples (
    sample_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    score INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    ttfb_ms INTEGER,
    status_code INTEGER,
    message TEXT NOT NULL,
    source_event_id TEXT,
    bucket_at TEXT,
    reliability REAL NOT NULL DEFAULT 1.0,
    ingested_at TEXT,
    legacy INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_guardian_samples_channel
    ON guardian_samples(channel_id, occurred_at DESC, sample_id DESC);
CREATE TABLE IF NOT EXISTS guardian_runs (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    dry_run INTEGER NOT NULL,
    status TEXT NOT NULL,
    result_json TEXT,
    error_code TEXT,
    error_message TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_guardian_runs_started
    ON guardian_runs(started_at DESC, run_id DESC);
CREATE TABLE IF NOT EXISTS guardian_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    channel_id TEXT,
    group_id TEXT,
    message TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_guardian_events_created
    ON guardian_events(created_at DESC, event_id DESC);
CREATE TABLE IF NOT EXISTS guardian_write_audits (
    audit_id TEXT PRIMARY KEY,
    run_id TEXT,
    channel_id TEXT,
    action TEXT NOT NULL,
    before_json TEXT,
    after_json TEXT,
    reason TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    outcome TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_probe_ledger (
    ledger_id TEXT PRIMARY KEY,
    channel_id TEXT,
    model TEXT NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    estimated_cost REAL,
    priced INTEGER NOT NULL,
    budget_date TEXT,
    request_source TEXT,
    blocked_reason TEXT,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_leases (
    lease_key TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_original_config (
    channel_id TEXT PRIMARY KEY,
    config_json TEXT NOT NULL,
    captured_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS guardian_idempotency (
    idempotency_key TEXT NOT NULL,
    action TEXT NOT NULL,
    subject TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(idempotency_key, action, subject)
);
CREATE TABLE IF NOT EXISTS guardian_input_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    consumed_at TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_guardian_input_snapshots_claim
    ON guardian_input_snapshots(consumed_at, captured_at, snapshot_id);
CREATE TABLE IF NOT EXISTS guardian_traffic_buckets (
    channel_id TEXT NOT NULL,
    bucket_at TEXT NOT NULL,
    event_count INTEGER NOT NULL,
    score_sum REAL NOT NULL,
    ttfb_p95_ms INTEGER,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(channel_id, bucket_at)
);
CREATE INDEX IF NOT EXISTS idx_guardian_traffic_buckets_recent
    ON guardian_traffic_buckets(bucket_at DESC, channel_id);
CREATE TABLE IF NOT EXISTS guardian_field_ownership (
    channel_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    owner TEXT NOT NULL,
    baseline_json TEXT,
    last_guardian_json TEXT,
    last_write_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(channel_id, field_name)
);
"""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _cursor(created_at: str, item_id: str) -> str:
    encoded = base64.urlsafe_b64encode(_json([created_at, item_id]).encode()).decode()
    return encoded.rstrip("=")


def _decode_cursor(value: str) -> tuple[str, str]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw_decoded: object = json.loads(base64.urlsafe_b64decode(padded).decode())
        if not isinstance(raw_decoded, list):
            raise ValueError
        decoded = cast(list[object], raw_decoded)
        if len(decoded) != 2:
            raise ValueError
        first, second = decoded
        if not isinstance(first, str) or not isinstance(second, str):
            raise ValueError
        return first, second
    except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise ServiceError("INVALID_CURSOR", "The Guardian cursor is invalid") from exc


class GuardianRepository:
    SCHEMA_VERSION = GUARDIAN_SCHEMA_VERSION

    def __init__(self, path: Path, *, clock: Callable[[], datetime] = _utc_now) -> None:
        self.path = path
        self._clock = clock

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        now = _iso(self._clock())
        default = GuardianPolicy()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for statement in GUARDIAN_SCHEMA_SQL.split(";"):
                if statement.strip():
                    connection.execute(statement)
            row = connection.execute(
                "SELECT value FROM guardian_metadata WHERE key = 'schema_version'"
            ).fetchone()
            current_version = int(row["value"]) if row is not None else 0
            if current_version > self.SCHEMA_VERSION:
                raise RuntimeError("Guardian database schema is newer than this service")
            if current_version < 2:
                self._migrate_v1_to_v2_sync(connection)
            connection.execute(
                "INSERT INTO guardian_metadata(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(GUARDIAN_SCHEMA_VERSION),),
            )
            connection.execute(
                "INSERT OR IGNORE INTO guardian_policy"
                "(singleton, policy_json, revision, updated_at) VALUES(1, ?, 1, ?)",
                (default.model_dump_json(), now),
            )
            connection.execute(
                "UPDATE guardian_runs SET status = 'INTERRUPTED', "
                "error_code = 'SERVICE_RESTARTED', "
                "error_message = 'The service restarted during this Guardian run', "
                "finished_at = ?, updated_at = ? WHERE status = 'RUNNING'",
                (now, now),
            )
            connection.execute("COMMIT")
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    @staticmethod
    def _migrate_v1_to_v2_sync(connection: sqlite3.Connection) -> None:
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_channels",
            "confidence",
            "REAL NOT NULL DEFAULT 0",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_channels",
            "freshness_state",
            "TEXT NOT NULL DEFAULT 'EXPIRED'",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_channels",
            "last_evidence_at",
            "TEXT",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_channels",
            "warmup_buckets",
            "INTEGER NOT NULL DEFAULT 0",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_samples",
            "source_event_id",
            "TEXT",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_samples",
            "bucket_at",
            "TEXT",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_samples",
            "reliability",
            "REAL NOT NULL DEFAULT 1.0",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_samples",
            "ingested_at",
            "TEXT",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_samples",
            "legacy",
            "INTEGER NOT NULL DEFAULT 1",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_probe_ledger",
            "budget_date",
            "TEXT",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_probe_ledger",
            "request_source",
            "TEXT",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_probe_ledger",
            "blocked_reason",
            "TEXT",
        )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_guardian_samples_source_event "
            "ON guardian_samples(channel_id, source, source_event_id) "
            "WHERE source_event_id IS NOT NULL"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_guardian_samples_bucket "
            "ON guardian_samples(channel_id, bucket_at DESC)"
        )

    @staticmethod
    def _ensure_column_sync(
        connection: sqlite3.Connection,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        columns = {
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    async def get_policy(self) -> GuardianPolicy:
        return await asyncio.to_thread(self._get_policy_sync)

    def _get_policy_sync(self) -> GuardianPolicy:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT policy_json, revision FROM guardian_policy WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise RuntimeError("Guardian repository has not been initialized")
        data = cast(dict[str, Any], json.loads(row["policy_json"]))
        data["revision"] = int(row["revision"])
        return GuardianPolicy.model_validate(data)

    async def update_policy(
        self, policy: GuardianPolicy, *, expected_revision: int
    ) -> GuardianPolicy:
        return await asyncio.to_thread(self._update_policy_sync, policy, expected_revision)

    def _update_policy_sync(self, policy: GuardianPolicy, expected_revision: int) -> GuardianPolicy:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT revision FROM guardian_policy WHERE singleton = 1"
            ).fetchone()
            if row is None:
                raise RuntimeError("Guardian repository has not been initialized")
            if int(row["revision"]) != expected_revision:
                connection.execute("ROLLBACK")
                raise ServiceError(
                    "POLICY_REVISION_CONFLICT",
                    "The Guardian policy was modified by another session",
                )
            saved = policy.model_copy(update={"revision": expected_revision + 1})
            connection.execute(
                "UPDATE guardian_policy SET policy_json = ?, revision = ?, updated_at = ? "
                "WHERE singleton = 1",
                (saved.model_dump_json(), saved.revision, _iso(self._clock())),
            )
            connection.execute("COMMIT")
            return saved
        except ServiceError:
            raise
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    async def upsert_group_override(self, group_id: str, policy: dict[str, Any]) -> dict[str, Any]:
        return await asyncio.to_thread(self._upsert_group_override_sync, group_id, policy)

    def _upsert_group_override_sync(self, group_id: str, policy: dict[str, Any]) -> dict[str, Any]:
        now = _iso(self._clock())
        with self._connect() as connection:
            row = connection.execute(
                "SELECT revision FROM guardian_group_overrides WHERE group_id = ?",
                (group_id,),
            ).fetchone()
            revision = int(row["revision"]) + 1 if row is not None else 1
            connection.execute(
                "INSERT INTO guardian_group_overrides"
                "(group_id, policy_json, revision, updated_at) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(group_id) DO UPDATE SET policy_json = excluded.policy_json, "
                "revision = excluded.revision, updated_at = excluded.updated_at",
                (group_id, _json(policy), revision, now),
            )
        return {"group_id": group_id, "policy": policy, "revision": revision, "updated_at": now}

    async def delete_group_override(self, group_id: str) -> None:
        await asyncio.to_thread(self._delete_group_override_sync, group_id)

    def _delete_group_override_sync(self, group_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM guardian_group_overrides WHERE group_id = ?", (group_id,)
            )

    async def list_group_overrides(self) -> dict[str, dict[str, Any]]:
        return await asyncio.to_thread(self._list_group_overrides_sync)

    def _list_group_overrides_sync(self) -> dict[str, dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_group_overrides ORDER BY group_id"
            ).fetchall()
        return {
            row["group_id"]: {
                "policy": json.loads(row["policy_json"]),
                "revision": int(row["revision"]),
                "updated_at": row["updated_at"],
            }
            for row in rows
        }

    async def upsert_channel_override(
        self, channel_id: str, override: ChannelPolicyOverride
    ) -> ChannelPolicyOverride:
        return await asyncio.to_thread(self._upsert_channel_override_sync, channel_id, override)

    def _upsert_channel_override_sync(
        self, channel_id: str, override: ChannelPolicyOverride
    ) -> ChannelPolicyOverride:
        now = _iso(self._clock())
        with self._connect() as connection:
            row = connection.execute(
                "SELECT revision FROM guardian_channel_overrides WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()
            revision = int(row["revision"]) + 1 if row is not None else 1
            connection.execute(
                "INSERT INTO guardian_channel_overrides"
                "(channel_id, override_json, revision, updated_at) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(channel_id) DO UPDATE SET override_json = excluded.override_json, "
                "revision = excluded.revision, updated_at = excluded.updated_at",
                (channel_id, override.model_dump_json(), revision, now),
            )
        return override

    async def get_channel_override(self, channel_id: str) -> ChannelPolicyOverride | None:
        return await asyncio.to_thread(self._get_channel_override_sync, channel_id)

    def _get_channel_override_sync(self, channel_id: str) -> ChannelPolicyOverride | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT override_json FROM guardian_channel_overrides WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()
        return (
            ChannelPolicyOverride.model_validate_json(row["override_json"])
            if row is not None
            else None
        )

    async def upsert_channel(
        self,
        *,
        channel_id: str,
        name: str,
        group_id: str | None,
        upstream_status: str,
        upstream_schedulable: bool,
        health: GuardianHealth,
        score: float,
        latency_ms: int | None,
        desired_schedulable: bool,
        manual_control: ManualControl,
        details: dict[str, Any],
        seen_at: datetime,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._upsert_channel_sync,
            channel_id,
            name,
            group_id,
            upstream_status,
            upstream_schedulable,
            health,
            score,
            latency_ms,
            desired_schedulable,
            manual_control,
            details,
            seen_at,
        )

    def _upsert_channel_sync(
        self,
        channel_id: str,
        name: str,
        group_id: str | None,
        upstream_status: str,
        upstream_schedulable: bool,
        health: GuardianHealth,
        score: float,
        latency_ms: int | None,
        desired_schedulable: bool,
        manual_control: ManualControl,
        details: dict[str, Any],
        seen_at: datetime,
    ) -> dict[str, Any]:
        now = _iso(self._clock())
        seen = _iso(seen_at)
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT first_seen_at FROM guardian_channels WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()
            first_seen = existing["first_seen_at"] if existing is not None else seen
            connection.execute(
                "INSERT INTO guardian_channels(channel_id, name, group_id, upstream_status, "
                "upstream_schedulable, health, score, latency_ms, desired_schedulable, "
                "manual_control, details_json, first_seen_at, last_seen_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel_id) DO UPDATE SET name = excluded.name, "
                "group_id = excluded.group_id, upstream_status = excluded.upstream_status, "
                "upstream_schedulable = excluded.upstream_schedulable, health = excluded.health, "
                "score = excluded.score, latency_ms = excluded.latency_ms, "
                "desired_schedulable = excluded.desired_schedulable, "
                "manual_control = excluded.manual_control, details_json = excluded.details_json, "
                "last_seen_at = excluded.last_seen_at, updated_at = excluded.updated_at",
                (
                    channel_id,
                    name,
                    group_id,
                    upstream_status,
                    int(upstream_schedulable),
                    health.value,
                    score,
                    latency_ms,
                    int(desired_schedulable),
                    manual_control.value,
                    _json(details),
                    first_seen,
                    seen,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM guardian_channels WHERE channel_id = ?", (channel_id,)
            ).fetchone()
        assert row is not None
        return self._channel(row)

    async def get_channel(self, channel_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_channel_sync, channel_id)

    def _get_channel_sync(self, channel_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT c.*, o.override_json AS channel_override_json "
                "FROM guardian_channels c LEFT JOIN guardian_channel_overrides o "
                "ON o.channel_id = c.channel_id WHERE c.channel_id = ?",
                (channel_id,),
            ).fetchone()
        return self._channel(row) if row is not None else None

    async def list_channels(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        group_id: str | None = None,
        health: str | None = None,
        query: str | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._list_channels_sync, limit, cursor, group_id, health, query
        )

    def _list_channels_sync(
        self,
        limit: int,
        cursor: str | None,
        group_id: str | None,
        health: str | None,
        query: str | None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 200:
            raise ServiceError("INVALID_PAGE_SIZE", "Page size must be between 1 and 200")
        conditions: list[str] = []
        params: list[object] = []
        if group_id:
            conditions.append("c.group_id = ?")
            params.append(group_id)
        if health:
            conditions.append("c.health = ?")
            params.append(health)
        if query:
            conditions.append("(c.name LIKE ? OR c.channel_id LIKE ?)")
            pattern = f"%{query[:100]}%"
            params.extend((pattern, pattern))
        if cursor:
            channel_cursor, _ = _decode_cursor(cursor)
            conditions.append("c.channel_id > ?")
            params.append(channel_cursor)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit + 1)
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT c.*, o.override_json AS channel_override_json "
                "FROM guardian_channels c LEFT JOIN guardian_channel_overrides o "
                f"ON o.channel_id = c.channel_id {where} "
                "ORDER BY c.channel_id LIMIT ?",
                params,
            ).fetchall()
        selected = rows[:limit]
        next_cursor = (
            _cursor(selected[-1]["channel_id"], selected[-1]["channel_id"])
            if len(rows) > limit and selected
            else None
        )
        return {
            "items": [self._channel(row) for row in selected],
            "next_cursor": next_cursor,
        }

    async def list_groups(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_groups_sync)

    def _list_groups_sync(self) -> list[dict[str, Any]]:
        overrides = self._list_group_overrides_sync()
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT COALESCE(group_id, 'ungrouped') AS group_id, "
                "MAX(json_extract(details_json, '$.group_name')) AS group_name, "
                "COUNT(*) AS channel_count, "
                "SUM(CASE WHEN desired_schedulable = 1 THEN 1 ELSE 0 END) AS available_count, "
                "AVG(score) AS score, AVG(latency_ms) AS latency_ms "
                "FROM guardian_channels GROUP BY COALESCE(group_id, 'ungrouped') "
                "ORDER BY group_id"
            ).fetchall()
        return [
            {
                "group_id": row["group_id"],
                "name": row["group_name"] or f"分组 {row['group_id']}",
                "channel_count": int(row["channel_count"]),
                "available_count": int(row["available_count"] or 0),
                "score": round(float(row["score"] or 0), 6),
                "latency_ms": (
                    round(float(row["latency_ms"]), 3) if row["latency_ms"] is not None else None
                ),
                "override": overrides.get(row["group_id"]),
            }
            for row in rows
        ]

    async def set_manual_control(
        self, channel_id: str, control: ManualControl | str
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._set_manual_control_sync, channel_id, control)

    def _set_manual_control_sync(
        self, channel_id: str, control: ManualControl | str
    ) -> dict[str, Any]:
        parsed = ManualControl(control)
        health_by_control = {
            ManualControl.NONE: GuardianHealth.PENDING,
            ManualControl.PAUSED: GuardianHealth.MANUALLY_PAUSED,
            ManualControl.EXCLUDED: GuardianHealth.EXCLUDED,
            ManualControl.FUSED: GuardianHealth.FUSED,
        }
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE guardian_channels SET manual_control = ?, health = ?, "
                "desired_schedulable = CASE WHEN ? = 'NONE' "
                "THEN upstream_schedulable ELSE 0 END, updated_at = ? "
                "WHERE channel_id = ?",
                (
                    parsed.value,
                    health_by_control[parsed].value,
                    parsed.value,
                    _iso(self._clock()),
                    channel_id,
                ),
            )
            if updated.rowcount != 1:
                raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
            row = connection.execute(
                "SELECT * FROM guardian_channels WHERE channel_id = ?", (channel_id,)
            ).fetchone()
        assert row is not None
        return self._channel(row)

    async def merge_channel_details(
        self, channel_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._merge_channel_details_sync, channel_id, updates)

    def _merge_channel_details_sync(
        self, channel_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT details_json FROM guardian_channels WHERE channel_id = ?",
                (channel_id,),
            ).fetchone()
            if row is None:
                raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
            details = cast(dict[str, Any], json.loads(row["details_json"]))
            details.update(updates)
            connection.execute(
                "UPDATE guardian_channels SET details_json = ?, updated_at = ? "
                "WHERE channel_id = ?",
                (_json(details), _iso(self._clock()), channel_id),
            )
            saved = connection.execute(
                "SELECT c.*, o.override_json AS channel_override_json "
                "FROM guardian_channels c LEFT JOIN guardian_channel_overrides o "
                "ON o.channel_id = c.channel_id WHERE c.channel_id = ?",
                (channel_id,),
            ).fetchone()
        assert saved is not None
        return self._channel(saved)

    async def append_sample(self, sample: GuardianSample) -> None:
        await asyncio.to_thread(self._append_sample_sync, sample)

    def _append_sample_sync(self, sample: GuardianSample) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO guardian_samples(sample_id, channel_id, source, event_type, "
                "score, occurred_at, ttfb_ms, status_code, message) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    sample.channel_id,
                    sample.source.value,
                    sample.event_type.value,
                    sample.score,
                    _iso(sample.occurred_at),
                    sample.ttfb_ms,
                    sample.status_code,
                    sample.message,
                ),
            )
            connection.execute(
                "DELETE FROM guardian_samples WHERE channel_id = ? AND sample_id NOT IN "
                "(SELECT sample_id FROM guardian_samples WHERE channel_id = ? "
                "ORDER BY occurred_at DESC, sample_id DESC LIMIT 10000)",
                (sample.channel_id, sample.channel_id),
            )

    async def list_samples(self, channel_id: str, *, limit: int = 60) -> list[GuardianSample]:
        return await asyncio.to_thread(self._list_samples_sync, channel_id, limit)

    def _list_samples_sync(self, channel_id: str, limit: int) -> list[GuardianSample]:
        if not 1 <= limit <= 10_000:
            raise ServiceError("INVALID_PAGE_SIZE", "Sample size must be between 1 and 10000")
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_samples WHERE channel_id = ? "
                "ORDER BY occurred_at DESC, sample_id DESC LIMIT ?",
                (channel_id, limit),
            ).fetchall()
        samples: list[GuardianSample] = []
        for row in rows:
            occurred_at = _dt(row["occurred_at"])
            assert occurred_at is not None
            samples.append(
                GuardianSample(
                    channel_id=row["channel_id"],
                    source=row["source"],
                    event_type=row["event_type"],
                    score=int(row["score"]),
                    occurred_at=occurred_at,
                    ttfb_ms=row["ttfb_ms"],
                    status_code=row["status_code"],
                    message=row["message"],
                )
            )
        return samples

    async def create_run(
        self, *, dry_run: bool, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._create_run_sync, dry_run, idempotency_key)

    def _create_run_sync(self, dry_run: bool, idempotency_key: str | None) -> dict[str, Any]:
        key = idempotency_key.strip()[:128] if idempotency_key else None
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            if key:
                existing = connection.execute(
                    "SELECT * FROM guardian_runs WHERE idempotency_key = ?", (key,)
                ).fetchone()
                if existing is not None:
                    connection.execute("COMMIT")
                    result = self._run(existing)
                    result["created"] = False
                    return result
            now = _iso(self._clock())
            run_id = str(uuid.uuid4())
            connection.execute(
                "INSERT INTO guardian_runs(run_id, idempotency_key, dry_run, status, "
                "started_at, updated_at) VALUES(?, ?, ?, 'RUNNING', ?, ?)",
                (run_id, key, int(dry_run), now, now),
            )
            row = connection.execute(
                "SELECT * FROM guardian_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert row is not None
        result = self._run(row)
        result["created"] = True
        return result

    async def finish_run(
        self,
        run_id: str,
        *,
        status: str,
        result: dict[str, Any] | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._finish_run_sync,
            run_id,
            status,
            result,
            error_code,
            error_message,
        )

    def _finish_run_sync(
        self,
        run_id: str,
        status: str,
        result: dict[str, Any] | None,
        error_code: str | None,
        error_message: str | None,
    ) -> dict[str, Any]:
        if status not in {"SUCCEEDED", "FAILED", "CANCELLED", "INTERRUPTED"}:
            raise ValueError("invalid Guardian run terminal status")
        now = _iso(self._clock())
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE guardian_runs SET status = ?, result_json = ?, error_code = ?, "
                "error_message = ?, finished_at = ?, updated_at = ? "
                "WHERE run_id = ? AND status = 'RUNNING'",
                (
                    status,
                    _json(result) if result is not None else None,
                    error_code,
                    error_message,
                    now,
                    now,
                    run_id,
                ),
            )
            if updated.rowcount != 1:
                raise ServiceError("INVALID_RUN_STATE", "The Guardian run is not running")
            row = connection.execute(
                "SELECT * FROM guardian_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        assert row is not None
        return self._run(row)

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_run_sync, run_id)

    def _get_run_sync(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM guardian_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        return self._run(row) if row is not None else None

    async def list_runs(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_runs_sync, limit)

    def _list_runs_sync(self, limit: int) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_runs ORDER BY started_at DESC, run_id DESC LIMIT ?",
                (max(1, min(limit, 100)),),
            ).fetchall()
        return [self._run(row) for row in rows]

    async def cancel_run(self, run_id: str) -> dict[str, Any]:
        return await asyncio.to_thread(self._cancel_run_sync, run_id)

    def _cancel_run_sync(self, run_id: str) -> dict[str, Any]:
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE guardian_runs SET cancel_requested = 1, updated_at = ? "
                "WHERE run_id = ? AND status = 'RUNNING'",
                (_iso(self._clock()), run_id),
            )
            if updated.rowcount != 1:
                raise ServiceError("INVALID_RUN_STATE", "The Guardian run is not running")
            row = connection.execute(
                "SELECT * FROM guardian_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
        assert row is not None
        return self._run(row)

    async def add_event(
        self,
        *,
        event_type: str,
        severity: str,
        message: str,
        channel_id: str | None = None,
        group_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._add_event_sync,
            event_type,
            severity,
            message,
            channel_id,
            group_id,
            details or {},
        )

    def _add_event_sync(
        self,
        event_type: str,
        severity: str,
        message: str,
        channel_id: str | None,
        group_id: str | None,
        details: dict[str, Any],
    ) -> dict[str, Any]:
        event_id = str(uuid.uuid4())
        now = _iso(self._clock())
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO guardian_events(event_id, event_type, severity, channel_id, "
                "group_id, message, details_json, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event_id,
                    event_type[:64],
                    severity[:16],
                    channel_id,
                    group_id,
                    message[:1000],
                    _json(details),
                    now,
                ),
            )
            connection.execute(
                "DELETE FROM guardian_events WHERE event_id NOT IN "
                "(SELECT event_id FROM guardian_events "
                "ORDER BY created_at DESC, event_id DESC LIMIT 100000)"
            )
        return {
            "event_id": event_id,
            "event_type": event_type[:64],
            "severity": severity[:16],
            "channel_id": channel_id,
            "group_id": group_id,
            "message": message[:1000],
            "details": details,
            "created_at": now,
        }

    async def list_events(
        self,
        *,
        limit: int = 50,
        cursor: str | None = None,
        event_type: str | None = None,
        severity: str | None = None,
    ) -> dict[str, Any]:
        return await asyncio.to_thread(self._list_events_sync, limit, cursor, event_type, severity)

    def _list_events_sync(
        self,
        limit: int,
        cursor: str | None,
        event_type: str | None,
        severity: str | None,
    ) -> dict[str, Any]:
        if not 1 <= limit <= 200:
            raise ServiceError("INVALID_PAGE_SIZE", "Page size must be between 1 and 200")
        conditions: list[str] = []
        params: list[object] = []
        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type[:64])
        if severity:
            conditions.append("severity = ?")
            params.append(severity[:16])
        if cursor:
            created_at, event_id = _decode_cursor(cursor)
            conditions.append("(created_at < ? OR (created_at = ? AND event_id < ?))")
            params.extend((created_at, created_at, event_id))
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit + 1)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM guardian_events {where} "
                "ORDER BY created_at DESC, event_id DESC LIMIT ?",
                params,
            ).fetchall()
        selected = rows[:limit]
        return {
            "items": [self._event(row) for row in selected],
            "next_cursor": (
                _cursor(selected[-1]["created_at"], selected[-1]["event_id"])
                if len(rows) > limit and selected
                else None
            ),
        }

    async def acquire_lease(self, lease_key: str, owner: str, *, seconds: int) -> bool:
        return await asyncio.to_thread(self._acquire_lease_sync, lease_key, owner, seconds)

    def _acquire_lease_sync(self, lease_key: str, owner: str, seconds: int) -> bool:
        now = self._clock().astimezone(UTC)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT owner, expires_at FROM guardian_leases WHERE lease_key = ?",
                (lease_key,),
            ).fetchone()
            now_text = _iso(now)
            if row is not None and row["owner"] != owner and row["expires_at"] > now_text:
                connection.execute("COMMIT")
                return False
            connection.execute(
                "INSERT INTO guardian_leases(lease_key, owner, expires_at) VALUES(?, ?, ?) "
                "ON CONFLICT(lease_key) DO UPDATE SET owner = excluded.owner, "
                "expires_at = excluded.expires_at",
                (lease_key, owner, _iso(now + timedelta(seconds=seconds))),
            )
            connection.execute("COMMIT")
            return True
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    async def release_lease(self, lease_key: str, owner: str) -> None:
        await asyncio.to_thread(self._release_lease_sync, lease_key, owner)

    def _release_lease_sync(self, lease_key: str, owner: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM guardian_leases WHERE lease_key = ? AND owner = ?",
                (lease_key, owner),
            )

    async def overview(self) -> dict[str, Any]:
        counts = await asyncio.to_thread(self._overview_counts_sync)
        runs = await self.list_runs(limit=1)
        policy = await self.get_policy()
        return {
            "observe_only": policy.observe_only,
            "policy_revision": policy.revision,
            "channel_count": counts["channel_count"],
            "group_count": counts["group_count"],
            "health_counts": counts["health_counts"],
            "last_run": runs[0] if runs else None,
        }

    def _overview_counts_sync(self) -> dict[str, Any]:
        with self._connect() as connection:
            totals = connection.execute(
                "SELECT COUNT(*) AS channel_count, "
                "COUNT(DISTINCT COALESCE(group_id, 'ungrouped')) AS group_count "
                "FROM guardian_channels"
            ).fetchone()
            rows = connection.execute(
                "SELECT health, COUNT(*) AS count FROM guardian_channels GROUP BY health"
            ).fetchall()
        return {
            "channel_count": int(totals["channel_count"] if totals else 0),
            "group_count": int(totals["group_count"] if totals else 0),
            "health_counts": {str(row["health"]): int(row["count"]) for row in rows},
        }

    async def probe_spend(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._probe_spend_sync)

    def _probe_spend_sync(self) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS probes, SUM(estimated_cost) AS cost, "
                "SUM(CASE WHEN priced = 0 THEN 1 ELSE 0 END) AS unpriced "
                "FROM guardian_probe_ledger"
            ).fetchone()
        return {
            "probe_count": int(row["probes"] or 0),
            "estimated_cost": float(row["cost"] or 0),
            "unpriced_count": int(row["unpriced"] or 0),
            "currency": "USD",
        }

    async def restore_preview(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._restore_preview_sync)

    def _restore_preview_sync(self) -> dict[str, Any]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT channel_id, config_json, captured_at FROM guardian_original_config "
                "ORDER BY channel_id"
            ).fetchall()
        return {
            "items": [
                {
                    "channel_id": row["channel_id"],
                    "original_config": json.loads(row["config_json"]),
                    "captured_at": row["captured_at"],
                }
                for row in rows
            ],
            "executable": False,
            "reason": "writeback_adapter_not_enabled",
        }

    async def get_idempotent_result(
        self, idempotency_key: str, action: str, subject: str | None
    ) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            self._get_idempotent_result_sync,
            idempotency_key,
            action,
            subject,
        )

    def _get_idempotent_result_sync(
        self, idempotency_key: str, action: str, subject: str | None
    ) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT result_json FROM guardian_idempotency "
                "WHERE idempotency_key = ? AND action = ? AND subject = ?",
                (idempotency_key, action, subject or ""),
            ).fetchone()
        return cast(dict[str, Any], json.loads(row["result_json"])) if row is not None else None

    async def save_idempotent_result(
        self,
        idempotency_key: str,
        action: str,
        subject: str | None,
        result: dict[str, Any],
    ) -> None:
        await asyncio.to_thread(
            self._save_idempotent_result_sync,
            idempotency_key,
            action,
            subject,
            result,
        )

    def _save_idempotent_result_sync(
        self,
        idempotency_key: str,
        action: str,
        subject: str | None,
        result: dict[str, Any],
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO guardian_idempotency"
                "(idempotency_key, action, subject, result_json, created_at) "
                "VALUES(?, ?, ?, ?, ?)",
                (
                    idempotency_key,
                    action,
                    subject or "",
                    _json(result),
                    _iso(self._clock()),
                ),
            )
            connection.execute(
                "DELETE FROM guardian_idempotency WHERE rowid NOT IN "
                "(SELECT rowid FROM guardian_idempotency "
                "ORDER BY created_at DESC LIMIT 10000)"
            )

    @staticmethod
    def _channel(row: sqlite3.Row) -> dict[str, Any]:
        try:
            override_json = row["channel_override_json"]
        except IndexError:
            override_json = None
        return {
            "channel_id": row["channel_id"],
            "name": row["name"],
            "group_id": row["group_id"],
            "upstream_status": row["upstream_status"],
            "upstream_schedulable": bool(row["upstream_schedulable"]),
            "health": row["health"],
            "score": float(row["score"]),
            "latency_ms": row["latency_ms"],
            "desired_schedulable": bool(row["desired_schedulable"]),
            "manual_control": row["manual_control"],
            "details": json.loads(row["details_json"]),
            "override": json.loads(override_json) if override_json else None,
            "first_seen_at": row["first_seen_at"],
            "last_seen_at": row["last_seen_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _run(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "dry_run": bool(row["dry_run"]),
            "status": row["status"],
            "result": json.loads(row["result_json"]) if row["result_json"] else None,
            "error_code": row["error_code"],
            "error_message": row["error_message"],
            "cancel_requested": bool(row["cancel_requested"]),
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _event(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "severity": row["severity"],
            "channel_id": row["channel_id"],
            "group_id": row["group_id"],
            "message": row["message"],
            "details": json.loads(row["details_json"]),
            "created_at": row["created_at"],
        }
