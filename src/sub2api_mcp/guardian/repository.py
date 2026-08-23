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
    GuardianHealth,
    GuardianPolicy,
    GuardianSample,
    ManualControl,
)

GUARDIAN_SCHEMA_VERSION = 1

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
    message TEXT NOT NULL
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
        with self._connect() as connection:
            connection.executescript(GUARDIAN_SCHEMA_SQL)
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
                "SELECT * FROM guardian_channels WHERE channel_id = ?", (channel_id,)
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
            conditions.append("group_id = ?")
            params.append(group_id)
        if health:
            conditions.append("health = ?")
            params.append(health)
        if query:
            conditions.append("(name LIKE ? OR channel_id LIKE ?)")
            pattern = f"%{query[:100]}%"
            params.extend((pattern, pattern))
        if cursor:
            channel_cursor, _ = _decode_cursor(cursor)
            conditions.append("channel_id > ?")
            params.append(channel_cursor)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit + 1)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM guardian_channels {where} ORDER BY channel_id LIMIT ?",
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
                "SELECT COALESCE(group_id, 'ungrouped') AS group_id, COUNT(*) AS channel_count, "
                "SUM(CASE WHEN desired_schedulable = 1 THEN 1 ELSE 0 END) AS available_count, "
                "AVG(score) AS score, AVG(latency_ms) AS latency_ms "
                "FROM guardian_channels GROUP BY COALESCE(group_id, 'ungrouped') "
                "ORDER BY group_id"
            ).fetchall()
        return [
            {
                "group_id": row["group_id"],
                "name": f"分组 {row['group_id']}",
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
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE guardian_channels SET manual_control = ?, updated_at = ? "
                "WHERE channel_id = ?",
                (parsed.value, _iso(self._clock()), channel_id),
            )
            if updated.rowcount != 1:
                raise ServiceError("CHANNEL_NOT_FOUND", "The Guardian channel does not exist")
            row = connection.execute(
                "SELECT * FROM guardian_channels WHERE channel_id = ?", (channel_id,)
            ).fetchone()
        assert row is not None
        return self._channel(row)

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

    async def list_samples(self, channel_id: str, *, limit: int = 60) -> list[GuardianSample]:
        return await asyncio.to_thread(self._list_samples_sync, channel_id, limit)

    def _list_samples_sync(self, channel_id: str, limit: int) -> list[GuardianSample]:
        if not 1 <= limit <= 1000:
            raise ServiceError("INVALID_PAGE_SIZE", "Sample size must be between 1 and 1000")
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
        channels = await self.list_channels(limit=200)
        items = cast(list[dict[str, Any]], channels["items"])
        groups = await self.list_groups()
        runs = await self.list_runs(limit=1)
        policy = await self.get_policy()
        by_health: dict[str, int] = {}
        for item in items:
            health = cast(str, item["health"])
            by_health[health] = by_health.get(health, 0) + 1
        return {
            "observe_only": policy.observe_only,
            "policy_revision": policy.revision,
            "channel_count": len(items),
            "group_count": len(groups),
            "health_counts": by_health,
            "last_run": runs[0] if runs else None,
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

    @staticmethod
    def _channel(row: sqlite3.Row) -> dict[str, Any]:
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
