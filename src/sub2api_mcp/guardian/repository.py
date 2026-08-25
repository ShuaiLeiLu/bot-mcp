"""Durable Guardian policy, channel, sample, run, and event persistence."""

from __future__ import annotations

import asyncio
import base64
import json
import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

from pydantic import ValidationError

from ..errors import ServiceError
from .contracts import (
    AccountRecoveryClassification,
    AccountRecoveryResult,
    AccountRecoveryRunStatus,
    AccountRecoveryRunTrigger,
    ChannelPolicyOverride,
    GuardianAccountObservation,
    GuardianAccountRecoveryRecord,
    GuardianAccountRecoveryRun,
    GuardianChannelErrorEpisode,
    GuardianEvidence,
    GuardianEvidenceBucket,
    GuardianFieldName,
    GuardianFieldOwnership,
    GuardianFreshness,
    GuardianHealth,
    GuardianPolicy,
    GuardianSample,
    GuardianSampleSource,
    ManualControl,
)

GUARDIAN_SCHEMA_VERSION = 4

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
    claim_owner TEXT,
    claim_expires_at TEXT,
    consumed_at TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_guardian_input_snapshots_claim
    ON guardian_input_snapshots(consumed_at, claim_expires_at, captured_at, snapshot_id);
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

GUARDIAN_ACCOUNT_RECOVERY_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS guardian_account_observations (
    snapshot_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    group_ids_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'error', 'disabled', 'inactive')),
    schedulable INTEGER NOT NULL CHECK (schedulable IN (0, 1)),
    expired INTEGER NOT NULL CHECK (expired IN (0, 1)),
    temporary_unavailable INTEGER NOT NULL CHECK (temporary_unavailable IN (0, 1)),
    observed_at TEXT NOT NULL,
    PRIMARY KEY(snapshot_id, account_id)
);
CREATE INDEX IF NOT EXISTS idx_guardian_account_observations_latest
    ON guardian_account_observations(account_id, observed_at DESC, snapshot_id DESC);
CREATE TABLE IF NOT EXISTS guardian_channel_error_episodes (
    episode_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    group_id TEXT,
    opened_snapshot_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'CLOSED')),
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_guardian_channel_error_episode_open
    ON guardian_channel_error_episodes(channel_id) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS idx_guardian_channel_error_episode_recent
    ON guardian_channel_error_episodes(opened_at DESC, episode_id DESC);
CREATE TABLE IF NOT EXISTS guardian_account_recovery_runs (
    run_id TEXT PRIMARY KEY,
    dedup_key TEXT NOT NULL UNIQUE,
    trigger TEXT NOT NULL CHECK (
        trigger IN ('BAD_ACCOUNT_STATE', 'CHANNEL_ERROR', 'MANUAL')
    ),
    snapshot_id TEXT,
    episode_id TEXT,
    policy_revision INTEGER NOT NULL CHECK (policy_revision > 0),
    status TEXT NOT NULL CHECK (
        status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'INTERRUPTED')
    ),
    result_json TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_guardian_account_recovery_runs_recent
    ON guardian_account_recovery_runs(started_at DESC, run_id DESC);
CREATE TABLE IF NOT EXISTS guardian_account_recovery_ledger (
    ledger_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL
        REFERENCES guardian_account_recovery_runs(run_id) ON DELETE CASCADE,
    dedup_key TEXT NOT NULL UNIQUE,
    account_id TEXT NOT NULL,
    channel_id TEXT,
    group_id TEXT,
    classification TEXT NOT NULL CHECK (
        classification IN (
            'AVAILABLE', 'MANUAL_PAUSE', 'UPSTREAM_ERROR',
            'DISABLED', 'SYSTEM_QUARANTINE', 'EXCLUDED'
        )
    ),
    result TEXT NOT NULL CHECK (
        result IN ('ENABLED', 'DISABLED', 'INDETERMINATE', 'SKIPPED')
    ),
    reason TEXT NOT NULL,
    tested INTEGER NOT NULL CHECK (tested IN (0, 1)),
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_guardian_account_recovery_ledger_run
    ON guardian_account_recovery_ledger(run_id, occurred_at, ledger_id);
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


def _snapshot_id(value: str) -> str:
    normalized = value.strip().lower()
    invalid_character = any(
        character not in "0123456789abcdef" for character in normalized
    )
    if len(normalized) != 64 or invalid_character:
        raise ValueError("snapshot ID must be a SHA-256 hex digest")
    return normalized


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
            if current_version < 3:
                self._migrate_v2_to_v3_sync(connection)
            if current_version < 4:
                self._migrate_v3_to_v4_sync(connection)
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
            connection.execute(
                "UPDATE guardian_account_recovery_runs SET status = 'INTERRUPTED', "
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
    def _migrate_v2_to_v3_sync(connection: sqlite3.Connection) -> None:
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_input_snapshots",
            "claim_owner",
            "TEXT",
        )
        GuardianRepository._ensure_column_sync(
            connection,
            "guardian_input_snapshots",
            "claim_expires_at",
            "TEXT",
        )
        connection.execute("DROP INDEX IF EXISTS idx_guardian_input_snapshots_claim")
        connection.execute(
            "CREATE INDEX idx_guardian_input_snapshots_claim "
            "ON guardian_input_snapshots"
            "(consumed_at, claim_expires_at, captured_at, snapshot_id)"
        )

    @staticmethod
    def _migrate_v3_to_v4_sync(connection: sqlite3.Connection) -> None:
        for statement in GUARDIAN_ACCOUNT_RECOVERY_SCHEMA_SQL.split(";"):
            if statement.strip():
                connection.execute(statement)

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

    async def get_field_ownership(
        self,
        channel_id: str,
        field_name: GuardianFieldName,
    ) -> GuardianFieldOwnership | None:
        return await asyncio.to_thread(
            self._get_field_ownership_sync,
            channel_id,
            field_name,
        )

    def _get_field_ownership_sync(
        self,
        channel_id: str,
        field_name: GuardianFieldName,
    ) -> GuardianFieldOwnership | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM guardian_field_ownership "
                "WHERE channel_id = ? AND field_name = ?",
                (channel_id, field_name.value),
            ).fetchone()
        if row is None:
            return None
        return GuardianFieldOwnership(
            channel_id=row["channel_id"],
            field_name=row["field_name"],
            owner=row["owner"],
            baseline_value=(json.loads(row["baseline_json"]) if row["baseline_json"] else None),
            last_guardian_value=(
                json.loads(row["last_guardian_json"])
                if row["last_guardian_json"]
                else None
            ),
            last_write_at=_dt(row["last_write_at"]),
        )

    async def save_field_ownership(self, value: GuardianFieldOwnership) -> None:
        await asyncio.to_thread(self._save_field_ownership_sync, value)

    def _save_field_ownership_sync(self, value: GuardianFieldOwnership) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO guardian_field_ownership"
                "(channel_id, field_name, owner, baseline_json, last_guardian_json, "
                "last_write_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel_id, field_name) DO UPDATE SET "
                "owner = excluded.owner, baseline_json = excluded.baseline_json, "
                "last_guardian_json = excluded.last_guardian_json, "
                "last_write_at = excluded.last_write_at, updated_at = excluded.updated_at",
                (
                    value.channel_id,
                    value.field_name.value,
                    value.owner.value,
                    _json(value.baseline_value),
                    (
                        _json(value.last_guardian_value)
                        if value.last_guardian_value is not None
                        else None
                    ),
                    _iso(value.last_write_at) if value.last_write_at is not None else None,
                    _iso(self._clock()),
                ),
            )

    async def add_write_audit(
        self,
        *,
        channel_id: str,
        action: str,
        before: object,
        after: object,
        reason: str,
        idempotency_key: str,
        outcome: str,
    ) -> None:
        await asyncio.to_thread(
            self._add_write_audit_sync,
            channel_id,
            action,
            before,
            after,
            reason,
            idempotency_key,
            outcome,
        )

    def _add_write_audit_sync(
        self,
        channel_id: str,
        action: str,
        before: object,
        after: object,
        reason: str,
        idempotency_key: str,
        outcome: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO guardian_write_audits"
                "(audit_id, channel_id, action, before_json, after_json, reason, "
                "idempotency_key, outcome, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    channel_id,
                    action[:64],
                    _json(before),
                    _json(after),
                    reason[:500],
                    idempotency_key,
                    outcome[:32],
                    _iso(self._clock()),
                ),
            )

    async def pending_input_snapshot_count(self) -> int:
        return await asyncio.to_thread(self._pending_input_snapshot_count_sync)

    def _pending_input_snapshot_count_sync(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM guardian_input_snapshots "
                "WHERE consumed_at IS NULL"
            ).fetchone()
        return int(row["count"] if row is not None else 0)

    async def shared_sampling_started(self) -> bool:
        return await asyncio.to_thread(self._shared_sampling_started_sync)

    def _shared_sampling_started_sync(self) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM guardian_metadata "
                "WHERE key = 'shared_sampling_started'"
            ).fetchone()
        return bool(row is not None and row["value"] == "true")

    async def claim_input_snapshot(
        self,
        owner: str,
        *,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            self._claim_input_snapshot_sync,
            owner,
            lease_seconds,
        )

    def _claim_input_snapshot_sync(
        self,
        owner: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        if not owner or len(owner) > 200:
            raise ValueError("invalid snapshot claim owner")
        now_value = self._clock().astimezone(UTC)
        now = _iso(now_value)
        expires_at = _iso(now_value + timedelta(seconds=max(1, lease_seconds)))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT snapshot_id FROM guardian_input_snapshots "
                "WHERE consumed_at IS NULL AND "
                "(claim_owner IS NULL OR claim_expires_at <= ?) "
                "ORDER BY captured_at, snapshot_id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            connection.execute(
                "UPDATE guardian_input_snapshots "
                "SET claim_owner = ?, claim_expires_at = ? "
                "WHERE snapshot_id = ? AND consumed_at IS NULL",
                (owner, expires_at, row["snapshot_id"]),
            )
            claimed = connection.execute(
                "SELECT snapshot_id, payload_json, captured_at "
                "FROM guardian_input_snapshots WHERE snapshot_id = ?",
                (row["snapshot_id"],),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert claimed is not None
        payload: object = json.loads(claimed["payload_json"])
        if not isinstance(payload, dict):
            raise RuntimeError("Guardian input snapshot payload is invalid")
        return {
            "snapshot_id": claimed["snapshot_id"],
            "payload": cast(dict[str, Any], payload),
            "captured_at": claimed["captured_at"],
        }

    async def consume_input_snapshot(self, snapshot_id: str, owner: str) -> bool:
        return await asyncio.to_thread(
            self._consume_input_snapshot_sync,
            snapshot_id,
            owner,
        )

    def _consume_input_snapshot_sync(self, snapshot_id: str, owner: str) -> bool:
        with self._connect() as connection:
            result = connection.execute(
                "UPDATE guardian_input_snapshots "
                "SET consumed_at = ?, claim_owner = NULL, claim_expires_at = NULL "
                "WHERE snapshot_id = ? AND claim_owner = ? AND consumed_at IS NULL",
                (_iso(self._clock()), snapshot_id, owner),
            )
        return result.rowcount == 1

    async def release_input_snapshot(self, snapshot_id: str, owner: str) -> None:
        await asyncio.to_thread(self._release_input_snapshot_sync, snapshot_id, owner)

    def _release_input_snapshot_sync(self, snapshot_id: str, owner: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE guardian_input_snapshots "
                "SET claim_owner = NULL, claim_expires_at = NULL "
                "WHERE snapshot_id = ? AND claim_owner = ? AND consumed_at IS NULL",
                (snapshot_id, owner),
            )

    async def upsert_account_observations(
        self,
        *,
        snapshot_id: str,
        observed_at: datetime,
        observations: list[GuardianAccountObservation],
    ) -> int:
        return await asyncio.to_thread(
            self._upsert_account_observations_sync,
            snapshot_id,
            observed_at,
            observations,
        )

    def _upsert_account_observations_sync(
        self,
        snapshot_id: str,
        observed_at: datetime,
        observations: list[GuardianAccountObservation],
    ) -> int:
        normalized_snapshot_id = _snapshot_id(snapshot_id)
        if observed_at.tzinfo is None:
            raise ValueError("account observation time must be timezone-aware")
        if len(observations) > 10_000:
            raise ValueError("account observation count exceeds the safe limit")
        account_ids = [item.account_id for item in observations]
        if len(account_ids) != len(set(account_ids)):
            raise ValueError("account observations contain duplicate account IDs")
        inserted = 0
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for observation in observations:
                existing = connection.execute(
                    "SELECT * FROM guardian_account_observations "
                    "WHERE snapshot_id = ? AND account_id = ?",
                    (normalized_snapshot_id, observation.account_id),
                ).fetchone()
                if existing is not None:
                    if (
                        self._account_observation_from_row(existing) != observation
                        or existing["observed_at"] != _iso(observed_at)
                    ):
                        raise ServiceError(
                            "ACCOUNT_OBSERVATION_CONFLICT",
                            "The account observation changed for the same snapshot",
                        )
                    continue
                connection.execute(
                    "INSERT INTO guardian_account_observations("
                    "snapshot_id, account_id, group_ids_json, status, schedulable, "
                    "expired, temporary_unavailable, observed_at"
                    ") VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        normalized_snapshot_id,
                        observation.account_id,
                        _json(list(observation.group_ids)),
                        observation.status.value,
                        int(observation.schedulable),
                        int(observation.expired),
                        int(observation.temporary_unavailable),
                        _iso(observed_at),
                    ),
                )
                inserted += 1
            connection.execute("COMMIT")
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return inserted

    async def list_account_observations(
        self,
        snapshot_id: str,
    ) -> list[GuardianAccountObservation]:
        return await asyncio.to_thread(
            self._list_account_observations_sync,
            snapshot_id,
        )

    def _list_account_observations_sync(
        self,
        snapshot_id: str,
    ) -> list[GuardianAccountObservation]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_account_observations WHERE snapshot_id = ? "
                "ORDER BY CAST(account_id AS INTEGER)",
                (_snapshot_id(snapshot_id),),
            ).fetchall()
        return [self._account_observation_from_row(row) for row in rows]

    async def latest_abnormal_account_snapshot(self) -> str | None:
        return await asyncio.to_thread(self._latest_abnormal_account_snapshot_sync)

    def _latest_abnormal_account_snapshot_sync(self) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                "WITH latest AS ("
                "SELECT snapshot_id FROM guardian_account_observations "
                "ORDER BY observed_at DESC, snapshot_id DESC LIMIT 1"
                ") SELECT observations.snapshot_id "
                "FROM guardian_account_observations AS observations "
                "JOIN latest ON latest.snapshot_id = observations.snapshot_id "
                "WHERE observations.status IN ('error', 'disabled', 'inactive') "
                "AND observations.expired = 0 "
                "AND observations.temporary_unavailable = 0 LIMIT 1"
            ).fetchone()
        return cast(str, row["snapshot_id"]) if row is not None else None

    async def open_channel_error_episode(
        self,
        *,
        channel_id: str,
        group_id: str | None,
        snapshot_id: str,
        opened_at: datetime,
    ) -> GuardianChannelErrorEpisode:
        return await asyncio.to_thread(
            self._open_channel_error_episode_sync,
            channel_id,
            group_id,
            snapshot_id,
            opened_at,
        )

    def _open_channel_error_episode_sync(
        self,
        channel_id: str,
        group_id: str | None,
        snapshot_id: str,
        opened_at: datetime,
    ) -> GuardianChannelErrorEpisode:
        if not 1 <= len(channel_id) <= 128:
            raise ValueError("channel ID is outside the safe range")
        if group_id is not None and (
            not group_id.isdigit() or int(group_id) <= 0 or len(group_id) > 20
        ):
            raise ValueError("group ID must be a positive decimal identifier")
        if opened_at.tzinfo is None:
            raise ValueError("episode opened_at must be timezone-aware")
        normalized_snapshot_id = _snapshot_id(snapshot_id)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM guardian_channel_error_episodes "
                "WHERE channel_id = ? AND status = 'OPEN'",
                (channel_id,),
            ).fetchone()
            if existing is not None:
                episode = self._channel_error_episode_from_row(existing)
                if episode.group_id != group_id:
                    raise ServiceError(
                        "CHANNEL_ERROR_EPISODE_CONFLICT",
                        "The open channel error episode has a different group mapping",
                    )
                connection.execute("COMMIT")
                return episode
            episode_id = str(uuid.uuid4())
            now = _iso(self._clock())
            connection.execute(
                "INSERT INTO guardian_channel_error_episodes("
                "episode_id, channel_id, group_id, opened_snapshot_id, status, "
                "opened_at, closed_at, updated_at"
                ") VALUES(?, ?, ?, ?, 'OPEN', ?, NULL, ?)",
                (
                    episode_id,
                    channel_id,
                    group_id,
                    normalized_snapshot_id,
                    _iso(opened_at),
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM guardian_channel_error_episodes WHERE episode_id = ?",
                (episode_id,),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert row is not None
        return self._channel_error_episode_from_row(row)

    async def get_open_channel_error_episode(
        self,
        channel_id: str,
    ) -> GuardianChannelErrorEpisode | None:
        return await asyncio.to_thread(
            self._get_open_channel_error_episode_sync,
            channel_id,
        )

    async def latest_open_channel_error_episode(
        self,
    ) -> GuardianChannelErrorEpisode | None:
        return await asyncio.to_thread(self._latest_open_channel_error_episode_sync)

    def _latest_open_channel_error_episode_sync(
        self,
    ) -> GuardianChannelErrorEpisode | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM guardian_channel_error_episodes "
                "WHERE status = 'OPEN' "
                "ORDER BY opened_at DESC, episode_id DESC LIMIT 1"
            ).fetchone()
        return self._channel_error_episode_from_row(row) if row is not None else None

    def _get_open_channel_error_episode_sync(
        self,
        channel_id: str,
    ) -> GuardianChannelErrorEpisode | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM guardian_channel_error_episodes "
                "WHERE channel_id = ? AND status = 'OPEN'",
                (channel_id,),
            ).fetchone()
        return self._channel_error_episode_from_row(row) if row is not None else None

    async def close_channel_error_episode(
        self,
        channel_id: str,
        *,
        closed_at: datetime,
    ) -> bool:
        return await asyncio.to_thread(
            self._close_channel_error_episode_sync,
            channel_id,
            closed_at,
        )

    def _close_channel_error_episode_sync(
        self,
        channel_id: str,
        closed_at: datetime,
    ) -> bool:
        if closed_at.tzinfo is None:
            raise ValueError("episode closed_at must be timezone-aware")
        with self._connect() as connection:
            result = connection.execute(
                "UPDATE guardian_channel_error_episodes SET status = 'CLOSED', "
                "closed_at = ?, updated_at = ? "
                "WHERE channel_id = ? AND status = 'OPEN'",
                (_iso(closed_at), _iso(self._clock()), channel_id),
            )
        return result.rowcount == 1

    async def create_account_recovery_run(
        self,
        *,
        dedup_key: str,
        trigger: AccountRecoveryRunTrigger,
        snapshot_id: str | None,
        episode_id: str | None,
        policy_revision: int,
        started_at: datetime,
    ) -> GuardianAccountRecoveryRun:
        return await asyncio.to_thread(
            self._create_account_recovery_run_sync,
            dedup_key,
            trigger,
            snapshot_id,
            episode_id,
            policy_revision,
            started_at,
        )

    def _create_account_recovery_run_sync(
        self,
        dedup_key: str,
        trigger: AccountRecoveryRunTrigger,
        snapshot_id: str | None,
        episode_id: str | None,
        policy_revision: int,
        started_at: datetime,
    ) -> GuardianAccountRecoveryRun:
        if not 1 <= len(dedup_key) <= 512:
            raise ValueError("account recovery dedup key is outside the safe range")
        normalized_snapshot_id = (
            _snapshot_id(snapshot_id) if snapshot_id is not None else None
        )
        if episode_id is not None and not 1 <= len(episode_id) <= 128:
            raise ValueError("episode ID is outside the safe range")
        if policy_revision < 1:
            raise ValueError("policy revision must be positive")
        if started_at.tzinfo is None:
            raise ValueError("account recovery start time must be timezone-aware")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM guardian_account_recovery_runs WHERE dedup_key = ?",
                (dedup_key,),
            ).fetchone()
            if existing is not None:
                connection.execute("COMMIT")
                return self._account_recovery_run_from_row(existing)
            run_id = str(uuid.uuid4())
            connection.execute(
                "INSERT INTO guardian_account_recovery_runs("
                "run_id, dedup_key, trigger, snapshot_id, episode_id, policy_revision, "
                "status, result_json, started_at, finished_at, updated_at"
                ") VALUES(?, ?, ?, ?, ?, ?, 'RUNNING', NULL, ?, NULL, ?)",
                (
                    run_id,
                    dedup_key,
                    trigger.value,
                    normalized_snapshot_id,
                    episode_id,
                    policy_revision,
                    _iso(started_at),
                    _iso(self._clock()),
                ),
            )
            row = connection.execute(
                "SELECT * FROM guardian_account_recovery_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert row is not None
        return self._account_recovery_run_from_row(row)

    async def finish_account_recovery_run(
        self,
        run_id: str,
        *,
        status: str,
        result: dict[str, int],
        finished_at: datetime,
    ) -> GuardianAccountRecoveryRun:
        return await asyncio.to_thread(
            self._finish_account_recovery_run_sync,
            run_id,
            status,
            result,
            finished_at,
        )

    def _finish_account_recovery_run_sync(
        self,
        run_id: str,
        status: str,
        result: dict[str, int],
        finished_at: datetime,
    ) -> GuardianAccountRecoveryRun:
        parsed_status = AccountRecoveryRunStatus(status)
        if parsed_status is AccountRecoveryRunStatus.RUNNING:
            raise ValueError("a finished account recovery run cannot remain running")
        if finished_at.tzinfo is None:
            raise ValueError("account recovery finish time must be timezone-aware")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM guardian_account_recovery_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if existing is None:
                raise ServiceError(
                    "ACCOUNT_RECOVERY_RUN_NOT_FOUND",
                    "The account recovery run does not exist",
                )
            current = self._account_recovery_run_from_row(existing)
            if current.status is not AccountRecoveryRunStatus.RUNNING:
                if current.status is parsed_status and current.result == result:
                    connection.execute("COMMIT")
                    return current
                raise ServiceError(
                    "ACCOUNT_RECOVERY_RUN_CONFLICT",
                    "The account recovery run is already complete",
                )
            connection.execute(
                "UPDATE guardian_account_recovery_runs SET status = ?, result_json = ?, "
                "finished_at = ?, updated_at = ? WHERE run_id = ?",
                (
                    parsed_status.value,
                    _json(result),
                    _iso(finished_at),
                    _iso(self._clock()),
                    run_id,
                ),
            )
            row = connection.execute(
                "SELECT * FROM guardian_account_recovery_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert row is not None
        return self._account_recovery_run_from_row(row)

    async def get_account_recovery_run(
        self,
        run_id: str,
    ) -> GuardianAccountRecoveryRun | None:
        return await asyncio.to_thread(self._get_account_recovery_run_sync, run_id)

    def _get_account_recovery_run_sync(
        self,
        run_id: str,
    ) -> GuardianAccountRecoveryRun | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM guardian_account_recovery_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        return self._account_recovery_run_from_row(row) if row is not None else None

    async def record_account_recovery_result(
        self,
        *,
        run_id: str,
        dedup_key: str,
        account_id: str,
        channel_id: str | None,
        group_id: str | None,
        classification: AccountRecoveryClassification,
        result: AccountRecoveryResult,
        reason: str,
        tested: bool,
        occurred_at: datetime,
    ) -> bool:
        return await asyncio.to_thread(
            self._record_account_recovery_result_sync,
            run_id,
            dedup_key,
            account_id,
            channel_id,
            group_id,
            classification,
            result,
            reason,
            tested,
            occurred_at,
        )

    def _record_account_recovery_result_sync(
        self,
        run_id: str,
        dedup_key: str,
        account_id: str,
        channel_id: str | None,
        group_id: str | None,
        classification: AccountRecoveryClassification,
        result: AccountRecoveryResult,
        reason: str,
        tested: bool,
        occurred_at: datetime,
    ) -> bool:
        record = GuardianAccountRecoveryRecord(
            ledger_id=str(uuid.uuid4()),
            run_id=run_id,
            dedup_key=dedup_key,
            account_id=account_id,
            channel_id=channel_id,
            group_id=group_id,
            classification=classification,
            result=result,
            reason=reason,
            tested=tested,
            occurred_at=occurred_at,
        )
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM guardian_account_recovery_ledger WHERE dedup_key = ?",
                (dedup_key,),
            ).fetchone()
            if existing is not None:
                stored = self._account_recovery_record_from_row(existing)
                if (
                    stored.model_dump(exclude={"ledger_id"})
                    != record.model_dump(exclude={"ledger_id"})
                ):
                    raise ServiceError(
                        "ACCOUNT_RECOVERY_RESULT_CONFLICT",
                        "The account recovery result changed for the same operation",
                    )
                connection.execute("COMMIT")
                return False
            run = connection.execute(
                "SELECT 1 FROM guardian_account_recovery_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run is None:
                raise ServiceError(
                    "ACCOUNT_RECOVERY_RUN_NOT_FOUND",
                    "The account recovery run does not exist",
                )
            connection.execute(
                "INSERT INTO guardian_account_recovery_ledger("
                "ledger_id, run_id, dedup_key, account_id, channel_id, group_id, "
                "classification, result, reason, tested, occurred_at"
                ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.ledger_id,
                    record.run_id,
                    record.dedup_key,
                    record.account_id,
                    record.channel_id,
                    record.group_id,
                    record.classification.value,
                    record.result.value,
                    record.reason,
                    int(record.tested),
                    _iso(record.occurred_at),
                ),
            )
            connection.execute("COMMIT")
        except Exception:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return True

    async def list_account_recovery_results(
        self,
        run_id: str,
    ) -> list[GuardianAccountRecoveryRecord]:
        return await asyncio.to_thread(
            self._list_account_recovery_results_sync,
            run_id,
        )

    def _list_account_recovery_results_sync(
        self,
        run_id: str,
    ) -> list[GuardianAccountRecoveryRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_account_recovery_ledger WHERE run_id = ? "
                "ORDER BY occurred_at, ledger_id",
                (run_id,),
            ).fetchall()
        return [self._account_recovery_record_from_row(row) for row in rows]

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
        confidence: float = 0,
        freshness_state: GuardianFreshness = GuardianFreshness.EXPIRED,
        last_evidence_at: datetime | None = None,
        warmup_buckets: int = 0,
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
            confidence,
            freshness_state,
            last_evidence_at,
            warmup_buckets,
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
        confidence: float,
        freshness_state: GuardianFreshness,
        last_evidence_at: datetime | None,
        warmup_buckets: int,
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
                "manual_control, details_json, confidence, freshness_state, "
                "last_evidence_at, warmup_buckets, first_seen_at, last_seen_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(channel_id) DO UPDATE SET name = excluded.name, "
                "group_id = excluded.group_id, upstream_status = excluded.upstream_status, "
                "upstream_schedulable = excluded.upstream_schedulable, health = excluded.health, "
                "score = excluded.score, latency_ms = excluded.latency_ms, "
                "desired_schedulable = excluded.desired_schedulable, "
                "manual_control = excluded.manual_control, details_json = excluded.details_json, "
                "confidence = excluded.confidence, "
                "freshness_state = excluded.freshness_state, "
                "last_evidence_at = excluded.last_evidence_at, "
                "warmup_buckets = excluded.warmup_buckets, "
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
                    confidence,
                    freshness_state.value,
                    _iso(last_evidence_at) if last_evidence_at is not None else None,
                    warmup_buckets,
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

    async def append_evidence(
        self,
        evidence: GuardianEvidence,
        *,
        bucket_at: datetime,
    ) -> bool:
        return await asyncio.to_thread(
            self._append_evidence_sync,
            evidence,
            bucket_at,
        )

    def _append_evidence_sync(
        self,
        evidence: GuardianEvidence,
        bucket_at: datetime,
    ) -> bool:
        if bucket_at.tzinfo is None:
            raise ValueError("bucket_at must be timezone-aware")
        with self._connect() as connection:
            result = connection.execute(
                "INSERT OR IGNORE INTO guardian_samples"
                "(sample_id, channel_id, source, event_type, score, occurred_at, ttfb_ms, "
                "status_code, message, source_event_id, bucket_at, reliability, ingested_at, "
                "legacy) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
                (
                    str(uuid.uuid4()),
                    evidence.channel_id,
                    evidence.source.value,
                    evidence.event_type.value,
                    evidence.score,
                    _iso(evidence.occurred_at),
                    evidence.ttfb_ms,
                    evidence.status_code,
                    evidence.message,
                    evidence.source_event_id,
                    _iso(bucket_at),
                    evidence.reliability,
                    _iso(self._clock()),
                ),
            )
        return result.rowcount == 1

    async def list_evidence(
        self,
        channel_id: str,
        *,
        since: datetime,
    ) -> list[GuardianEvidence]:
        return await asyncio.to_thread(self._list_evidence_sync, channel_id, since)

    def _list_evidence_sync(
        self,
        channel_id: str,
        since: datetime,
    ) -> list[GuardianEvidence]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_samples WHERE channel_id = ? AND legacy = 0 "
                "AND occurred_at >= ? ORDER BY occurred_at DESC, sample_id DESC",
                (channel_id, _iso(since)),
            ).fetchall()
        evidence: list[GuardianEvidence] = []
        for row in rows:
            occurred_at = _dt(row["occurred_at"])
            assert occurred_at is not None
            evidence.append(
                GuardianEvidence(
                    source_event_id=row["source_event_id"],
                    channel_id=row["channel_id"],
                    source=row["source"],
                    event_type=row["event_type"],
                    score=int(row["score"]),
                    occurred_at=occurred_at,
                    reliability=float(row["reliability"]),
                    event_count=1,
                    ttfb_ms=row["ttfb_ms"],
                    status_code=row["status_code"],
                    message=row["message"],
                )
            )
        return evidence

    async def list_traffic_buckets(
        self,
        channel_id: str,
        *,
        since: datetime,
    ) -> list[GuardianEvidenceBucket]:
        return await asyncio.to_thread(
            self._list_traffic_buckets_sync,
            channel_id,
            since,
        )

    def _list_traffic_buckets_sync(
        self,
        channel_id: str,
        since: datetime,
    ) -> list[GuardianEvidenceBucket]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_traffic_buckets WHERE channel_id = ? "
                "AND bucket_at >= ? ORDER BY bucket_at DESC",
                (channel_id, _iso(since)),
            ).fetchall()
        buckets: list[GuardianEvidenceBucket] = []
        for row in rows:
            bucket_at = _dt(row["bucket_at"])
            assert bucket_at is not None
            event_count = int(row["event_count"])
            buckets.append(
                GuardianEvidenceBucket(
                    channel_id=channel_id,
                    bucket_at=bucket_at,
                    score=float(row["score_sum"]) / event_count,
                    quality=min(1.0, event_count / 5),
                    sources=frozenset({GuardianSampleSource.TRAFFIC}),
                    event_count=event_count,
                    ttfb_p95_ms=row["ttfb_p95_ms"],
                )
            )
        return buckets

    async def upsert_traffic_buckets(
        self,
        buckets: list[GuardianEvidenceBucket],
    ) -> None:
        await asyncio.to_thread(self._upsert_traffic_buckets_sync, buckets)

    def _upsert_traffic_buckets_sync(
        self,
        buckets: list[GuardianEvidenceBucket],
    ) -> None:
        now = _iso(self._clock())
        with self._connect() as connection:
            for bucket in buckets:
                if bucket.sources != frozenset({GuardianSampleSource.TRAFFIC}):
                    raise ValueError("only TRAFFIC buckets can be persisted as traffic")
                connection.execute(
                    "INSERT INTO guardian_traffic_buckets"
                    "(channel_id, bucket_at, event_count, score_sum, ttfb_p95_ms, "
                    "details_json, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(channel_id, bucket_at) DO UPDATE SET "
                    "event_count = excluded.event_count, score_sum = excluded.score_sum, "
                    "ttfb_p95_ms = excluded.ttfb_p95_ms, "
                    "details_json = excluded.details_json, updated_at = excluded.updated_at",
                    (
                        bucket.channel_id,
                        _iso(bucket.bucket_at),
                        bucket.event_count,
                        bucket.score * bucket.event_count,
                        bucket.ttfb_p95_ms,
                        _json({"quality": bucket.quality}),
                        now,
                        now,
                    ),
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

    async def sampling_status(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sampling_status_sync)

    def _sampling_status_sync(self) -> dict[str, Any]:
        with self._connect() as connection:
            snapshots = connection.execute(
                "SELECT COUNT(*) AS total, "
                "COUNT(CASE WHEN consumed_at IS NULL THEN 1 END) AS pending, "
                "MAX(captured_at) AS latest FROM guardian_input_snapshots"
            ).fetchone()
            traffic = connection.execute(
                "SELECT COUNT(*) AS count, MAX(bucket_at) AS latest "
                "FROM guardian_traffic_buckets"
            ).fetchone()
            freshness = connection.execute(
                "SELECT freshness_state, COUNT(*) AS count FROM guardian_channels "
                "GROUP BY freshness_state"
            ).fetchall()
        return {
            "shared_snapshots": int(snapshots["total"] or 0),
            "pending_snapshots": int(snapshots["pending"] or 0),
            "latest_snapshot_at": snapshots["latest"],
            "traffic_buckets": int(traffic["count"] or 0),
            "latest_traffic_bucket_at": traffic["latest"],
            "channels_by_freshness": {
                row["freshness_state"]: int(row["count"]) for row in freshness
            },
        }

    async def list_field_ownership(self) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self._list_field_ownership_sync)

    def _list_field_ownership_sync(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM guardian_field_ownership "
                "ORDER BY channel_id, field_name"
            ).fetchall()
        return [
            {
                "channel_id": row["channel_id"],
                "field_name": row["field_name"],
                "owner": row["owner"],
                "baseline_value": (
                    json.loads(row["baseline_json"]) if row["baseline_json"] else None
                ),
                "last_guardian_value": (
                    json.loads(row["last_guardian_json"])
                    if row["last_guardian_json"]
                    else None
                ),
                "last_write_at": row["last_write_at"],
            }
            for row in rows
        ]

    async def record_recovery_probe(
        self,
        *,
        channel_id: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        estimated_cost: float | None,
        priced: bool,
        occurred_at: datetime,
    ) -> None:
        await asyncio.to_thread(
            self._record_recovery_probe_sync,
            channel_id,
            model,
            input_tokens,
            output_tokens,
            estimated_cost,
            priced,
            occurred_at,
            None,
        )

    async def record_recovery_probe_blocked(
        self,
        *,
        channel_id: str,
        reason: str,
        occurred_at: datetime,
    ) -> None:
        await asyncio.to_thread(
            self._record_recovery_probe_sync,
            channel_id,
            "",
            None,
            None,
            None,
            False,
            occurred_at,
            reason,
        )

    def _record_recovery_probe_sync(
        self,
        channel_id: str,
        model: str,
        input_tokens: int | None,
        output_tokens: int | None,
        estimated_cost: float | None,
        priced: bool,
        occurred_at: datetime,
        blocked_reason: str | None,
    ) -> None:
        if occurred_at.tzinfo is None:
            raise ValueError("probe ledger time must be timezone-aware")
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO guardian_probe_ledger"
                "(ledger_id, channel_id, model, input_tokens, output_tokens, estimated_cost, "
                "priced, budget_date, request_source, blocked_reason, occurred_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'RECOVERY_PROBE', ?, ?)",
                (
                    str(uuid.uuid4()),
                    channel_id,
                    model[:200],
                    input_tokens,
                    output_tokens,
                    estimated_cost,
                    int(priced),
                    occurred_at.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat(),
                    blocked_reason[:200] if blocked_reason else None,
                    _iso(occurred_at),
                ),
            )

    async def recovery_probe_budget_summary(self, budget_date: date) -> dict[str, Any]:
        return await asyncio.to_thread(
            self._recovery_probe_budget_summary_sync,
            budget_date.isoformat(),
        )

    def _recovery_probe_budget_summary_sync(self, budget_date: str) -> dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(CASE WHEN blocked_reason IS NULL THEN 1 END) AS requests, "
                "SUM(CASE WHEN blocked_reason IS NULL "
                "THEN COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0) ELSE 0 END) "
                "AS tokens, SUM(CASE WHEN blocked_reason IS NULL THEN estimated_cost ELSE 0 END) "
                "AS cost, COUNT(CASE WHEN blocked_reason IS NOT NULL THEN 1 END) AS blocked "
                "FROM guardian_probe_ledger "
                "WHERE request_source = 'RECOVERY_PROBE' AND budget_date = ?",
                (budget_date,),
            ).fetchone()
        return {
            "request_count": int(row["requests"] or 0),
            "total_tokens": int(row["tokens"] or 0),
            "estimated_cost": float(row["cost"] or 0),
            "blocked_count": int(row["blocked"] or 0),
        }

    async def cleanup_retention(
        self,
        *,
        now: datetime | None = None,
        batch_size: int = 500,
    ) -> dict[str, int]:
        """Remove or redact eligible evidence within one global bounded batch."""
        reference = now or self._clock()
        if reference.tzinfo is None:
            raise ValueError("retention time must be timezone-aware")
        if not 1 <= batch_size <= 10_000:
            raise ValueError("batch_size must be between 1 and 10000")
        return await asyncio.to_thread(
            self._cleanup_retention_sync,
            reference,
            batch_size,
        )

    def _cleanup_retention_sync(
        self,
        now: datetime,
        batch_size: int,
    ) -> dict[str, int]:
        cutoffs = {
            "dedup": _iso(now - timedelta(days=7)),
            "traffic": _iso(now - timedelta(days=30)),
            "samples": _iso(now - timedelta(days=90)),
            "snapshots": _iso(now - timedelta(days=90)),
        }
        counts = {
            "samples": 0,
            "traffic_buckets": 0,
            "snapshots": 0,
            "snapshot_payloads_redacted": 0,
            "source_ids_redacted": 0,
        }
        remaining = batch_size
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")

            def execute_bounded(key: str, sql: str, params: tuple[object, ...]) -> None:
                nonlocal remaining
                if remaining <= 0:
                    return
                cursor = connection.execute(sql, (*params, remaining))
                changed = max(0, cursor.rowcount)
                counts[key] += changed
                remaining -= changed

            execute_bounded(
                "samples",
                "DELETE FROM guardian_samples WHERE rowid IN "
                "(SELECT rowid FROM guardian_samples WHERE occurred_at < ? "
                "ORDER BY occurred_at LIMIT ?)",
                (cutoffs["samples"],),
            )
            execute_bounded(
                "traffic_buckets",
                "DELETE FROM guardian_traffic_buckets WHERE rowid IN "
                "(SELECT rowid FROM guardian_traffic_buckets WHERE bucket_at < ? "
                "ORDER BY bucket_at LIMIT ?)",
                (cutoffs["traffic"],),
            )
            execute_bounded(
                "snapshots",
                "DELETE FROM guardian_input_snapshots WHERE rowid IN "
                "(SELECT rowid FROM guardian_input_snapshots "
                "WHERE consumed_at IS NOT NULL AND captured_at < ? "
                "ORDER BY captured_at LIMIT ?)",
                (cutoffs["snapshots"],),
            )
            execute_bounded(
                "snapshot_payloads_redacted",
                "UPDATE guardian_input_snapshots SET payload_json = '{}' WHERE rowid IN "
                "(SELECT rowid FROM guardian_input_snapshots "
                "WHERE consumed_at IS NOT NULL AND captured_at < ? AND payload_json <> '{}' "
                "ORDER BY captured_at LIMIT ?)",
                (cutoffs["dedup"],),
            )
            execute_bounded(
                "source_ids_redacted",
                "UPDATE guardian_samples SET source_event_id = NULL WHERE rowid IN "
                "(SELECT rowid FROM guardian_samples "
                "WHERE occurred_at < ? AND source_event_id IS NOT NULL "
                "ORDER BY occurred_at LIMIT ?)",
                (cutoffs["dedup"],),
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        counts["deleted_total"] = (
            counts["samples"] + counts["traffic_buckets"] + counts["snapshots"]
        )
        counts["processed_total"] = batch_size - remaining
        return counts

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
    def _account_observation_from_row(
        row: sqlite3.Row,
    ) -> GuardianAccountObservation:
        try:
            return GuardianAccountObservation.model_validate(
                {
                    "account_id": row["account_id"],
                    "group_ids": json.loads(row["group_ids_json"]),
                    "status": row["status"],
                    "schedulable": bool(row["schedulable"]),
                    "expired": bool(row["expired"]),
                    "temporary_unavailable": bool(row["temporary_unavailable"]),
                }
            )
        except (ValidationError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ServiceError(
                "ACCOUNT_OBSERVATION_DATA_INVALID",
                "Persisted Guardian account observation data is invalid",
            ) from exc

    @staticmethod
    def _channel_error_episode_from_row(
        row: sqlite3.Row,
    ) -> GuardianChannelErrorEpisode:
        try:
            return GuardianChannelErrorEpisode.model_validate(
                {
                    "episode_id": row["episode_id"],
                    "channel_id": row["channel_id"],
                    "group_id": row["group_id"],
                    "opened_snapshot_id": row["opened_snapshot_id"],
                    "status": row["status"],
                    "opened_at": _dt(row["opened_at"]),
                    "closed_at": _dt(row["closed_at"]),
                }
            )
        except (ValidationError, ValueError, TypeError) as exc:
            raise ServiceError(
                "CHANNEL_ERROR_EPISODE_DATA_INVALID",
                "Persisted Guardian channel error episode data is invalid",
            ) from exc

    @staticmethod
    def _account_recovery_run_from_row(
        row: sqlite3.Row,
    ) -> GuardianAccountRecoveryRun:
        try:
            return GuardianAccountRecoveryRun.model_validate(
                {
                    "run_id": row["run_id"],
                    "dedup_key": row["dedup_key"],
                    "trigger": row["trigger"],
                    "snapshot_id": row["snapshot_id"],
                    "episode_id": row["episode_id"],
                    "policy_revision": row["policy_revision"],
                    "status": row["status"],
                    "result": (
                        json.loads(row["result_json"])
                        if row["result_json"] is not None
                        else None
                    ),
                    "started_at": _dt(row["started_at"]),
                    "finished_at": _dt(row["finished_at"]),
                }
            )
        except (ValidationError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ServiceError(
                "ACCOUNT_RECOVERY_RUN_DATA_INVALID",
                "Persisted Guardian account recovery run data is invalid",
            ) from exc

    @staticmethod
    def _account_recovery_record_from_row(
        row: sqlite3.Row,
    ) -> GuardianAccountRecoveryRecord:
        try:
            return GuardianAccountRecoveryRecord.model_validate(
                {
                    "ledger_id": row["ledger_id"],
                    "run_id": row["run_id"],
                    "dedup_key": row["dedup_key"],
                    "account_id": row["account_id"],
                    "channel_id": row["channel_id"],
                    "group_id": row["group_id"],
                    "classification": row["classification"],
                    "result": row["result"],
                    "reason": row["reason"],
                    "tested": bool(row["tested"]),
                    "occurred_at": _dt(row["occurred_at"]),
                }
            )
        except (ValidationError, ValueError, TypeError) as exc:
            raise ServiceError(
                "ACCOUNT_RECOVERY_RESULT_DATA_INVALID",
                "Persisted Guardian account recovery result data is invalid",
            ) from exc

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
            "confidence": float(row["confidence"]),
            "freshness_state": row["freshness_state"],
            "last_evidence_at": row["last_evidence_at"],
            "warmup_buckets": int(row["warmup_buckets"]),
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
