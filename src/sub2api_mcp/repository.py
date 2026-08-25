"""Versioned SQLite persistence with explicit transactional claims."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import sqlite3
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError

from .contracts import (
    AccountBinding,
    AccountQuarantinePage,
    AccountQuarantineReason,
    AccountQuarantineRecord,
    ClaimedDelivery,
    DeliveryStatus,
    DeliveryTargetCreate,
    DeliveryTargetPage,
    DeliveryTargetRecord,
    JobPage,
    JobRecord,
    JobStatus,
    JobType,
    OutboxEventRecord,
    OutboxEventType,
    QuarantineProbeResult,
)
from .errors import ServiceError
from .schema import (
    ACCOUNT_QUARANTINE_INDEX_DDL,
    ACCOUNT_QUARANTINE_TABLE_DDL,
    SCHEMA_SQL,
)
from .schema import SCHEMA_VERSION as CURRENT_SCHEMA_VERSION


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class SqliteRepository:
    SCHEMA_VERSION = CURRENT_SCHEMA_VERSION

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
        with self._connect() as connection:
            current_version = self._current_schema_version(connection)
            if current_version > self.SCHEMA_VERSION:
                raise RuntimeError("database schema is newer than this service")
            connection.executescript(SCHEMA_SQL)
            if current_version == 2:
                self._migrate_quarantine_probe_result(connection)
            connection.execute(
                "INSERT INTO service_metadata(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(self.SCHEMA_VERSION),),
            )
            connection.execute(
                "UPDATE jobs SET status = ?, error_code = ?, error_message = ?, "
                "updated_at = ?, finished_at = ?, worker_id = NULL "
                "WHERE status = ?",
                (
                    JobStatus.INTERRUPTED.value,
                    "SERVICE_RESTARTED",
                    "The service restarted while this job was running",
                    now,
                    now,
                    JobStatus.RUNNING.value,
                ),
            )

    @staticmethod
    def _current_schema_version(connection: sqlite3.Connection) -> int:
        metadata_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("service_metadata",),
        ).fetchone()
        if metadata_exists is None:
            return 0
        row = connection.execute(
            "SELECT value FROM service_metadata WHERE key = ?",
            ("schema_version",),
        ).fetchone()
        if row is None:
            return 0
        try:
            version = int(row["value"])
        except (TypeError, ValueError) as exc:
            raise RuntimeError("database schema version is invalid") from exc
        if version < 0:
            raise RuntimeError("database schema version is invalid")
        return version

    @staticmethod
    def _migrate_quarantine_probe_result(connection: sqlite3.Connection) -> None:
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("account_quarantines",),
        ).fetchone()
        if table_exists is None:
            return
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DROP INDEX IF EXISTS idx_account_quarantines_probe")
            connection.execute(
                "ALTER TABLE account_quarantines RENAME TO account_quarantines_v2"
            )
            connection.execute(ACCOUNT_QUARANTINE_TABLE_DDL)
            connection.execute(ACCOUNT_QUARANTINE_INDEX_DDL)
            connection.execute(
                "INSERT INTO account_quarantines("
                "account_id, reason, group_ids_json, threshold_ms, observed_count, "
                "quarantined_at, last_probe_at, last_probe_latency_ms, "
                "last_probe_result, updated_at"
                ") SELECT account_id, reason, group_ids_json, threshold_ms, "
                "observed_count, quarantined_at, last_probe_at, last_probe_latency_ms, "
                "CASE WHEN last_probe_result = 'SUCCESS' THEN 'RECOVERED' "
                "ELSE last_probe_result END, updated_at FROM account_quarantines_v2"
            )
            connection.execute("DROP TABLE account_quarantines_v2")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

    async def create_job(self, job_type: JobType, payload: dict[str, Any]) -> JobRecord:
        return await asyncio.to_thread(self._create_job_sync, job_type, payload)

    def _create_job_sync(self, job_type: JobType, payload: dict[str, Any]) -> JobRecord:
        job_id = str(uuid.uuid4())
        now = _iso(self._clock())
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO jobs(job_id, job_type, status, payload_json, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                (job_id, job_type.value, JobStatus.QUEUED.value, _json(payload), now, now),
            )
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        assert row is not None
        return self._job_from_row(row)

    async def create_job_with_capacity(
        self,
        job_type: JobType,
        payload: dict[str, Any],
        *,
        max_active: int,
    ) -> tuple[JobRecord, int] | None:
        return await asyncio.to_thread(
            self._create_job_with_capacity_sync, job_type, payload, max_active
        )

    def _create_job_with_capacity_sync(
        self,
        job_type: JobType,
        payload: dict[str, Any],
        max_active: int,
    ) -> tuple[JobRecord, int] | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            active = int(
                connection.execute(
                    "SELECT COUNT(*) FROM jobs WHERE job_type = ? AND status IN (?, ?)",
                    (
                        job_type.value,
                        JobStatus.QUEUED.value,
                        JobStatus.RUNNING.value,
                    ),
                ).fetchone()[0]
            )
            if active >= max_active:
                connection.execute("COMMIT")
                return None
            job_id = str(uuid.uuid4())
            now = _iso(self._clock())
            connection.execute(
                "INSERT INTO jobs(job_id, job_type, status, payload_json, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                (job_id, job_type.value, JobStatus.QUEUED.value, _json(payload), now, now),
            )
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert row is not None
        return self._job_from_row(row), active + 1

    async def active_job_count(self, job_type: JobType) -> int:
        return await asyncio.to_thread(self._active_job_count_sync, job_type)

    def _active_job_count_sync(self, job_type: JobType) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM jobs WHERE job_type = ? AND status IN (?, ?)",
                (job_type.value, JobStatus.QUEUED.value, JobStatus.RUNNING.value),
            ).fetchone()
        return int(row["count"] if row is not None else 0)

    async def get_job(self, job_id: str) -> JobRecord | None:
        return await asyncio.to_thread(self._get_job_sync, job_id)

    def _get_job_sync(self, job_id: str) -> JobRecord | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return self._job_from_row(row) if row is not None else None

    @staticmethod
    def _encode_cursor(created_at: str, job_id: str) -> str:
        raw = _json([created_at, job_id]).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str) -> tuple[str, str]:
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            raw_value: object = json.loads(base64.urlsafe_b64decode(padded).decode())
            if not isinstance(raw_value, list):
                raise ValueError
            value = cast(list[object], raw_value)
            if len(value) != 2 or not all(isinstance(item, str) for item in value):
                raise ValueError
            return cast(str, value[0]), cast(str, value[1])
        except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise ServiceError("INVALID_CURSOR", "The job cursor is invalid") from exc

    async def list_jobs(
        self,
        *,
        limit: int = 20,
        cursor: str | None = None,
        job_type: JobType | None = None,
        status: JobStatus | None = None,
    ) -> JobPage:
        return await asyncio.to_thread(
            self._list_jobs_sync, limit, cursor, job_type, status
        )

    def _list_jobs_sync(
        self,
        limit: int,
        cursor: str | None,
        job_type: JobType | None,
        status: JobStatus | None,
    ) -> JobPage:
        if not 1 <= limit <= 100:
            raise ServiceError("INVALID_PAGE_SIZE", "Job page size must be between 1 and 100")
        parameters: list[object] = []
        conditions: list[str] = []
        if job_type is not None:
            conditions.append("job_type = ?")
            parameters.append(job_type.value)
        if status is not None:
            conditions.append("status = ?")
            parameters.append(status.value)
        if cursor:
            created_at, job_id = self._decode_cursor(cursor)
            conditions.append("(created_at < ? OR (created_at = ? AND job_id < ?))")
            parameters.extend((created_at, created_at, job_id))
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        parameters.append(limit + 1)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM jobs {where} ORDER BY created_at DESC, job_id DESC LIMIT ?",
                parameters,
            ).fetchall()
        has_more = len(rows) > limit
        selected = rows[:limit]
        next_cursor = None
        if has_more and selected:
            next_cursor = self._encode_cursor(selected[-1]["created_at"], selected[-1]["job_id"])
        return JobPage(items=[self._job_from_row(row) for row in selected], next_cursor=next_cursor)

    async def claim_next_job(
        self, job_types: set[JobType], worker_id: str
    ) -> JobRecord | None:
        return await asyncio.to_thread(self._claim_next_job_sync, job_types, worker_id)

    def _claim_next_job_sync(
        self, job_types: set[JobType], worker_id: str
    ) -> JobRecord | None:
        if not job_types:
            return None
        now = _iso(self._clock())
        values = sorted(item.value for item in job_types)
        placeholders = ",".join("?" for _ in values)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"SELECT * FROM jobs WHERE status = ? AND job_type IN ({placeholders}) "
                "ORDER BY created_at, job_id LIMIT 1",
                [JobStatus.QUEUED.value, *values],
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            connection.execute(
                "UPDATE jobs SET status = ?, worker_id = ?, started_at = ?, updated_at = ? "
                "WHERE job_id = ? AND status = ?",
                (
                    JobStatus.RUNNING.value,
                    worker_id,
                    now,
                    now,
                    row["job_id"],
                    JobStatus.QUEUED.value,
                ),
            )
            claimed = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (row["job_id"],)
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert claimed is not None
        return self._job_from_row(claimed)

    async def complete_job(self, job_id: str, result: dict[str, Any]) -> JobRecord:
        return await asyncio.to_thread(
            self._finish_job_sync, job_id, JobStatus.SUCCEEDED, result, None, None
        )

    async def fail_job(self, job_id: str, error_code: str, message: str) -> JobRecord:
        return await asyncio.to_thread(
            self._finish_job_sync, job_id, JobStatus.FAILED, None, error_code, message
        )

    def _finish_job_sync(
        self,
        job_id: str,
        status: JobStatus,
        result: dict[str, Any] | None,
        error_code: str | None,
        error_message: str | None,
    ) -> JobRecord:
        now = _iso(self._clock())
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE jobs SET status = ?, result_json = ?, error_code = ?, error_message = ?, "
                "updated_at = ?, finished_at = ?, worker_id = NULL WHERE job_id = ? AND status = ?",
                (
                    status.value,
                    _json(result) if result is not None else None,
                    error_code,
                    error_message,
                    now,
                    now,
                    job_id,
                    JobStatus.RUNNING.value,
                ),
            )
            if updated.rowcount != 1:
                raise ServiceError("INVALID_JOB_STATE", "The job is not running")
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        assert row is not None
        return self._job_from_row(row)

    async def cancel_job(self, job_id: str) -> JobRecord:
        return await asyncio.to_thread(self._cancel_job_sync, job_id)

    def _cancel_job_sync(self, job_id: str) -> JobRecord:
        now = _iso(self._clock())
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            if row is None:
                raise ServiceError("JOB_NOT_FOUND", "The job does not exist")
            status = JobStatus(row["status"])
            if status is JobStatus.QUEUED:
                connection.execute(
                    "UPDATE jobs SET status = ?, cancel_requested = 1, updated_at = ?, "
                    "finished_at = ? WHERE job_id = ?",
                    (JobStatus.CANCELLED.value, now, now, job_id),
                )
            elif status is JobStatus.RUNNING:
                connection.execute(
                    "UPDATE jobs SET cancel_requested = 1, updated_at = ? WHERE job_id = ?",
                    (now, job_id),
                )
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        assert row is not None
        return self._job_from_row(row)

    async def mark_running_job_cancelled(self, job_id: str) -> JobRecord:
        return await asyncio.to_thread(self._mark_running_job_cancelled_sync, job_id)

    def _mark_running_job_cancelled_sync(self, job_id: str) -> JobRecord:
        now = _iso(self._clock())
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE jobs SET status = ?, cancel_requested = 1, updated_at = ?, "
                "finished_at = ?, worker_id = NULL WHERE job_id = ? AND status = ?",
                (
                    JobStatus.CANCELLED.value,
                    now,
                    now,
                    job_id,
                    JobStatus.RUNNING.value,
                ),
            )
            if updated.rowcount != 1:
                raise ServiceError("INVALID_JOB_STATE", "The job is not running")
            row = connection.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        assert row is not None
        return self._job_from_row(row)

    async def acquire_scheduler_lease(self, owner: str, *, lease_seconds: int) -> bool:
        return await asyncio.to_thread(self._acquire_scheduler_lease_sync, owner, lease_seconds)

    def _acquire_scheduler_lease_sync(self, owner: str, lease_seconds: int) -> bool:
        now = self._clock().astimezone(UTC)
        expires = _iso(now + timedelta(seconds=lease_seconds))
        now_text = _iso(now)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT owner, expires_at FROM scheduler_lease WHERE singleton = 1"
            ).fetchone()
            if row is not None and row["owner"] != owner and row["expires_at"] > now_text:
                connection.execute("COMMIT")
                return False
            connection.execute(
                "INSERT INTO scheduler_lease(singleton, owner, expires_at) VALUES(1, ?, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET owner = excluded.owner, "
                "expires_at = excluded.expires_at",
                (owner, expires),
            )
            connection.execute("COMMIT")
            return True
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    async def upsert_delivery_target(
        self, target: DeliveryTargetCreate
    ) -> DeliveryTargetRecord:
        return await asyncio.to_thread(self._upsert_delivery_target_sync, target)

    def _upsert_delivery_target_sync(
        self, target: DeliveryTargetCreate
    ) -> DeliveryTargetRecord:
        now = _iso(self._clock())
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT delivery_target_id, created_at FROM delivery_targets WHERE name = ?",
                (target.name,),
            ).fetchone()
            delivery_target_id = (
                existing["delivery_target_id"] if existing is not None else str(uuid.uuid4())
            )
            created_at = existing["created_at"] if existing is not None else now
            connection.execute(
                "INSERT INTO delivery_targets(delivery_target_id, name, bot_uuid, target_type, "
                "target_id, purposes_json, media_policy, required, enabled, created_at, "
                "updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET bot_uuid = excluded.bot_uuid, "
                "target_type = excluded.target_type, target_id = excluded.target_id, "
                "purposes_json = excluded.purposes_json, media_policy = excluded.media_policy, "
                "required = excluded.required, enabled = excluded.enabled, "
                "updated_at = excluded.updated_at",
                (
                    delivery_target_id,
                    target.name,
                    target.bot_uuid,
                    target.target_type.value,
                    target.target_id,
                    _json(sorted(item.value for item in target.purposes)),
                    target.media_policy.value,
                    int(target.required),
                    int(target.enabled),
                    created_at,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM delivery_targets WHERE name = ?", (target.name,)
            ).fetchone()
        assert row is not None
        return self._target_from_row(row)

    async def list_delivery_targets(self) -> list[DeliveryTargetRecord]:
        return await asyncio.to_thread(self._list_delivery_targets_sync)

    def _list_delivery_targets_sync(self) -> list[DeliveryTargetRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM delivery_targets ORDER BY name, delivery_target_id"
            ).fetchall()
        return [self._target_from_row(row) for row in rows]

    async def list_delivery_targets_page(
        self, *, limit: int = 20, cursor: str | None = None
    ) -> DeliveryTargetPage:
        return await asyncio.to_thread(
            self._list_delivery_targets_page_sync, limit, cursor
        )

    def _list_delivery_targets_page_sync(
        self, limit: int, cursor: str | None
    ) -> DeliveryTargetPage:
        if not 1 <= limit <= 100:
            raise ServiceError(
                "INVALID_PAGE_SIZE", "Delivery target page size must be between 1 and 100"
            )
        parameters: list[object] = []
        where = ""
        if cursor:
            name, delivery_target_id = self._decode_cursor(cursor)
            where = "WHERE name > ? OR (name = ? AND delivery_target_id > ?)"
            parameters.extend((name, name, delivery_target_id))
        parameters.append(limit + 1)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM delivery_targets {where} "
                "ORDER BY name, delivery_target_id LIMIT ?",
                parameters,
            ).fetchall()
        has_more = len(rows) > limit
        selected = rows[:limit]
        next_cursor = None
        if has_more and selected:
            next_cursor = self._encode_cursor(
                selected[-1]["name"], selected[-1]["delivery_target_id"]
            )
        return DeliveryTargetPage(
            items=[self._target_from_row(row) for row in selected],
            next_cursor=next_cursor,
        )

    async def get_delivery_target(
        self, delivery_target_id: str
    ) -> DeliveryTargetRecord | None:
        return await asyncio.to_thread(
            self._get_delivery_target_sync, delivery_target_id
        )

    def _get_delivery_target_sync(
        self, delivery_target_id: str
    ) -> DeliveryTargetRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM delivery_targets WHERE delivery_target_id = ?",
                (delivery_target_id,),
            ).fetchone()
        return self._target_from_row(row) if row is not None else None

    async def delete_delivery_target(self, delivery_target_id: str) -> None:
        await asyncio.to_thread(self._delete_delivery_target_sync, delivery_target_id)

    def _delete_delivery_target_sync(self, delivery_target_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE delivery_targets SET enabled = 0, updated_at = ? "
                "WHERE delivery_target_id = ?",
                (_iso(self._clock()), delivery_target_id),
            )

    async def upsert_account_quarantine(
        self,
        record: AccountQuarantineRecord,
    ) -> AccountQuarantineRecord:
        return await asyncio.to_thread(self._upsert_account_quarantine_sync, record)

    def _upsert_account_quarantine_sync(
        self,
        record: AccountQuarantineRecord,
    ) -> AccountQuarantineRecord:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO account_quarantines("
                "account_id, reason, group_ids_json, threshold_ms, observed_count, "
                "quarantined_at, last_probe_at, last_probe_latency_ms, "
                "last_probe_result, updated_at"
                ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(account_id) DO UPDATE SET "
                "reason = excluded.reason, group_ids_json = excluded.group_ids_json, "
                "threshold_ms = excluded.threshold_ms, "
                "observed_count = excluded.observed_count, "
                "quarantined_at = excluded.quarantined_at, "
                "last_probe_at = excluded.last_probe_at, "
                "last_probe_latency_ms = excluded.last_probe_latency_ms, "
                "last_probe_result = excluded.last_probe_result, "
                "updated_at = excluded.updated_at",
                (
                    record.account_id,
                    record.reason.value,
                    _json(list(record.group_ids)),
                    record.threshold_ms,
                    record.observed_count,
                    _iso(record.quarantined_at),
                    _iso(record.last_probe_at) if record.last_probe_at is not None else None,
                    record.last_probe_latency_ms,
                    record.last_probe_result.value,
                    _iso(self._clock()),
                ),
            )
            row = connection.execute(
                "SELECT * FROM account_quarantines WHERE account_id = ?",
                (record.account_id,),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert row is not None
        return self._quarantine_from_row(row)

    async def get_account_quarantine(
        self,
        account_id: str,
    ) -> AccountQuarantineRecord | None:
        return await asyncio.to_thread(self._get_account_quarantine_sync, account_id)

    def _get_account_quarantine_sync(
        self,
        account_id: str,
    ) -> AccountQuarantineRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM account_quarantines WHERE account_id = ?",
                (account_id,),
            ).fetchone()
        return self._quarantine_from_row(row) if row is not None else None

    async def list_account_quarantines(
        self,
        *,
        limit: int = 20,
        cursor: str | None = None,
        reason: AccountQuarantineReason | None = None,
    ) -> AccountQuarantinePage:
        return await asyncio.to_thread(
            self._list_account_quarantines_sync,
            limit,
            cursor,
            reason,
        )

    def _list_account_quarantines_sync(
        self,
        limit: int,
        cursor: str | None,
        reason: AccountQuarantineReason | None,
    ) -> AccountQuarantinePage:
        if not 1 <= limit <= 100:
            raise ServiceError(
                "INVALID_PAGE_SIZE",
                "Account quarantine page size must be between 1 and 100",
            )
        conditions: list[str] = []
        parameters: list[object] = []
        if reason is not None:
            conditions.append("reason = ?")
            parameters.append(reason.value)
        if cursor:
            kind, account_id = self._decode_cursor(cursor)
            if kind != "account-quarantine":
                raise ServiceError("INVALID_CURSOR", "The quarantine cursor is invalid")
            conditions.append("account_id > ?")
            parameters.append(account_id)
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        parameters.append(limit + 1)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT * FROM account_quarantines {where} "
                "ORDER BY account_id ASC LIMIT ?",
                parameters,
            ).fetchall()
        selected = rows[:limit]
        next_cursor = None
        if len(rows) > limit and selected:
            next_cursor = self._encode_cursor(
                "account-quarantine",
                selected[-1]["account_id"],
            )
        return AccountQuarantinePage(
            items=[self._quarantine_from_row(row) for row in selected],
            next_cursor=next_cursor,
        )

    async def list_account_quarantines_for_probe(
        self,
        *,
        limit: int = 5,
    ) -> list[AccountQuarantineRecord]:
        return await asyncio.to_thread(
            self._list_account_quarantines_for_probe_sync,
            limit,
        )

    def _list_account_quarantines_for_probe_sync(
        self,
        limit: int,
    ) -> list[AccountQuarantineRecord]:
        if not 1 <= limit <= 5:
            raise ServiceError(
                "INVALID_PAGE_SIZE",
                "Account quarantine probe limit must be between 1 and 5",
            )
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM account_quarantines "
                "ORDER BY CASE WHEN last_probe_at IS NULL THEN 0 ELSE 1 END, "
                "last_probe_at ASC, quarantined_at ASC, account_id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._quarantine_from_row(row) for row in rows]

    async def update_account_quarantine_probe(
        self,
        account_id: str,
        *,
        probed_at: datetime,
        latency_ms: int | None,
        result: QuarantineProbeResult,
    ) -> AccountQuarantineRecord:
        if result is QuarantineProbeResult.NEVER:
            raise ValueError("a completed quarantine probe cannot use NEVER")
        if probed_at.tzinfo is None:
            raise ValueError("probed_at must be timezone-aware")
        if latency_ms is not None and latency_ms < 0:
            raise ValueError("latency_ms must be non-negative")
        return await asyncio.to_thread(
            self._update_account_quarantine_probe_sync,
            account_id,
            probed_at,
            latency_ms,
            result,
        )

    def _update_account_quarantine_probe_sync(
        self,
        account_id: str,
        probed_at: datetime,
        latency_ms: int | None,
        result: QuarantineProbeResult,
    ) -> AccountQuarantineRecord:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                "UPDATE account_quarantines SET last_probe_at = ?, "
                "last_probe_latency_ms = ?, last_probe_result = ?, updated_at = ? "
                "WHERE account_id = ?",
                (
                    _iso(probed_at),
                    latency_ms,
                    result.value,
                    _iso(self._clock()),
                    account_id,
                ),
            )
            if updated.rowcount != 1:
                raise ServiceError(
                    "QUARANTINE_NOT_FOUND",
                    "The account quarantine does not exist",
                )
            row = connection.execute(
                "SELECT * FROM account_quarantines WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert row is not None
        return self._quarantine_from_row(row)

    async def remove_verified_account_quarantine(self, account_id: str) -> bool:
        return await asyncio.to_thread(
            self._remove_verified_account_quarantine_sync,
            account_id,
        )

    def _remove_verified_account_quarantine_sync(self, account_id: str) -> bool:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            removed = connection.execute(
                "DELETE FROM account_quarantines WHERE account_id = ?",
                (account_id,),
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return removed.rowcount == 1

    async def account_quarantine_count(
        self,
        reason: AccountQuarantineReason | None = None,
    ) -> int:
        return await asyncio.to_thread(self._account_quarantine_count_sync, reason)

    def _account_quarantine_count_sync(
        self,
        reason: AccountQuarantineReason | None,
    ) -> int:
        with self._connect() as connection:
            if reason is None:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM account_quarantines"
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM account_quarantines WHERE reason = ?",
                    (reason.value,),
                ).fetchone()
        return int(row["count"] if row is not None else 0)

    async def bind_actor(self, actor_key: str, user_id: str, masked_email: str) -> AccountBinding:
        return await asyncio.to_thread(self._bind_actor_sync, actor_key, user_id, masked_email)

    def _bind_actor_sync(self, actor_key: str, user_id: str, masked_email: str) -> AccountBinding:
        now = _iso(self._clock())
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO account_bindings(actor_key, user_id, masked_email, bound_at) "
                    "VALUES(?, ?, ?, ?)",
                    (actor_key, user_id, masked_email, now),
                )
        except sqlite3.IntegrityError as exc:
            raise ServiceError(
                "BINDING_CONFLICT", "The actor or Sub2API account is already bound"
            ) from exc
        return AccountBinding(
            actor_key=actor_key,
            user_id=user_id,
            masked_email=masked_email,
            bound_at=self._clock(),
        )

    async def get_binding(self, actor_key: str) -> AccountBinding | None:
        return await asyncio.to_thread(self._get_binding_sync, actor_key)

    def _get_binding_sync(self, actor_key: str) -> AccountBinding | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM account_bindings WHERE actor_key = ?", (actor_key,)
            ).fetchone()
        if row is None:
            return None
        bound_at = _datetime(row["bound_at"])
        assert bound_at is not None
        return AccountBinding(
            actor_key=row["actor_key"],
            user_id=row["user_id"],
            masked_email=row["masked_email"],
            bound_at=bound_at,
        )

    async def unbind_actor(self, actor_key: str) -> None:
        await asyncio.to_thread(self._unbind_actor_sync, actor_key)

    def _unbind_actor_sync(self, actor_key: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM account_bindings WHERE actor_key = ?", (actor_key,))

    async def claim_actor_nonce(
        self,
        nonce: str,
        expires_at: datetime,
        *,
        claimed_at: datetime | None = None,
    ) -> bool:
        return await asyncio.to_thread(
            self._claim_actor_nonce_sync, nonce, expires_at, claimed_at
        )

    def _claim_actor_nonce_sync(
        self,
        nonce: str,
        expires_at: datetime,
        claimed_at: datetime | None,
    ) -> bool:
        now = _iso(claimed_at or self._clock())
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM actor_nonces WHERE expires_at <= ?", (now,))
            try:
                connection.execute(
                    "INSERT INTO actor_nonces(nonce, expires_at) VALUES(?, ?)",
                    (nonce, _iso(expires_at)),
                )
            except sqlite3.IntegrityError:
                connection.execute("COMMIT")
                return False
            connection.execute("COMMIT")
            return True
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    async def enqueue_outbox(
        self,
        event_type: OutboxEventType,
        payload: dict[str, Any],
        target_ids: list[str],
    ) -> OutboxEventRecord:
        return await asyncio.to_thread(
            self._enqueue_outbox_sync, event_type, payload, target_ids
        )

    def _enqueue_outbox_sync(
        self,
        event_type: OutboxEventType,
        payload: dict[str, Any],
        target_ids: list[str],
    ) -> OutboxEventRecord:
        if not target_ids:
            raise ServiceError("NO_DELIVERY_TARGET", "At least one delivery target is required")
        unique_target_ids = list(dict.fromkeys(target_ids))
        event_id = str(uuid.uuid4())
        now = _iso(self._clock())
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            for target_id in unique_target_ids:
                exists = connection.execute(
                    "SELECT 1 FROM delivery_targets "
                    "WHERE delivery_target_id = ? AND enabled = 1",
                    (target_id,),
                ).fetchone()
                if exists is None:
                    raise ServiceError(
                        "DELIVERY_TARGET_NOT_FOUND",
                        "A delivery target does not exist or is disabled",
                    )
            if event_type is OutboxEventType.STATUS_CHANGED:
                placeholders = ",".join("?" for _ in unique_target_ids)
                raw_coalesce_key = payload.get("coalesceKey")
                coalesce_key = (
                    raw_coalesce_key
                    if isinstance(raw_coalesce_key, str)
                    and 1 <= len(raw_coalesce_key) <= 128
                    else None
                )
                key_filter = (
                    " AND json_extract(payload_json, '$.coalesceKey') = ?"
                    if coalesce_key is not None
                    else ""
                )
                connection.execute(
                    "DELETE FROM notification_deliveries "
                    "WHERE event_id IN ("
                    "SELECT event_id FROM notification_outbox WHERE event_type = ?"
                    f"{key_filter}"
                    ") "
                    f"AND delivery_target_id IN ({placeholders}) "
                    "AND status IN (?, ?)",
                    (
                        event_type.value,
                        *((coalesce_key,) if coalesce_key is not None else ()),
                        *unique_target_ids,
                        DeliveryStatus.PENDING.value,
                        DeliveryStatus.FAILED.value,
                    ),
                )
                connection.execute(
                    "DELETE FROM notification_outbox "
                    "WHERE event_type = ? AND NOT EXISTS ("
                    "SELECT 1 FROM notification_deliveries "
                    "WHERE notification_deliveries.event_id = notification_outbox.event_id"
                    ")",
                    (event_type.value,),
                )
            connection.execute(
                "INSERT INTO notification_outbox(event_id, event_type, payload_json, created_at) "
                "VALUES(?, ?, ?, ?)",
                (event_id, event_type.value, _json(payload), now),
            )
            for target_id in unique_target_ids:
                connection.execute(
                    "INSERT INTO notification_deliveries(delivery_id, event_id, "
                    "delivery_target_id, status) VALUES(?, ?, ?, ?)",
                    (str(uuid.uuid4()), event_id, target_id, DeliveryStatus.PENDING.value),
                )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        return OutboxEventRecord(
            event_id=event_id,
            event_type=event_type,
            payload=payload,
            created_at=self._clock(),
        )

    async def claim_next_delivery(
        self, worker_id: str, *, lease_seconds: int
    ) -> ClaimedDelivery | None:
        return await asyncio.to_thread(
            self._claim_next_delivery_sync, worker_id, lease_seconds
        )

    def _claim_next_delivery_sync(
        self, worker_id: str, lease_seconds: int
    ) -> ClaimedDelivery | None:
        now_value = self._clock().astimezone(UTC)
        now = _iso(now_value)
        lease_expires = _iso(now_value + timedelta(seconds=lease_seconds))
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT d.delivery_id FROM notification_deliveries d "
                "WHERE (d.status IN (?, ?) AND "
                "(d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)) "
                "OR (d.status = ? AND d.lease_expires_at <= ?) "
                "ORDER BY d.rowid LIMIT 1",
                (
                    DeliveryStatus.PENDING.value,
                    DeliveryStatus.FAILED.value,
                    now,
                    DeliveryStatus.LEASED.value,
                    now,
                ),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            connection.execute(
                "UPDATE notification_deliveries SET status = ?, attempts = attempts + 1, "
                "lease_owner = ?, lease_expires_at = ? WHERE delivery_id = ?",
                (DeliveryStatus.LEASED.value, worker_id, lease_expires, row["delivery_id"]),
            )
            claimed = connection.execute(
                "SELECT d.*, e.event_type, e.payload_json, t.* "
                "FROM notification_deliveries d "
                "JOIN notification_outbox e ON e.event_id = d.event_id "
                "JOIN delivery_targets t ON t.delivery_target_id = d.delivery_target_id "
                "WHERE d.delivery_id = ?",
                (row["delivery_id"],),
            ).fetchone()
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        assert claimed is not None
        return ClaimedDelivery(
            delivery_id=claimed["delivery_id"],
            event_id=claimed["event_id"],
            event_type=OutboxEventType(claimed["event_type"]),
            payload=json.loads(claimed["payload_json"]),
            target=self._target_from_row(claimed),
            attempt=int(claimed["attempts"]),
        )

    async def mark_delivery_succeeded(self, delivery_id: str) -> None:
        await asyncio.to_thread(self._mark_delivery_succeeded_sync, delivery_id)

    def _mark_delivery_succeeded_sync(self, delivery_id: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE notification_deliveries SET status = ?, delivered_at = ?, "
                "lease_owner = NULL, lease_expires_at = NULL, next_attempt_at = NULL, "
                "last_error_code = NULL WHERE delivery_id = ? AND status = ?",
                (
                    DeliveryStatus.SUCCEEDED.value,
                    _iso(self._clock()),
                    delivery_id,
                    DeliveryStatus.LEASED.value,
                ),
            )

    async def mark_delivery_failed(
        self, delivery_id: str, error_code: str, *, retry_after_seconds: int
    ) -> None:
        await asyncio.to_thread(
            self._mark_delivery_failed_sync, delivery_id, error_code, retry_after_seconds
        )

    def _mark_delivery_failed_sync(
        self, delivery_id: str, error_code: str, retry_after_seconds: int
    ) -> None:
        retry_at = _iso(self._clock() + timedelta(seconds=retry_after_seconds))
        with self._connect() as connection:
            connection.execute(
                "UPDATE notification_deliveries SET status = ?, next_attempt_at = ?, "
                "last_error_code = ?, lease_owner = NULL, lease_expires_at = NULL "
                "WHERE delivery_id = ? AND status = ?",
                (
                    DeliveryStatus.FAILED.value,
                    retry_at,
                    error_code,
                    delivery_id,
                    DeliveryStatus.LEASED.value,
                ),
            )

    async def outbox_backlog(self) -> int:
        return await asyncio.to_thread(self._outbox_backlog_sync)

    def _outbox_backlog_sync(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(DISTINCT event_id) AS count FROM notification_deliveries "
                "WHERE status != ?",
                (DeliveryStatus.SUCCEEDED.value,),
            ).fetchone()
        return int(row["count"] if row is not None else 0)

    async def finalize_event_if_required_delivered(self, event_id: str) -> bool:
        return await asyncio.to_thread(
            self._finalize_event_if_required_delivered_sync, event_id
        )

    def _finalize_event_if_required_delivered_sync(self, event_id: str) -> bool:
        now = _iso(self._clock())
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT e.payload_json, COUNT(CASE WHEN t.required = 1 AND d.status != ? "
                "THEN 1 END) AS required_pending "
                "FROM notification_outbox e "
                "JOIN notification_deliveries d ON d.event_id = e.event_id "
                "JOIN delivery_targets t ON t.delivery_target_id = d.delivery_target_id "
                "WHERE e.event_id = ? GROUP BY e.event_id",
                (DeliveryStatus.SUCCEEDED.value, event_id),
            ).fetchone()
            if row is None or int(row["required_pending"]) != 0:
                connection.execute("COMMIT")
                return False
            payload: object = json.loads(row["payload_json"])
            payload_dict = cast(dict[str, object], payload) if isinstance(payload, dict) else {}
            delivered_snapshot: object = payload_dict.get("deliveredSnapshot")
            if isinstance(delivered_snapshot, dict):
                snapshot_json = _json(cast(dict[str, Any], delivered_snapshot))
                connection.execute(
                    "INSERT INTO probe_snapshots(snapshot_key, payload_json, updated_at) "
                    "VALUES('delivered', ?, ?) ON CONFLICT(snapshot_key) DO UPDATE SET "
                    "payload_json = excluded.payload_json, updated_at = excluded.updated_at",
                    (snapshot_json, now),
                )
                connection.execute(
                    "DELETE FROM probe_snapshots WHERE snapshot_key = 'pending' "
                    "AND payload_json = ?",
                    (snapshot_json,),
                )
            connection.execute("COMMIT")
            return True
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    async def set_snapshot(self, key: str, payload: dict[str, Any]) -> None:
        await asyncio.to_thread(self._set_snapshot_sync, key, payload)

    def _set_snapshot_sync(self, key: str, payload: dict[str, Any]) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO probe_snapshots(snapshot_key, payload_json, updated_at) "
                "VALUES(?, ?, ?) "
                "ON CONFLICT(snapshot_key) DO UPDATE SET payload_json = excluded.payload_json, "
                "updated_at = excluded.updated_at",
                (key, _json(payload), _iso(self._clock())),
            )

    async def publish_guardian_snapshot(
        self,
        payload: dict[str, Any],
        *,
        captured_at: datetime,
    ) -> str:
        return await asyncio.to_thread(
            self._publish_guardian_snapshot_sync,
            payload,
            captured_at,
        )

    def _publish_guardian_snapshot_sync(
        self,
        payload: dict[str, Any],
        captured_at: datetime,
    ) -> str:
        if captured_at.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")
        serialized = _json(payload)
        if len(serialized.encode("utf-8")) > 2 * 1024 * 1024:
            raise ServiceError(
                "GUARDIAN_SNAPSHOT_TOO_LARGE",
                "The Guardian snapshot exceeds the storage limit",
            )
        payload_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        captured = _iso(captured_at)
        snapshot_id = hashlib.sha256(
            f"1\0{captured}\0{payload_hash}".encode()
        ).hexdigest()
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO guardian_input_snapshots"
                "(snapshot_id, schema_version, payload_json, payload_hash, captured_at, "
                "created_at) VALUES(?, 1, ?, ?, ?, ?)",
                (
                    snapshot_id,
                    serialized,
                    payload_hash,
                    captured,
                    _iso(self._clock()),
                ),
            )
            connection.execute(
                "INSERT INTO guardian_metadata(key, value) "
                "VALUES('shared_sampling_started', 'true') "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            )
        return snapshot_id

    async def get_snapshot(self, key: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._get_snapshot_sync, key)

    def _get_snapshot_sync(self, key: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM probe_snapshots WHERE snapshot_key = ?", (key,)
            ).fetchone()
        return json.loads(row["payload_json"]) if row is not None else None

    async def set_scheduler_value(self, key: str, value: object) -> None:
        await asyncio.to_thread(self._set_scheduler_value_sync, key, value)

    def _set_scheduler_value_sync(self, key: str, value: object) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO scheduler_state(key, value, updated_at) VALUES(?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
                "updated_at = excluded.updated_at",
                (key, _json(value), _iso(self._clock())),
            )

    async def get_scheduler_value(self, key: str) -> object | None:
        return await asyncio.to_thread(self._get_scheduler_value_sync, key)

    def _get_scheduler_value_sync(self, key: str) -> object | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM scheduler_state WHERE key = ?", (key,)
            ).fetchone()
        return json.loads(row["value"]) if row is not None else None

    async def audit(
        self, principal: str, action: str, subject: str | None, outcome: str
    ) -> None:
        await asyncio.to_thread(self._audit_sync, principal, action, subject, outcome)

    def _audit_sync(
        self, principal: str, action: str, subject: str | None, outcome: str
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_events(audit_id, principal, action, subject, outcome, "
                "created_at) "
                "VALUES(?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), principal, action, subject, outcome, _iso(self._clock())),
            )

    @staticmethod
    def _job_from_row(row: sqlite3.Row) -> JobRecord:
        created_at = _datetime(row["created_at"])
        updated_at = _datetime(row["updated_at"])
        assert created_at is not None and updated_at is not None
        return JobRecord(
            job_id=row["job_id"],
            job_type=JobType(row["job_type"]),
            status=JobStatus(row["status"]),
            payload=json.loads(row["payload_json"]),
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            error_code=row["error_code"],
            error_message=row["error_message"],
            cancel_requested=bool(row["cancel_requested"]),
            created_at=created_at,
            updated_at=updated_at,
            started_at=_datetime(row["started_at"]),
            finished_at=_datetime(row["finished_at"]),
        )

    @staticmethod
    def _target_from_row(row: sqlite3.Row) -> DeliveryTargetRecord:
        created_at = _datetime(row["created_at"])
        updated_at = _datetime(row["updated_at"])
        assert created_at is not None and updated_at is not None
        return DeliveryTargetRecord.model_validate(
            {
                "delivery_target_id": row["delivery_target_id"],
                "name": row["name"],
                "bot_uuid": row["bot_uuid"],
                "target_type": row["target_type"],
                "target_id": row["target_id"],
                "purposes": json.loads(row["purposes_json"]),
                "media_policy": row["media_policy"],
                "required": bool(row["required"]),
                "enabled": bool(row["enabled"]),
                "created_at": created_at,
                "updated_at": updated_at,
            }
        )

    @staticmethod
    def _quarantine_from_row(row: sqlite3.Row) -> AccountQuarantineRecord:
        try:
            group_ids = json.loads(row["group_ids_json"])
            return AccountQuarantineRecord.model_validate(
                {
                    "account_id": row["account_id"],
                    "reason": row["reason"],
                    "group_ids": group_ids,
                    "threshold_ms": row["threshold_ms"],
                    "observed_count": row["observed_count"],
                    "quarantined_at": _datetime(row["quarantined_at"]),
                    "last_probe_at": _datetime(row["last_probe_at"]),
                    "last_probe_latency_ms": row["last_probe_latency_ms"],
                    "last_probe_result": row["last_probe_result"],
                }
            )
        except (ValidationError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise ServiceError(
                "QUARANTINE_DATA_INVALID",
                "Persisted account quarantine data is invalid",
            ) from exc
