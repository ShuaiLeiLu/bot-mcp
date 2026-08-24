from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sub2api_mcp.errors import ServiceError
from sub2api_mcp.guardian.contracts import (
    ChannelPolicyOverride,
    GuardianEventType,
    GuardianHealth,
    GuardianPolicy,
    GuardianSample,
    GuardianSampleSource,
    ManualControl,
)
from sub2api_mcp.guardian.repository import GuardianRepository


def _create_minimal_v1_database(path: Path) -> None:
    policy_json = json.dumps(
        {
            "revision": 1,
            "enabled": False,
            "observe_only": True,
            "scan_interval_seconds": 15,
        }
    )
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE guardian_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO guardian_metadata(key, value) VALUES('schema_version', '1');
            CREATE TABLE guardian_policy (
                singleton INTEGER PRIMARY KEY,
                policy_json TEXT NOT NULL,
                revision INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE guardian_channels (
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
            CREATE TABLE guardian_samples (
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
            """
        )
        connection.execute(
            "INSERT INTO guardian_policy"
            "(singleton, policy_json, revision, updated_at) VALUES(1, ?, 1, ?)",
            (policy_json, "2026-08-23T00:00:00Z"),
        )
        connection.execute(
            "INSERT INTO guardian_channels VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "channel-legacy",
                "Legacy",
                "group-1",
                "operational",
                1,
                "HEALTHY",
                91.5,
                1200,
                1,
                "NONE",
                "{}",
                "2026-08-23T00:00:00Z",
                "2026-08-23T00:00:00Z",
                "2026-08-23T00:00:00Z",
            ),
        )


@pytest.mark.asyncio
async def test_repository_initializes_safe_policy_and_revision_updates(
    tmp_path: Path,
) -> None:
    repository = GuardianRepository(tmp_path / "state.db")
    await repository.initialize()

    policy = await repository.get_policy()
    updated = policy.model_copy(update={"scan_interval_seconds": 30})
    saved = await repository.update_policy(updated, expected_revision=1)

    assert policy.observe_only is True
    assert policy.auto_apply.schedulable is False
    assert saved.revision == 2
    assert saved.scan_interval_seconds == 30
    with pytest.raises(ServiceError, match="modified") as conflict:
        await repository.update_policy(updated, expected_revision=1)
    assert conflict.value.code == "POLICY_REVISION_CONFLICT"


@pytest.mark.asyncio
async def test_repository_initializes_v2_evidence_schema(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    repository = GuardianRepository(path)

    await repository.initialize()

    with sqlite3.connect(path) as connection:
        version = connection.execute(
            "SELECT value FROM guardian_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        sample_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(guardian_samples)")
        }
        channel_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(guardian_channels)")
        }

    assert version == str(repository.SCHEMA_VERSION)
    assert {
        "guardian_input_snapshots",
        "guardian_traffic_buckets",
        "guardian_field_ownership",
    } <= tables
    assert {"source_event_id", "bucket_at", "reliability", "ingested_at", "legacy"} <= (
        sample_columns
    )
    assert {"confidence", "freshness_state", "last_evidence_at", "warmup_buckets"} <= (
        channel_columns
    )


@pytest.mark.asyncio
async def test_v1_database_migrates_without_losing_policy_or_channels(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    _create_minimal_v1_database(path)

    repository = GuardianRepository(path)
    await repository.initialize()
    await repository.initialize()

    policy = await repository.get_policy()
    channel = await repository.get_channel("channel-legacy")
    with sqlite3.connect(path) as connection:
        version = connection.execute(
            "SELECT value FROM guardian_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        row = connection.execute(
            "SELECT confidence, freshness_state, warmup_buckets "
            "FROM guardian_channels WHERE channel_id = 'channel-legacy'"
        ).fetchone()

    assert version == str(repository.SCHEMA_VERSION)
    assert policy.observe_only is True
    assert policy.sampling.mode.value == "SHARED"
    assert channel is not None
    assert channel["score"] == 91.5
    assert row == (0.0, "EXPIRED", 0)


@pytest.mark.asyncio
async def test_failed_v2_migration_rolls_back_and_keeps_v1_version(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    _create_minimal_v1_database(path)

    class FailingMigrationRepository(GuardianRepository):
        @staticmethod
        def _migrate_v1_to_v2_sync(connection: sqlite3.Connection) -> None:
            connection.execute("CREATE TABLE migration_should_rollback(value TEXT)")
            raise RuntimeError("migration failed")

    with pytest.raises(RuntimeError, match="migration failed"):
        await FailingMigrationRepository(path).initialize()

    with sqlite3.connect(path) as connection:
        version = connection.execute(
            "SELECT value FROM guardian_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        sentinel = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'migration_should_rollback'"
        ).fetchone()

    assert version == "1"
    assert sentinel is None


@pytest.mark.asyncio
async def test_repository_persists_channels_samples_and_paginated_events(
    tmp_path: Path,
) -> None:
    repository = GuardianRepository(tmp_path / "state.db")
    await repository.initialize()
    now = datetime(2026, 8, 23, 8, tzinfo=UTC)

    await repository.upsert_channel(
        channel_id="channel-1",
        name="Claude",
        group_id="group-1",
        upstream_status="operational",
        upstream_schedulable=True,
        health=GuardianHealth.HEALTHY,
        score=98.5,
        latency_ms=1234,
        desired_schedulable=True,
        manual_control=ManualControl.NONE,
        details={"reason": "healthy"},
        seen_at=now,
    )
    await repository.append_sample(
        GuardianSample(
            channel_id="channel-1",
            event_type=GuardianEventType.PERFECT,
            score=100,
            occurred_at=now,
            source=GuardianSampleSource.PROBE,
            ttfb_ms=1234,
        )
    )
    first = await repository.add_event(
        event_type="CHANNEL_SYNCED",
        severity="INFO",
        channel_id="channel-1",
        group_id="group-1",
        message="channel synced",
        details={"score": 98.5},
    )
    await repository.add_event(
        event_type="CHANNEL_HEALTHY",
        severity="INFO",
        channel_id="channel-1",
        group_id="group-1",
        message="channel healthy",
        details={},
    )

    channel = await repository.get_channel("channel-1")
    samples = await repository.list_samples("channel-1", limit=10)
    page = await repository.list_events(limit=1)
    next_page = await repository.list_events(limit=10, cursor=page["next_cursor"])

    assert channel is not None
    assert channel["manual_control"] == "NONE"
    assert samples[0].event_type is GuardianEventType.PERFECT
    assert len(page["items"]) == 1
    assert page["next_cursor"] is not None
    event_ids = {
        page["items"][0]["event_id"],
        next_page["items"][0]["event_id"],
    }
    assert first["event_id"] in event_ids


@pytest.mark.asyncio
async def test_run_idempotency_and_restart_durability(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    repository = GuardianRepository(path)
    await repository.initialize()

    first = await repository.create_run(dry_run=True, idempotency_key="run-once")
    repeated = await repository.create_run(dry_run=True, idempotency_key="run-once")
    await repository.finish_run(
        first["run_id"],
        status="SUCCEEDED",
        result={"channels": 0, "writes": 0},
    )

    reopened = GuardianRepository(path)
    await reopened.initialize()
    loaded = await reopened.get_run(first["run_id"])

    assert repeated["run_id"] == first["run_id"]
    assert loaded is not None
    assert loaded["status"] == "SUCCEEDED"
    assert loaded["result"]["writes"] == 0


def test_policy_json_round_trip_is_strict() -> None:
    policy = GuardianPolicy()

    restored = GuardianPolicy.model_validate_json(policy.model_dump_json())

    assert restored == policy


@pytest.mark.asyncio
async def test_channel_override_is_durable_and_attached_to_channel(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    repository = GuardianRepository(path)
    await repository.initialize()
    now = datetime(2026, 8, 23, 8, tzinfo=UTC)
    await repository.upsert_channel(
        channel_id="11",
        name="Claude",
        group_id="3",
        upstream_status="operational",
        upstream_schedulable=True,
        health=GuardianHealth.HEALTHY,
        score=100,
        latency_ms=100,
        desired_schedulable=True,
        manual_control=ManualControl.NONE,
        details={},
        seen_at=now,
    )
    override = ChannelPolicyOverride(
        priority=2,
        load_factor=80,
        concurrency=4,
        schedule_multiplier=1.25,
        probe_model="claude-test",
    )

    await repository.upsert_channel_override("11", override)
    reopened = GuardianRepository(path)
    await reopened.initialize()
    channel = await reopened.get_channel("11")

    assert channel is not None
    assert channel["override"]["priority"] == 2
    assert channel["override"]["schedule_multiplier"] == 1.25
