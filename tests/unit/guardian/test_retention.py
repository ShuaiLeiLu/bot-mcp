from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from sub2api_mcp.guardian.repository import GuardianRepository

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_retention_is_age_scoped_and_globally_batch_bounded(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    repository = GuardianRepository(path, clock=lambda: NOW)
    await repository.initialize()
    old = "2026-01-01T00:00:00+00:00"
    recent = "2026-08-24T11:00:00+00:00"
    with sqlite3.connect(path) as connection:
        for index in range(3):
            connection.execute(
                "INSERT INTO guardian_samples(sample_id, channel_id, source, event_type, "
                "score, occurred_at, message, legacy) VALUES(?, 'c1', 'TRAFFIC', "
                "'PERFECT', 100, ?, '', 0)",
                (f"old-sample-{index}", old),
            )
        connection.execute(
            "INSERT INTO guardian_samples(sample_id, channel_id, source, event_type, "
            "score, occurred_at, message, legacy) VALUES('recent-sample', 'c1', "
            "'TRAFFIC', 'PERFECT', 100, ?, '', 0)",
            (recent,),
        )
        connection.execute(
            "INSERT INTO guardian_traffic_buckets(channel_id, bucket_at, event_count, "
            "score_sum, details_json, created_at, updated_at) VALUES"
            "('c1', ?, 1, 100, '{}', ?, ?)",
            (old, old, old),
        )
        connection.execute(
            "INSERT INTO guardian_input_snapshots(snapshot_id, schema_version, payload_json, "
            "payload_hash, captured_at, consumed_at, created_at) VALUES"
            "('old-snapshot', 2, '{}', 'hash', ?, ?, ?)",
            (old, old, old),
        )
        connection.execute(
            "INSERT INTO guardian_write_audits(audit_id, channel_id, action, reason, outcome, "
            "created_at) VALUES('audit-forever', 'c1', 'NONE', '', 'SKIPPED', ?)",
            (old,),
        )

    first = await repository.cleanup_retention(now=NOW, batch_size=2)
    assert first["deleted_total"] == 2
    assert first["processed_total"] == 2

    for _ in range(4):
        await repository.cleanup_retention(now=NOW, batch_size=2)

    with sqlite3.connect(path) as connection:
        old_samples = connection.execute(
            "SELECT COUNT(*) FROM guardian_samples WHERE sample_id LIKE 'old-%'"
        ).fetchone()[0]
        recent_samples = connection.execute(
            "SELECT COUNT(*) FROM guardian_samples WHERE sample_id = 'recent-sample'"
        ).fetchone()[0]
        old_buckets = connection.execute(
            "SELECT COUNT(*) FROM guardian_traffic_buckets WHERE bucket_at = ?", (old,)
        ).fetchone()[0]
        old_snapshots = connection.execute(
            "SELECT COUNT(*) FROM guardian_input_snapshots WHERE snapshot_id = 'old-snapshot'"
        ).fetchone()[0]
        audits = connection.execute(
            "SELECT COUNT(*) FROM guardian_write_audits WHERE audit_id = 'audit-forever'"
        ).fetchone()[0]

    assert (old_samples, old_buckets, old_snapshots) == (0, 0, 0)
    assert recent_samples == 1
    assert audits == 1
