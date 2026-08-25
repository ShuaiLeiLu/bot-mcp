from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sub2api_mcp.contracts import (
    AccountQuarantineReason,
    AccountQuarantineRecord,
    DeliveryPurpose,
    DeliveryTargetCreate,
    JobStatus,
    JobType,
    MediaPolicy,
    OutboxEventType,
    OutboxPayload,
    QuarantineProbeResult,
    TargetType,
)
from sub2api_mcp.errors import ServiceError
from sub2api_mcp.repository import SqliteRepository


class MutableClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


async def _repo(tmp_path: Path, clock: MutableClock) -> SqliteRepository:
    repository = SqliteRepository(tmp_path / "state.db", clock=clock)
    await repository.initialize()
    return repository


@pytest.mark.asyncio
async def test_running_jobs_become_interrupted_after_restart(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    created = await repository.create_job(JobType.VIDEO, {"prompt": "cat"})
    claimed = await repository.claim_next_job({JobType.VIDEO}, "worker-1")
    assert claimed is not None
    assert claimed.status is JobStatus.RUNNING

    restarted = await _repo(tmp_path, clock)
    job = await restarted.get_job(created.job_id)

    assert job is not None
    assert job.status is JobStatus.INTERRUPTED
    assert job.error_code == "SERVICE_RESTARTED"


@pytest.mark.asyncio
async def test_quarantine_round_trips_and_survives_restart(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    marker = AccountQuarantineRecord(
        account_id="997",
        reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
        group_ids=("36", "41"),
        threshold_ms=30_000,
        observed_count=3,
        quarantined_at=clock.now,
    )

    saved = await repository.upsert_account_quarantine(marker)
    clock.now += timedelta(minutes=1)
    probed = await repository.update_account_quarantine_probe(
        "997",
        probed_at=clock.now,
        latency_ms=45_001,
        result=QuarantineProbeResult.SLOW,
    )
    restarted = await _repo(tmp_path, clock)
    page = await restarted.list_account_quarantines(limit=20)

    assert saved == marker
    assert probed.last_probe_at == clock.now
    assert probed.last_probe_latency_ms == 45_001
    assert probed.last_probe_result is QuarantineProbeResult.SLOW
    assert page.items == [probed]
    assert await restarted.account_quarantine_count() == 1
    assert await restarted.remove_verified_account_quarantine("997") is True
    assert await restarted.remove_verified_account_quarantine("997") is False
    assert await restarted.account_quarantine_count() == 0


@pytest.mark.asyncio
async def test_quarantine_listing_is_bounded_and_cursor_paginated(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    for account_id in ("101", "102"):
        await repository.upsert_account_quarantine(
            AccountQuarantineRecord(
                account_id=account_id,
                reason=AccountQuarantineReason.CHANNEL_TEST_FAILED,
                group_ids=("36",),
                threshold_ms=30_000,
                observed_count=1,
                quarantined_at=clock.now,
            )
        )

    first = await repository.list_account_quarantines(limit=1)
    second = await repository.list_account_quarantines(
        limit=1,
        cursor=first.next_cursor,
    )

    assert [item.account_id for item in first.items] == ["101"]
    assert [item.account_id for item in second.items] == ["102"]
    assert first.next_cursor is not None

    with pytest.raises(ServiceError) as invalid_page:
        await repository.list_account_quarantines(limit=101)
    assert invalid_page.value.code == "INVALID_PAGE_SIZE"


@pytest.mark.asyncio
async def test_quarantine_probe_selection_rotates_oldest_observations(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    for account_id in ("101", "102", "103", "104", "105", "106"):
        await repository.upsert_account_quarantine(
            AccountQuarantineRecord(
                account_id=account_id,
                reason=AccountQuarantineReason.SLOW_FIRST_TOKEN,
                group_ids=("36",),
                threshold_ms=30_000,
                observed_count=3,
                quarantined_at=clock.now,
            )
        )

    first = await repository.list_account_quarantines_for_probe(limit=5)
    clock.now += timedelta(minutes=1)
    await repository.update_account_quarantine_probe(
        "101",
        probed_at=clock.now,
        latency_ms=45_000,
        result=QuarantineProbeResult.SLOW,
    )
    rotated = await repository.list_account_quarantines_for_probe(limit=5)

    assert [item.account_id for item in first] == ["101", "102", "103", "104", "105"]
    assert [item.account_id for item in rotated] == ["102", "103", "104", "105", "106"]

    with pytest.raises(ServiceError) as invalid_limit:
        await repository.list_account_quarantines_for_probe(limit=6)
    assert invalid_limit.value.code == "INVALID_PAGE_SIZE"


@pytest.mark.asyncio
async def test_quarantine_invalid_persisted_groups_fail_closed(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    with sqlite3.connect(repository.path) as connection:
        connection.execute(
            "INSERT INTO account_quarantines("
            "account_id, reason, group_ids_json, threshold_ms, observed_count, "
            "quarantined_at, last_probe_result, updated_at"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "997",
                AccountQuarantineReason.SLOW_FIRST_TOKEN.value,
                '["not-an-id"]',
                30_000,
                3,
                clock.now.isoformat(),
                QuarantineProbeResult.NEVER.value,
                clock.now.isoformat(),
            ),
        )

    with pytest.raises(ServiceError) as invalid_record:
        await repository.list_account_quarantines(limit=20)

    assert invalid_record.value.code == "QUARANTINE_DATA_INVALID"


@pytest.mark.asyncio
async def test_schema_v2_success_probe_state_migrates_to_recovered(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    quarantined_at = datetime(2026, 8, 25, 2, tzinfo=UTC).isoformat()
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE service_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO service_metadata(key, value) VALUES('schema_version', '2');
            CREATE TABLE account_quarantines (
                account_id TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                group_ids_json TEXT NOT NULL,
                threshold_ms INTEGER NOT NULL,
                observed_count INTEGER NOT NULL,
                quarantined_at TEXT NOT NULL,
                last_probe_at TEXT,
                last_probe_latency_ms INTEGER,
                last_probe_result TEXT NOT NULL CHECK (
                    last_probe_result IN ('NEVER', 'SUCCESS', 'FAILED', 'SLOW', 'INVALID')
                ),
                updated_at TEXT NOT NULL
            );
            CREATE INDEX idx_account_quarantines_probe
                ON account_quarantines(last_probe_at, quarantined_at, account_id);
            """
        )
        connection.execute(
            "INSERT INTO account_quarantines VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "997",
                "SLOW_FIRST_TOKEN",
                '["7"]',
                30_000,
                3,
                quarantined_at,
                quarantined_at,
                2_000,
                "SUCCESS",
                quarantined_at,
            ),
        )

    repository = SqliteRepository(path)
    await repository.initialize()
    marker = await repository.get_account_quarantine("997")
    with sqlite3.connect(path) as connection:
        version = connection.execute(
            "SELECT value FROM service_metadata WHERE key = 'schema_version'"
        ).fetchone()

    assert version == ("3",)
    assert marker is not None
    assert marker.last_probe_result is QuarantineProbeResult.RECOVERED


@pytest.mark.asyncio
async def test_only_one_worker_can_claim_a_job(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    await repository.create_job(JobType.PROBE, {})

    claimed = await asyncio.gather(
        repository.claim_next_job({JobType.PROBE}, "worker-1"),
        repository.claim_next_job({JobType.PROBE}, "worker-2"),
    )

    assert sum(item is not None for item in claimed) == 1


@pytest.mark.asyncio
async def test_job_listing_uses_an_opaque_cursor(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    first = await repository.create_job(JobType.PROBE, {})
    clock.now += timedelta(seconds=1)
    second = await repository.create_job(JobType.VIDEO, {"prompt": "cat"})

    page_one = await repository.list_jobs(limit=1)
    page_two = await repository.list_jobs(limit=1, cursor=page_one.next_cursor)

    assert [item.job_id for item in page_one.items] == [second.job_id]
    assert [item.job_id for item in page_two.items] == [first.job_id]
    assert page_one.next_cursor is not None


@pytest.mark.asyncio
async def test_scheduler_lease_excludes_another_owner_until_expiry(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)

    assert await repository.acquire_scheduler_lease("owner-a", lease_seconds=60) is True
    assert await repository.acquire_scheduler_lease("owner-b", lease_seconds=60) is False
    clock.now += timedelta(seconds=61)
    assert await repository.acquire_scheduler_lease("owner-b", lease_seconds=60) is True


@pytest.mark.asyncio
async def test_delivery_targets_are_platform_neutral(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)

    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="future-adapter-target",
            bot_uuid="bot-uuid",
            target_type=TargetType.GROUP,
            target_id="opaque-platform-id",
            purposes=frozenset({DeliveryPurpose.STATUS, DeliveryPurpose.VIDEO_RESULT}),
            media_policy=MediaPolicy.AUTO,
            required=True,
        )
    )
    targets = await repository.list_delivery_targets()

    assert targets == [target]
    assert targets[0].target_id == "opaque-platform-id"


@pytest.mark.asyncio
async def test_delivery_target_listing_is_cursor_paginated(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    for name in ("alpha", "beta"):
        await repository.upsert_delivery_target(
            DeliveryTargetCreate(
                name=name,
                bot_uuid="bot-uuid",
                target_type=TargetType.PERSON,
                target_id=f"{name}-id",
                purposes=frozenset({DeliveryPurpose.STATUS}),
                media_policy=MediaPolicy.TEXT_ONLY,
            )
        )

    first = await repository.list_delivery_targets_page(limit=1)
    second = await repository.list_delivery_targets_page(
        limit=1, cursor=first.next_cursor
    )

    assert [item.name for item in first.items] == ["alpha"]
    assert [item.name for item in second.items] == ["beta"]
    assert first.next_cursor is not None


@pytest.mark.asyncio
async def test_job_listing_filters_by_type_and_status(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    await repository.create_job(JobType.PROBE, {})
    await repository.create_job(JobType.VIDEO, {"prompt": "cat"})

    page = await repository.list_jobs(
        limit=20,
        job_type=JobType.VIDEO,
        status=JobStatus.QUEUED,
    )

    assert [item.job_type for item in page.items] == [JobType.VIDEO]


@pytest.mark.asyncio
async def test_binding_registry_enforces_one_to_one_relationship(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    await repository.bind_actor("actor-a", "user-1", "a***@example.com")

    with pytest.raises(ServiceError) as duplicate_actor:
        await repository.bind_actor("actor-a", "user-2", "b***@example.com")
    with pytest.raises(ServiceError) as duplicate_user:
        await repository.bind_actor("actor-b", "user-1", "a***@example.com")

    assert duplicate_actor.value.code == "BINDING_CONFLICT"
    assert duplicate_user.value.code == "BINDING_CONFLICT"


@pytest.mark.asyncio
async def test_outbox_tracks_delivery_per_target(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="admin",
            bot_uuid="bot-uuid",
            target_type=TargetType.PERSON,
            target_id="person-id",
            purposes=frozenset({DeliveryPurpose.RECOVERY_ADMIN}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    event = await repository.enqueue_outbox(
        OutboxEventType.RECOVERY_RESULT,
        {"text": "recovered"},
        [target.delivery_target_id],
    )

    delivery = await repository.claim_next_delivery("delivery-worker", lease_seconds=30)
    assert delivery is not None
    assert delivery.event_id == event.event_id
    await repository.mark_delivery_succeeded(delivery.delivery_id)

    assert await repository.outbox_backlog() == 0


@pytest.mark.asyncio
async def test_new_status_event_supersedes_older_undelivered_status_for_target(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="status",
            bot_uuid="bot-uuid",
            target_type=TargetType.PERSON,
            target_id="person-id",
            purposes=frozenset({DeliveryPurpose.STATUS}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    first = await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {"notification": {"text": "old status"}},
        [target.delivery_target_id],
    )
    latest = await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {"notification": {"text": "latest status"}},
        [target.delivery_target_id],
    )

    delivery = await repository.claim_next_delivery("delivery-worker", lease_seconds=30)

    assert first.event_id != latest.event_id
    assert await repository.outbox_backlog() == 1
    assert delivery is not None
    assert delivery.event_id == latest.event_id
    assert delivery.payload["notification"]["text"] == "latest status"


@pytest.mark.asyncio
async def test_new_status_event_supersedes_an_older_failed_attempt(tmp_path: Path) -> None:
    clock = MutableClock()
    repository = await _repo(tmp_path, clock)
    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="status",
            bot_uuid="bot-uuid",
            target_type=TargetType.PERSON,
            target_id="person-id",
            purposes=frozenset({DeliveryPurpose.STATUS}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {"notification": {"text": "failed status"}},
        [target.delivery_target_id],
    )
    failed = await repository.claim_next_delivery("delivery-worker", lease_seconds=30)
    assert failed is not None
    await repository.mark_delivery_failed(
        failed.delivery_id,
        "UPSTREAM_UNAVAILABLE",
        retry_after_seconds=300,
    )

    latest = await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {"notification": {"text": "latest status"}},
        [target.delivery_target_id],
    )
    delivery = await repository.claim_next_delivery("delivery-worker", lease_seconds=30)

    assert await repository.outbox_backlog() == 1
    assert delivery is not None
    assert delivery.event_id == latest.event_id


@pytest.mark.asyncio
async def test_status_events_with_distinct_coalesce_keys_are_both_preserved(
    tmp_path: Path,
) -> None:
    repository = await _repo(tmp_path, MutableClock())
    target = await repository.upsert_delivery_target(
        DeliveryTargetCreate(
            name="status",
            bot_uuid="bot-uuid",
            target_type=TargetType.PERSON,
            target_id="person-id",
            purposes=frozenset({DeliveryPurpose.STATUS}),
            media_policy=MediaPolicy.TEXT_ONLY,
            required=True,
        )
    )
    await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {"coalesceKey": "guardian:run", "notification": {"text": "run"}},
        [target.delivery_target_id],
    )
    await repository.enqueue_outbox(
        OutboxEventType.STATUS_CHANGED,
        {"coalesceKey": "guardian:budget", "notification": {"text": "budget"}},
        [target.delivery_target_id],
    )

    assert await repository.outbox_backlog() == 2
    first = await repository.claim_next_delivery("delivery-worker", lease_seconds=30)
    assert first is not None
    parsed = OutboxPayload.model_validate(first.payload)
    assert parsed.coalesce_key is not None
    assert parsed.coalesce_key.startswith("guardian:")
