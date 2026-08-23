from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from sub2api_mcp.contracts import (
    DeliveryPurpose,
    DeliveryTargetCreate,
    JobStatus,
    JobType,
    MediaPolicy,
    OutboxEventType,
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
