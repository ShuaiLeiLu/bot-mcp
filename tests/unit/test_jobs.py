from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from sub2api_mcp.contracts import JobStatus, JobType, SubmitVideoInput, VideoOutput
from sub2api_mcp.errors import ServiceError
from sub2api_mcp.jobs import JobManager, VideoJobService
from sub2api_mcp.metrics import Metrics
from sub2api_mcp.repository import SqliteRepository


@dataclass
class FakeVideoGenerator:
    output: VideoOutput | None = None
    error: ServiceError | None = None

    async def generate(self, request: SubmitVideoInput) -> VideoOutput:
        if self.error is not None:
            raise self.error
        assert self.output is not None
        return self.output


async def _repository(tmp_path: Path) -> SqliteRepository:
    repository = SqliteRepository(tmp_path / "state.db")
    await repository.initialize()
    return repository


@pytest.mark.asyncio
async def test_video_submission_returns_job_and_current_queue_count(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    service = VideoJobService(
        repository,
        FakeVideoGenerator(output=VideoOutput(url="https://video.example/a.mp4", filename="a.mp4")),
        max_pending=20,
    )

    first = await service.submit(SubmitVideoInput(prompt="cat"))
    second = await service.submit(SubmitVideoInput(prompt="dog"))

    assert first.queue_count == 1
    assert second.queue_count == 2
    assert second.job.status is JobStatus.QUEUED


@pytest.mark.asyncio
async def test_video_queue_limit_is_enforced_before_insertion(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    service = VideoJobService(
        repository,
        FakeVideoGenerator(output=VideoOutput(url="https://video.example/a.mp4", filename="a.mp4")),
        max_pending=1,
    )
    await service.submit(SubmitVideoInput(prompt="cat"))

    with pytest.raises(ServiceError) as captured:
        await service.submit(SubmitVideoInput(prompt="dog"))

    assert captured.value.code == "VIDEO_QUEUE_FULL"
    assert await repository.active_job_count(JobType.VIDEO) == 1


@pytest.mark.asyncio
async def test_worker_completes_video_job_with_validated_result(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    service = VideoJobService(
        repository,
        FakeVideoGenerator(
            output=VideoOutput(url="https://video.example/outputs/a.mp4", filename="a.mp4")
        ),
        max_pending=20,
    )
    submitted = await service.submit(SubmitVideoInput(prompt="cat"))
    manager = JobManager(repository, Metrics.create())
    manager.register(JobType.VIDEO, service.handle)

    assert await manager.run_once({JobType.VIDEO}, "video-worker-1") is True
    completed = await repository.get_job(submitted.job.job_id)

    assert completed is not None
    assert completed.status is JobStatus.SUCCEEDED
    assert completed.result == {
        "url": "https://video.example/outputs/a.mp4",
        "filename": "a.mp4",
    }


@pytest.mark.asyncio
async def test_explicit_video_failure_becomes_a_safe_terminal_error(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    service = VideoJobService(
        repository,
        FakeVideoGenerator(
            error=ServiceError("VIDEO_UPSTREAM_FAILED", "Video generation failed")
        ),
        max_pending=20,
    )
    submitted = await service.submit(SubmitVideoInput(prompt="cat"))
    manager = JobManager(repository, Metrics.create())
    manager.register(JobType.VIDEO, service.handle)

    await manager.run_once({JobType.VIDEO}, "video-worker-1")
    failed = await repository.get_job(submitted.job.job_id)

    assert failed is not None
    assert failed.status is JobStatus.FAILED
    assert failed.error_code == "VIDEO_UPSTREAM_FAILED"
    assert failed.error_message == "Video generation failed"


@pytest.mark.asyncio
async def test_queued_video_job_can_be_cancelled_idempotently(tmp_path: Path) -> None:
    repository = await _repository(tmp_path)
    service = VideoJobService(
        repository,
        FakeVideoGenerator(output=VideoOutput(url="https://video.example/a.mp4", filename="a.mp4")),
        max_pending=20,
    )
    submitted = await service.submit(SubmitVideoInput(prompt="cat"))

    first = await repository.cancel_job(submitted.job.job_id)
    second = await repository.cancel_job(submitted.job.job_id)

    assert first.status is JobStatus.CANCELLED
    assert second.status is JobStatus.CANCELLED

