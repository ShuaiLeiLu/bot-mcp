"""Adapter from durable video jobs to the existing validated video client."""

from __future__ import annotations

from typing import Protocol

from client_errors import MonitorRequestError
from video import VideoGenerationClient, VideoGenerationOptions

from ..contracts import SubmitVideoInput, VideoOutput
from ..errors import ServiceError


class VideoGenerator(Protocol):
    async def generate(self, request: SubmitVideoInput) -> VideoOutput: ...


class LegacyVideoGenerator:
    def __init__(self, endpoint: str) -> None:
        self._client = VideoGenerationClient(endpoint=endpoint)

    async def generate(self, request: SubmitVideoInput) -> VideoOutput:
        try:
            result = await self._client.generate(
                request.prompt,
                VideoGenerationOptions(
                    length=request.length,
                    width=request.width,
                    height=request.height,
                    steps=request.steps,
                ),
            )
        except MonitorRequestError as exc:
            raise ServiceError(
                "VIDEO_UPSTREAM_FAILED",
                "Video generation failed",
            ) from exc
        return VideoOutput(url=result.url, filename=result.filename)

