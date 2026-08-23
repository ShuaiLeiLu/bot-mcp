from __future__ import annotations

import asyncio
import json
import posixpath
import re
import time
from dataclasses import dataclass, field, replace
from typing import Any, Callable
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from client_errors import MonitorRequestError


class _RejectRedirectHandler(urllib_request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del newurl
        raise urllib_error.HTTPError(req.full_url, code, msg, headers, fp)


@dataclass(frozen=True, slots=True)
class VideoGenerationResult:
    url: str
    filename: str


@dataclass(frozen=True, slots=True)
class VideoGenerationOptions:
    length: int = 22
    width: int = 768
    height: int = 448
    steps: int = 20

    def __post_init__(self) -> None:
        _bounded_int(self.length, "length", 1, 3600)
        _bounded_int(self.width, "width", 64, 2048)
        _bounded_int(self.height, "height", 64, 2048)
        _bounded_int(self.steps, "steps", 1, 100)


class VideoQueueFullError(MonitorRequestError):
    """Raised when accepting another video would exceed the queue cap."""


@dataclass(frozen=True, slots=True)
class VideoQueueTicket:
    queue_count: int
    position: int
    _task: asyncio.Task[VideoGenerationResult] = field(repr=False, compare=False)

    async def wait(self) -> VideoGenerationResult:
        return await self._task


class VideoGenerationClient:
    """Client for the fixed-shape video generation endpoint.

    The endpoint response is external input.  The returned URL is only exposed
    when it is an HTTPS MP4 on the same host as the configured generation API.
    """

    DEFAULT_ENDPOINT = "https://h3.fzypod.com:9090/v1/video/generations"
    DEFAULT_LENGTH = 22
    DEFAULT_WIDTH = 768
    DEFAULT_HEIGHT = 448
    DEFAULT_STEPS = 20
    DEFAULT_TIMEOUT_SECONDS = 300
    MAX_PROMPT_LENGTH = 2000
    MAX_RESPONSE_BYTES = 1024 * 1024

    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        *,
        length: int = DEFAULT_LENGTH,
        width: int = DEFAULT_WIDTH,
        height: int = DEFAULT_HEIGHT,
        steps: int = DEFAULT_STEPS,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        retry_delay_seconds: int = 5,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        self.endpoint = self._validate_endpoint(endpoint)
        self.default_options = VideoGenerationOptions(
            length=length,
            width=width,
            height=height,
            steps=steps,
        )
        self.length = self.default_options.length
        self.width = self.default_options.width
        self.height = self.default_options.height
        self.steps = self.default_options.steps
        self.timeout_seconds = _bounded_int(timeout_seconds, "timeout_seconds", 10, 900)
        self.retry_delay_seconds = _bounded_int(retry_delay_seconds, "retry_delay_seconds", 0, 60)
        self._opener = opener or urllib_request.build_opener(
            _RejectRedirectHandler()
        ).open
        parsed_endpoint = urllib_parse.urlsplit(self.endpoint)
        self._output_origin = (
            parsed_endpoint.hostname,
            parsed_endpoint.port or 443,
        )

    async def generate(
        self,
        prompt: str,
        options: VideoGenerationOptions | None = None,
    ) -> VideoGenerationResult:
        normalized_prompt = _normalize_prompt(prompt)
        selected_options = options or self.default_options
        return await asyncio.to_thread(
            self._generate_sync,
            normalized_prompt,
            selected_options,
        )

    def parse_result(self, payload: Any) -> VideoGenerationResult:
        if not isinstance(payload, dict):
            raise ValueError("video generation response must be an object")
        data = payload.get("data")
        if not isinstance(data, list) or not data or not isinstance(data[0], dict):
            raise ValueError("video generation response data must contain an item")
        raw_url = data[0].get("url")
        if not isinstance(raw_url, str) or not raw_url.strip():
            raise ValueError("video generation response data[0].url is required")
        url = self._validate_output_url(raw_url.strip())
        filename = posixpath.basename(urllib_parse.urlsplit(url).path)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", filename):
            filename = "generated-video.mp4"
        return VideoGenerationResult(url=url, filename=filename)

    def _generate_sync(
        self,
        prompt: str,
        options: VideoGenerationOptions,
    ) -> VideoGenerationResult:
        body = json.dumps(
            {
                "prompt": prompt,
                "length": options.length,
                "width": options.width,
                "height": options.height,
                "steps": options.steps,
                "stream": False,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        request = urllib_request.Request(
            self.endpoint,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        retry_delay = self.retry_delay_seconds
        while True:
            try:
                # The upstream API keeps this synchronous request open until
                # the MP4 URL is ready. Do not impose a client-side deadline:
                # a long generation is still in progress, not a failure.
                with self._opener(request, timeout=None) as response:
                    response_body = response.read(self.MAX_RESPONSE_BYTES + 1)
                if len(response_body) > self.MAX_RESPONSE_BYTES:
                    raise MonitorRequestError("video generation response is too large")
                break
            except MonitorRequestError:
                raise
            except urllib_error.HTTPError as exc:
                # An explicit HTTP error is an upstream failure. Transport
                # errors below are retried because the server may still be
                # rendering the same synchronous request.
                detail = ""
                try:
                    detail = exc.read(4096).decode("utf-8", errors="replace")
                except (OSError, UnicodeError):
                    pass
                detail = " ".join(detail.split())[:512]
                message = f"video generation request failed (HTTP {exc.code})"
                if detail:
                    message = f"{message}: {detail}"
                raise MonitorRequestError(message) from exc
            except (urllib_error.URLError, TimeoutError, OSError):
                if retry_delay:
                    time.sleep(retry_delay)
                    retry_delay = min(max(retry_delay * 2, 1), 60)

        try:
            payload = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MonitorRequestError("video generation returned invalid JSON") from exc
        try:
            return self.parse_result(payload)
        except ValueError as exc:
            raise MonitorRequestError("video generation returned an invalid video URL") from exc

    @classmethod
    def _validate_endpoint(cls, endpoint: str) -> str:
        if not isinstance(endpoint, str):
            raise ValueError("video API endpoint must be a URL")
        normalized = endpoint.strip().rstrip("/")
        parsed = urllib_parse.urlsplit(normalized)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("video API endpoint must be an HTTPS URL without credentials")
        if parsed.port is None:
            return normalized
        if not 1 <= parsed.port <= 65535:
            raise ValueError("video API endpoint has an invalid port")
        return normalized

    def _validate_output_url(self, raw_url: str) -> str:
        parsed = urllib_parse.urlsplit(raw_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("video output URL must use HTTPS")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("video output URL cannot contain credentials")
        origin = (parsed.hostname, parsed.port or 443)
        if origin != self._output_origin:
            raise ValueError("video output URL host is not allowed")
        if not parsed.path.startswith("/outputs/") or parsed.path.endswith("/"):
            raise ValueError("video output URL path is not allowed")
        if not parsed.path.casefold().endswith(".mp4"):
            raise ValueError("video output URL must point to an MP4")
        if len(raw_url) > 2048:
            raise ValueError("video output URL is too long")
        return raw_url


class VideoGenerationQueue:
    """Bounded in-memory queue that limits expensive upstream generations."""

    def __init__(
        self,
        client: VideoGenerationClient,
        *,
        max_concurrency: int = 2,
        max_pending: int = 20,
    ) -> None:
        self.client = client
        self.max_concurrency = _bounded_int(max_concurrency, "concurrency", 1, 8)
        self.max_pending = _bounded_int(max_pending, "queue", 1, 100)
        self._semaphore = asyncio.Semaphore(self.max_concurrency)
        self._lock = asyncio.Lock()
        self._pending_count = 0
        self._tasks: set[asyncio.Task[VideoGenerationResult]] = set()

    @property
    def pending_count(self) -> int:
        return self._pending_count

    async def submit(
        self,
        prompt: str,
        options: VideoGenerationOptions,
    ) -> VideoQueueTicket:
        async with self._lock:
            if self._pending_count >= self.max_pending:
                raise VideoQueueFullError("video generation queue is full")
            self._pending_count += 1
            queue_count = self._pending_count
            task = asyncio.create_task(
                self._run(prompt, options),
                name="sub2api-video-generation",
            )
            self._tasks.add(task)
            task.add_done_callback(self._task_done)
        return VideoQueueTicket(queue_count, queue_count, task)

    async def _run(
        self,
        prompt: str,
        options: VideoGenerationOptions,
    ) -> VideoGenerationResult:
        try:
            async with self._semaphore:
                return await self.client.generate(prompt, options)
        finally:
            async with self._lock:
                self._pending_count -= 1

    def _task_done(self, task: asyncio.Task[VideoGenerationResult]) -> None:
        self._tasks.discard(task)
        if not task.cancelled():
            task.exception()


def parse_video_parameters(
    params: list[str],
    defaults: VideoGenerationOptions,
) -> tuple[VideoGenerationOptions, str]:
    """Parse flags before the prompt into validated generation options."""

    options = defaults
    prompt_tokens: list[str] = []
    index = 0
    aliases = {
        "--length": "length",
        "-l": "length",
        "--steps": "steps",
        "-s": "steps",
        "--resolution": "resolution",
        "--size": "resolution",
        "-r": "resolution",
    }
    while index < len(params):
        token = params[index]
        if token.startswith("--") and "=" in token:
            flag, raw_value = token.split("=", 1)
            field_name = aliases.get(flag)
            if field_name is None:
                raise ValueError("unknown video option")
            index += 1
        elif token in aliases:
            field_name = aliases[token]
            index += 1
            if index >= len(params):
                raise ValueError(f"video {field_name} value is required")
            raw_value = params[index]
            index += 1
        else:
            if token.startswith("-"):
                raise ValueError("unknown video option")
            prompt_tokens.extend(params[index:])
            break

        if field_name == "resolution":
            match = re.fullmatch(r"(\d+)[xX×](\d+)", raw_value)
            if match is None:
                raise ValueError("video resolution must be WIDTHxHEIGHT")
            options = replace(
                options,
                width=int(match.group(1)),
                height=int(match.group(2)),
            )
        else:
            try:
                parsed_value = int(raw_value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"video {field_name} must be an integer") from exc
            options = replace(options, **{field_name: parsed_value})

    prompt = " ".join(prompt_tokens).strip()
    _normalize_prompt(prompt)
    return options, prompt


def normalize_video_api_url(value: Any) -> str:
    """Validate and normalize the configured generation endpoint."""

    return VideoGenerationClient._validate_endpoint(value)


def _normalize_prompt(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("video prompt must be text")
    prompt = value.strip()
    if not prompt:
        raise ValueError("video prompt is required")
    if len(prompt) > VideoGenerationClient.MAX_PROMPT_LENGTH:
        raise ValueError("video prompt is too long")
    if any(ord(character) < 32 and character not in "\n\r\t" for character in prompt):
        raise ValueError("video prompt contains invalid control characters")
    return prompt


def _bounded_int(value: Any, field_name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool):
        raise ValueError(f"video {field_name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"video {field_name} must be an integer") from exc
    if not minimum <= parsed <= maximum:
        raise ValueError(f"video {field_name} must be between {minimum} and {maximum}")
    return parsed
