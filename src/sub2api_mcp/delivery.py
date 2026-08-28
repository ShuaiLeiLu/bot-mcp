"""Build common LangBot MessageChains with explicit media fallback rules."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from typing import Any

from pydantic import ValidationError

from .adapters.langbot import (
    LangBotClient,
    LangBotRequestError,
    LangBotUnsupportedMediaError,
)
from .contracts import (
    DeliveryResult,
    DeliveryTargetRecord,
    MediaPolicy,
    NotificationPayload,
    OutboxPayload,
)
from .errors import ServiceError
from .logging import log_event
from .metrics import Metrics
from .repository import SqliteRepository


class DeliveryService:
    def __init__(self, client: LangBotClient) -> None:
        self._client = client

    async def deliver(
        self, target: DeliveryTargetRecord, payload: NotificationPayload
    ) -> DeliveryResult:
        preferred = self._preferred_chain(target.media_policy, payload)
        try:
            await self._client.send_message(target, preferred)
            return DeliveryResult(used_fallback=False)
        except LangBotUnsupportedMediaError:
            if target.media_policy is not MediaPolicy.AUTO or not self._has_media(payload):
                raise
            await self._client.send_message(target, self._text_chain(payload, include_links=True))
            return DeliveryResult(used_fallback=True)
        except LangBotRequestError as exc:
            if (
                exc.status_code != 500
                or target.media_policy is not MediaPolicy.AUTO
                or not self._has_media(payload)
            ):
                raise
            await self._client.send_message(target, self._text_chain(payload, include_links=True))
            return DeliveryResult(used_fallback=True)

    @classmethod
    def _preferred_chain(
        cls, policy: MediaPolicy, payload: NotificationPayload
    ) -> list[dict[str, Any]]:
        if policy is MediaPolicy.TEXT_ONLY:
            return cls._text_chain(payload, include_links=False)
        if policy is MediaPolicy.LINK:
            return cls._text_chain(payload, include_links=True)
        if policy in {MediaPolicy.AUTO, MediaPolicy.FILE} and payload.file_url:
            return [
                {"type": "File", "url": payload.file_url, "name": payload.file_name},
            ]
        if policy in {MediaPolicy.AUTO, MediaPolicy.IMAGE}:
            if payload.image_base64:
                return [
                    {
                        "type": "Image",
                        "base64": f"data:image/png;base64,{payload.image_base64}",
                    },
                ]
            if payload.image_url:
                return [
                    {"type": "Image", "url": payload.image_url},
                ]
        return cls._text_chain(payload, include_links=True)

    @staticmethod
    def _has_media(payload: NotificationPayload) -> bool:
        return bool(payload.file_url or payload.image_url or payload.image_base64)

    @staticmethod
    def _text_chain(
        payload: NotificationPayload, *, include_links: bool
    ) -> list[dict[str, Any]]:
        text = payload.text
        if include_links:
            links = [value for value in (payload.file_url, payload.image_url) if value]
            if links:
                text = "\n".join([text, *links])
        return [{"type": "Plain", "text": text}]


class OutboxWorker:
    RETRY_BASE_SECONDS = 30
    RETRY_MAX_SECONDS = 900

    def __init__(
        self,
        repository: SqliteRepository,
        delivery: DeliveryService,
        metrics: Metrics,
    ) -> None:
        self._repository = repository
        self._delivery = delivery
        self._metrics = metrics
        self._logger = logging.getLogger("sub2api_mcp")
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def run_once(self, worker_id: str) -> bool:
        claimed = await self._repository.claim_next_delivery(
            worker_id, lease_seconds=60
        )
        if claimed is None:
            return False
        try:
            payload = OutboxPayload.model_validate(claimed.payload)
            result = await self._delivery.deliver(claimed.target, payload.notification)
        except (ServiceError, ValidationError) as exc:
            error_code = exc.code if isinstance(exc, ServiceError) else "OUTBOX_PAYLOAD_INVALID"
            retryable = exc.retryable if isinstance(exc, ServiceError) else False
            retry_after_seconds: int | None = None
            if retryable:
                retry_after_seconds = self._retry_delay_seconds(claimed.attempt)
                await self._repository.mark_delivery_failed(
                    claimed.delivery_id,
                    error_code,
                    retry_after_seconds=retry_after_seconds,
                )
            else:
                await self._repository.mark_delivery_terminal(
                    claimed.delivery_id,
                    error_code,
                )
            self._metrics.upstream_calls.labels(
                dependency="langbot", status="retry" if retryable else "discarded"
            ).inc()
            backlog, terminal_failures = await self._refresh_outbox_metrics()
            log_event(
                self._logger,
                logging.WARNING,
                "delivery_failed",
                eventId=claimed.event_id,
                status="retrying" if retryable else "discarded",
                errorCode=error_code,
                attempt=claimed.attempt,
                queueDepth=backlog,
                terminalFailures=terminal_failures,
                nextRetrySeconds=retry_after_seconds,
            )
            return True
        await self._repository.mark_delivery_succeeded(claimed.delivery_id)
        await self._repository.finalize_event_if_required_delivered(claimed.event_id)
        self._metrics.upstream_calls.labels(
            dependency="langbot",
            status="fallback" if result.used_fallback else "ok",
        ).inc()
        backlog, terminal_failures = await self._refresh_outbox_metrics()
        log_event(
            self._logger,
            logging.INFO,
            "delivery_finished",
            eventId=claimed.event_id,
            status="fallback" if result.used_fallback else "ok",
            attempt=claimed.attempt,
            queueDepth=backlog,
            terminalFailures=terminal_failures,
        )
        return True

    @classmethod
    def _retry_delay_seconds(cls, attempt: int) -> int:
        exponent = max(0, min(attempt - 1, 5))
        return min(cls.RETRY_BASE_SECONDS * (2**exponent), cls.RETRY_MAX_SECONDS)

    async def _refresh_outbox_metrics(self) -> tuple[int, int]:
        backlog = await self._repository.outbox_backlog()
        terminal_failures = await self._repository.outbox_terminal_failure_count()
        self._metrics.outbox_backlog.set(backlog)
        self._metrics.outbox_terminal_failures.set(terminal_failures)
        return backlog, terminal_failures

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        await self._refresh_outbox_metrics()
        worker_id = f"delivery-{uuid.uuid4()}"
        self._task = asyncio.create_task(self._loop(worker_id), name=worker_id)

    async def _loop(self, worker_id: str) -> None:
        while not self._stop.is_set():
            handled = await self.run_once(worker_id)
            if not handled:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=0.5)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None
