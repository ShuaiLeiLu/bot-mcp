"""Platform-neutral LangBot bot discovery and message delivery client."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from ..contracts import DeliveryTargetRecord, LangBotBot
from ..errors import ServiceError


class _Envelope(BaseModel):
    model_config = ConfigDict(extra="ignore")

    code: int
    data: dict[str, Any] | None = None
    msg: str | None = None
    message: str | None = None


class _BotItem(BaseModel):
    model_config = ConfigDict(extra="ignore")

    uuid: str
    name: str | None = None
    adapter: str


class _BotsData(BaseModel):
    model_config = ConfigDict(extra="ignore")

    bots: list[_BotItem]


class LangBotRequestError(ServiceError):
    def __init__(self, *, retryable: bool) -> None:
        super().__init__(
            "LANGBOT_REQUEST_FAILED",
            "LangBot could not deliver the message",
            retryable=retryable,
        )


class LangBotUnsupportedMediaError(ServiceError):
    def __init__(self) -> None:
        super().__init__(
            "LANGBOT_MEDIA_UNSUPPORTED",
            "The selected LangBot adapter does not support this media type",
        )


class LangBotClient:
    MAX_RESPONSE_BYTES = 2 * 1024 * 1024

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout_seconds: float = 15,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": api_key, "Accept": "application/json"},
            timeout=timeout_seconds,
            follow_redirects=False,
            transport=transport,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def list_bots(self) -> list[LangBotBot]:
        payload = await self._request_json("GET", "/api/v1/platform/bots")
        data = self._require_success(payload)
        try:
            parsed = _BotsData.model_validate(data)
        except ValidationError as exc:
            raise LangBotRequestError(retryable=False) from exc
        return [
            LangBotBot(uuid=item.uuid, name=item.name or item.uuid, adapter=item.adapter)
            for item in parsed.bots
        ]

    async def send_message(
        self,
        target: DeliveryTargetRecord,
        message_chain: list[dict[str, Any]],
    ) -> None:
        payload = await self._request_json(
            "POST",
            f"/api/v1/platform/bots/{quote(target.bot_uuid, safe='')}/send_message",
            json_body={
                "target_type": target.target_type.value,
                "target_id": target.target_id,
                "message_chain": message_chain,
            },
        )
        self._require_success(payload)

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
    ) -> _Envelope:
        try:
            response = await self._client.request(method, path, json=json_body)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            raise LangBotRequestError(retryable=True) from exc
        if len(response.content) > self.MAX_RESPONSE_BYTES:
            raise LangBotRequestError(retryable=False)
        try:
            payload: object = response.json()
        except (json.JSONDecodeError, UnicodeError) as exc:
            raise LangBotRequestError(retryable=response.status_code >= 500) from exc
        try:
            envelope = _Envelope.model_validate(payload)
        except ValidationError as exc:
            raise LangBotRequestError(retryable=response.status_code >= 500) from exc
        if response.status_code in {400, 422} and self._is_unsupported_media(envelope):
            raise LangBotUnsupportedMediaError
        if response.status_code >= 400:
            raise LangBotRequestError(retryable=response.status_code >= 500)
        return envelope

    @staticmethod
    def _is_unsupported_media(payload: _Envelope) -> bool:
        message = str(payload.msg or payload.message or "").casefold()
        markers = ("unsupported", "not support", "不支持")
        return any(marker in message for marker in markers) and any(
            media in message for media in ("media", "file", "image", "video", "message type")
        )

    @staticmethod
    def _require_success(payload: _Envelope) -> dict[str, Any]:
        if payload.code != 0 or payload.data is None:
            raise LangBotRequestError(retryable=False)
        return payload.data
