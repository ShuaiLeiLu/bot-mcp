"""Signed, platform-neutral actor commands for deterministic user operations."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .errors import ServiceError
from .repository import SqliteRepository


class StrictActorModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ActorIdentity(StrictActorModel):
    workspace_uuid: str = Field(min_length=1, max_length=128)
    bot_uuid: str = Field(min_length=1, max_length=128)
    adapter: str = Field(min_length=1, max_length=128)
    launcher_type: str = Field(min_length=1, max_length=32)
    launcher_id: str = Field(min_length=1, max_length=512)


class ActorCommand(StrEnum):
    BIND = "BIND"
    UNBIND = "UNBIND"
    ACCOUNT = "ACCOUNT"


class ActorBridgeRequest(StrictActorModel):
    identity: ActorIdentity
    command: ActorCommand
    email: str | None = Field(default=None, max_length=320)

    @model_validator(mode="after")
    def validate_command_parameters(self) -> ActorBridgeRequest:
        if self.command is ActorCommand.BIND and not self.email:
            raise ValueError("email is required for BIND")
        if self.command is not ActorCommand.BIND and self.email is not None:
            raise ValueError("email is only accepted for BIND")
        return self


class ActorBridgeResponse(StrictActorModel):
    text: str


class ActorAccount(StrictActorModel):
    user_id: str
    email_masked: str
    status: str


class ActorAccountProvider(Protocol):
    async def find_active_account(self, email: str) -> ActorAccount | None: ...

    async def account_report(self, user_id: str) -> str: ...


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _canonical_identity(identity: ActorIdentity) -> bytes:
    return json.dumps(
        identity.model_dump(mode="json"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def actor_key(identity: ActorIdentity, secret: str) -> str:
    digest = hmac.new(secret.encode(), _canonical_identity(identity), hashlib.sha256).hexdigest()
    return f"v1:{digest}"


def sign_actor_request(secret: str, timestamp: str, nonce: str, body: bytes) -> str:
    signed = b".".join((timestamp.encode(), nonce.encode(), body))
    return hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()


class ActorRequestVerifier:
    def __init__(
        self,
        repository: SqliteRepository,
        secret: str,
        *,
        replay_window_seconds: int,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._repository = repository
        self._secret = secret
        self._replay_window_seconds = replay_window_seconds
        self._clock = clock

    async def verify(
        self,
        timestamp: str,
        nonce: str,
        body: bytes,
        signature: str,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z0-9._-]{12,128}", nonce):
            raise ServiceError("ACTOR_NONCE_INVALID", "The actor nonce is invalid")
        try:
            requested_at = datetime.fromtimestamp(int(timestamp), tz=UTC)
        except (OverflowError, TypeError, ValueError) as exc:
            raise ServiceError(
                "ACTOR_TIMESTAMP_INVALID", "The actor timestamp is invalid"
            ) from exc
        current = self._clock().astimezone(UTC)
        if abs((current - requested_at).total_seconds()) > self._replay_window_seconds:
            raise ServiceError("ACTOR_TIMESTAMP_INVALID", "The actor timestamp is stale")
        expected = sign_actor_request(self._secret, timestamp, nonce, body)
        if not hmac.compare_digest(expected, signature):
            raise ServiceError("ACTOR_SIGNATURE_INVALID", "The actor signature is invalid")
        claimed = await self._repository.claim_actor_nonce(
            nonce,
            current + timedelta(seconds=self._replay_window_seconds),
            claimed_at=current,
        )
        if not claimed:
            raise ServiceError("ACTOR_REPLAY", "The actor request was already used")


def _normalize_email(value: str) -> str:
    normalized = value.strip().casefold()
    if len(normalized) > 320 or not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", normalized):
        raise ServiceError("EMAIL_INVALID", "The email address is invalid")
    return normalized


class ActorService:
    def __init__(
        self,
        repository: SqliteRepository,
        provider: ActorAccountProvider,
        *,
        actor_secret: str,
    ) -> None:
        self._repository = repository
        self._provider = provider
        self._actor_secret = actor_secret

    async def execute(self, request: ActorBridgeRequest) -> ActorBridgeResponse:
        key = actor_key(request.identity, self._actor_secret)
        if request.command is ActorCommand.BIND:
            assert request.email is not None
            account = await self._provider.find_active_account(_normalize_email(request.email))
            if account is None or account.status != "active":
                raise ServiceError(
                    "ACCOUNT_NOT_BINDABLE",
                    "The account does not exist or is not active",
                )
            await self._repository.bind_actor(
                key,
                account.user_id,
                account.email_masked,
            )
            return ActorBridgeResponse(text=f"绑定成功：{account.email_masked}。")
        if request.command is ActorCommand.UNBIND:
            await self._repository.unbind_actor(key)
            return ActorBridgeResponse(text="绑定已解除。")
        binding = await self._repository.get_binding(key)
        if binding is None:
            raise ServiceError("ACCOUNT_NOT_BOUND", "The platform user has no binding")
        return ActorBridgeResponse(text=await self._provider.account_report(binding.user_id))

