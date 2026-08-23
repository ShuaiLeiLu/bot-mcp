from __future__ import annotations

import hashlib
import json
import re
from collections import deque
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

from monitor import normalize_email


class BindingDataError(ValueError):
    """Raised when persisted account bindings fail validation."""


class BindingOutcome(str, Enum):
    CREATED = "created"
    ALREADY_BOUND = "already_bound"
    ACTOR_CONFLICT = "actor_conflict"
    ACCOUNT_CONFLICT = "account_conflict"


@dataclass(frozen=True, slots=True)
class AccountBinding:
    actor_key: str
    user_id: str
    email_masked: str
    bound_at: str


def binding_actor_key(
    bot_uuid: str | None,
    sender_id: str | int | None,
) -> str:
    if bot_uuid is None or sender_id is None:
        raise ValueError("invalid binding identity")
    bot = str(bot_uuid).strip()
    sender = str(sender_id).strip()
    if not bot or not sender or sender == "0" or len(bot) > 128 or len(sender) > 256:
        raise ValueError("invalid binding identity")
    return hashlib.sha256(f"{bot}\0{sender}".encode("utf-8")).hexdigest()


def mask_email(email: str) -> str:
    normalized = normalize_email(email)
    local_part, domain = normalized.split("@", 1)
    if len(local_part) == 1:
        masked_local = f"{local_part}***"
    else:
        masked_local = f"{local_part[0]}***{local_part[-1]}"
    return f"{masked_local}@{domain}"


class BindingRegistry:
    VERSION = 1
    MAX_BINDINGS = 10_000
    MAX_STORAGE_BYTES = 1024 * 1024

    def __init__(self, bindings: list[AccountBinding] | None = None):
        self._by_actor: dict[str, AccountBinding] = {}
        self._actor_by_account: dict[str, str] = {}
        for binding in bindings or []:
            self._add_loaded(binding)

    def _add_loaded(self, binding: AccountBinding) -> None:
        if binding.actor_key in self._by_actor:
            raise BindingDataError("duplicate actor binding")
        if binding.user_id in self._actor_by_account:
            raise BindingDataError("duplicate account binding")
        if len(self._by_actor) >= self.MAX_BINDINGS:
            raise BindingDataError("too many account bindings")
        self._by_actor[binding.actor_key] = binding
        self._actor_by_account[binding.user_id] = binding.actor_key

    def bind(
        self,
        actor_key: str,
        user_id: str | int,
        email: str,
        *,
        bound_at: str,
    ) -> BindingOutcome:
        normalized_actor = _validate_actor_key(actor_key)
        normalized_user_id = _validate_user_id(user_id)
        _validate_bound_at(bound_at)
        existing = self._by_actor.get(normalized_actor)
        if existing is not None:
            if existing.user_id == normalized_user_id:
                return BindingOutcome.ALREADY_BOUND
            return BindingOutcome.ACTOR_CONFLICT
        if normalized_user_id in self._actor_by_account:
            return BindingOutcome.ACCOUNT_CONFLICT
        if len(self._by_actor) >= self.MAX_BINDINGS:
            raise BindingDataError("too many account bindings")
        binding = AccountBinding(
            actor_key=normalized_actor,
            user_id=normalized_user_id,
            email_masked=mask_email(email),
            bound_at=bound_at,
        )
        self._by_actor[normalized_actor] = binding
        self._actor_by_account[normalized_user_id] = normalized_actor
        return BindingOutcome.CREATED

    def get(self, actor_key: str) -> AccountBinding | None:
        return self._by_actor.get(_validate_actor_key(actor_key))

    def unbind(self, actor_key: str) -> bool:
        normalized_actor = _validate_actor_key(actor_key)
        binding = self._by_actor.pop(normalized_actor, None)
        if binding is None:
            return False
        self._actor_by_account.pop(binding.user_id, None)
        return True

    def to_bytes(self) -> bytes:
        payload = {
            "version": self.VERSION,
            "bindings": [
                asdict(binding)
                for binding in sorted(self._by_actor.values(), key=lambda item: item.actor_key)
            ],
        }
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > self.MAX_STORAGE_BYTES:
            raise BindingDataError("account binding storage is too large")
        return encoded

    @classmethod
    def from_bytes(cls, stored: bytes | None) -> BindingRegistry:
        if not stored:
            return cls()
        if not isinstance(stored, bytes) or len(stored) > cls.MAX_STORAGE_BYTES:
            raise BindingDataError("invalid account binding storage")
        try:
            payload = json.loads(stored.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BindingDataError("invalid account binding storage") from exc
        if not isinstance(payload, dict) or payload.get("version") != cls.VERSION:
            raise BindingDataError("unsupported account binding storage version")
        items = payload.get("bindings")
        if not isinstance(items, list) or len(items) > cls.MAX_BINDINGS:
            raise BindingDataError("invalid account binding list")
        return cls([_parse_binding(item) for item in items])


class BindingAttemptLimiter:
    def __init__(
        self,
        *,
        max_attempts: int = 5,
        window_seconds: int = 600,
        max_tracked_actors: int = BindingRegistry.MAX_BINDINGS,
    ):
        self.max_attempts = max(1, min(int(max_attempts), 20))
        self.window_seconds = max(1, min(int(window_seconds), 3600))
        self.max_tracked_actors = max(
            1,
            min(int(max_tracked_actors), BindingRegistry.MAX_BINDINGS),
        )
        self._attempts: dict[str, deque[float]] = {}

    def allow(self, actor_key: str, *, now: float) -> bool:
        normalized_actor = _validate_actor_key(actor_key)
        cutoff = float(now) - self.window_seconds
        attempts = self._attempts.get(normalized_actor)
        if attempts is None:
            if len(self._attempts) >= self.max_tracked_actors:
                expired_actors = [
                    key
                    for key, values in self._attempts.items()
                    if not values or values[-1] <= cutoff
                ]
                for key in expired_actors:
                    self._attempts.pop(key, None)
                if len(self._attempts) >= self.max_tracked_actors:
                    return False
            attempts = deque()
            self._attempts[normalized_actor] = attempts
        while attempts and attempts[0] <= cutoff:
            attempts.popleft()
        if len(attempts) >= self.max_attempts:
            return False
        attempts.append(float(now))
        return True


def _validate_actor_key(value: Any) -> str:
    actor_key = str(value or "").strip().lower()
    if not re.fullmatch(r"[a-f0-9]{64}", actor_key):
        raise BindingDataError("invalid binding actor key")
    return actor_key


def _validate_user_id(value: Any) -> str:
    user_id = str(value or "").strip()
    if not user_id.isdigit() or len(user_id) > 20:
        raise BindingDataError("invalid binding account id")
    return user_id


def _validate_bound_at(value: Any) -> str:
    bound_at = str(value or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", bound_at):
        raise BindingDataError("invalid binding timestamp")
    return bound_at


def _parse_binding(item: Any) -> AccountBinding:
    if not isinstance(item, dict):
        raise BindingDataError("binding item must be an object")
    actor_key = _validate_actor_key(item.get("actor_key"))
    user_id = _validate_user_id(item.get("user_id"))
    email_masked = str(item.get("email_masked") or "").strip().casefold()
    if len(email_masked) > 254 or not re.fullmatch(r"[^@]{1,68}\*{3}[^@]*@[a-z0-9.-]+\.[a-z]{2,63}", email_masked):
        raise BindingDataError("invalid masked binding email")
    bound_at = _validate_bound_at(item.get("bound_at"))
    return AccountBinding(
        actor_key=actor_key,
        user_id=user_id,
        email_masked=email_masked,
        bound_at=bound_at,
    )
