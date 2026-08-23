"""Opaque API-key authentication and request-scoped authorization."""

from __future__ import annotations

import contextlib
import contextvars
import hmac
from collections.abc import Generator
from dataclasses import dataclass

from .config import AccessTokenConfig, Scope
from .errors import AuthenticationError, AuthorizationError

__all__ = [
    "ApiKeyAuthenticator",
    "AuthenticationError",
    "AuthorizationError",
    "Principal",
    "bind_principal",
    "current_principal",
    "current_request_id",
    "require_scope",
]


@dataclass(frozen=True, slots=True)
class Principal:
    name: str
    scopes: frozenset[Scope]


_principal_var: contextvars.ContextVar[Principal | None] = contextvars.ContextVar(
    "sub2api_mcp_principal", default=None
)
_request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "sub2api_mcp_request_id", default=None
)


class ApiKeyAuthenticator:
    """Match X-API-Key or Bearer credentials using constant-time comparison."""

    def __init__(self, tokens: list[AccessTokenConfig]) -> None:
        self._tokens = tuple(
            (item.token.get_secret_value(), Principal(item.name, item.scopes)) for item in tokens
        )

    @staticmethod
    def _extract(headers: list[tuple[bytes, bytes]]) -> str:
        header_map = {key.lower(): value for key, value in headers}
        api_key = header_map.get(b"x-api-key", b"").decode("latin-1").strip()
        if api_key:
            return api_key
        authorization = header_map.get(b"authorization", b"").decode("latin-1").strip()
        scheme, separator, value = authorization.partition(" ")
        if separator and scheme.casefold() == "bearer":
            return value.strip()
        return ""

    def authenticate(self, headers: list[tuple[bytes, bytes]]) -> Principal | None:
        supplied = self._extract(headers)
        matched: Principal | None = None
        for expected, principal in self._tokens:
            if hmac.compare_digest(supplied, expected):
                matched = principal
        return matched


@contextlib.contextmanager
def bind_principal(principal: Principal, request_id: str) -> Generator[None]:
    principal_token = _principal_var.set(principal)
    request_token = _request_id_var.set(request_id)
    try:
        yield
    finally:
        _request_id_var.reset(request_token)
        _principal_var.reset(principal_token)


def current_principal() -> Principal | None:
    return _principal_var.get()


def current_request_id() -> str | None:
    return _request_id_var.get()


def require_scope(scope: Scope) -> Principal:
    principal = current_principal()
    if principal is None:
        raise AuthenticationError
    if "sub2api:admin" not in principal.scopes and scope not in principal.scopes:
        raise AuthorizationError
    return principal
