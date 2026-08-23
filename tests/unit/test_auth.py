from __future__ import annotations

import pytest
from pydantic import SecretStr

from sub2api_mcp.auth import (
    ApiKeyAuthenticator,
    AuthorizationError,
    Principal,
    bind_principal,
    current_principal,
    require_scope,
)
from sub2api_mcp.config import AccessTokenConfig, Scope


def _authenticator() -> ApiKeyAuthenticator:
    return ApiKeyAuthenticator(
        [
            AccessTokenConfig(
                name="reader",
                token=SecretStr("r" * 32),
                scopes=frozenset[Scope]({"sub2api:read"}),
            ),
            AccessTokenConfig(
                name="admin",
                token=SecretStr("a" * 32),
                scopes=frozenset[Scope]({"sub2api:admin"}),
            ),
        ]
    )


def test_authenticator_accepts_x_api_key_and_bearer() -> None:
    authenticator = _authenticator()

    by_key = authenticator.authenticate([(b"x-api-key", ("r" * 32).encode())])
    by_bearer = authenticator.authenticate(
        [(b"authorization", ("Bearer " + "a" * 32).encode())]
    )

    assert by_key == Principal("reader", frozenset({"sub2api:read"}))
    assert by_bearer == Principal("admin", frozenset({"sub2api:admin"}))


def test_authenticator_rejects_missing_or_invalid_keys() -> None:
    authenticator = _authenticator()

    assert authenticator.authenticate([]) is None
    assert authenticator.authenticate([(b"x-api-key", b"wrong")]) is None


def test_admin_scope_satisfies_all_service_scopes() -> None:
    principal = Principal("admin", frozenset({"sub2api:admin"}))

    with bind_principal(principal, "request-1"):
        assert require_scope("sub2api:read") == principal
        assert require_scope("sub2api:write") == principal
        assert require_scope("sub2api:admin") == principal


def test_actor_scope_does_not_satisfy_read_scope() -> None:
    principal = Principal("bridge", frozenset({"sub2api:actor"}))

    with bind_principal(principal, "request-2"), pytest.raises(AuthorizationError):
        require_scope("sub2api:read")


def test_principal_context_is_reset_after_request() -> None:
    principal = Principal("reader", frozenset({"sub2api:read"}))

    with bind_principal(principal, "request-3"):
        assert current_principal() == principal

    assert current_principal() is None

