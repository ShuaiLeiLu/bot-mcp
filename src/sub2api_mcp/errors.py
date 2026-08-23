"""Stable service errors safe to expose through MCP or HTTP envelopes."""

from __future__ import annotations


class ServiceError(Exception):
    """An expected service failure with a stable public code."""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.safe_message = message
        self.retryable = retryable


class AuthenticationError(ServiceError):
    def __init__(self) -> None:
        super().__init__("UNAUTHENTICATED", "A valid API key is required")


class AuthorizationError(ServiceError):
    def __init__(self) -> None:
        super().__init__("FORBIDDEN", "The API key lacks the required scope")

