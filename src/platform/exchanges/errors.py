from __future__ import annotations

from typing import Any, Mapping


class ExchangeError(Exception):
    """Base exception for all exchange adapter errors."""


class UnsupportedExchangeError(ExchangeError):
    pass


class ExchangeConfigError(ExchangeError):
    pass


class ExchangeApiError(ExchangeError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class ExchangeMappingError(ExchangeError):
    def __init__(self, message: str, *, payload: Mapping[str, Any] | list[Any] | None = None) -> None:
        super().__init__(message)
        self.payload = payload
