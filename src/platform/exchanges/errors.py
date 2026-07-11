from __future__ import annotations

from typing import Any, Mapping


class ExchangeError(Exception):
    """Base exception for all exchange adapter errors."""


class UnsupportedExchangeError(ExchangeError):
    pass


class ExchangeConfigError(ExchangeError):
    pass


class PrivateCredentialValidationError(ExchangeConfigError):
    """Safe validation error for credentials needed by private APIs."""

    def __init__(
        self,
        *,
        exchange: str,
        code: str,
        missing_fields: tuple[str, ...] = (),
        placeholder_fields: tuple[str, ...] = (),
    ) -> None:
        self.exchange = exchange
        self.code = code
        self.missing_fields = missing_fields
        self.placeholder_fields = placeholder_fields
        parts = [code, f"exchange={exchange}"]
        if missing_fields:
            parts.append("missing_fields=" + ",".join(missing_fields))
        if placeholder_fields:
            parts.append(
                "placeholder_fields=" + ",".join(placeholder_fields)
            )
        super().__init__(" ".join(parts))


class ExchangeApiError(ExchangeError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: Any = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class ExchangeMappingError(ExchangeError):
    def __init__(self, message: str, *, payload: Mapping[str, Any] | list[Any] | None = None) -> None:
        super().__init__(message)
        self.payload = payload
