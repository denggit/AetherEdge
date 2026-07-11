from __future__ import annotations

import re
from typing import Protocol

from src.platform.exchanges.errors import PrivateCredentialValidationError


class _CredentialConfig(Protocol):
    api_key: object
    api_secret: object
    passphrase: object


_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "okx": ("api_key", "api_secret", "passphrase"),
    "binance": ("api_key", "api_secret"),
}

_PLACEHOLDER_VALUES = frozenset(
    {
        "你的_okx_api_key",
        "你的_okx_secret_key",
        "你的_okx_passphrase",
        "你的_binance_api_key",
        "你的_binance_secret_key",
        "your_okx_api_key",
        "your_okx_secret_key",
        "your_okx_passphrase",
        "your_binance_api_key",
        "your_binance_secret_key",
        "<okx_api_key>",
        "<okx_secret_key>",
        "<okx_passphrase>",
        "<binance_api_key>",
        "<binance_secret_key>",
        "${okx_api_key}",
        "${okx_secret_key}",
        "${okx_passphrase}",
        "${binance_api_key}",
        "${binance_secret_key}",
    }
)

_GENERIC_PLACEHOLDER_VALUES = frozenset(
    {
        "changeme",
        "change_me",
        "change-me",
        "placeholder",
        "your_api_key",
        "your_secret_key",
        "your_passphrase",
        "xxx",
    }
)

_ANGLE_BRACKET_PLACEHOLDER = re.compile(r"<[^<>]*>")
_ENV_TEMPLATE_PLACEHOLDER = re.compile(r"\$\{[^{}]*\}")


def validate_private_credentials(
    exchange: object,
    config: _CredentialConfig,
) -> None:
    """Validate credentials before any private REST or WebSocket operation.

    The error deliberately contains only a stable code, exchange, and field
    names; it never interpolates credential values or configuration objects.
    """

    exchange_name = _exchange_name(exchange)
    required_fields = _REQUIRED_FIELDS.get(exchange_name)
    if required_fields is None:
        return

    missing_fields: list[str] = []
    placeholder_fields: list[str] = []
    for field in required_fields:
        value = getattr(config, field, None)
        normalized = _normalized_value(value)
        if not normalized:
            missing_fields.append(field)
        elif _is_placeholder_value(normalized):
            placeholder_fields.append(field)

    if not missing_fields and not placeholder_fields:
        return
    code = (
        "missing_private_credentials"
        if missing_fields and not placeholder_fields
        else "placeholder_private_credentials"
        if placeholder_fields and not missing_fields
        else "invalid_private_credentials"
    )
    raise PrivateCredentialValidationError(
        exchange=exchange_name,
        code=code,
        missing_fields=tuple(missing_fields),
        placeholder_fields=tuple(placeholder_fields),
    )


def _exchange_name(exchange: object) -> str:
    return str(getattr(exchange, "value", exchange)).strip().lower()


def _normalized_value(value: object) -> str:
    return "" if value is None else str(value).strip().casefold()


def _is_placeholder_value(normalized: str) -> bool:
    return (
        normalized in _PLACEHOLDER_VALUES
        or normalized in _GENERIC_PLACEHOLDER_VALUES
        or _ANGLE_BRACKET_PLACEHOLDER.fullmatch(normalized) is not None
        or _ENV_TEMPLATE_PLACEHOLDER.fullmatch(normalized) is not None
    )


__all__ = ["validate_private_credentials"]
