"""Exchange-specific order ID validation for live trading hygiene.

These functions are deliberately NOT in the exchange clients — they are a
local application concern about data quality, not an exchange API behavior.
Both the reconciliation service and the sync service depend on them.
"""

from __future__ import annotations

import re

from src.platform.exchanges.models import ExchangeName

# ── Patterns identifying test / fake / placeholder order IDs ──
# These MUST NOT be sent to exchange REST endpoints as orderId / algoId.

FAKE_ID_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"^okx-order-"),
    re.compile(r"^okx-stop-"),
    re.compile(r"^binance-order-"),
    re.compile(r"^binance-stop-"),
    re.compile(r"^okx-\d+$"),
    re.compile(r"^binance-\d+$"),
    re.compile(r"^test-", re.IGNORECASE),
    re.compile(r"^fake-", re.IGNORECASE),
    re.compile(r"^dummy-", re.IGNORECASE),
)

_INVALID_SENTINELS: frozenset[str] = frozenset(
    {"", "none", "null", "nan", "n/a", "na", "undefined"}
)


def is_fake_order_id(order_id: str | None) -> bool:
    """Return True if *order_id* matches known fake/test patterns."""
    if order_id is None:
        return False
    text = str(order_id).strip()
    if not text:
        return False
    if text.lower() in _INVALID_SENTINELS:
        return True
    return any(pattern.search(text) for pattern in FAKE_ID_PATTERNS)


def is_valid_exchange_order_id(exchange: ExchangeName, order_id: str | None) -> bool:
    """Return True when *order_id* is a valid exchange-assigned numeric ID.

    OKX ordId / algoId must be numeric.
    Binance orderId / algoId must be numeric.

    Returns False for None, empty, sentinel values, fake patterns, or
    non-digit strings.
    """
    if order_id is None:
        return False
    text = str(order_id).strip()
    if not text:
        return False
    if text.lower() in _INVALID_SENTINELS:
        return False
    if is_fake_order_id(text):
        return False
    # Exchange-assigned IDs must be purely numeric
    return text.isdigit()


def is_valid_client_order_id(client_order_id: str | None) -> bool:
    """Return True when *client_order_id* is a usable non-empty string."""
    if client_order_id is None:
        return False
    text = str(client_order_id).strip()
    if not text:
        return False
    if text.lower() in _INVALID_SENTINELS:
        return False
    return True


def resolve_query_params(
    exchange: ExchangeName,
    order_id: str | None,
    client_order_id: str | None,
) -> tuple[str | None, str | None]:
    """Resolve which ID(s) to use when querying exchange order status.

    Priority:
    1. If exchange order_id is valid (numeric, not fake), use it
       (with optional client_order_id as additional lookup param).
    2. If exchange order_id is invalid but client_order_id is valid,
       use only the client_order_id.
    3. If both are invalid, return (None, None) — the caller must skip
       the query entirely.

    Returns (resolved_order_id, resolved_client_order_id).
    """
    oid = str(order_id).strip() if order_id else None
    cid = str(client_order_id).strip() if client_order_id else None

    oid_valid = is_valid_exchange_order_id(exchange, oid)
    cid_valid = is_valid_client_order_id(cid)

    if oid_valid:
        # Exchange-assigned numeric ID is the preferred lookup key
        return (oid, cid if cid_valid else None)

    if cid_valid:
        # Fall back to client-side ID only — must NOT send fake order_id
        return (None, cid)

    # Both invalid: caller must skip
    return (None, None)
