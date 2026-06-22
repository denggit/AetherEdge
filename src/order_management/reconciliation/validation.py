"""Exchange-specific order ID validation for live trading hygiene.

Re-exports core validation functions from ``src.platform.exchanges.order_ids``
so existing callers in ``order_management`` continue to work without changes.

The canonical implementations live in the platform layer so that exchange
adapters can also import them without creating a ``platform → order_management``
circular dependency.
"""

from __future__ import annotations

# ── Re-export canonical implementations from platform layer ──
from src.platform.exchanges.order_ids import (
    FAKE_ID_PATTERNS,
    is_fake_order_id,
    is_valid_client_order_id,
    is_valid_exchange_order_id,
    resolve_order_query_ids,
)

# Backward-compatible alias for existing callers
resolve_query_params = resolve_order_query_ids

__all__ = [
    "FAKE_ID_PATTERNS",
    "is_fake_order_id",
    "is_valid_client_order_id",
    "is_valid_exchange_order_id",
    "resolve_order_query_ids",
    "resolve_query_params",
]
