#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : trading_mappers.py
@Description: Pure mappers from Binance API responses to TradingClientPort DTOs.

No HTTP calls.  No API keys.  No live / Trader / factory wiring.
All numeric values are converted to ``Decimal``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.execution.trading_client_port import (
    AlgoOrderSnapshot,
    BalanceSnapshot,
    OrderSnapshot,
    OrderStatusSnapshot,
    PositionSnapshot,
)


# ---------------------------------------------------------------------------
# Decimal helpers
# ---------------------------------------------------------------------------


def _required_decimal(value: Any, field_name: str) -> Decimal:
    """Convert *value* to Decimal.  Raises ValueError on missing / invalid input."""
    if value is None:
        raise ValueError(f"Missing required decimal field: {field_name}")
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if text == "":
        raise ValueError(f"Missing required decimal field: {field_name}")
    try:
        return Decimal(text)
    except Exception as exc:
        raise ValueError(
            f"Invalid decimal field {field_name}={value!r}"
        ) from exc


def _optional_decimal(value: Any, field_name: str) -> Decimal | None:
    """Convert *value* to Decimal.  Returns None for empty/None.  Raises on invalid."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    text = str(value).strip()
    if text == "":
        return None
    try:
        return Decimal(text)
    except Exception as exc:
        raise ValueError(
            f"Invalid decimal field {field_name}={value!r}"
        ) from exc


# ---------------------------------------------------------------------------
# Side / status mapping
# ---------------------------------------------------------------------------


def map_binance_side_to_port_side(raw_side: Any) -> str:
    """Normalise Binance side to uppercase: BUY / SELL."""
    text = str(raw_side or "").strip().upper()
    if text in ("BUY", "SELL"):
        return text
    raise ValueError(f"Unrecognised Binance side: {raw_side!r}")


def map_binance_status_to_port_status(raw_status: Any) -> str:
    """Map Binance order status to TradingClientPort normalised status.

    Mapping
    -------
    NEW                    -> OPEN
    PARTIALLY_FILLED       -> PARTIALLY_FILLED
    FILLED                 -> FILLED
    CANCELED / CANCELLED   -> CANCELED
    REJECTED               -> REJECTED
    EXPIRED                -> EXPIRED
    (anything else)        -> UNKNOWN
    """
    text = str(raw_status or "").strip().upper()
    mapping: dict[str, str] = {
        "NEW": "OPEN",
        "PARTIALLY_FILLED": "PARTIALLY_FILLED",
        "FILLED": "FILLED",
        "CANCELED": "CANCELED",
        "CANCELLED": "CANCELED",
        "REJECTED": "REJECTED",
        "EXPIRED": "EXPIRED",
    }
    return mapping.get(text, "UNKNOWN")


# ---------------------------------------------------------------------------
# Balance mapper
# ---------------------------------------------------------------------------


def map_binance_balance_to_snapshot(
    raw: Mapping[str, Any],
    *,
    margin_asset: str,
) -> BalanceSnapshot:
    """Map a single Binance balance entry to a ``BalanceSnapshot``.

    Expected raw fields
    -------------------
    asset, balance, availableBalance.
    """
    return BalanceSnapshot(
        asset=str(raw.get("asset", margin_asset)),
        total=_required_decimal(raw.get("balance"), "balance"),
        available=_optional_decimal(raw.get("availableBalance"), "availableBalance"),
        raw=dict(raw),
    )


# ---------------------------------------------------------------------------
# Position mapper
# ---------------------------------------------------------------------------


def map_binance_position_to_snapshot(
    raw: Mapping[str, Any],
) -> PositionSnapshot:
    """Map a Binance position-risk entry to a ``PositionSnapshot``.

    Position side inference (one-way / net mode):
        positionAmt > 0  -> LONG
        positionAmt < 0  -> SHORT
        positionAmt == 0 -> no position (side=None)
    """
    position_amt = _required_decimal(raw.get("positionAmt"), "positionAmt")

    if position_amt == 0:
        return PositionSnapshot(
            side=None,
            qty=Decimal("0"),
            avg_entry_price=None,
            raw=dict(raw),
        )

    side = "LONG" if position_amt > 0 else "SHORT"

    return PositionSnapshot(
        side=side,
        qty=abs(position_amt),
        avg_entry_price=_optional_decimal(raw.get("entryPrice"), "entryPrice"),
        raw=dict(raw),
    )


# ---------------------------------------------------------------------------
# Order mapper
# ---------------------------------------------------------------------------


def map_binance_order_to_snapshot(
    raw: Mapping[str, Any],
) -> OrderSnapshot:
    """Map a Binance order dict to an ``OrderSnapshot``.

    Trigger price is derived from ``stopPrice`` when present and non-zero.
    """
    order_id_raw = raw.get("orderId")
    order_id = str(order_id_raw) if order_id_raw is not None else None

    client_order_id_raw = raw.get("clientOrderId")
    client_order_id = (
        str(client_order_id_raw) if client_order_id_raw is not None else None
    )

    side = map_binance_side_to_port_side(raw.get("side"))

    qty = _required_decimal(
        raw.get("origQty") if raw.get("origQty") is not None else raw.get("quantity"),
        "origQty",
    )

    price = _optional_decimal(raw.get("price"), "price")

    stop_price = _optional_decimal(raw.get("stopPrice"), "stopPrice")
    trigger_price: Decimal | None = None
    if stop_price is not None and stop_price > 0:
        trigger_price = stop_price

    reduce_only_raw = raw.get("reduceOnly")
    if isinstance(reduce_only_raw, bool):
        reduce_only = reduce_only_raw
    else:
        reduce_only = str(reduce_only_raw or "").strip().lower() in {
            "true", "1", "yes", "y", "on",
        }

    return OrderSnapshot(
        order_id=order_id,
        client_order_id=client_order_id,
        side=side,
        qty=qty,
        price=price,
        trigger_price=trigger_price,
        reduce_only=reduce_only,
        raw=dict(raw),
    )


# ---------------------------------------------------------------------------
# Order status mapper
# ---------------------------------------------------------------------------


def map_binance_order_to_status_snapshot(
    raw: Mapping[str, Any],
) -> OrderStatusSnapshot:
    """Map a Binance order dict to an ``OrderStatusSnapshot``.

    Maps Binance status strings to normalised TradingClientPort status values.
    """
    order_id_raw = raw.get("orderId")
    order_id = str(order_id_raw) if order_id_raw is not None else None

    client_order_id_raw = raw.get("clientOrderId")
    client_order_id = (
        str(client_order_id_raw) if client_order_id_raw is not None else None
    )

    status = map_binance_status_to_port_status(raw.get("status"))

    filled_qty = _optional_decimal(raw.get("executedQty"), "executedQty")
    avg_fill_price = _optional_decimal(raw.get("avgPrice"), "avgPrice")

    return OrderStatusSnapshot(
        order_id=order_id,
        client_order_id=client_order_id,
        status=status,
        filled_qty=filled_qty,
        avg_fill_price=avg_fill_price,
        raw=dict(raw),
    )


# ---------------------------------------------------------------------------
# Algo order mapper
# ---------------------------------------------------------------------------


def map_binance_algo_order_to_snapshot(
    raw: Mapping[str, Any],
) -> AlgoOrderSnapshot:
    """Map a Binance stop / conditional order dict to an ``AlgoOrderSnapshot``.

    Used for orders with ``type`` in {STOP_MARKET, TAKE_PROFIT_MARKET}.
    """
    order_id_raw = raw.get("orderId")
    order_id = str(order_id_raw) if order_id_raw is not None else None

    client_order_id_raw = raw.get("clientOrderId")
    client_order_id = (
        str(client_order_id_raw) if client_order_id_raw is not None else None
    )

    side = map_binance_side_to_port_side(raw.get("side"))

    qty = _required_decimal(
        raw.get("origQty") if raw.get("origQty") is not None else raw.get("quantity"),
        "origQty",
    )

    stop_price = _optional_decimal(raw.get("stopPrice"), "stopPrice")
    trigger_price: Decimal | None = None
    if stop_price is not None and stop_price > 0:
        trigger_price = stop_price

    status = map_binance_status_to_port_status(raw.get("status"))

    return AlgoOrderSnapshot(
        order_id=order_id,
        client_order_id=client_order_id,
        side=side,
        qty=qty,
        trigger_price=trigger_price,
        status=status,
        raw=dict(raw),
    )


__all__ = [
    "map_binance_algo_order_to_snapshot",
    "map_binance_balance_to_snapshot",
    "map_binance_order_to_snapshot",
    "map_binance_order_to_status_snapshot",
    "map_binance_position_to_snapshot",
    "map_binance_side_to_port_side",
    "map_binance_status_to_port_status",
]
