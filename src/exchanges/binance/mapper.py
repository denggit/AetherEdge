#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : mapper.py
@Description: Binance USD-M ETHUSDT futures response mapper.

Pure functions that translate Binance raw API fields into generic Broker* DTOs.
No HTTP calls.  No API keys.  No live / Trader / factory wiring.

Only ETHUSDT is supported in this phase.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.exchanges.errors import ExchangeError, ExchangeErrorKind
from src.exchanges.models import (
    BrokerOrder,
    BrokerOrderSide,
    BrokerOrderStatus,
    BrokerOrderType,
    BrokerPosition,
    BrokerPositionSide,
    BrokerQuantityUnit,
    ExchangeName,
)

__all__ = [
    "BINANCE_ETH_USDT_SYMBOL",
    "assert_binance_ethusdt_symbol",
    "map_binance_error",
    "map_binance_order",
    "map_binance_order_side",
    "map_binance_order_status",
    "map_binance_order_type",
    "map_binance_position",
    "map_binance_position_side",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_ETH_USDT_SYMBOL = "ETHUSDT"


# ---------------------------------------------------------------------------
# Symbol guard
# ---------------------------------------------------------------------------

def assert_binance_ethusdt_symbol(symbol: str) -> None:
    """Raise ValueError if *symbol* is not the supported ETHUSDT symbol."""
    if symbol != BINANCE_ETH_USDT_SYMBOL:
        raise ValueError(f"Unsupported Binance symbol: {symbol}")


# ---------------------------------------------------------------------------
# Decimal helpers
# ---------------------------------------------------------------------------

def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return Decimal(text)


def _decimal_or_zero(value: Any) -> Decimal:
    parsed = _decimal_or_none(value)
    return parsed if parsed is not None else Decimal("0")


# ---------------------------------------------------------------------------
# Boolean helper
# ---------------------------------------------------------------------------

def _bool_from_binance(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# ---------------------------------------------------------------------------
# Enum mappings
# ---------------------------------------------------------------------------

def map_binance_order_side(value: Any) -> BrokerOrderSide:
    """Map Binance raw side field to BrokerOrderSide."""
    value_str = str(value or "").upper()
    if value_str == "BUY":
        return BrokerOrderSide.BUY
    if value_str == "SELL":
        return BrokerOrderSide.SELL
    return BrokerOrderSide.UNKNOWN


def map_binance_position_side(value: Any) -> BrokerPositionSide:
    """Map Binance raw positionSide field to BrokerPositionSide."""
    value_str = str(value or "").upper()
    if value_str == "LONG":
        return BrokerPositionSide.LONG
    if value_str == "SHORT":
        return BrokerPositionSide.SHORT
    if value_str == "BOTH":
        return BrokerPositionSide.NET
    return BrokerPositionSide.UNKNOWN


def map_binance_order_type(value: Any) -> BrokerOrderType:
    """Map Binance raw type field to BrokerOrderType."""
    value_str = str(value or "").upper()
    if value_str == "MARKET":
        return BrokerOrderType.MARKET
    if value_str == "LIMIT":
        return BrokerOrderType.LIMIT
    if value_str == "STOP_MARKET":
        return BrokerOrderType.STOP_MARKET
    if value_str == "TAKE_PROFIT_MARKET":
        return BrokerOrderType.TAKE_PROFIT_MARKET
    return BrokerOrderType.UNKNOWN


def map_binance_order_status(value: Any) -> BrokerOrderStatus:
    """Map Binance raw status field to BrokerOrderStatus."""
    value_str = str(value or "").upper()
    mapping: dict[str, BrokerOrderStatus] = {
        "NEW": BrokerOrderStatus.OPEN,
        "PARTIALLY_FILLED": BrokerOrderStatus.PARTIALLY_FILLED,
        "FILLED": BrokerOrderStatus.FILLED,
        "CANCELED": BrokerOrderStatus.CANCELED,
        "CANCELLED": BrokerOrderStatus.CANCELED,
        "REJECTED": BrokerOrderStatus.REJECTED,
        "EXPIRED": BrokerOrderStatus.EXPIRED,
    }
    return mapping.get(value_str, BrokerOrderStatus.UNKNOWN)


# ---------------------------------------------------------------------------
# Order mapper
# ---------------------------------------------------------------------------

def map_binance_order(raw: Mapping[str, Any]) -> BrokerOrder:
    """Map a Binance USD-M futures order response into a BrokerOrder.

    Supported raw fields
    --------------------
    symbol, orderId, clientOrderId, side, positionSide, type, status,
    price, origQty, executedQty, avgPrice, reduceOnly, stopPrice.

    quantity_unit is always BASE_ASSET because Binance USD-M futures use
    base-asset quantities (e.g. 0.1 ETH).
    """
    symbol = str(raw.get("symbol", ""))
    assert_binance_ethusdt_symbol(symbol)

    order_id_raw = raw.get("orderId")
    order_id = str(order_id_raw) if order_id_raw is not None else None

    client_order_id_raw = raw.get("clientOrderId")
    client_order_id = str(client_order_id_raw) if client_order_id_raw is not None else None

    return BrokerOrder(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        side=map_binance_order_side(raw.get("side")),
        position_side=map_binance_position_side(raw.get("positionSide")),
        order_type=map_binance_order_type(raw.get("type")),
        status=map_binance_order_status(raw.get("status")),
        price=_decimal_or_none(raw.get("price")),
        quantity=_decimal_or_none(
            raw.get("origQty") if raw.get("origQty") is not None else raw.get("quantity")
        ),
        quantity_unit=BrokerQuantityUnit.BASE_ASSET,
        filled_quantity=_decimal_or_none(raw.get("executedQty")),
        average_price=_decimal_or_none(raw.get("avgPrice")),
        reduce_only=_bool_from_binance(raw.get("reduceOnly")),
        trigger_price=_decimal_or_none(raw.get("stopPrice")),
        raw=dict(raw),
    )


# ---------------------------------------------------------------------------
# Position mapper
# ---------------------------------------------------------------------------

def map_binance_position(raw: Mapping[str, Any]) -> BrokerPosition | None:
    """Map a Binance USD-M futures position response into a BrokerPosition.

    Returns None when positionAmt == 0 (no open position).

    Hedge mode (positionSide in {LONG, SHORT}):
        Uses the declared side.  Quantity is abs(positionAmt).

    One-way / net mode (positionSide == BOTH):
        positionAmt > 0  -> LONG
        positionAmt < 0  -> SHORT

    quantity_unit is always BASE_ASSET.
    """
    symbol = str(raw.get("symbol", ""))
    assert_binance_ethusdt_symbol(symbol)

    position_amt = _decimal_or_zero(raw.get("positionAmt"))
    if position_amt == 0:
        return None

    raw_position_side = str(raw.get("positionSide", "")).upper()
    if raw_position_side == "LONG":
        position_side = BrokerPositionSide.LONG
    elif raw_position_side == "SHORT":
        position_side = BrokerPositionSide.SHORT
    elif raw_position_side == "BOTH":
        position_side = BrokerPositionSide.LONG if position_amt > 0 else BrokerPositionSide.SHORT
    else:
        position_side = BrokerPositionSide.UNKNOWN

    return BrokerPosition(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        position_side=position_side,
        quantity=abs(position_amt),
        quantity_unit=BrokerQuantityUnit.BASE_ASSET,
        average_entry_price=_decimal_or_none(raw.get("entryPrice")),
        mark_price=_decimal_or_none(raw.get("markPrice")),
        unrealized_pnl=_decimal_or_none(raw.get("unRealizedProfit")),
        leverage=_decimal_or_none(raw.get("leverage")),
        raw=dict(raw),
    )


# ---------------------------------------------------------------------------
# Error mapper
# ---------------------------------------------------------------------------

def map_binance_error(
    *,
    status_code: int | None = None,
    payload: Mapping[str, Any] | None = None,
    message: str | None = None,
) -> ExchangeError:
    """Map Binance error response / HTTP status into an ExchangeError.

    Supported Binance error codes
    -----------------------------
    -2015 / -1022  — AUTH_ERROR  (also triggered by HTTP 401, 403)
    -1003          — RATE_LIMITED (also triggered by HTTP 418, 429)
    -2019          — INSUFFICIENT_BALANCE
    -2011          — ORDER_NOT_FOUND

    All other known codes default to EXCHANGE_REJECTED.
    Empty / missing payload with no status_code defaults to UNKNOWN.
    """
    payload = payload or {}
    code = payload.get("code")
    msg = message or str(payload.get("msg") or payload.get("message") or "Binance request failed")

    kind: ExchangeErrorKind = ExchangeErrorKind.EXCHANGE_REJECTED

    if status_code in {401, 403} or code in {-2015, -1022}:
        kind = ExchangeErrorKind.AUTH_ERROR
    elif status_code in {418, 429} or code in {-1003}:
        kind = ExchangeErrorKind.RATE_LIMITED
    elif code in {-2019}:
        kind = ExchangeErrorKind.INSUFFICIENT_BALANCE
    elif code in {-2011}:
        kind = ExchangeErrorKind.ORDER_NOT_FOUND
    elif status_code is None and not payload:
        kind = ExchangeErrorKind.UNKNOWN

    return ExchangeError(
        exchange=ExchangeName.BINANCE,
        kind=kind,
        message=msg,
        raw={"status_code": status_code, "payload": dict(payload)},
    )
