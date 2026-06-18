#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : request_mapper.py
@Description: Binance USD-M ETHUSDT futures request mapper.

Translates BrokerOrderRequest -> Binance order params dict.
No HTTP calls.  No API keys.  No live / Trader / factory wiring.

Only ETHUSDT is supported.  Binance orders are mapped for One-way / net
position mode.  ``positionSide`` is never emitted in raw params because
One-way mode does not require it.  Reduce-only safety is enforced through
the ``reduceOnly`` param.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from src.exchanges.models import (
    BrokerOrderRequest,
    BrokerOrderSide,
    BrokerOrderType,
    BrokerPositionSide,
    BrokerQuantityUnit,
    ExchangeName,
)

__all__ = [
    "BINANCE_ETH_CONTRACT_SIZE_BASE",
    "BINANCE_ETH_USDT_SYMBOL",
    "broker_order_request_to_binance_params",
    "broker_order_side_to_binance",
    "broker_order_type_to_binance",
    "broker_quantity_to_binance_base_quantity",
    "_normalize_binance_position_mode",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_ETH_USDT_SYMBOL = "ETHUSDT"
BINANCE_ETH_CONTRACT_SIZE_BASE = Decimal("0.1")

# ---------------------------------------------------------------------------
# Decimal format helper
# ---------------------------------------------------------------------------


def _format_decimal(value: Decimal) -> str:
    """Format a Decimal value for Binance API params.

    Decimal("0.100")  -> "0.1"
    Decimal("1.000")  -> "1"
    Decimal("3100.50") -> "3100.5"
    """
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def _assert_binance_request(request: BrokerOrderRequest) -> None:
    """Raise ValueError if request is not for Binance ETHUSDT."""
    if request.exchange != ExchangeName.BINANCE:
        raise ValueError(
            f"BrokerOrderRequest.exchange must be BINANCE, got: {request.exchange.value}"
        )
    if request.symbol != BINANCE_ETH_USDT_SYMBOL:
        raise ValueError(f"Unsupported Binance symbol: {request.symbol}")


# ---------------------------------------------------------------------------
# Quantity converter
# ---------------------------------------------------------------------------


def broker_quantity_to_binance_base_quantity(
    *,
    quantity: Decimal,
    quantity_unit: BrokerQuantityUnit,
) -> Decimal:
    """Convert a generic broker quantity into a Binance base-asset quantity.

    Binance USD-M ETHUSDT futures use BASE_ASSET quantities (ETH).
    This function bridges CONTRACTS (1 contract = 0.1 ETH) into base-asset form.
    QUOTE_ASSET is rejected — Binance does not accept quote-denominated size.
    """
    if quantity <= 0:
        raise ValueError("quantity must be positive")

    if quantity_unit == BrokerQuantityUnit.BASE_ASSET:
        return quantity

    if quantity_unit == BrokerQuantityUnit.CONTRACTS:
        return quantity * BINANCE_ETH_CONTRACT_SIZE_BASE

    raise ValueError(
        f"Unsupported Binance quantity unit: {quantity_unit.value}"
    )


# ---------------------------------------------------------------------------
# Enum mappers
# ---------------------------------------------------------------------------


def broker_order_side_to_binance(side: BrokerOrderSide) -> str:
    """Map BrokerOrderSide to Binance side string."""
    if side == BrokerOrderSide.BUY:
        return "BUY"
    if side == BrokerOrderSide.SELL:
        return "SELL"
    raise ValueError(f"Unsupported Binance order side: {side.value}")


def broker_position_side_to_binance(position_side: BrokerPositionSide) -> str:
    """One-way / net mode does NOT use positionSide.

    This function is retained for backward compatibility with the public
    API surface but always raises an error.  Callers must not invoke it
    for raw param building.
    """
    raise ValueError(
        "Binance One-way mode does not use positionSide; "
        f"got {position_side.value}"
    )


def broker_order_type_to_binance(order_type: BrokerOrderType) -> str:
    """Map BrokerOrderType to Binance type string."""
    if order_type == BrokerOrderType.MARKET:
        return "MARKET"
    if order_type == BrokerOrderType.LIMIT:
        return "LIMIT"
    if order_type == BrokerOrderType.STOP_MARKET:
        return "STOP_MARKET"
    if order_type == BrokerOrderType.TAKE_PROFIT_MARKET:
        return "TAKE_PROFIT_MARKET"
    raise ValueError(f"Unsupported Binance order type: {order_type.value}")


# ---------------------------------------------------------------------------
# Position mode normalizer
# ---------------------------------------------------------------------------


def _normalize_binance_position_mode(position_mode: str) -> str:
    """Normalize and validate a Binance position mode string.

    Accepted values are ``net``, ``one_way``, and ``one-way`` (all
    case-insensitive).  ``hedge``, ``dual``, and ``dual_side`` are
    explicitly rejected.

    Returns the canonical string ``"net"``.
    """
    value = str(position_mode).strip().lower().replace("-", "_")
    if value in {"net", "one_way", "oneway"}:
        return "net"
    raise ValueError(f"Unsupported Binance position mode: {position_mode}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def broker_order_request_to_binance_params(
    request: BrokerOrderRequest,
    *,
    position_mode: str = "net",
) -> dict[str, Any]:
    """Translate a BrokerOrderRequest into Binance order params.

    Parameters
    ----------
    request : BrokerOrderRequest
        The canonical order request.
    position_mode : str
        Must be one of ``net`` / ``one_way`` / ``one-way``.
        ``hedge`` is rejected.  One-way / net mode never emits
        ``positionSide`` in raw params; the order side (BUY/SELL)
        alone determines direction.

    Returns
    -------
    dict[str, Any]
        Params dict ready for a Binance order request.

    Raises
    ------
    ValueError
        If any field is invalid or unsupported.
    """
    _normalize_binance_position_mode(position_mode)

    _assert_binance_request(request)

    quantity = broker_quantity_to_binance_base_quantity(
        quantity=request.quantity,
        quantity_unit=request.quantity_unit,
    )

    params: dict[str, Any] = {
        "symbol": request.symbol,
        "side": broker_order_side_to_binance(request.side),
        "type": broker_order_type_to_binance(request.order_type),
        "quantity": _format_decimal(quantity),
    }

    if request.client_order_id:
        params["newClientOrderId"] = request.client_order_id

    if request.order_type == BrokerOrderType.LIMIT:
        if request.price is None:
            raise ValueError("LIMIT order requires price")
        params["price"] = _format_decimal(request.price)
        params["timeInForce"] = "GTC"

    if request.order_type in {
        BrokerOrderType.STOP_MARKET,
        BrokerOrderType.TAKE_PROFIT_MARKET,
    }:
        if request.trigger_price is None:
            raise ValueError(
                f"{request.order_type.value} order requires trigger_price"
            )
        params["stopPrice"] = _format_decimal(request.trigger_price)

    # One-way mode: reduce-only must be explicit for close orders.
    if request.reduce_only is True:
        params["reduceOnly"] = "true"

    return params
