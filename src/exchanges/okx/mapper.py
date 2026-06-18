#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/12
@File       : mapper.py
@Description: OKX raw API field mapper.

Pure functions that translate OKX raw API fields into generic Broker* DTOs.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from src.exchanges.models import (
    BrokerBalance,
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
    "broker_balance_from_okx_balance_detail",
    "broker_order_from_okx_pending_algo_order",
    "broker_order_from_okx_pending_order",
    "broker_position_from_okx_position",
]


def broker_order_from_okx_pending_order(raw: Mapping[str, Any]) -> BrokerOrder:
    """Map an OKX ordinary pending order item into a generic broker order."""
    return BrokerOrder(
        exchange=ExchangeName.OKX,
        symbol=_to_str(raw.get("instId")),
        order_id=_to_str_optional(raw.get("ordId")),
        client_order_id=_to_str_optional(raw.get("clOrdId")),
        side=_map_okx_side(raw.get("side")),
        position_side=_map_okx_position_side(raw.get("posSide")),
        order_type=_map_okx_order_type(raw),
        status=_map_okx_order_status(raw.get("state")),
        price=_to_decimal_optional(raw.get("px")),
        quantity=_to_decimal_optional(raw.get("sz")),
        quantity_unit=BrokerQuantityUnit.CONTRACTS,
        filled_quantity=_to_decimal_optional(raw.get("accFillSz")),
        average_price=_to_decimal_optional(raw.get("avgPx")),
        reduce_only=_okx_bool(raw.get("reduceOnly")),
        raw=dict(raw),
        metadata={
            "source": "ordinary",
            "okx_order_id_field": "ordId",
        },
    )


def broker_order_from_okx_pending_algo_order(raw: Mapping[str, Any]) -> BrokerOrder:
    """Map an OKX pending algo order item into a generic broker order."""
    order_id_field = "algoId" if _is_present(raw.get("algoId")) else "ordId"

    return BrokerOrder(
        exchange=ExchangeName.OKX,
        symbol=_to_str(raw.get("instId")),
        order_id=_to_str_optional(raw.get(order_id_field)),
        client_order_id=_to_str_optional(raw.get("algoClOrdId")),
        side=_map_okx_side(raw.get("side")),
        position_side=_map_okx_position_side(raw.get("posSide")),
        order_type=_map_okx_order_type(raw),
        status=_map_okx_order_status(raw.get("state"), default_open=True),
        price=None,
        quantity=_to_decimal_optional(raw.get("sz")),
        quantity_unit=BrokerQuantityUnit.CONTRACTS,
        reduce_only=_okx_bool(raw.get("reduceOnly")),
        trigger_price=_to_decimal_optional(
            raw.get("slTriggerPx")
            if _is_present(raw.get("slTriggerPx"))
            else raw.get("tpTriggerPx")
        ),
        raw=dict(raw),
        metadata={
            "source": "algo",
            "okx_order_id_field": order_id_field,
        },
    )


def broker_position_from_okx_position(raw: Mapping[str, Any]) -> BrokerPosition:
    """Map an OKX account position item into a generic broker position."""
    return BrokerPosition(
        exchange=ExchangeName.OKX,
        symbol=_to_str(raw.get("instId")),
        position_side=_map_okx_position_side(raw.get("posSide"), pos=raw.get("pos")),
        quantity=_to_decimal_abs_or_zero(raw.get("pos")),
        quantity_unit=BrokerQuantityUnit.CONTRACTS,
        average_entry_price=_to_decimal_optional(raw.get("avgPx")),
        mark_price=_to_decimal_optional(raw.get("markPx")),
        unrealized_pnl=_to_decimal_optional(raw.get("upl")),
        leverage=_to_decimal_optional(raw.get("lever")),
        raw=dict(raw),
        metadata={
            "source": "position",
        },
    )


def broker_balance_from_okx_balance_detail(raw: Mapping[str, Any]) -> BrokerBalance:
    """Map a single OKX balance detail item into a generic broker balance."""
    available_value = (
        raw.get("availEq") if _is_present(raw.get("availEq")) else raw.get("availBal")
    )

    return BrokerBalance(
        exchange=ExchangeName.OKX,
        asset=_to_str(raw.get("ccy")),
        total=_to_decimal_optional(raw.get("eq")) or Decimal("0"),
        available=_to_decimal_optional(available_value),
        frozen=_to_decimal_optional(raw.get("frozenBal")),
        raw=dict(raw),
        metadata={
            "source": "balance",
        },
    )


def _to_decimal_optional(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_decimal_abs_or_zero(value: Any) -> Decimal:
    decimal_value = _to_decimal_optional(value)
    if decimal_value is None:
        return Decimal("0")
    return abs(decimal_value)


def _okx_bool(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1"}
    return value == 1


def _map_okx_side(value: Any) -> BrokerOrderSide:
    if value == "buy":
        return BrokerOrderSide.BUY
    if value == "sell":
        return BrokerOrderSide.SELL
    return BrokerOrderSide.UNKNOWN


def _map_okx_position_side(
    value: Any,
    *,
    pos: Any | None = None,
) -> BrokerPositionSide:
    if value == "long":
        return BrokerPositionSide.LONG
    if value == "short":
        return BrokerPositionSide.SHORT
    if _is_present(value):
        return BrokerPositionSide.UNKNOWN

    pos_decimal = _to_decimal_optional(pos)
    if pos_decimal is None:
        return BrokerPositionSide.UNKNOWN
    if pos_decimal > 0:
        return BrokerPositionSide.LONG
    if pos_decimal < 0:
        return BrokerPositionSide.SHORT
    return BrokerPositionSide.NET


def _map_okx_order_type(raw: Mapping[str, Any]) -> BrokerOrderType:
    ord_type = raw.get("ordType")
    if ord_type == "market":
        return BrokerOrderType.MARKET
    if ord_type == "limit":
        return BrokerOrderType.LIMIT
    if ord_type == "conditional":
        if _is_present(raw.get("slTriggerPx")) and str(raw.get("slOrdPx")) == "-1":
            return BrokerOrderType.STOP_MARKET
        if _is_present(raw.get("tpTriggerPx")):
            return BrokerOrderType.TAKE_PROFIT_MARKET
    return BrokerOrderType.UNKNOWN


def _map_okx_order_status(
    value: Any,
    *,
    default_open: bool = False,
) -> BrokerOrderStatus:
    if not _is_present(value):
        if default_open:
            return BrokerOrderStatus.OPEN
        return BrokerOrderStatus.UNKNOWN

    if value == "live":
        return BrokerOrderStatus.OPEN
    if value == "partially_filled":
        return BrokerOrderStatus.PARTIALLY_FILLED
    if value == "filled":
        return BrokerOrderStatus.FILLED
    if value in {"canceled", "cancelled"}:
        return BrokerOrderStatus.CANCELED
    if value == "expired":
        return BrokerOrderStatus.EXPIRED
    if value == "rejected":
        return BrokerOrderStatus.REJECTED
    return BrokerOrderStatus.UNKNOWN


def _is_present(value: Any) -> bool:
    return value is not None and value != ""


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _to_str_optional(value: Any) -> str | None:
    if not _is_present(value):
        return None
    return str(value)
