#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/12
@File       : models.py
@Description: Generic broker DTOs and enums.

These models are exchange-agnostic.  No OKX / Binance / Bybit private fields
are allowed as first-class attributes.  Exchange-specific raw data is stored in
``raw`` or ``metadata`` (both ``Mapping[str, Any]``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ExchangeName(str, Enum):
    OKX = "okx"
    BINANCE = "binance"
    BYBIT = "bybit"
    UNKNOWN = "unknown"


class BrokerMarketType(str, Enum):
    SPOT = "SPOT"
    MARGIN = "MARGIN"
    SWAP = "SWAP"
    FUTURES = "FUTURES"


class BrokerPositionMode(str, Enum):
    NET = "NET"
    HEDGE = "HEDGE"


class BrokerPositionSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NET = "NET"
    UNKNOWN = "UNKNOWN"


class BrokerOrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class BrokerOrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"
    UNKNOWN = "UNKNOWN"


class BrokerOrderStatus(str, Enum):
    OPEN = "OPEN"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"


class BrokerQuantityUnit(str, Enum):
    CONTRACTS = "CONTRACTS"
    BASE_ASSET = "BASE_ASSET"
    QUOTE_ASSET = "QUOTE_ASSET"


# ---------------------------------------------------------------------------
# Dataclass DTOs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BrokerSymbol:
    exchange: ExchangeName
    raw_symbol: str
    base_asset: str
    quote_asset: str
    market_type: BrokerMarketType
    contract_type: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerOrder:
    exchange: ExchangeName
    symbol: str
    order_id: str | None
    client_order_id: str | None
    side: BrokerOrderSide
    position_side: BrokerPositionSide
    order_type: BrokerOrderType
    status: BrokerOrderStatus
    price: Decimal | None
    quantity: Decimal | None
    quantity_unit: BrokerQuantityUnit | None
    filled_quantity: Decimal | None = None
    average_price: Decimal | None = None
    reduce_only: bool = False
    trigger_price: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerPosition:
    exchange: ExchangeName
    symbol: str
    position_side: BrokerPositionSide
    quantity: Decimal
    quantity_unit: BrokerQuantityUnit
    average_entry_price: Decimal | None = None
    mark_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    leverage: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerBalance:
    exchange: ExchangeName
    asset: str
    total: Decimal
    available: Decimal | None = None
    frozen: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerOrderRequest:
    exchange: ExchangeName
    symbol: str
    side: BrokerOrderSide
    position_side: BrokerPositionSide
    order_type: BrokerOrderType
    quantity: Decimal
    quantity_unit: BrokerQuantityUnit
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    reduce_only: bool = False
    close_position: bool = False
    client_order_id: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerOrderResult:
    exchange: ExchangeName
    symbol: str
    ok: bool
    order_id: str | None = None
    client_order_id: str | None = None
    order: BrokerOrder | None = None
    message: str = ""
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerCancelResult:
    exchange: ExchangeName
    symbol: str
    ok: bool
    order_id: str | None = None
    client_order_id: str | None = None
    message: str = ""
    raw: Mapping[str, Any] = field(default_factory=dict)
