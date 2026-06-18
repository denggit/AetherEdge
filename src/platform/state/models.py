from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Mapping

from src.platform.account.events import AccountEventType
from src.platform.exchanges.models import ExchangeName, OrderSide, OrderStatus, OrderType, PositionMode, PositionSide


@dataclass(frozen=True)
class StoredOrder:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str | None
    order_id: str | None
    client_order_id: str | None
    status: OrderStatus
    side: OrderSide | None = None
    order_type: OrderType | None = None
    price: Decimal | None = None
    quantity: Decimal | None = None
    filled_quantity: Decimal | None = None
    updated_time_ms: int | None = None
    is_stop_order: bool = False
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StoredFill:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str | None
    order_id: str | None
    trade_id: str
    side: OrderSide | None = None
    price: Decimal | None = None
    quantity: Decimal | None = None
    fee: Decimal | None = None
    fee_asset: str | None = None
    event_time_ms: int | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StoredAccountSnapshot:
    exchange: ExchangeName
    symbol: str
    asset: str
    total: Decimal
    available: Decimal
    positions_json: str
    leverage: Decimal | None
    position_mode: PositionMode
    created_time_ms: int
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StoredEvent:
    id: int
    exchange: ExchangeName
    event_type: AccountEventType
    symbol: str | None
    event_time_ms: int | None
    raw: Mapping[str, Any] = field(default_factory=dict)
