from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from src.platform.exchanges.models import ExchangeName, OrderSide, OrderStatus, PositionSide


class AccountEventType(str, Enum):
    ORDER = "order"
    BALANCE = "balance"
    POSITION = "position"
    SYSTEM = "system"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class AccountEvent:
    """Unified private account/order event.

    This is intentionally generic: strategy/runtime code can consume the stable
    fields and still inspect ``raw`` when an exchange adds extra data.
    """

    exchange: ExchangeName
    event_type: AccountEventType
    symbol: str | None = None
    raw_symbol: str | None = None
    event_time_ms: int | None = None
    order_id: str | None = None
    client_order_id: str | None = None
    order_status: OrderStatus | None = None
    side: OrderSide | None = None
    position_side: PositionSide | None = None
    price: Decimal | None = None
    quantity: Decimal | None = None
    filled_quantity: Decimal | None = None
    asset: str | None = None
    balance: Decimal | None = None
    available: Decimal | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
