from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class SignalAction(str, Enum):
    OPEN_LONG = "open_long"
    OPEN_SHORT = "open_short"
    CLOSE_LONG = "close_long"
    CLOSE_SHORT = "close_short"
    REDUCE_LONG = "reduce_long"
    REDUCE_SHORT = "reduce_short"
    PLACE_STOP_LOSS_LONG = "place_stop_loss_long"
    PLACE_STOP_LOSS_SHORT = "place_stop_loss_short"
    CANCEL_ALL_ORDERS = "cancel_all_orders"
    CANCEL_ALL_STOP_ORDERS = "cancel_all_stop_orders"


class SignalOrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


@dataclass(frozen=True)
class TradeSignal:
    """Strategy output model.

    A signal describes intent only. It must not call exchange APIs or depend on
    OKX/Binance payloads.
    """

    symbol: str
    action: SignalAction
    quantity: Decimal | None = None
    order_type: SignalOrderType = SignalOrderType.MARKET
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    client_order_id: str | None = None
    reason: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)
    created_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if self.action in {SignalAction.CANCEL_ALL_ORDERS, SignalAction.CANCEL_ALL_STOP_ORDERS}:
            return
        if self.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            if self.trigger_price is None or self.trigger_price <= 0:
                raise ValueError("trigger_price must be positive for stop-loss signals")
        if self.quantity is None or self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.order_type is SignalOrderType.LIMIT and (self.price is None or self.price <= 0):
            raise ValueError("price must be positive for limit signals")


@dataclass(frozen=True)
class SignalBatch:
    signals: tuple[TradeSignal, ...]

    @classmethod
    def from_iterable(cls, signals) -> "SignalBatch":
        return cls(tuple(signals))
