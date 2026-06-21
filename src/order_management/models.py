from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from src.platform.exchanges.models import ExchangeName, OrderSide, OrderStatus
from src.signals.models import TradeSignal


class OrderIntentStatus(str, Enum):
    CREATED = "created"
    PLANNED = "planned"
    SUBMITTED = "submitted"
    PARTIALLY_SUBMITTED = "partially_submitted"
    FILLED = "filled"
    FAILED = "failed"
    RECOVERED = "recovered"
    CANCELED = "canceled"


@dataclass(frozen=True)
class OrderIntent:
    """Durable command describing one strategy execution intent."""

    intent_id: str
    strategy_id: str
    signal: TradeSignal
    target_exchanges: tuple[ExchangeName, ...]
    status: OrderIntentStatus = OrderIntentStatus.CREATED
    created_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.intent_id:
            raise ValueError("intent_id is required")
        if not self.strategy_id:
            raise ValueError("strategy_id is required")
        if not self.target_exchanges:
            raise ValueError("target_exchanges must not be empty")


@dataclass(frozen=True)
class ExchangeOrderResult:
    exchange: ExchangeName
    ok: bool
    order_id: str | None = None
    client_order_id: str | None = None
    status: OrderStatus | None = None
    side: OrderSide | None = None
    quantity: Decimal | None = None
    error: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrderJournalEvent:
    intent_id: str
    status: OrderIntentStatus
    message: str = ""
    exchange: ExchangeName | None = None
    created_time_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    metadata: Mapping[str, Any] = field(default_factory=dict)
