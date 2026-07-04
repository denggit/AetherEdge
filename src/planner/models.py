from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from src.platform.exchanges.models import CancelStopOrderRequest, OrderRequest, StopMarketOrderRequest
from src.signals.models import TradeSignal


class PlannedExecutionAction(str, Enum):
    PLACE_ORDER = "place_order"
    PLACE_STOP_MARKET_ORDER = "place_stop_market_order"
    CANCEL_ALL_ORDERS = "cancel_all_orders"
    CANCEL_ALL_STOP_ORDERS = "cancel_all_stop_orders"
    CANCEL_STOP_ORDER = "cancel_stop_order"


@dataclass(frozen=True)
class PlannedExecution:
    action: PlannedExecutionAction
    signal: TradeSignal
    order_request: OrderRequest | None = None
    stop_market_request: StopMarketOrderRequest | None = None
    cancel_stop_request: CancelStopOrderRequest | None = None


@dataclass(frozen=True)
class ExecutionPlan:
    items: tuple[PlannedExecution, ...]

    @classmethod
    def single(cls, item: PlannedExecution) -> "ExecutionPlan":
        return cls((item,))
