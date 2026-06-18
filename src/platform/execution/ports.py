from __future__ import annotations

from typing import Protocol

from src.platform.exchanges.models import AmendOrderRequest, CancelOrderRequest, ExchangeName, Order, OrderQuery, OrderRequest, Position, StopMarketOrderRequest, TriggerPriceType
from src.platform.markets import MarketProfile


class ExecutionClient(Protocol):
    """Single execution interface used by runtime code."""

    @property
    def exchange(self) -> ExchangeName:
        ...

    @property
    def symbol(self) -> str:
        ...

    @property
    def market_profile(self) -> MarketProfile:
        ...

    async def place_order(self, request: OrderRequest) -> Order:
        ...

    async def place_stop_market_order(self, request: StopMarketOrderRequest) -> Order:
        ...

    async def place_stop_loss_for_position(
        self,
        position: Position,
        *,
        trigger_price,
        client_order_id: str | None = None,
        trigger_price_type: TriggerPriceType = TriggerPriceType.LAST,
    ) -> Order:
        ...

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        ...

    async def amend_order(self, request: AmendOrderRequest) -> Order:
        ...

    async def fetch_order_status(self, query: OrderQuery) -> Order:
        ...

    async def fetch_open_orders(self) -> list[Order]:
        ...

    async def replace_order(self, cancel_request: CancelOrderRequest, new_order: OrderRequest) -> Order:
        ...
