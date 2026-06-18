from __future__ import annotations

from typing import Protocol

from src.platform.exchanges.models import AmendOrderRequest, CancelOrderRequest, ExchangeName, Order, OrderQuery, OrderRequest
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
