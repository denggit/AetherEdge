from __future__ import annotations

from typing import Protocol

from src.platform.exchanges.models import CancelOrderRequest, ExchangeName, Order, OrderRequest


class ExecutionClient(Protocol):
    """Single execution interface used by runtime code."""

    @property
    def exchange(self) -> ExchangeName:
        ...

    async def place_order(self, request: OrderRequest) -> Order:
        ...

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        ...

    async def replace_order(self, cancel_request: CancelOrderRequest, new_order: OrderRequest) -> Order:
        ...
