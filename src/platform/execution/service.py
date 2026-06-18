from __future__ import annotations

from src.platform.exchanges.models import CancelOrderRequest, ExchangeName, Order, OrderRequest
from src.platform.exchanges.ports import ExchangeExecutionClient


class ExchangeExecutionService:
    """Execution facade: order actions only, no market data or account reads."""

    def __init__(self, exchange_client: ExchangeExecutionClient) -> None:
        self._exchange_client = exchange_client

    @property
    def exchange(self) -> ExchangeName:
        return self._exchange_client.exchange

    async def place_order(self, request: OrderRequest) -> Order:
        return await self._exchange_client.place_order(request)

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        return await self._exchange_client.cancel_order(request)

    async def replace_order(self, cancel_request: CancelOrderRequest, new_order: OrderRequest) -> Order:
        # Portable first version: cancel then place. Later this can route to native
        # amend endpoints per exchange without changing runtime code.
        await self.cancel_order(cancel_request)
        return await self.place_order(new_order)
