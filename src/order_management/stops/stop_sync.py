from __future__ import annotations

import asyncio
from typing import Sequence

from src.order_management.models import ExchangeOrderResult
from src.platform.execution import ExecutionClient
from src.platform.exchanges.models import Order, StopMarketOrderRequest


class StopOrderSyncService:
    """Synchronize reduce-only stop orders across execution clients.

    The stop price and quantity are decided by the strategy. This service only
    handles cancel/replace mechanics and per-exchange result collection.
    """

    def __init__(self, clients: Sequence[ExecutionClient]) -> None:
        if not clients:
            raise ValueError("at least one execution client is required")
        self.clients = tuple(clients)

    async def replace_all(self, request: StopMarketOrderRequest) -> list[ExchangeOrderResult]:
        groups = await asyncio.gather(*(self._replace_one(client, request) for client in self.clients))
        return list(groups)

    async def _replace_one(self, client: ExecutionClient, request: StopMarketOrderRequest) -> ExchangeOrderResult:
        try:
            await client.cancel_all_stop_orders()
            order = await client.place_stop_market_order(request)
            return _order_to_result(order)
        except Exception as exc:
            return ExchangeOrderResult(exchange=client.exchange, ok=False, client_order_id=request.client_order_id, error=str(exc))


def _order_to_result(order: Order) -> ExchangeOrderResult:
    return ExchangeOrderResult(
        exchange=order.exchange,
        ok=True,
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        status=order.status,
        side=order.side,
        quantity=order.quantity,
        raw=order.raw,
    )
