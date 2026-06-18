from __future__ import annotations

import asyncio
from dataclasses import dataclass

from src.platform.execution.ports import ExecutionClient
from src.platform.exchanges.models import (
    CancelOrderRequest,
    CancelStopOrderRequest,
    ExchangeName,
    Order,
    OrderRequest,
    StopMarketOrderRequest,
)


@dataclass(frozen=True)
class ExecutionResult:
    exchange: ExchangeName
    order: Order | None = None
    error: Exception | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.order is not None


class MultiExchangeExecutionClient:
    """Fan-out execution facade for sending one action to multiple exchanges."""

    def __init__(self, clients: list[ExecutionClient], *, fail_fast: bool = False) -> None:
        if not clients:
            raise ValueError("at least one execution client is required")
        self._clients = clients
        self._fail_fast = fail_fast

    async def place_order_all(self, request: OrderRequest) -> list[ExecutionResult]:
        return await self._run_all(lambda client: client.place_order(request))

    async def cancel_order_all(self, request: CancelOrderRequest) -> list[ExecutionResult]:
        return await self._run_all(lambda client: client.cancel_order(request))

    async def cancel_all_orders_all(self) -> list[ExecutionResult]:
        return await self._run_all(lambda client: _first_order_or_none(client.cancel_all_orders()))

    async def place_stop_market_order_all(self, request: StopMarketOrderRequest) -> list[ExecutionResult]:
        return await self._run_all(lambda client: client.place_stop_market_order(request))

    async def cancel_stop_order_all(self, request: CancelStopOrderRequest) -> list[ExecutionResult]:
        return await self._run_all(lambda client: client.cancel_stop_order(request))

    async def cancel_all_stop_orders_all(self) -> list[ExecutionResult]:
        return await self._run_all(lambda client: _first_order_or_none(client.cancel_all_stop_orders()))

    async def _run_all(self, call):
        if self._fail_fast:
            results = []
            for client in self._clients:
                results.append(await self._run_one(client, call))
                if not results[-1].ok:
                    break
            return results
        return list(await asyncio.gather(*(self._run_one(client, call) for client in self._clients)))

    async def _run_one(self, client: ExecutionClient, call) -> ExecutionResult:
        try:
            order = await call(client)
            return ExecutionResult(exchange=client.exchange, order=order)
        except Exception as exc:  # pragma: no cover - branch covered by tests via behavior
            return ExecutionResult(exchange=client.exchange, error=exc)


async def _first_order_or_none(coro) -> Order | None:
    orders = await coro
    return orders[0] if orders else None
