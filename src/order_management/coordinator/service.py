from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Sequence

from src.order_management.idempotency.client_order_id import DeterministicClientOrderIdFactory
from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus
from src.order_management.ports import ClientOrderIdFactory, DuplicateOrderGuard, OrderIntentRepository
from src.planner import ExecutionPlanner, PlannedExecution, PlannedExecutionAction
from src.platform.execution import ExecutionClient
from src.platform.exchanges.models import Order, OrderRequest, StopMarketOrderRequest


class MultiExchangeOrderCoordinator:
    """Execute one strategy intent across multiple exchange clients."""

    def __init__(
        self,
        *,
        clients: Sequence[ExecutionClient],
        repository: OrderIntentRepository,
        planner: ExecutionPlanner | None = None,
        client_order_id_factory: ClientOrderIdFactory | None = None,
        duplicate_guard: DuplicateOrderGuard | None = None,
    ) -> None:
        if not clients:
            raise ValueError("at least one execution client is required")
        self.clients = tuple(clients)
        self.repository = repository
        self.planner = planner or ExecutionPlanner()
        self.client_order_id_factory = client_order_id_factory or DeterministicClientOrderIdFactory()
        self.duplicate_guard = duplicate_guard

    async def execute(self, intent: OrderIntent) -> list[ExchangeOrderResult]:
        if self.duplicate_guard is not None:
            self.duplicate_guard.assert_not_duplicate(intent)
        self.repository.save_intent(intent)
        self.repository.update_status(intent_id=intent.intent_id, status=OrderIntentStatus.PLANNED)
        plan = self.planner.plan(intent.signal)
        target_values = {exchange.value for exchange in intent.target_exchanges}
        clients = [client for client in self.clients if client.exchange.value in target_values]
        results_nested = await asyncio.gather(*(self._execute_for_client(client, intent, plan.items) for client in clients))
        results = [item for group in results_nested for item in group]
        for result in results:
            save_result = getattr(self.repository, "save_result", None)
            if save_result is not None:
                save_result(intent_id=intent.intent_id, result=result)
        final_status = _final_status(results)
        self.repository.update_status(intent_id=intent.intent_id, status=final_status)
        return results

    async def _execute_for_client(self, client: ExecutionClient, intent: OrderIntent, items: Sequence[PlannedExecution]) -> list[ExchangeOrderResult]:
        results: list[ExchangeOrderResult] = []
        for sequence, item in enumerate(items):
            client_order_id = self.client_order_id_factory.create(strategy_id=intent.strategy_id, signal=item.signal, exchange=client.exchange, sequence=sequence)
            try:
                order = await self._execute_item(client, item, client_order_id=client_order_id)
                results.append(_order_to_result(order))
            except Exception as exc:
                results.append(ExchangeOrderResult(exchange=client.exchange, ok=False, client_order_id=client_order_id, error=str(exc)))
        return results

    async def _execute_item(self, client: ExecutionClient, item: PlannedExecution, *, client_order_id: str) -> Order:
        if item.action is PlannedExecutionAction.PLACE_ORDER:
            if item.order_request is None:
                raise ValueError("order_request is required")
            return await client.place_order(_with_order_client_id(item.order_request, client_order_id))
        if item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
            if item.stop_market_request is None:
                raise ValueError("stop_market_request is required")
            return await client.place_stop_market_order(_with_stop_client_id(item.stop_market_request, client_order_id))
        if item.action is PlannedExecutionAction.CANCEL_ALL_ORDERS:
            orders = await client.cancel_all_orders()
            return orders[0] if orders else _synthetic_order(client, client_order_id)
        if item.action is PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS:
            orders = await client.cancel_all_stop_orders()
            return orders[0] if orders else _synthetic_order(client, client_order_id)
        raise ValueError(f"unsupported planned action: {item.action}")


def _with_order_client_id(request: OrderRequest, client_order_id: str) -> OrderRequest:
    return replace(request, client_order_id=request.client_order_id or client_order_id)


def _with_stop_client_id(request: StopMarketOrderRequest, client_order_id: str) -> StopMarketOrderRequest:
    return replace(request, client_order_id=request.client_order_id or client_order_id)


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


def _synthetic_order(client: ExecutionClient, client_order_id: str) -> Order:
    from src.platform.exchanges.models import OrderStatus

    return Order(exchange=client.exchange, symbol=client.symbol, raw_symbol=client.symbol, order_id=None, client_order_id=client_order_id, status=OrderStatus.CANCELED)


def _final_status(results: Sequence[ExchangeOrderResult]) -> OrderIntentStatus:
    if not results:
        return OrderIntentStatus.FAILED
    ok_count = sum(1 for result in results if result.ok)
    if ok_count == len(results):
        return OrderIntentStatus.SUBMITTED
    if ok_count > 0:
        return OrderIntentStatus.PARTIALLY_SUBMITTED
    return OrderIntentStatus.FAILED
