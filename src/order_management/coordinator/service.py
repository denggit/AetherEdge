from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Sequence

from src.order_management.idempotency.client_order_id import DeterministicClientOrderIdFactory
from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus
from src.order_management.ports import ClientOrderIdFactory, DuplicateOrderGuard, OrderIntentRepository
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.master_follower import MasterFollowerExecutionPolicy, MasterFollowerPolicyEvaluator
from src.order_management.sync import OrderStatusSynchronizer, extract_avg_fill_price, extract_fee
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
        quantity_converter: NativeQuantityConverter | None = None,
        order_status_synchronizer: OrderStatusSynchronizer | None = None,
        master_follower_policy: MasterFollowerExecutionPolicy | None = None,
    ) -> None:
        if not clients:
            raise ValueError("at least one execution client is required")
        self.clients = tuple(clients)
        self.repository = repository
        self.planner = planner or ExecutionPlanner()
        self.client_order_id_factory = client_order_id_factory or DeterministicClientOrderIdFactory()
        self.duplicate_guard = duplicate_guard
        self.quantity_converter = quantity_converter or NativeQuantityConverter()
        self.order_status_synchronizer = order_status_synchronizer or OrderStatusSynchronizer()
        self.master_follower_policy = master_follower_policy
        self.master_follower_evaluator = MasterFollowerPolicyEvaluator(master_follower_policy) if master_follower_policy is not None else None

    async def execute(self, intent: OrderIntent) -> list[ExchangeOrderResult]:
        if self.duplicate_guard is not None:
            self.duplicate_guard.assert_not_duplicate(intent)
        plan = self.planner.plan(intent.signal)
        target_values = {exchange.value for exchange in intent.target_exchanges}
        clients = [client for client in self.clients if client.exchange.value in target_values]
        intent = self._with_execution_metadata(intent, clients=clients, items=plan.items)
        self.repository.save_intent(intent)
        self.repository.update_status(intent_id=intent.intent_id, status=OrderIntentStatus.PLANNED)
        if self.master_follower_policy is not None:
            results = await self._execute_master_follower(clients, intent, plan.items)
        else:
            results_nested = await asyncio.gather(*(self._execute_for_client(client, intent, plan.items) for client in clients))
            results = [item for group in results_nested for item in group]
        for result in results:
            save_result = getattr(self.repository, "save_result", None)
            if save_result is not None:
                save_result(intent_id=intent.intent_id, result=result)
        final_status = _final_status(results)
        self.repository.update_status(intent_id=intent.intent_id, status=final_status)
        if self.master_follower_evaluator is not None:
            decision = self.master_follower_evaluator.evaluate(intent=intent, results=results)
            add_event = getattr(self.repository, "add_event", None)
            if callable(add_event):
                from src.order_management.models import OrderJournalEvent

                add_event(
                    OrderJournalEvent(
                        intent_id=intent.intent_id,
                        status=final_status,
                        message="master_follower_policy",
                        metadata={
                            "status": decision.status.value,
                            "alerts": list(decision.alerts),
                            "actions": list(decision.actions),
                            **dict(decision.metadata),
                        },
                    )
                )
        return results

    async def _execute_master_follower(self, clients: Sequence[ExecutionClient], intent: OrderIntent, items: Sequence[PlannedExecution]) -> list[ExchangeOrderResult]:
        assert self.master_follower_policy is not None
        client_by_exchange = {client.exchange: client for client in clients}
        master = client_by_exchange.get(self.master_follower_policy.master_exchange)
        followers = [client_by_exchange[exchange] for exchange in self.master_follower_policy.followers_for(intent.target_exchanges) if exchange in client_by_exchange]
        if master is None:
            return [ExchangeOrderResult(exchange=self.master_follower_policy.master_exchange, ok=False, error="master execution client not available")]

        master_results = await self._execute_for_client(
            master,
            intent,
            items,
            max_attempts=self.master_follower_policy.master_entry_retry.max_attempts,
            retry_delay_seconds=self.master_follower_policy.master_entry_retry.retry_delay_seconds,
        )
        # Followers only mirror a successfully submitted master. This prevents
        # creating avoidable orphan follower entries. If an orphan exists from an
        # external/manual flow, the policy evaluator still detects it.
        if not all(result.ok for result in master_results):
            return master_results
        follower_nested = await asyncio.gather(
            *(
                self._execute_for_client(
                    client,
                    intent,
                    items,
                    max_attempts=self.master_follower_policy.follower_entry_retry.max_attempts,
                    retry_delay_seconds=self.master_follower_policy.follower_entry_retry.retry_delay_seconds,
                )
                for client in followers
            )
        )
        return [*master_results, *(item for group in follower_nested for item in group)]

    async def _execute_for_client(
        self,
        client: ExecutionClient,
        intent: OrderIntent,
        items: Sequence[PlannedExecution],
        *,
        max_attempts: int = 1,
        retry_delay_seconds: float = 0.0,
    ) -> list[ExchangeOrderResult]:
        results: list[ExchangeOrderResult] = []
        for sequence, item in enumerate(items):
            client_order_id = self.client_order_id_factory.create(strategy_id=intent.strategy_id, signal=item.signal, exchange=client.exchange, sequence=sequence)
            last_error: Exception | None = None
            for attempt in range(max_attempts):
                try:
                    order = await self._execute_item(client, item, client_order_id=client_order_id)
                    synced = await self.order_status_synchronizer.sync_after_submit(client=client, item=item, order=order)
                    results.append(_order_to_result(synced, attempts=attempt + 1))
                    break
                except Exception as exc:
                    last_error = exc
                    if attempt < max_attempts - 1 and retry_delay_seconds > 0:
                        await asyncio.sleep(retry_delay_seconds)
            else:
                results.append(
                    ExchangeOrderResult(
                        exchange=client.exchange,
                        ok=False,
                        client_order_id=client_order_id,
                        error=str(last_error) if last_error is not None else "execution failed",
                        raw={"attempts": max_attempts},
                    )
                )
        return results

    async def _execute_item(self, client: ExecutionClient, item: PlannedExecution, *, client_order_id: str) -> Order:
        if item.action is PlannedExecutionAction.PLACE_ORDER:
            if item.order_request is None:
                raise ValueError("order_request is required")
            request = self._convert_order_for_client(client, item.order_request)
            return await client.place_order(_with_order_client_id(request, client_order_id))
        if item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
            if item.stop_market_request is None:
                raise ValueError("stop_market_request is required")
            request = self._convert_stop_for_client(client, item.stop_market_request)
            return await client.place_stop_market_order(_with_stop_client_id(request, client_order_id))
        if item.action is PlannedExecutionAction.CANCEL_ALL_ORDERS:
            orders = await client.cancel_all_orders()
            return orders[0] if orders else _synthetic_order(client, client_order_id)
        if item.action is PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS:
            orders = await client.cancel_all_stop_orders()
            return orders[0] if orders else _synthetic_order(client, client_order_id)
        raise ValueError(f"unsupported planned action: {item.action}")

    def _with_execution_metadata(self, intent: OrderIntent, *, clients: Sequence[ExecutionClient], items: Sequence[PlannedExecution]) -> OrderIntent:
        conversions: list[dict[str, object]] = []
        for client in clients:
            for sequence, item in enumerate(items):
                conversion = self._preview_conversion(client, item)
                if conversion:
                    conversion["sequence"] = sequence
                    conversion["action"] = item.action.value
                    conversions.append(conversion)
        metadata = {
            **dict(intent.metadata),
            "action": intent.signal.action.value,
            "signal_created_time_ms": intent.signal.created_time_ms,
            "original_canonical_quantity": None if intent.signal.quantity is None else str(intent.signal.quantity),
            "target_exchanges": [exchange.value for exchange in intent.target_exchanges],
            "quantity_semantics": "base_asset",
            "per_exchange_quantity": conversions,
        }
        return replace(intent, metadata=metadata)

    def _preview_conversion(self, client: ExecutionClient, item: PlannedExecution) -> dict[str, object] | None:
        profile = _client_market_profile(client)
        if profile is None:
            return None
        try:
            if item.action is PlannedExecutionAction.PLACE_ORDER and item.order_request is not None:
                _, conversion = self.quantity_converter.convert_order_request(
                    item.order_request,
                    exchange=client.exchange,
                    market_profile=profile,
                )
                return conversion.metadata()
            if item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER and item.stop_market_request is not None:
                _, conversion = self.quantity_converter.convert_stop_market_request(
                    item.stop_market_request,
                    exchange=client.exchange,
                    market_profile=profile,
                )
                return None if conversion is None else conversion.metadata()
        except Exception as exc:
            return {"exchange": client.exchange.value, "error": str(exc)}
        return None

    def _convert_order_for_client(self, client: ExecutionClient, request: OrderRequest) -> OrderRequest:
        profile = _client_market_profile(client)
        if profile is None:
            return request
        converted, _ = self.quantity_converter.convert_order_request(
            request,
            exchange=client.exchange,
            market_profile=profile,
        )
        return converted

    def _convert_stop_for_client(self, client: ExecutionClient, request: StopMarketOrderRequest) -> StopMarketOrderRequest:
        profile = _client_market_profile(client)
        if profile is None:
            return request
        converted, _ = self.quantity_converter.convert_stop_market_request(
            request,
            exchange=client.exchange,
            market_profile=profile,
        )
        return converted


def _with_order_client_id(request: OrderRequest, client_order_id: str) -> OrderRequest:
    return replace(request, client_order_id=request.client_order_id or client_order_id)


def _with_stop_client_id(request: StopMarketOrderRequest, client_order_id: str) -> StopMarketOrderRequest:
    return replace(request, client_order_id=request.client_order_id or client_order_id)


def _client_market_profile(client: ExecutionClient):
    try:
        return client.market_profile
    except Exception:
        return None


def _order_to_result(order: Order, *, attempts: int = 1) -> ExchangeOrderResult:
    fee, fee_asset = extract_fee(order)
    raw = {**dict(order.raw), "status_sync_attempts": attempts}
    return ExchangeOrderResult(
        exchange=order.exchange,
        ok=True,
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        status=order.status,
        side=order.side,
        quantity=order.quantity,
        filled_quantity=order.filled_quantity,
        avg_fill_price=extract_avg_fill_price(order),
        fee=fee,
        fee_asset=fee_asset,
        raw=raw,
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
