from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from typing import Sequence

from src.order_management.idempotency.client_order_id import DeterministicClientOrderIdFactory
from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus
from src.order_management.position_plan import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.order_management.ports import ClientOrderIdFactory, DuplicateOrderGuard, OrderIntentRepository
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.master_follower import MasterFollowerExecutionPolicy, MasterFollowerPolicyEvaluator
from src.order_management.sync import OrderStatusSynchronizer, extract_avg_fill_price, extract_fee
from src.planner import ExecutionPlanner, PlannedExecution, PlannedExecutionAction
from src.platform.execution import ExecutionClient
from src.platform.exchanges.models import ExchangeName, Order, OrderRequest, OrderStatus, StopMarketOrderRequest
from src.signals.models import SignalAction
from src.utils.log import get_logger


_MASTER_GATED_PURPOSES = {"normal_entry", "normal_close"}
_BYPASS_MASTER_PURPOSES = {
    "stop_sync",
    "follower_recovery_topup",
    "follower_close_after_master_close",
}

logger = get_logger(__name__)


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
        position_plan_store=None,
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
        self.position_plan_store = position_plan_store

    async def execute(self, intent: OrderIntent) -> list[ExchangeOrderResult]:
        if self.duplicate_guard is not None:
            self.duplicate_guard.assert_not_duplicate(intent)
        plan = self.planner.plan(intent.signal)
        target_values = {exchange.value for exchange in intent.target_exchanges}
        clients = [client for client in self.clients if client.exchange.value in target_values]
        intent = self._with_execution_metadata(intent, clients=clients, items=plan.items)
        logger.info(
            "Order intent planned | intent_id=%s action=%s targets=%s planned_items=%s master_follower=%s",
            intent.intent_id,
            intent.signal.action.value,
            ",".join(exchange.value for exchange in intent.target_exchanges),
            len(plan.items),
            self.master_follower_policy is not None,
        )
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
        self._record_position_plan(intent, results)
        final_status = _final_status(results)
        self.repository.update_status(intent_id=intent.intent_id, status=final_status)
        ok_count = sum(1 for result in results if result.ok)
        if final_status is OrderIntentStatus.SUBMITTED:
            logger.info("Order intent completed | intent_id=%s status=%s ok=%s total=%s", intent.intent_id, final_status.value, ok_count, len(results))
        elif final_status is OrderIntentStatus.PARTIALLY_SUBMITTED:
            logger.warning(
                "Order intent partially completed | intent_id=%s status=%s ok=%s total=%s errors=%s",
                intent.intent_id,
                final_status.value,
                ok_count,
                len(results),
                [result.error for result in results if not result.ok],
            )
        else:
            logger.error(
                "Order intent failed | intent_id=%s status=%s ok=%s total=%s errors=%s",
                intent.intent_id,
                final_status.value,
                ok_count,
                len(results),
                [result.error for result in results if not result.ok],
            )
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

    def _record_position_plan(self, intent: OrderIntent, results: Sequence[ExchangeOrderResult]) -> None:
        if self.position_plan_store is None:
            return
        signal = intent.signal
        purpose = str(signal.metadata.get("execution_purpose", "") if signal.metadata else "").strip().lower()
        if signal.action in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT}:
            self._record_open_or_topup_plan(intent, results, purpose=purpose)
        elif signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            self._record_stop_plan(intent, results)
        elif signal.action in {SignalAction.CLOSE_LONG, SignalAction.CLOSE_SHORT}:
            self._record_close_plan(intent, results, purpose=purpose)

    def _record_open_or_topup_plan(self, intent: OrderIntent, results: Sequence[ExchangeOrderResult], *, purpose: str) -> None:
        signal = intent.signal
        if signal.quantity is None or signal.quantity <= 0:
            return
        position_id = str(signal.metadata.get("position_id") or f"pos-{intent.intent_id}") if signal.metadata else f"pos-{intent.intent_id}"
        side = "long" if signal.action is SignalAction.OPEN_LONG else "short"
        existing = self.position_plan_store.get_position(position_id)
        master_exchange = self.master_follower_policy.master_exchange if self.master_follower_policy is not None else intent.target_exchanges[0]
        entry_engine = str(signal.metadata.get("engine") or (existing.entry_engine if existing else "unknown")) if signal.metadata else (existing.entry_engine if existing else "unknown")
        stop_price = _optional_decimal(signal.metadata.get("estimated_initial_stop") if signal.metadata else None)
        if existing is None:
            master_target_qty = _signal_exchange_quantity(signal, master_exchange, fallback=Decimal("0")) if master_exchange in intent.target_exchanges else Decimal("0")
            self.position_plan_store.upsert_position(
                PositionPlan(
                    position_id=position_id,
                    strategy_id=intent.strategy_id,
                    entry_engine=entry_engine,
                    side=side,
                    status=PositionPlanStatus.ACTIVE,
                    canonical_stop_price=stop_price,
                    master_exchange=master_exchange,
                    master_target_qty_base=master_target_qty,
                    master_filled_qty_base=Decimal("0"),
                    metadata={"intent_id": intent.intent_id},
                )
            )
            for exchange in intent.target_exchanges:
                role = LegRole.MASTER if exchange is master_exchange else LegRole.FOLLOWER
                target_qty = _signal_exchange_quantity(signal, exchange, fallback=signal.quantity)
                self.position_plan_store.upsert_leg(
                    LegPlan(
                        position_id=position_id,
                        exchange=exchange,
                        role=role,
                        target_qty_base=target_qty,
                        sync_status=LegSyncStatus.PLANNED,
                    )
                )
        elif purpose != "follower_recovery_topup":
            for exchange in intent.target_exchanges:
                delta_qty = _signal_exchange_quantity(signal, exchange, fallback=signal.quantity)
                self.position_plan_store.add_to_leg_target(position_id=position_id, exchange=exchange, delta_target_qty_base=delta_qty)
            if master_exchange in intent.target_exchanges:
                master_delta = _signal_exchange_quantity(signal, master_exchange, fallback=signal.quantity)
                self.position_plan_store.upsert_position(replace(existing, master_target_qty_base=existing.master_target_qty_base + master_delta))
        by_exchange = {result.exchange: result for result in results}
        for exchange in intent.target_exchanges:
            result = by_exchange.get(exchange)
            if result is None:
                continue
            leg = {item.exchange: item for item in self.position_plan_store.get_legs(position_id)}.get(exchange)
            if leg is None:
                continue
            if result.ok:
                target_qty = _signal_exchange_quantity(signal, exchange, fallback=signal.quantity)
                filled = target_qty if result.filled_quantity is None else min(target_qty, _result_filled_base(result, fallback=target_qty))
                self.position_plan_store.upsert_leg(
                    replace(
                        leg,
                        filled_qty_base=max(leg.filled_qty_base, filled) if purpose == "follower_recovery_topup" else leg.filled_qty_base + filled,
                        entry_order_id=result.order_id or leg.entry_order_id,
                        entry_client_order_id=result.client_order_id or leg.entry_client_order_id,
                        sync_status=LegSyncStatus.TOPUP_SUBMITTED if purpose == "follower_recovery_topup" else LegSyncStatus.OPEN,
                    )
                )
                if exchange is master_exchange:
                    plan = self.position_plan_store.get_position(position_id)
                    if plan is not None:
                        self.position_plan_store.upsert_position(replace(plan, master_filled_qty_base=plan.master_filled_qty_base + filled))
            elif purpose == "follower_recovery_topup":
                self.position_plan_store.update_leg_sync_status(position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.TOPUP_FAILED)

    def _record_stop_plan(self, intent: OrderIntent, results: Sequence[ExchangeOrderResult]) -> None:
        signal = intent.signal
        if signal.trigger_price is None:
            return
        if not signal.metadata or not signal.metadata.get("position_id"):
            return
        position_id = str(signal.metadata["position_id"])
        for result in results:
            if result.ok:
                self.position_plan_store.update_stop(
                    position_id=position_id,
                    exchange=result.exchange,
                    stop_price=signal.trigger_price,
                    stop_order_id=result.order_id,
                    stop_client_order_id=result.client_order_id,
                )

    def _record_close_plan(self, intent: OrderIntent, results: Sequence[ExchangeOrderResult], *, purpose: str) -> None:
        signal = intent.signal
        if not signal.metadata or not signal.metadata.get("position_id"):
            return
        position_id = str(signal.metadata["position_id"])
        existing = self.position_plan_store.get_position(position_id)
        if existing is None:
            return
        by_exchange = {result.exchange: result for result in results}
        master_exchange = existing.master_exchange

        if purpose == "follower_close_after_master_close":
            for exchange in intent.target_exchanges:
                result = by_exchange.get(exchange)
                if result is None:
                    self.position_plan_store.update_leg_sync_status(
                        position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
                    )
                    continue
                if _result_is_filled(result):
                    self.position_plan_store.update_leg_sync_status(
                        position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.CLOSED,
                    )
                else:
                    self.position_plan_store.update_leg_sync_status(
                        position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
                    )
            self._maybe_close_position_plan(position_id)
            return

        if purpose == "normal_close":
            master_result = by_exchange.get(master_exchange)
            if master_result is not None and _result_is_filled(master_result):
                self.position_plan_store.update_leg_sync_status(
                    position_id=position_id, exchange=master_exchange, sync_status=LegSyncStatus.CLOSED,
                )
            elif master_result is not None:
                # Master close was attempted but not filled — leave leg status
                # unchanged so the runtime can retry or alert.
                logger.warning(
                    "Master close result not filled | position_id=%s exchange=%s status=%s filled_qty=%s",
                    position_id,
                    master_exchange.value,
                    master_result.status.value if master_result.status else "unknown",
                    str(master_result.filled_quantity or Decimal("0")),
                )
            for exchange in intent.target_exchanges:
                if exchange == master_exchange:
                    continue
                result = by_exchange.get(exchange)
                if result is not None and _result_is_filled(result):
                    self.position_plan_store.update_leg_sync_status(
                        position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.CLOSED,
                    )
                elif result is not None:
                    self.position_plan_store.update_leg_sync_status(
                        position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
                    )
                else:
                    # Follower close result is missing entirely — mark failed.
                    self.position_plan_store.update_leg_sync_status(
                        position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
                    )
            has_unresolved = False
            for leg in self.position_plan_store.get_legs(position_id):
                if leg.exchange == master_exchange or leg.role == LegRole.MASTER:
                    continue
                if leg.sync_status != LegSyncStatus.CLOSED:
                    has_unresolved = True
                    break
            if has_unresolved:
                self.position_plan_store.upsert_position(
                    replace(existing, status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED),
                )
            else:
                self._maybe_close_position_plan(position_id)
            return

        self._maybe_close_position_plan(position_id)

    def _maybe_close_position_plan(self, position_id: str) -> None:
        plan = self.position_plan_store.get_position(position_id)
        if plan is None:
            return
        legs = self.position_plan_store.get_legs(position_id)
        all_closed = all(
            leg.sync_status == LegSyncStatus.CLOSED for leg in legs
            if leg.role in {LegRole.MASTER, LegRole.FOLLOWER}
        )
        if all_closed and plan.status != PositionPlanStatus.CLOSED:
            self.position_plan_store.upsert_position(replace(plan, status=PositionPlanStatus.CLOSED))

    async def _execute_master_follower(self, clients: Sequence[ExecutionClient], intent: OrderIntent, items: Sequence[PlannedExecution]) -> list[ExchangeOrderResult]:
        assert self.master_follower_policy is not None
        if self._bypasses_master_gating(intent):
            purpose = str(intent.signal.metadata.get("execution_purpose", "") if intent.signal.metadata else "").strip().lower()
            if purpose == "follower_close_after_master_close":
                close_retry = self.master_follower_policy.follower_close_retry
                results_nested = await asyncio.gather(
                    *(
                        self._execute_for_client(
                            client,
                            intent,
                            items,
                            max_attempts=close_retry.max_attempts,
                            retry_delay_seconds=close_retry.retry_delay_seconds,
                        )
                        for client in clients
                    )
                )
            else:
                results_nested = await asyncio.gather(*(self._execute_for_client(client, intent, items) for client in clients))
            return [item for group in results_nested for item in group]
        client_by_exchange = {client.exchange: client for client in clients}
        master = client_by_exchange.get(self.master_follower_policy.master_exchange)
        followers = [client_by_exchange[exchange] for exchange in self.master_follower_policy.followers_for(intent.target_exchanges) if exchange in client_by_exchange]
        if master is None:
            logger.error("Master execution client unavailable | intent_id=%s master=%s", intent.intent_id, self.master_follower_policy.master_exchange.value)
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
            logger.warning("Master execution failed; followers skipped | intent_id=%s master=%s", intent.intent_id, self.master_follower_policy.master_exchange.value)
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

    def _bypasses_master_gating(self, intent: OrderIntent) -> bool:
        purpose = str(intent.signal.metadata.get("execution_purpose", "") if intent.signal.metadata else "").strip().lower()
        if purpose in _BYPASS_MASTER_PURPOSES:
            return True
        if purpose in _MASTER_GATED_PURPOSES:
            return False
        if self.master_follower_policy.master_exchange not in intent.target_exchanges:
            # Leg-specific stop replacement, follower recovery top-up, and
            # follower close intents intentionally target only follower venues.
            # They must not require a master client in the filtered target set.
            return True
        if intent.signal.action in {
            SignalAction.CANCEL_ALL_STOP_ORDERS,
            SignalAction.PLACE_STOP_LOSS_LONG,
            SignalAction.PLACE_STOP_LOSS_SHORT,
        }:
            return True
        return False

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
                    order = await self._execute_item(client, item, intent=intent, client_order_id=client_order_id)
                    synced = await self.order_status_synchronizer.sync_after_submit(client=client, item=item, order=order)
                    results.append(_order_to_result(synced, attempts=attempt + 1))
                    break
                except Exception as exc:
                    last_error = exc
                    logger.warning(
                        "Order execution attempt failed | intent_id=%s exchange=%s action=%s attempt=%s max_attempts=%s error=%s",
                        intent.intent_id,
                        client.exchange.value,
                        item.action.value,
                        attempt + 1,
                        max_attempts,
                        exc,
                    )
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

    async def _execute_item(self, client: ExecutionClient, item: PlannedExecution, *, intent: OrderIntent, client_order_id: str) -> Order:
        if item.action is PlannedExecutionAction.PLACE_ORDER:
            if item.order_request is None:
                raise ValueError("order_request is required")
            request = self._convert_order_for_client(client, _with_exchange_quantity(item.order_request, intent=intent, exchange=client.exchange))
            return await client.place_order(_with_order_client_id(request, client_order_id))
        if item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
            if item.stop_market_request is None:
                raise ValueError("stop_market_request is required")
            request = self._convert_stop_for_client(client, _with_exchange_quantity(item.stop_market_request, intent=intent, exchange=client.exchange))
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
                conversion = self._preview_conversion(client, item, intent=intent)
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

    def _preview_conversion(self, client: ExecutionClient, item: PlannedExecution, *, intent: OrderIntent) -> dict[str, object] | None:
        profile = _client_market_profile(client)
        if profile is None:
            return None
        try:
            if item.action is PlannedExecutionAction.PLACE_ORDER and item.order_request is not None:
                request = _with_exchange_quantity(item.order_request, intent=intent, exchange=client.exchange)
                _, conversion = self.quantity_converter.convert_order_request(
                    request,
                    exchange=client.exchange,
                    market_profile=profile,
                )
                return conversion.metadata()
            if item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER and item.stop_market_request is not None:
                request = _with_exchange_quantity(item.stop_market_request, intent=intent, exchange=client.exchange)
                _, conversion = self.quantity_converter.convert_stop_market_request(
                    request,
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


def _signal_exchange_quantities(signal) -> dict[ExchangeName, Decimal]:
    raw = signal.metadata.get("exchange_quantities_base") if signal.metadata else None
    if raw is None:
        raw = signal.metadata.get("per_exchange_quantity_base") if signal.metadata else None
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[ExchangeName, Decimal] = {}
    for key, value in raw.items():
        try:
            exchange = key if isinstance(key, ExchangeName) else ExchangeName(str(key).strip().lower())
            qty = Decimal(str(value))
        except Exception:
            continue
        if qty > 0:
            out[exchange] = qty
    return out


def _signal_exchange_quantity(signal, exchange: ExchangeName, *, fallback: Decimal | None) -> Decimal:
    quantities = _signal_exchange_quantities(signal)
    value = quantities.get(exchange)
    if value is not None and value > 0:
        return value
    if fallback is None:
        return Decimal("0")
    return fallback


def _with_exchange_quantity(request, *, intent: OrderIntent, exchange: ExchangeName):
    quantity = getattr(request, "quantity", None)
    if quantity is None:
        return request
    override = _signal_exchange_quantities(intent.signal).get(exchange)
    if override is None or override <= 0:
        return request
    return replace(request, quantity=override)


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


def _optional_decimal(value) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _result_is_filled(result: ExchangeOrderResult) -> bool:
    """A close/entry result is only considered truly filled when the exchange
    confirms FILLED status AND a positive filled quantity."""
    return (
        result.ok
        and result.status is OrderStatus.FILLED
        and result.filled_quantity is not None
        and result.filled_quantity > Decimal("0")
    )


def _result_filled_base(result: ExchangeOrderResult, *, fallback: Decimal) -> Decimal:
    if result.filled_quantity is not None and result.filled_quantity > 0:
        return result.filled_quantity
    if result.quantity is not None and result.quantity > 0:
        return result.quantity
    return fallback
