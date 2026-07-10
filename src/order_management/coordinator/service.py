from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence

from src.order_management.idempotency.client_order_id import DeterministicClientOrderIdFactory
from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus, OrderJournalEvent
from src.order_management.position_plan import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.order_management.ports import ClientOrderIdFactory, DuplicateOrderGuard, OrderIntentRepository
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.master_follower import MasterFollowerExecutionPolicy, MasterFollowerPolicyEvaluator
from src.order_management.safety import ExitSafetyError, ExitSafetyGuard, is_exit_action, normalize_exit_request_for_exchange, target_position_side_for_action
from src.order_management.sync import OrderStatusSynchronizer, extract_avg_fill_price, extract_fee
from src.planner import ExecutionPlanner, PlannedExecution, PlannedExecutionAction
from src.platform.execution import ExecutionClient
from src.platform.exchanges.models import CancelStopOrderRequest, ExchangeName, Order, OrderRequest, OrderStatus, PositionMode, PositionSide, StopMarketOrderRequest
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
        exit_safety_guard: ExitSafetyGuard | None = None,
        position_plan_store=None,
        post_result_validator=None,
    ) -> None:
        if not clients:
            raise ValueError("at least one execution client is required")
        self.clients = tuple(clients)
        self.repository = repository
        self.planner = planner or ExecutionPlanner()
        self.client_order_id_factory = client_order_id_factory or DeterministicClientOrderIdFactory()
        self.duplicate_guard = duplicate_guard
        self.quantity_converter = quantity_converter or NativeQuantityConverter()
        self.exit_safety_guard = exit_safety_guard or ExitSafetyGuard(quantity_converter=self.quantity_converter)
        self.order_status_synchronizer = order_status_synchronizer or OrderStatusSynchronizer()
        self.master_follower_policy = master_follower_policy
        self.master_follower_evaluator = MasterFollowerPolicyEvaluator(master_follower_policy) if master_follower_policy is not None else None
        self.position_plan_store = position_plan_store
        self.post_result_validator = post_result_validator
        self._position_mode_cache: dict[ExchangeName, PositionMode] = {}

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
        if callable(self.post_result_validator):
            results = list(await self.post_result_validator(intent=intent, results=tuple(results)))
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
            plan_metadata = _position_plan_metadata(
                signal.metadata,
                intent_id=intent.intent_id,
            )
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
                    metadata=plan_metadata,
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
        master_result = by_exchange.get(master_exchange)
        master_entry_ok = bool(master_result and master_result.ok)
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
                        metadata = dict(plan.metadata)
                        if (
                            result.avg_fill_price is not None
                            and result.avg_fill_price > 0
                        ):
                            metadata["average_entry_price"] = str(
                                result.avg_fill_price
                            )
                        self.position_plan_store.upsert_position(
                            replace(
                                plan,
                                master_filled_qty_base=(
                                    plan.master_filled_qty_base + filled
                                ),
                                metadata=metadata,
                            )
                        )
            elif purpose == "follower_recovery_topup":
                self.position_plan_store.update_leg_sync_status(position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.TOPUP_FAILED)
            elif master_entry_ok and exchange is not master_exchange:
                self.position_plan_store.update_leg_sync_status(position_id=position_id, exchange=exchange, sync_status=LegSyncStatus.FOLLOWER_ENTRY_FAILED)
                add_event = getattr(self.repository, "add_event", None)
                if callable(add_event):
                    add_event(
                        OrderJournalEvent(
                            intent_id=intent.intent_id,
                            status=OrderIntentStatus.PARTIALLY_SUBMITTED,
                            message="critical_follower_entry_failed",
                            exchange=exchange,
                            metadata={
                                "severity": "CRITICAL",
                                "position_id": position_id,
                                "master_exchange": master_exchange.value,
                                "follower_exchange": exchange.value,
                                "error": result.error or "follower entry failed",
                                "policy": "master_kept_follower_manual_required",
                                "auto_close_master": False,
                                "auto_reduce_master": False,
                            },
                        )
                    )

    def _record_stop_plan(self, intent: OrderIntent, results: Sequence[ExchangeOrderResult]) -> None:
        signal = intent.signal
        if signal.trigger_price is None:
            return
        if not signal.metadata or not signal.metadata.get("position_id"):
            return
        position_id = str(signal.metadata["position_id"])
        plan = self.position_plan_store.get_position(position_id)
        if plan is not None:
            self.position_plan_store.upsert_position(
                replace(
                    plan,
                    metadata={
                        **dict(plan.metadata),
                        "strategy_theoretical_stop_price": str(
                            signal.trigger_price
                        ),
                    },
                )
            )
        master_exchange = (
            None if plan is None else plan.master_exchange
        )
        for result in results:
            if result.ok:
                effective_stop_price = (
                    _optional_decimal(
                        result.raw.get("confirmed_stop_price")
                    )
                    or _optional_decimal(
                        result.raw.get("actual_exchange_stop_price")
                    )
                    or signal.trigger_price
                )
                self.position_plan_store.update_stop(
                    position_id=position_id,
                    exchange=result.exchange,
                    stop_price=effective_stop_price,
                    stop_order_id=result.order_id,
                    stop_client_order_id=result.client_order_id,
                    update_canonical=(
                        master_exchange is None
                        or result.exchange is master_exchange
                    ),
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
            master_filled = master_result is not None and _result_is_filled(master_result)
            if not master_filled:
                # Master close was attempted but NOT filled.
                # We must NOT mark master CLOSED, must NOT mark follower
                # FOLLOWER_CLOSE_FAILED, and must NOT enter
                # MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED.
                # The position plan stays ACTIVE so the runtime can retry
                # or alert.
                logger.warning(
                    "Master close not filled — position plan unchanged | position_id=%s exchange=%s status=%s filled_qty=%s ok=%s error=%s",
                    position_id,
                    master_exchange.value,
                    master_result.status.value if master_result is not None and master_result.status else "missing",
                    str(master_result.filled_quantity) if master_result is not None and master_result.filled_quantity is not None else "missing",
                    master_result.ok if master_result is not None else "missing",
                    master_result.error if master_result is not None else "missing result",
                )
                add_event = getattr(self.repository, "add_event", None)
                if callable(add_event):
                    add_event(
                        OrderJournalEvent(
                            intent_id=intent.intent_id,
                            status=OrderIntentStatus.PARTIALLY_SUBMITTED,
                            message="master_close_not_filled",
                            exchange=master_exchange,
                            metadata={
                                "position_id": position_id,
                                "master_exchange": master_exchange.value,
                                "status": master_result.status.value if master_result is not None and master_result.status else "missing",
                                "filled_quantity": str(master_result.filled_quantity) if master_result is not None and master_result.filled_quantity is not None else "0",
                                "ok": master_result.ok if master_result is not None else False,
                                "error": master_result.error if master_result is not None else "missing result",
                            },
                        )
                    )
                if _requires_manual_on_unconfirmed_master_close(
                    signal.metadata
                ):
                    metadata = {
                        **dict(existing.metadata),
                        "pending_close_unconfirmed": True,
                        "pending_close_intent_id": intent.intent_id,
                    }
                    self.position_plan_store.upsert_position(
                        replace(
                            existing,
                            status=PositionPlanStatus.MANUAL_REQUIRED,
                            metadata=_json_safe_value(metadata),
                        )
                    )
                return

            # Master close FILLED — mark CLOSED before processing followers.
            self.position_plan_store.update_leg_sync_status(
                position_id=position_id, exchange=master_exchange, sync_status=LegSyncStatus.CLOSED,
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
            SignalAction.CANCEL_STOP_ORDER,
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
            client_order_id = self._execution_client_order_id(
                client=client,
                intent=intent,
                item=item,
                sequence=sequence,
            )
            last_error: Exception | None = None
            for attempt in range(max_attempts):
                try:
                    order = await self._execute_item(client, item, intent=intent, client_order_id=client_order_id)
                    synced = await self.order_status_synchronizer.sync_after_submit(client=client, item=item, order=order)
                    result = _order_to_result(synced, client=client, quantity_converter=self.quantity_converter, attempts=attempt + 1)
                    if _requires_real_fill(intent.signal.action, item) and not _result_has_real_fill(result):
                        result = ExchangeOrderResult(
                            exchange=result.exchange,
                            ok=False,
                            order_id=result.order_id,
                            client_order_id=result.client_order_id,
                            status=result.status,
                            side=result.side,
                            quantity=result.quantity,
                            filled_quantity=result.filled_quantity,
                            avg_fill_price=result.avg_fill_price,
                            fee=result.fee,
                            fee_asset=result.fee_asset,
                            error="missing_real_fill_price_or_quantity",
                            raw={**dict(result.raw), "real_fill_required": True},
                        )
                    results.append(result)
                    break
                except ExitSafetyError as exc:
                    last_error = exc
                    self._record_exit_safety_event(intent=intent, exchange=client.exchange, error=exc)
                    logger.critical(
                        "Exit safety rejected order | intent_id=%s exchange=%s action=%s reason=%s metadata=%s",
                        intent.intent_id,
                        client.exchange.value,
                        item.signal.action.value,
                        exc.reason,
                        exc.metadata,
                    )
                    results.append(
                        ExchangeOrderResult(
                            exchange=client.exchange,
                            ok=False,
                            client_order_id=client_order_id,
                            error=exc.reason,
                            raw={"attempts": attempt + 1, "exit_safety": exc.metadata},
                        )
                    )
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

    def _execution_client_order_id(
        self,
        *,
        client: ExecutionClient,
        intent: OrderIntent,
        item: PlannedExecution,
        sequence: int,
    ) -> str | None:
        if item.action is PlannedExecutionAction.CANCEL_STOP_ORDER:
            request = item.cancel_stop_request
            return None if request is None else request.client_order_id
        return self.client_order_id_factory.create(
            strategy_id=intent.strategy_id,
            signal=item.signal,
            exchange=client.exchange,
            sequence=sequence,
        )

    async def _execute_item(self, client: ExecutionClient, item: PlannedExecution, *, intent: OrderIntent, client_order_id: str | None) -> Order:
        if item.action is PlannedExecutionAction.PLACE_ORDER:
            if item.order_request is None:
                raise ValueError("order_request is required")
            if client_order_id is None:
                raise ValueError("client_order_id is required")
            request = await self._normalize_order_for_client(
                client,
                item.signal.action,
                _with_exchange_quantity(item.order_request, intent=intent, exchange=client.exchange),
            )
            request = self._convert_order_for_client(client, request)
            return await client.place_order(_with_order_client_id(request, client_order_id))
        if item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
            if item.stop_market_request is None:
                raise ValueError("stop_market_request is required")
            if client_order_id is None:
                raise ValueError("client_order_id is required")
            request = await self._normalize_stop_for_client(
                client,
                item.signal.action,
                _with_exchange_quantity(item.stop_market_request, intent=intent, exchange=client.exchange),
            )
            request = self._convert_stop_for_client(client, request)
            return await client.place_stop_market_order(_with_stop_client_id(request, client_order_id))
        if item.action is PlannedExecutionAction.CANCEL_ALL_ORDERS:
            if client_order_id is None:
                raise ValueError("client_order_id is required")
            orders = await client.cancel_all_orders()
            return orders[0] if orders else _synthetic_order(client, client_order_id)
        if item.action is PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS:
            if client_order_id is None:
                raise ValueError("client_order_id is required")
            orders = await client.cancel_all_stop_orders()
            return orders[0] if orders else _synthetic_order(client, client_order_id)
        if item.action is PlannedExecutionAction.CANCEL_STOP_ORDER:
            if item.cancel_stop_request is None:
                raise ValueError("cancel_stop_request is required")
            order = await client.cancel_stop_order(item.cancel_stop_request)
            return _with_scoped_cancel_audit(order, item.cancel_stop_request)
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

    async def _normalize_order_for_client(self, client: ExecutionClient, action: SignalAction, request: OrderRequest) -> OrderRequest:
        position_mode = await self._position_mode_for_client(client)
        if is_exit_action(action) and self._client_supports_exit_safety(client):
            profile = _client_market_profile(client)
            assert profile is not None
            positions = await _client_positions(client)
            normalized, report = self.exit_safety_guard.normalize_order(
                exchange=client.exchange,
                action=action,
                request=request,
                position_mode=position_mode,
                positions=positions,
                market_profile=profile,
            )
            if report is not None:
                logger.info("Exit safety approved order | %s", report.as_log_fields())
            exchange_normalized = normalize_exit_request_for_exchange(
                exchange=client.exchange,
                action=action,
                request=normalized,
                position_mode=position_mode,
                safety_report=report,
            )
            if exchange_normalized.metadata:
                _log_exchange_exit_normalization(exchange_normalized.metadata)
            return exchange_normalized.request  # type: ignore[return-value]
        return _with_position_side_for_mode(request, action=action, exchange=client.exchange, position_mode=position_mode)

    async def _normalize_stop_for_client(self, client: ExecutionClient, action: SignalAction, request: StopMarketOrderRequest) -> StopMarketOrderRequest:
        position_mode = await self._position_mode_for_client(client)
        if is_exit_action(action) and self._client_supports_exit_safety(client):
            profile = _client_market_profile(client)
            assert profile is not None
            positions = await _client_positions(client)
            normalized, report = self.exit_safety_guard.normalize_stop_market(
                exchange=client.exchange,
                action=action,
                request=request,
                position_mode=position_mode,
                positions=positions,
                market_profile=profile,
            )
            if report is not None:
                logger.info("Exit safety approved stop order | %s", report.as_log_fields())
            exchange_normalized = normalize_exit_request_for_exchange(
                exchange=client.exchange,
                action=action,
                request=normalized,
                position_mode=position_mode,
                safety_report=report,
            )
            if exchange_normalized.metadata:
                _log_exchange_exit_normalization(exchange_normalized.metadata)
            return exchange_normalized.request  # type: ignore[return-value]
        return _with_position_side_for_mode(request, action=action, exchange=client.exchange, position_mode=position_mode)

    async def _position_mode_for_client(self, client: ExecutionClient) -> PositionMode:
        cached = self._position_mode_cache.get(client.exchange)
        if cached is not None:
            return cached
        fetch_position_mode = getattr(client, "fetch_position_mode", None)
        if callable(fetch_position_mode):
            mode = await fetch_position_mode()
            if not isinstance(mode, PositionMode):
                mode = PositionMode(str(mode).strip().lower())
            self._position_mode_cache[client.exchange] = mode
            return mode
        positions = await _client_positions(client)
        mode = PositionMode.HEDGE if any(position.side in {PositionSide.LONG, PositionSide.SHORT} for position in positions) else PositionMode.ONE_WAY
        self._position_mode_cache[client.exchange] = mode
        return mode

    def _client_supports_exit_safety(self, client: ExecutionClient) -> bool:
        return _client_market_profile(client) is not None and callable(getattr(client, "fetch_positions", None))

    def _record_exit_safety_event(self, *, intent: OrderIntent, exchange: ExchangeName, error: ExitSafetyError) -> None:
        add_event = getattr(self.repository, "add_event", None)
        if callable(add_event):
            add_event(
                OrderJournalEvent(
                    intent_id=intent.intent_id,
                    status=OrderIntentStatus.FAILED,
                    message="critical_exit_safety_rejected",
                    exchange=exchange,
                    metadata={"severity": "CRITICAL", "reason": error.reason, **dict(error.metadata)},
                )
            )


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


def _log_exchange_exit_normalization(metadata) -> None:
    if metadata.get("exchange") == "binance" and metadata.get("position_mode") == "hedge":
        logger.info(
            "Binance hedge exit request normalized | "
            "exchange=%s position_mode=%s action=%s position_side=%s side=%s "
            "base_quantity=%s current_position_base_quantity=%s "
            "reduce_only_requested=%s reduce_only_sent=%s "
            "exit_safety_equivalent_reduce_only=%s "
            "reduce_only_omitted_reason=%s safety_basis=%s",
            metadata.get("exchange"),
            metadata.get("position_mode"),
            metadata.get("action"),
            metadata.get("position_side"),
            metadata.get("side"),
            metadata.get("base_quantity"),
            metadata.get("current_position_base_quantity"),
            metadata.get("reduce_only_requested"),
            metadata.get("reduce_only_sent"),
            metadata.get("exit_safety_equivalent_reduce_only"),
            metadata.get("reduce_only_omitted_reason"),
            metadata.get("safety_basis"),
        )
        return
    logger.info("Exchange exit request normalized | %s", metadata)


async def _client_positions(client: ExecutionClient):
    fetch_positions = getattr(client, "fetch_positions", None)
    if not callable(fetch_positions):
        return ()
    return tuple(await fetch_positions())


def _with_position_side_for_mode(request, *, action: SignalAction, exchange: ExchangeName, position_mode: PositionMode):
    target_side = target_position_side_for_action(action)
    if target_side is None:
        return request
    if position_mode is PositionMode.HEDGE:
        return replace(request, position_side=target_side)
    if exchange.value in {"okx", "binance"} and getattr(request, "position_side", None) is not None:
        return replace(request, position_side=None)
    return request


def _order_to_result(order: Order, *, client: ExecutionClient | None = None, quantity_converter: NativeQuantityConverter | None = None, attempts: int = 1) -> ExchangeOrderResult:
    fee, fee_asset = extract_fee(order)
    raw = {**dict(order.raw), "status_sync_attempts": attempts}
    quantity = order.quantity
    filled_quantity = order.filled_quantity
    profile = _client_market_profile(client) if client is not None else None
    if profile is not None and quantity_converter is not None:
        if quantity is not None:
            raw["native_quantity"] = str(quantity)
            quantity = quantity_converter.native_to_base_quantity(
                exchange=order.exchange,
                symbol=order.symbol,
                native_quantity=abs(quantity),
                market_profile=profile,
            )
        if filled_quantity is not None:
            raw["native_filled_quantity"] = str(filled_quantity)
            filled_quantity = quantity_converter.native_to_base_quantity(
                exchange=order.exchange,
                symbol=order.symbol,
                native_quantity=abs(filled_quantity),
                market_profile=profile,
            )
        raw["quantity_semantics"] = "base_asset"
    return ExchangeOrderResult(
        exchange=order.exchange,
        ok=True,
        order_id=order.order_id,
        client_order_id=order.client_order_id,
        status=order.status,
        side=order.side,
        quantity=quantity,
        filled_quantity=filled_quantity,
        avg_fill_price=extract_avg_fill_price(order),
        fee=fee,
        fee_asset=fee_asset,
        raw=raw,
    )


def _synthetic_order(client: ExecutionClient, client_order_id: str) -> Order:
    from src.platform.exchanges.models import OrderStatus

    return Order(exchange=client.exchange, symbol=client.symbol, raw_symbol=client.symbol, order_id=None, client_order_id=client_order_id, status=OrderStatus.CANCELED)


def _with_scoped_cancel_audit(order: Order, request: CancelStopOrderRequest) -> Order:
    return replace(
        order,
        raw={
            **dict(order.raw),
            "execution_action": PlannedExecutionAction.CANCEL_STOP_ORDER.value,
            "cancel_stop_metadata": dict(request.metadata or {}),
        },
    )


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


def _requires_manual_on_unconfirmed_master_close(
    signal_metadata: Mapping[str, Any] | None,
) -> bool:
    if not signal_metadata:
        return False
    return (
        str(
            signal_metadata.get(
                "unconfirmed_master_close_policy",
                "",
            )
        )
        .strip()
        .lower()
        == "manual_required"
    )


def _position_plan_metadata(
    signal_metadata: Mapping[str, Any] | None,
    *,
    intent_id: str,
) -> dict[str, Any]:
    safe_signal = _json_safe_value(dict(signal_metadata or {}))
    metadata: dict[str, Any] = {
        "intent_id": intent_id,
        "signal_metadata": safe_signal,
    }
    for key in (
        "sleeve_id",
        "position_id",
        "engine",
        "entry_execution_time_ms",
        "entry_tradebar_open_time_ms",
        "signal_time_ms",
        "fixed_time_exit_holding_minutes",
        "exit_variant",
        "quantity_scope",
        "protective_stop_required",
        "unconfirmed_master_close_policy",
    ):
        if key in safe_signal:
            metadata[key] = safe_signal[key]
    return metadata


def _json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Enum):
        return _json_safe_value(value.value)
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe_value(item) for item in value]
    return str(value)


def _result_is_filled(result: ExchangeOrderResult) -> bool:
    """A close/entry result is only considered truly filled when the exchange
    confirms FILLED status AND a positive filled quantity."""
    return (
        result.ok
        and result.status is OrderStatus.FILLED
        and result.filled_quantity is not None
        and result.filled_quantity > Decimal("0")
    )


def _requires_real_fill(action: SignalAction, item: PlannedExecution) -> bool:
    return action in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT} and item.action is PlannedExecutionAction.PLACE_ORDER


def _result_has_real_fill(result: ExchangeOrderResult) -> bool:
    return (
        result.ok
        and result.status is OrderStatus.FILLED
        and result.filled_quantity is not None
        and result.filled_quantity > Decimal("0")
        and result.avg_fill_price is not None
        and result.avg_fill_price > Decimal("0")
    )


def _result_filled_base(result: ExchangeOrderResult, *, fallback: Decimal) -> Decimal:
    if result.filled_quantity is not None and result.filled_quantity > 0:
        return result.filled_quantity
    if result.quantity is not None and result.quantity > 0:
        return result.quantity
    return fallback
