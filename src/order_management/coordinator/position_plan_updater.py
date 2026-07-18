from __future__ import annotations

import asyncio
from dataclasses import replace
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence

from src.order_management.idempotency.client_order_id import DeterministicClientOrderIdFactory
from src.order_management.idempotency.duplicate_guard import RepositoryDuplicateOrderGuard
from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus, OrderJournalEvent
from src.order_management.position_plan import LegPlan, LegRole, LegSyncStatus, PositionPlan, PositionPlanStatus
from src.order_management.ports import ClientOrderIdFactory, DuplicateOrderGuard, OrderIntentRepository
from src.order_management.quantity import (
    NativeQuantityConverter,
    resolve_executable_base_quantity,
)
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


from src.order_management.coordinator.support import *  # noqa: F403


class PositionPlanUpdater:
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

    def _record_open_or_topup_plan(
        self,
        intent: OrderIntent,
        results: Sequence[ExchangeOrderResult],
        *,
        purpose: str,
    ) -> None:
        prepared = self._prepare_open_or_topup_plan(
            intent,
            purpose=purpose,
        )
        if prepared is None:
            return
        position_id, master_exchange = prepared
        by_exchange = {result.exchange: result for result in results}
        master_result = by_exchange.get(master_exchange)
        master_entry_ok = bool(master_result and master_result.ok)
        for exchange in intent.target_exchanges:
            result = by_exchange.get(exchange)
            if result is None:
                continue
            self._record_open_leg_result(
                intent=intent,
                result=result,
                position_id=position_id,
                master_exchange=master_exchange,
                purpose=purpose,
                master_entry_ok=master_entry_ok,
            )
        if purpose == "follower_recovery_topup":
            self._advance_topup_generation(intent)

    def _prepare_open_or_topup_plan(
        self,
        intent: OrderIntent,
        *,
        purpose: str,
    ) -> tuple[str, ExchangeName] | None:
        signal = intent.signal
        if signal.quantity is None or signal.quantity <= 0:
            return None
        position_id = (
            str(signal.metadata.get("position_id") or f"pos-{intent.intent_id}")
            if signal.metadata
            else f"pos-{intent.intent_id}"
        )
        existing = self.position_plan_store.get_position(position_id)
        master_exchange = (
            self.master_follower_policy.master_exchange
            if self.master_follower_policy is not None
            else intent.target_exchanges[0]
        )
        entry_engine = (
            str(
                signal.metadata.get("engine")
                or (existing.entry_engine if existing else "unknown")
            )
            if signal.metadata
            else (existing.entry_engine if existing else "unknown")
        )
        if existing is None:
            self._create_open_position_plan(
                intent=intent,
                position_id=position_id,
                master_exchange=master_exchange,
                entry_engine=entry_engine,
            )
        elif purpose != "follower_recovery_topup":
            self._increase_open_position_targets(
                intent=intent,
                position_id=position_id,
                master_exchange=master_exchange,
                existing=existing,
            )
        return position_id, master_exchange

    def _create_open_position_plan(
        self,
        *,
        intent: OrderIntent,
        position_id: str,
        master_exchange: ExchangeName,
        entry_engine: str,
    ) -> None:
        signal = intent.signal
        side = (
            "long"
            if signal.action is SignalAction.OPEN_LONG
            else "short"
        )
        stop_price = _optional_decimal(
            signal.metadata.get("estimated_initial_stop")
            if signal.metadata
            else None
        )
        master_target_qty = (
            _signal_exchange_quantity(
                signal,
                master_exchange,
                fallback=Decimal("0"),
            )
            if master_exchange in intent.target_exchanges
            else Decimal("0")
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
                metadata=_position_plan_metadata(
                    signal.metadata,
                    intent_id=intent.intent_id,
                ),
            )
        )
        for exchange in intent.target_exchanges:
            self.position_plan_store.upsert_leg(
                LegPlan(
                    position_id=position_id,
                    exchange=exchange,
                    role=(
                        LegRole.MASTER
                        if exchange is master_exchange
                        else LegRole.FOLLOWER
                    ),
                    target_qty_base=_signal_exchange_quantity(
                        signal,
                        exchange,
                        fallback=signal.quantity,
                    ),
                    sync_status=LegSyncStatus.PLANNED,
                )
            )

    def _increase_open_position_targets(
        self,
        *,
        intent: OrderIntent,
        position_id: str,
        master_exchange: ExchangeName,
        existing: PositionPlan,
    ) -> None:
        signal = intent.signal
        for exchange in intent.target_exchanges:
            delta_qty = _signal_exchange_quantity(
                signal,
                exchange,
                fallback=signal.quantity,
            )
            self.position_plan_store.add_to_leg_target(
                position_id=position_id,
                exchange=exchange,
                delta_target_qty_base=delta_qty,
            )
        if master_exchange not in intent.target_exchanges:
            return
        master_delta = _signal_exchange_quantity(
            signal,
            master_exchange,
            fallback=signal.quantity,
        )
        self.position_plan_store.upsert_position(
            replace(
                existing,
                master_target_qty_base=(
                    existing.master_target_qty_base + master_delta
                ),
            )
        )

    def _record_open_leg_result(
        self,
        *,
        intent: OrderIntent,
        result: ExchangeOrderResult,
        position_id: str,
        master_exchange: ExchangeName,
        purpose: str,
        master_entry_ok: bool,
    ) -> None:
        legs = {
            item.exchange: item
            for item in self.position_plan_store.get_legs(position_id)
        }
        leg = legs.get(result.exchange)
        if leg is None:
            return
        if result.ok:
            self._record_successful_open_leg(
                intent=intent,
                result=result,
                leg=leg,
                position_id=position_id,
                master_exchange=master_exchange,
                purpose=purpose,
            )
        elif purpose == "follower_recovery_topup":
            self.position_plan_store.update_leg_sync_status(
                position_id=position_id,
                exchange=result.exchange,
                sync_status=LegSyncStatus.TOPUP_FAILED,
            )
        elif master_entry_ok and result.exchange is not master_exchange:
            self._record_follower_entry_failure(
                intent=intent,
                result=result,
                position_id=position_id,
                master_exchange=master_exchange,
            )

    def _record_successful_open_leg(
        self,
        *,
        intent: OrderIntent,
        result: ExchangeOrderResult,
        leg: LegPlan,
        position_id: str,
        master_exchange: ExchangeName,
        purpose: str,
    ) -> None:
        signal = intent.signal
        target_qty = _signal_exchange_quantity(
            signal,
            result.exchange,
            fallback=signal.quantity,
        )
        filled = (
            target_qty
            if result.filled_quantity is None
            else min(
                target_qty,
                _result_filled_base(result, fallback=target_qty),
            )
        )
        is_topup = purpose == "follower_recovery_topup"
        self.position_plan_store.upsert_leg(
            replace(
                leg,
                filled_qty_base=(
                    max(leg.filled_qty_base, filled)
                    if is_topup
                    else leg.filled_qty_base + filled
                ),
                entry_order_id=result.order_id or leg.entry_order_id,
                entry_client_order_id=(
                    result.client_order_id or leg.entry_client_order_id
                ),
                sync_status=(
                    LegSyncStatus.TOPUP_SUBMITTED
                    if is_topup
                    else LegSyncStatus.OPEN
                ),
            )
        )
        if result.exchange is master_exchange:
            self._record_master_entry_fill(position_id, result, filled)

    def _record_master_entry_fill(
        self,
        position_id: str,
        result: ExchangeOrderResult,
        filled: Decimal,
    ) -> None:
        plan = self.position_plan_store.get_position(position_id)
        if plan is None:
            return
        metadata = dict(plan.metadata)
        if result.avg_fill_price is not None and result.avg_fill_price > 0:
            metadata["average_entry_price"] = str(result.avg_fill_price)
        self.position_plan_store.upsert_position(
            replace(
                plan,
                master_filled_qty_base=(
                    plan.master_filled_qty_base + filled
                ),
                metadata=metadata,
            )
        )

    def _record_follower_entry_failure(
        self,
        *,
        intent: OrderIntent,
        result: ExchangeOrderResult,
        position_id: str,
        master_exchange: ExchangeName,
    ) -> None:
        self.position_plan_store.update_leg_sync_status(
            position_id=position_id,
            exchange=result.exchange,
            sync_status=LegSyncStatus.FOLLOWER_ENTRY_FAILED,
        )
        add_event = getattr(self.repository, "add_event", None)
        if not callable(add_event):
            return
        add_event(
            OrderJournalEvent(
                intent_id=intent.intent_id,
                status=OrderIntentStatus.PARTIALLY_SUBMITTED,
                message="critical_follower_entry_failed",
                exchange=result.exchange,
                metadata={
                    "severity": "CRITICAL",
                    "position_id": position_id,
                    "master_exchange": master_exchange.value,
                    "follower_exchange": result.exchange.value,
                    "error": result.error or "follower entry failed",
                    "policy": "master_kept_follower_manual_required",
                    "auto_close_master": False,
                    "auto_reduce_master": False,
                },
            )
        )

    def _advance_topup_generation(self, intent: OrderIntent) -> None:
        signal = intent.signal
        metadata = dict(signal.metadata or {})
        if (
            str(metadata.get("execution_purpose") or "").strip().lower()
            != "follower_recovery_topup"
        ):
            return
        generation = _durable_generation(metadata, "topup_generation")
        position_id = str(metadata.get("position_id") or "").strip()
        if generation is None or not position_id:
            return
        legs = {
            leg.exchange: leg
            for leg in self.position_plan_store.get_legs(position_id)
        }
        for exchange in intent.target_exchanges:
            leg = legs.get(exchange)
            if leg is None:
                continue
            leg_metadata = dict(leg.metadata or {})
            persisted = _durable_generation(
                leg_metadata, "topup_generation", default=0
            )
            next_generation = max(persisted or 0, generation + 1)
            if persisted == next_generation:
                continue
            leg_metadata["topup_generation"] = next_generation
            self.position_plan_store.upsert_leg(
                replace(leg, metadata=leg_metadata)
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
        self._advance_stop_generation(signal)

    def _advance_stop_generation(self, signal) -> None:
        metadata = dict(signal.metadata or {})
        generation = _durable_generation(metadata, "stop_generation")
        position_id = str(metadata.get("position_id") or "").strip()
        if generation is None or not position_id:
            return
        plan = self.position_plan_store.get_position(position_id)
        if plan is None:
            return
        plan_metadata = dict(plan.metadata or {})
        persisted = _durable_generation(
            plan_metadata, "stop_generation", default=0
        )
        next_generation = max(persisted or 0, generation + 1)
        if persisted == next_generation:
            return
        plan_metadata["stop_generation"] = next_generation
        self.position_plan_store.upsert_position(
            replace(plan, metadata=plan_metadata)
        )

    def _record_close_plan(
        self,
        intent: OrderIntent,
        results: Sequence[ExchangeOrderResult],
        *,
        purpose: str,
    ) -> None:
        signal = intent.signal
        if not signal.metadata or not signal.metadata.get("position_id"):
            return
        position_id = str(signal.metadata["position_id"])
        existing = self.position_plan_store.get_position(position_id)
        if existing is None:
            return
        by_exchange = {result.exchange: result for result in results}
        if purpose == "follower_close_after_master_close":
            self._record_follower_close_results(
                intent=intent,
                by_exchange=by_exchange,
                position_id=position_id,
            )
            self._advance_follower_close_generation(signal)
            return
        if purpose == "normal_close":
            self._record_normal_close_results(
                intent=intent,
                by_exchange=by_exchange,
                position_id=position_id,
                existing=existing,
            )
            return
        self._maybe_close_position_plan(position_id)

    def _record_follower_close_results(
        self,
        *,
        intent: OrderIntent,
        by_exchange: Mapping[ExchangeName, ExchangeOrderResult],
        position_id: str,
    ) -> None:
        for exchange in intent.target_exchanges:
            result = by_exchange.get(exchange)
            status = (
                LegSyncStatus.CLOSED
                if result is not None and _result_is_filled(result)
                else LegSyncStatus.FOLLOWER_CLOSE_FAILED
            )
            self.position_plan_store.update_leg_sync_status(
                position_id=position_id,
                exchange=exchange,
                sync_status=status,
            )
        self._maybe_close_position_plan(position_id)

    def _record_normal_close_results(
        self,
        *,
        intent: OrderIntent,
        by_exchange: Mapping[ExchangeName, ExchangeOrderResult],
        position_id: str,
        existing: PositionPlan,
    ) -> None:
        master_exchange = existing.master_exchange
        master_result = by_exchange.get(master_exchange)
        if master_result is None or not _result_is_filled(master_result):
            self._record_unfilled_master_close(
                intent=intent,
                master_result=master_result,
                position_id=position_id,
                existing=existing,
            )
            return
        self.position_plan_store.update_leg_sync_status(
            position_id=position_id,
            exchange=master_exchange,
            sync_status=LegSyncStatus.CLOSED,
        )
        for exchange in intent.target_exchanges:
            if exchange == master_exchange:
                continue
            result = by_exchange.get(exchange)
            status = (
                LegSyncStatus.CLOSED
                if result is not None and _result_is_filled(result)
                else LegSyncStatus.FOLLOWER_CLOSE_FAILED
            )
            self.position_plan_store.update_leg_sync_status(
                position_id=position_id,
                exchange=exchange,
                sync_status=status,
            )
        self._finalize_normal_close_position(
            position_id=position_id,
            existing=existing,
            master_exchange=master_exchange,
        )

    def _record_unfilled_master_close(
        self,
        *,
        intent: OrderIntent,
        master_result: ExchangeOrderResult | None,
        position_id: str,
        existing: PositionPlan,
    ) -> None:
        master_exchange = existing.master_exchange
        status = (
            master_result.status.value
            if master_result is not None and master_result.status
            else "missing"
        )
        filled_quantity = (
            str(master_result.filled_quantity)
            if (
                master_result is not None
                and master_result.filled_quantity is not None
            )
            else "missing"
        )
        logger.warning(
            "Master close not filled — position plan unchanged | position_id=%s exchange=%s status=%s filled_qty=%s ok=%s error=%s",
            position_id,
            master_exchange.value,
            status,
            filled_quantity,
            master_result.ok if master_result is not None else "missing",
            (
                master_result.error
                if master_result is not None
                else "missing result"
            ),
        )
        self._journal_unfilled_master_close(
            intent=intent,
            master_result=master_result,
            position_id=position_id,
            master_exchange=master_exchange,
            status=status,
        )
        if not _requires_manual_on_unconfirmed_master_close(
            intent.signal.metadata
        ):
            return
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

    def _journal_unfilled_master_close(
        self,
        *,
        intent: OrderIntent,
        master_result: ExchangeOrderResult | None,
        position_id: str,
        master_exchange: ExchangeName,
        status: str,
    ) -> None:
        add_event = getattr(self.repository, "add_event", None)
        if not callable(add_event):
            return
        add_event(
            OrderJournalEvent(
                intent_id=intent.intent_id,
                status=OrderIntentStatus.PARTIALLY_SUBMITTED,
                message="master_close_not_filled",
                exchange=master_exchange,
                metadata={
                    "position_id": position_id,
                    "master_exchange": master_exchange.value,
                    "status": status,
                    "filled_quantity": (
                        str(master_result.filled_quantity)
                        if (
                            master_result is not None
                            and master_result.filled_quantity is not None
                        )
                        else "0"
                    ),
                    "ok": (
                        master_result.ok
                        if master_result is not None
                        else False
                    ),
                    "error": (
                        master_result.error
                        if master_result is not None
                        else "missing result"
                    ),
                },
            )
        )

    def _finalize_normal_close_position(
        self,
        *,
        position_id: str,
        existing: PositionPlan,
        master_exchange: ExchangeName,
    ) -> None:
        has_unresolved = any(
            leg.sync_status != LegSyncStatus.CLOSED
            for leg in self.position_plan_store.get_legs(position_id)
            if (
                leg.exchange != master_exchange
                and leg.role != LegRole.MASTER
            )
        )
        if has_unresolved:
            self.position_plan_store.upsert_position(
                replace(
                    existing,
                    status=(
                        PositionPlanStatus
                        .MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED
                    ),
                )
            )
        else:
            self._maybe_close_position_plan(position_id)

    def _advance_follower_close_generation(self, signal) -> None:
        metadata = dict(signal.metadata or {})
        generation = _durable_generation(
            metadata, "follower_close_generation"
        )
        position_id = str(metadata.get("position_id") or "").strip()
        if generation is None or not position_id:
            return
        plan = self.position_plan_store.get_position(position_id)
        if plan is None:
            return
        plan_metadata = dict(plan.metadata or {})
        persisted = _durable_generation(
            plan_metadata, "follower_close_generation", default=0
        )
        next_generation = max(persisted or 0, generation + 1)
        if persisted == next_generation:
            return
        plan_metadata["follower_close_generation"] = next_generation
        self.position_plan_store.upsert_position(
            replace(plan, metadata=plan_metadata)
        )

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


__all__ = ["PositionPlanUpdater"]
