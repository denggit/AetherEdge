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


class ExecutionResultRecorder:
    def _record_skipped_recovery_topup(
        self,
        *,
        intent: OrderIntent,
        resolutions: Mapping[ExchangeName, Any],
    ) -> list[ExchangeOrderResult]:
        outcome = "skipped_non_executable_quantity"
        resolution_metadata = {
            exchange.value: resolution.metadata()
            for exchange, resolution in resolutions.items()
        }
        recovered_intent = replace(
            intent,
            status=OrderIntentStatus.RECOVERED,
            metadata={
                **dict(intent.metadata),
                "execution_outcome": outcome,
                "quantity_resolutions": resolution_metadata,
            },
        )
        self.repository.update_claimed_intent(recovered_intent)
        self.repository.update_status(
            intent_id=intent.intent_id, status=OrderIntentStatus.RECOVERED
        )
        results = [
            ExchangeOrderResult(
                exchange=exchange,
                ok=True,
                status=OrderStatus.CANCELED,
                quantity=Decimal("0"),
                filled_quantity=Decimal("0"),
                raw={
                    "execution_outcome": outcome,
                    **resolution.metadata(),
                },
            )
            for exchange, resolution in resolutions.items()
        ]
        save_result = getattr(self.repository, "save_result", None)
        if callable(save_result):
            for result in results:
                save_result(intent_id=intent.intent_id, result=result)
    
        if self.position_plan_store is not None and intent.signal.metadata:
            position_id = str(intent.signal.metadata.get("position_id") or "")
            apply_resolution = getattr(
                self.position_plan_store, "apply_recovery_leg_resolution", None
            )
            if position_id and callable(apply_resolution):
                for exchange, resolution in resolutions.items():
                    apply_resolution(
                        position_id=position_id,
                        exchange=exchange,
                        sync_status=LegSyncStatus.SYNCED,
                        metadata={
                            **dict(intent.signal.metadata),
                            "normalized_delta": str(
                                resolution.normalized_base_quantity
                            ),
                            "reason": "non_executable_rounding_dust",
                            "execution_outcome": outcome,
                        },
                    )
            self._advance_topup_generation(intent)
        logger.info(
            "Follower recovery top-up skipped before planning | "
            "intent_id=%s targets=%s outcome=%s resolutions=%s",
            intent.intent_id,
            [exchange.value for exchange in intent.target_exchanges],
            outcome,
            resolution_metadata,
        )
        return results

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


__all__ = ["ExecutionResultRecorder"]

