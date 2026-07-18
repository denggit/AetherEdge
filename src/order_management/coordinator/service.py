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


from src.order_management.coordinator.intent_planner import OrderIntentPlanner
from src.order_management.coordinator.master_follower_executor import MasterFollowerExecutor
from src.order_management.coordinator.multi_exchange_executor import MultiExchangeExecutor
from src.order_management.coordinator.position_plan_updater import PositionPlanUpdater
from src.order_management.coordinator.result_recorder import ExecutionResultRecorder
from src.order_management.coordinator.safety_validator import OrderSafetyValidator
from src.order_management.coordinator.support import _final_status


class MultiExchangeOrderCoordinator(
    OrderIntentPlanner,
    MasterFollowerExecutor,
    MultiExchangeExecutor,
    OrderSafetyValidator,
    ExecutionResultRecorder,
    PositionPlanUpdater,
):
    """Orchestrate intent planning, exchange execution and result recording."""

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
        self.duplicate_guard = duplicate_guard or RepositoryDuplicateOrderGuard(
            repository
        )
        self.quantity_converter = quantity_converter or NativeQuantityConverter()
        self.exit_safety_guard = exit_safety_guard or ExitSafetyGuard(quantity_converter=self.quantity_converter)
        self.order_status_synchronizer = order_status_synchronizer or OrderStatusSynchronizer()
        self.master_follower_policy = master_follower_policy
        self.master_follower_evaluator = MasterFollowerPolicyEvaluator(master_follower_policy) if master_follower_policy is not None else None
        self.position_plan_store = position_plan_store
        self.post_result_validator = post_result_validator
        self._position_mode_cache: dict[ExchangeName, PositionMode] = {}

    async def execute(self, intent: OrderIntent) -> list[ExchangeOrderResult]:
        self.duplicate_guard.claim_or_raise(intent)
        intent, skipped = await self._normalize_recovery_topup_intent(intent)
        if skipped is not None:
            return skipped
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
        self.repository.update_claimed_intent(intent)
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


__all__ = ["MultiExchangeOrderCoordinator"]

