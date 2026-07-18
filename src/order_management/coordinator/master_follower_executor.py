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
from src.order_management.coordinator.multi_exchange_executor import (
    MultiExchangeExecutor,
)


_MASTER_GATED_PURPOSES = {"normal_entry", "normal_close"}
_BYPASS_MASTER_PURPOSES = {
    "stop_sync",
    "follower_recovery_topup",
    "follower_close_after_master_close",
}

logger = get_logger(__name__)


class MasterFollowerExecutor:
    def __init__(
        self,
        *,
        policy: MasterFollowerExecutionPolicy,
        executor: MultiExchangeExecutor,
    ) -> None:
        self.master_follower_policy = policy
        self._executor = executor

    async def _execute_master_follower(self, clients: Sequence[ExecutionClient], intent: OrderIntent, items: Sequence[PlannedExecution]) -> list[ExchangeOrderResult]:
        assert self.master_follower_policy is not None
        if self._bypasses_master_gating(intent):
            purpose = str(intent.signal.metadata.get("execution_purpose", "") if intent.signal.metadata else "").strip().lower()
            if purpose == "follower_close_after_master_close":
                close_retry = self.master_follower_policy.follower_close_retry
                results_nested = await asyncio.gather(
                    *(
                        self._executor._execute_for_client(
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
                results_nested = await asyncio.gather(*(self._executor._execute_for_client(client, intent, items) for client in clients))
            return [item for group in results_nested for item in group]
        client_by_exchange = {client.exchange: client for client in clients}
        master = client_by_exchange.get(self.master_follower_policy.master_exchange)
        followers = [client_by_exchange[exchange] for exchange in self.master_follower_policy.followers_for(intent.target_exchanges) if exchange in client_by_exchange]
        if master is None:
            logger.error("Master execution client unavailable | intent_id=%s master=%s", intent.intent_id, self.master_follower_policy.master_exchange.value)
            return [ExchangeOrderResult(exchange=self.master_follower_policy.master_exchange, ok=False, error="master execution client not available")]
    
        master_results = await self._executor._execute_for_client(
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
                self._executor._execute_for_client(
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


__all__ = ["MasterFollowerExecutor"]
