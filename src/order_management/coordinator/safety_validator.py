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


class OrderSafetyValidator:
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


__all__ = ["OrderSafetyValidator"]

