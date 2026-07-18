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


class MultiExchangeExecutor:
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
            intent_id=intent.intent_id,
            action=item.signal.action,
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


__all__ = ["MultiExchangeExecutor"]

