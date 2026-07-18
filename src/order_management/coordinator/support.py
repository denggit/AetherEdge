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

def _durable_generation(
    metadata: Mapping[str, Any],
    key: str,
    *,
    default: int | None = None,
) -> int | None:
    value = metadata.get(key, default)
    if value is None or isinstance(value, bool):
        return None
    try:
        generation = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return generation if generation >= 0 else None

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


__all__ = [
    "_signal_exchange_quantities",
    "_signal_exchange_quantity",
    "_with_exchange_quantity",
    "_with_order_client_id",
    "_with_stop_client_id",
    "_client_market_profile",
    "_log_exchange_exit_normalization",
    "_client_positions",
    "_with_position_side_for_mode",
    "_order_to_result",
    "_synthetic_order",
    "_with_scoped_cancel_audit",
    "_final_status",
    "_optional_decimal",
    "_durable_generation",
    "_requires_manual_on_unconfirmed_master_close",
    "_position_plan_metadata",
    "_json_safe_value",
    "_result_is_filled",
    "_requires_real_fill",
    "_result_has_real_fill",
    "_result_filled_base",
]

