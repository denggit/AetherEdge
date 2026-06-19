from __future__ import annotations

from typing import Iterable

from src.planner.models import ExecutionPlan, PlannedExecution, PlannedExecutionAction
from src.platform.exchanges.models import OrderRequest, OrderSide, OrderType, StopMarketOrderRequest
from src.signals.models import SignalAction, SignalOrderType, TradeSignal


class SignalPlanningError(ValueError):
    pass


class ExecutionPlanner:
    """Convert strategy intent into platform execution requests.

    This service does not place orders. It only maps normalized signals into
    request objects consumed by ``src.platform.execution``.
    """

    def plan(self, signal: TradeSignal) -> ExecutionPlan:
        if signal.action is SignalAction.CANCEL_ALL_ORDERS:
            return ExecutionPlan.single(PlannedExecution(action=PlannedExecutionAction.CANCEL_ALL_ORDERS, signal=signal))
        if signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS:
            return ExecutionPlan.single(PlannedExecution(action=PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS, signal=signal))
        if signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            request = StopMarketOrderRequest(
                symbol=signal.symbol,
                side=_stop_side(signal.action),
                quantity=signal.quantity,
                trigger_price=signal.trigger_price,  # type: ignore[arg-type]
                client_order_id=signal.client_order_id,
                reduce_only=True,
            )
            return ExecutionPlan.single(
                PlannedExecution(
                    action=PlannedExecutionAction.PLACE_STOP_MARKET_ORDER,
                    signal=signal,
                    stop_market_request=request,
                )
            )

        request = OrderRequest(
            symbol=signal.symbol,
            side=_order_side(signal.action),
            order_type=_order_type(signal.order_type),
            quantity=signal.quantity,  # type: ignore[arg-type]
            price=signal.price,
            client_order_id=signal.client_order_id,
            reduce_only=_reduce_only(signal.action),
        )
        return ExecutionPlan.single(
            PlannedExecution(
                action=PlannedExecutionAction.PLACE_ORDER,
                signal=signal,
                order_request=request,
            )
        )

    def plan_many(self, signals: Iterable[TradeSignal]) -> ExecutionPlan:
        items: list[PlannedExecution] = []
        for signal in signals:
            items.extend(self.plan(signal).items)
        return ExecutionPlan(tuple(items))


def _order_side(action: SignalAction) -> OrderSide:
    mapping = {
        SignalAction.OPEN_LONG: OrderSide.BUY,
        SignalAction.OPEN_SHORT: OrderSide.SELL,
        SignalAction.CLOSE_LONG: OrderSide.SELL,
        SignalAction.CLOSE_SHORT: OrderSide.BUY,
        SignalAction.REDUCE_LONG: OrderSide.SELL,
        SignalAction.REDUCE_SHORT: OrderSide.BUY,
    }
    try:
        return mapping[action]
    except KeyError as exc:
        raise SignalPlanningError(f"unsupported order signal action: {action.value}") from exc


def _stop_side(action: SignalAction) -> OrderSide:
    if action is SignalAction.PLACE_STOP_LOSS_LONG:
        return OrderSide.SELL
    if action is SignalAction.PLACE_STOP_LOSS_SHORT:
        return OrderSide.BUY
    raise SignalPlanningError(f"unsupported stop signal action: {action.value}")


def _reduce_only(action: SignalAction) -> bool:
    return action in {
        SignalAction.CLOSE_LONG,
        SignalAction.CLOSE_SHORT,
        SignalAction.REDUCE_LONG,
        SignalAction.REDUCE_SHORT,
    }


def _order_type(order_type: SignalOrderType) -> OrderType:
    if order_type is SignalOrderType.MARKET:
        return OrderType.MARKET
    if order_type is SignalOrderType.LIMIT:
        return OrderType.LIMIT
    raise SignalPlanningError(f"unsupported order type: {order_type.value}")
