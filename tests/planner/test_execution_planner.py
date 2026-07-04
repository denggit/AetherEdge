from decimal import Decimal

from src.planner import ExecutionPlanner, PlannedExecutionAction
from src.platform import OrderSide, OrderType
from src.signals import SignalAction, SignalOrderType, TradeSignal


def test_planner_maps_open_long_market_to_buy_order():
    plan = ExecutionPlanner().plan(
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.OPEN_LONG,
            quantity=Decimal("0.1"),
        )
    )

    item = plan.items[0]
    assert item.action is PlannedExecutionAction.PLACE_ORDER
    assert item.order_request is not None
    assert item.order_request.side is OrderSide.BUY
    assert item.order_request.order_type is OrderType.MARKET
    assert item.order_request.reduce_only is False


def test_planner_maps_close_short_to_reduce_only_buy():
    plan = ExecutionPlanner().plan(
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.CLOSE_SHORT,
            quantity=Decimal("0.1"),
            order_type=SignalOrderType.LIMIT,
            price=Decimal("2800"),
        )
    )

    request = plan.items[0].order_request
    assert request is not None
    assert request.side is OrderSide.BUY
    assert request.order_type is OrderType.LIMIT
    assert request.price == Decimal("2800")
    assert request.reduce_only is True


def test_planner_maps_long_stop_loss_to_sell_stop_market():
    plan = ExecutionPlanner().plan(
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.PLACE_STOP_LOSS_LONG,
            quantity=Decimal("0.1"),
            trigger_price=Decimal("2500"),
        )
    )

    item = plan.items[0]
    assert item.action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER
    request = item.stop_market_request
    assert request is not None
    assert request.side is OrderSide.SELL
    assert request.trigger_price == Decimal("2500")
    assert request.reduce_only is True


def test_planner_maps_cancel_all_without_execution_side_effect():
    plan = ExecutionPlanner().plan(TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_STOP_ORDERS))

    assert plan.items[0].action is PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS
    assert plan.items[0].order_request is None
    assert plan.items[0].stop_market_request is None


def test_planner_maps_scoped_stop_cancel_and_preserves_metadata():
    metadata = {
        "strategy_id": "eth_portfolio_v1",
        "sleeve_id": "lf",
        "position_id": "position-1",
        "stop_order_id": "stop-123",
        "stop_client_order_id": "metadata-client-id",
        "target_exchanges": ["okx"],
    }
    plan = ExecutionPlanner().plan(
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.CANCEL_STOP_ORDER,
            client_order_id="signal-client-id",
            metadata=metadata,
        )
    )

    item = plan.items[0]
    assert item.action is PlannedExecutionAction.CANCEL_STOP_ORDER
    assert item.order_request is None
    assert item.stop_market_request is None
    assert item.cancel_stop_request is not None
    assert item.cancel_stop_request.symbol == "ETH-USDT-PERP"
    assert item.cancel_stop_request.client_order_id == "signal-client-id"
    assert item.cancel_stop_request.stop_order_id == "stop-123"
    assert item.cancel_stop_request.metadata is metadata


def test_planner_uses_metadata_stop_client_order_id_as_fallback():
    plan = ExecutionPlanner().plan(
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.CANCEL_STOP_ORDER,
            client_order_id="   ",
            metadata={
                "stop_client_order_id": "metadata-client-id",
                "strategy_id": "eth_portfolio_v1",
                "sleeve_id": "mf",
                "position_id": "position-2",
            },
        )
    )

    request = plan.items[0].cancel_stop_request
    assert request is not None
    assert request.client_order_id == "metadata-client-id"
    assert request.stop_order_id is None


def test_plan_many_keeps_signal_order():
    planner = ExecutionPlanner()
    plan = planner.plan_many(
        [
            TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1")),
            TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_ORDERS),
        ]
    )

    assert [item.action for item in plan.items] == [
        PlannedExecutionAction.PLACE_ORDER,
        PlannedExecutionAction.CANCEL_ALL_ORDERS,
    ]
