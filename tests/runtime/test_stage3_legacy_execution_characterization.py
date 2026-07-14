from __future__ import annotations

import ast
from dataclasses import fields
from decimal import Decimal
from pathlib import Path

import pytest

from src.order_management.models import OrderIntent
from src.order_management.position_plan.models import PositionPlan
from src.planner import ExecutionPlanner, PlannedExecutionAction
from src.platform import ExchangeName, OrderSide
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.signal_execution_service import (
    RuntimeSignalExecutionPlan,
    RuntimeSignalExecutionRequest,
    RuntimeSignalExecutionService,
)
from src.signals import SignalAction, TradeSignal


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _signal(action: SignalAction, **overrides: object) -> TradeSignal:
    values: dict[str, object] = {
        "symbol": "ETH-USDT-PERP",
        "action": action,
        "quantity": Decimal("0.2"),
        "created_time_ms": 100,
    }
    if action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
        values["trigger_price"] = Decimal("1900")
    if action in {SignalAction.CANCEL_ALL_ORDERS, SignalAction.CANCEL_ALL_STOP_ORDERS}:
        values.pop("quantity")
    if action is SignalAction.CANCEL_STOP_ORDER:
        values.pop("quantity")
        values["client_order_id"] = "stop-client-1"
    values.update(overrides)
    return TradeSignal(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("action", "planned_action", "side", "reduce_only"),
    (
        (SignalAction.OPEN_LONG, PlannedExecutionAction.PLACE_ORDER, OrderSide.BUY, False),
        (SignalAction.OPEN_SHORT, PlannedExecutionAction.PLACE_ORDER, OrderSide.SELL, False),
        (SignalAction.CLOSE_LONG, PlannedExecutionAction.PLACE_ORDER, OrderSide.SELL, True),
        (SignalAction.CLOSE_SHORT, PlannedExecutionAction.PLACE_ORDER, OrderSide.BUY, True),
        (SignalAction.REDUCE_LONG, PlannedExecutionAction.PLACE_ORDER, OrderSide.SELL, True),
        (SignalAction.REDUCE_SHORT, PlannedExecutionAction.PLACE_ORDER, OrderSide.BUY, True),
        (
            SignalAction.PLACE_STOP_LOSS_LONG,
            PlannedExecutionAction.PLACE_STOP_MARKET_ORDER,
            OrderSide.SELL,
            True,
        ),
        (
            SignalAction.PLACE_STOP_LOSS_SHORT,
            PlannedExecutionAction.PLACE_STOP_MARKET_ORDER,
            OrderSide.BUY,
            True,
        ),
        (SignalAction.CANCEL_ALL_ORDERS, PlannedExecutionAction.CANCEL_ALL_ORDERS, None, None),
        (
            SignalAction.CANCEL_ALL_STOP_ORDERS,
            PlannedExecutionAction.CANCEL_ALL_STOP_ORDERS,
            None,
            None,
        ),
        (SignalAction.CANCEL_STOP_ORDER, PlannedExecutionAction.CANCEL_STOP_ORDER, None, None),
    ),
)
def test_execution_planner_freezes_every_signal_classification_and_side(
    action: SignalAction,
    planned_action: PlannedExecutionAction,
    side: OrderSide | None,
    reduce_only: bool | None,
) -> None:
    item = ExecutionPlanner().plan(_signal(action)).items[0]

    assert item.action is planned_action
    if planned_action is PlannedExecutionAction.PLACE_ORDER:
        assert item.order_request is not None
        assert item.order_request.side is side
        assert item.order_request.reduce_only is reduce_only
        assert item.stop_market_request is None
        assert item.cancel_stop_request is None
    elif planned_action is PlannedExecutionAction.PLACE_STOP_MARKET_ORDER:
        assert item.stop_market_request is not None
        assert item.stop_market_request.side is side
        assert item.stop_market_request.reduce_only is reduce_only
        assert item.order_request is None
        assert item.cancel_stop_request is None
    else:
        assert item.order_request is None
        assert item.stop_market_request is None
        if planned_action is PlannedExecutionAction.CANCEL_STOP_ORDER:
            assert item.cancel_stop_request is not None
        else:
            assert item.cancel_stop_request is None


def test_order_intent_and_position_plan_public_field_order_is_frozen() -> None:
    assert tuple(field.name for field in fields(OrderIntent)) == (
        "intent_id",
        "strategy_id",
        "signal",
        "target_exchanges",
        "status",
        "created_time_ms",
        "metadata",
    )
    assert tuple(field.name for field in fields(PositionPlan)) == (
        "position_id",
        "strategy_id",
        "entry_engine",
        "side",
        "status",
        "canonical_stop_price",
        "master_exchange",
        "master_target_qty_base",
        "master_filled_qty_base",
        "created_time_ms",
        "updated_time_ms",
        "metadata",
    )


def _factory() -> LiveOrderIntentFactory:
    return LiveOrderIntentFactory(
        strategy_id="portfolio-v1",
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )


def _identity_signal(**overrides: object) -> TradeSignal:
    values: dict[str, object] = {
        "symbol": "ETH-USDT-PERP",
        "action": SignalAction.OPEN_LONG,
        "quantity": Decimal("0.2"),
        "metadata": {"position_id": "position-1", "execution_purpose": "normal_entry"},
        "created_time_ms": 100,
    }
    values.update(overrides)
    return TradeSignal(**values)  # type: ignore[arg-type]


def test_live_order_intent_identity_ignores_creation_clock_for_same_business_event() -> None:
    factory = _factory()
    first = factory.create(_identity_signal(created_time_ms=100), event_time_ms=1_000)
    replay = factory.create(_identity_signal(created_time_ms=999), event_time_ms=1_000)

    assert first.intent_id == replay.intent_id


@pytest.mark.parametrize(
    "changed",
    (
        {"symbol": "BTC-USDT-PERP"},
        {"action": SignalAction.OPEN_SHORT},
        {"metadata": {"position_id": "position-2", "execution_purpose": "normal_entry"}},
        {"metadata": {"position_id": "position-1", "execution_purpose": "recovery_topup"}},
    ),
)
def test_live_order_intent_identity_changes_with_stable_business_identity(
    changed: dict[str, object]
) -> None:
    factory = _factory()
    baseline = factory.create(_identity_signal(), event_time_ms=1_000)
    different = factory.create(_identity_signal(**changed), event_time_ms=1_000)

    assert baseline.intent_id != different.intent_id


def test_live_order_intent_identity_changes_with_event_or_target_exchange_set() -> None:
    factory = _factory()
    baseline = factory.create(_identity_signal(), event_time_ms=1_000)
    later_event = factory.create(_identity_signal(), event_time_ms=2_000)
    okx_only = factory.create(
        _identity_signal(
            metadata={
                "position_id": "position-1",
                "execution_purpose": "normal_entry",
                "target_exchanges": ["okx"],
            }
        ),
        event_time_ms=1_000,
    )

    assert baseline.intent_id != later_event.intent_id
    assert baseline.intent_id != okx_only.intent_id


@pytest.mark.asyncio
async def test_runtime_signal_execution_records_complete_flow_and_recursive_follow_up() -> None:
    root = _signal(SignalAction.OPEN_LONG)
    child = _signal(SignalAction.CANCEL_ALL_STOP_ORDERS)
    events: list[tuple[str, object]] = []

    def prepare(signal: TradeSignal, request: RuntimeSignalExecutionRequest) -> bool:
        events.append(("prepare", signal))
        assert isinstance(signal, TradeSignal)
        return True

    def create(signal: TradeSignal, request: RuntimeSignalExecutionRequest) -> object:
        intent = ("intent", signal.action)
        events.append(("create", intent))
        return intent

    async def execute(intent: object) -> tuple[object, ...]:
        events.append(("execute", intent))
        return (("result", intent),)

    async def post_submit(signal: TradeSignal, request: RuntimeSignalExecutionRequest) -> None:
        events.append(("post-submit", signal))

    def handle(signal: TradeSignal, results: tuple[object, ...]) -> None:
        events.append(("handle", signal))

    async def post_order(signal: TradeSignal, request: RuntimeSignalExecutionRequest) -> None:
        events.append(("post-order", signal))

    async def feedback(
        signal: TradeSignal,
        results: tuple[object, ...],
        request: RuntimeSignalExecutionRequest,
    ) -> tuple[TradeSignal, ...]:
        events.append(("feedback", signal))
        return (child,) if signal is root else ()

    def build(
        signal: TradeSignal,
        follow_up: tuple[TradeSignal, ...],
        request: RuntimeSignalExecutionRequest,
    ) -> RuntimeSignalExecutionRequest:
        events.append(("feedback-request", signal))
        return RuntimeSignalExecutionRequest(
            signals=follow_up,
            source="order_result_feedback",
            event_time_ms=request.event_time_ms,
            feedback_depth=request.feedback_depth + 1,
        )

    plan = RuntimeSignalExecutionPlan(
        prepare_signal=prepare,
        create_intent=create,
        execute_intent=execute,
        post_submit_sync=post_submit,
        handle_results=handle,
        post_order_sync=post_order,
        process_feedback=feedback,
        build_feedback_request=build,
    )
    await RuntimeSignalExecutionService().execute(
        RuntimeSignalExecutionRequest(
            signals=(root,), source="characterization", event_time_ms=123
        ),
        plan,
    )

    assert [(name, value is child) for name, value in events] == [
        ("prepare", False),
        ("create", False),
        ("execute", False),
        ("post-submit", False),
        ("handle", False),
        ("post-order", False),
        ("feedback", False),
        ("feedback-request", False),
        ("prepare", True),
        ("create", False),
        ("execute", False),
        ("post-submit", True),
        ("handle", True),
        ("post-order", True),
        ("feedback", True),
    ]


def test_runner_order_result_feedback_depth_is_frozen_at_five() -> None:
    path = PROJECT_ROOT / "src" / "runtime" / "runner.py"
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    function = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_build_signal_feedback_request"
    )
    comparisons = [node for node in ast.walk(function) if isinstance(node, ast.Compare)]

    assert any(
        isinstance(compare.left, ast.Attribute)
        and compare.left.attr == "feedback_depth"
        and len(compare.ops) == 1
        and isinstance(compare.ops[0], ast.GtE)
        and len(compare.comparators) == 1
        and isinstance(compare.comparators[0], ast.Constant)
        and compare.comparators[0].value == 5
        for compare in comparisons
    )

