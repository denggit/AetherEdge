from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from decimal import Decimal
from typing import get_args, get_type_hints

import pytest

from src.runtime.strategy_host import StrategyHost
from src.signals.models import SignalAction, SignalOrderType, TradeSignal
from src.strategy.ports import StrategyPort


EXPECTED_ACTIONS = (
    ("OPEN_LONG", "open_long"),
    ("OPEN_SHORT", "open_short"),
    ("CLOSE_LONG", "close_long"),
    ("CLOSE_SHORT", "close_short"),
    ("REDUCE_LONG", "reduce_long"),
    ("REDUCE_SHORT", "reduce_short"),
    ("PLACE_STOP_LOSS_LONG", "place_stop_loss_long"),
    ("PLACE_STOP_LOSS_SHORT", "place_stop_loss_short"),
    ("CANCEL_ALL_ORDERS", "cancel_all_orders"),
    ("CANCEL_ALL_STOP_ORDERS", "cancel_all_stop_orders"),
    ("CANCEL_STOP_ORDER", "cancel_stop_order"),
)


def _signal(**overrides: object) -> TradeSignal:
    values: dict[str, object] = {
        "symbol": "ETH-USDT-PERP",
        "action": SignalAction.OPEN_LONG,
        "quantity": Decimal("0.25"),
        "created_time_ms": 1_700_000_000_000,
    }
    values.update(overrides)
    return TradeSignal(**values)  # type: ignore[arg-type]


def test_signal_action_members_and_values_are_frozen_exactly() -> None:
    assert tuple((member.name, member.value) for member in SignalAction) == EXPECTED_ACTIONS


def test_trade_signal_public_field_order_and_frozen_contract() -> None:
    signal = _signal()

    assert tuple(field.name for field in fields(TradeSignal)) == (
        "symbol",
        "action",
        "quantity",
        "order_type",
        "price",
        "trigger_price",
        "client_order_id",
        "reason",
        "metadata",
        "created_time_ms",
    )
    with pytest.raises(FrozenInstanceError):
        signal.quantity = Decimal("1")  # type: ignore[misc]


@pytest.mark.parametrize(
    ("case", "kwargs"),
    [
        pytest.param("missing", {}, id="quantity-missing"),
        pytest.param("explicit-none", {"quantity": None}, id="quantity-none"),
        pytest.param("zero", {"quantity": Decimal("0")}, id="quantity-zero"),
        pytest.param("negative", {"quantity": Decimal("-0.01")}, id="quantity-negative"),
    ],
)
@pytest.mark.parametrize(
    "action",
    (
        SignalAction.OPEN_LONG,
        SignalAction.OPEN_SHORT,
        SignalAction.CLOSE_LONG,
        SignalAction.CLOSE_SHORT,
        SignalAction.REDUCE_LONG,
        SignalAction.REDUCE_SHORT,
    ),
)
def test_position_changing_signals_require_positive_quantity(
    action: SignalAction, case: str, kwargs: dict[str, object]
) -> None:
    del case
    base = {"symbol": "ETH-USDT-PERP", "action": action, "created_time_ms": 10}

    with pytest.raises(ValueError, match="quantity must be positive"):
        TradeSignal(**base, **kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "action",
    (
        SignalAction.OPEN_LONG,
        SignalAction.OPEN_SHORT,
        SignalAction.CLOSE_LONG,
        SignalAction.CLOSE_SHORT,
        SignalAction.REDUCE_LONG,
        SignalAction.REDUCE_SHORT,
    ),
)
def test_all_ordinary_position_actions_accept_positive_quantity(action: SignalAction) -> None:
    assert _signal(action=action).quantity == Decimal("0.25")


@pytest.mark.parametrize("price", (None, Decimal("0"), Decimal("-1")))
def test_limit_signal_requires_positive_price(price: Decimal | None) -> None:
    with pytest.raises(ValueError, match="price must be positive"):
        _signal(order_type=SignalOrderType.LIMIT, price=price)


@pytest.mark.parametrize(
    "action", (SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT)
)
@pytest.mark.parametrize("trigger_price", (None, Decimal("0"), Decimal("-1")))
def test_stop_signal_requires_positive_trigger_price(
    action: SignalAction, trigger_price: Decimal | None
) -> None:
    with pytest.raises(ValueError, match="trigger_price must be positive"):
        _signal(action=action, trigger_price=trigger_price)


@pytest.mark.parametrize(
    "action", (SignalAction.CANCEL_ALL_ORDERS, SignalAction.CANCEL_ALL_STOP_ORDERS)
)
def test_cancel_all_actions_do_not_require_quantity(action: SignalAction) -> None:
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=action, created_time_ms=10)
    assert signal.quantity is None


@pytest.mark.parametrize(
    ("case", "kwargs"),
    [
        pytest.param("missing", {}, id="identifier-missing"),
        pytest.param("none", {"client_order_id": None}, id="identifier-none"),
        pytest.param("empty", {"client_order_id": ""}, id="identifier-empty"),
        pytest.param("whitespace", {"client_order_id": "   "}, id="identifier-whitespace"),
    ],
)
def test_scoped_stop_cancel_rejects_each_absent_identifier_shape(
    case: str, kwargs: dict[str, object]
) -> None:
    del case
    with pytest.raises(ValueError, match="required for scoped stop cancel"):
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.CANCEL_STOP_ORDER,
            created_time_ms=10,
            **kwargs,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "kwargs",
    (
        {"client_order_id": "client-1"},
        {"metadata": {"stop_order_id": "order-1"}},
        {"metadata": {"stop_client_order_id": "client-2"}},
    ),
)
def test_scoped_stop_cancel_accepts_each_public_identifier(kwargs: dict[str, object]) -> None:
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        created_time_ms=10,
        **kwargs,  # type: ignore[arg-type]
    )
    assert signal.action is SignalAction.CANCEL_STOP_ORDER


def test_strategy_port_callbacks_retain_sequence_of_trade_signal_return_contract() -> None:
    callback_names = (
        "on_start",
        "on_kline",
        "on_ticker",
        "on_trade",
        "on_order_book",
        "on_account_event",
    )

    for name in callback_names:
        annotation = get_type_hints(getattr(StrategyPort, name))["return"]
        assert get_args(annotation) == (TradeSignal,)
        assert all(
            target_name not in repr(annotation)
            for target_name in (
                "StrategyDecision",
                "VirtualSleeveTarget",
                "StrategyTargetPosition",
            )
        )


@pytest.mark.asyncio
async def test_strategy_host_normalizes_none_and_preserves_order_result_feedback_boundary() -> None:
    signal = _signal()
    calls: list[tuple[str, object]] = []

    class FakeStrategy:
        async def on_start(self, snapshot):
            calls.append(("start", snapshot))
            return None

        async def on_order_results(self, **kwargs):
            calls.append(("feedback", kwargs))
            return (signal,)

    strategy = FakeStrategy()
    host = StrategyHost(strategy)

    assert await host.on_start(None) == ()  # type: ignore[arg-type]
    result = await host.on_order_results(
        signal=signal,
        results=(),
        source="characterization",
        event_time_ms=123,
    )

    assert result == (signal,)
    assert calls[1][1] == {
        "signal": signal,
        "results": (),
        "source": "characterization",
        "event_time_ms": 123,
    }
    assert vars(host) == {"_strategy": strategy}
