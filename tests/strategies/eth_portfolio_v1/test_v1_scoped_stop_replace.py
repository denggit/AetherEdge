from __future__ import annotations

from decimal import Decimal

from src.order_management.models import ExchangeOrderResult
from src.platform.exchanges.models import ExchangeName, OrderStatus
from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.strategy import Strategy


def test_v1_stop_replace_places_new_stop_before_exact_scoped_cancel() -> None:
    strategy = _long_strategy()
    strategy.position.record_stop_order(
        exchange="okx",
        stop_order_id="okx-old-stop-order",
        stop_client_order_id="okx-old-stop-client",
        stop_price=Decimal("2400"),
    )

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("0.40"),
        stop_price=Decimal("2450"),
        reason="V1_LF_TRAILING_STOP_UPDATE",
        bar_close_time_ms=2,
    )

    assert [signal.action for signal in signals] == [
        SignalAction.PLACE_STOP_LOSS_LONG,
        SignalAction.CANCEL_STOP_ORDER,
    ]
    new_stop, old_stop_cancel = signals

    assert new_stop.quantity == Decimal("0.40")
    assert new_stop.metadata["reduce_only"] is True
    assert new_stop.metadata["target_exchanges"] == ["okx"]
    assert new_stop.metadata["strategy_id"] == "eth_portfolio_v1"
    assert new_stop.metadata["sleeve_id"] == "lf"
    assert new_stop.metadata["position_id"] == "v1-lf-position"
    assert new_stop.metadata["scoped_cancel_pending"] is True
    assert new_stop.metadata["manual_stop_cleanup_required"] is False

    assert old_stop_cancel.client_order_id == "okx-old-stop-client"
    assert old_stop_cancel.metadata["stop_order_id"] == "okx-old-stop-order"
    assert old_stop_cancel.metadata["stop_client_order_id"] == "okx-old-stop-client"
    assert old_stop_cancel.metadata["strategy_id"] == "eth_portfolio_v1"
    assert old_stop_cancel.metadata["sleeve_id"] == "lf"
    assert old_stop_cancel.metadata["position_id"] == "v1-lf-position"
    assert old_stop_cancel.metadata["position_side"] == "long"
    assert old_stop_cancel.metadata["target_exchanges"] == ["okx"]


def test_v1_stop_replace_without_old_identifier_never_uses_global_cancel() -> None:
    strategy = _long_strategy()

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("0.40"),
        stop_price=Decimal("2450"),
        reason="V1_LF_TRAILING_STOP_UPDATE",
        bar_close_time_ms=2,
    )

    assert [signal.action for signal in signals] == [SignalAction.PLACE_STOP_LOSS_LONG]
    assert all(signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS for signal in signals)
    new_stop = signals[0]
    assert new_stop.metadata["reduce_only"] is True
    assert new_stop.metadata["target_exchanges"] == ["okx"]
    assert new_stop.metadata["scoped_cancel_pending"] is False
    assert new_stop.metadata["scoped_cancel_skip_reason"] == "missing_old_stop_identifier"
    assert new_stop.metadata["manual_stop_cleanup_required"] is True


def test_v1_stop_results_store_new_identifiers_for_the_next_replace() -> None:
    strategy = _long_strategy()
    strategy.position.mark_pending_stop_replace(
        desired_stop_price=Decimal("2450"),
        reason="V1_LF_TRAILING_STOP_UPDATE",
        bar_close_time_ms=2,
    )
    signal = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("0.40"),
        stop_price=Decimal("2450"),
        reason="V1_LF_TRAILING_STOP_UPDATE",
        bar_close_time_ms=2,
    )[0]

    strategy._handle_stop_order_results(
        signal=signal,
        results=[
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=True,
                order_id="okx-new-stop-order",
                client_order_id="okx-new-stop-client",
                status=OrderStatus.NEW,
            )
        ],
        event_time_ms=3,
    )

    leg = strategy.position.legs["okx"]
    assert leg.stop_order_id == "okx-new-stop-order"
    assert leg.stop_client_order_id == "okx-new-stop-client"
    assert leg.stop_price == Decimal("2450")


def test_v1_multi_exchange_replace_cancels_only_each_legs_old_stop() -> None:
    strategy = _long_strategy()
    strategy.position.mark_leg_open(
        exchange="binance",
        avg_fill_price=Decimal("2500"),
        base_qty=Decimal("0.20"),
    )
    strategy.position.record_stop_order(
        exchange="okx",
        stop_order_id="okx-old-stop-order",
        stop_client_order_id=None,
        stop_price=Decimal("2400"),
    )
    strategy.position.record_stop_order(
        exchange="binance",
        stop_order_id="binance-old-stop-order",
        stop_client_order_id="binance-old-stop-client",
        stop_price=Decimal("2400"),
    )

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx", "binance"],
        quantity=Decimal("0.20"),
        stop_price=Decimal("2450"),
        reason="V1_LF_TRAILING_STOP_UPDATE",
        bar_close_time_ms=2,
        exchange_quantities={
            "okx": Decimal("0.20"),
            "binance": Decimal("0.20"),
        },
    )

    assert signals[0].action is SignalAction.PLACE_STOP_LOSS_LONG
    assert signals[0].metadata["target_exchanges"] == ["okx", "binance"]
    cancels = signals[1:]
    assert [signal.action for signal in cancels] == [
        SignalAction.CANCEL_STOP_ORDER,
        SignalAction.CANCEL_STOP_ORDER,
    ]
    assert {
        (
            tuple(signal.metadata["target_exchanges"]),
            signal.metadata["stop_order_id"],
            signal.metadata["stop_client_order_id"],
        )
        for signal in cancels
    } == {
        (("okx",), "okx-old-stop-order", None),
        (("binance",), "binance-old-stop-order", "binance-old-stop-client"),
    }


def _long_strategy() -> Strategy:
    strategy = Strategy()
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.40"),
        stop_price=Decimal("2400"),
        entry_engine="MOMENTUM_V3",
        position_id="v1-lf-position",
    )
    strategy.position.mark_leg_open(
        exchange="okx",
        avg_fill_price=Decimal("2500"),
        base_qty=Decimal("0.40"),
    )
    return strategy
