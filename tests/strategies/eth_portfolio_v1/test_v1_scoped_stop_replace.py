from __future__ import annotations

from decimal import Decimal

from src.order_management.models import ExchangeOrderResult
from src.platform.exchanges.models import ExchangeName, OrderStatus
from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.strategy import Strategy


def test_v1_replace_initial_signals_contain_only_new_stop() -> None:
    strategy = _long_strategy()
    strategy.position.record_stop_order(
        exchange="okx",
        stop_order_id="okx-old-stop-order",
        stop_client_order_id="okx-old-stop-client",
        stop_price=Decimal("2400"),
    )

    signals = _replace_stop(strategy, target_exchanges=["okx"])

    assert [signal.action for signal in signals] == [SignalAction.PLACE_STOP_LOSS_LONG]
    new_stop = signals[0]
    assert new_stop.quantity == Decimal("0.40")
    assert new_stop.metadata["reduce_only"] is True
    assert new_stop.metadata["target_exchanges"] == ["okx"]
    assert new_stop.metadata["scoped_cancel_pending"] is True
    assert new_stop.metadata["scoped_cancel_targets"] == [
        {
            "exchange": "okx",
            "stop_order_id": "okx-old-stop-order",
            "stop_client_order_id": "okx-old-stop-client",
        }
    ]
    assert new_stop.metadata["manual_stop_cleanup_required"] is False


def test_v1_successful_new_stop_returns_exact_scoped_cancel_and_records_new_id() -> None:
    strategy = _long_strategy()
    strategy.position.record_stop_order(
        exchange="okx",
        stop_order_id="okx-old-stop-order",
        stop_client_order_id="okx-old-stop-client",
        stop_price=Decimal("2400"),
    )
    new_stop = _replace_stop(strategy, target_exchanges=["okx"])[0]

    follow_up = strategy._handle_stop_order_results(
        signal=new_stop,
        results=[
            _successful_stop_result(
                exchange=ExchangeName.OKX,
                order_id="okx-new-stop-order",
                client_order_id="okx-new-stop-client",
            )
        ],
        event_time_ms=3,
    )

    assert [signal.action for signal in follow_up] == [SignalAction.CANCEL_STOP_ORDER]
    old_stop_cancel = follow_up[0]
    assert old_stop_cancel.client_order_id == "okx-old-stop-client"
    assert old_stop_cancel.metadata["stop_order_id"] == "okx-old-stop-order"
    assert old_stop_cancel.metadata["stop_client_order_id"] == "okx-old-stop-client"
    assert old_stop_cancel.metadata["strategy_id"] == "eth_portfolio_v1"
    assert old_stop_cancel.metadata["sleeve_id"] == "lf"
    assert old_stop_cancel.metadata["position_id"] == "v1-lf-position"
    assert old_stop_cancel.metadata["position_side"] == "long"
    assert old_stop_cancel.metadata["target_exchanges"] == ["okx"]

    leg = strategy.position.legs["okx"]
    assert leg.stop_order_id == "okx-new-stop-order"
    assert leg.stop_client_order_id == "okx-new-stop-client"
    assert leg.stop_price == Decimal("2450")


def test_v1_successful_stop_records_verified_exchange_effective_price() -> None:
    strategy = _long_strategy()
    canonical = Decimal("1738.2542231936259150")
    strategy.position.mark_pending_stop_replace(
        desired_stop_price=canonical,
        reason="TEST_TICK_NORMALIZATION",
        bar_close_time_ms=2,
    )
    signal = strategy._place_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("0.40"),
        stop_price=canonical,
        reason="TEST_TICK_NORMALIZATION",
        bar_close_time_ms=2,
    )[0]
    result = _successful_stop_result(
        exchange=ExchangeName.OKX,
        order_id="okx-normalized-stop",
        client_order_id="okx-normalized-client",
    )
    result = ExchangeOrderResult(
        exchange=result.exchange,
        ok=result.ok,
        order_id=result.order_id,
        client_order_id=result.client_order_id,
        status=result.status,
        raw={
            **dict(result.raw),
            "canonical_stop_price": str(canonical),
            "effective_expected_stop_price": "1738.25",
            "actual_exchange_stop_price": "1738.25",
            "confirmed_stop_price": "1738.25",
            "price_tick": "0.01",
            "price_difference": "0",
        },
    )

    strategy._handle_stop_order_results(
        signal=signal,
        results=[result],
        event_time_ms=3,
    )

    assert strategy.position.confirmed_stop_price == Decimal("1738.25")
    assert strategy.position.stop_price == Decimal("1738.25")
    assert strategy.position.legs["okx"].stop_price == Decimal("1738.25")


def test_v1_failed_new_stop_never_returns_old_stop_cancel() -> None:
    strategy = _long_strategy()
    strategy.position.record_stop_order(
        exchange="okx",
        stop_order_id="okx-old-stop-order",
        stop_client_order_id="okx-old-stop-client",
        stop_price=Decimal("2400"),
    )
    new_stop = _replace_stop(strategy, target_exchanges=["okx"])[0]

    follow_up = strategy._handle_stop_order_results(
        signal=new_stop,
        results=[
            ExchangeOrderResult(
                exchange=ExchangeName.OKX,
                ok=False,
                status=OrderStatus.REJECTED,
                error="new stop rejected",
            )
        ],
        event_time_ms=3,
    )

    assert follow_up == []
    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert strategy.position.legs["okx"].stop_order_id == "okx-old-stop-order"


def test_v1_multi_exchange_all_success_returns_each_old_stop_cancel() -> None:
    strategy = _multi_exchange_long_strategy()
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
    new_stop = _replace_stop(
        strategy,
        target_exchanges=["okx", "binance"],
        exchange_quantities={"okx": Decimal("0.40"), "binance": Decimal("0.40")},
    )[0]

    follow_up = strategy._handle_stop_order_results(
        signal=new_stop,
        results=[
            _successful_stop_result(
                exchange=ExchangeName.OKX,
                order_id="okx-new-stop-order",
                client_order_id="okx-new-stop-client",
            ),
            _successful_stop_result(
                exchange=ExchangeName.BINANCE,
                order_id="binance-new-stop-order",
                client_order_id="binance-new-stop-client",
            ),
        ],
        event_time_ms=3,
    )

    assert [signal.action for signal in follow_up] == [
        SignalAction.CANCEL_STOP_ORDER,
        SignalAction.CANCEL_STOP_ORDER,
    ]
    assert {
        (
            tuple(signal.metadata["target_exchanges"]),
            signal.metadata["stop_order_id"],
            signal.metadata["stop_client_order_id"],
        )
        for signal in follow_up
    } == {
        (("okx",), "okx-old-stop-order", None),
        (("binance",), "binance-old-stop-order", "binance-old-stop-client"),
    }


def test_v1_multi_exchange_partial_success_never_cancels_any_old_stop() -> None:
    strategy = _multi_exchange_long_strategy()
    strategy.position.record_stop_order(
        exchange="okx",
        stop_order_id="okx-old-stop-order",
        stop_client_order_id=None,
        stop_price=Decimal("2400"),
    )
    strategy.position.record_stop_order(
        exchange="binance",
        stop_order_id="binance-old-stop-order",
        stop_client_order_id=None,
        stop_price=Decimal("2400"),
    )
    new_stop = _replace_stop(
        strategy,
        target_exchanges=["okx", "binance"],
        exchange_quantities={"okx": Decimal("0.40"), "binance": Decimal("0.40")},
    )[0]

    follow_up = strategy._handle_stop_order_results(
        signal=new_stop,
        results=[
            _successful_stop_result(
                exchange=ExchangeName.OKX,
                order_id="okx-new-stop-order",
                client_order_id="okx-new-stop-client",
            ),
            ExchangeOrderResult(
                exchange=ExchangeName.BINANCE,
                ok=False,
                status=OrderStatus.REJECTED,
                error="binance new stop rejected",
            ),
        ],
        event_time_ms=3,
    )

    assert follow_up == []
    assert strategy.position.legs["okx"].stop_order_id == "okx-old-stop-order"
    assert strategy.position.legs["binance"].stop_order_id == "binance-old-stop-order"


def test_v1_missing_old_identifier_places_new_stop_without_any_cancel() -> None:
    strategy = _long_strategy()

    signals = _replace_stop(strategy, target_exchanges=["okx"])

    assert [signal.action for signal in signals] == [SignalAction.PLACE_STOP_LOSS_LONG]
    new_stop = signals[0]
    assert new_stop.metadata["scoped_cancel_pending"] is False
    assert new_stop.metadata["scoped_cancel_targets"] == []
    assert new_stop.metadata["scoped_cancel_skip_reason"] == "missing_old_stop_identifier"
    assert new_stop.metadata["scoped_cancel_missing_target_exchanges"] == ["okx"]
    assert new_stop.metadata["manual_stop_cleanup_required"] is True

    follow_up = strategy._handle_stop_order_results(
        signal=new_stop,
        results=[
            _successful_stop_result(
                exchange=ExchangeName.OKX,
                order_id="okx-new-stop-order",
                client_order_id="okx-new-stop-client",
            )
        ],
        event_time_ms=3,
    )

    assert follow_up == []
    assert all(signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS for signal in signals)


def _replace_stop(
    strategy: Strategy,
    *,
    target_exchanges: list[str],
    exchange_quantities: dict[str, Decimal] | None = None,
):
    return strategy._replace_stop_signals(
        target_exchanges=target_exchanges,
        quantity=Decimal("0.40"),
        stop_price=Decimal("2450"),
        reason="V1_LF_TRAILING_STOP_UPDATE",
        bar_close_time_ms=2,
        exchange_quantities=exchange_quantities,
    )


def _successful_stop_result(
    *,
    exchange: ExchangeName,
    order_id: str,
    client_order_id: str,
) -> ExchangeOrderResult:
    raw = {}
    if exchange is ExchangeName.OKX:
        raw = {
            "exchange_position_source": "stop_post_check",
            "exchange_position_entry_price": "2500",
            "exchange_position_base_quantity": "0.40",
            "exchange_position_native_quantity": "4",
            "exchange_position_side": "long",
        }
    return ExchangeOrderResult(
        exchange=exchange,
        ok=True,
        order_id=order_id,
        client_order_id=client_order_id,
        status=OrderStatus.NEW,
        raw=raw,
    )


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


def _multi_exchange_long_strategy() -> Strategy:
    strategy = _long_strategy()
    strategy.position.mark_leg_open(
        exchange="binance",
        avg_fill_price=Decimal("2500"),
        base_qty=Decimal("0.40"),
    )
    return strategy
