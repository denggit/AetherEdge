from __future__ import annotations

from decimal import Decimal

from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety import RecoveryExitOrderValidator
from src.order_management.stops import ScopedStopReplaceService, StopScope
from src.platform import ExchangeName
from src.platform.exchanges.models import PositionMode, PositionSide
from src.platform.markets import get_market_profile
from src.signals import SignalAction, TradeSignal
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy


def test_invalid_stop_replace_does_not_cancel_confirmed_stop() -> None:
    strategy = _short_strategy()

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("2.55"),
        stop_price=Decimal("1613.6875683869736345"),
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=8,
        reference_price=Decimal("1621.55"),
    )

    assert signals == []
    assert strategy.position.stop_price == Decimal("1686.4243161302636550")
    assert strategy.position.desired_stop_price is None
    assert strategy.position.pending_stop_replace is False
    assert strategy.last_stop_reject_reason == "invalid_stop:stop_not_exchange_valid"


def test_missing_exchange_stop_while_in_position_sets_manual_required() -> None:
    strategy = _short_strategy()
    validator = RecoveryExitOrderValidator(quantity_converter=NativeQuantityConverter())
    validation = validator.validate_stop_orders(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        strategy_id=strategy.config.strategy_id,
        position_id=strategy.position.position_id,
        position_side=PositionSide.SHORT,
        position_mode=PositionMode.ONE_WAY,
        current_position_native_quantity=Decimal("25.5"),
        canonical_stop_price=Decimal("1686.4243161302636550"),
        open_stop_orders=[],
        open_orders=[],
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    signals = strategy._signals_from_recovery_exit_validation(
        validation=validation,
        exchange="okx",
        quantity=Decimal("2.55"),
        stop_price=Decimal("1686.4243161302636550"),
        reason="RECOVERY_MASTER_STOP_SYNC",
    )

    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "critical_stop_missing_while_in_position_manual_required:okx" in strategy.recovery_alerts
    assert any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT for signal in signals)


def test_stop_replace_metadata_marks_non_atomic_when_no_targeted_cancel() -> None:
    strategy = _short_strategy()

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("2.55"),
        stop_price=Decimal("1670"),
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=8,
    )

    place = next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_SHORT)
    assert place.metadata["stop_replace_atomic_supported"] is False
    assert place.metadata["stop_replace_mode"] == "cancel_then_place_validated"
    assert place.metadata["stop_replace_non_atomic_reason"] == "no_targeted_stop_cancel_capability"
    assert place.metadata["replace_mode"] == "cancel_then_place_validated"


def test_stop_replace_never_places_new_then_cancel_all_without_targeted_cancel() -> None:
    strategy = _short_strategy()

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("2.55"),
        stop_price=Decimal("1670"),
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=8,
    )

    seen_place = False
    for signal in signals:
        if signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            seen_place = True
            assert signal.metadata.get("stop_replace_mode") != "place_new_then_cancel_all"
        if seen_place:
            assert signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS


def test_scoped_stop_replace_builds_cancel_for_exact_scope() -> None:
    scope = _scoped_stop()

    signal = ScopedStopReplaceService().build_cancel_signal(scope)

    assert signal.action is SignalAction.CANCEL_STOP_ORDER
    assert signal.symbol == scope.symbol
    assert signal.client_order_id == "lf-old-stop-client"
    assert signal.metadata["stop_order_id"] == "lf-old-stop-order"
    assert signal.metadata["stop_client_order_id"] == "lf-old-stop-client"
    assert signal.metadata["strategy_id"] == "eth_portfolio_v1"
    assert signal.metadata["sleeve_id"] == "lf"
    assert signal.metadata["position_id"] == "lf-position-1"
    assert signal.metadata["position_side"] == "long"
    assert signal.metadata["target_exchanges"] == ["okx"]


def test_scoped_replace_stages_new_stop_before_scoped_old_stop_cancel() -> None:
    scope = _scoped_stop()
    new_stop = TradeSignal(
        symbol=scope.symbol,
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        quantity=Decimal("0.25"),
        trigger_price=Decimal("2450"),
        client_order_id="lf-new-stop-client",
        metadata={
            "strategy_id": scope.strategy_id,
            "sleeve_id": scope.sleeve_id,
            "position_id": scope.position_id,
        },
    )

    signals = ScopedStopReplaceService().build_replace_signals(scope, new_stop)

    # R001 only stages the two boundaries. R002 must verify the first stop at
    # the venue before it dispatches the second signal.
    assert signals[0] is new_stop
    assert signals[0].action is SignalAction.PLACE_STOP_LOSS_LONG
    assert signals[0].metadata["sleeve_id"] == "lf"
    assert signals[1].action is SignalAction.CANCEL_STOP_ORDER
    assert signals[1].metadata["sleeve_id"] == "lf"
    assert all(signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS for signal in signals)


def _scoped_stop() -> StopScope:
    return StopScope(
        strategy_id="eth_portfolio_v1",
        sleeve_id="lf",
        position_id="lf-position-1",
        symbol="ETH-USDT-PERP",
        position_side=PositionSide.LONG,
        target_exchanges=(ExchangeName.OKX,),
        stop_client_order_id="lf-old-stop-client",
        stop_order_id="lf-old-stop-order",
    )


def _short_strategy() -> Strategy:
    strategy = Strategy()
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=1,
        avg_entry=Decimal("1620.30"),
        qty=Decimal("2.55"),
        stop_price=Decimal("1686.4243161302636550"),
        entry_engine="MOMENTUM_V3",
        position_id="stop-replace-safety",
    )
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("1620.30"), base_qty=Decimal("2.55"))
    return strategy
