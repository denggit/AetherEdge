from __future__ import annotations

from decimal import Decimal

from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety import RecoveryExitOrderValidator
from src.platform import ExchangeName
from src.platform.exchanges.models import PositionMode, PositionSide
from src.platform.markets import get_market_profile
from src.signals import SignalAction
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
