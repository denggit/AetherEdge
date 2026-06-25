from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management.models import ExchangeOrderResult
from src.platform import ExchangeName
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import OrderSide, OrderStatus
from src.signals import SignalAction
from strategies.eth_lf_portfolio_v8.domain.models import (
    BarReadyContext,
    ClosedKlineContext,
    MicroDecision,
    RangeAggregateContext,
    RoutedSignal,
    Side,
)
from strategies.eth_lf_portfolio_v8.strategy import PendingEntryPlan, Strategy


FIRST_ENTRY = Decimal("1620.30")
INITIAL_STOP = Decimal("1686.4243161302636550")
PROTECTED_STOP = Decimal("1613.6875683869736345")


def test_stop_update_takes_priority_over_add_on_same_closed_bar() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(
        close=Decimal("1583.72"),
        high=Decimal("1625"),
        low=Decimal("1550"),
        atr=None,
    )

    signals = strategy._position_lifecycle_signals(context)

    assert [signal.action for signal in signals] == [
        SignalAction.CANCEL_ALL_STOP_ORDERS,
        SignalAction.PLACE_STOP_LOSS_SHORT,
    ]
    assert signals[1].trigger_price == PROTECTED_STOP
    assert not any(signal.action is SignalAction.OPEN_SHORT for signal in signals)
    assert strategy.pending_entry is None


def test_master_add_fill_does_not_expand_old_stop_quantity_without_stop_check() -> None:
    strategy = _started_short_strategy()
    strategy.pending_entry = PendingEntryPlan(
        position_id="short-add-without-stop-check",
        side=Side.SHORT,
        engine="MOMENTUM_V3",
        quantity=Decimal("4.68"),
        estimated_entry_price=Decimal("1583.72"),
        atr=Decimal("10"),
        initial_atr_mult=Decimal("2.2"),
        bar_close_time_ms=4,
        entry_risk_scale=Decimal("1.3"),
        risk_mult=Decimal("1"),
        quality_mult=Decimal("1"),
        is_add=True,
    )
    event = _master_fill_event(price=Decimal("1583.72"), quantity=Decimal("4.68"), event_time_ms=5)

    signals = strategy._handle_master_entry_fill(event=event, filled_qty=Decimal("4.68"))

    assert signals == []
    assert strategy.position.qty == Decimal("7.23")
    assert not any(
        signal.action is SignalAction.PLACE_STOP_LOSS_SHORT
        and signal.trigger_price == INITIAL_STOP
        and signal.quantity == Decimal("7.23")
        for signal in signals
    )
    assert not any(signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS for signal in signals)
    assert strategy.last_stop_reject_reason == "invalid_stop:add_fill_stop_update_not_checked"


def test_invalid_short_protected_stop_is_blocked_before_exchange_signal() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(
        close=Decimal("1621.55"),
        high=Decimal("1625"),
        low=Decimal("1618"),
        atr=None,
    )
    strategy.position.max_fav = Decimal("1550")

    signals = strategy._stop_update_signals_if_needed(context)

    assert signals == []
    assert strategy.position.stop_price == INITIAL_STOP
    assert strategy.position.confirmed_stop_price == INITIAL_STOP
    assert strategy.position.desired_stop_price is None
    assert strategy.position.pending_stop_replace is False
    assert strategy.last_stop_reject_reason == "invalid_stop:stop_not_exchange_valid"
    assert not any(signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS for signal in signals)
    assert not any(signal.action is SignalAction.PLACE_STOP_LOSS_SHORT for signal in signals)


def test_valid_short_protected_stop_generates_stop_update() -> None:
    strategy = _started_short_strategy()
    strategy.position.max_fav = Decimal("1550")
    context = _bar_ready_context(
        close=Decimal("1583.72"),
        high=Decimal("1625"),
        low=Decimal("1550"),
        atr=None,
    )

    signals = strategy._stop_update_signals_if_needed(context)

    assert [signal.action for signal in signals] == [
        SignalAction.CANCEL_ALL_STOP_ORDERS,
        SignalAction.PLACE_STOP_LOSS_SHORT,
    ]
    assert signals[1].trigger_price == PROTECTED_STOP
    assert strategy.position.stop_price == INITIAL_STOP
    assert strategy.position.desired_stop_price == PROTECTED_STOP
    assert strategy.position.pending_stop_replace is True
    assert signals[1].metadata["replace_mode"] == "cancel_then_place_validated"


def test_stop_order_success_confirms_pending_stop_replace() -> None:
    strategy = _started_short_strategy()
    strategy.position.mark_pending_stop_replace(
        desired_stop_price=PROTECTED_STOP,
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=4,
    )

    asyncio.run(
        strategy.on_order_results(
            signal=_stop_signal(PROTECTED_STOP),
            results=[
                ExchangeOrderResult(
                    exchange=ExchangeName.OKX,
                    ok=True,
                    order_id="okx-stop-1",
                    status=OrderStatus.NEW,
                    filled_quantity=Decimal("0"),
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.position.stop_price == PROTECTED_STOP
    assert strategy.position.pending_stop_replace is False


def test_stop_order_failure_keeps_confirmed_stop_and_requires_manual() -> None:
    strategy = _started_short_strategy()
    strategy.position.mark_pending_stop_replace(
        desired_stop_price=PROTECTED_STOP,
        reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
        bar_close_time_ms=4,
    )

    asyncio.run(
        strategy.on_order_results(
            signal=_stop_signal(PROTECTED_STOP),
            results=[
                ExchangeOrderResult(
                    exchange=ExchangeName.OKX,
                    ok=False,
                    error="exchange rejected stop",
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.position.stop_price == INITIAL_STOP
    assert strategy.position.pending_stop_replace is False
    assert strategy.recovery_manual_required is True
    assert any("stop_replace_failed_manual_required" in item for item in strategy.recovery_alerts)


def test_initial_stop_uses_real_fill_price_not_estimated_close() -> None:
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.pending_entry = PendingEntryPlan(
        position_id="real-fill-entry",
        side=Side.LONG,
        engine="MOMENTUM_V3",
        quantity=Decimal("0.5"),
        estimated_entry_price=Decimal("1617.46"),
        atr=Decimal("10"),
        initial_atr_mult=Decimal("2"),
        bar_close_time_ms=4,
        entry_risk_scale=Decimal("1.3"),
        risk_mult=Decimal("1"),
        quality_mult=Decimal("1"),
    )

    signals = asyncio.run(
        strategy.on_order_results(
            signal=_open_signal(Side.LONG),
            results=[
                ExchangeOrderResult(
                    exchange=ExchangeName.OKX,
                    ok=True,
                    status=OrderStatus.FILLED,
                    side=OrderSide.BUY,
                    quantity=Decimal("0.5"),
                    filled_quantity=Decimal("0.5"),
                    avg_fill_price=Decimal("1620.30"),
                    raw={"fill_price_source": "order_status"},
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.position.avg_entry == Decimal("1620.30")
    assert strategy.position.stop_price == Decimal("1600.30")
    stop = next(signal for signal in signals if signal.action is SignalAction.PLACE_STOP_LOSS_LONG)
    assert stop.trigger_price == Decimal("1600.30")


def test_add_fill_updates_average_entry_from_real_fill_price() -> None:
    strategy = _started_short_strategy()
    strategy.pending_entry = PendingEntryPlan(
        position_id="real-fill-add",
        side=Side.SHORT,
        engine="MOMENTUM_V3",
        quantity=Decimal("4.68"),
        estimated_entry_price=Decimal("1583.72"),
        atr=Decimal("10"),
        initial_atr_mult=Decimal("2.2"),
        bar_close_time_ms=4,
        entry_risk_scale=Decimal("1.3"),
        risk_mult=Decimal("1"),
        quality_mult=Decimal("1"),
        is_add=True,
        stop_update_checked_at_ms=4,
    )
    event = _master_fill_event(price=Decimal("1585.00"), quantity=Decimal("4.68"), event_time_ms=5)

    strategy._handle_master_entry_fill(event=event, filled_qty=Decimal("4.68"))

    expected_avg = (FIRST_ENTRY * Decimal("2.55") + Decimal("1585.00") * Decimal("4.68")) / Decimal("7.23")
    assert strategy.position.qty == Decimal("7.23")
    assert strategy.position.avg_entry == expected_avg


def test_missing_real_fill_does_not_open_position_or_place_stop() -> None:
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.pending_entry = PendingEntryPlan(
        position_id="missing-real-fill",
        side=Side.LONG,
        engine="MOMENTUM_V3",
        quantity=Decimal("0.5"),
        estimated_entry_price=Decimal("1617.46"),
        atr=Decimal("10"),
        initial_atr_mult=Decimal("2"),
        bar_close_time_ms=4,
        entry_risk_scale=Decimal("1.3"),
        risk_mult=Decimal("1"),
        quality_mult=Decimal("1"),
    )

    signals = asyncio.run(
        strategy.on_order_results(
            signal=_open_signal(Side.LONG),
            results=[
                ExchangeOrderResult(
                    exchange=ExchangeName.OKX,
                    ok=False,
                    error="missing_real_fill_price_or_quantity",
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert signals == []
    assert strategy.position.in_pos is False
    assert strategy.pending_entry is None
    assert strategy.recovery_manual_required is True
    assert any("entry_real_fill_missing_manual_required" in item for item in strategy.recovery_alerts)


def _started_short_strategy() -> Strategy:
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.exchange_equity["okx"] = Decimal("1000")
    strategy.exchange_available["okx"] = Decimal("1000")
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=0,
        avg_entry=FIRST_ENTRY,
        qty=Decimal("2.55"),
        stop_price=INITIAL_STOP,
        entry_engine="MOMENTUM_V3",
        entry_risk_mult=Decimal("1"),
        position_id="short-stop-add-safety",
    )
    strategy.position.first_entry = FIRST_ENTRY
    strategy.position.risk_per_coin = INITIAL_STOP - FIRST_ENTRY
    strategy.position.max_fav = FIRST_ENTRY
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=FIRST_ENTRY, base_qty=Decimal("2.55"))
    return strategy


def _bar_ready_context(
    *,
    close: Decimal,
    high: Decimal,
    low: Decimal,
    atr: Decimal | None,
) -> BarReadyContext:
    close_time_ms = 4 * 60 * 60_000
    return BarReadyContext(
        kline=ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=0,
            close_time_ms=close_time_ms,
            open=FIRST_ENTRY,
            high=high,
            low=low,
            close=close,
            volume=Decimal("1000"),
            quote_volume=Decimal("1000000"),
        ),
        range_aggregate=RangeAggregateContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            bucket_start_ms=0,
            bucket_end_ms=close_time_ms,
            range_pct=Decimal("0.002"),
            bar_count=8,
            first_open=FIRST_ENTRY,
            last_close=close,
            high=high,
            low=low,
            buy_notional_sum=Decimal("400000"),
            sell_notional_sum=Decimal("600000"),
            delta_notional_sum=Decimal("-200000"),
            notional_sum=Decimal("1000000"),
            micro_return_pct=Decimal("-0.02"),
            imbalance=Decimal("-0.1"),
            taker_buy_ratio=Decimal("0.4"),
            close_pos=Decimal("0.2"),
        ),
        micro=MicroDecision(
            signal_side=Side.SHORT,
            context_available=True,
            aligned=True,
            contra=False,
            entry_risk_scale=Decimal("1"),
            action="allow",
        ),
        global_risk_scale=Decimal("1.3"),
        routed_signal=RoutedSignal(
            side=Side.SHORT,
            engine="MOMENTUM_V3",
            priority=150,
            risk_mult=Decimal("1"),
            quality_mult=Decimal("1"),
            reason="test_short",
        ),
        engine_features={"momentum": {} if atr is None else {"atr": atr}},
    )


def _master_fill_event(*, price: Decimal, quantity: Decimal, event_time_ms: int) -> AccountEvent:
    return AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol="ETH-USDT-PERP",
        order_status=OrderStatus.FILLED,
        side=OrderSide.SELL,
        price=price,
        quantity=quantity,
        filled_quantity=quantity,
        event_time_ms=event_time_ms,
        raw={"quantity_semantics": "base_asset"},
    )


def _stop_signal(stop_price: Decimal):
    from src.signals import TradeSignal

    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.PLACE_STOP_LOSS_SHORT,
        quantity=Decimal("2.55"),
        trigger_price=stop_price,
        metadata={"target_exchanges": ["okx"]},
    )


def _open_signal(side: Side):
    from src.signals import TradeSignal

    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG if side is Side.LONG else SignalAction.OPEN_SHORT,
        quantity=Decimal("0.5"),
        metadata={"target_exchanges": ["okx"], "execution_purpose": "normal_entry"},
    )
