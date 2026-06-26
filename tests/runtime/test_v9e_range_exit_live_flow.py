from __future__ import annotations

from decimal import Decimal

from src.order_management.models import ExchangeOrderResult
from src.platform import ExchangeName
from src.signals import SignalAction
from strategies.eth_lf_portfolio_v8.domain.models import (
    BarReadyContext,
    ClosedKlineContext,
    MicroDecision,
    RangeAggregateContext,
    RoutedSignal,
    Side,
)
from strategies.eth_lf_portfolio_v8.strategy import Strategy
from strategies.eth_lf_portfolio_v8.strategy import PendingAddAfterStopUpdatePlan, PendingEntryPlan
from tools.v8_live_preflight_check import PreflightReport, _check_range_exit_config, _check_strategy_identity


H4 = 4 * 60 * 60_000


def test_range_exit_long_produces_close_long_with_metadata() -> None:
    strategy = _started_strategy(Side.LONG)
    context = _bar_ready_context(side=Side.LONG, close=Decimal("114"), high=Decimal("140"), low=Decimal("98"))

    signals = strategy._position_lifecycle_signals(context)

    assert [signal.action for signal in signals] == [SignalAction.CLOSE_LONG]
    assert signals[0].reason == "RANGE_EXIT_NEXT_OPEN"
    assert signals[0].metadata["range_exit_triggered"] is True
    assert signals[0].metadata["range_exit_peak_r"] == "4"
    assert signals[0].metadata["range_exit_current_r"] == "1.4"
    assert signals[0].metadata["range_exit_giveback_frac"] == "0.65"
    assert signals[0].metadata["range_exit_min_mfe_r"] == "2.0"
    assert signals[0].metadata["range_exit_giveback_threshold"] == "0.65"
    assert signals[0].metadata["range_exit_contra_imbalance"] == "0.05"
    assert signals[0].metadata["range_exit_bad_close_pos"] == "0.35"
    assert signals[0].metadata["micro_context_available"] is True


def test_range_exit_short_produces_close_short() -> None:
    strategy = _started_strategy(Side.SHORT)
    context = _bar_ready_context(side=Side.SHORT, close=Decimal("86"), high=Decimal("102"), low=Decimal("60"))

    signals = strategy._position_lifecycle_signals(context)

    assert [signal.action for signal in signals] == [SignalAction.CLOSE_SHORT]
    assert signals[0].reason == "RANGE_EXIT_NEXT_OPEN"


def test_range_exit_priority_is_below_channel_and_opposite() -> None:
    strategy = _started_strategy(Side.LONG)

    channel = strategy._position_lifecycle_signals(
        _bar_ready_context(side=Side.LONG, close=Decimal("114"), high=Decimal("140"), low=Decimal("98"), exit_channel=True)
    )[0]
    strategy = _started_strategy(Side.LONG)
    opposite = strategy._position_lifecycle_signals(
        _bar_ready_context(side=Side.LONG, close=Decimal("114"), high=Decimal("140"), low=Decimal("98"), opposite=True)
    )[0]

    assert channel.reason == "V8_CHANNEL_EXIT"
    assert opposite.reason == "V8_OPPOSITE_SIGNAL_EXIT"


def test_range_exit_priority_is_above_max_hold() -> None:
    strategy = _started_strategy(Side.SHORT)
    context = _bar_ready_context(side=Side.SHORT, close=Decimal("86"), high=Decimal("102"), low=Decimal("60"), close_time_ms=200 * H4)

    signals = strategy._position_lifecycle_signals(context)

    assert signals[0].reason == "RANGE_EXIT_NEXT_OPEN"


def test_range_exit_blocks_same_bar_add_and_clears_pending_add() -> None:
    strategy = _started_strategy(Side.LONG)
    context = _bar_ready_context(side=Side.LONG, close=Decimal("114"), high=Decimal("140"), low=Decimal("98"))
    strategy.pending_add_after_stop_update = _pending_add_plan(strategy, context)

    signals = strategy._position_lifecycle_signals(context)

    assert [signal.action for signal in signals] == [SignalAction.CLOSE_LONG]
    assert strategy.pending_entry is None
    assert strategy.pending_add_after_stop_update is None
    assert not any(signal.action in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT} for signal in signals)


def test_range_exit_metadata_appears_in_decision_audit() -> None:
    strategy = _started_strategy(Side.LONG)
    context = _bar_ready_context(side=Side.LONG, close=Decimal("114"), high=Decimal("140"), low=Decimal("98"))

    signals = strategy._position_lifecycle_signals(context)
    audit = strategy._build_decision_audit(context, signals)

    assert audit["range_exit_triggered"] is True
    assert audit["range_exit_reason"] == "RANGE_EXIT_NEXT_OPEN"
    assert audit["range_exit_peak_r"] == "4"
    assert audit["range_exit_current_r"] == "1.4"
    assert audit["range_exit_giveback_frac"] == "0.65"


def test_preflight_requires_v9e_strategy_id_and_range_exit_config() -> None:
    strategy = Strategy()
    report = PreflightReport(started_time_ms=1)
    requirements = type(
        "Req",
        (),
        {
            "range_bars": type("RangeBars", (), {"enabled": True})(),
            "trades": type("Trades", (), {"enabled": True, "stream_enabled": True})(),
        },
    )()

    _check_strategy_identity(report, strategy)
    _check_range_exit_config(report, strategy, requirements=requirements)

    assert any(check.name == "strategy_id_v9e" and check.status == "ok" for check in report.checks)
    assert any(check.name == "range_exit_configured" and check.status == "ok" for check in report.checks)
    assert any(check.name == "range_exit_no_delay" and check.status == "ok" for check in report.checks)


def test_v9e_risk_per_coin_preserved_after_add_fill() -> None:
    strategy = _started_strategy(Side.LONG)

    strategy.position.add_master_fill(avg_fill_price=Decimal("110"), add_qty=Decimal("1"))

    assert strategy.position.first_entry == Decimal("100")
    assert strategy.position.initial_sl == Decimal("90")
    assert strategy.position.avg_entry == Decimal("105")
    assert strategy.position.risk_per_coin == Decimal("10")


def test_v9e_risk_per_coin_preserved_after_master_position_reconcile() -> None:
    strategy = _started_strategy(Side.LONG)

    strategy._reconcile_master_position_from_exchange_result(
        result=_master_position_result(entry_price=Decimal("105"), base_quantity=Decimal("2"), side="long"),
        event_time_ms=H4,
    )

    assert strategy.position.avg_entry == Decimal("105")
    assert strategy.position.qty == Decimal("2")
    assert strategy.position.risk_per_coin == Decimal("10")


def test_v9e_risk_per_coin_initialized_from_first_entry_when_missing() -> None:
    strategy = _started_strategy(Side.LONG)
    strategy.position.risk_per_coin = None

    strategy._reconcile_master_position_from_exchange_result(
        result=_master_position_result(entry_price=Decimal("105"), base_quantity=Decimal("2"), side="long"),
        event_time_ms=H4,
    )

    assert strategy.position.avg_entry == Decimal("105")
    assert strategy.position.risk_per_coin == Decimal("10")


def test_v9e_range_exit_uses_preserved_initial_risk_after_add_reconcile() -> None:
    strategy = _started_strategy(Side.LONG)
    strategy.position.avg_entry = Decimal("105")
    strategy.position.qty = Decimal("2")
    strategy.position.units = 2
    strategy.position.risk_per_coin = Decimal("10")
    context = _bar_ready_context(side=Side.LONG, close=Decimal("118"), high=Decimal("145"), low=Decimal("104"))

    signals = strategy._position_lifecycle_signals(context)

    assert [signal.action for signal in signals] == [SignalAction.CLOSE_LONG]
    assert signals[0].reason == "RANGE_EXIT_NEXT_OPEN"
    assert signals[0].metadata["range_exit_peak_r"] == "4"
    assert signals[0].metadata["range_exit_current_r"] == "1.3"
    assert signals[0].metadata["range_exit_giveback_frac"] == "0.675"


def _started_strategy(side: Side) -> Strategy:
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.exchange_equity["okx"] = Decimal("1000")
    strategy.exchange_available["okx"] = Decimal("1000")
    stop = Decimal("90") if side is Side.LONG else Decimal("110")
    action_qty = Decimal("1")
    strategy.position.open_master(
        side=side,
        entry_time_ms=0,
        avg_entry=Decimal("100"),
        qty=action_qty,
        stop_price=stop,
        entry_engine="MOMENTUM_V3",
        entry_risk_mult=Decimal("1"),
        position_id=f"v9e-{side.name.lower()}",
    )
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("100"), base_qty=action_qty)
    return strategy


def _bar_ready_context(
    *,
    side: Side,
    close: Decimal,
    high: Decimal,
    low: Decimal,
    close_time_ms: int = 3 * H4,
    exit_channel: bool = False,
    opposite: bool = False,
) -> BarReadyContext:
    if side is Side.LONG:
        imbalance = Decimal("-0.06")
        close_pos = Decimal("0.50")
        routed_side = Side.SHORT if opposite else Side.LONG
        channel_key = "long_exit_channel"
    else:
        imbalance = Decimal("0.06")
        close_pos = Decimal("0.50")
        routed_side = Side.LONG if opposite else Side.SHORT
        channel_key = "short_exit_channel"
    return BarReadyContext(
        kline=ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=close_time_ms - H4,
            close_time_ms=close_time_ms,
            open=Decimal("100"),
            high=high,
            low=low,
            close=close,
            volume=Decimal("1"),
        ),
        range_aggregate=RangeAggregateContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            bucket_start_ms=close_time_ms - H4,
            bucket_end_ms=close_time_ms,
            range_pct=Decimal("0.002"),
            bar_count=8,
            first_open=Decimal("100"),
            last_close=close,
            high=high,
            low=low,
            buy_notional_sum=Decimal("530"),
            sell_notional_sum=Decimal("470"),
            delta_notional_sum=Decimal("60"),
            notional_sum=Decimal("1000"),
            micro_return_pct=Decimal("0.01"),
            imbalance=imbalance,
            taker_buy_ratio=Decimal("0.53"),
            close_pos=close_pos,
        ),
        micro=MicroDecision(
            signal_side=side,
            context_available=True,
            aligned=False,
            contra=True,
            entry_risk_scale=Decimal("1"),
            action="CONTRA_RISK_REDUCED",
        ),
        global_risk_scale=Decimal("1.3"),
        routed_signal=RoutedSignal(routed_side, "MOMENTUM_V3", 100, Decimal("1"), Decimal("1")),
        engine_features={"momentum": {"atr": Decimal("5"), channel_key: exit_channel}},
    )


def _pending_add_plan(strategy: Strategy, context: BarReadyContext) -> PendingAddAfterStopUpdatePlan:
    entry = PendingEntryPlan(
        position_id=strategy.position.position_id or "v9e-long",
        side=strategy.position.side,
        engine=strategy.position.entry_engine,
        quantity=Decimal("0.1"),
        estimated_entry_price=context.kline.close,
        atr=Decimal("5"),
        initial_atr_mult=Decimal("2.2"),
        bar_close_time_ms=context.kline.close_time_ms,
        entry_risk_scale=Decimal("1.3"),
        risk_mult=Decimal("1"),
        quality_mult=Decimal("1"),
        is_add=True,
        stop_update_checked_at_ms=context.kline.close_time_ms,
    )
    return PendingAddAfterStopUpdatePlan(
        entry=entry,
        exchange_quantities={"okx": Decimal("0.1")},
        stop_price=Decimal("90"),
        add_unit_number=2,
        position_qty=strategy.position.qty,
        position_units=strategy.position.units,
    )


def _master_position_result(*, entry_price: Decimal, base_quantity: Decimal, side: str) -> ExchangeOrderResult:
    return ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        raw={
            "exchange_position_entry_price": entry_price,
            "exchange_position_base_quantity": base_quantity,
            "exchange_position_side": side,
            "exchange_position_source": "stop_post_check",
        },
    )
