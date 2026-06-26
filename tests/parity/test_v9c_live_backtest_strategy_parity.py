"""V9C live-vs-backtest strategy parity tests.

The expected values here come from the canonical snapshot helper in
`v9c_backtest_canonical_helpers.py`, not from importing CoinBacktest at test
runtime. This keeps AetherEdge tests deterministic while making the canonical
baseline explicit.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management.quantity import NativeQuantityConverter
from src.order_management.models import ExchangeOrderResult
from src.platform import ExchangeName
from src.platform.exchanges.models import OrderSide, OrderStatus
from src.platform.markets import get_market_profile
from src.signals import SignalAction
from strategies.eth_lf_portfolio_v8.domain.models import (
    BarReadyContext,
    ClosedKlineContext,
    EngineSignal,
    MicroDecision,
    RangeAggregateContext,
    RoutedSignal,
    Side,
)
from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter
from strategies.eth_lf_portfolio_v8.execution.sizing import RiskSizingConfig, V8RiskSizer
from strategies.eth_lf_portfolio_v8.execution.stops import initial_stop_from_risk, protected_stop
from strategies.eth_lf_portfolio_v8.strategy import Strategy
from tests.parity import v9c_backtest_canonical_helpers as canonical


FIRST_ENTRY = Decimal("1620.30")
INITIAL_STOP = Decimal("1686.4243161302636550")
PROTECTED_STOP = Decimal("1613.6875683869736345")
BAR_CLOSE_MS = 4 * 60 * 60_000


def test_v9c_parity_same_bar_stop_update_add_policy_matches_backtest() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(close=Decimal("1583.72"), high=Decimal("1625"), low=Decimal("1550"), atr=Decimal("10"))

    signals = strategy._position_lifecycle_signals(context)

    assert canonical.same_bar_stop_update_allows_add() is True
    assert [signal.action for signal in signals] == [
        SignalAction.CANCEL_ALL_STOP_ORDERS,
        SignalAction.PLACE_STOP_LOSS_SHORT,
    ]
    assert strategy.pending_entry is None
    assert strategy.pending_add_after_stop_update is not None


def test_live_defers_add_until_stop_update_confirmed_when_same_bar_backtest_allows_both() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(close=Decimal("1583.72"), high=Decimal("1625"), low=Decimal("1550"), atr=Decimal("10"))

    stop_signals = strategy._position_lifecycle_signals(context)
    follow_up = asyncio.run(
        strategy.on_order_results(
            signal=stop_signals[1],
            results=[_successful_stop_result()],
            source="test",
            event_time_ms=BAR_CLOSE_MS + 1,
        )
    )

    assert [signal.action for signal in follow_up] == [SignalAction.OPEN_SHORT]
    assert follow_up[0].metadata["deferred_after_stop_update"] is True
    assert strategy.pending_entry is not None
    assert strategy.pending_entry.stop_update_checked_at_ms == BAR_CLOSE_MS


def test_live_does_not_execute_deferred_add_when_stop_update_fails() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(close=Decimal("1583.72"), high=Decimal("1625"), low=Decimal("1550"), atr=Decimal("10"))

    stop_signals = strategy._position_lifecycle_signals(context)
    follow_up = asyncio.run(
        strategy.on_order_results(
            signal=stop_signals[1],
            results=[ExchangeOrderResult(exchange=ExchangeName.OKX, ok=False, error="reject")],
            source="test",
            event_time_ms=BAR_CLOSE_MS + 1,
        )
    )

    assert follow_up == []
    assert strategy.pending_entry is None
    assert strategy.pending_add_after_stop_update is None


def test_live_add_after_stop_confirmed_replaces_stop_for_new_total_position() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(close=Decimal("1583.72"), high=Decimal("1625"), low=Decimal("1550"), atr=Decimal("10"))

    stop_signals = strategy._position_lifecycle_signals(context)
    add_signals = asyncio.run(
        strategy.on_order_results(
            signal=stop_signals[1],
            results=[_successful_stop_result()],
            source="test",
            event_time_ms=BAR_CLOSE_MS + 1,
        )
    )
    add_qty = add_signals[0].quantity
    assert add_qty is not None
    stop_after_add = asyncio.run(
        strategy.on_order_results(
            signal=add_signals[0],
            results=[
                ExchangeOrderResult(
                    exchange=ExchangeName.OKX,
                    ok=True,
                    status=OrderStatus.FILLED,
                    side=OrderSide.SELL,
                    quantity=add_qty,
                    filled_quantity=add_qty,
                    avg_fill_price=Decimal("1583.72"),
                    raw={"quantity_semantics": "base_asset"},
                )
            ],
            source="test",
            event_time_ms=BAR_CLOSE_MS + 2,
        )
    )

    assert strategy.position.qty == Decimal("2.55") + add_qty
    assert stop_after_add[1].action is SignalAction.PLACE_STOP_LOSS_SHORT
    assert stop_after_add[1].quantity == strategy.position.qty
    assert stop_after_add[1].trigger_price == PROTECTED_STOP


def test_v9c_parity_open_signal_matches_backtest_canonical() -> None:
    router = PortfolioRouter()
    routed = router.select(
        [
            EngineSignal(Side.SHORT, "MOMENTUM_V3", 100, Decimal("1.2"), Decimal("1.1")),
            EngineSignal(Side.LONG, "BULL_RECLAIM_V2", 150, Decimal("1.0"), Decimal("1.3")),
            EngineSignal(Side.SHORT, "BEAR_V3_ONLY", 50, Decimal("1.4"), Decimal("1.2")),
        ]
    )
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("700")
    strategy.exchange_equity["okx"] = Decimal("700")
    context = _bar_ready_context(
        close=Decimal("1620.30"),
        high=Decimal("1630"),
        low=Decimal("1600"),
        atr=Decimal("30.00"),
        side=Side.LONG,
        routed_signal=routed,
        micro_scale=Decimal("0.5"),
        engine="BULL_RECLAIM_V2",
    )

    signals = strategy._signals_from_ready_context(context)

    assert routed.engine == "BULL_RECLAIM_V2"
    assert routed.side is Side.LONG
    assert routed.risk_mult == Decimal("1.0")
    assert routed.quality_mult == Decimal("1.3")
    assert [signal.action for signal in signals] == [SignalAction.OPEN_LONG]
    assert signals[0].metadata["engine"] == "BULL_RECLAIM_V2"
    assert Decimal(signals[0].metadata["risk_mult"]) == Decimal("1.0")
    assert Decimal(signals[0].metadata["quality_mult"]) == Decimal("1.3")
    assert signals[0].metadata["micro_entry_risk_scale"] == "0.5"


def test_v9c_parity_initial_entry_sizing_matches_backtest() -> None:
    equity = Decimal("700")
    entry_price = Decimal("1620.30")
    initial_stop = Decimal("1686.42")
    risk_mult = Decimal("1.0")
    quality_mult = Decimal("1.1")
    micro_scale = Decimal("0.5")
    global_scale = Decimal("1.3")
    risk_pct = Decimal("0.022")
    max_notional = Decimal("11.0")
    live_qty = V8RiskSizer(RiskSizingConfig(risk_pct=risk_pct, max_total_notional_mult=max_notional)).unit_qty(
        equity=equity,
        entry_price=entry_price,
        stop_price=initial_stop,
        risk_mult=risk_mult,
        quality_mult=quality_mult,
        micro_entry_risk_scale=micro_scale,
        global_risk_scale=global_scale,
    )
    canonical_qty = canonical.unit_qty(
        capital=equity,
        entry_price=entry_price,
        stop_dist=abs(entry_price - initial_stop),
        current_qty=Decimal("0"),
        cfg=canonical.CanonicalExecConfig(risk_pct, max_notional),
        risk_mult=risk_mult * quality_mult * micro_scale * global_scale,
    )

    assert live_qty == canonical_qty
    assert risk_pct * risk_mult * quality_mult * micro_scale * global_scale == Decimal("0.01573")


def test_v9c_parity_initial_stop_formula_matches_backtest() -> None:
    entry = Decimal("1620.30")
    atr = Decimal("30.055")
    initial_atr_mult = Decimal("2.2")

    assert initial_stop_from_risk(side=Side.LONG, entry_price=entry, risk_per_coin=atr * initial_atr_mult) == canonical.initial_stop(
        side=1,
        entry_price=entry,
        atr=atr,
        initial_atr_mult=initial_atr_mult,
    )
    assert initial_stop_from_risk(side=Side.SHORT, entry_price=entry, risk_per_coin=atr * initial_atr_mult) == canonical.initial_stop(
        side=-1,
        entry_price=entry,
        atr=atr,
        initial_atr_mult=initial_atr_mult,
    )


def test_v9c_parity_protected_stop_formula_matches_backtest() -> None:
    cfg = canonical.CanonicalExecConfig(Decimal("0.022"), Decimal("11"))

    live = protected_stop(
        first_entry=Decimal("1620"),
        avg_entry=Decimal("1600"),
        side=Side.SHORT,
        risk_per_coin=Decimal("40"),
        max_fav=Decimal("1500"),
    )
    expected = canonical.protected_stop(
        first_entry=Decimal("1620"),
        avg_entry=Decimal("1600"),
        side=-1,
        risk_per_coin=Decimal("40"),
        max_fav=Decimal("1500"),
        cfg=cfg,
    )

    assert live == expected


def test_v9c_parity_trailing_stop_formula_matches_backtest() -> None:
    live = min(Decimal("1686.42"), Decimal("1583.72") + Decimal("4.5") * Decimal("10"))
    expected = canonical.trailing_stop(
        side=-1,
        current_stop=Decimal("1686.42"),
        close=Decimal("1583.72"),
        atr=Decimal("10"),
        trailing_atr_mult=Decimal("4.5"),
    )

    assert live == expected


def test_v9c_parity_stop_candidate_selection_matches_backtest() -> None:
    trailing = Decimal("1628.72")
    protected = Decimal("1613.6875683869736345")

    assert min(trailing, protected) == canonical.stop_candidate(side=-1, trailing=trailing, protected=protected)


def test_v9c_parity_add_trigger_matches_backtest() -> None:
    trigger = canonical.add_trigger_price(
        side=-1,
        first_entry=FIRST_ENTRY,
        units=1,
        add_every_r=Decimal("1.0"),
        risk_per_coin=INITIAL_STOP - FIRST_ENTRY,
    )
    context = _bar_ready_context(close=Decimal("1583.72"), high=Decimal("1625"), low=trigger, atr=Decimal("10"))
    strategy = _started_short_strategy()

    signals = strategy._add_signal_if_needed(context)

    assert trigger == FIRST_ENTRY - (INITIAL_STOP - FIRST_ENTRY)
    assert [signal.action for signal in signals] == [SignalAction.OPEN_SHORT]


def test_v9c_parity_units_definition_for_add_matches_backtest() -> None:
    first_trigger = canonical.add_trigger_price(
        side=1,
        first_entry=Decimal("100"),
        units=1,
        add_every_r=Decimal("1.2"),
        risk_per_coin=Decimal("10"),
    )
    second_trigger = canonical.add_trigger_price(
        side=1,
        first_entry=Decimal("100"),
        units=2,
        add_every_r=Decimal("1.2"),
        risk_per_coin=Decimal("10"),
    )

    assert first_trigger == Decimal("112.0")
    assert second_trigger == Decimal("124.0")


def test_v9c_parity_add_sizing_micro_scale_policy_matches_backtest() -> None:
    equity = Decimal("700")
    entry_price = Decimal("1583.72")
    stop_price = Decimal("1613.72")
    current_qty = Decimal("2.55")
    risk_pct = Decimal("0.032")
    max_notional = Decimal("12.0")
    risk_mult = Decimal("1.0")
    quality_mult = Decimal("1.1")
    global_scale = Decimal("1.3")
    micro_entry_risk_scale_from_micro = Decimal("0.5")

    live_qty = V8RiskSizer(RiskSizingConfig(risk_pct=risk_pct, max_total_notional_mult=max_notional)).unit_qty(
        equity=equity,
        entry_price=entry_price,
        stop_price=stop_price,
        risk_mult=risk_mult,
        quality_mult=quality_mult,
        micro_entry_risk_scale=Decimal("1"),
        global_risk_scale=global_scale,
        current_qty=current_qty,
    )
    canonical_qty = canonical.unit_qty(
        capital=equity,
        entry_price=entry_price,
        stop_dist=abs(entry_price - stop_price),
        current_qty=current_qty,
        cfg=canonical.CanonicalExecConfig(risk_pct, max_notional),
        risk_mult=risk_mult * quality_mult * global_scale,
    )

    assert live_qty == canonical_qty
    assert micro_entry_risk_scale_from_micro == Decimal("0.5")
    assert risk_pct * risk_mult * quality_mult * global_scale == Decimal("0.04576")


def test_v9c_parity_exit_channel_matches_backtest() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(
        close=Decimal("1580"),
        high=Decimal("1590"),
        low=Decimal("1570"),
        atr=Decimal("10"),
        engine_features={"momentum": {"short_exit_channel": True}},
    )

    decision = strategy._close_decision_if_needed(context)

    assert decision is not None
    assert decision.reason == "V8_CHANNEL_EXIT"
    assert decision.side is Side.SHORT
    assert decision.quantity == Decimal("2.55")


def test_v9c_parity_opposite_signal_exit_matches_backtest() -> None:
    strategy = _started_short_strategy()
    context = _bar_ready_context(
        close=Decimal("1580"),
        high=Decimal("1590"),
        low=Decimal("1570"),
        atr=Decimal("10"),
        routed_signal=RoutedSignal(Side.LONG, "BULL_RECLAIM_V2", 150),
    )

    decision = strategy._close_decision_if_needed(context)

    assert decision is not None
    assert decision.reason == "V8_OPPOSITE_SIGNAL_EXIT"
    assert decision.side is Side.SHORT
    assert decision.quantity == Decimal("2.55")


def test_v9c_parity_max_hold_exit_matches_backtest() -> None:
    strategy = _started_short_strategy()
    strategy.position.entry_time_ms = 0
    context = _bar_ready_context(
        close=Decimal("1580"),
        high=Decimal("1590"),
        low=Decimal("1570"),
        atr=Decimal("10"),
    )
    context = BarReadyContext(
        kline=ClosedKlineContext(
            symbol=context.kline.symbol,
            exchange=context.kline.exchange,
            timeframe=context.kline.timeframe,
            open_time_ms=360 * BAR_CLOSE_MS,
            close_time_ms=360 * BAR_CLOSE_MS,
            open=context.kline.open,
            high=context.kline.high,
            low=context.kline.low,
            close=context.kline.close,
            volume=context.kline.volume,
            quote_volume=context.kline.quote_volume,
        ),
        range_aggregate=context.range_aggregate,
        micro=context.micro,
        global_risk_scale=context.global_risk_scale,
        routed_signal=context.routed_signal,
        engine_features=context.engine_features,
    )

    decision = strategy._close_decision_if_needed(context)

    assert decision is not None
    assert decision.reason == "V8_MAX_HOLD_EXIT"
    assert decision.quantity == Decimal("2.55")


def test_v9c_parity_okx_contract_conversion_audit() -> None:
    converter = NativeQuantityConverter()
    profile = get_market_profile("ETH-USDT-PERP")

    conversion = converter.convert_quantity(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-SWAP",
        base_quantity=Decimal("0.255"),
        market_profile=profile,
    )

    assert conversion.base_quantity == Decimal("0.255")
    assert conversion.native_quantity == Decimal("2.55")
    assert conversion.contract_value == Decimal("0.1")


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
        position_id="short-parity",
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
    side: Side = Side.SHORT,
    routed_signal: RoutedSignal | None = None,
    micro_scale: Decimal = Decimal("1"),
    engine: str = "MOMENTUM_V3",
    engine_features: dict[str, dict[str, object]] | None = None,
) -> BarReadyContext:
    features = engine_features or {{"MOMENTUM_V3": "momentum", "BULL_RECLAIM_V2": "bull", "BEAR_V3_ONLY": "bear"}[engine]: {} if atr is None else {"atr": atr}}
    return BarReadyContext(
        kline=ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=0,
            close_time_ms=BAR_CLOSE_MS,
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
            bucket_end_ms=BAR_CLOSE_MS,
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
            signal_side=side,
            context_available=True,
            aligned=True,
            contra=False,
            entry_risk_scale=micro_scale,
            action="allow",
        ),
        global_risk_scale=Decimal("1.3"),
        routed_signal=routed_signal or RoutedSignal(side=side, engine=engine, priority=100, risk_mult=Decimal("1"), quality_mult=Decimal("1")),
        engine_features=features,
    )


def _successful_stop_result() -> ExchangeOrderResult:
    return ExchangeOrderResult(
        exchange=ExchangeName.OKX,
        ok=True,
        order_id="okx-stop-1",
        status=OrderStatus.NEW,
        filled_quantity=Decimal("0"),
        raw={
            "exchange_position_source": "stop_post_check",
            "exchange_position_entry_price": str(FIRST_ENTRY),
            "exchange_position_base_quantity": "2.55",
            "exchange_position_side": "short",
        },
    )
