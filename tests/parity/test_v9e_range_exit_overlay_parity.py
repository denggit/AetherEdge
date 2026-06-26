from __future__ import annotations

from decimal import Decimal

from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, ClosedKlineContext, MicroDecision, RangeAggregateContext, RoutedSignal
from strategies.eth_lf_portfolio_v8.execution.range_exit import RangeExitConfig, evaluate_range_exit
from strategies.eth_lf_portfolio_v8.strategy import Strategy
from tests.parity import v9e_range_exit_canonical_snapshot as canonical


H4 = 4 * 60 * 60_000


def test_v9e_long_peak_current_giveback_formula_matches_canonical() -> None:
    live = evaluate_range_exit(
        side=Side.LONG,
        avg_entry=Decimal("100"),
        risk_per_coin=Decimal("10"),
        max_fav=Decimal("140"),
        hold_bars=3,
        close=Decimal("114"),
        micro_context_available=True,
        rf_imbalance=Decimal("-0.06"),
        rf_close_pos=Decimal("0.50"),
        config=RangeExitConfig(),
    )
    expected, reason, meta = canonical.range_exit_signal(
        side=1,
        avg_entry=Decimal("100"),
        risk_per_coin=Decimal("10"),
        max_fav=Decimal("140"),
        hold_bars=3,
        close=Decimal("114"),
        micro_context_available=True,
        rf_imbalance=Decimal("-0.06"),
        rf_close_pos=Decimal("0.50"),
    )

    assert live.should_exit is expected
    assert live.reason == reason == "RANGE_EXIT_NEXT_OPEN"
    assert Decimal(live.metadata["range_exit_peak_r"]) == meta["range_exit_peak_r"]
    assert Decimal(live.metadata["range_exit_current_r"]) == meta["range_exit_current_r"]
    assert Decimal(live.metadata["range_exit_giveback_frac"]) == meta["range_exit_giveback_frac"]


def test_v9e_short_peak_current_giveback_formula_matches_canonical() -> None:
    live = evaluate_range_exit(
        side=Side.SHORT,
        avg_entry=Decimal("100"),
        risk_per_coin=Decimal("10"),
        max_fav=Decimal("60"),
        hold_bars=3,
        close=Decimal("86"),
        micro_context_available=True,
        rf_imbalance=Decimal("0.06"),
        rf_close_pos=Decimal("0.50"),
        config=RangeExitConfig(),
    )
    expected, reason, meta = canonical.range_exit_signal(
        side=-1,
        avg_entry=Decimal("100"),
        risk_per_coin=Decimal("10"),
        max_fav=Decimal("60"),
        hold_bars=3,
        close=Decimal("86"),
        micro_context_available=True,
        rf_imbalance=Decimal("0.06"),
        rf_close_pos=Decimal("0.50"),
    )

    assert live.should_exit is expected
    assert live.reason == reason == "RANGE_EXIT_NEXT_OPEN"
    assert Decimal(live.metadata["range_exit_peak_r"]) == meta["range_exit_peak_r"]
    assert Decimal(live.metadata["range_exit_current_r"]) == meta["range_exit_current_r"]
    assert Decimal(live.metadata["range_exit_giveback_frac"]) == meta["range_exit_giveback_frac"]


def test_v9e_range_exit_formula_uses_preserved_initial_risk_after_add_reconcile() -> None:
    live = evaluate_range_exit(
        side=Side.LONG,
        avg_entry=Decimal("105"),
        risk_per_coin=Decimal("10"),
        max_fav=Decimal("145"),
        hold_bars=3,
        close=Decimal("118"),
        micro_context_available=True,
        rf_imbalance=Decimal("-0.06"),
        rf_close_pos=Decimal("0.50"),
        config=RangeExitConfig(),
    )
    expected, reason, meta = canonical.range_exit_signal(
        side=1,
        avg_entry=Decimal("105"),
        risk_per_coin=Decimal("10"),
        max_fav=Decimal("145"),
        hold_bars=3,
        close=Decimal("118"),
        micro_context_available=True,
        rf_imbalance=Decimal("-0.06"),
        rf_close_pos=Decimal("0.50"),
    )

    assert live.should_exit is expected is True
    assert live.reason == reason == "RANGE_EXIT_NEXT_OPEN"
    assert Decimal(live.metadata["range_exit_peak_r"]) == meta["range_exit_peak_r"] == Decimal("4")
    assert Decimal(live.metadata["range_exit_current_r"]) == meta["range_exit_current_r"] == Decimal("1.3")
    assert Decimal(live.metadata["range_exit_giveback_frac"]) == meta["range_exit_giveback_frac"] == Decimal("0.675")


def test_v9e_range_exit_priority_between_opposite_and_max_hold() -> None:
    strategy = _started_strategy(Side.SHORT)
    strategy.position.entry_time_ms = 0
    channel = _bar_ready_context(side=Side.SHORT, close=Decimal("86"), high=Decimal("102"), low=Decimal("60"), exit_channel=True)
    opposite = _bar_ready_context(side=Side.SHORT, close=Decimal("86"), high=Decimal("102"), low=Decimal("60"), opposite=True)
    range_bar = _bar_ready_context(side=Side.SHORT, close=Decimal("86"), high=Decimal("102"), low=Decimal("60"))
    max_hold = _bar_ready_context(side=Side.SHORT, close=Decimal("86"), high=Decimal("102"), low=Decimal("60"), close_time_ms=200 * 4 * 60 * 60_000)

    assert strategy._close_decision_if_needed(channel).reason == "V8_CHANNEL_EXIT"  # type: ignore[union-attr]
    assert strategy._close_decision_if_needed(opposite).reason == "V8_OPPOSITE_SIGNAL_EXIT"  # type: ignore[union-attr]
    strategy.position.max_fav = Decimal("60")
    assert strategy._close_decision_if_needed(range_bar).reason == "RANGE_EXIT_NEXT_OPEN"  # type: ignore[union-attr]
    assert Strategy().config.strategy_id == "eth_lf_portfolio_v9e_range_exit_overlay"

    strategy.position.max_fav = Decimal("95")
    assert strategy._close_decision_if_needed(max_hold).reason == "V8_MAX_HOLD_EXIT"  # type: ignore[union-attr]


def _started_strategy(side: Side) -> Strategy:
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.exchange_equity["okx"] = Decimal("1000")
    stop = Decimal("90") if side is Side.LONG else Decimal("110")
    strategy.position.open_master(
        side=side,
        entry_time_ms=0,
        avg_entry=Decimal("100"),
        qty=Decimal("1"),
        stop_price=stop,
        entry_engine="MOMENTUM_V3",
        position_id=f"v9e-{side.name.lower()}",
    )
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("100"), base_qty=Decimal("1"))
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
    imbalance = Decimal("-0.06") if side is Side.LONG else Decimal("0.06")
    routed_side = (Side.SHORT if side is Side.LONG else Side.LONG) if opposite else side
    channel_key = "long_exit_channel" if side is Side.LONG else "short_exit_channel"
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
            close_pos=Decimal("0.50"),
        ),
        micro=MicroDecision(side, True, False, True, Decimal("1"), "CONTRA_RISK_REDUCED"),
        global_risk_scale=Decimal("1.3"),
        routed_signal=RoutedSignal(routed_side, "MOMENTUM_V3", 100),
        engine_features={"momentum": {"atr": Decimal("5"), channel_key: exit_channel}},
    )
