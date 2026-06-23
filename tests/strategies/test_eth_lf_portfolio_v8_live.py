from __future__ import annotations

from decimal import Decimal

from src.app import AppConfig, AppContext, AsyncAlertDispatcher, NoopAlertSink
from src.platform import ExchangeName
from src.planner import ExecutionPlanner
from src.runtime import LiveRuntimeConfig, LiveRuntimeRunner, RuntimeMode
from src.runtime.tasks import ClosedBarScheduler
from src.signals import SignalAction, TradeSignal
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, ClosedKlineContext, MicroDecision, RangeAggregateContext, RoutedSignal, Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy
from strategies.eth_lf_portfolio_v8.strategy import _default_engine_execution_params


H4 = 4 * 60 * 60_000


class _FakeData:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    async def fetch_klines(self, **kwargs):
        return []

    async def stream_trades(self):
        if False:
            yield None

    async def stream_order_book(self):
        if False:
            yield None


class _FakeStateStore:
    def list_open_orders(self, **kwargs):
        return []

    def save_snapshot(self, snapshot):
        pass


def test_v9c_live_momentum_execution_params_match_coinbacktest_turbo():
    params = _default_engine_execution_params()["MOMENTUM_V3"]
    assert params.initial_atr_mult == Decimal("2.2")
    assert params.trailing_atr_mult == Decimal("4.0")
    assert params.unit_risk_per_trade == Decimal("0.032")
    assert params.max_total_notional_mult == Decimal("12.0")
    assert params.max_units == 4
    assert params.add_every_r == Decimal("1.0")
    assert params.max_hold_bars == 180
    assert params.cooldown_bars == 4


def test_v9c_live_bear_and_bull_execution_params_unchanged():
    params = _default_engine_execution_params()

    bear = params["BEAR_V3_ONLY"]
    assert bear.initial_atr_mult == Decimal("2.5")
    assert bear.trailing_atr_mult == Decimal("4.5")
    assert bear.unit_risk_per_trade == Decimal("0.022")
    assert bear.max_total_notional_mult == Decimal("11.0")
    assert bear.max_units == 5
    assert bear.add_every_r == Decimal("1.0")
    assert bear.max_hold_bars == 360
    assert bear.cooldown_bars == 8

    bull = params["BULL_RECLAIM_V2"]
    assert bull.initial_atr_mult == Decimal("2.2")
    assert bull.trailing_atr_mult == Decimal("3.5")
    assert bull.unit_risk_per_trade == Decimal("0.020")
    assert bull.max_total_notional_mult == Decimal("8.0")
    assert bull.max_units == 3
    assert bull.add_every_r == Decimal("1.2")
    assert bull.max_hold_bars == 90
    assert bull.cooldown_bars == 4


def test_v9c_live_stop_update_long_uses_better_of_atr_trailing_and_protected(monkeypatch):
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=0,
        avg_entry=Decimal("100"),
        qty=Decimal("1"),
        stop_price=Decimal("90"),
        entry_engine="MOMENTUM_V3",
        entry_risk_mult=Decimal("1"),
        position_id="test-long",
    )
    strategy.position.first_entry = Decimal("100")
    strategy.position.risk_per_coin = Decimal("10")
    strategy.position.max_fav = Decimal("120")
    monkeypatch.setattr(
        "strategies.eth_lf_portfolio_v8.strategy.protected_stop",
        lambda **kwargs: Decimal("95"),
    )

    signals = strategy._stop_update_signals_if_needed(
        _bar_ready_context(close=Decimal("120"), engine_features={"momentum": {"atr": Decimal("5")}})
    )

    assert len(signals) == 2
    assert any(s.action is SignalAction.CANCEL_ALL_STOP_ORDERS for s in signals)
    assert any(s.action is SignalAction.PLACE_STOP_LOSS_LONG for s in signals)
    place = next(s for s in signals if s.action is SignalAction.PLACE_STOP_LOSS_LONG)
    assert place.trigger_price == Decimal("100")
    assert place.reason == "V8_PROTECTED_TRAILING_STOP_UPDATE"


def test_v9c_live_stop_update_short_uses_better_of_atr_trailing_and_protected(monkeypatch):
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=0,
        avg_entry=Decimal("100"),
        qty=Decimal("1"),
        stop_price=Decimal("110"),
        entry_engine="MOMENTUM_V3",
        entry_risk_mult=Decimal("1"),
        position_id="test-short",
    )
    strategy.position.first_entry = Decimal("100")
    strategy.position.risk_per_coin = Decimal("10")
    strategy.position.max_fav = Decimal("80")
    monkeypatch.setattr(
        "strategies.eth_lf_portfolio_v8.strategy.protected_stop",
        lambda **kwargs: Decimal("105"),
    )

    signals = strategy._stop_update_signals_if_needed(
        _bar_ready_context(close=Decimal("80"), engine_features={"momentum": {"atr": Decimal("5")}})
    )

    assert len(signals) == 2
    assert any(s.action is SignalAction.CANCEL_ALL_STOP_ORDERS for s in signals)
    assert any(s.action is SignalAction.PLACE_STOP_LOSS_SHORT for s in signals)
    place = next(s for s in signals if s.action is SignalAction.PLACE_STOP_LOSS_SHORT)
    assert place.trigger_price == Decimal("100")
    assert place.reason == "V8_PROTECTED_TRAILING_STOP_UPDATE"


def test_v9c_live_stop_update_falls_back_to_protected_when_atr_missing():
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=0,
        avg_entry=Decimal("100"),
        qty=Decimal("1"),
        stop_price=Decimal("90"),
        entry_engine="MOMENTUM_V3",
        entry_risk_mult=Decimal("1"),
        position_id="test-long-protected",
    )
    strategy.position.first_entry = Decimal("100")
    strategy.position.risk_per_coin = Decimal("10")
    strategy.position.max_fav = Decimal("120")

    signals = strategy._stop_update_signals_if_needed(
        _bar_ready_context(close=Decimal("120"), engine_features={"momentum": {}})
    )

    assert len(signals) == 2
    place = next(s for s in signals if s.action is SignalAction.PLACE_STOP_LOSS_LONG)
    assert place.trigger_price == Decimal("107")
    assert place.reason == "V8_PROTECTED_TRAILING_STOP_UPDATE"


def test_v9c_strategy_config_min_range_bars_is_read_by_runner():
    """Real V9C Strategy.config.micro_context.min_range_bars (object path)
    must be read correctly by runner._get_min_range_bars() and return 5."""

    strategy = Strategy()  # uses default config.json → min_range_bars=5

    cfg = AppConfig(
        symbol="ETH-USDT-PERP",
        exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
        data_exchange=ExchangeName.OKX,
        strategy="strategies.eth_lf_portfolio_v8.strategy:Strategy",
        data_streams=(),
        state_db_path="unused.sqlite3",
        market_queue_maxsize=20,
        signal_queue_maxsize=20,
        alert_queue_maxsize=20,
        dry_run=True,
        enable_email_alerts=False,
    )

    context = AppContext(
        data=_FakeData(),
        execution=object(),
        state_store=_FakeStateStore(),
        strategy=strategy,
        planner=ExecutionPlanner(),
        alerts=AsyncAlertDispatcher(NoopAlertSink()),
    )

    runtime_config = LiveRuntimeConfig(
        app=cfg,
        mode=RuntimeMode.LIVE_RUNTIME,
        closed_bar_buffer_ms=60_000,
    )

    runner = LiveRuntimeRunner(
        app_config=cfg,
        app_context=context,
        runtime_config=runtime_config,
    )

    # Real V9C config is a V8Config dataclass with micro_context.min_range_bars=5.
    assert runner._get_min_range_bars() == 5, (
        f"Expected _get_min_range_bars() == 5 for real V9C Strategy, "
        f"got {runner._get_min_range_bars()}"
    )


def test_v9c_strategy_builds_last_decision_audit_for_no_signal():
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    context = _bar_ready_context(
        close=Decimal("100"),
        engine_features={},
        routed_signal=RoutedSignal.flat(),
    )

    audit = strategy._build_decision_audit(context, [])

    assert audit["signal_count"] == 0
    assert audit["reason"] in {"flat_route", "micro_blocked", "no_signal"}
    assert "range_available" in audit
    assert "range_bar_count" in audit
    assert "range_imbalance" in audit
    assert "range_close_pos" in audit
    assert "micro_entry_risk_scale" in audit


def test_v9c_strategy_builds_last_decision_audit_for_open_signal():
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    context = _bar_ready_context(
        close=Decimal("100"),
        engine_features={},
        routed_signal=RoutedSignal(
            side=Side.LONG,
            engine="BULL_RECLAIM_V2",
            priority=10,
            risk_mult=Decimal("1.2"),
            quality_mult=Decimal("0.8"),
        ),
    )
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.1"),
    )

    audit = strategy._build_decision_audit(context, [signal])

    assert audit["reason"] == "entry_signal"
    assert "open_long" in audit["actions"]
    assert audit["selected_engine"]
    assert audit["selected_side"] == "long"


def test_v9c_strategy_decision_audit_includes_range_bar_fields():
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    aggregate = _range_aggregate(bar_count=37)
    context = _bar_ready_context(
        close=Decimal("101"),
        engine_features={},
        range_aggregate=aggregate,
        micro=MicroDecision(
            signal_side=Side.LONG,
            context_available=True,
            aligned=True,
            contra=False,
            entry_risk_scale=Decimal("1"),
            action="allow",
        ),
        routed_signal=RoutedSignal(
            side=Side.LONG,
            engine="BULL_RECLAIM_V2",
            priority=10,
        ),
    )

    audit = strategy._build_decision_audit(context, [])

    assert audit["range_available"] is True
    assert audit["range_bar_count"] == 37
    assert audit["range_imbalance"] is not None
    assert audit["range_taker_buy_ratio"] is not None
    assert audit["range_close_pos"] is not None
    assert audit["range_micro_return_pct"] is not None


def _bar_ready_context(
    *,
    close: Decimal,
    engine_features: dict[str, dict[str, Decimal]],
    range_aggregate: RangeAggregateContext | None = None,
    micro: MicroDecision | None = None,
    routed_signal: RoutedSignal | None = None,
) -> BarReadyContext:
    return BarReadyContext(
        kline=ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=0,
            close_time_ms=H4,
            open=close,
            high=close,
            low=close,
            close=close,
            volume=Decimal("1"),
        ),
        range_aggregate=range_aggregate,
        micro=micro or MicroDecision(
            signal_side=Side.FLAT,
            context_available=False,
            aligned=False,
            contra=False,
            entry_risk_scale=Decimal("1"),
            action="skip",
        ),
        global_risk_scale=Decimal("1"),
        routed_signal=routed_signal or RoutedSignal.flat(),
        engine_features=engine_features,
    )


def _range_aggregate(*, bar_count: int) -> RangeAggregateContext:
    return RangeAggregateContext(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        timeframe="4h",
        bucket_start_ms=0,
        bucket_end_ms=H4,
        range_pct=Decimal("0.002"),
        bar_count=bar_count,
        first_open=Decimal("100"),
        last_close=Decimal("101"),
        high=Decimal("102"),
        low=Decimal("99"),
        buy_notional_sum=Decimal("560"),
        sell_notional_sum=Decimal("440"),
        delta_notional_sum=Decimal("120"),
        notional_sum=Decimal("1000"),
        micro_return_pct=Decimal("0.01"),
        imbalance=Decimal("0.12"),
        taker_buy_ratio=Decimal("0.56"),
        close_pos=Decimal("0.6666666667"),
    )
