from __future__ import annotations

import json
from decimal import Decimal

import pytest

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform import ExchangeName
from src.strategy.loader import load_strategy
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.features.micro_context import MicroContextConfig, MicroContextEngine
from strategies.eth_lf_portfolio_v8.strategy import Strategy


def _closed_kline(close_time_ms: int) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.CLOSED_KLINE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=close_time_ms,
        data={
            "open_time_ms": close_time_ms - 4 * 60 * 60 * 1000,
            "close_time_ms": close_time_ms,
            "open": "100",
            "high": "110",
            "low": "95",
            "close": "108",
            "volume": "1000",
            "quote_volume": "100000",
            "is_closed": True,
        },
    )


def _range_aggregate(close_time_ms: int, *, imbalance: str = "0.1", close_pos: str = "0.8") -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.RANGE_AGGREGATE,
        symbol="ETH-USDT-PERP",
        exchange=ExchangeName.OKX,
        timeframe="4h",
        event_time_ms=close_time_ms,
        data={
            "range_pct": "0.002",
            "bucket_start_ms": close_time_ms - 4 * 60 * 60 * 1000,
            "bucket_end_ms": close_time_ms,
            "bar_count": 8,
            "first_open": "100",
            "last_close": "108",
            "high": "110",
            "low": "95",
            "buy_notional_sum": "60000",
            "sell_notional_sum": "40000",
            "delta_notional_sum": "20000",
            "notional_sum": "100000",
            "micro_return_pct": "0.08",
            "imbalance": imbalance,
            "taker_buy_ratio": "0.6",
            "close_pos": close_pos,
        },
    )


def test_v8_strategy_loads_through_standard_loader() -> None:
    strategy = load_strategy("strategies.eth_lf_portfolio_v8:Strategy")

    assert isinstance(strategy, Strategy)


def test_v8_runtime_requirements_do_not_subscribe_order_book() -> None:
    strategy = Strategy()
    req = strategy.runtime_requirements()

    assert req["closed_kline"]["enabled"] is True
    assert req["closed_kline"]["interval"] == "4h"
    assert req["trades"]["stream_enabled"] is True
    assert req["range_bars"]["range_pct"] == "0.002"
    assert req["order_book"]["enabled"] is False
    assert req["private_account_stream"]["enabled"] is True


@pytest.mark.asyncio
async def test_v8_buffers_closed_kline_until_range_aggregate_arrives() -> None:
    strategy = Strategy()
    close_time_ms = 1_700_000_000_000

    signals = await strategy.on_market_feature(_closed_kline(close_time_ms))
    assert signals == []
    assert strategy.bar_ready_events == []

    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))
    assert signals == []
    assert len(strategy.bar_ready_events) == 1
    ready = strategy.bar_ready_events[0]
    assert ready.kline.close_time_ms == close_time_ms
    assert ready.range_aggregate is not None
    assert ready.final_entry_risk_scale == Decimal("1.3")


@pytest.mark.asyncio
async def test_v8_ignores_orderbook_by_runtime_requirement_but_keeps_interface() -> None:
    strategy = Strategy()
    assert await strategy.on_order_book(object()) == []  # type: ignore[arg-type]


def test_micro_context_soft_long_aligned_and_contra() -> None:
    engine = MicroContextEngine(MicroContextConfig(mode="soft"))
    close_time_ms = 1_700_000_000_000
    aligned = engine.evaluate(signal_side=Side.LONG, aggregate=_ctx(_range_aggregate(close_time_ms, imbalance="0.1", close_pos="0.8")))
    contra = engine.evaluate(signal_side=Side.LONG, aggregate=_ctx(_range_aggregate(close_time_ms, imbalance="-0.1", close_pos="0.2")))

    assert aligned.aligned is True
    assert aligned.entry_risk_scale == Decimal("1")
    assert contra.contra is True
    assert contra.entry_risk_scale == Decimal("0.50")


def test_micro_context_strict_blocks_not_aligned() -> None:
    engine = MicroContextEngine(MicroContextConfig(mode="strict"))
    close_time_ms = 1_700_000_000_000
    decision = engine.evaluate(signal_side=Side.SHORT, aggregate=_ctx(_range_aggregate(close_time_ms, imbalance="0", close_pos="0.5")))

    assert decision.context_available is True
    assert decision.entry_risk_scale == Decimal("0")
    assert decision.action == "NOT_ALIGNED_BLOCKED"


def _ctx(event: MarketFeatureEvent):
    from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_range_aggregate

    return parse_range_aggregate(event)

from dataclasses import dataclass
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import OrderSide, OrderStatus
from src.signals import SignalAction
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, EngineSignal, RoutedSignal, V8DecisionType, V8TradeDecision
from strategies.eth_lf_portfolio_v8.domain.position_state import V8PositionState
from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter
from strategies.eth_lf_portfolio_v8.execution.signal_mapper import SignalMapperConfig, V8SignalMapper
from strategies.eth_lf_portfolio_v8.persistence.state_store import JsonV8StateStore


@dataclass(frozen=True)
class _StaticEngine:
    name: str
    priority: int
    side: Side

    def evaluate(self, context: BarReadyContext):
        if self.side is Side.FLAT:
            return None
        return EngineSignal(side=self.side, engine=self.name, priority=self.priority, reason=self.name)


def test_v8_router_respects_engine_priority() -> None:
    router = PortfolioRouter()
    selected = router.select(
        [
            EngineSignal(side=Side.LONG, engine="bull_reclaim_v2", priority=50),
            EngineSignal(side=Side.SHORT, engine="bear_v3_only", priority=90),
            EngineSignal(side=Side.LONG, engine="momentum_v3", priority=150),
        ]
    )

    assert selected.side is Side.LONG
    assert selected.engine == "momentum_v3"
    assert selected.priority == 150


def test_v8_signal_mapper_maps_open_close_and_stop() -> None:
    mapper = V8SignalMapper(SignalMapperConfig(strategy_id="v8", target_exchanges=("okx", "binance")))
    open_signal = mapper.map_decision(
        V8TradeDecision(
            decision_type=V8DecisionType.OPEN,
            side=Side.LONG,
            symbol="ETH-USDT-PERP",
            quantity=Decimal("0.12"),
            engine="momentum_v3",
            bar_close_time_ms=1_700_000_000_000,
            entry_risk_scale=Decimal("0.65"),
        )
    )[0]
    stop_signal = mapper.map_decision(
        V8TradeDecision(
            decision_type=V8DecisionType.PLACE_STOP,
            side=Side.LONG,
            symbol="ETH-USDT-PERP",
            quantity=Decimal("0.12"),
            stop_price=Decimal("1650"),
            engine="momentum_v3",
        )
    )[0]
    close_signal = mapper.map_decision(
        V8TradeDecision(
            decision_type=V8DecisionType.CLOSE,
            side=Side.LONG,
            symbol="ETH-USDT-PERP",
            quantity=Decimal("0.12"),
            engine="momentum_v3",
        )
    )[0]

    assert open_signal.action is SignalAction.OPEN_LONG
    assert open_signal.quantity == Decimal("0.12")
    assert open_signal.metadata["target_exchanges"] == ["okx", "binance"]
    assert open_signal.metadata["entry_risk_scale"] == "0.65"
    assert stop_signal.action is SignalAction.PLACE_STOP_LOSS_LONG
    assert stop_signal.trigger_price == Decimal("1650")
    assert close_signal.action is SignalAction.CLOSE_LONG
    assert close_signal.metadata["reduce_only"] is True


def test_v8_position_state_tracks_master_and_follower_legs() -> None:
    state = V8PositionState()
    state.open_master(
        side=Side.LONG,
        entry_time_ms=1_700_000_000_000,
        avg_entry=Decimal("1730"),
        qty=Decimal("0.12"),
        stop_price=Decimal("1640"),
        entry_engine="momentum_v3",
        entry_risk_mult=Decimal("1.3"),
    )
    state.mark_leg_open(exchange="okx", avg_fill_price=Decimal("1730"), base_qty=Decimal("0.12"), native_qty=Decimal("1.2"))
    state.mark_leg_open(exchange="binance", avg_fill_price=Decimal("1731"), base_qty=Decimal("0.12"), native_qty=Decimal("0.12"))

    assert state.in_pos is True
    assert state.risk_per_coin == Decimal("90")
    assert set(state.open_legs) == {"okx", "binance"}

    state.mark_leg_closed(exchange="binance", sync_status="follower_closed_early")
    assert state.in_pos is True
    assert state.legs["binance"].sync_status == "follower_closed_early"
    assert set(state.open_legs) == {"okx"}


def test_v8_position_state_applies_master_account_event_without_following_follower() -> None:
    state = V8PositionState()
    state.apply_account_event(
        AccountEvent(
            exchange=ExchangeName.BINANCE,
            event_type=AccountEventType.ORDER,
            symbol="ETH-USDT-PERP",
            order_status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            price=Decimal("1731"),
            filled_quantity=Decimal("0.12"),
        ),
        master_exchange="okx",
    )
    assert state.in_pos is False
    assert state.legs["binance"].is_open is True

    state.apply_account_event(
        AccountEvent(
            exchange=ExchangeName.OKX,
            event_type=AccountEventType.ORDER,
            symbol="ETH-USDT-PERP",
            order_status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            price=Decimal("1730"),
            filled_quantity=Decimal("0.12"),
        ),
        master_exchange="okx",
    )
    assert state.in_pos is True
    assert state.avg_entry == Decimal("1730")

    state.apply_account_event(
        AccountEvent(
            exchange=ExchangeName.OKX,
            event_type=AccountEventType.ORDER,
            symbol="ETH-USDT-PERP",
            order_status=OrderStatus.FILLED,
            side=OrderSide.SELL,
            price=Decimal("1720"),
            filled_quantity=Decimal("0.12"),
        ),
        master_exchange="okx",
    )
    assert state.in_pos is False
    assert state.legs == {}


def test_v8_state_store_roundtrips_position_state(tmp_path) -> None:
    store = JsonV8StateStore(tmp_path / "v8_state.json")
    state = V8PositionState()
    state.open_master(
        side=Side.SHORT,
        entry_time_ms=1,
        avg_entry=Decimal("1800"),
        qty=Decimal("0.2"),
        stop_price=Decimal("1880"),
        entry_engine="bear_v3_only",
    )
    state.mark_leg_open(exchange="okx", avg_fill_price=Decimal("1800"), base_qty=Decimal("0.2"), native_qty=Decimal("2"))

    store.save(state)
    loaded = store.load()

    assert loaded.in_pos is True
    assert loaded.side is Side.SHORT
    assert loaded.avg_entry == Decimal("1800")
    assert loaded.legs["okx"].native_qty == Decimal("2")


@pytest.mark.asyncio
async def test_strategy_router_flow_records_routed_signal_without_emitting_orders() -> None:
    strategy = Strategy()
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="momentum_v3", priority=150, side=Side.LONG),))
    close_time_ms = 1_700_000_000_000

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms, imbalance="-0.1", close_pos="0.2"))

    assert signals == []
    ready = strategy.bar_ready_events[-1]
    assert ready.routed_signal.engine == "momentum_v3"
    assert ready.micro.signal_side is Side.LONG
    assert ready.micro.contra is True
    assert ready.final_entry_risk_scale == Decimal("0.650")

from strategies.eth_lf_portfolio_v8.persistence.parity_audit import ReadonlyParityChecker, SignalAuditReference


def _write_reference_csv(path, *, timestamp="2023-11-14 18:13:20", signal=0, selected_engine="NONE", priority=0, micro_action="NO_SIGNAL") -> None:
    path.write_text(
        "timestamp,signal,selected_engine,selected_priority,micro_context_available,micro_aligned,micro_contra,micro_entry_risk_scale,final_entry_risk_scale,micro_filter_action\n"
        f"{timestamp},{signal},{selected_engine},{priority},False,False,False,1,1.3,{micro_action}\n",
        encoding="utf-8",
    )


def test_readonly_parity_checker_matches_reference_row(tmp_path) -> None:
    reference_path = tmp_path / "signal_audit.csv"
    _write_reference_csv(reference_path)
    reference = SignalAuditReference.from_csv(reference_path)
    checker = ReadonlyParityChecker(reference, timestamp_key="open_time_ms")
    close_time_ms = 1_700_000_000_000
    kline = _ctx_kline(_closed_kline(close_time_ms))
    aggregate = _ctx(_range_aggregate(close_time_ms))
    micro = MicroContextEngine(MicroContextConfig(mode="soft")).evaluate(signal_side=Side.FLAT, aggregate=aggregate)
    comparison = checker.compare(BarReadyContext(kline=kline, range_aggregate=aggregate, micro=micro, global_risk_scale=Decimal("1.3")))

    assert comparison.matched is True
    assert checker.mismatch_count == 0


def test_readonly_parity_checker_reports_mismatch(tmp_path) -> None:
    reference_path = tmp_path / "signal_audit.csv"
    _write_reference_csv(reference_path, signal=1, selected_engine="MOMENTUM_V3", priority=150)
    reference = SignalAuditReference.from_csv(reference_path)
    checker = ReadonlyParityChecker(reference, timestamp_key="open_time_ms")
    close_time_ms = 1_700_000_000_000
    kline = _ctx_kline(_closed_kline(close_time_ms))
    aggregate = _ctx(_range_aggregate(close_time_ms))
    micro = MicroContextEngine(MicroContextConfig(mode="soft")).evaluate(signal_side=Side.FLAT, aggregate=aggregate)
    comparison = checker.compare(BarReadyContext(kline=kline, range_aggregate=aggregate, micro=micro, global_risk_scale=Decimal("1.3")))

    assert comparison.matched is False
    assert "signal" in comparison.mismatches
    assert "selected_engine" in comparison.mismatches


@pytest.mark.asyncio
async def test_strategy_readonly_parity_mode_records_comparison(tmp_path) -> None:
    reference_path = tmp_path / "signal_audit.csv"
    _write_reference_csv(reference_path)
    config_path = tmp_path / "config.json"
    base = Strategy().config
    # Keep a minimal config file so the plugin is exercised through normal config parsing.
    config_path.write_text(
        json.dumps(
            {
                "strategy_id": base.strategy_id,
                "symbol": base.symbol,
                "runtime_requirements": base.runtime_requirements,
                "micro_context": {"mode": "soft"},
                "risk": {"global_risk_scale": "1.3"},
                "readonly_parity": {
                    "enabled": True,
                    "reference_signal_audit_csv": str(reference_path),
                    "timestamp_key": "open_time_ms",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    strategy = Strategy(config_path=config_path)
    close_time_ms = 1_700_000_000_000

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))

    assert signals == []
    assert len(strategy.parity_results) == 1
    assert strategy.parity_results[0].matched is True


def _ctx_kline(event: MarketFeatureEvent):
    from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_closed_kline

    return parse_closed_kline(event)
