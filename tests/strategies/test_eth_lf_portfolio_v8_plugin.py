from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform import ExchangeName
from src.strategy.loader import load_strategy
from strategies.eth_lf_portfolio_v8.domain.models import ClosedKlineContext, Side
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


def test_v9c_router_respects_reclaim_first_priority() -> None:
    router = PortfolioRouter()
    selected = router.select(
        [
            EngineSignal(side=Side.LONG, engine="bull_reclaim_v2", priority=150),
            EngineSignal(side=Side.SHORT, engine="bear_v3_only", priority=50),
            EngineSignal(side=Side.LONG, engine="momentum_v3", priority=100),
        ]
    )

    assert selected.side is Side.LONG
    assert selected.engine == "bull_reclaim_v2"
    assert selected.priority == 150


def test_v9c_default_engine_priorities_are_reclaim_momentum_bear() -> None:
    assert BullReclaimV2Engine().priority == 150
    assert MomentumV3Engine().priority == 100
    assert BearV3OnlyEngine().priority == 50


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

from strategies.eth_lf_portfolio_v8.features.live_features import V8LiveFeatureBuilder
from strategies.eth_lf_portfolio_v8.engines.momentum_v3 import MomentumV3Engine
from strategies.eth_lf_portfolio_v8.engines.bear_v3 import BearV3OnlyEngine
from strategies.eth_lf_portfolio_v8.engines.bull_reclaim_v2 import BullReclaimV2Engine


def test_v8_live_feature_builder_truncates_future_bars() -> None:
    klines = _synthetic_klines(950)
    target_close = sorted(klines)[900]
    builder = V8LiveFeatureBuilder()

    with_future = builder.build_latest(klines, target_close_time_ms=target_close)
    without_future = builder.build_latest({k: v for k, v in klines.items() if k <= target_close}, target_close_time_ms=target_close)

    assert with_future.momentum == without_future.momentum
    assert with_future.bear == without_future.bear
    assert with_future.bull == without_future.bull


def test_v8_engines_emit_from_feature_rows() -> None:
    close_time_ms = 1_700_000_000_000
    kline = _ctx_kline(_closed_kline(close_time_ms))
    aggregate = _ctx(_range_aggregate(close_time_ms))
    micro = MicroContextEngine(MicroContextConfig(mode="soft")).evaluate(signal_side=Side.FLAT, aggregate=aggregate)
    context = BarReadyContext(
        kline=kline,
        range_aggregate=aggregate,
        micro=micro,
        global_risk_scale=Decimal("1.3"),
        engine_features={
            "momentum": {"signal": 1, "risk_mult": 1.2, "quality_mult": 0.5, "long_signal": True},
            "bear": {"signal": -1, "risk_mult": 1.1, "quality_mult": 1.3, "short_signal": True},
            "bull": {"signal": 1, "risk_mult": 0.8, "quality_mult": 1.0, "long_signal": True},
        },
    )

    momentum = MomentumV3Engine().evaluate(context)
    bear = BearV3OnlyEngine().evaluate(context)
    bull = BullReclaimV2Engine().evaluate(context)

    assert momentum is not None and momentum.side is Side.LONG and momentum.engine == "MOMENTUM_V3"
    assert momentum.risk_mult == Decimal("1.2")
    assert bear is not None and bear.side is Side.SHORT and bear.engine == "BEAR_V3_ONLY"
    assert bull is not None and bull.side is Side.LONG and bull.engine == "BULL_RECLAIM_V2"


def _synthetic_klines(count: int) -> dict[int, object]:
    out = {}
    start_open_ms = 1_600_000_000_000
    step = 4 * 60 * 60 * 1000
    price = Decimal("1000")
    for i in range(count):
        open_time = start_open_ms + i * step
        close_time = open_time + step
        drift = Decimal(i) * Decimal("0.4")
        open_price = price + drift
        close_price = open_price + Decimal("1")
        out[close_time] = ClosedKlineContext(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            timeframe="4h",
            open_time_ms=open_time,
            close_time_ms=close_time,
            open=open_price,
            high=close_price + Decimal("5"),
            low=open_price - Decimal("5"),
            close=close_price,
            volume=Decimal("1000") + Decimal(i),
            quote_volume=None,
        )
    return out


def _ctx_kline(event: MarketFeatureEvent):
    from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_closed_kline

    return parse_closed_kline(event)

from src.platform.exchanges.models import Balance, LeverageInfo, MarginMode, PositionMode
from src.platform.snapshot import PlatformSnapshot


class _FakeFeatureBuilder:
    def __init__(self, *, atr="10", exit_long=False):
        self.atr = atr
        self.exit_long = exit_long

    def build_latest(self, klines, *, target_close_time_ms):
        from strategies.eth_lf_portfolio_v8.features.live_features import V8EngineFeatureRows

        return V8EngineFeatureRows(
            momentum={"atr": self.atr, "long_exit_channel": self.exit_long, "short_exit_channel": False},
            bear={"atr": self.atr, "short_exit_channel": False},
            bull={"atr": self.atr, "long_exit_channel": self.exit_long},
        )


@pytest.mark.asyncio
async def test_strategy_emits_live_open_signal_from_routed_engine() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="MOMENTUM_V3", priority=150, side=Side.LONG),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10")
    close_time_ms = 1_700_000_000_000

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms, imbalance="0.1", close_pos="0.8"))

    assert len(signals) == 1
    signal = signals[0]
    assert signal.action is SignalAction.OPEN_LONG
    assert signal.quantity is not None and signal.quantity > 0
    assert signal.metadata["engine"] == "MOMENTUM_V3"
    assert signal.metadata["await_master_fill_before_stop"] is True
    assert strategy.pending_entry is not None


@pytest.mark.asyncio
async def test_strategy_places_master_and_follower_stop_after_fills() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="MOMENTUM_V3", priority=150, side=Side.LONG),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10")
    close_time_ms = 1_700_000_000_000
    await strategy.on_market_feature(_closed_kline(close_time_ms))
    await strategy.on_market_feature(_range_aggregate(close_time_ms, imbalance="0.1", close_pos="0.8"))

    master_stop = await strategy.on_account_event(
        AccountEvent(
            exchange=ExchangeName.OKX,
            event_type=AccountEventType.ORDER,
            symbol="ETH-USDT-PERP",
            event_time_ms=close_time_ms + 1,
            order_status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            price=Decimal("2000"),
            filled_quantity=Decimal("0.1"),
        )
    )
    assert len(master_stop) == 2
    assert master_stop[0].action is SignalAction.CANCEL_ALL_STOP_ORDERS
    assert master_stop[0].metadata["target_exchanges"] == ["okx"]
    assert master_stop[1].action is SignalAction.PLACE_STOP_LOSS_LONG
    assert master_stop[1].trigger_price == Decimal("1978.0")
    assert master_stop[1].metadata["target_exchanges"] == ["okx"]

    follower_stop = await strategy.on_account_event(
        AccountEvent(
            exchange=ExchangeName.BINANCE,
            event_type=AccountEventType.ORDER,
            symbol="ETH-USDT-PERP",
            event_time_ms=close_time_ms + 2,
            order_status=OrderStatus.FILLED,
            side=OrderSide.BUY,
            price=Decimal("2001"),
            filled_quantity=Decimal("0.1"),
        )
    )
    assert len(follower_stop) == 2
    assert follower_stop[0].action is SignalAction.CANCEL_ALL_STOP_ORDERS
    assert follower_stop[0].metadata["target_exchanges"] == ["binance"]
    assert follower_stop[1].trigger_price == Decimal("1978.0")
    assert follower_stop[1].metadata["target_exchanges"] == ["binance"]


@pytest.mark.asyncio
async def test_strategy_emits_close_signal_for_active_position_on_exit_channel() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2000"),
        qty=Decimal("0.1"),
        stop_price=Decimal("1978"),
        entry_engine="MOMENTUM_V3",
    )
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="none", priority=0, side=Side.FLAT),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10", exit_long=True)
    close_time_ms = 1_700_100_000_000

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))

    assert len(signals) == 1
    assert signals[0].action is SignalAction.CLOSE_LONG
    assert signals[0].metadata["reduce_only"] is True


@pytest.mark.asyncio
async def test_strategy_emits_add_signal_when_next_r_trigger_is_hit() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    close_time_ms = 1_700_000_000_000
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=close_time_ms - 4 * 60 * 60 * 1000,
        avg_entry=Decimal("100"),
        qty=Decimal("0.1"),
        stop_price=Decimal("90"),
        entry_engine="MOMENTUM_V3",
    )
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="none", priority=0, side=Side.FLAT),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10")

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))

    assert len(signals) == 1
    assert signals[0].action is SignalAction.OPEN_LONG
    assert signals[0].metadata["decision_type"] == "add"
    assert signals[0].metadata["micro_entry_risk_scale_applied"] is False
    assert strategy.pending_entry is not None and strategy.pending_entry.is_add is True


@pytest.mark.asyncio
async def test_strategy_updates_protected_stop_when_favorable_move_reaches_threshold() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    close_time_ms = 1_700_000_000_000
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=close_time_ms - 4 * 60 * 60 * 1000,
        avg_entry=Decimal("100"),
        qty=Decimal("0.1"),
        stop_price=Decimal("90"),
        entry_engine="MOMENTUM_V3",
        units=4,
    )
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("100"), base_qty=Decimal("0.1"))
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="none", priority=0, side=Side.FLAT),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10")

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))

    assert len(signals) == 2
    assert signals[0].action is SignalAction.CANCEL_ALL_STOP_ORDERS
    assert signals[0].metadata["target_exchanges"] == ["okx"]
    assert signals[1].action is SignalAction.PLACE_STOP_LOSS_LONG
    assert signals[1].trigger_price == Decimal("101.0")
    assert strategy.position.stop_price == Decimal("101.0")


@pytest.mark.asyncio
async def test_strategy_emits_close_signal_when_max_hold_is_reached() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    close_time_ms = 1_700_000_000_000
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=close_time_ms - 181 * 4 * 60 * 60 * 1000,
        avg_entry=Decimal("2000"),
        qty=Decimal("0.1"),
        stop_price=Decimal("1978"),
        entry_engine="MOMENTUM_V3",
    )
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="none", priority=0, side=Side.FLAT),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10")

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))

    assert len(signals) == 1
    assert signals[0].action is SignalAction.CLOSE_LONG
    assert signals[0].reason == "V8_MAX_HOLD_EXIT"


@pytest.mark.asyncio
async def test_strategy_respects_cooldown_after_master_exit() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    close_time_ms = 1_700_000_000_000
    strategy.position.last_exit_time_ms = close_time_ms - 4 * 60 * 60 * 1000
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="MOMENTUM_V3", priority=150, side=Side.LONG),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10")

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms, imbalance="0.1", close_pos="0.8"))

    assert signals == []
    assert strategy.pending_entry is None

def _snapshot() -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol="ETH-USDT-PERP",
        balance=Balance(exchange=ExchangeName.OKX, asset="USDT", total=Decimal("1000"), available=Decimal("1000")),
        positions=[],
        open_orders=[],
        open_stop_orders=[],
        leverage=LeverageInfo(exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP", leverage=Decimal("10"), margin_mode=MarginMode.ISOLATED),
        position_mode=PositionMode.ONE_WAY,
    )


@pytest.mark.asyncio
async def test_strategy_close_signal_uses_open_leg_quantities_by_exchange() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2000"),
        qty=Decimal("0.10"),
        stop_price=Decimal("1978"),
        entry_engine="MOMENTUM_V3",
    )
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.10"))
    strategy.position.mark_leg_open(exchange="binance", avg_fill_price=Decimal("2001"), base_qty=Decimal("0.005"))
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="none", priority=0, side=Side.FLAT),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10", exit_long=True)
    close_time_ms = 1_700_100_000_000

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))

    assert len(signals) == 1
    assert signals[0].action is SignalAction.CLOSE_LONG
    assert signals[0].metadata["target_exchanges"] == ["binance", "okx"]
    assert signals[0].metadata["exchange_quantities_base"] == {"okx": "0.10", "binance": "0.005"}
    assert signals[0].quantity == Decimal("0.10")


@pytest.mark.asyncio
async def test_strategy_protected_stop_update_uses_open_leg_quantities_by_exchange() -> None:
    strategy = Strategy()
    await strategy.on_start(_snapshot())
    close_time_ms = 1_700_000_000_000
    strategy.position.open_master(
        side=Side.LONG,
        entry_time_ms=close_time_ms - 4 * 60 * 60 * 1000,
        avg_entry=Decimal("100"),
        qty=Decimal("0.10"),
        stop_price=Decimal("90"),
        entry_engine="MOMENTUM_V3",
        units=4,
    )
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("100"), base_qty=Decimal("0.10"))
    strategy.position.mark_leg_open(exchange="binance", avg_fill_price=Decimal("100"), base_qty=Decimal("0.005"))
    strategy.router = PortfolioRouter(engines=(_StaticEngine(name="none", priority=0, side=Side.FLAT),))
    strategy.feature_builder = _FakeFeatureBuilder(atr="10")

    await strategy.on_market_feature(_closed_kline(close_time_ms))
    signals = await strategy.on_market_feature(_range_aggregate(close_time_ms))

    assert len(signals) == 2
    assert signals[0].action is SignalAction.CANCEL_ALL_STOP_ORDERS
    assert signals[0].metadata["target_exchanges"] == ["binance", "okx"]
    assert signals[1].action is SignalAction.PLACE_STOP_LOSS_LONG
    assert signals[1].metadata["target_exchanges"] == ["binance", "okx"]
    assert signals[1].metadata["exchange_quantities_base"] == {"okx": "0.10", "binance": "0.005"}
    assert signals[1].quantity == Decimal("0.10")
