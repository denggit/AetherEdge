from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot
from src.reconcile.models import ReconcileReport
from src.signals import TradeSignal
from src.strategy import StrategyRecoveryContext
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, Side
from strategies.eth_lf_portfolio_v8.engines.bear_v3 import BearV3OnlyEngine
from strategies.eth_lf_portfolio_v8.engines.bull_reclaim_v2 import BullReclaimV2Engine
from strategies.eth_lf_portfolio_v8.engines.momentum_v3 import MomentumV3Engine
from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter
from strategies.eth_lf_portfolio_v8.execution.signal_mapper import SignalMapperConfig, V8SignalMapper
from strategies.eth_lf_portfolio_v8.domain.position_state import V8PositionState
from strategies.eth_lf_portfolio_v8.features.buffer import V8FeatureBuffer
from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_closed_kline, parse_range_aggregate
from strategies.eth_lf_portfolio_v8.features.micro_context import MicroContextConfig, MicroContextEngine


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"


@dataclass(frozen=True)
class V8Config:
    strategy_id: str
    symbol: str
    runtime_requirements: Mapping[str, Any]
    micro_context: MicroContextConfig
    global_risk_scale: Decimal

    @classmethod
    def from_file(cls, path: str | Path = DEFAULT_CONFIG_PATH) -> "V8Config":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        micro = data.get("micro_context", {})
        return cls(
            strategy_id=str(data.get("strategy_id", "eth_lf_portfolio_v8")),
            symbol=str(data.get("symbol", "ETH-USDT-PERP")),
            runtime_requirements=data.get("runtime_requirements", {}),
            micro_context=MicroContextConfig(
                mode=str(micro.get("mode", "soft")),
                min_range_bars=int(micro.get("min_range_bars", 5)),
                contra_imbalance=Decimal(str(micro.get("contra_imbalance", "0.05"))),
                aligned_imbalance=Decimal(str(micro.get("aligned_imbalance", "0.05"))),
                bad_close_pos=Decimal(str(micro.get("bad_close_pos", "0.35"))),
                good_close_pos=Decimal(str(micro.get("good_close_pos", "0.65"))),
                contra_risk_scale=Decimal(str(micro.get("contra_risk_scale", "0.50"))),
                not_aligned_risk_scale=Decimal(str(micro.get("not_aligned_risk_scale", "0.50"))),
            ),
            global_risk_scale=Decimal(str(data.get("risk", {}).get("global_risk_scale", "1.3"))),
        )


class Strategy:
    """AetherEdge live plugin for ETH LF Portfolio V8.

    Current package implements plugin loadability, runtime requirements, feature
    buffering and V8 micro confirmation. LF engines and live position execution
    are intentionally delivered in later Board 5 packages.
    """

    def __init__(self, config_path: str | Path | None = None) -> None:
        self.config = V8Config.from_file(config_path or DEFAULT_CONFIG_PATH)
        self.buffer = V8FeatureBuffer()
        self.micro_engine = MicroContextEngine(self.config.micro_context)
        self.position = V8PositionState()
        self.router = PortfolioRouter(engines=(MomentumV3Engine(), BearV3OnlyEngine(), BullReclaimV2Engine()))
        self.signal_mapper = V8SignalMapper(SignalMapperConfig(strategy_id=self.config.strategy_id))
        self.bar_ready_events: list[BarReadyContext] = []
        self.recovered = False
        self.started = False

    def runtime_requirements(self) -> Mapping[str, Any]:
        return dict(self.config.runtime_requirements)

    async def on_start(self, snapshot: PlatformSnapshot) -> Sequence[TradeSignal]:
        self.started = True
        return []

    async def recover(self, context: StrategyRecoveryContext) -> Sequence[TradeSignal]:
        self.recovered = True
        return []

    async def on_kline(self, kline: MarketKline) -> Sequence[TradeSignal]:
        return []

    async def on_ticker(self, ticker: MarketTicker) -> Sequence[TradeSignal]:
        return []

    async def on_trade(self, trade: MarketTrade) -> Sequence[TradeSignal]:
        return []

    async def on_order_book(self, order_book: MarketOrderBook) -> Sequence[TradeSignal]:
        return []

    async def on_account_event(self, event: AccountEvent) -> Sequence[TradeSignal]:
        return []

    async def on_market_feature(self, event: MarketFeatureEvent) -> Sequence[TradeSignal]:
        if event.type_value == MarketFeatureEventType.CLOSED_KLINE.value:
            kline = parse_closed_kline(event)
            if kline.timeframe.lower() == "4h":
                self.buffer.put_kline(kline)
        elif event.type_value == MarketFeatureEventType.RANGE_AGGREGATE.value:
            aggregate = parse_range_aggregate(event)
            if aggregate.timeframe.lower() == "4h":
                self.buffer.put_range_aggregate(aggregate)
        else:
            return []
        return self._evaluate_ready_bars()

    def _evaluate_ready_bars(self) -> list[TradeSignal]:
        # Engine classes are present but still signal-empty in this package.
        # The next package plugs in the real LF engine votes for live signal generation.
        signals: list[TradeSignal] = []
        for close_time_ms in self.buffer.ready_times():
            kline = self.buffer.closed_klines[close_time_ms]
            aggregate = self.buffer.range_aggregates.get(close_time_ms)
            bootstrap_micro = self.micro_engine.evaluate(signal_side=Side.FLAT, aggregate=aggregate)
            bootstrap_context = BarReadyContext(
                kline=kline,
                range_aggregate=aggregate,
                micro=bootstrap_micro,
                global_risk_scale=self.config.global_risk_scale,
            )
            routed = self.router.evaluate(bootstrap_context)
            micro = self.micro_engine.evaluate(signal_side=routed.side, aggregate=aggregate)
            ready = BarReadyContext(
                kline=kline,
                range_aggregate=aggregate,
                micro=micro,
                global_risk_scale=self.config.global_risk_scale,
                routed_signal=routed,
            )
            self.bar_ready_events.append(ready)
            # No live order is emitted until LF engine rules are migrated.
            # Signal mapper is delivered and unit-tested separately.
            self.buffer.mark_evaluated(close_time_ms)
        return signals
