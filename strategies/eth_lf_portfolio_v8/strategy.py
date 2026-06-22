from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.snapshot import PlatformSnapshot
from src.platform.exchanges.models import OrderSide, OrderStatus
from src.reconcile.models import ReconcileReport
from src.signals import TradeSignal
from src.strategy import StrategyRecoveryContext
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, Side, V8DecisionType, V8TradeDecision
from strategies.eth_lf_portfolio_v8.engines.bear_v3 import BearV3OnlyEngine
from strategies.eth_lf_portfolio_v8.engines.bull_reclaim_v2 import BullReclaimV2Engine
from strategies.eth_lf_portfolio_v8.engines.momentum_v3 import MomentumV3Engine
from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter
from strategies.eth_lf_portfolio_v8.execution.signal_mapper import SignalMapperConfig, V8SignalMapper
from strategies.eth_lf_portfolio_v8.execution.sizing import RiskSizingConfig, V8RiskSizer
from strategies.eth_lf_portfolio_v8.execution.stops import initial_stop_from_risk
from strategies.eth_lf_portfolio_v8.domain.position_state import V8PositionState
from strategies.eth_lf_portfolio_v8.features.buffer import V8FeatureBuffer
from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_closed_kline, parse_range_aggregate
from strategies.eth_lf_portfolio_v8.features.micro_context import MicroContextConfig, MicroContextEngine
from strategies.eth_lf_portfolio_v8.features.live_features import V8LiveFeatureBuilder


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"


@dataclass(frozen=True)
class V8Config:
    strategy_id: str
    symbol: str
    data_exchange: str
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
            data_exchange=str(data.get("data_exchange", "okx")).strip().lower(),
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


@dataclass(frozen=True)
class EngineExecutionParams:
    initial_atr_mult: Decimal
    unit_risk_per_trade: Decimal
    max_total_notional_mult: Decimal


@dataclass(frozen=True)
class PendingEntryPlan:
    side: Side
    engine: str
    quantity: Decimal
    estimated_entry_price: Decimal
    atr: Decimal
    initial_atr_mult: Decimal
    bar_close_time_ms: int
    entry_risk_scale: Decimal
    risk_mult: Decimal
    quality_mult: Decimal

    @property
    def risk_per_coin(self) -> Decimal:
        return self.atr * self.initial_atr_mult


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
        self.feature_builder = V8LiveFeatureBuilder()
        self.position = V8PositionState()
        self.router = PortfolioRouter(engines=(MomentumV3Engine(), BearV3OnlyEngine(), BullReclaimV2Engine()))
        self.signal_mapper = V8SignalMapper(SignalMapperConfig(strategy_id=self.config.strategy_id))
        self.engine_params = _default_engine_execution_params()
        self.equity: Decimal | None = None
        self.pending_entry: PendingEntryPlan | None = None
        self.bar_ready_events: list[BarReadyContext] = []
        self.recovered = False
        self.started = False

    def runtime_requirements(self) -> Mapping[str, Any]:
        return dict(self.config.runtime_requirements)

    async def on_start(self, snapshot: PlatformSnapshot) -> Sequence[TradeSignal]:
        self.started = True
        self.equity = snapshot.balance.available if snapshot.balance.available > 0 else snapshot.balance.total
        return []

    async def recover(self, context: StrategyRecoveryContext) -> Sequence[TradeSignal]:
        self.recovered = True
        for snapshot in context.snapshots:
            if snapshot.balance.exchange.value == self.config.data_exchange:
                self.equity = snapshot.balance.available if snapshot.balance.available > 0 else snapshot.balance.total
                break
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
        if event.event_type is not AccountEventType.ORDER or event.symbol != self.config.symbol:
            return []
        self.position.apply_account_event(event, master_exchange=self.config.data_exchange)
        if event.order_status is not OrderStatus.FILLED or event.price is None:
            return []
        filled_qty = event.filled_quantity or event.quantity or Decimal("0")
        if filled_qty <= 0:
            return []

        exchange = event.exchange.value
        is_master = exchange == self.config.data_exchange
        signals: list[TradeSignal] = []

        if self.pending_entry is not None and is_master and _is_entry_side(event.side, self.pending_entry.side):
            stop_price = initial_stop_from_risk(
                side=self.pending_entry.side,
                entry_price=event.price,
                risk_per_coin=self.pending_entry.risk_per_coin,
            )
            self.position.open_master(
                side=self.pending_entry.side,
                entry_time_ms=event.event_time_ms or self.pending_entry.bar_close_time_ms,
                avg_entry=event.price,
                qty=filled_qty,
                stop_price=stop_price,
                entry_engine=self.pending_entry.engine,
                entry_risk_mult=self.pending_entry.entry_risk_scale,
            )
            self.position.mark_leg_open(
                exchange=exchange,
                avg_fill_price=event.price,
                base_qty=filled_qty,
                order_id=event.order_id,
                client_order_id=event.client_order_id,
            )
            signals.extend(
                self.signal_mapper.map_decision(
                    V8TradeDecision(
                        decision_type=V8DecisionType.PLACE_STOP,
                        side=self.pending_entry.side,
                        symbol=self.config.symbol,
                        quantity=filled_qty,
                        stop_price=stop_price,
                        engine=self.pending_entry.engine,
                        reason="MASTER_ENTRY_FILLED_PLACE_STOP",
                        bar_close_time_ms=self.pending_entry.bar_close_time_ms,
                        metadata={"target_exchanges": [exchange], "stop_price_source": "master_fill"},
                    )
                )
            )
            return signals

        if self.position.in_pos and not is_master and _is_entry_side(event.side, self.position.side):
            self.position.mark_leg_open(
                exchange=exchange,
                avg_fill_price=event.price,
                base_qty=filled_qty,
                order_id=event.order_id,
                client_order_id=event.client_order_id,
            )
            if self.position.stop_price is not None:
                signals.extend(
                    self.signal_mapper.map_decision(
                        V8TradeDecision(
                            decision_type=V8DecisionType.PLACE_STOP,
                            side=self.position.side,
                            symbol=self.config.symbol,
                            quantity=filled_qty,
                            stop_price=self.position.stop_price,
                            engine=self.position.entry_engine,
                            reason="FOLLOWER_ENTRY_FILLED_PLACE_STOP",
                            metadata={"target_exchanges": [exchange], "stop_price_source": "master_canonical"},
                        )
                    )
                )
        return signals

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
        # Build engine features using only bars up to the bar being evaluated.
        signals: list[TradeSignal] = []
        for close_time_ms in self.buffer.ready_times():
            kline = self.buffer.closed_klines[close_time_ms]
            aggregate = self.buffer.range_aggregates.get(close_time_ms)
            feature_rows = self.feature_builder.build_latest(self.buffer.closed_klines, target_close_time_ms=close_time_ms)
            engine_features = {
                "momentum": feature_rows.momentum or {},
                "bear": feature_rows.bear or {},
                "bull": feature_rows.bull or {},
            }
            bootstrap_micro = self.micro_engine.evaluate(signal_side=Side.FLAT, aggregate=aggregate)
            bootstrap_context = BarReadyContext(
                kline=kline,
                range_aggregate=aggregate,
                micro=bootstrap_micro,
                global_risk_scale=self.config.global_risk_scale,
                engine_features=engine_features,
            )
            routed = self.router.evaluate(bootstrap_context)
            micro = self.micro_engine.evaluate(signal_side=routed.side, aggregate=aggregate)
            ready = BarReadyContext(
                kline=kline,
                range_aggregate=aggregate,
                micro=micro,
                global_risk_scale=self.config.global_risk_scale,
                routed_signal=routed,
                engine_features=engine_features,
            )
            self.bar_ready_events.append(ready)
            signals.extend(self._signals_from_ready_context(ready))
            self.buffer.mark_evaluated(close_time_ms)
        return signals


    def _signals_from_ready_context(self, context: BarReadyContext) -> list[TradeSignal]:
        if not self.started or self.equity is None:
            return []
        routed = context.routed_signal
        if self.position.in_pos:
            close_decision = self._close_decision_if_needed(context)
            return self.signal_mapper.map_decision(close_decision) if close_decision is not None else []
        if self.pending_entry is not None:
            return []
        if routed.side is Side.FLAT or context.micro.entry_risk_scale <= 0:
            return []
        params = self.engine_params.get(routed.engine)
        if params is None:
            return []
        atr_value = _feature_decimal(context, routed.engine, "atr")
        if atr_value is None or atr_value <= 0:
            return []
        estimated_entry = context.kline.close
        estimated_stop = initial_stop_from_risk(
            side=routed.side,
            entry_price=estimated_entry,
            risk_per_coin=atr_value * params.initial_atr_mult,
        )
        qty = V8RiskSizer(
            RiskSizingConfig(
                risk_pct=params.unit_risk_per_trade,
                max_total_notional_mult=params.max_total_notional_mult,
            )
        ).unit_qty(
            equity=self.equity,
            entry_price=estimated_entry,
            stop_price=estimated_stop,
            risk_mult=routed.risk_mult,
            quality_mult=routed.quality_mult,
            micro_entry_risk_scale=context.micro.entry_risk_scale,
            global_risk_scale=self.config.global_risk_scale,
        )
        if qty <= 0:
            return []
        entry_risk_scale = context.micro.entry_risk_scale * self.config.global_risk_scale
        self.pending_entry = PendingEntryPlan(
            side=routed.side,
            engine=routed.engine,
            quantity=qty,
            estimated_entry_price=estimated_entry,
            atr=atr_value,
            initial_atr_mult=params.initial_atr_mult,
            bar_close_time_ms=context.kline.close_time_ms,
            entry_risk_scale=entry_risk_scale,
            risk_mult=routed.risk_mult,
            quality_mult=routed.quality_mult,
        )
        return self.signal_mapper.map_decision(
            V8TradeDecision(
                decision_type=V8DecisionType.OPEN,
                side=routed.side,
                symbol=self.config.symbol,
                quantity=qty,
                stop_price=estimated_stop,
                engine=routed.engine,
                reason="V8_LIVE_ENTRY",
                bar_close_time_ms=context.kline.close_time_ms,
                entry_risk_scale=entry_risk_scale,
                risk_mult=routed.risk_mult,
                quality_mult=routed.quality_mult,
                metadata={
                    "estimated_entry_price": str(estimated_entry),
                    "estimated_initial_stop": str(estimated_stop),
                    "micro_filter_action": context.micro.action,
                    "await_master_fill_before_stop": True,
                },
            )
        )

    def _close_decision_if_needed(self, context: BarReadyContext) -> V8TradeDecision | None:
        if not self.position.in_pos or self.position.side is Side.FLAT or self.position.qty <= 0:
            return None
        exit_channel = _entry_engine_exit_channel(context, self.position.entry_engine, self.position.side)
        opposite = context.routed_signal.side is not Side.FLAT and context.routed_signal.side is not self.position.side
        if not exit_channel and not opposite:
            return None
        reason = "V8_CHANNEL_EXIT" if exit_channel else "V8_OPPOSITE_SIGNAL_EXIT"
        return V8TradeDecision(
            decision_type=V8DecisionType.CLOSE,
            side=self.position.side,
            symbol=self.config.symbol,
            quantity=self.position.qty,
            engine=self.position.entry_engine,
            reason=reason,
            bar_close_time_ms=context.kline.close_time_ms,
            metadata={"reduce_only": True},
        )


def _default_engine_execution_params() -> dict[str, EngineExecutionParams]:
    return {
        "MOMENTUM_V3": EngineExecutionParams(Decimal("2.2"), Decimal("0.026"), Decimal("11.0")),
        "BEAR_V3_ONLY": EngineExecutionParams(Decimal("2.5"), Decimal("0.022"), Decimal("11.0")),
        "BULL_RECLAIM_V2": EngineExecutionParams(Decimal("2.2"), Decimal("0.020"), Decimal("8.0")),
    }


def _feature_decimal(context: BarReadyContext, engine: str, key: str) -> Decimal | None:
    feature_key = {
        "MOMENTUM_V3": "momentum",
        "BEAR_V3_ONLY": "bear",
        "BULL_RECLAIM_V2": "bull",
    }.get(engine)
    if feature_key is None:
        return None
    value = context.engine_features.get(feature_key, {}).get(key)
    if value is None:
        return None
    return Decimal(str(value))


def _entry_engine_exit_channel(context: BarReadyContext, entry_engine: str, side: Side) -> bool:
    if entry_engine.startswith("BEAR") and side is Side.SHORT:
        return bool(context.engine_features.get("bear", {}).get("short_exit_channel", False))
    if entry_engine.startswith("BULL") and side is Side.LONG:
        return bool(context.engine_features.get("bull", {}).get("long_exit_channel", False))
    row = context.engine_features.get("momentum", {})
    return bool(row.get("long_exit_channel" if side is Side.LONG else "short_exit_channel", False))


def _is_entry_side(order_side: OrderSide | None, position_side: Side) -> bool:
    if position_side is Side.LONG:
        return order_side is OrderSide.BUY
    if position_side is Side.SHORT:
        return order_side is OrderSide.SELL
    return False
