from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.exchanges.models import OrderSide, OrderStatus
from src.platform.snapshot import PlatformSnapshot
from src.signals import SignalAction, TradeSignal
from src.strategy import StrategyRecoveryContext
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, Side, V8DecisionType, V8TradeDecision
from strategies.eth_lf_portfolio_v8.domain.position_state import V8PositionState
from strategies.eth_lf_portfolio_v8.engines.bear_v3 import BearV3OnlyEngine
from strategies.eth_lf_portfolio_v8.engines.bull_reclaim_v2 import BullReclaimV2Engine
from strategies.eth_lf_portfolio_v8.engines.momentum_v3 import MomentumV3Engine
from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter
from strategies.eth_lf_portfolio_v8.execution.signal_mapper import SignalMapperConfig, V8SignalMapper
from strategies.eth_lf_portfolio_v8.execution.sizing import RiskSizingConfig, V8RiskSizer
from strategies.eth_lf_portfolio_v8.execution.stops import initial_stop_from_risk, is_better_stop, protected_stop
from strategies.eth_lf_portfolio_v8.features.buffer import V8FeatureBuffer
from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_closed_kline, parse_range_aggregate
from strategies.eth_lf_portfolio_v8.features.live_features import V8LiveFeatureBuilder
from strategies.eth_lf_portfolio_v8.features.micro_context import MicroContextConfig, MicroContextEngine


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
FOUR_HOURS_MS = 4 * 60 * 60 * 1000


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
    trailing_atr_mult: Decimal
    unit_risk_per_trade: Decimal
    max_total_notional_mult: Decimal
    max_units: int
    add_every_r: Decimal
    max_hold_bars: int
    cooldown_bars: int
    breakeven_after_r: Decimal = Decimal("1.0")
    breakeven_lock_r: Decimal = Decimal("0.10")
    lock_after_2r: Decimal = Decimal("1.7")
    lock_2r: Decimal = Decimal("0.70")
    lock_after_3r: Decimal = Decimal("2.8")
    lock_3r: Decimal = Decimal("1.50")


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
    is_add: bool = False

    @property
    def risk_per_coin(self) -> Decimal:
        return self.atr * self.initial_atr_mult


class Strategy:
    """AetherEdge live plugin for ETH LF Portfolio V8."""

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
        if event.order_status is not OrderStatus.FILLED or event.price is None:
            return []
        filled_qty = event.filled_quantity or event.quantity or Decimal("0")
        if filled_qty <= 0:
            return []

        exchange = event.exchange.value
        is_master = exchange == self.config.data_exchange
        signals: list[TradeSignal] = []

        if self.pending_entry is not None and _is_entry_side(event.side, self.pending_entry.side):
            if is_master:
                signals.extend(self._handle_master_entry_fill(event=event, filled_qty=filled_qty))
            elif self.position.in_pos:
                signals.extend(self._handle_follower_entry_fill(event=event, filled_qty=filled_qty))
            return signals

        if self.position.in_pos and _is_entry_side(event.side, self.position.side) and not is_master:
            return self._handle_follower_entry_fill(event=event, filled_qty=filled_qty)

        if self.position.in_pos and is_master and _is_close_side(event.side, self.position.side):
            self.position.close_master(exit_time_ms=event.event_time_ms)
            self.pending_entry = None
        elif self.position.in_pos and not is_master and _is_close_side(event.side, self.position.side):
            self.position.mark_leg_closed(exchange=exchange, sync_status="follower_closed")
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
        if self.position.in_pos:
            return self._position_lifecycle_signals(context)
        if self.pending_entry is not None or not self._cooldown_ok(context.kline.close_time_ms):
            return []
        return self._entry_signal_if_any(context)

    def _entry_signal_if_any(self, context: BarReadyContext) -> list[TradeSignal]:
        routed = context.routed_signal
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
        qty = self._unit_qty(
            params=params,
            entry_price=estimated_entry,
            stop_price=estimated_stop,
            risk_mult=routed.risk_mult,
            quality_mult=routed.quality_mult,
            micro_entry_risk_scale=context.micro.entry_risk_scale,
            current_qty=Decimal("0"),
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
            is_add=False,
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

    def _position_lifecycle_signals(self, context: BarReadyContext) -> list[TradeSignal]:
        self.position.update_favorable_extremes(high=context.kline.high, low=context.kline.low)
        close_decision = self._close_decision_if_needed(context)
        if close_decision is not None:
            return self.signal_mapper.map_decision(close_decision)
        add_signals = self._add_signal_if_needed(context)
        if add_signals:
            return add_signals
        return self._stop_update_signals_if_needed(context)

    def _close_decision_if_needed(self, context: BarReadyContext) -> V8TradeDecision | None:
        if not self.position.in_pos or self.position.side is Side.FLAT or self.position.qty <= 0:
            return None
        params = self.engine_params.get(self.position.entry_engine)
        hold_bars = self._holding_bars(context.kline.close_time_ms)
        exit_channel = _entry_engine_exit_channel(context, self.position.entry_engine, self.position.side)
        opposite = context.routed_signal.side is not Side.FLAT and context.routed_signal.side is not self.position.side
        max_hold = params is not None and hold_bars is not None and hold_bars >= params.max_hold_bars
        if not exit_channel and not opposite and not max_hold:
            return None
        reason = "V8_CHANNEL_EXIT" if exit_channel else "V8_OPPOSITE_SIGNAL_EXIT" if opposite else "V8_MAX_HOLD_EXIT"
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

    def _add_signal_if_needed(self, context: BarReadyContext) -> list[TradeSignal]:
        if self.pending_entry is not None or not self.position.in_pos or self.position.risk_per_coin is None:
            return []
        params = self.engine_params.get(self.position.entry_engine)
        if params is None or self.position.units >= params.max_units or self.position.first_entry is None:
            return []
        trigger_r = Decimal(str(self.position.units)) * params.add_every_r
        if self.position.side is Side.LONG:
            triggered = context.kline.high >= self.position.first_entry + trigger_r * self.position.risk_per_coin
        else:
            triggered = context.kline.low <= self.position.first_entry - trigger_r * self.position.risk_per_coin
        if not triggered:
            return []
        atr_value = _feature_decimal(context, self.position.entry_engine, "atr")
        if atr_value is None or atr_value <= 0:
            return []
        estimated_entry = context.kline.close
        stop_dist = max(params.initial_atr_mult * atr_value, self.position.risk_per_coin)
        estimated_stop = initial_stop_from_risk(side=self.position.side, entry_price=estimated_entry, risk_per_coin=stop_dist)
        qty = self._unit_qty(
            params=params,
            entry_price=estimated_entry,
            stop_price=estimated_stop,
            risk_mult=context.routed_signal.risk_mult,
            quality_mult=context.routed_signal.quality_mult,
            micro_entry_risk_scale=Decimal("1"),
            current_qty=self.position.qty,
        )
        if qty <= 0:
            return []
        self.pending_entry = PendingEntryPlan(
            side=self.position.side,
            engine=self.position.entry_engine,
            quantity=qty,
            estimated_entry_price=estimated_entry,
            atr=atr_value,
            initial_atr_mult=params.initial_atr_mult,
            bar_close_time_ms=context.kline.close_time_ms,
            entry_risk_scale=self.config.global_risk_scale,
            risk_mult=context.routed_signal.risk_mult,
            quality_mult=context.routed_signal.quality_mult,
            is_add=True,
        )
        return self.signal_mapper.map_decision(
            V8TradeDecision(
                decision_type=V8DecisionType.ADD,
                side=self.position.side,
                symbol=self.config.symbol,
                quantity=qty,
                stop_price=estimated_stop,
                engine=self.position.entry_engine,
                reason="V8_ADD_UNIT",
                bar_close_time_ms=context.kline.close_time_ms,
                entry_risk_scale=self.config.global_risk_scale,
                risk_mult=context.routed_signal.risk_mult,
                quality_mult=context.routed_signal.quality_mult,
                metadata={"add_unit_number": self.position.units + 1, "micro_entry_risk_scale_applied": False},
            )
        )

    def _stop_update_signals_if_needed(self, context: BarReadyContext) -> list[TradeSignal]:
        if not self.position.in_pos or self.position.first_entry is None or self.position.avg_entry is None or self.position.risk_per_coin is None:
            return []
        params = self.engine_params.get(self.position.entry_engine)
        if params is None:
            return []
        candidate = protected_stop(
            first_entry=self.position.first_entry,
            avg_entry=self.position.avg_entry,
            side=self.position.side,
            risk_per_coin=self.position.risk_per_coin,
            max_fav=self.position.max_fav,
            breakeven_after_r=params.breakeven_after_r,
            breakeven_lock_r=params.breakeven_lock_r,
            lock_after_2r=params.lock_after_2r,
            lock_2r=params.lock_2r,
            lock_after_3r=params.lock_after_3r,
            lock_3r=params.lock_3r,
        )
        if not is_better_stop(side=self.position.side, current_stop=self.position.stop_price, candidate=candidate):
            return []
        assert candidate is not None
        self.position.update_stop(candidate)
        target_exchanges = sorted(self.position.open_legs)
        if not target_exchanges:
            target_exchanges = [self.config.data_exchange]
        return self._replace_stop_signals(
            target_exchanges=target_exchanges,
            quantity=self.position.qty,
            stop_price=candidate,
            reason="V8_PROTECTED_STOP_UPDATE",
            bar_close_time_ms=context.kline.close_time_ms,
        )

    def _handle_master_entry_fill(self, *, event: AccountEvent, filled_qty: Decimal) -> list[TradeSignal]:
        assert self.pending_entry is not None
        exchange = event.exchange.value
        if self.pending_entry.is_add and self.position.in_pos:
            self.position.add_master_fill(avg_fill_price=event.price, add_qty=filled_qty)  # type: ignore[arg-type]
        else:
            stop_price = initial_stop_from_risk(
                side=self.pending_entry.side,
                entry_price=event.price,  # type: ignore[arg-type]
                risk_per_coin=self.pending_entry.risk_per_coin,
            )
            self.position.open_master(
                side=self.pending_entry.side,
                entry_time_ms=event.event_time_ms or self.pending_entry.bar_close_time_ms,
                avg_entry=event.price,  # type: ignore[arg-type]
                qty=filled_qty,
                stop_price=stop_price,
                entry_engine=self.pending_entry.engine,
                entry_risk_mult=self.pending_entry.entry_risk_scale,
            )
        self.position.mark_leg_open(
            exchange=exchange,
            avg_fill_price=event.price,  # type: ignore[arg-type]
            base_qty=filled_qty if not self.pending_entry.is_add else self.position.qty,
            order_id=event.order_id,
            client_order_id=event.client_order_id,
        )
        self.pending_entry = None
        if self.position.stop_price is None:
            return []
        return self._replace_stop_signals(
            target_exchanges=[exchange],
            quantity=self.position.qty,
            stop_price=self.position.stop_price,
            reason="MASTER_ENTRY_FILLED_REPLACE_STOP",
            bar_close_time_ms=event.event_time_ms,
        )

    def _handle_follower_entry_fill(self, *, event: AccountEvent, filled_qty: Decimal) -> list[TradeSignal]:
        exchange = event.exchange.value
        self.position.add_leg_fill(
            exchange=exchange,
            avg_fill_price=event.price,  # type: ignore[arg-type]
            add_base_qty=filled_qty,
            order_id=event.order_id,
            client_order_id=event.client_order_id,
        )
        if self.position.stop_price is None:
            return []
        leg_qty = self.position.legs[exchange].base_qty
        return self._replace_stop_signals(
            target_exchanges=[exchange],
            quantity=leg_qty,
            stop_price=self.position.stop_price,
            reason="FOLLOWER_ENTRY_FILLED_REPLACE_STOP",
            bar_close_time_ms=event.event_time_ms,
        )

    def _replace_stop_signals(self, *, target_exchanges: list[str], quantity: Decimal, stop_price: Decimal, reason: str, bar_close_time_ms: int | None) -> list[TradeSignal]:
        cancel = TradeSignal(
            symbol=self.config.symbol,
            action=SignalAction.CANCEL_ALL_STOP_ORDERS,
            reason=f"{reason}_CANCEL_OLD",
            metadata={"target_exchanges": target_exchanges},
        )
        stop = self.signal_mapper.map_decision(
            V8TradeDecision(
                decision_type=V8DecisionType.PLACE_STOP,
                side=self.position.side,
                symbol=self.config.symbol,
                quantity=quantity,
                stop_price=stop_price,
                engine=self.position.entry_engine,
                reason=reason,
                bar_close_time_ms=bar_close_time_ms,
                metadata={"target_exchanges": target_exchanges, "stop_price_source": "master_canonical"},
            )
        )[0]
        return [cancel, stop]

    def _unit_qty(
        self,
        *,
        params: EngineExecutionParams,
        entry_price: Decimal,
        stop_price: Decimal,
        risk_mult: Decimal,
        quality_mult: Decimal,
        micro_entry_risk_scale: Decimal,
        current_qty: Decimal,
    ) -> Decimal:
        assert self.equity is not None
        return V8RiskSizer(RiskSizingConfig(risk_pct=params.unit_risk_per_trade, max_total_notional_mult=params.max_total_notional_mult)).unit_qty(
            equity=self.equity,
            entry_price=entry_price,
            stop_price=stop_price,
            risk_mult=risk_mult,
            quality_mult=quality_mult,
            micro_entry_risk_scale=micro_entry_risk_scale,
            global_risk_scale=self.config.global_risk_scale,
            current_qty=current_qty,
        )

    def _holding_bars(self, current_close_time_ms: int) -> int | None:
        if self.position.entry_time_ms is None:
            return None
        return max(0, int((current_close_time_ms - self.position.entry_time_ms) // FOUR_HOURS_MS))

    def _cooldown_ok(self, current_close_time_ms: int) -> bool:
        last_exit = self.position.last_exit_time_ms
        if last_exit is None:
            return True
        return (current_close_time_ms - last_exit) >= _max_cooldown_bars(self.engine_params) * FOUR_HOURS_MS


def _default_engine_execution_params() -> dict[str, EngineExecutionParams]:
    return {
        "MOMENTUM_V3": EngineExecutionParams(Decimal("2.2"), Decimal("4.0"), Decimal("0.026"), Decimal("11.0"), 4, Decimal("1.0"), 180, 4),
        "BEAR_V3_ONLY": EngineExecutionParams(Decimal("2.5"), Decimal("4.5"), Decimal("0.022"), Decimal("11.0"), 5, Decimal("1.0"), 360, 8),
        "BULL_RECLAIM_V2": EngineExecutionParams(Decimal("2.2"), Decimal("3.5"), Decimal("0.020"), Decimal("8.0"), 3, Decimal("1.2"), 90, 4, Decimal("0.80"), Decimal("0.05"), Decimal("1.60"), Decimal("0.60"), Decimal("2.60"), Decimal("1.20")),
    }


def _max_cooldown_bars(params: Mapping[str, EngineExecutionParams]) -> int:
    return max((item.cooldown_bars for item in params.values()), default=0)


def _feature_decimal(context: BarReadyContext, engine: str, key: str) -> Decimal | None:
    feature_key = {"MOMENTUM_V3": "momentum", "BEAR_V3_ONLY": "bear", "BULL_RECLAIM_V2": "bull"}.get(engine)
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


def _is_close_side(order_side: OrderSide | None, position_side: Side) -> bool:
    if position_side is Side.LONG:
        return order_side is OrderSide.SELL
    if position_side is Side.SHORT:
        return order_side is OrderSide.BUY
    return False
