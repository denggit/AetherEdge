from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.exchanges.models import OrderSide, OrderStatus, Position, PositionSide
from src.order_management.models import ExchangeOrderResult
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
    position_id: str
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
    """AetherEdge live plugin for ETH LF Portfolio V9C reclaim-first routing."""

    def __init__(self, config_path: str | Path | None = None) -> None:
        self.config = V8Config.from_file(config_path or DEFAULT_CONFIG_PATH)
        self.buffer = V8FeatureBuffer()
        self.micro_engine = MicroContextEngine(self.config.micro_context)
        self.feature_builder = V8LiveFeatureBuilder()
        self.position = V8PositionState()
        self.router = PortfolioRouter(engines=(BullReclaimV2Engine(), MomentumV3Engine(), BearV3OnlyEngine()))
        self.signal_mapper = V8SignalMapper(SignalMapperConfig(strategy_id=self.config.strategy_id))
        self.engine_params = _default_engine_execution_params()
        self.equity: Decimal | None = None
        self.exchange_equity: dict[str, Decimal] = {}
        self.recovery_manual_required = False
        self.pending_entry: PendingEntryPlan | None = None
        self.bar_ready_events: list[BarReadyContext] = []
        self.recovery_alerts: list[str] = []
        self.recovered = False
        self.started = False

    def runtime_requirements(self) -> Mapping[str, Any]:
        return dict(self.config.runtime_requirements)

    async def on_start(self, snapshot: PlatformSnapshot) -> Sequence[TradeSignal]:
        self.started = True
        balance = snapshot.balance.available if snapshot.balance.available > 0 else snapshot.balance.total
        self.equity = balance
        self.exchange_equity[snapshot.balance.exchange.value] = balance
        return []

    async def recover(self, context: StrategyRecoveryContext) -> Sequence[TradeSignal]:
        self.recovered = True
        for snapshot in context.snapshots:
            balance = snapshot.balance.available if snapshot.balance.available > 0 else snapshot.balance.total
            if balance > 0:
                self.exchange_equity[snapshot.balance.exchange.value] = balance
            if snapshot.balance.exchange.value == self.config.data_exchange:
                self.equity = balance
        plans = tuple(context.metadata.get("active_position_plans", ()) if context.metadata else ())
        return self._recover_position_from_plans(snapshots=context.snapshots, plans=plans)

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
            follower_close_signals = self._follower_close_signals_after_master_close(event_time_ms=event.event_time_ms)
            self.position.close_master(exit_time_ms=event.event_time_ms)
            self.pending_entry = None
            signals.extend(follower_close_signals)
        elif self.position.in_pos and not is_master and _is_close_side(event.side, self.position.side):
            self.position.mark_leg_closed(exchange=exchange, sync_status="follower_closed")
        return signals

    async def on_order_results(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
        source: str,
        event_time_ms: int | None,
    ) -> Sequence[TradeSignal]:
        if signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT, SignalAction.CANCEL_ALL_STOP_ORDERS}:
            return []
        if signal.action not in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT, SignalAction.CLOSE_LONG, SignalAction.CLOSE_SHORT}:
            return []

        signals: list[TradeSignal] = []
        events = [
            event
            for result in results
            if (event := _account_event_from_order_result(signal=signal, result=result, event_time_ms=event_time_ms)) is not None
        ]
        if signal.action in {SignalAction.CLOSE_LONG, SignalAction.CLOSE_SHORT}:
            return self._handle_close_order_result_events(signal=signal, events=events)

        # Entry fills can intentionally cascade: the master fill establishes the
        # canonical stop, then follower fills reuse it. Keep this path
        # sequential while close handling below remains two-phase to avoid
        # duplicate follower close signals.
        for event in events:
            exchange = event.exchange.value
            is_master = exchange == self.config.data_exchange
            filled_qty = event.filled_quantity or event.quantity or Decimal("0")
            if filled_qty <= 0:
                continue

            if self.pending_entry is not None and _is_entry_side(event.side, self.pending_entry.side):
                if is_master:
                    signals.extend(self._handle_master_entry_fill(event=event, filled_qty=filled_qty))
                elif self.position.in_pos:
                    signals.extend(self._handle_follower_entry_fill(event=event, filled_qty=filled_qty))
            elif self.position.in_pos and _is_entry_side(event.side, self.position.side) and not is_master:
                signals.extend(self._handle_follower_entry_fill(event=event, filled_qty=filled_qty))
        return signals

    def _handle_close_order_result_events(self, *, signal: TradeSignal, events: Sequence[AccountEvent]) -> Sequence[TradeSignal]:
        if not self.position.in_pos or self.position.side is Side.FLAT:
            return []
        target_exchanges = _target_exchanges(signal)
        master_close_event: AccountEvent | None = None
        follower_closed_exchanges: list[str] = []
        for event in events:
            exchange = event.exchange.value
            is_master = exchange == self.config.data_exchange
            filled_qty = event.filled_quantity or event.quantity or Decimal("0")
            if filled_qty <= 0:
                continue
            if is_master and _is_close_side(event.side, self.position.side):
                master_close_event = event
                continue
            if not is_master and _is_close_side(event.side, self.position.side):
                follower_closed_exchanges.append(exchange)

        if master_close_event is None:
            for exchange in follower_closed_exchanges:
                self.position.mark_leg_closed(exchange=exchange, sync_status="follower_closed")
            return []

        # Collect unclosed follower exchanges BEFORE close_master() resets open_legs.
        unclosed_followers: list[str] = []
        for exchange, leg in sorted(self.position.open_legs.items()):
            if exchange == self.config.data_exchange:
                continue
            if leg.base_qty <= 0:
                continue
            if exchange not in follower_closed_exchanges:
                unclosed_followers.append(exchange)

        follow_up: list[TradeSignal] = []
        if unclosed_followers:
            follow_up = self._follower_close_signals_after_master_close(
                event_time_ms=master_close_event.event_time_ms,
                only_exchanges=unclosed_followers,
            )

        self.position.close_master(exit_time_ms=master_close_event.event_time_ms)
        self.pending_entry = None
        for exchange in follower_closed_exchanges:
            self.position.mark_leg_closed(exchange=exchange, sync_status="follower_closed")
        return follow_up

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
        if not self.started or self.equity is None or self.recovery_manual_required:
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
        exchange_quantities = self._entry_exchange_quantities(
            params=params,
            entry_price=estimated_entry,
            stop_price=estimated_stop,
            risk_mult=routed.risk_mult,
            quality_mult=routed.quality_mult,
            micro_entry_risk_scale=context.micro.entry_risk_scale,
            current_by_exchange={},
        )
        qty = exchange_quantities.get(self.config.data_exchange, Decimal("0"))
        if qty <= 0:
            return []
        position_id = f"v9c-{context.kline.close_time_ms}-{routed.engine}-{routed.side.name.lower()}"
        entry_risk_scale = context.micro.entry_risk_scale * self.config.global_risk_scale
        self.pending_entry = PendingEntryPlan(
            position_id=position_id,
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
                    "execution_purpose": "normal_entry",
                    "position_id": position_id,
                    "target_exchanges": sorted(exchange_quantities),
                    "exchange_quantities_base": _exchange_quantity_metadata(exchange_quantities),
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
        exchange_quantities = self._open_leg_quantities()
        quantity = exchange_quantities.get(self.config.data_exchange, self.position.qty)
        return V8TradeDecision(
            decision_type=V8DecisionType.CLOSE,
            side=self.position.side,
            symbol=self.config.symbol,
            quantity=quantity,
            engine=self.position.entry_engine,
            reason=reason,
            bar_close_time_ms=context.kline.close_time_ms,
            metadata={
                "reduce_only": True,
                "execution_purpose": "normal_close",
                "position_id": self.position.position_id,
                "target_exchanges": sorted(exchange_quantities),
                "exchange_quantities_base": _exchange_quantity_metadata(exchange_quantities),
            },
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
        current_by_exchange = self._open_leg_quantities()
        exchange_quantities = self._entry_exchange_quantities(
            params=params,
            entry_price=estimated_entry,
            stop_price=estimated_stop,
            risk_mult=context.routed_signal.risk_mult,
            quality_mult=context.routed_signal.quality_mult,
            micro_entry_risk_scale=Decimal("1"),
            current_by_exchange=current_by_exchange,
        )
        qty = exchange_quantities.get(self.config.data_exchange, Decimal("0"))
        if qty <= 0:
            return []
        position_id = self.position.position_id or f"v9c-add-{context.kline.close_time_ms}-{self.position.entry_engine}-{self.position.side.name.lower()}"
        self.pending_entry = PendingEntryPlan(
            position_id=position_id,
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
                metadata={
                    "add_unit_number": self.position.units + 1,
                    "micro_entry_risk_scale_applied": False,
                    "execution_purpose": "normal_entry",
                    "position_id": position_id,
                    "target_exchanges": sorted(exchange_quantities),
                    "exchange_quantities_base": _exchange_quantity_metadata(exchange_quantities),
                },
            )
        )

    def _stop_update_signals_if_needed(self, context: BarReadyContext) -> list[TradeSignal]:
        if not self.position.in_pos or self.position.first_entry is None or self.position.avg_entry is None or self.position.risk_per_coin is None:
            return []
        params = self.engine_params.get(self.position.entry_engine)
        if params is None:
            return []
        atr_value = _feature_decimal(context, self.position.entry_engine, "atr")
        candidates: list[Decimal] = []
        if atr_value is not None and atr_value > 0:
            if self.position.side is Side.LONG:
                candidates.append(context.kline.close - params.trailing_atr_mult * atr_value)
            elif self.position.side is Side.SHORT:
                candidates.append(context.kline.close + params.trailing_atr_mult * atr_value)
        protected = protected_stop(
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
        if protected is not None:
            candidates.append(protected)
        if not candidates:
            return []
        if self.position.side is Side.LONG:
            candidate = max(candidates)
        elif self.position.side is Side.SHORT:
            candidate = min(candidates)
        else:
            return []
        if not is_better_stop(side=self.position.side, current_stop=self.position.stop_price, candidate=candidate):
            return []
        self.position.update_stop(candidate)
        exchange_quantities = self._open_leg_quantities()
        target_exchanges = sorted(exchange_quantities)
        if not target_exchanges:
            target_exchanges = [self.config.data_exchange]
            exchange_quantities = {self.config.data_exchange: self.position.qty}
        return self._replace_stop_signals(
            target_exchanges=target_exchanges,
            quantity=exchange_quantities.get(self.config.data_exchange, self.position.qty),
            stop_price=candidate,
            reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
            bar_close_time_ms=context.kline.close_time_ms,
            exchange_quantities=exchange_quantities,
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
                position_id=self.pending_entry.position_id,
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

    def _recover_position_from_plans(self, *, snapshots: Sequence[PlatformSnapshot], plans: Sequence[Mapping[str, Any]]) -> list[TradeSignal]:
        snapshot_by_exchange = {snapshot.balance.exchange.value: snapshot for snapshot in snapshots}
        master_snapshot = snapshot_by_exchange.get(self.config.data_exchange)
        if master_snapshot is None:
            return []
        active_master = _first_active_position(master_snapshot.positions)
        active_plan = _first_active_plan(plans)
        if active_master is not None and active_plan is not None:
            return self._recover_active_master_with_plan(master=active_master, master_snapshot=master_snapshot, snapshots=snapshot_by_exchange, plan_payload=active_plan)
        if active_master is not None:
            self._recover_active_master_without_plan(active_master)
            return []
        if active_plan is not None:
            return self._recover_master_closed_with_active_plan(snapshots=snapshot_by_exchange, plan_payload=active_plan)
        return []

    def _recover_active_master_with_plan(self, *, master: Position, master_snapshot: PlatformSnapshot, snapshots: Mapping[str, PlatformSnapshot], plan_payload: Mapping[str, Any]) -> list[TradeSignal]:
        plan = dict(plan_payload.get("position", {}))
        legs = [dict(item) for item in plan_payload.get("legs", [])]
        side = _side_from_plan(plan.get("side"))
        actual_side = _side_from_position(master)
        if side is Side.FLAT or actual_side is Side.FLAT or side is not actual_side:
            self.recovery_alerts.append("master_active_plan_side_mismatch_manual_required")
            self._recover_active_master_without_plan(master)
            return []
        stop_price = _dec_or_none(plan.get("canonical_stop_price"))
        if stop_price is None:
            self.recovery_alerts.append("master_active_plan_missing_canonical_stop_manual_required")
            self._recover_active_master_without_plan(master)
            return []
        qty = abs(master.quantity)
        entry_price = master.entry_price or stop_price
        self.position.open_master(
            side=side,
            entry_time_ms=int(plan.get("created_time_ms") or 0),
            avg_entry=entry_price,
            qty=qty,
            stop_price=stop_price,
            entry_engine=str(plan.get("entry_engine") or "unknown"),
            position_id=str(plan.get("position_id") or ""),
        )
        self.position.mark_leg_open(exchange=self.config.data_exchange, avg_fill_price=entry_price, base_qty=qty, sync_status="recovered_master")
        signals: list[TradeSignal] = []
        if not _has_stop_at_price(master_snapshot.open_stop_orders, stop_price):
            signals.extend(
                self._replace_stop_signals(
                    target_exchanges=[self.config.data_exchange],
                    quantity=qty,
                    stop_price=stop_price,
                    reason="RECOVERY_MASTER_STOP_SYNC",
                    bar_close_time_ms=None,
                )
            )
        for leg in legs:
            exchange = str(leg.get("exchange") or "").lower()
            if not exchange or exchange == self.config.data_exchange:
                continue
            target_qty = _dec_or_zero(leg.get("target_qty_base"))
            follower_snapshot = snapshots.get(exchange)
            same_qty, reverse_qty = _side_quantities(follower_snapshot.positions if follower_snapshot else [], side)
            if reverse_qty > 0:
                self.position.legs[exchange] = self.position.legs.get(exchange) or self.position.mark_leg_closed(exchange=exchange, sync_status="reverse_position_manual_required")
                self.position.legs[exchange].sync_status = "reverse_position_manual_required"
                self.recovery_alerts.append(f"follower_reverse_position:{exchange}")
                continue
            if same_qty <= 0 and target_qty > 0:
                self.position.mark_leg_closed(exchange=exchange, sync_status="missing")
                signals.append(self._follower_topup_signal(exchange=exchange, side=side, quantity=target_qty, plan=plan))
            elif same_qty < target_qty:
                self.position.mark_leg_open(exchange=exchange, avg_fill_price=entry_price, base_qty=same_qty, sync_status="underfilled")
                signals.append(self._follower_topup_signal(exchange=exchange, side=side, quantity=target_qty - same_qty, plan=plan))
            elif same_qty > target_qty and target_qty > 0:
                self.position.mark_leg_open(exchange=exchange, avg_fill_price=entry_price, base_qty=same_qty, sync_status="overfilled")
                self.recovery_alerts.append(f"follower_overfilled:{exchange}")
            elif same_qty > 0:
                self.position.mark_leg_open(exchange=exchange, avg_fill_price=entry_price, base_qty=same_qty, sync_status="synced")
        return signals

    def _recover_active_master_without_plan(self, master: Position) -> None:
        side = _side_from_position(master)
        if side is Side.FLAT:
            return
        entry_price = master.entry_price or Decimal("1")
        self.position.in_pos = True
        self.position.side = side
        self.position.entry_time_ms = 0
        self.position.first_entry = entry_price
        self.position.avg_entry = entry_price
        self.position.qty = abs(master.quantity)
        self.position.units = 1
        self.position.entry_engine = "unknown"
        self.position.stop_price = None
        self.position.risk_per_coin = None
        self.position.mark_leg_open(exchange=self.config.data_exchange, avg_fill_price=entry_price, base_qty=abs(master.quantity), sync_status="master_active_plan_unknown")
        self.recovery_manual_required = True
        self.recovery_alerts.append("master_active_plan_unknown_manual_required")

    def _recover_master_closed_with_active_plan(self, *, snapshots: Mapping[str, PlatformSnapshot], plan_payload: Mapping[str, Any]) -> list[TradeSignal]:
        plan = dict(plan_payload.get("position", {}))
        side = _side_from_plan(plan.get("side"))
        if side is Side.FLAT:
            return []
        signals: list[TradeSignal] = []
        for leg in plan_payload.get("legs", []):
            exchange = str(dict(leg).get("exchange") or "").lower()
            if not exchange or exchange == self.config.data_exchange:
                continue
            snapshot = snapshots.get(exchange)
            same_qty, _ = _side_quantities(snapshot.positions if snapshot else [], side)
            if same_qty > 0:
                signals.append(
                    TradeSignal(
                        symbol=self.config.symbol,
                        action=SignalAction.CLOSE_LONG if side is Side.LONG else SignalAction.CLOSE_SHORT,
                        quantity=same_qty,
                        reason="RECOVERY_MASTER_CLOSED_CLOSE_FOLLOWER",
                        metadata={
                            "target_exchanges": [exchange],
                            "reduce_only": True,
                            "execution_purpose": "follower_close_after_master_close",
                            "position_id": plan.get("position_id"),
                        },
                    )
                )
                self.recovery_alerts.append(f"master_closed_follower_still_open:{exchange}")
        return signals

    def _follower_topup_signal(self, *, exchange: str, side: Side, quantity: Decimal, plan: Mapping[str, Any]) -> TradeSignal:
        return TradeSignal(
            symbol=self.config.symbol,
            action=SignalAction.OPEN_LONG if side is Side.LONG else SignalAction.OPEN_SHORT,
            quantity=quantity,
            reason="RECOVERY_FOLLOWER_TOPUP",
            metadata={
                "target_exchanges": [exchange],
                "execution_purpose": "follower_recovery_topup",
                "position_id": plan.get("position_id"),
                "engine": plan.get("entry_engine"),
            },
        )

    def _replace_stop_signals(self, *, target_exchanges: list[str], quantity: Decimal, stop_price: Decimal, reason: str, bar_close_time_ms: int | None, exchange_quantities: Mapping[str, Decimal] | None = None) -> list[TradeSignal]:
        exchange_quantities = dict(exchange_quantities or {})
        cancel = TradeSignal(
            symbol=self.config.symbol,
            action=SignalAction.CANCEL_ALL_STOP_ORDERS,
            reason=f"{reason}_CANCEL_OLD",
            metadata={
                "target_exchanges": target_exchanges,
                "execution_purpose": "stop_sync",
                "position_id": self.position.position_id,
            },
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
                metadata={
                    "target_exchanges": target_exchanges,
                    "stop_price_source": "master_canonical",
                    "execution_purpose": "stop_sync",
                    "position_id": self.position.position_id,
                    **({"exchange_quantities_base": _exchange_quantity_metadata(exchange_quantities)} if exchange_quantities else {}),
                },
            )
        )[0]
        return [cancel, stop]

    def _follower_close_signals_after_master_close(self, *, event_time_ms: int | None, only_exchanges: list[str] | None = None) -> list[TradeSignal]:
        if not self.position.in_pos or self.position.side is Side.FLAT:
            return []
        action = SignalAction.CLOSE_LONG if self.position.side is Side.LONG else SignalAction.CLOSE_SHORT
        only_set: set[str] | None = set(only_exchanges) if only_exchanges is not None else None
        signals: list[TradeSignal] = []
        for exchange, leg in sorted(self.position.open_legs.items()):
            if exchange == self.config.data_exchange or leg.base_qty <= 0:
                continue
            if only_set is not None and exchange not in only_set:
                continue
            signals.append(
                TradeSignal(
                    symbol=self.config.symbol,
                    action=action,
                    quantity=leg.base_qty,
                    reason="MASTER_CLOSE_FILLED_CLOSE_FOLLOWER",
                    metadata={
                        "target_exchanges": [exchange],
                        "reduce_only": True,
                        "execution_purpose": "follower_close_after_master_close",
                        "position_id": self.position.position_id,
                        "master_close_event_time_ms": event_time_ms,
                        "master_already_closed": True,
                        "close_required_reason": "master_closed_follower_not_closed",
                    },
                )
            )
        return signals

    def _entry_exchange_quantities(
        self,
        *,
        params: EngineExecutionParams,
        entry_price: Decimal,
        stop_price: Decimal,
        risk_mult: Decimal,
        quality_mult: Decimal,
        micro_entry_risk_scale: Decimal,
        current_by_exchange: Mapping[str, Decimal],
    ) -> dict[str, Decimal]:
        quantities: dict[str, Decimal] = {}
        exchanges = set(self.exchange_equity) | {self.config.data_exchange}
        for exchange in sorted(exchanges):
            equity = self.exchange_equity.get(exchange)
            if equity is None or equity <= 0:
                if exchange == self.config.data_exchange and self.equity is not None:
                    equity = self.equity
                else:
                    continue
            qty = V8RiskSizer(RiskSizingConfig(risk_pct=params.unit_risk_per_trade, max_total_notional_mult=params.max_total_notional_mult)).unit_qty(
                equity=equity,
                entry_price=entry_price,
                stop_price=stop_price,
                risk_mult=risk_mult,
                quality_mult=quality_mult,
                micro_entry_risk_scale=micro_entry_risk_scale,
                global_risk_scale=self.config.global_risk_scale,
                current_qty=current_by_exchange.get(exchange, Decimal("0")),
            )
            if qty > 0:
                quantities[exchange] = qty
        return quantities

    def _open_leg_quantities(self) -> dict[str, Decimal]:
        quantities = {exchange: leg.base_qty for exchange, leg in self.position.open_legs.items() if leg.base_qty > 0}
        if self.config.data_exchange not in quantities and self.position.qty > 0:
            quantities[self.config.data_exchange] = self.position.qty
        return quantities


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


def _exchange_quantity_metadata(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {str(exchange): str(quantity) for exchange, quantity in values.items() if quantity > 0}


def _default_engine_execution_params() -> dict[str, EngineExecutionParams]:
    return {
        "MOMENTUM_V3": EngineExecutionParams(Decimal("2.2"), Decimal("4.0"), Decimal("0.032"), Decimal("12.0"), 4, Decimal("1.0"), 180, 4),
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


def _account_event_from_order_result(*, signal: TradeSignal, result: ExchangeOrderResult, event_time_ms: int | None) -> AccountEvent | None:
    if not result.ok or result.status is not OrderStatus.FILLED:
        return None
    price = result.avg_fill_price
    filled_qty = result.filled_quantity or result.quantity
    if price is None or filled_qty is None or filled_qty <= 0:
        return None
    side = result.side or _signal_order_side(signal.action)
    if side is None:
        return None
    return AccountEvent(
        exchange=result.exchange,
        event_type=AccountEventType.ORDER,
        symbol=signal.symbol,
        raw_symbol=signal.symbol,
        event_time_ms=event_time_ms or signal.created_time_ms,
        order_id=result.order_id,
        client_order_id=result.client_order_id,
        order_status=result.status,
        side=side,
        price=price,
        quantity=result.quantity,
        filled_quantity=filled_qty,
        raw={**dict(result.raw), "source": "request_order_result"},
    )


def _signal_order_side(action: SignalAction) -> OrderSide | None:
    if action in {SignalAction.OPEN_LONG, SignalAction.CLOSE_SHORT}:
        return OrderSide.BUY
    if action in {SignalAction.OPEN_SHORT, SignalAction.CLOSE_LONG}:
        return OrderSide.SELL
    return None


def _target_exchanges(signal: TradeSignal) -> tuple[str, ...]:
    raw = signal.metadata.get("target_exchanges") if signal.metadata else None
    if raw is None:
        return ()
    if isinstance(raw, str):
        items = raw.split(",")
    else:
        try:
            items = tuple(raw)
        except TypeError:
            items = (raw,)
    return tuple(str(item.value if hasattr(item, "value") else item).strip().lower() for item in items if str(item).strip())


def _first_active_position(positions: Sequence[Position]) -> Position | None:
    for position in positions:
        if position.quantity != 0:
            return position
    return None


def _first_active_plan(plans: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for item in plans:
        plan = item.get("position", {}) if isinstance(item, Mapping) else {}
        if str(plan.get("status", "")).lower() == "active":
            return item
    return plans[0] if plans else None


def _side_from_plan(value: Any) -> Side:
    text = str(value or "").lower()
    if text == "long":
        return Side.LONG
    if text == "short":
        return Side.SHORT
    return Side.FLAT


def _side_from_position(position: Position) -> Side:
    if position.side is PositionSide.LONG:
        return Side.LONG
    if position.side is PositionSide.SHORT:
        return Side.SHORT
    if position.quantity > 0:
        return Side.LONG
    if position.quantity < 0:
        return Side.SHORT
    return Side.FLAT


def _side_quantities(positions: Sequence[Position], side: Side) -> tuple[Decimal, Decimal]:
    same = Decimal("0")
    reverse = Decimal("0")
    for position in positions:
        if position.quantity == 0:
            continue
        pos_side = _side_from_position(position)
        qty = abs(position.quantity)
        if pos_side is side:
            same += qty
        elif pos_side is not Side.FLAT:
            reverse += qty
    return same, reverse


def _has_stop_at_price(orders, stop_price: Decimal) -> bool:
    for order in orders:
        if order.price is not None and order.price == stop_price:
            return True
    return False


def _dec_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _dec_or_zero(value: Any) -> Decimal:
    return _dec_or_none(value) or Decimal("0")
