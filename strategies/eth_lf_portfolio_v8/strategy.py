from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety import RecoveryExitOrderValidator, RecoveryExitValidationResult, is_bot_owned_order
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.models import MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.exchanges.models import Order, OrderSide, OrderStatus, Position, PositionSide
from src.platform.markets import get_market_profile
from src.order_management.models import ExchangeOrderResult
from src.platform.snapshot import PlatformSnapshot
from src.signals import SignalAction, TradeSignal
from src.strategy import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
    StrategyRecoveryContext,
    StrategyRecoveryStatus,
)
from strategies.eth_lf_portfolio_v8.domain.models import BarReadyContext, Side, V8DecisionType, V8TradeDecision
from strategies.eth_lf_portfolio_v8.domain.position_state import V8PositionState
from strategies.eth_lf_portfolio_v8.engines.bear_v3 import BearV3OnlyEngine
from strategies.eth_lf_portfolio_v8.engines.bull_reclaim_v2 import BullReclaimV2Engine
from strategies.eth_lf_portfolio_v8.engines.momentum_v3 import MomentumV3Engine
from strategies.eth_lf_portfolio_v8.engines.router import PortfolioRouter
from strategies.eth_lf_portfolio_v8.execution.signal_mapper import SignalMapperConfig, V8SignalMapper
from strategies.eth_lf_portfolio_v8.execution.range_exit import RangeExitConfig, evaluate_range_exit
from strategies.eth_lf_portfolio_v8.execution.sizing import RiskSizingConfig, V8RiskSizer
from strategies.eth_lf_portfolio_v8.execution.stops import initial_stop_from_risk, is_better_stop, protected_stop, validate_exchange_stop
from strategies.eth_lf_portfolio_v8.features.buffer import V8FeatureBuffer
from strategies.eth_lf_portfolio_v8.features.feature_frame import parse_closed_kline, parse_range_aggregate
from strategies.eth_lf_portfolio_v8.features.live_features import V8LiveFeatureBuilder
from strategies.eth_lf_portfolio_v8.features.micro_context import MicroContextConfig, MicroContextEngine


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
FOUR_HOURS_MS = 4 * 60 * 60 * 1000
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class V8Config:
    strategy_id: str
    symbol: str
    data_exchange: str
    runtime_requirements: Mapping[str, Any]
    micro_context: MicroContextConfig
    range_exit: RangeExitConfig
    global_risk_scale: Decimal

    @classmethod
    def from_file(cls, path: str | Path = DEFAULT_CONFIG_PATH) -> "V8Config":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        micro = data.get("micro_context", {})
        range_exit = RangeExitConfig.from_mapping(data.get("range_exit", {}))
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
            range_exit=range_exit,
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
    stop_update_checked_at_ms: int | None = None

    @property
    def risk_per_coin(self) -> Decimal:
        return self.atr * self.initial_atr_mult


@dataclass(frozen=True)
class PendingAddAfterStopUpdatePlan:
    entry: PendingEntryPlan
    exchange_quantities: Mapping[str, Decimal]
    stop_price: Decimal
    add_unit_number: int
    position_qty: Decimal
    position_units: int


class Strategy:
    """AetherEdge live plugin for ETH LF Portfolio V9E range-exit overlay."""

    raw_trade_callbacks_enabled = False
    observer_id = "primary"
    enabled = True

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
        self.exchange_available: dict[str, Decimal] = {}
        self.exchange_equity_updated_at_ms: dict[str, int] = {}
        self.recovery_manual_required = False
        self.recovery_blocking_manual_required = False
        self.pending_entry: PendingEntryPlan | None = None
        self.pending_add_after_stop_update: PendingAddAfterStopUpdatePlan | None = None
        self.bar_ready_events: list[BarReadyContext] = []
        self.last_decision_audit: dict[str, Any] | None = None
        self.recovery_alerts: list[str] = []
        self.stop_safety_alerts: list[str] = []
        self.last_stop_reject_reason: str | None = None
        self.last_stop_reject_metadata: dict[str, Any] | None = None
        self._stop_update_checked_bar_close_time_ms: int | None = None
        self._stop_update_blocked_bar_close_time_ms: int | None = None
        self.recovered = False
        self.started = False

    def runtime_requirements(self) -> Mapping[str, Any]:
        return dict(self.config.runtime_requirements)

    def market_feature_observers(self) -> tuple[object, ...]:
        return (self,)

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        if not self.position.in_pos or not self.position.position_id:
            return ()
        side = {
            Side.LONG: StrategyPositionSide.LONG,
            Side.SHORT: StrategyPositionSide.SHORT,
        }.get(self.position.side, StrategyPositionSide.FLAT)
        active_exchanges = tuple(self.position.open_legs)
        return (
            StrategyPositionSnapshot(
                strategy_id=self.config.strategy_id,
                position_id=self.position.position_id,
                symbol=self.config.symbol,
                side=side,
                status=StrategyPositionStatus.ACTIVE,
                base_quantity=self.position.qty,
                average_entry_price=self.position.avg_entry,
                stop_price=self.position.stop_price,
                engine=self.position.entry_engine,
                entry_time_ms=self.position.entry_time_ms,
                metadata={"active_exchanges": active_exchanges} if active_exchanges else {},
            ),
        )

    def recovery_status(self) -> StrategyRecoveryStatus:
        return StrategyRecoveryStatus(
            blocking_manual_required=self.recovery_blocking_manual_required,
            alerts=tuple(self.recovery_alerts),
        )

    def strategy_identity(self) -> str:
        return self.config.strategy_id

    def decision_audit(self) -> Mapping[str, Any] | None:
        return self.last_decision_audit

    def has_pending_strategy_work(self) -> bool:
        return self.pending_entry is not None

    def capture_startup_preview_state(self) -> object:
        return (
            self.pending_entry,
            set(self.buffer.evaluated_bars),
            len(self.bar_ready_events),
        )

    def restore_startup_preview_state(self, state: object) -> None:
        pending_entry, evaluated_bars, events_len = state  # type: ignore[misc]
        self.pending_entry = pending_entry
        self.buffer.evaluated_bars = set(evaluated_bars)
        del self.bar_ready_events[events_len:]

    async def on_start(self, snapshot: PlatformSnapshot) -> Sequence[TradeSignal]:
        self.started = True
        self._refresh_account_equity(snapshot)
        return []

    async def recover(self, context: StrategyRecoveryContext) -> Sequence[TradeSignal]:
        self.recovered = True
        for snapshot in context.snapshots:
            self._refresh_account_equity(snapshot)
        plans = tuple(context.metadata.get("active_position_plans", ()) if context.metadata else ())
        return self._recover_position_from_plans(snapshots=context.snapshots, plans=plans)

    async def on_account_snapshot(self, snapshot: PlatformSnapshot) -> None:
        self._refresh_account_equity(snapshot)

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
            self._clear_pending_add_after_stop_update(reason="master_close_event")
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
        if signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            return self._handle_stop_order_results(signal=signal, results=results, event_time_ms=event_time_ms)
        if signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS:
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

        if signal.action in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT} and not self._entry_has_master_fill_event(events):
            self._record_entry_fill_failure(signal=signal, results=results, event_time_ms=event_time_ms)
            return []

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

    def _entry_has_master_fill_event(self, events: Sequence[AccountEvent]) -> bool:
        return any(event.exchange.value == self.config.data_exchange for event in events)

    def _record_entry_fill_failure(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
        event_time_ms: int | None,
    ) -> None:
        if self.pending_entry is None:
            return
        master_result = next((result for result in results if result.exchange.value == self.config.data_exchange), None)
        if master_result is None:
            detail = "missing_master_order_result"
        elif not master_result.ok:
            detail = master_result.error or "master_order_result_failed"
        elif master_result.avg_fill_price is None or master_result.avg_fill_price <= 0:
            detail = "missing_avg_fill_price"
        elif master_result.filled_quantity is None or master_result.filled_quantity <= 0:
            detail = "missing_filled_quantity"
        else:
            detail = "master_fill_not_confirmed"
        self.recovery_manual_required = True
        self.recovery_blocking_manual_required = True
        self.recovery_alerts.append(f"entry_real_fill_missing_manual_required:{detail}")
        self.pending_entry = None
        self._clear_pending_add_after_stop_update(reason="entry_fill_failure")
        logger.critical(
            "Entry real fill missing | action=%s detail=%s event_time_ms=%s",
            signal.action.value,
            detail,
            event_time_ms,
        )

    def _handle_stop_order_results(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
        event_time_ms: int | None,
    ) -> list[TradeSignal]:
        target_exchanges = _target_exchanges(signal)
        if not target_exchanges:
            target_exchanges = tuple(result.exchange.value for result in results)
        successful = [
            result
            for result in results
            if result.exchange.value in target_exchanges
            and result.ok
            and result.status in {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}
            and (result.order_id is not None or result.client_order_id is not None)
        ]
        if target_exchanges and len(successful) == len(set(target_exchanges)):
            stop_price = self.position.desired_stop_price or signal.trigger_price
            initial_stop_pending = self.position.confirmed_stop_price is None
            master_metadata_ok = True
            for result in successful:
                if result.exchange.value == self.config.data_exchange:
                    master_metadata_ok = self._validate_master_position_reconcile_metadata(
                        result=result,
                        event_time_ms=event_time_ms,
                    )
                    if not master_metadata_ok:
                        break
            if not master_metadata_ok:
                self.position.reject_pending_stop_replace()
                self._clear_pending_add_after_stop_update(reason="stop_update_metadata_rejected")
                return []
            if stop_price is not None:
                self.position.confirm_pending_stop_replace(stop_price=stop_price)
            for result in successful:
                self._reconcile_master_position_from_exchange_result(
                    result=result,
                    event_time_ms=event_time_ms,
                    initial_stop_pending=initial_stop_pending,
                )
            return self._deferred_add_after_confirmed_stop_update_signals()
        self.position.reject_pending_stop_replace()
        self._clear_pending_add_after_stop_update(reason="stop_update_failed")
        self.recovery_manual_required = True
        self.recovery_blocking_manual_required = True
        errors = [
            result.error or (result.status.value if result.status else "unknown")
            for result in results
            if not result.ok or result.status in {OrderStatus.CANCELED, OrderStatus.REJECTED}
        ]
        self.recovery_alerts.append(f"stop_replace_failed_manual_required:{','.join(errors) or 'missing_success'}")
        logger.critical(
            "Stop replace failed | action=%s trigger_price=%s event_time_ms=%s target_exchanges=%s errors=%s",
            signal.action.value,
            signal.trigger_price,
            event_time_ms,
            list(target_exchanges),
            errors,
        )
        return []

    def _validate_master_position_reconcile_metadata(
        self,
        *,
        result: ExchangeOrderResult,
        event_time_ms: int | None,
    ) -> bool:
        if result.exchange.value != self.config.data_exchange:
            return True

        raw = dict(result.raw)
        source = str(raw.get("exchange_position_source") or "").strip()
        if not source:
            logger.debug(
                "Master exchange position reconcile metadata source missing; allowing legacy stop confirmation | exchange=%s raw_keys=%s event_time_ms=%s",
                result.exchange.value,
                sorted(raw),
                event_time_ms,
            )
            return True
        if source != "stop_post_check":
            return True

        entry_price = _dec_or_none(raw.get("exchange_position_entry_price"))
        base_quantity = _dec_or_none(raw.get("exchange_position_base_quantity"))
        exchange_side = str(raw.get("exchange_position_side") or "").strip().lower()
        local_side = _side_label(self.position.side)

        if entry_price is None or entry_price <= 0:
            self.recovery_manual_required = True
            self.recovery_blocking_manual_required = True
            self.recovery_alerts.append("master_position_entry_price_missing_manual_required")
            logger.critical(
                "Master exchange position entry price missing before stop confirm | exchange=%s raw_keys=%s event_time_ms=%s",
                result.exchange.value,
                sorted(raw),
                event_time_ms,
            )
            return False
        if base_quantity is None or base_quantity <= 0:
            self.recovery_manual_required = True
            self.recovery_blocking_manual_required = True
            self.recovery_alerts.append("master_position_quantity_missing_manual_required")
            logger.critical(
                "Master exchange position quantity missing before stop confirm | exchange=%s event_time_ms=%s",
                result.exchange.value,
                event_time_ms,
            )
            return False
        if exchange_side and exchange_side != local_side:
            self.recovery_manual_required = True
            self.recovery_blocking_manual_required = True
            self.recovery_alerts.append("master_position_side_mismatch_manual_required")
            logger.critical(
                "Master exchange position side mismatch before stop confirm | local_side=%s exchange_side=%s event_time_ms=%s",
                local_side,
                exchange_side,
                event_time_ms,
            )
            return False
        return True

    def _reconcile_master_position_from_exchange_result(
        self,
        *,
        result: ExchangeOrderResult,
        event_time_ms: int | None,
        initial_stop_pending: bool = False,
    ) -> None:
        if result.exchange.value != self.config.data_exchange:
            return
        raw = dict(result.raw)
        entry_price = _dec_or_none(raw.get("exchange_position_entry_price"))
        base_quantity = _dec_or_none(raw.get("exchange_position_base_quantity"))
        native_quantity = _dec_or_none(raw.get("exchange_position_native_quantity"))
        exchange_side = str(raw.get("exchange_position_side") or "").strip().lower()
        local_side = _side_label(self.position.side)
        if exchange_side and exchange_side != local_side:
            self.recovery_manual_required = True
            self.recovery_blocking_manual_required = True
            self.recovery_alerts.append("master_position_side_mismatch_manual_required")
            logger.critical(
                "Master exchange position side mismatch | local_side=%s exchange_side=%s event_time_ms=%s",
                local_side,
                exchange_side,
                event_time_ms,
            )
            return

        if entry_price is None or entry_price <= 0:
            self.recovery_manual_required = True
            self.recovery_alerts.append("master_position_entry_price_missing_manual_required")
            logger.warning(
                "Master exchange position entry price missing after stop confirm | exchange=%s raw_keys=%s event_time_ms=%s",
                result.exchange.value,
                sorted(raw),
                event_time_ms,
            )
            return
        if base_quantity is None or base_quantity <= 0:
            self.recovery_manual_required = True
            self.recovery_alerts.append("master_position_quantity_missing_manual_required")
            logger.warning(
                "Master exchange position quantity missing after stop confirm | exchange=%s native_quantity=%s convert_error=%s event_time_ms=%s",
                result.exchange.value,
                native_quantity,
                raw.get("exchange_position_base_quantity_convert_error"),
                event_time_ms,
            )
            return

        old_avg_entry = self.position.avg_entry
        old_qty = self.position.qty
        if old_avg_entry is not None and old_avg_entry > 0 and _relative_diff(old_avg_entry, entry_price) >= Decimal("0.005"):
            self.recovery_manual_required = True
            self.recovery_alerts.append("master_avg_entry_large_diff_manual_required")
            logger.warning(
                "Master exchange avg entry differs from local canonical position | old_avg_entry=%s new_avg_entry=%s event_time_ms=%s",
                old_avg_entry,
                entry_price,
                event_time_ms,
            )
        if old_qty > 0 and _relative_diff(old_qty, base_quantity) >= Decimal("0.005"):
            self.recovery_manual_required = True
            self.recovery_alerts.append("master_qty_large_diff_manual_required")
            logger.warning(
                "Master exchange quantity differs from local canonical position | old_qty=%s new_qty=%s event_time_ms=%s",
                old_qty,
                base_quantity,
                event_time_ms,
            )

        self.position.avg_entry = entry_price
        if self.position.first_entry is None or initial_stop_pending:
            self.position.first_entry = entry_price
        self.position.qty = base_quantity
        self.position.initialize_initial_risk_if_missing()
        master_leg = self.position.legs.get(self.config.data_exchange)
        if master_leg is not None and master_leg.is_open:
            master_leg.avg_fill_price = entry_price
            master_leg.base_qty = base_quantity
            if native_quantity is not None and native_quantity > 0:
                master_leg.native_qty = native_quantity
        logger.info(
            "Master exchange position reconciled | source=master_exchange_position old_avg_entry=%s new_avg_entry=%s old_qty=%s new_qty=%s event_time_ms=%s",
            old_avg_entry,
            entry_price,
            old_qty,
            base_quantity,
            event_time_ms,
        )

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
        self._clear_pending_add_after_stop_update(reason="master_close_order_result")
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
            bar_signals = self._signals_from_ready_context(ready)
            self.last_decision_audit = self._build_decision_audit(ready, bar_signals)
            signals.extend(bar_signals)
            self.buffer.mark_evaluated(close_time_ms)
        return signals

    def _build_decision_audit(
        self,
        context: BarReadyContext,
        signals: Sequence[TradeSignal],
    ) -> dict[str, Any]:
        routed = context.routed_signal
        is_flat = routed.side is Side.FLAT
        selected_engine = "NONE" if is_flat else routed.engine
        selected_feature_key = _feature_key_for_engine(None if is_flat else routed.engine)
        audit_risk_mult = (
            _engine_feature_value(context.engine_features, "momentum", "risk_mult")
            if is_flat
            else routed.risk_mult
        )
        audit_quality_mult = (
            _engine_feature_value(context.engine_features, "momentum", "quality_mult")
            if is_flat
            else routed.quality_mult
        )
        actions = [signal.action.value for signal in signals]

        has_open = any(action in {"open_long", "open_short"} for action in actions)
        has_close = any(action in {"close_long", "close_short"} for action in actions)
        has_stop = any("stop" in action for action in actions)
        range_exit_signal = next((signal for signal in signals if signal.metadata.get("range_exit_triggered") is True), None)
        range_exit_metadata = dict(range_exit_signal.metadata) if range_exit_signal is not None else {}

        reason = "no_signal"
        if not self.started or self.equity is None:
            reason = "strategy_not_started"
        elif self.recovery_manual_required:
            reason = "recovery_manual_required"
        elif self.position.in_pos:
            if signals:
                if has_close:
                    reason = "position_close_signal"
                elif has_stop:
                    reason = "position_stop_update"
                else:
                    reason = "position_signal"
            else:
                reason = "position_hold"
        elif has_open:
            reason = "entry_signal"
        elif signals:
            reason = "non_entry_signal"
        elif self.pending_entry is not None:
            reason = "pending_entry_exists"
        elif not self._cooldown_ok(context.kline.close_time_ms):
            reason = "cooldown"
        elif routed is None or routed.side is Side.FLAT:
            reason = "flat_route"
        elif context.micro.entry_risk_scale <= 0:
            reason = "micro_blocked"

        aggregate = context.range_aggregate
        range_min_required = self.config.micro_context.min_range_bars
        range_bar_count = None if aggregate is None else aggregate.bar_count
        if aggregate is None:
            range_available = False
            range_status = "unavailable"
        elif aggregate.bar_count < range_min_required:
            range_available = False
            range_status = "insufficient"
        else:
            range_available = True
            range_status = "ok"

        return {
            "strategy_id": self.config.strategy_id,
            "symbol": self.config.symbol,
            "bar_open_time_ms": context.kline.open_time_ms,
            "bar_close_time_ms": context.kline.close_time_ms,
            "signal_count": len(signals),
            "actions": actions,
            "reason": reason,

            "position_in_pos": self.position.in_pos,
            "position_side": _side_label(self.position.side),
            "position_engine": self.position.entry_engine,
            "position_qty": str(self.position.qty),
            "position_stop": None if self.position.stop_price is None else str(self.position.stop_price),
            "pending_entry": self.pending_entry is not None,
            "stop_reject_reason": self.last_stop_reject_reason,
            "stop_reject_metadata": self.last_stop_reject_metadata,

            "open": str(context.kline.open),
            "high": str(context.kline.high),
            "low": str(context.kline.low),
            "close": str(context.kline.close),
            "volume": str(context.kline.volume),
            "signal": int(routed.side.value),
            "selected_engine": selected_engine,
            "selected_side": _side_label(routed.side),
            "selected_priority": 0 if is_flat else int(routed.priority),
            "risk_mult": str(audit_risk_mult if audit_risk_mult is not None else Decimal("1")),
            "quality_mult": str(audit_quality_mult if audit_quality_mult is not None else Decimal("1")),
            "momentum_signal": _engine_signal(context.engine_features.get("momentum")),
            "bear_signal": _engine_signal(context.engine_features.get("bear")),
            "bull_signal": _engine_signal(context.engine_features.get("bull")),
            "momentum_selected": selected_engine == "MOMENTUM_V3",
            "bear_only": selected_engine == "BEAR_V3_ONLY",
            "bull_reclaim": selected_engine == "BULL_RECLAIM_V2",
            "long_signal": routed.side is Side.LONG,
            "short_signal": routed.side is Side.SHORT,

            "micro_context_available": context.micro.context_available,
            "micro_aligned": context.micro.aligned,
            "micro_contra": context.micro.contra,
            "micro_entry_risk_scale": str(context.micro.entry_risk_scale),
            "micro_filter_action": context.micro.action,
            "micro_action": context.micro.action,

            "atr": _string_or_none(_engine_feature_value(context.engine_features, selected_feature_key, "atr")),
            "atr_pct": _string_or_none(_engine_feature_value(context.engine_features, selected_feature_key, "atr_pct")),
            "adx": _string_or_none(_engine_feature_value(context.engine_features, selected_feature_key, "adx")),
            "momentum_long_exit_channel": _engine_feature_bool_value(context.engine_features, "momentum", "long_exit_channel"),
            "momentum_short_exit_channel": _engine_feature_bool_value(context.engine_features, "momentum", "short_exit_channel"),
            "bear_short_exit_channel": _engine_feature_bool_value(context.engine_features, "bear", "short_exit_channel"),
            "bull_long_exit_channel": _engine_feature_bool_value(context.engine_features, "bull", "long_exit_channel"),

            "range_available": range_available,
            "range_status": range_status,
            "range_bar_count": range_bar_count,
            "range_min_required": range_min_required,
            "range_imbalance": None if not range_available else str(aggregate.imbalance),
            "range_taker_buy_ratio": None if not range_available else str(aggregate.taker_buy_ratio),
            "range_close_pos": None if not range_available else str(aggregate.close_pos),
            "range_micro_return_pct": None if not range_available else str(aggregate.micro_return_pct),
            "rf_bar_count": range_bar_count,
            "rf_micro_return_pct": None if aggregate is None else str(aggregate.micro_return_pct),
            "rf_close_pos": None if aggregate is None else str(aggregate.close_pos),
            "rf_delta_sum": None if aggregate is None else str(aggregate.delta_notional_sum),
            "rf_imbalance": None if aggregate is None else str(aggregate.imbalance),
            "rf_taker_buy_ratio": None if aggregate is None else str(aggregate.taker_buy_ratio),
            "range_exit_triggered": bool(range_exit_metadata.get("range_exit_triggered", False)),
            "range_exit_reason": range_exit_metadata.get("range_exit_reason", ""),
            "range_exit_peak_r": range_exit_metadata.get("range_exit_peak_r"),
            "range_exit_current_r": range_exit_metadata.get("range_exit_current_r"),
            "range_exit_giveback_frac": range_exit_metadata.get("range_exit_giveback_frac"),
        }

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
        position_id = f"v9e-{context.kline.close_time_ms}-{routed.engine}-{routed.side.name.lower()}"
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
                    "micro_entry_risk_scale": str(context.micro.entry_risk_scale),
                    "await_master_fill_before_stop": True,
                    "execution_purpose": "normal_entry",
                    "position_id": position_id,
                    "target_exchanges": sorted(exchange_quantities),
                    "exchange_quantities_base": _exchange_quantity_metadata(exchange_quantities),
                    **self._sizing_equity_metadata(exchange_quantities),
                },
            )
        )

    def _position_lifecycle_signals(self, context: BarReadyContext) -> list[TradeSignal]:
        self._clear_stale_pending_add_after_stop_update(context)
        self.position.update_favorable_extremes(high=context.kline.high, low=context.kline.low)
        self._stop_update_checked_bar_close_time_ms = None
        close_decision = self._close_decision_if_needed(context)
        if close_decision is not None:
            self._clear_pending_add_after_stop_update(reason="position_close_decision")
            return self.signal_mapper.map_decision(close_decision)
        stop_signals = self._stop_update_signals_if_needed(context)
        if stop_signals:
            self._defer_add_after_stop_update_if_needed(context)
            return stop_signals
        if self.position.pending_stop_replace:
            return []
        if self._stop_update_blocked_bar_close_time_ms == context.kline.close_time_ms:
            return []
        self._stop_update_checked_bar_close_time_ms = context.kline.close_time_ms
        add_signals = self._add_signal_if_needed(context)
        if add_signals:
            return add_signals
        return []

    def _close_decision_if_needed(self, context: BarReadyContext) -> V8TradeDecision | None:
        if not self.position.in_pos or self.position.side is Side.FLAT or self.position.qty <= 0:
            return None
        params = self.engine_params.get(self.position.entry_engine)
        hold_bars = self._holding_bars(context.kline.close_time_ms)
        exit_channel = _entry_engine_exit_channel(context, self.position.entry_engine, self.position.side)
        opposite = context.routed_signal.side is not Side.FLAT and context.routed_signal.side is not self.position.side
        max_hold = params is not None and hold_bars is not None and hold_bars >= params.max_hold_bars
        range_exit = None
        if not exit_channel and not opposite and hold_bars is not None and self.position.avg_entry is not None and self.position.risk_per_coin is not None:
            aggregate = context.range_aggregate
            range_context_available = (
                aggregate is not None
                and str(aggregate.coverage_status).strip().upper() == "COMPLETE"
                and aggregate.bar_count >= self.config.micro_context.min_range_bars
            )
            range_exit = evaluate_range_exit(
                side=self.position.side,
                avg_entry=self.position.avg_entry,
                risk_per_coin=self.position.risk_per_coin,
                max_fav=self.position.max_fav,
                hold_bars=hold_bars,
                close=context.kline.close,
                micro_context_available=range_context_available,
                rf_imbalance=None if aggregate is None else aggregate.imbalance,
                rf_close_pos=None if aggregate is None else aggregate.close_pos,
                config=self.config.range_exit,
            )
        range_exit_now = bool(range_exit is not None and range_exit.should_exit)
        if not exit_channel and not opposite and not range_exit_now and not max_hold:
            return None
        reason = (
            "V8_CHANNEL_EXIT"
            if exit_channel
            else "V8_OPPOSITE_SIGNAL_EXIT"
            if opposite
            else range_exit.reason
            if range_exit_now and range_exit is not None
            else "V8_MAX_HOLD_EXIT"
        )
        exchange_quantities = self._open_leg_quantities()
        quantity = exchange_quantities.get(self.config.data_exchange, self.position.qty)
        range_exit_metadata = dict(range_exit.metadata) if range_exit_now and range_exit is not None else {}
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
                **range_exit_metadata,
            },
        )

    def _add_signal_if_needed(self, context: BarReadyContext) -> list[TradeSignal]:
        plan = self._build_add_plan_if_needed(
            context,
            stop_update_checked_at_ms=self._stop_update_checked_bar_close_time_ms,
        )
        if plan is None:
            return []
        self.pending_entry = plan.entry
        return self._add_signals_from_plan(plan, deferred_after_stop_update=False)

    def _build_add_plan_if_needed(
        self,
        context: BarReadyContext,
        *,
        stop_update_checked_at_ms: int | None,
    ) -> PendingAddAfterStopUpdatePlan | None:
        if self.pending_entry is not None or not self.position.in_pos or self.position.risk_per_coin is None:
            return None
        params = self.engine_params.get(self.position.entry_engine)
        if params is None or self.position.units >= params.max_units or self.position.first_entry is None:
            return None
        trigger_r = Decimal(str(self.position.units)) * params.add_every_r
        if self.position.side is Side.LONG:
            triggered = context.kline.high >= self.position.first_entry + trigger_r * self.position.risk_per_coin
        else:
            triggered = context.kline.low <= self.position.first_entry - trigger_r * self.position.risk_per_coin
        if not triggered:
            return None
        atr_value = _feature_decimal(context, self.position.entry_engine, "atr")
        if atr_value is None or atr_value <= 0:
            return None
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
            return None
        position_id = self.position.position_id or f"v9e-add-{context.kline.close_time_ms}-{self.position.entry_engine}-{self.position.side.name.lower()}"
        entry = PendingEntryPlan(
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
            stop_update_checked_at_ms=stop_update_checked_at_ms,
        )
        return PendingAddAfterStopUpdatePlan(
            entry=entry,
            exchange_quantities=dict(exchange_quantities),
            stop_price=estimated_stop,
            add_unit_number=self.position.units + 1,
            position_qty=self.position.qty,
            position_units=self.position.units,
        )

    def _add_signals_from_plan(
        self,
        plan: PendingAddAfterStopUpdatePlan,
        *,
        deferred_after_stop_update: bool,
    ) -> list[TradeSignal]:
        metadata: dict[str, Any] = {
            "add_unit_number": plan.add_unit_number,
            "micro_entry_risk_scale_applied": False,
            "micro_entry_risk_scale": "1",
            "execution_purpose": "normal_entry",
            "position_id": plan.entry.position_id,
            "target_exchanges": sorted(plan.exchange_quantities),
            "exchange_quantities_base": _exchange_quantity_metadata(plan.exchange_quantities),
            **self._sizing_equity_metadata(plan.exchange_quantities),
        }
        if deferred_after_stop_update:
            metadata.update(
                {
                    "deferred_after_stop_update": True,
                    "stop_update_confirmed_before_add": True,
                    "stop_update_checked_at_ms": plan.entry.stop_update_checked_at_ms,
                }
            )
        return self.signal_mapper.map_decision(
            V8TradeDecision(
                decision_type=V8DecisionType.ADD,
                side=plan.entry.side,
                symbol=self.config.symbol,
                quantity=plan.entry.quantity,
                stop_price=plan.stop_price,
                engine=plan.entry.engine,
                reason="V8_ADD_UNIT",
                bar_close_time_ms=plan.entry.bar_close_time_ms,
                entry_risk_scale=plan.entry.entry_risk_scale,
                risk_mult=plan.entry.risk_mult,
                quality_mult=plan.entry.quality_mult,
                metadata=metadata,
            )
        )

    def _defer_add_after_stop_update_if_needed(self, context: BarReadyContext) -> None:
        plan = self._build_add_plan_if_needed(
            context,
            stop_update_checked_at_ms=context.kline.close_time_ms,
        )
        if plan is None:
            self._clear_pending_add_after_stop_update(reason="no_same_bar_add_plan")
            return
        self.pending_add_after_stop_update = plan

    def _deferred_add_after_confirmed_stop_update_signals(self) -> list[TradeSignal]:
        plan = self.pending_add_after_stop_update
        if plan is None:
            return []
        self.pending_add_after_stop_update = None
        if self.pending_entry is not None:
            return []
        if not self.position.in_pos or self.position.side is not plan.entry.side:
            return []
        if self.position.position_id != plan.entry.position_id:
            return []
        if self.position.entry_engine != plan.entry.engine:
            return []
        if self.position.pending_stop_replace:
            return []
        if self.recovery_blocking_manual_required:
            return []
        if self.position.units != plan.position_units or self.position.qty != plan.position_qty:
            return []
        self.pending_entry = plan.entry
        return self._add_signals_from_plan(plan, deferred_after_stop_update=True)

    def _clear_pending_add_after_stop_update(self, *, reason: str) -> None:
        if self.pending_add_after_stop_update is not None:
            logger.info("Clearing deferred add-after-stop-update plan | reason=%s", reason)
        self.pending_add_after_stop_update = None

    def _clear_stale_pending_add_after_stop_update(self, context: BarReadyContext) -> None:
        plan = self.pending_add_after_stop_update
        if plan is None:
            return
        stale = (
            plan.entry.bar_close_time_ms != context.kline.close_time_ms
            or not self.position.in_pos
            or self.position.side is not plan.entry.side
            or self.position.position_id != plan.entry.position_id
            or self.position.entry_engine != plan.entry.engine
        )
        if stale:
            self._clear_pending_add_after_stop_update(reason="stale_or_position_changed")

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
        exchange_quantities = self._open_leg_quantities()
        target_exchanges = sorted(exchange_quantities)
        if not target_exchanges:
            target_exchanges = [self.config.data_exchange]
            exchange_quantities = {self.config.data_exchange: self.position.qty}
        signals = self._replace_stop_signals(
            target_exchanges=target_exchanges,
            quantity=exchange_quantities.get(self.config.data_exchange, self.position.qty),
            stop_price=candidate,
            reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
            bar_close_time_ms=context.kline.close_time_ms,
            exchange_quantities=exchange_quantities,
            reference_price=context.kline.close,
        )
        if signals:
            self.position.mark_pending_stop_replace(
                desired_stop_price=candidate,
                reason="V8_PROTECTED_TRAILING_STOP_UPDATE",
                bar_close_time_ms=context.kline.close_time_ms,
            )
        return signals

    def _handle_master_entry_fill(self, *, event: AccountEvent, filled_qty: Decimal) -> list[TradeSignal]:
        assert self.pending_entry is not None
        if event.price is None or event.price <= 0 or filled_qty <= 0:
            self._record_entry_fill_failure(
                signal=TradeSignal(
                    symbol=self.config.symbol,
                    action=SignalAction.OPEN_LONG if self.pending_entry.side is Side.LONG else SignalAction.OPEN_SHORT,
                    quantity=self.pending_entry.quantity,
                ),
                results=(),
                event_time_ms=event.event_time_ms,
            )
            return []
        exchange = event.exchange.value
        base_filled_qty = filled_qty if event.raw.get("quantity_semantics") == "base_asset" else self.pending_entry.quantity
        native_filled_qty = None if base_filled_qty == filled_qty else filled_qty
        is_add_fill = self.pending_entry.is_add and self.position.in_pos
        add_stop_checked = (
            is_add_fill
            and self.pending_entry.stop_update_checked_at_ms == self.pending_entry.bar_close_time_ms
        )
        if is_add_fill:
            self.position.add_master_fill(avg_fill_price=event.price, add_qty=base_filled_qty)  # type: ignore[arg-type]
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
                qty=base_filled_qty,
                stop_price=stop_price,
                entry_engine=self.pending_entry.engine,
                entry_risk_mult=self.pending_entry.entry_risk_scale,
                position_id=self.pending_entry.position_id,
                stop_confirmed=False,
            )
        self.position.mark_leg_open(
            exchange=exchange,
            avg_fill_price=event.price,  # type: ignore[arg-type]
            base_qty=base_filled_qty if not self.pending_entry.is_add else self.position.qty,
            native_qty=native_filled_qty,
            order_id=event.order_id,
            client_order_id=event.client_order_id,
        )
        if is_add_fill and not add_stop_checked:
            self._record_stop_reject(
                reason="add_fill_stop_update_not_checked",
                stop_price=self.position.stop_price,
                reference_price=event.price,
                bar_close_time_ms=event.event_time_ms,
                signal_reason="MASTER_ADD_FILLED_REPLACE_STOP",
            )
            self.pending_entry = None
            return []
        self.pending_entry = None
        stop_signal_price = self.position.desired_stop_price or self.position.stop_price
        if stop_signal_price is None:
            return []
        return self._replace_stop_signals(
            target_exchanges=[exchange],
            quantity=self.position.qty,
            stop_price=stop_signal_price,
            reason="MASTER_ENTRY_FILLED_REPLACE_STOP",
            bar_close_time_ms=event.event_time_ms,
            reference_price=event.price,
        )

    def _handle_follower_entry_fill(self, *, event: AccountEvent, filled_qty: Decimal) -> list[TradeSignal]:
        exchange = event.exchange.value
        was_add_fill = exchange in self.position.open_legs and self.position.open_legs[exchange].base_qty > 0
        self.position.add_leg_fill(
            exchange=exchange,
            avg_fill_price=event.price,  # type: ignore[arg-type]
            add_base_qty=filled_qty,
            order_id=event.order_id,
            client_order_id=event.client_order_id,
        )
        if was_add_fill:
            self._record_stop_reject(
                reason="follower_add_fill_stop_update_not_checked",
                stop_price=self.position.stop_price,
                reference_price=event.price,
                bar_close_time_ms=event.event_time_ms,
                signal_reason="FOLLOWER_ADD_FILLED_REPLACE_STOP",
            )
            return []
        stop_signal_price = self.position.desired_stop_price or self.position.stop_price
        if stop_signal_price is None:
            return []
        leg_qty = self.position.legs[exchange].base_qty
        return self._replace_stop_signals(
            target_exchanges=[exchange],
            quantity=leg_qty,
            stop_price=stop_signal_price,
            reason="FOLLOWER_ENTRY_FILLED_REPLACE_STOP",
            bar_close_time_ms=event.event_time_ms,
            reference_price=event.price,
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
        market_profile = get_market_profile(self.config.symbol)
        converter = NativeQuantityConverter()
        validator = RecoveryExitOrderValidator(quantity_converter=converter)
        master_native_qty = abs(master.quantity)
        qty = converter.native_to_base_quantity(
            exchange=master.exchange,
            symbol=self.config.symbol,
            native_quantity=master_native_qty,
            market_profile=market_profile,
        )
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
        self.position.mark_leg_open(exchange=self.config.data_exchange, avg_fill_price=entry_price, base_qty=qty, native_qty=master_native_qty, sync_status="recovered_master")
        signals: list[TradeSignal] = []
        master_validation = validator.validate_stop_orders(
            exchange=master.exchange,
            symbol=self.config.symbol,
            strategy_id=self.config.strategy_id,
            position_id=self.position.position_id,
            position_side=_position_side_for_strategy_side(side),
            position_mode=master_snapshot.position_mode,
            current_position_native_quantity=master_native_qty,
            canonical_stop_price=stop_price,
            open_stop_orders=master_snapshot.open_stop_orders,
            open_orders=master_snapshot.open_orders,
            market_profile=market_profile,
        )
        signals.extend(
            self._signals_from_recovery_exit_validation(
                validation=master_validation,
                exchange=self.config.data_exchange,
                quantity=qty,
                stop_price=stop_price,
                reason="RECOVERY_MASTER_STOP_SYNC",
            )
        )
        for leg in legs:
            exchange = str(leg.get("exchange") or "").lower()
            if not exchange or exchange == self.config.data_exchange:
                continue
            target_qty = _dec_or_zero(leg.get("target_qty_base"))
            follower_snapshot = snapshots.get(exchange)
            same_qty, same_native_qty, reverse_qty, _reverse_native_qty = _side_quantities_with_native(
                follower_snapshot.positions if follower_snapshot else [],
                side,
                market_profile=market_profile,
            )
            if reverse_qty > 0:
                self.position.legs[exchange] = self.position.legs.get(exchange) or self.position.mark_leg_closed(exchange=exchange, sync_status="reverse_position_manual_required")
                self.position.legs[exchange].sync_status = "reverse_position_manual_required"
                self.recovery_manual_required = True
                self.recovery_blocking_manual_required = True
                self.recovery_alerts.append(f"follower_reverse_position:{exchange}")
                self.recovery_alerts.append(f"follower_position_side_mismatch_manual_required:{exchange}")
                logger.critical(
                    "Follower position side mismatch blocks stop repair | exchange=%s master_side=%s reverse_base_quantity=%s reverse_native_quantity=%s position_id=%s",
                    exchange,
                    _side_label(side),
                    reverse_qty,
                    _reverse_native_qty,
                    self.position.position_id,
                )
                continue
            if same_qty <= 0 and target_qty > 0:
                self.position.mark_leg_closed(exchange=exchange, sync_status="missing")
                signals.extend(
                    self._cleanup_missing_follower_stops(
                        snapshot=follower_snapshot,
                        exchange=exchange,
                        position_id=self.position.position_id,
                    )
                )
                self.recovery_alerts.append(f"follower_missing_manual_required:{exchange}")
                if not self.recovery_manual_required:
                    signals.append(self._follower_topup_signal(exchange=exchange, side=side, quantity=target_qty, plan=plan))
            elif same_qty < target_qty:
                self.position.mark_leg_open(exchange=exchange, avg_fill_price=entry_price, base_qty=same_qty, native_qty=same_native_qty, sync_status="underfilled")
                if follower_snapshot is not None:
                    follower_validation = validator.validate_stop_orders(
                        exchange=follower_snapshot.balance.exchange,
                        symbol=self.config.symbol,
                        strategy_id=self.config.strategy_id,
                        position_id=self.position.position_id,
                        position_side=_position_side_for_strategy_side(side),
                        position_mode=follower_snapshot.position_mode,
                        current_position_native_quantity=same_native_qty,
                        canonical_stop_price=stop_price,
                        open_stop_orders=follower_snapshot.open_stop_orders,
                        open_orders=follower_snapshot.open_orders,
                        market_profile=market_profile,
                    )
                    signals.extend(
                        self._signals_from_recovery_exit_validation(
                            validation=follower_validation,
                            exchange=exchange,
                            quantity=same_qty,
                            stop_price=stop_price,
                            reason="RECOVERY_FOLLOWER_STOP_SYNC",
                        )
                    )
                signals.append(self._follower_topup_signal(exchange=exchange, side=side, quantity=target_qty - same_qty, plan=plan))
            elif same_qty > target_qty and target_qty > 0:
                self.position.mark_leg_open(exchange=exchange, avg_fill_price=entry_price, base_qty=same_qty, native_qty=same_native_qty, sync_status="overfilled")
                self.recovery_alerts.append(f"follower_overfilled:{exchange}")
                if follower_snapshot is not None:
                    follower_validation = validator.validate_stop_orders(
                        exchange=follower_snapshot.balance.exchange,
                        symbol=self.config.symbol,
                        strategy_id=self.config.strategy_id,
                        position_id=self.position.position_id,
                        position_side=_position_side_for_strategy_side(side),
                        position_mode=follower_snapshot.position_mode,
                        current_position_native_quantity=same_native_qty,
                        canonical_stop_price=stop_price,
                        open_stop_orders=follower_snapshot.open_stop_orders,
                        open_orders=follower_snapshot.open_orders,
                        market_profile=market_profile,
                    )
                    signals.extend(
                        self._signals_from_recovery_exit_validation(
                            validation=follower_validation,
                            exchange=exchange,
                            quantity=same_qty,
                            stop_price=stop_price,
                            reason="RECOVERY_FOLLOWER_STOP_SYNC",
                        )
                    )
            elif same_qty > 0:
                self.position.mark_leg_open(exchange=exchange, avg_fill_price=entry_price, base_qty=same_qty, native_qty=same_native_qty, sync_status="synced")
                if follower_snapshot is not None:
                    follower_validation = validator.validate_stop_orders(
                        exchange=follower_snapshot.balance.exchange,
                        symbol=self.config.symbol,
                        strategy_id=self.config.strategy_id,
                        position_id=self.position.position_id,
                        position_side=_position_side_for_strategy_side(side),
                        position_mode=follower_snapshot.position_mode,
                        current_position_native_quantity=same_native_qty,
                        canonical_stop_price=stop_price,
                        open_stop_orders=follower_snapshot.open_stop_orders,
                        open_orders=follower_snapshot.open_orders,
                        market_profile=market_profile,
                    )
                    signals.extend(
                        self._signals_from_recovery_exit_validation(
                            validation=follower_validation,
                            exchange=exchange,
                            quantity=same_qty,
                            stop_price=stop_price,
                            reason="RECOVERY_FOLLOWER_STOP_SYNC",
                        )
                    )
        return signals

    def _recover_active_master_without_plan(self, master: Position) -> None:
        side = _side_from_position(master)
        if side is Side.FLAT:
            return
        entry_price = master.entry_price or Decimal("1")
        market_profile = get_market_profile(self.config.symbol)
        native_qty = abs(master.quantity)
        qty = NativeQuantityConverter().native_to_base_quantity(
            exchange=master.exchange,
            symbol=self.config.symbol,
            native_quantity=native_qty,
            market_profile=market_profile,
        )
        self.position.in_pos = True
        self.position.side = side
        self.position.entry_time_ms = 0
        self.position.first_entry = entry_price
        self.position.avg_entry = entry_price
        self.position.qty = qty
        self.position.units = 1
        self.position.entry_engine = "unknown"
        self.position.stop_price = None
        self.position.risk_per_coin = None
        self.position.mark_leg_open(exchange=self.config.data_exchange, avg_fill_price=entry_price, base_qty=qty, native_qty=native_qty, sync_status="master_active_plan_unknown")
        self.recovery_manual_required = True
        self.recovery_blocking_manual_required = True
        self.recovery_alerts.append("master_active_plan_unknown_manual_required")
        self.recovery_alerts.append("active_master_without_position_plan_blocking")

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

    def _signals_from_recovery_exit_validation(
        self,
        *,
        validation: RecoveryExitValidationResult,
        exchange: str,
        quantity: Decimal,
        stop_price: Decimal,
        reason: str,
    ) -> list[TradeSignal]:
        signals: list[TradeSignal] = []
        for check in validation.checks:
            action = (
                "cancel_replace"
                if check.bot_owned and validation.should_cancel_and_replace_bot_stops
                else "keep"
                if check.valid
                else "alert_manual_required"
                if not check.bot_owned
                else "place_new_stop"
            )
            logger.info("Recovery exit order validation | %s", check.log_fields(action=action))
        # ── Non-blocking: unknown/manual stop exists but bot can place its own ──
        #     valid stop alongside. Alert the operator but do NOT block startup.
        if validation.unknown_exit_orders:
            for order in validation.unknown_exit_orders:
                self.recovery_alerts.append(f"unknown_exit_order_manual_required:{exchange}:{order.order_id or order.client_order_id or 'unknown'}")
        # ── Blocking: unsupported bot exit orders (take-profit / trailing) ──
        if validation.unsupported_bot_exit_orders:
            self.recovery_manual_required = True
            self.recovery_blocking_manual_required = True
            for order in validation.unsupported_bot_exit_orders:
                self.recovery_alerts.append(f"unsupported_take_profit_or_trailing_manual_required:{exchange}:{order.order_id or order.client_order_id or 'unknown'}")
        if any(check.order is None and check.invalid_reason == "missing_bot_owned_stop" for check in validation.checks):
            if exchange == self.config.data_exchange:
                self.recovery_manual_required = True
                self.recovery_blocking_manual_required = True
                self.recovery_alerts.append(f"critical_stop_missing_while_in_position_manual_required:{exchange}")
                logger.critical(
                    "Stop missing while position is active | exchange=%s position_id=%s quantity=%s stop_price=%s",
                    exchange,
                    self.position.position_id,
                    quantity,
                    stop_price,
                )
            else:
                logger.warning(
                    "Follower stop missing; scheduling repair | exchange=%s position_id=%s quantity=%s stop_price=%s",
                    exchange,
                    self.position.position_id,
                    quantity,
                    stop_price,
                )
        if validation.should_keep_existing_stop:
            return signals
        repair_metadata = self._follower_stop_repair_metadata(validation=validation, exchange=exchange)
        if validation.should_cancel_and_replace_bot_stops:
            action = "manual_required" if validation.has_unknown_exit_orders else "cancel_replace"
            logger.info(
                "Recovery exit order resync | reason=%s valid_bot_stop_count=%s invalid_bot_stop_count=%s unknown_stop_count=%s action=%s",
                validation.primary_invalid_reason,
                len(validation.valid_bot_owned_orders),
                len(validation.invalid_bot_owned_orders),
                len(validation.unknown_exit_orders),
                action,
            )
            if validation.has_unknown_exit_orders:
                # ── Blocking: unknown stop prevents precise cancel of invalid bot stops ──
                self.recovery_manual_required = True
                self.recovery_blocking_manual_required = True
                self.recovery_alerts.append(f"critical_recovery_exit_order_manual_required:{exchange}:unknown_stop_blocks_cancel_all")
                logger.critical(
                    "Recovery exit order manual required | reason=unknown_stop_blocks_cancel_all exchange=%s position_id=%s invalid_bot_stop_count=%s unknown_stop_count=%s",
                    exchange,
                    self.position.position_id,
                    len(validation.invalid_bot_owned_orders),
                    len(validation.unknown_exit_orders),
                )
                return signals
            signals.extend(
                self._replace_stop_signals(
                    target_exchanges=[exchange],
                    quantity=quantity,
                    stop_price=stop_price,
                    reason=reason,
                    bar_close_time_ms=None,
                    metadata_overrides=repair_metadata,
                )
            )
            cancel_count = len(validation.bot_owned_orders)
        else:
            signals.extend(
                self._place_stop_signals(
                    target_exchanges=[exchange],
                    quantity=quantity,
                    stop_price=stop_price,
                    reason=reason,
                    bar_close_time_ms=None,
                    metadata_overrides=repair_metadata,
                )
            )
            cancel_count = 0
        logger.info(
            "Recovery exit order resync | exchange=%s reason=%s valid_bot_stop_count=%s invalid_bot_stop_count=%s unknown_stop_count=%s action=%s cancel_count=%s new_stop_base_quantity=%s new_stop_native_quantity_preview=%s stop_price=%s",
            exchange,
            validation.primary_invalid_reason,
            len(validation.valid_bot_owned_orders),
            len(validation.invalid_bot_owned_orders),
            len(validation.unknown_exit_orders),
            "cancel_replace" if cancel_count else "place_new_stop",
            cancel_count,
            quantity,
            validation.expected_native_quantity,
            stop_price,
        )
        return signals

    def _follower_stop_repair_metadata(
        self,
        *,
        validation: RecoveryExitValidationResult,
        exchange: str,
    ) -> dict[str, Any] | None:
        if exchange == self.config.data_exchange:
            return None
        return {
            "execution_purpose": "follower_stop_repair",
            "target_exchanges": [exchange],
            "canonical_source_exchange": self.config.data_exchange,
            "canonical_stop_price": str(validation.canonical_stop_price),
            "follower_position_native_quantity": str(validation.current_position_native_quantity),
            "follower_position_base_quantity": str(validation.current_position_base_quantity),
            "repair_reason": _follower_stop_repair_reason(validation),
        }

    def _cleanup_missing_follower_stops(self, *, snapshot: PlatformSnapshot | None, exchange: str, position_id: str | None) -> list[TradeSignal]:
        if snapshot is None:
            return []
        bot_owned_stops: list[Order] = []
        unknown_stops: list[Order] = []
        for order in snapshot.open_stop_orders:
            if order.symbol != self.config.symbol:
                continue
            if is_bot_owned_order(order=order, strategy_id=self.config.strategy_id, position_id=position_id):
                bot_owned_stops.append(order)
            else:
                unknown_stops.append(order)
        # ── Non-blocking: alert operator about unknown/manual stops on missing follower ──
        for order in unknown_stops:
            self.recovery_alerts.append(f"unknown_exit_order_manual_required:{exchange}:{order.order_id or order.client_order_id or 'unknown'}")
            logger.info(
                "Recovery exit order validation | exchange=%s symbol=%s position_side=None position_mode=%s current_position_base_quantity=0 current_position_native_quantity=0 canonical_stop_price=None existing_order_id=%s existing_client_order_id=%s valid=false invalid_reason=follower_missing_unknown_stop action=alert_manual_required",
                exchange,
                self.config.symbol,
                snapshot.position_mode.value,
                order.order_id,
                order.client_order_id,
            )
        if not bot_owned_stops:
            return []
        if unknown_stops:
            # ── Blocking: unknown stops prevent precise cancel of bot stops ──
            self.recovery_manual_required = True
            self.recovery_blocking_manual_required = True
            self.recovery_alerts.append(f"critical_recovery_exit_order_manual_required:{exchange}:unknown_stop_blocks_cancel_all")
            logger.critical(
                "Recovery exit order manual required | reason=unknown_stop_blocks_cancel_all exchange=%s position_id=%s invalid_bot_stop_count=%s unknown_stop_count=%s",
                exchange,
                position_id,
                len(bot_owned_stops),
                len(unknown_stops),
            )
            return []
        self.recovery_alerts.append(f"no_position_stop_cancelled:{exchange}")
        logger.info(
            "Recovery exit order resync | exchange=%s reason=follower_missing_no_position_stop_cancelled valid_bot_stop_count=0 invalid_bot_stop_count=%s unknown_stop_count=0 action=cancel_replace cancel_count=%s new_stop_base_quantity=0 new_stop_native_quantity_preview=0 stop_price=None",
            exchange,
            len(bot_owned_stops),
            len(bot_owned_stops),
        )
        return [
            self._cancel_stop_signal(
                target_exchanges=[exchange],
                reason="RECOVERY_FOLLOWER_MISSING_CANCEL_STOP",
            )
        ]

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

    def _replace_stop_signals(
        self,
        *,
        target_exchanges: list[str],
        quantity: Decimal,
        stop_price: Decimal,
        reason: str,
        bar_close_time_ms: int | None,
        exchange_quantities: Mapping[str, Decimal] | None = None,
        reference_price: Decimal | None = None,
        metadata_overrides: Mapping[str, Any] | None = None,
    ) -> list[TradeSignal]:
        if not self._stop_is_exchange_valid(
            stop_price=stop_price,
            reference_price=reference_price,
            reason=reason,
            bar_close_time_ms=bar_close_time_ms,
        ):
            return []
        return [
            self._cancel_stop_signal(target_exchanges=target_exchanges, reason=f"{reason}_CANCEL_OLD"),
            *self._place_stop_signals(
                target_exchanges=target_exchanges,
                quantity=quantity,
                stop_price=stop_price,
                reason=reason,
                bar_close_time_ms=bar_close_time_ms,
                exchange_quantities=exchange_quantities,
                reference_price=reference_price,
                metadata_overrides=metadata_overrides,
            ),
        ]

    def _cancel_stop_signal(self, *, target_exchanges: list[str], reason: str) -> TradeSignal:
        return TradeSignal(
            symbol=self.config.symbol,
            action=SignalAction.CANCEL_ALL_STOP_ORDERS,
            reason=reason,
            metadata={
                "target_exchanges": target_exchanges,
                "execution_purpose": "stop_sync",
                "position_id": self.position.position_id,
            },
        )

    def _place_stop_signals(
        self,
        *,
        target_exchanges: list[str],
        quantity: Decimal,
        stop_price: Decimal,
        reason: str,
        bar_close_time_ms: int | None,
        exchange_quantities: Mapping[str, Decimal] | None = None,
        reference_price: Decimal | None = None,
        metadata_overrides: Mapping[str, Any] | None = None,
    ) -> list[TradeSignal]:
        if not self._stop_is_exchange_valid(
            stop_price=stop_price,
            reference_price=reference_price,
            reason=reason,
            bar_close_time_ms=bar_close_time_ms,
        ):
            return []
        exchange_quantities = dict(exchange_quantities or {})
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
                    "replace_mode": "cancel_then_place_validated",
                    "stop_replace_atomic_supported": False,
                    "stop_replace_mode": "cancel_then_place_validated",
                    "stop_replace_non_atomic_reason": "no_targeted_stop_cancel_capability",
                    "desired_stop_price": str(stop_price),
                    "confirmed_stop_price": None if self.position.stop_price is None else str(self.position.stop_price),
                    "position_id": self.position.position_id,
                    **({"exchange_quantities_base": _exchange_quantity_metadata(exchange_quantities)} if exchange_quantities else {}),
                    **dict(metadata_overrides or {}),
                },
            )
        )[0]
        return [stop]

    def _stop_is_exchange_valid(
        self,
        *,
        stop_price: Decimal,
        reference_price: Decimal | None,
        reason: str,
        bar_close_time_ms: int | None,
    ) -> bool:
        if reference_price is None:
            return True
        validation = validate_exchange_stop(
            side=self.position.side,
            stop_price=stop_price,
            reference_price=reference_price,
        )
        if validation.valid:
            return True
        self._record_stop_reject(
            reason=validation.reason,
            stop_price=stop_price,
            reference_price=reference_price,
            bar_close_time_ms=bar_close_time_ms,
            signal_reason=reason,
            buffer=validation.buffer,
        )
        if bar_close_time_ms is not None:
            self._stop_update_blocked_bar_close_time_ms = bar_close_time_ms
        return False

    def _record_stop_reject(
        self,
        *,
        reason: str,
        stop_price: Decimal | None,
        reference_price: Decimal | None,
        bar_close_time_ms: int | None,
        signal_reason: str,
        buffer: Decimal | None = None,
    ) -> None:
        alert = f"invalid_stop:{reason}"
        self.last_stop_reject_reason = alert
        self.last_stop_reject_metadata = {
            "reason": reason,
            "signal_reason": signal_reason,
            "side": _side_label(self.position.side),
            "stop_price": None if stop_price is None else str(stop_price),
            "reference_price": None if reference_price is None else str(reference_price),
            "buffer": None if buffer is None else str(buffer),
            "bar_close_time_ms": bar_close_time_ms,
        }
        self.stop_safety_alerts.append(alert)
        logger.warning(
            "Blocked stop signal | reason=%s signal_reason=%s side=%s stop_price=%s reference_price=%s buffer=%s bar_close_time_ms=%s",
            reason,
            signal_reason,
            _side_label(self.position.side),
            stop_price,
            reference_price,
            buffer,
            bar_close_time_ms,
        )

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
        effective_risk_pct = params.unit_risk_per_trade * risk_mult * quality_mult * micro_entry_risk_scale * self.config.global_risk_scale
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
                logger.info(
                    "V9C entry sizing equity | exchange=%s sizing_equity=%s available_equity=%s equity_updated_at_ms=%s unit_risk_per_trade=%s global_risk_scale=%s risk_mult=%s quality_mult=%s micro_scale=%s effective_risk_pct=%s current_qty=%s planned_qty=%s entry_price=%s stop_price=%s",
                    exchange,
                    equity,
                    self.exchange_available.get(exchange),
                    self.exchange_equity_updated_at_ms.get(exchange),
                    params.unit_risk_per_trade,
                    self.config.global_risk_scale,
                    risk_mult,
                    quality_mult,
                    micro_entry_risk_scale,
                    effective_risk_pct,
                    current_by_exchange.get(exchange, Decimal("0")),
                    qty,
                    entry_price,
                    stop_price,
                )
        return quantities

    def _refresh_account_equity(self, snapshot: PlatformSnapshot) -> None:
        exchange = snapshot.balance.exchange.value
        sizing_equity = _snapshot_sizing_equity(snapshot)
        available = snapshot.balance.available
        if sizing_equity > 0:
            self.exchange_equity[exchange] = sizing_equity
            if exchange == self.config.data_exchange:
                self.equity = sizing_equity
        if available >= 0:
            self.exchange_available[exchange] = available
        self.exchange_equity_updated_at_ms[exchange] = int(time.time() * 1000)

    def _sizing_equity_metadata(self, exchange_quantities: Mapping[str, Decimal]) -> dict[str, Mapping[str, str]]:
        exchanges = sorted(exchange_quantities)
        return {
            "sizing_equity_by_exchange": {
                exchange: str(self.exchange_equity[exchange])
                for exchange in exchanges
                if exchange in self.exchange_equity
            },
            "available_equity_by_exchange": {
                exchange: str(self.exchange_available[exchange])
                for exchange in exchanges
                if exchange in self.exchange_available
            },
            "equity_updated_at_ms_by_exchange": {
                exchange: str(self.exchange_equity_updated_at_ms[exchange])
                for exchange in exchanges
                if exchange in self.exchange_equity_updated_at_ms
            },
        }

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


def _snapshot_sizing_equity(snapshot: PlatformSnapshot) -> Decimal:
    if snapshot.balance.total > 0:
        return snapshot.balance.total
    return snapshot.balance.available


def _exchange_quantity_metadata(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {str(exchange): str(quantity) for exchange, quantity in values.items() if quantity > 0}


def _relative_diff(old: Decimal, new: Decimal) -> Decimal:
    if old == 0:
        return Decimal("0")
    return abs(new - old) / abs(old)


def _follower_stop_repair_reason(validation: RecoveryExitValidationResult) -> str:
    reasons = {check.invalid_reason for check in validation.checks if check.invalid_reason}
    if "missing_bot_owned_stop" in reasons:
        return "missing_bot_owned_stop"
    if "quantity_below_position" in reasons:
        return "under_protected:quantity_too_small"
    if "trigger_price_mismatch" in reasons:
        return "price_mismatch"
    if "wrong_side" in reasons or "wrong_position_side" in reasons:
        return "stop_side_mismatch"
    if len(validation.bot_owned_orders) > 1:
        return "duplicate_bot_owned_stop"
    if reasons:
        return ",".join(sorted(reasons))
    return validation.primary_invalid_reason or "stop_coverage_repair"


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


def _feature_key_for_engine(engine: str | None) -> str | None:
    return {"MOMENTUM_V3": "momentum", "BEAR_V3_ONLY": "bear", "BULL_RECLAIM_V2": "bull"}.get(str(engine or "").upper())


def _engine_feature_value(
    engine_features: Mapping[str, Mapping[str, Any]],
    feature_key: str | None,
    key: str,
    *,
    fallback: bool = True,
) -> Any:
    if feature_key is not None:
        value = engine_features.get(feature_key, {}).get(key)
        if value is not None:
            return value
    if not fallback:
        return None
    for fallback_feature_key in ("momentum", "bear", "bull"):
        value = engine_features.get(fallback_feature_key, {}).get(key)
        if value is not None:
            return value
    return None


def _engine_signal(row: Mapping[str, Any] | None) -> int:
    if not row:
        return 0
    value = row.get("signal", 0)
    if value is None:
        return 0
    return int(value)


def _engine_feature_bool_value(engine_features: Mapping[str, Mapping[str, Any]], feature_key: str, key: str) -> bool:
    value = _engine_feature_value(engine_features, feature_key, key, fallback=False)
    if value is None:
        return False
    return bool(value)


def _string_or_none(value: Any) -> str | None:
    return None if value is None else str(value)


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
        raw={**dict(result.raw), "source": "request_order_result", "fill_price_source": "order_status"},
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


def _side_label(side: Side) -> str:
    if side is Side.LONG:
        return "long"
    if side is Side.SHORT:
        return "short"
    return "flat"


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


def _position_side_for_strategy_side(side: Side) -> PositionSide:
    if side is Side.LONG:
        return PositionSide.LONG
    if side is Side.SHORT:
        return PositionSide.SHORT
    raise ValueError("strategy side must be long or short")


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


def _side_quantities_with_native(positions: Sequence[Position], side: Side, *, market_profile) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    same_base = Decimal("0")
    same_native = Decimal("0")
    reverse_base = Decimal("0")
    reverse_native = Decimal("0")
    converter = NativeQuantityConverter()
    for position in positions:
        if position.quantity == 0:
            continue
        pos_side = _side_from_position(position)
        native_qty = abs(position.quantity)
        base_qty = converter.native_to_base_quantity(
            exchange=position.exchange,
            symbol=position.symbol or market_profile.symbol,
            native_quantity=native_qty,
            market_profile=market_profile,
        )
        if pos_side is side:
            same_base += base_qty
            same_native += native_qty
        elif pos_side is not Side.FLAT:
            reverse_base += base_qty
            reverse_native += native_qty
    return same_base, same_native, reverse_base, reverse_native


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
