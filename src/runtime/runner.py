from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.app import AppConfig, AppContext
from src.app.alerts import AppAlert
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import MarketDataSet, RangeBar, RangeBarAggregate, TimeRange, WarmupRequest
from src.market_data.storage import SqliteKlineStore, SqliteRangeBarStore
from src.market_data.warmup.gap_detector import interval_to_ms
from src.market_data.warmup.service import KlineWarmupService
from src.order_management import LegSyncStatus, MasterFollowerExecutionPolicy, MultiExchangeOrderCoordinator, PositionPlanStatus, RepositoryDuplicateOrderGuard, SqliteOrderJournalStore, SqlitePositionPlanStore
from src.order_management.position_plan.models import LegRole
from src.order_management.models import ExchangeOrderResult, OrderIntentStatus
from src.order_management.reconciliation.service import LiveStateReconciliationService
from src.platform import create_account_client, create_execution_client
from src.platform.account.events import AccountEvent
from src.platform.account.ports import AccountClient
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, Order, OrderStatus
from src.platform.execution.ports import ExecutionClient
from src.platform.snapshot import PlatformSnapshot
from src.runtime.account_sync import AccountStateSyncService, OrderStateSyncService, RequestThrottle, SyncExchangeContext
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.features import closed_kline_feature, range_aggregate_feature, range_aggregate_unavailable_feature, range_bar_closed_feature
from src.runtime.heartbeat import RuntimeHeartbeatService
from src.runtime.models import RuntimeHealth, RuntimePhase
from src.runtime.requirements import StrategyRuntimeRequirements, resolve_strategy_runtime_requirements
from src.runtime.startup_catchup import (
    StartupCatchupConfig,
    StartupCatchupDecision,
    _check_price_guard,
    _deviation_pct,
    evaluate_startup_catchup_eligibility,
)
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.recovery.service import RecoveryExchangeContext, RuntimeRecoveryService
from src.runtime.tasks import ClosedBarScheduler, ProducerHealthMonitor, ProducerSupervisor
from src.runtime.tasks.scheduler import closed_bar_open_time_ms
from src.signals import TradeSignal
from src.signals.models import SignalAction
from src.utils.log import get_logger

logger = get_logger(__name__)


@dataclass
class LiveRuntimeStats:
    market_events_seen: int = 0
    account_events_seen: int = 0
    feature_events_seen: int = 0
    signals_seen: int = 0
    dry_run_actions: int = 0
    order_intents_created: int = 0
    order_results_seen: int = 0
    submitted_intents: int = 0
    partial_failures: int = 0
    failed_intents: int = 0
    range_bars_closed: int = 0
    range_aggregates_created: int = 0
    closed_klines_seen: int = 0
    warmup_runs: int = 0
    recovery_runs: int = 0
    on_start_called: bool = False
    producer_failures: int = 0
    producer_stale: int = 0
    errors: int = 0
    market_events_dropped: int = 0


@dataclass(frozen=True)
class MarketQueueDrainResult:
    processed: int
    deferred: int
    examined: int
    queue_size_before: int
    queue_size_after: int
    duration_ms: int
    hit_event_limit: bool
    hit_time_limit: bool


class LiveRuntimeError(RuntimeError):
    pass


# ── Fatal error classification markers ──
FATAL_STARTUP_ERROR_MARKERS = (
    "closed-kline warmup loaded insufficient records",
    "closed-kline warmup did not catch up",
    "startup snapshot is required before live trading",
    "startup reconciliation missing exchange snapshots",
    "runtime recovery failed",
)


def _is_fatal_startup_error(exc: BaseException) -> bool:
    """Return True when the error should cause a fatal exit (code 78)."""
    text = str(exc).lower()
    return any(marker in text for marker in FATAL_STARTUP_ERROR_MARKERS)


@dataclass
class StartupPreviewState:
    """Snapshot of strategy mutable state captured before a startup catch-up
    preview so it can be rolled back when the previewed signal is ultimately
    NOT executed (e.g. price guard failure or journal dedupe)."""

    pending_entry: object | None
    evaluated_bars: set[int] | None
    bar_ready_events_len: int | None


class LiveRuntimeRunner:
    """Live runtime orchestration for strategy plugins.

    The legacy ``AppRunner`` path is intentionally left untouched. This runner
    composes existing platform, market_data, order_management and recovery
    services into the ``AETHER_RUNTIME_MODE=live_runtime`` path.
    """

    def __init__(
        self,
        *,
        app_config: AppConfig,
        app_context: AppContext,
        runtime_config: LiveRuntimeConfig | None = None,
        services: Mapping[str, Any] | None = None,
    ) -> None:
        self.app_config = app_config
        self.runtime_config = runtime_config or live_runtime_config_from_app(app_config)
        self.context = app_context
        self.services = dict(services or {})
        self.requirements: StrategyRuntimeRequirements = self.services.get("runtime_requirements") or resolve_strategy_runtime_requirements(app_context.strategy, fallback_data_streams=app_config.data_streams)
        self.stats = LiveRuntimeStats()
        self._market_queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=app_config.market_queue_maxsize)
        self._stop_event = asyncio.Event()
        self._producer_tasks: list[asyncio.Task] = []
        self._sync_tasks: list[asyncio.Task] = []
        self._execution_clients: tuple[ExecutionClient, ...] | None = None
        self._account_clients: tuple[AccountClient, ...] | None = None
        self._order_journal = self.services.get("order_journal")
        self._position_plan_store = self.services.get("position_plan_store")
        self._order_coordinator = self.services.get("order_coordinator")
        self._account_sync_service = self.services.get("account_sync_service")
        self._order_sync_service = self.services.get("order_sync_service")
        self._request_sync_throttle = self.services.get("request_sync_throttle") or RequestThrottle(min_interval_seconds=0.25)
        self._recovery_service = self.services.get("recovery_service", "__default__")
        self._reconciliation_service = self.services.get("reconciliation_service", "__default__")
        self._range_bar_store = self.services.get("range_bar_store")
        self._range_bar_builder = self.services.get("range_bar_builder")
        self._range_bar_aggregator = self.services.get("range_bar_aggregator")
        self._producer_monitor: ProducerHealthMonitor = self.services.get("producer_monitor") or ProducerHealthMonitor()
        self._producer_supervisor: ProducerSupervisor = self.services.get("producer_supervisor") or ProducerSupervisor(
            monitor=self._producer_monitor,
            stale_after_ms=self.runtime_config.producer_stale_timeout_ms,
        )
        self._closed_bar_interval = self.requirements.closed_kline.interval if self.requirements.closed_kline.enabled else self.runtime_config.closed_bar_interval
        self._closed_bar_buffer_ms = self.requirements.closed_kline.close_buffer_ms if self.requirements.closed_kline.close_buffer_ms is not None else self.runtime_config.closed_bar_buffer_ms
        self._closed_bar_interval_ms = interval_to_ms(self._closed_bar_interval)
        self._range_pct = self.requirements.range_bars.range_pct if self.requirements.range_bars.enabled else self.runtime_config.range_pct
        self._range_aggregate_interval = self.requirements.range_bars.aggregate_interval if self.requirements.range_bars.enabled else self._closed_bar_interval
        self._closed_bar_scheduler: ClosedBarScheduler = self.services.get("closed_bar_scheduler") or ClosedBarScheduler(
            interval_ms=self._closed_bar_interval_ms,
            close_buffer_ms=self._closed_bar_buffer_ms,
        )
        self._rangebar_trust_start_bucket_ms: int | None = None
        self._intent_factory = self.services.get("intent_factory") or LiveOrderIntentFactory(
            strategy_id=self.app_config.strategy,
            target_exchanges=self.app_config.exchanges,
        )
        self._last_snapshot: PlatformSnapshot | None = self.services.get("snapshot")
        self._last_snapshots: tuple[PlatformSnapshot, ...] = ()
        self._last_market_queue_full_log_ms = 0
        self._last_market_queue_full_alert_ms = 0
        self._last_market_queue_backlog_log_ms = 0
        self._market_queue_backlog_warn_threshold = int(
            os.getenv("AETHER_MARKET_QUEUE_BACKLOG_WARN_THRESHOLD", "500")
        )
        self._market_queue_drain_batch_size = int(os.getenv("AETHER_MARKET_QUEUE_DRAIN_BATCH_SIZE", "1000"))
        self._last_trade_health_update_ms = 0
        self._range_context_degraded_buckets: dict[int, str] = {}
        self._follower_close_alert_last_ms: dict[str, int] = {}
        self._health = RuntimeHealth(
            phase=RuntimePhase.CREATED,
            warmup_complete=not self.runtime_config.warmup_enabled,
            caught_up=not self.runtime_config.warmup_enabled,
            metadata={"runtime_mode": self.runtime_config.mode.value, "strategy": self.app_config.strategy},
        )
        self._heartbeat_service = RuntimeHeartbeatService()
        self._startup_catchup_decision: StartupCatchupDecision | None = None
        self._startup_catchup_evaluated = False

    async def run(self, *, max_market_events: int | None = None) -> LiveRuntimeStats:
        logger.info(
            "Live runtime starting | symbol=%s strategy=%s exchanges=%s data_exchange=%s dry_run=%s max_market_events=%s",
            self.app_config.symbol,
            self.app_config.strategy,
            ",".join(exchange.value for exchange in self.app_config.exchanges),
            self.app_config.data_exchange.value,
            self.app_config.dry_run,
            max_market_events,
        )
        logger.info(
            "Market queue settings | maxsize=%s backlog_warn_threshold=%s drain_batch_size=%s full_alert_cooldown_seconds=%s",
            self._market_queue.maxsize,
            self._market_queue_backlog_warn_threshold,
            self._market_queue_drain_batch_size,
            300,
        )
        self.context.alerts.start()
        try:
            await self._startup()
            self._producer_tasks = self._start_producers()
            self._sync_tasks = self._start_sync_tasks()
            await self._consume_market_events(max_market_events=max_market_events)
            self._set_health(RuntimePhase.STOPPED, healthy=True)
            logger.info("Live runtime stopped | stats=%s", self.stats)
            return self.stats
        except Exception as exc:
            self.stats.errors += 1
            self._set_health(RuntimePhase.ERROR, healthy=False, error=str(exc))
            logger.exception("Live runtime error")
            self.context.alerts.emit(AppAlert(subject="AetherEdge live runtime error", content=str(exc), severity="error"))
            raise
        finally:
            await self._stop_sync_tasks()
            await self._stop_producers()
            await self.context.alerts.stop()

    async def start(self) -> RuntimeHealth:
        self._set_health(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)
        return self._health

    async def stop(self) -> RuntimeHealth:
        self._stop_event.set()
        await self._stop_producers()
        self._set_health(RuntimePhase.STOPPED, healthy=True)
        return self._health

    async def health(self) -> RuntimeHealth:
        return self._health

    async def process_market_event(self, event: MarketEvent) -> None:
        self.stats.market_events_seen += 1
        is_trade = isinstance(event, MarketTrade) or event.event_type is MarketEventType.TRADE
        event_ms = _event_time_ms(event)
        hb = getattr(self, "_heartbeat_service", None)
        if hb is not None:
            hb.note_market_event(event_ms)

        should_update_health = True
        if is_trade:
            now_ms = int(time.time() * 1000)
            should_update_health = now_ms - self._last_trade_health_update_ms >= 1000
            if should_update_health:
                self._last_trade_health_update_ms = now_ms

        if should_update_health:
            self._set_health(
                RuntimePhase.RUNNING,
                healthy=self._health.healthy,
                last_market_event_time_ms=event_ms,
                metadata={**dict(self._health.metadata), "last_event_type": event.event_type.value},
            )

        if is_trade:
            await self._process_trade(event)  # type: ignore[arg-type]
            if self._trade_events_are_range_only():
                return
        signals = await self._call_strategy_market_event(event)
        await self._execute_signals(signals, source=event.event_type.value, event_time_ms=event_ms)

    async def process_market_feature(self, event: MarketFeatureEvent) -> None:
        self.stats.feature_events_seen += 1
        # Track closed bar open times for heartbeat diagnostics.
        hb = getattr(self, "_heartbeat_service", None)
        if hb is not None and event.event_type.value == "closed_kline":
            open_ms = event.data.get("open_time_ms") if isinstance(event.data, dict) else None
            if isinstance(open_ms, int):
                hb.note_closed_bar(open_ms)
        handler = getattr(self.context.strategy, "on_market_feature", None)
        if not callable(handler):
            return
        signals = await handler(event)
        await self._execute_signals(signals or (), source=event.type_value, event_time_ms=event.event_time_ms, metadata={"feature_type": event.type_value})

    async def process_account_event(self, event: AccountEvent) -> None:
        await self._process_account_event(event)

    def _log_4h_decision_summary(self, *, open_time_ms: int, closed_kline: MarketKline) -> None:
        audit = getattr(self.context.strategy, "last_decision_audit", None)

        if not isinstance(audit, dict) or audit.get("bar_open_time_ms") != open_time_ms:
            logger.info(
                "4H decision completed | "
                f"symbol={self.app_config.symbol} interval={self._closed_bar_interval} "
                f"open_time_ms={closed_kline.open_time_ms} close_time_ms={closed_kline.close_time_ms} "
                "decision=no_audit reason=no_strategy_audit actions= selected_engine=None selected_side=None "
                "risk_mult=None quality_mult=None micro_action=None micro_scale=None micro_aligned=None micro_contra=None "
                "range_available=False range_status=no_audit range_bar_count=None range_min_required=None "
                "range_imbalance=None range_taker_buy_ratio=None range_close_pos=None range_micro_return_pct=None "
                f"position=None position_side=None position_engine=None position_stop=None close={closed_kline.close} "
                f"close_buffer_ms={self._closed_bar_buffer_ms}"
            )
            return

        actions = audit.get("actions") or []
        logger.info(
            "4H decision completed | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s decision=%s actions=%s selected_engine=%s selected_side=%s risk_mult=%s quality_mult=%s micro_action=%s micro_scale=%s micro_aligned=%s micro_contra=%s range_available=%s range_status=%s range_bar_count=%s range_min_required=%s range_imbalance=%s range_taker_buy_ratio=%s range_close_pos=%s range_micro_return_pct=%s position=%s position_side=%s position_engine=%s position_stop=%s close=%s close_buffer_ms=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            audit.get("bar_open_time_ms"),
            audit.get("bar_close_time_ms"),
            audit.get("reason"),
            ",".join(actions),
            audit.get("selected_engine"),
            audit.get("selected_side"),
            audit.get("risk_mult"),
            audit.get("quality_mult"),
            audit.get("micro_action"),
            audit.get("micro_entry_risk_scale"),
            audit.get("micro_aligned"),
            audit.get("micro_contra"),
            audit.get("range_available"),
            audit.get("range_status"),
            audit.get("range_bar_count"),
            audit.get("range_min_required"),
            audit.get("range_imbalance"),
            audit.get("range_taker_buy_ratio"),
            audit.get("range_close_pos"),
            audit.get("range_micro_return_pct"),
            audit.get("position_in_pos"),
            audit.get("position_side"),
            audit.get("position_engine"),
            audit.get("position_stop"),
            closed_kline.close,
            self._closed_bar_buffer_ms,
        )

    async def poll_closed_bar_once(self, *, now_ms: int | None = None) -> list[MarketFeatureEvent]:
        now = int(time.time() * 1000) if now_ms is None else now_ms
        open_time_ms = self._closed_bar_scheduler.due_closed_bar(now)
        if open_time_ms is None:
            return []
        rows = await self.context.data.fetch_klines(
            interval=self._closed_bar_interval,
            limit=10,
            use_cache=True,
            oldest_first=True,
        )
        closed_rows = [row for row in rows if row.is_closed and row.open_time_ms == open_time_ms]
        if not closed_rows:
            return []
        closed_kline = closed_rows[-1]
        drain_result = await self._drain_market_events_before_closed_bar(
            closed_bar_close_time_ms=closed_kline.close_time_ms,
        )
        if drain_result.hit_event_limit or drain_result.hit_time_limit:
            self._mark_range_context_degraded_bucket(
                bucket_start_ms=open_time_ms,
                reason="market_queue_drain_incomplete_before_closed_bar",
                event_time_ms=closed_kline.close_time_ms,
            )
            logger.warning(
                "Market queue drain incomplete before closed-bar decision | close_time_ms=%s processed=%s deferred=%s examined=%s queue_size_before=%s queue_size_after=%s hit_event_limit=%s hit_time_limit=%s",
                closed_kline.close_time_ms,
                drain_result.processed,
                drain_result.deferred,
                drain_result.examined,
                drain_result.queue_size_before,
                drain_result.queue_size_after,
                drain_result.hit_event_limit,
                drain_result.hit_time_limit,
            )
        event = closed_kline_feature(closed_kline)
        self.stats.closed_klines_seen += 1
        await self.process_market_feature(event)
        mark_emitted = getattr(self._closed_bar_scheduler, "mark_emitted", None)
        if callable(mark_emitted):
            mark_emitted(open_time_ms)
        else:
            self._closed_bar_scheduler.last_emitted_open_time_ms = open_time_ms
        features = [event]
        is_mid_bucket_restart = (
            self.requirements.range_bars.enabled
            and self.requirements.trades.enabled
            and self._rangebar_trust_start_bucket_ms is not None
            and open_time_ms < self._rangebar_trust_start_bucket_ms
        )
        if self.requirements.range_bars.enabled and self.requirements.trades.enabled:
            if open_time_ms in self._range_context_degraded_buckets:
                reason = self._range_context_degraded_buckets.get(open_time_ms, "range_context_degraded")
                logger.warning(
                    "4H range context unavailable diagnostics | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s reason=%s trust_start_bucket_ms=%s queue_size=%s",
                    self.app_config.symbol,
                    self._closed_bar_interval,
                    open_time_ms,
                    open_time_ms + self._closed_bar_interval_ms - 1,
                    reason,
                    self._rangebar_trust_start_bucket_ms,
                    self._market_queue.qsize(),
                )
                unavailable = range_aggregate_unavailable_feature(
                    symbol=self.app_config.symbol,
                    exchange=self.app_config.data_exchange,
                    timeframe=self._range_aggregate_interval,
                    range_pct=self._range_pct,
                    bucket_start_ms=open_time_ms,
                    bucket_end_ms=open_time_ms + self._closed_bar_interval_ms - 1,
                    reference_price=closed_kline.close,
                    reason=reason,
                )
                await self.process_market_feature(unavailable)
                features.append(unavailable)
                self._log_4h_decision_summary(open_time_ms=open_time_ms, closed_kline=closed_kline)
                return features

        best_range_bar_count = 0
        min_range_bars = self._get_min_range_bars()
        range_aggregates = self._load_range_aggregates_for_bucket(open_time_ms)
        if range_aggregates:
            best_range_bar_count = max((int(aggregate.bar_count) for aggregate in range_aggregates), default=0)
            if not is_mid_bucket_restart or best_range_bar_count >= min_range_bars:
                if is_mid_bucket_restart:
                    logger.info(
                        "Range aggregate loaded despite mid-bucket restart | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s trust_start_bucket_ms=%s range_bar_count=%s min_range_bars=%s",
                        self.app_config.symbol,
                        self._closed_bar_interval,
                        open_time_ms,
                        open_time_ms + self._closed_bar_interval_ms - 1,
                        self._rangebar_trust_start_bucket_ms,
                        best_range_bar_count,
                        min_range_bars,
                    )
                features.extend(await self._emit_range_aggregates(range_aggregates))
                self._log_4h_decision_summary(open_time_ms=open_time_ms, closed_kline=closed_kline)
                return features

        if is_mid_bucket_restart:
            logger.warning(
                "Range aggregate unavailable after store load due to mid-bucket live trade collection | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s trust_start_bucket_ms=%s range_bar_count=%s min_range_bars=%s",
                self.app_config.symbol,
                self._closed_bar_interval,
                open_time_ms,
                open_time_ms + self._closed_bar_interval_ms - 1,
                self._rangebar_trust_start_bucket_ms,
                best_range_bar_count,
                min_range_bars,
            )
            logger.warning(
                "4H range context unavailable diagnostics | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s reason=%s trust_start_bucket_ms=%s queue_size=%s",
                self.app_config.symbol,
                self._closed_bar_interval,
                open_time_ms,
                open_time_ms + self._closed_bar_interval_ms - 1,
                "live_trade_collection_started_mid_bucket",
                self._rangebar_trust_start_bucket_ms,
                self._market_queue.qsize(),
            )
            unavailable = range_aggregate_unavailable_feature(
                symbol=self.app_config.symbol,
                exchange=self.app_config.data_exchange,
                timeframe=self._range_aggregate_interval,
                range_pct=self._range_pct,
                bucket_start_ms=open_time_ms,
                bucket_end_ms=open_time_ms + self._closed_bar_interval_ms - 1,
                reference_price=closed_kline.close,
                reason="live_trade_collection_started_mid_bucket",
            )
            await self.process_market_feature(unavailable)
            features.append(unavailable)
            self._log_4h_decision_summary(open_time_ms=open_time_ms, closed_kline=closed_kline)
            return features

        self._log_4h_decision_summary(open_time_ms=open_time_ms, closed_kline=closed_kline)
        return features

    def _load_range_aggregates_for_bucket(self, bucket_start_ms: int) -> list[RangeBarAggregate]:
        store = self._get_range_bar_store()
        rows = store.load(
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            time_range=TimeRange(bucket_start_ms, bucket_start_ms + self._closed_bar_interval_ms - 1),
        )
        if not rows:
            return []
        aggregates = self._get_range_bar_aggregator().aggregate(rows, bucket_ms=self._closed_bar_interval_ms)
        return [aggregate for aggregate in aggregates if aggregate.bucket_start_ms == bucket_start_ms]

    async def _emit_range_aggregates(self, aggregates: Sequence[RangeBarAggregate]) -> list[MarketFeatureEvent]:
        events: list[MarketFeatureEvent] = []
        for aggregate in aggregates:
            event = range_aggregate_feature(aggregate, exchange=self.app_config.data_exchange, timeframe=self._range_aggregate_interval)
            self.stats.range_aggregates_created += 1
            await self.process_market_feature(event)
            events.append(event)
        return events

    async def emit_range_aggregate_for_bucket(self, bucket_start_ms: int) -> list[MarketFeatureEvent]:
        return await self._emit_range_aggregates(self._load_range_aggregates_for_bucket(bucket_start_ms))

    async def _startup(self) -> None:
        logger.info("Live runtime startup phase started")
        self._initialize_rangebar_trust_window()
        self._set_health(RuntimePhase.WARMING_UP, healthy=True)
        await self._run_warmup()
        self._set_health(RuntimePhase.CATCHING_UP, healthy=True, warmup_complete=True)
        snapshots = await self._run_recovery()
        # ── State convergence: reconcile exchange truth against local state ──
        #     CRITICAL: must reconcile ALL exchange snapshots, not just one.
        #     A single-exchange view can miss follower positions and wrongly
        #     close active PositionPlans (master/follower safety violation).
        await self._run_reconciliation(snapshots)
        await self._call_on_start(snapshots[0])
        # ── Startup catch-up: one-time guarded check for the most recent
        #     closed 4H bar.  Only eligible inside the fresh-open window
        #     (first 5 min of a new 4H candle). ──
        await self._evaluate_startup_catchup_once(snapshots[0])
        # ── Start heartbeat service ──
        self._heartbeat_service.start(
            runtime_id=f"{self.app_config.strategy}::{self.app_config.symbol}",
        )
        self._set_health(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)
        logger.info("Live runtime startup phase completed")

    def _initialize_rangebar_trust_window(self) -> None:
        if not self.requirements.range_bars.enabled or not self.requirements.trades.enabled:
            self._rangebar_trust_start_bucket_ms = None
            return
        now_ms = int(time.time() * 1000)
        current_bucket = (now_ms // self._closed_bar_interval_ms) * self._closed_bar_interval_ms
        start_lag_tolerance_ms = int(os.getenv("AETHER_TRUST_CURRENT_BUCKET_START_LAG_MS", "10000"))
        if now_ms - current_bucket <= start_lag_tolerance_ms:
            self._rangebar_trust_start_bucket_ms = current_bucket
        else:
            self._rangebar_trust_start_bucket_ms = current_bucket + self._closed_bar_interval_ms
        logger.info(
            "Rangebar trust window initialized | symbol=%s interval=%s now_ms=%s current_bucket_ms=%s trust_start_bucket_ms=%s start_lag_tolerance_ms=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            now_ms,
            current_bucket,
            self._rangebar_trust_start_bucket_ms,
            start_lag_tolerance_ms,
        )

    async def _run_warmup(self) -> None:
        warmup_services = self.services.get("warmup_services") or self.services.get("warmup_service")
        if warmup_services is not None:
            if not isinstance(warmup_services, (list, tuple)):
                warmup_services = (warmup_services,)
            for service in warmup_services:
                result = service() if callable(service) and not hasattr(service, "warmup") else service
                if hasattr(result, "warmup"):
                    maybe = result.warmup()
                else:
                    maybe = result
                if asyncio.iscoroutine(maybe):
                    await maybe
                self.stats.warmup_runs += 1
        await self._run_requirement_warmup()

    def _count_available_closed_klines(self, repository, *, symbol: str, interval: str, time_range: TimeRange) -> int:
        """Return the number of closed klines currently available in the repository.

        This counts **all** closed klines in the store for the given range,
        NOT just records that were newly saved by the most recent warmup pass.
        """
        rows = repository.load(symbol=symbol, interval=interval, time_range=time_range)
        return sum(1 for row in rows if row.is_closed)

    async def _run_requirement_warmup(self) -> None:
        # Closed-kline warmup is generic and can be built from the platform data feed.
        # Historical-trade warmup remains an adapter-specific capability; if a
        # strategy requires it without injecting an implementation, fail fast
        # instead of silently starting with incomplete range-bar context.
        if self.requirements.closed_kline.enabled and self.requirements.closed_kline.warmup_days > 0:
            end_open = closed_bar_open_time_ms(
                int(time.time() * 1000),
                interval_ms=self._closed_bar_interval_ms,
                close_buffer_ms=self._closed_bar_buffer_ms,
            )
            if end_open >= 0:
                start_open = max(0, end_open - int(self.requirements.closed_kline.warmup_days) * 24 * 60 * 60_000)
                repository = self.services.get("kline_store") or SqliteKlineStore()
                service = KlineWarmupService(data_feed=self.context.data, repository=repository)
                result = await service.warmup(
                    WarmupRequest(
                        symbol=self.app_config.symbol,
                        dataset=MarketDataSet.KLINES,
                        interval=self._closed_bar_interval,
                        time_range=TimeRange(start_open, end_open),
                    )
                )
                self.stats.warmup_runs += 1

                min_records = max(1, int(self.requirements.closed_kline.min_records or 1))
                time_range = TimeRange(start_open, end_open)
                newly_loaded_records = result.records_loaded  # newly saved this pass
                available_records_before_backfill = self._count_available_closed_klines(
                    repository, symbol=self.app_config.symbol, interval=self._closed_bar_interval, time_range=time_range
                )

                if not result.caught_up:
                    gap_details = [
                        {
                            "start_time_ms": gap.time_range.start_time_ms,
                            "end_time_ms": gap.time_range.end_time_ms,
                            "reason": gap.reason,
                        }
                        for gap in result.gaps_after[:10]
                    ]
                    logger.error(
                        "Closed-kline warmup gaps remain | interval=%s gap_count=%s first_gaps=%s "
                        "newly_loaded=%s available=%s",
                        self._closed_bar_interval,
                        len(result.gaps_after),
                        gap_details,
                        newly_loaded_records,
                        available_records_before_backfill,
                    )
                    raise LiveRuntimeError(f"closed-kline warmup did not catch up: {len(result.gaps_after)} gaps remain")

                logger.info(
                    "Closed-kline warmup completed | interval=%s start_open=%s end_open=%s "
                    "newly_loaded=%s available=%s min_records=%s caught_up=%s",
                    self._closed_bar_interval,
                    start_open,
                    end_open,
                    newly_loaded_records,
                    available_records_before_backfill,
                    min_records,
                    result.caught_up,
                )

                # ── Backfill fallback: when the local store is insufficient,
                #     attempt a direct REST historical kline backfill.
                #     CRITICAL: use available_records (total in store), NOT
                #     newly_loaded_records (only what warmup just saved). ──
                store_path = str(getattr(repository, "path", ""))
                store_class = type(repository).__name__
                backfill_attempted = False
                available_records = available_records_before_backfill

                if available_records < min_records:
                    logger.warning(
                        "Closed-kline warmup insufficient — attempting REST backfill | "
                        "symbol=%s interval=%s newly_loaded=%s available=%s min_records=%s",
                        self.app_config.symbol,
                        self._closed_bar_interval,
                        newly_loaded_records,
                        available_records,
                        min_records,
                    )
                    try:
                        from src.market_data.warmup.kline_provider import MarketDataKlineProvider

                        provider = MarketDataKlineProvider(
                            data_feed=self.context.data,
                            repository=repository,
                        )
                        backfill_diag = await provider.backfill_and_reload(
                            symbol=self.app_config.symbol,
                            interval=self._closed_bar_interval,
                            time_range=time_range,
                            min_records=min_records,
                            store_class=store_class,
                            store_path=store_path,
                        )
                        backfill_attempted = True
                        # Re-count available records directly from the repository
                        # after backfill, rather than relying on a single field.
                        available_records = self._count_available_closed_klines(
                            repository, symbol=self.app_config.symbol, interval=self._closed_bar_interval, time_range=time_range
                        )
                        logger.info(
                            "REST kline backfill completed | symbol=%s interval=%s "
                            "fetched=%s saved=%s available_after=%s success=%s",
                            backfill_diag.symbol,
                            backfill_diag.interval,
                            backfill_diag.fetched_records,
                            backfill_diag.saved_records,
                            available_records,
                            backfill_diag.success,
                        )
                    except Exception as backfill_exc:
                        logger.error(
                            "REST kline backfill failed | symbol=%s interval=%s error=%s",
                            self.app_config.symbol,
                            self._closed_bar_interval,
                            backfill_exc,
                        )

                # ── Hydrate strategy state with closed klines ──
                await self._hydrate_strategy_closed_klines(repository, time_range=time_range)

                # ── Fail fast when repository still has too few available records ──
                if available_records < min_records:
                    dry_run = self.app_config.dry_run
                    # Build rich diagnostics for operators.
                    raw_aliases_str = "N/A"
                    try:
                        from src.platform.markets import get_market_profile
                        profile = get_market_profile(self.app_config.symbol)
                        raw_aliases_str = ", ".join(
                            f"{exchange.value}:{profile.raw_symbol(exchange)}"
                            for exchange in profile.exchange_symbols
                        )
                    except Exception:
                        pass

                    from datetime import datetime, timezone
                    start_utc = datetime.fromtimestamp(start_open / 1000, tz=timezone.utc).isoformat()
                    end_utc = datetime.fromtimestamp(end_open / 1000, tz=timezone.utc).isoformat()

                    diag_content = (
                        f"symbol={self.app_config.symbol}\n"
                        f"raw_aliases={raw_aliases_str}\n"
                        f"interval={self._closed_bar_interval}\n"
                        f"start_open_ms={start_open}\n"
                        f"end_open_ms={end_open}\n"
                        f"start_open_utc={start_utc}\n"
                        f"end_open_utc={end_utc}\n"
                        f"newly_loaded_records={newly_loaded_records}\n"
                        f"available_records_before_backfill={available_records_before_backfill}\n"
                        f"available_records_after_backfill={available_records}\n"
                        f"backfill_attempted={backfill_attempted}\n"
                        f"min_records={min_records}\n"
                        f"kline_store_class={store_class}\n"
                        f"kline_store_path={store_path}\n"
                        f"warmup_days={self.requirements.closed_kline.warmup_days}\n"
                        f"dry_run={dry_run}\n"
                    )
                    if dry_run:
                        logger.warning(
                            "Closed-kline warmup loaded fewer records than required — continuing in dry-run mode | "
                            "interval=%s warmup_days=%s available_records=%s min_records=%s",
                            self._closed_bar_interval,
                            self.requirements.closed_kline.warmup_days,
                            available_records,
                            min_records,
                        )
                        self.context.alerts.emit(
                            AppAlert(
                                subject="AetherEdge closed-kline warmup below minimum records",
                                content=diag_content,
                                severity="warning",
                            )
                        )
                    else:
                        self.context.alerts.emit(
                            AppAlert(
                                subject="AetherEdge closed-kline warmup failed",
                                content=diag_content,
                                severity="error",
                            )
                        )
                        raise LiveRuntimeError(
                            f"closed-kline warmup loaded insufficient records "
                            f"(symbol={self.app_config.symbol} interval={self._closed_bar_interval} "
                            f"available_records={available_records} min_records={min_records})"
                        )

    async def _hydrate_strategy_closed_klines(self, repository, *, time_range: TimeRange) -> None:
        handler = getattr(self.context.strategy, "on_market_feature", None)
        if not callable(handler):
            return
        rows = repository.load(symbol=self.app_config.symbol, interval=self._closed_bar_interval, time_range=time_range)
        for row in rows:
            if not row.is_closed:
                continue
            await self.process_market_feature(closed_kline_feature(row))

    async def _run_recovery(self) -> tuple[PlatformSnapshot, ...]:
        service = self._get_recovery_service()
        if service is None:
            if self._last_snapshot is None:
                raise LiveRuntimeError("startup snapshot is required before live trading")
            return (self._last_snapshot,)
        report = await service.recover(strategy=self.context.strategy)
        self.stats.recovery_runs += 1
        if not report.ok:
            raise LiveRuntimeError(f"runtime recovery failed: {tuple(report.issues)}")
        logger.info(
            "Runtime recovery completed | snapshots=%s strategy_signals=%s issues=%s",
            len(report.snapshots),
            len(report.strategy_signals),
            len(report.issues),
        )
        if report.strategy_signals:
            await self._execute_signals(report.strategy_signals, source="recovery", event_time_ms=int(time.time() * 1000), metadata={"feature_type": "recovery"})
        if report.snapshots:
            self._last_snapshots = tuple(report.snapshots)
            self._last_snapshot = report.snapshots[0]  # backward-compat for on_start / legacy consumers
        if not self._last_snapshots:
            raise LiveRuntimeError("recovery completed without a startup snapshot")
        return self._last_snapshots

    async def _call_on_start(self, snapshot: PlatformSnapshot) -> None:
        on_start = getattr(self.context.strategy, "on_start", None)
        if not callable(on_start):
            return
        signals = await on_start(snapshot)
        self.stats.on_start_called = True
        logger.info("Strategy on_start completed | signals=%s", len(signals or ()))
        await self._execute_signals(signals or (), source="on_start", event_time_ms=int(time.time() * 1000))

    async def _fetch_current_market_price(self) -> Decimal | None:
        """Fetch current market price for price guard validation.

        Uses the data feed ticker endpoint.  Returns ``None`` when the
        price cannot be obtained so the caller can skip catch-up with
        ``reason=current_price_unavailable``.
        """
        try:
            ticker = await self.context.data.fetch_ticker()
            return ticker.price
        except Exception:
            logger.warning("Startup catchup cannot fetch current market price")
            return None

    async def _fetch_current_4h_open_price(self, current_4h_open_ms: int) -> Decimal | None:
        """Fetch the open price of the current (still-forming) 4H bar.

        Returns ``None`` when unavailable; the caller should fall back to
        the candidate closed bar close price.
        """
        try:
            rows = await self.context.data.fetch_klines(
                interval=self._closed_bar_interval,
                limit=1,
                start_time_ms=current_4h_open_ms,
                end_time_ms=current_4h_open_ms,
                use_cache=True,
                oldest_first=True,
            )
            if rows:
                return rows[0].open
        except Exception:
            pass
        return None

    def _has_any_active_position_for_catchup(
        self, snapshots: tuple[PlatformSnapshot, ...]
    ) -> bool:
        """Return True when ANY active-position or pending-state source is true.

        Checks (any single true → skip catch-up):
        1. Exchange snapshot positions with non-zero quantity
        2. Strategy ``position.in_pos`` is True
        3. Strategy ``pending_entry`` is not None
        4. PositionPlanStore ``list_active_positions()`` non-empty
        5. StateStore has open orders (including stop orders)
        """
        # 1. Exchange snapshots — any non-zero position quantity
        for snap in snapshots:
            for pos in getattr(snap, "positions", ()) or ():
                qty = getattr(pos, "quantity", None)
                if qty is not None and qty != 0:
                    return True

        # 2 & 3. Strategy-internal position / pending entry
        strategy = self.context.strategy
        position = getattr(strategy, "position", None)
        if position is not None and bool(getattr(position, "in_pos", False)):
            return True
        if getattr(strategy, "pending_entry", None) is not None:
            return True

        # 4. PositionPlanStore active plans
        store = self._position_plan_store or self._get_position_plan_store()
        try:
            if store.list_active_positions():
                return True
        except Exception:
            pass

        # 5. StateStore open orders
        if self._has_open_orders():
            return True

        # 6. Unresolved follower close (master closed, follower still open)
        if self._has_unresolved_follower_close():
            return True

        return False

    async def _preview_strategy_market_features(
        self, events: Sequence[MarketFeatureEvent]
    ) -> list[TradeSignal]:
        """Feed market-feature events to the strategy **without** executing.

        Returns the raw ``TradeSignal`` objects the strategy produced.
        The caller is responsible for filtering, price-guard checks, and
        eventual execution.
        """
        handler = getattr(self.context.strategy, "on_market_feature", None)
        if not callable(handler):
            return []
        signals: list[TradeSignal] = []
        for event in events:
            result = await handler(event)
            if result:
                signals.extend(result)
        return signals

    def _capture_startup_preview_state(self) -> StartupPreviewState:
        strategy = self.context.strategy

        pending_entry = getattr(strategy, "pending_entry", None)

        evaluated_bars = None
        buffer_obj = getattr(strategy, "buffer", None)
        if buffer_obj is not None and hasattr(buffer_obj, "evaluated_bars"):
            evaluated_bars = set(getattr(buffer_obj, "evaluated_bars"))

        bar_ready_events_len = None
        events = getattr(strategy, "bar_ready_events", None)
        if isinstance(events, list):
            bar_ready_events_len = len(events)

        return StartupPreviewState(
            pending_entry=pending_entry,
            evaluated_bars=evaluated_bars,
            bar_ready_events_len=bar_ready_events_len,
        )

    def _restore_startup_preview_state(self, state: StartupPreviewState) -> None:
        strategy = self.context.strategy

        if hasattr(strategy, "pending_entry"):
            setattr(strategy, "pending_entry", state.pending_entry)

        buffer_obj = getattr(strategy, "buffer", None)
        if (
            buffer_obj is not None
            and state.evaluated_bars is not None
            and hasattr(buffer_obj, "evaluated_bars")
        ):
            buffer_obj.evaluated_bars = set(state.evaluated_bars)

        events = getattr(strategy, "bar_ready_events", None)
        if isinstance(events, list) and state.bar_ready_events_len is not None:
            del events[state.bar_ready_events_len:]

    async def _build_range_aggregate_events_for_bucket(
        self, bucket_start_ms: int
    ) -> list[MarketFeatureEvent]:
        """Build RangeAggregate feature events **without** dispatching.

        Unlike :meth:`emit_range_aggregate_for_bucket`, this method never
        calls :meth:`process_market_feature`.  The caller controls exactly
        when and whether the events reach the strategy.

        Also enforces ``min_range_bars`` from strategy config so that
        buckets with too few range bars are treated as unavailable.
        """
        store = self._get_range_bar_store()
        rows = store.load(
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            time_range=TimeRange(
                bucket_start_ms,
                bucket_start_ms + self._closed_bar_interval_ms - 1,
            ),
        )
        if not rows:
            return []
        aggregates = self._get_range_bar_aggregator().aggregate(
            rows, bucket_ms=self._closed_bar_interval_ms
        )
        min_bars = self._get_min_range_bars()
        events: list[MarketFeatureEvent] = []
        for aggregate in aggregates:
            if aggregate.bucket_start_ms != bucket_start_ms:
                continue
            if aggregate.bar_count < min_bars:
                logger.info(
                    "Startup catchup range aggregate below min_range_bars | "
                    "bucket_start_ms=%s bar_count=%s min_range_bars=%s",
                    bucket_start_ms,
                    aggregate.bar_count,
                    min_bars,
                )
                continue
            event = range_aggregate_feature(
                aggregate,
                exchange=self.app_config.data_exchange,
                timeframe=self._range_aggregate_interval,
            )
            events.append(event)
        return events

    def _get_min_range_bars(self) -> int:
        """Read ``min_range_bars`` from the strategy config, default 1.

        V9C declares this via ``config.micro_context.min_range_bars``
        (an object/dataclass) or ``config["micro_context"]["min_range_bars"]``
        (a dict).
        """
        strategy = self.context.strategy
        cfg = getattr(strategy, "config", None)

        # 1. Object/dataclass path: strategy.config.micro_context.min_range_bars
        micro_obj = getattr(cfg, "micro_context", None)
        value = getattr(micro_obj, "min_range_bars", None)
        if value is not None:
            try:
                return max(1, int(value))
            except (TypeError, ValueError):
                pass

        # 2. Dict path: strategy.config["micro_context"]["min_range_bars"]
        if isinstance(cfg, dict):
            micro = cfg.get("micro_context", {})
            if isinstance(micro, dict):
                value = micro.get("min_range_bars")
                if value is not None:
                    try:
                        return max(1, int(value))
                    except (TypeError, ValueError):
                        pass

        # 3. Default
        return 1

    async def _evaluate_startup_catchup_once(self, snapshot: PlatformSnapshot) -> None:
        """Evaluate whether the most recent closed 4H bar qualifies for a
        guarded startup catch-up entry.

        This runs exactly once per startup, after reconciliation and
        on_start but before producers and sync tasks.  It is the only code
        path that can produce a startup catch-up signal — the normal
        :meth:`poll_closed_bar_once` path does NOT retry startup bars.

        **P0 safety requirements enforced here:**

        * Current price from live market data (NOT ``kline.close``).
        * Side from real strategy ``TradeSignal.action`` (NOT kline colour).
        * Range aggregate MUST be available; no unavailable placeholder.
        * Active position / pending / unresolved follower → skip.
        """
        if self._startup_catchup_evaluated:
            return
        self._startup_catchup_evaluated = True

        if not self.requirements.closed_kline.enabled:
            logger.info("Startup catchup skipped | reason=closed_kline_disabled")
            return

        config: StartupCatchupConfig = self.runtime_config.startup_catchup
        if not config.enabled:
            logger.info("Startup catchup skipped | reason=startup_catchup_disabled")
            return

        now_ms = int(time.time() * 1000)
        h4_ms = self._closed_bar_interval_ms
        current_4h_open = (now_ms // h4_ms) * h4_ms
        candidate_open = current_4h_open - h4_ms
        candidate_close = current_4h_open - 1

        # ── 1. Fresh-open window check ──────────────────────────────────────
        fresh_window_age_ms = now_ms - current_4h_open
        fresh_window_ms = config.fresh_open_window_seconds * 1000
        if fresh_window_age_ms > fresh_window_ms:
            logger.info(
                "Startup catchup skipped | reason=outside_fresh_4h_open_window "
                "age_seconds=%s window_seconds=%s",
                fresh_window_age_ms // 1000,
                config.fresh_open_window_seconds,
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open)
            return

        # ── 2. Previous heartbeat (informational only) ──────────────────────
        previous_heartbeat = self._heartbeat_service.read_previous()

        # ── 3. Active-position / pending / unresolved-follower guard ────────
        snapshots_tuple: tuple[PlatformSnapshot, ...] = (snapshot,)
        if self._last_snapshots:
            snapshots_tuple = self._last_snapshots
        if self._has_any_active_position_for_catchup(snapshots_tuple):
            logger.info(
                "Startup catchup skipped | reason=active_position_or_pending_state_exists"
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open)
            return

        # ── 4. Load candidate closed kline ──────────────────────────────────
        repository = self.services.get("kline_store") or SqliteKlineStore()
        rows = repository.load(
            symbol=self.app_config.symbol,
            interval=self._closed_bar_interval,
            time_range=TimeRange(candidate_open, candidate_close),
        )
        closed_rows = [
            r for r in rows if r.is_closed and r.open_time_ms == candidate_open
        ]
        if not closed_rows:
            logger.info(
                "Startup catchup skipped | reason=no_closed_bar_found "
                "candidate_open_ms=%s",
                candidate_open,
            )
            return
        kline = closed_rows[-1]

        # ── 5. Candidate bar must match expected previous 4H bar ────────────
        expected_close = current_4h_open - 1
        expected_open = current_4h_open - h4_ms
        if kline.close_time_ms != expected_close or kline.open_time_ms != expected_open:
            logger.info(
                "Startup catchup skipped | reason=candidate_bar_not_previous_4h "
                "expected_open_ms=%s expected_close_ms=%s actual_open_ms=%s actual_close_ms=%s",
                expected_open,
                expected_close,
                kline.open_time_ms,
                kline.close_time_ms,
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open)
            return

        # ── 6. Dedup guard (scheduler layer) ────────────────────────────────
        if self._closed_bar_scheduler.last_emitted_open_time_ms == candidate_open:
            logger.info(
                "Startup catchup skipped | reason=already_executed "
                "candidate_open_ms=%s",
                candidate_open,
            )
            return

        # ── 7. Range aggregate — MUST be available; NO placeholder ─────────
        range_events: list[MarketFeatureEvent] = []
        if self.requirements.range_bars.enabled:
            range_events = await self._build_range_aggregate_events_for_bucket(
                candidate_open
            )
            if not range_events:
                logger.info(
                    "Startup catchup skipped | reason=range_aggregate_unavailable "
                    "bucket_start_ms=%s",
                    candidate_open,
                )
                self._closed_bar_scheduler.mark_emitted(candidate_open)
                return
            logger.info(
                "Startup catchup range aggregate ready | bucket_start_ms=%s "
                "events=%s",
                candidate_open,
                len(range_events),
            )

        # ── 8. Fetch current market price (P0-2 fix) ────────────────────────
        current_price = await self._fetch_current_market_price()
        if current_price is None:
            logger.info(
                "Startup catchup skipped | reason=current_price_unavailable "
                "candidate_open_ms=%s",
                candidate_open,
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open)
            return

        # ── 9. Theoretical open (P0-2 fix) ──────────────────────────────────
        #     Prefer current 4H bar open; fall back to candidate bar close.
        theoretical_open = await self._fetch_current_4h_open_price(current_4h_open)
        if theoretical_open is None:
            theoretical_open = kline.close
            logger.info(
                "Startup catchup using bar close as theoretical open | "
                "current_4h_open_ms=%s fallback=%s",
                current_4h_open,
                theoretical_open,
            )
        else:
            logger.info(
                "Startup catchup using live 4H open | open_price=%s",
                theoretical_open,
            )

        # ── 10. Preview strategy signals WITHOUT executing (P0-3 fix) ───────
        preview_events: list[MarketFeatureEvent] = [closed_kline_feature(kline)]
        preview_events.extend(range_events)
        preview_state = self._capture_startup_preview_state()
        signals = await self._preview_strategy_market_features(preview_events)
        logger.info(
            "Startup catchup strategy preview | total_signals=%s",
            len(signals),
        )

        # ── 11. Filter OPEN signals + apply price guard per signal ──────────
        signals_to_execute: list[TradeSignal] = []
        range_bar_count = (
            range_events[0].data.get("bar_count", 0) if range_events else 0
        )
        for signal in signals:
            if signal.action not in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT}:
                continue

            # Side from REAL signal action — NOT kline colour (P0-3 fix).
            side = "long" if signal.action == SignalAction.OPEN_LONG else "short"

            # Price guard with REAL current price (P0-2 fix).
            price_ok = _check_price_guard(
                current_price=current_price,
                theoretical_open_price=theoretical_open,
                side=side,
                max_adverse_pct=config.max_adverse_price_pct,
                max_favorable_pct=config.max_favorable_price_pct,
            )
            deviation_pct = _deviation_pct(current_price, theoretical_open)

            if not price_ok:
                logger.info(
                    "Startup catchup signal discarded | reason=price_guard_failed "
                    "action=%s side=%s current_price=%s theoretical_open=%s "
                    "deviation_pct=%s",
                    signal.action.value,
                    side,
                    current_price,
                    theoretical_open,
                    deviation_pct,
                )
                continue

            # OrderJournal dedupe guard — skip signal when an intent with the
            # same position_id already exists in the order journal.
            position_id = None
            if signal.metadata:
                position_id = signal.metadata.get("position_id")

            if position_id:
                journal = self._order_journal or self._get_order_journal()
                has_duplicate = False
                has_fn = getattr(journal, "has_intent_with_position_id", None)
                if callable(has_fn):
                    has_duplicate = bool(has_fn(str(position_id)))
                if has_duplicate:
                    logger.info(
                        "Startup catchup signal discarded | reason=order_journal_duplicate "
                        "position_id=%s action=%s candidate_open_ms=%s",
                        position_id,
                        signal.action.value,
                        candidate_open,
                    )
                    continue

            # Enrich signal with startup_catchup source / metadata.
            enriched = TradeSignal(
                symbol=signal.symbol,
                action=signal.action,
                quantity=signal.quantity,
                order_type=signal.order_type,
                price=signal.price,
                trigger_price=signal.trigger_price,
                client_order_id=signal.client_order_id,
                reason=signal.reason or "startup_catchup",
                metadata={
                    **dict(signal.metadata or {}),
                    "startup_catchup": True,
                    "fresh_window_age_seconds": fresh_window_age_ms // 1000,
                    "price_guard": "passed",
                    "current_price": str(current_price),
                    "theoretical_open_price": str(theoretical_open),
                    "price_deviation_pct": str(deviation_pct),
                    "range_bar_count": range_bar_count,
                    "side": side,
                    "candidate_open_ms": candidate_open,
                },
                created_time_ms=signal.created_time_ms,
            )
            signals_to_execute.append(enriched)

        if not signals_to_execute:
            self._restore_startup_preview_state(preview_state)
            logger.info(
                "Startup catchup skipped | reason=no_open_signal_after_price_guard "
                "total_signals=%s candidate_open_ms=%s",
                len(signals),
                candidate_open,
            )
            self._closed_bar_scheduler.mark_emitted(candidate_open)
            return

        # ── 12. Execute signals that passed all guards ──────────────────────
        logger.info(
            "Startup catchup executing signals | count=%s candidate_open_ms=%s",
            len(signals_to_execute),
            candidate_open,
        )
        await self._execute_signals(
            signals_to_execute,
            source="startup_catchup",
            event_time_ms=now_ms,
            metadata={
                "startup_catchup": True,
                "fresh_window_age_seconds": fresh_window_age_ms // 1000,
                "current_price": str(current_price),
                "theoretical_open_price": str(theoretical_open),
                "range_bar_count": range_bar_count,
                "candidate_open_ms": candidate_open,
            },
        )

        # ── 13. Mark scheduler emitted ──────────────────────────────────────
        self._closed_bar_scheduler.mark_emitted(candidate_open)
        self.stats.closed_klines_seen += 1

        self._startup_catchup_decision = StartupCatchupDecision(
            eligible=True,
            reason="all_guards_passed",
            metadata={
                "fresh_window_age_seconds": fresh_window_age_ms // 1000,
                "current_price": str(current_price),
                "theoretical_open_price": str(theoretical_open),
                "range_bar_count": range_bar_count,
                "candidate_open_ms": candidate_open,
                "signals_executed": len(signals_to_execute),
            },
        )

    def _has_open_orders(self) -> bool:
        """Check for open orders across all configured exchanges."""
        list_open = getattr(self.context.state_store, "list_open_orders", None)
        if not callable(list_open):
            return False
        for exchange in self.app_config.exchanges:
            if list_open(exchange=exchange, symbol=self.app_config.symbol, include_stop_orders=True):
                return True
        return False

    def _start_producers(self) -> list[asyncio.Task]:
        tasks: list[asyncio.Task] = []
        if self.requirements.trades.enabled and self.requirements.trades.stream_enabled:
            logger.info("Starting runtime producer | name=trades")
            tasks.append(
                asyncio.create_task(
                    self._producer_supervisor.run_resilient_stream(
                        name="trades",
                        stream_factory=self.context.data.stream_trades,
                        on_item=self._enqueue_market_event,
                    )
                )
            )
        if self.requirements.order_book.enabled and self.requirements.order_book.stream_enabled:
            logger.info("Starting runtime producer | name=order_book")
            tasks.append(
                asyncio.create_task(
                    self._producer_supervisor.run_resilient_stream(
                        name="order_book",
                        stream_factory=self.context.data.stream_order_book,
                        on_item=self._enqueue_market_event,
                    )
                )
            )
        return tasks

    def _start_sync_tasks(self) -> list[asyncio.Task]:
        tasks: list[asyncio.Task] = []
        if self.requirements.account_state.poll_enabled:
            tasks.append(asyncio.create_task(self._get_account_sync_service().run_periodic(self._stop_event)))
        if self.requirements.order_state.poll_when_position_enabled:
            tasks.append(asyncio.create_task(self._get_order_sync_service().run_periodic(self._stop_event)))
            tasks.append(asyncio.create_task(self._periodic_follower_close_check(self._stop_event)))
        # Heartbeat periodic task
        tasks.append(asyncio.create_task(self._heartbeat_service.run_periodic(self._stop_event)))
        return tasks

    async def _periodic_follower_close_check(self, stop_event: asyncio.Event) -> None:
        await asyncio.sleep(30)
        while not stop_event.is_set():
            try:
                signals = self._build_unresolved_follower_close_signals()
                if signals:
                    logger.info(
                        "Auto-triggering follower close retry for %s unresolved follower(s)",
                        len(signals),
                    )
                    await self._execute_signals(
                        signals,
                        source="follower_close_periodic_check",
                        event_time_ms=None,
                        metadata={"trigger": "periodic_follower_close_check"},
                    )
            except Exception as exc:
                logger.error("Periodic follower close check error | error=%s", exc)
            await _jittered_sleep(stop_event, 60)

    def _build_unresolved_follower_close_signals(self) -> list[TradeSignal]:
        """Build standard TradeSignals for follower legs that still need closing.

        Scans PositionPlanStore for MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED plans
        and constructs follower-only close signals from the stored leg data.
        This is an order-lifecycle safety net — it does not depend on any
        strategy private method.
        """
        store = self._position_plan_store
        if store is None:
            return []
        signals: list[TradeSignal] = []
        for plan in store.list_active_positions():
            if plan.status != PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED:
                continue
            for leg in store.get_legs(plan.position_id):
                if leg.exchange == plan.master_exchange:
                    continue
                if leg.role not in {LegRole.FOLLOWER, "follower"}:
                    continue
                if leg.sync_status == LegSyncStatus.CLOSED:
                    continue
                # Determine quantity: prefer filled_qty_base, fall back to target_qty_base.
                qty = leg.filled_qty_base if leg.filled_qty_base > Decimal("0") else leg.target_qty_base
                if qty <= Decimal("0"):
                    logger.warning(
                        "Unresolved follower close skipped — zero quantity | position_id=%s exchange=%s",
                        plan.position_id,
                        leg.exchange.value,
                    )
                    continue
                action = SignalAction.CLOSE_LONG if plan.side == "long" else SignalAction.CLOSE_SHORT
                signals.append(
                    TradeSignal(
                        symbol=self.app_config.symbol,
                        action=action,
                        quantity=qty,
                        reason="PERIODIC_MASTER_CLOSED_CLOSE_FOLLOWER",
                        metadata={
                            "target_exchanges": [leg.exchange.value],
                            "reduce_only": True,
                            "execution_purpose": "follower_close_after_master_close",
                            "position_id": plan.position_id,
                            "strategy_id": plan.strategy_id,
                            "master_already_closed": True,
                            "close_required_reason": "master_closed_follower_not_closed",
                            "trigger": "periodic_follower_close_check",
                        },
                    )
                )
                logger.warning(
                    "Unresolved follower close detected | position_id=%s exchange=%s sync_status=%s qty=%s",
                    plan.position_id,
                    leg.exchange.value,
                    leg.sync_status.value if hasattr(leg.sync_status, "value") else str(leg.sync_status),
                    str(qty),
                )
        return signals

    def _has_unresolved_follower_close(self) -> bool:
        """Return True when at least one position plan has unresolved follower
        close after master close, blocking new entries."""
        store = self._position_plan_store
        if store is None:
            return False
        for plan in store.list_active_positions():
            if plan.status == PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED:
                return True
        return False

    async def _enqueue_market_event(self, event: MarketEvent) -> None:
        if self._market_queue.full():
            self.stats.market_events_dropped += 1
            self._emit_market_queue_full_alert(event)
            self._mark_range_context_degraded_for_event(event, reason="market_queue_dropped_trade")
            try:
                self._market_queue.get_nowait()
                self._market_queue.task_done()
            except asyncio.QueueEmpty:
                pass
        else:
            self._maybe_log_market_queue_backlog(event=event)
        await self._market_queue.put(event)

    def _maybe_log_market_queue_backlog(self, *, event: MarketEvent) -> None:
        qsize = self._market_queue.qsize()
        threshold = self._market_queue_backlog_warn_threshold
        if qsize < threshold:
            return

        now_ms = int(time.time() * 1000)
        if now_ms - self._last_market_queue_backlog_log_ms < 60_000:
            return

        self._last_market_queue_backlog_log_ms = now_ms
        logger.warning(
            "Market queue backlog high | incoming_event_type=%s queue_size=%s threshold=%s maxsize=%s dropped_total=%s",
            event.event_type.value,
            qsize,
            threshold,
            self._market_queue.maxsize,
            self.stats.market_events_dropped,
        )

    def _mark_range_context_degraded_for_event(self, event: MarketEvent, *, reason: str) -> None:
        if not isinstance(event, MarketTrade) and event.event_type is not MarketEventType.TRADE:
            return

        event_ms = _event_time_ms(event)
        if event_ms is None:
            event_ms = int(time.time() * 1000)

        bucket_start = (event_ms // self._closed_bar_interval_ms) * self._closed_bar_interval_ms
        self._mark_range_context_degraded_bucket(bucket_start_ms=bucket_start, reason=reason, event_time_ms=event_ms)

    def _mark_range_context_degraded_bucket(self, *, bucket_start_ms: int, reason: str, event_time_ms: int | None = None) -> None:
        if bucket_start_ms not in self._range_context_degraded_buckets:
            self._range_context_degraded_buckets[bucket_start_ms] = reason
            logger.warning(
                "Range context degraded | reason=%s bucket_start_ms=%s event_time_ms=%s dropped_total=%s",
                reason,
                bucket_start_ms,
                event_time_ms,
                self.stats.market_events_dropped,
            )

    def _emit_market_queue_full_alert(self, event: MarketEvent) -> None:
        now_ms = int(time.time() * 1000)
        # Avoid flooding email/alert sinks during a burst, but never drop market
        # data silently.  The closed-bar catch-up path can repair range bars,
        # while this alert tells operators the live stream fell behind.
        if now_ms - self._last_market_queue_full_log_ms >= 60_000:
            self._last_market_queue_full_log_ms = now_ms
            logger.warning(
                "Market queue full; dropped oldest event | incoming_event_type=%s queue_size=%s maxsize=%s dropped_total=%s",
                event.event_type.value,
                self._market_queue.qsize(),
                self._market_queue.maxsize,
                self.stats.market_events_dropped,
            )
        if now_ms - self._last_market_queue_full_alert_ms < 300_000:
            return
        self._last_market_queue_full_alert_ms = now_ms
        self.context.alerts.emit(
            AppAlert(
                subject="AetherEdge market queue full",
                content=(
                    f"Dropped oldest market event before enqueueing {event.event_type.value}; "
                    f"queue_size={self._market_queue.qsize()} maxsize={self._market_queue.maxsize}\n"
                    f"pid={os.getpid()}\n"
                    f"runtime_id={self.app_config.strategy}::{self.app_config.symbol}\n"
                    f"dropped_total={self.stats.market_events_dropped}\n"
                ),
                severity="error",
            )
        )

    async def _process_account_event(self, event: AccountEvent) -> None:
        self.stats.account_events_seen += 1
        save = getattr(self.context.state_store, "save_account_event", None)
        if callable(save):
            await asyncio.to_thread(save, event)
        handler = getattr(self.context.strategy, "on_account_event", None)
        if not callable(handler):
            return
        signals = await handler(event)
        await self._execute_signals(signals or (), source=f"account:{event.exchange.value}", event_time_ms=event.event_time_ms)

    async def _drain_market_events_before_closed_bar(
        self,
        *,
        closed_bar_close_time_ms: int,
        max_events: int = 10_000,
        max_duration_ms: int = 3_000,
    ) -> MarketQueueDrainResult:
        queue_size_before = self._market_queue.qsize()
        started = time.monotonic()
        processed = 0
        examined = 0
        deferred: list[MarketEvent] = []
        max_duration_seconds = max(float(max_duration_ms), 0.0) / 1000.0

        while examined < max_events:
            if max_duration_seconds and time.monotonic() - started >= max_duration_seconds:
                break
            try:
                event = self._market_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            examined += 1
            try:
                if _is_trade_at_or_before(event, closed_bar_close_time_ms):
                    await self.process_market_event(event)
                    processed += 1
                else:
                    deferred.append(event)
            finally:
                self._market_queue.task_done()

        for event in deferred:
            await self._market_queue.put(event)

        elapsed_seconds = time.monotonic() - started
        duration_ms = int(elapsed_seconds * 1000)
        queue_size_after = self._market_queue.qsize()
        hit_event_limit = examined >= max_events
        hit_time_limit = max_duration_seconds > 0 and elapsed_seconds >= max_duration_seconds
        logger.info(
            "Drained market events before closed-bar decision | close_time_ms=%s processed=%s deferred=%s queue_size_before=%s queue_size_after=%s duration_ms=%s",
            closed_bar_close_time_ms,
            processed,
            len(deferred),
            queue_size_before,
            queue_size_after,
            duration_ms,
        )
        return MarketQueueDrainResult(
            processed=processed,
            deferred=len(deferred),
            examined=examined,
            queue_size_before=queue_size_before,
            queue_size_after=queue_size_after,
            duration_ms=duration_ms,
            hit_event_limit=hit_event_limit,
            hit_time_limit=hit_time_limit,
        )

    async def _consume_market_events(self, *, max_market_events: int | None) -> None:
        while not self._stop_event.is_set():
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                break
            if self.requirements.closed_kline.enabled:
                await self.poll_closed_bar_once()
            self._raise_on_unhealthy_producer()
            if self._all_producers_done() and self._market_queue.empty():
                break
            try:
                event = await asyncio.wait_for(self._market_queue.get(), timeout=max(self.runtime_config.scheduler_poll_seconds, 0.05))
            except asyncio.TimeoutError:
                continue
            events = [event]
            remaining_capacity = self._market_queue_drain_batch_size - 1
            if max_market_events is not None:
                remaining_capacity = min(remaining_capacity, max(0, max_market_events - self.stats.market_events_seen - 1))
            for _ in range(max(0, remaining_capacity)):
                try:
                    events.append(self._market_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for event in events:
                try:
                    await self.process_market_event(event)
                finally:
                    self._market_queue.task_done()
            if max_market_events is not None and self.stats.market_events_seen >= max_market_events:
                break

    async def _process_trade(self, trade: MarketTrade) -> None:
        if not self.requirements.range_bars.enabled:
            return
        builder = self._get_range_bar_builder()
        closed = builder.on_trade(trade)
        if not closed:
            return
        store = self._get_range_bar_store()
        for bar in closed:
            await asyncio.to_thread(store.save, [bar])
            self.stats.range_bars_closed += 1
            await self.process_market_feature(range_bar_closed_feature(bar, exchange=trade.exchange))

    async def _call_strategy_market_event(self, event: MarketEvent) -> Sequence[TradeSignal]:
        strategy = self.context.strategy
        if isinstance(event, MarketKline) or event.event_type is MarketEventType.KLINE:
            handler = getattr(strategy, "on_kline", None)
        elif isinstance(event, MarketTicker) or event.event_type is MarketEventType.TICKER:
            handler = getattr(strategy, "on_ticker", None)
        elif isinstance(event, MarketTrade) or event.event_type is MarketEventType.TRADE:
            handler = getattr(strategy, "on_trade", None)
        elif isinstance(event, MarketOrderBook) or event.event_type is MarketEventType.ORDER_BOOK:
            handler = getattr(strategy, "on_order_book", None)
        else:
            handler = None
        if not callable(handler):
            return ()
        return await handler(event) or ()

    def _trade_events_are_range_only(self) -> bool:
        strategy = self.context.strategy
        raw_flag = getattr(strategy, "raw_trade_callbacks_enabled", None)
        if raw_flag is False:
            return True

        cfg = getattr(strategy, "config", None)
        strategy_id = getattr(cfg, "strategy_id", "")
        if str(strategy_id).lower().startswith("eth_lf_portfolio_v9c"):
            return True

        return False

    async def _execute_signals(
        self,
        signals: Sequence[TradeSignal],
        *,
        source: str,
        event_time_ms: int | None,
        metadata: Mapping[str, Any] | None = None,
        feedback_depth: int = 0,
    ) -> None:
        for signal in signals:
            self.stats.signals_seen += 1
            if self.app_config.dry_run:
                self.stats.dry_run_actions += 1
                logger.info(
                    "Dry-run signal skipped | action=%s source=%s event_time_ms=%s",
                    signal.action.value,
                    source,
                    event_time_ms,
                )
                continue
            # ── Entry guard: block new OPEN signals while any follower close
            #     is still unresolved after master close. ──
            if signal.action in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT}:
                purpose = str(signal.metadata.get("execution_purpose", "") if signal.metadata else "").strip().lower()
                if purpose not in {"follower_recovery_topup"} and self._has_unresolved_follower_close():
                    logger.warning(
                        "Blocking new entry — unresolved follower close after master close detected | action=%s source=%s",
                        signal.action.value,
                        source,
                    )
                    self.context.alerts.emit(
                        AppAlert(
                            subject="AetherEdge entry blocked due to unresolved follower close",
                            severity="warning",
                            content=(
                                f"action={signal.action.value}\n"
                                f"source={source}\n"
                                f"reason=unresolved_follower_close_after_master_close\n"
                            ),
                        )
                    )
                    continue
            logger.info(
                "Executing signal | action=%s source=%s event_time_ms=%s",
                signal.action.value,
                source,
                event_time_ms,
            )
            intent = self._intent_factory.create(signal, source=source, event_time_ms=event_time_ms, metadata=metadata)
            results = await self._get_order_coordinator().execute(intent)
            self._record_order_results(results)
            self._save_order_results(signal, results)
            self._check_follower_close_failure(signal, results)
            if self.requirements.order_state.post_submit_sync_enabled:
                logger.info("Post-submit order sync started | action=%s source=%s", signal.action.value, source)
                await self._get_order_sync_service().sync_once(sync_type="post_submit", priority=True)
            if self.requirements.account_state.post_order_sync_enabled and signal.action in {SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT, SignalAction.CLOSE_LONG, SignalAction.CLOSE_SHORT}:
                await self._get_account_sync_service().sync_once(sync_type="post_order_account", priority=True)
            follow_up = await self._process_order_result_feedback(signal=signal, results=results, source=source, event_time_ms=event_time_ms)
            if follow_up:
                if feedback_depth >= 5:
                    logger.error("Order result feedback depth exceeded | action=%s source=%s", signal.action.value, source)
                    self.context.alerts.emit(AppAlert(subject="AetherEdge order feedback recursion blocked", content=f"action={signal.action.value} source={source}", severity="error"))
                    continue
                await self._execute_signals(follow_up, source="order_result_feedback", event_time_ms=event_time_ms, metadata={"parent_source": source}, feedback_depth=feedback_depth + 1)

    def _record_order_results(self, results: Sequence[ExchangeOrderResult]) -> None:
        self.stats.order_intents_created += 1
        self.stats.order_results_seen += len(results)
        ok_count = sum(1 for result in results if result.ok)
        if ok_count == len(results) and results:
            self.stats.submitted_intents += 1
            logger.info("Order intent submitted | exchanges=%s results=%s", ",".join(result.exchange.value for result in results), len(results))
            return
        if ok_count > 0:
            self.stats.partial_failures += 1
            logger.warning(
                "Order intent partially failed | ok=%s total=%s errors=%s",
                ok_count,
                len(results),
                [result.error for result in results if not result.ok],
            )
            self._set_health(
                RuntimePhase.RUNNING,
                healthy=False,
                error="partial exchange execution failure",
                metadata={**dict(self._health.metadata), "partial_failures": self.stats.partial_failures},
            )
        else:
            self.stats.failed_intents += 1
            logger.error("Order intent failed | total=%s errors=%s", len(results), [result.error for result in results])
            self._set_health(RuntimePhase.RUNNING, healthy=False, error="exchange execution failed")

    def _check_follower_close_failure(self, signal: TradeSignal, results: Sequence[ExchangeOrderResult]) -> None:
        purpose = str(signal.metadata.get("execution_purpose", "") if signal.metadata else "").strip().lower()
        if purpose != "follower_close_after_master_close":
            return
        now_ms = int(time.time() * 1000)
        position_id = str(signal.metadata.get("position_id", "unknown")) if signal.metadata else "unknown"
        target_exchanges = signal.metadata.get("target_exchanges", []) if signal.metadata else []
        # Check every targeted follower exchange independently. A single
        # filled result does not excuse another follower that is still open.
        for exchange_name in target_exchanges:
            exchange_str = str(exchange_name.value if hasattr(exchange_name, "value") else exchange_name).strip().lower()
            matched = [r for r in results if r.exchange.value == exchange_str]
            result = matched[0] if matched else None
            is_failure = (
                result is None
                or not result.ok
                or result.status is not OrderStatus.FILLED
                or result.filled_quantity is None
                or result.filled_quantity <= Decimal("0")
            )
            if not is_failure:
                continue
            throttle_key = f"{position_id}:{exchange_str}"
            last_ms = self._follower_close_alert_last_ms.get(throttle_key, 0)
            if now_ms - last_ms < 60_000:
                continue
            self._follower_close_alert_last_ms[throttle_key] = now_ms
            attempts = result.raw.get("attempts", 0) if result is not None and isinstance(result.raw, dict) else 0
            error_str = result.error if result is not None and result.error else ("missing result" if result is None else "not filled")
            self.context.alerts.emit(
                AppAlert(
                    subject="AetherEdge follower close failed after master close",
                    severity="error",
                    content=(
                        f"strategy_id={signal.metadata.get('strategy_id', 'unknown') if signal.metadata else 'unknown'}\n"
                        f"position_id={position_id}\n"
                        f"master_exchange={self.app_config.data_exchange.value}\n"
                        f"follower_exchange={exchange_str}\n"
                        f"symbol={signal.symbol}\n"
                        f"side={signal.action.value}\n"
                        f"quantity={str(signal.quantity)}\n"
                        f"status={result.status.value if result is not None and result.status else 'N/A'}\n"
                        f"filled_quantity={str(result.filled_quantity) if result is not None and result.filled_quantity is not None else 'N/A'}\n"
                        f"order_id={result.order_id if result is not None else 'N/A'}\n"
                        f"client_order_id={result.client_order_id if result is not None else 'N/A'}\n"
                        f"attempts={attempts}\n"
                        f"error={error_str}\n"
                        f"timestamp={now_ms}\n"
                    ),
                )
            )
            logger.error(
                "Follower close failed after master close | position_id=%s exchange=%s error=%s attempts=%s",
                position_id,
                exchange_str,
                error_str,
                attempts,
            )

    def _save_order_results(self, signal: TradeSignal, results: Sequence[ExchangeOrderResult]) -> None:
        save_order = getattr(self.context.state_store, "save_order", None)
        if not callable(save_order):
            return
        is_stop = signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}
        for result in results:
            if not result.ok:
                continue
            save_order(
                Order(
                    exchange=result.exchange,
                    symbol=signal.symbol,
                    raw_symbol=signal.symbol,
                    order_id=result.order_id,
                    client_order_id=result.client_order_id,
                    status=result.status or OrderStatus.UNKNOWN,
                    side=result.side,
                    quantity=result.quantity,
                    filled_quantity=result.filled_quantity,
                    raw=result.raw,
                ),
                is_stop_order=is_stop,
            )

    async def _process_order_result_feedback(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
        source: str,
        event_time_ms: int | None,
    ) -> Sequence[TradeSignal]:
        handler = getattr(self.context.strategy, "on_order_results", None)
        if not callable(handler):
            return ()
        follow_up = await handler(signal=signal, results=results, source=source, event_time_ms=event_time_ms)
        follow_up_count = len(follow_up or ())
        if follow_up_count > 0:
            logger.info("Strategy order results processed | action=%s results=%s follow_up_signals=%s", signal.action.value, len(results), follow_up_count)
        else:
            logger.debug("Strategy order results processed | action=%s results=%s follow_up_signals=0", signal.action.value, len(results))
        return follow_up or ()

    async def _stop_producers(self) -> None:
        for task in self._producer_tasks:
            task.cancel()
        if self._producer_tasks:
            await asyncio.gather(*self._producer_tasks, return_exceptions=True)
        self._producer_tasks = []

    async def _stop_sync_tasks(self) -> None:
        for task in self._sync_tasks:
            task.cancel()
        if self._sync_tasks:
            await asyncio.gather(*self._sync_tasks, return_exceptions=True)
        self._sync_tasks = []

    def _raise_on_unhealthy_producer(self) -> None:
        unhealthy = self._producer_supervisor.check()
        if not unhealthy:
            return
        self.stats.producer_failures += sum(1 for item in unhealthy if item.status.value == "failed")
        self.stats.producer_stale += sum(1 for item in unhealthy if item.status.value == "stale")
        message = "; ".join(f"{item.name}:{item.status.value}:{item.error}" for item in unhealthy)
        logger.error("Runtime producer unhealthy | %s", message)
        raise LiveRuntimeError(f"producer unhealthy: {message}")

    def _all_producers_done(self) -> bool:
        return bool(self._producer_tasks) and all(task.done() for task in self._producer_tasks)

    def _get_execution_clients(self) -> tuple[ExecutionClient, ...]:
        if self._execution_clients is None:
            injected = self.services.get("execution_clients")
            if injected is not None:
                self._execution_clients = tuple(injected)
            else:
                self._execution_clients = tuple(
                    create_execution_client(exchange, symbol=self.app_config.symbol, config=ExchangeConfig.from_env(exchange))
                    for exchange in self.app_config.exchanges
                )
        return self._execution_clients

    def _get_account_clients(self) -> tuple[AccountClient, ...]:
        if self._account_clients is None:
            injected = self.services.get("account_clients")
            if injected is not None:
                self._account_clients = tuple(injected)
            else:
                self._account_clients = tuple(
                    create_account_client(exchange, symbol=self.app_config.symbol, config=ExchangeConfig.from_env(exchange))
                    for exchange in self.app_config.exchanges
                )
        return self._account_clients

    def _get_order_journal(self):
        if self._order_journal is None:
            path = os.getenv("AETHER_ORDER_JOURNAL_DB", "data/state/aether_order_journal.sqlite3")
            self._order_journal = SqliteOrderJournalStore(path)
        return self._order_journal

    def _get_order_coordinator(self):
        if self._order_coordinator is None:
            journal = self._get_order_journal()
            self._order_coordinator = MultiExchangeOrderCoordinator(
                clients=self._get_execution_clients(),
                repository=journal,
                planner=self.context.planner,
                duplicate_guard=RepositoryDuplicateOrderGuard(journal),
                master_follower_policy=(
                    None
                    if self.runtime_config.master_follower_policy is None
                    else MasterFollowerExecutionPolicy.from_config(self.runtime_config.master_follower_policy)
                ),
                position_plan_store=self._get_position_plan_store(),
            )
        return self._order_coordinator

    def _get_recovery_service(self):
        if self._recovery_service == "__default__":
            clients = self._get_execution_clients()
            accounts = self._get_account_clients()
            contexts = [
                RecoveryExchangeContext(account=account, execution=execution, state_store=self.context.state_store)
                for account, execution in zip(accounts, clients, strict=False)
            ]
            self._recovery_service = RuntimeRecoveryService(exchange_contexts=contexts, order_journal=self._get_order_journal(), position_plan_store=self._get_position_plan_store())
        return self._recovery_service

    def _get_reconciliation_service(self):
        if self._reconciliation_service == "__default__":
            self._reconciliation_service = LiveStateReconciliationService(
                position_plan_store=self._get_position_plan_store(),
                order_journal=self._get_order_journal(),
                state_store=self.context.state_store,
                alert_sink=self.context.alerts,
            )
        return self._reconciliation_service

    async def _run_reconciliation(self, snapshots: tuple[PlatformSnapshot, ...]) -> None:
        """Run startup state reconciliation: compare exchange truth against
        local PositionPlan / LegPlan / order journal state and repair stale
        artifacts before producers or sync tasks start.

        CRITICAL: All exchange snapshots MUST be present. Reconciling with
        only one exchange can miss follower positions on the other exchange
        and wrongly close active PositionPlans (master/follower safety
        violation).
        """
        service = self._get_reconciliation_service()
        if service is None:
            return

        expected = len(self.app_config.exchanges)
        if len(snapshots) != expected:
            snapshot_exchanges = sorted(
                s.leverage.exchange.value if hasattr(s, "leverage") else str(s)
                for s in snapshots
            )
            raise LiveRuntimeError(
                f"startup reconciliation missing exchange snapshots: "
                f"expected {expected} exchanges "
                f"({', '.join(ex.value for ex in self.app_config.exchanges)}), "
                f"got {len(snapshots)} ({', '.join(snapshot_exchanges) if snapshot_exchanges else 'none'})"
            )

        exchange_names = ", ".join(
            s.leverage.exchange.value if hasattr(s, "leverage") else "?"
            for s in snapshots
        )
        logger.info("Startup reconciliation starting | exchanges=%s count=%s", exchange_names, len(snapshots))
        report = await service.reconcile_and_apply(snapshots)
        if report.stale_plans_closed > 0:
            logger.warning(
                "Startup reconciliation closed %s stale position plan(s) | "
                "fake_refs=%s verdict=%s",
                report.stale_plans_closed,
                len(report.fake_order_refs_found),
                report.verdict.value,
            )
        if report.fake_order_refs_found:
            for ref in report.fake_order_refs_found:
                logger.warning(
                    "Fake order ref cleaned | position_id=%s exchange=%s "
                    "field=%s value=%s reason=%s",
                    ref.position_id,
                    ref.exchange,
                    ref.field,
                    ref.value,
                    ref.reason,
                )
        if report.unresolved_follower_positions > 0:
            logger.warning(
                "Startup reconciliation: %s unresolved follower position(s) | "
                "position_id(s)=%s",
                report.unresolved_follower_positions,
                ", ".join(
                    a.target for a in report.actions
                    if a.action_type == "set_master_closed_follower_close_required"
                ),
            )
        for alert_dict in report.alerts:
            self.context.alerts.emit(
                AppAlert(
                    subject=alert_dict["subject"],
                    content=alert_dict["content"],
                    severity=alert_dict.get("severity", "error"),
                )
            )
        if report.verdict in {
            "fail_unresolved_follower_position",
        }:
            logger.error(
                "Startup reconciliation failed | verdict=%s issues=%s",
                report.verdict.value,
                report.issues,
            )
        elif report.stale_plans_closed > 0 or report.fake_order_refs_found:
            logger.info(
                "Startup reconciliation passed with cleanup | "
                "verdict=%s stale_plans_closed=%s fake_refs=%s",
                report.verdict.value,
                report.stale_plans_closed,
                len(report.fake_order_refs_found),
            )
        else:
            logger.info(
                "Startup reconciliation passed | verdict=%s",
                report.verdict.value,
            )

    def _get_sync_contexts(self) -> tuple[SyncExchangeContext, ...]:
        if ("execution_clients" in self.services) != ("account_clients" in self.services):
            raise LiveRuntimeError("request sync requires account_clients and execution_clients to be injected together")
        clients = self._get_execution_clients()
        accounts = self._get_account_clients()
        execution_by_exchange = {client.exchange: client for client in clients}
        account_by_exchange = {client.exchange: client for client in accounts}
        expected = set(self.app_config.exchanges)
        if set(execution_by_exchange) != set(account_by_exchange):
            raise LiveRuntimeError(
                "request sync account/execution exchange mismatch: "
                f"accounts={sorted(exchange.value for exchange in account_by_exchange)} "
                f"executions={sorted(exchange.value for exchange in execution_by_exchange)}"
            )
        if set(execution_by_exchange) != expected:
            raise LiveRuntimeError(
                "request sync clients do not cover configured exchanges: "
                f"expected={sorted(exchange.value for exchange in expected)} "
                f"actual={sorted(exchange.value for exchange in execution_by_exchange)}"
            )
        return tuple(
            SyncExchangeContext(account=account_by_exchange[exchange], execution=execution_by_exchange[exchange], state_store=self.context.state_store)
            for exchange in self.app_config.exchanges
        )

    def _get_account_sync_service(self):
        if self._account_sync_service is None:
            self._account_sync_service = AccountStateSyncService(
                contexts=self._get_sync_contexts(),
                config=self.requirements.account_state,
                alert_sink=self.context.alerts,
                throttle=self._request_sync_throttle,
            )
        return self._account_sync_service

    def _get_order_sync_service(self):
        if self._order_sync_service is None:
            self._order_sync_service = OrderStateSyncService(
                contexts=self._get_sync_contexts(),
                config=self.requirements.order_state,
                alert_sink=self.context.alerts,
                throttle=self._request_sync_throttle,
                active_check=self._order_sync_active,
                position_plan_store=self._get_position_plan_store(),
            )
        return self._order_sync_service

    def _order_sync_active(self) -> bool:
        strategy = self.context.strategy
        position = getattr(strategy, "position", None)
        if bool(getattr(position, "in_pos", False)):
            return True
        if getattr(strategy, "pending_entry", None) is not None:
            return True
        store = self._position_plan_store
        if store is not None and callable(getattr(store, "list_active_positions", None)) and store.list_active_positions():
            return True
        list_open = getattr(self.context.state_store, "list_open_orders", None)
        if callable(list_open):
            for exchange in self.app_config.exchanges:
                if list_open(exchange=exchange, symbol=self.app_config.symbol, include_stop_orders=True):
                    return True
        return False

    def _get_position_plan_store(self):
        if self._position_plan_store is None:
            path = os.getenv("AETHER_POSITION_PLAN_DB", "data/state/aether_position_plan.sqlite3")
            self._position_plan_store = SqlitePositionPlanStore(path)
        return self._position_plan_store

    def _get_range_bar_builder(self):
        if self._range_bar_builder is None:
            profile = self.context.data.market_profile
            contract_value = profile.contract_value(self.app_config.data_exchange) or Decimal("1")
            self._range_bar_builder = RangeBarBuilder(range_pct=self._range_pct, contract_value=contract_value)
        return self._range_bar_builder

    def _get_range_bar_store(self):
        if self._range_bar_store is None:
            self._range_bar_store = SqliteRangeBarStore()
        return self._range_bar_store

    def _get_range_bar_aggregator(self):
        if self._range_bar_aggregator is None:
            self._range_bar_aggregator = RangeBarAggregator()
        return self._range_bar_aggregator

    def _set_health(
        self,
        phase: RuntimePhase,
        *,
        healthy: bool | None = None,
        warmup_complete: bool | None = None,
        caught_up: bool | None = None,
        last_market_event_time_ms: int | None = None,
        error: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        self._health = RuntimeHealth(
            phase=phase,
            healthy=self._health.healthy if healthy is None else healthy,
            warmup_complete=self._health.warmup_complete if warmup_complete is None else warmup_complete,
            caught_up=self._health.caught_up if caught_up is None else caught_up,
            last_market_event_time_ms=self._health.last_market_event_time_ms if last_market_event_time_ms is None else last_market_event_time_ms,
            error=error if error is not None else self._health.error,
            metadata=dict(self._health.metadata if metadata is None else metadata),
        )


def _event_time_ms(event: MarketEvent) -> int | None:
    if isinstance(event, MarketTrade):
        return event.trade_time_ms if event.trade_time_ms is not None else event.event_time_ms
    if isinstance(event, MarketOrderBook):
        return event.event_time_ms
    if isinstance(event, MarketKline):
        return event.close_time_ms
    if isinstance(event, MarketTicker):
        return event.time_ms
    return None


def _is_trade_at_or_before(event: MarketEvent, close_time_ms: int) -> bool:
    if not isinstance(event, MarketTrade) and event.event_type is not MarketEventType.TRADE:
        return False
    event_ms = _event_time_ms(event)
    return event_ms is not None and event_ms <= close_time_ms


async def _jittered_sleep(stop_event: asyncio.Event, interval_seconds: float) -> None:
    import random
    jitter = random.uniform(0, min(5.0, interval_seconds * 0.1))
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds + jitter)
    except asyncio.TimeoutError:
        pass
