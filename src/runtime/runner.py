from __future__ import annotations

import asyncio
import inspect
import logging
import os
import queue
from pathlib import Path
import threading
import time
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence

from src.app import AppConfig, AppContext
from src.app.alerts import AppAlert
from src.market_data.derived import (
    RangeBarAggregator,
    RangeBarBuilder,
)
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import MarketDataSet, RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange, WarmupRequest
from src.market_data.range_checkpoint import (
    RangeBuilderCheckpoint,
    RangeCheckpointRecovery,
    RangeCheckpointWriter,
    SqliteRangeCheckpointStore,
    aggregate_snapshot,
)
from src.market_data.range_repair import (
    JOURNAL_INVALID_DROPPED_TRADE,
    JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
    JOURNAL_INVALID_PRODUCER_FAILED,
    JOURNAL_INVALID_PRODUCER_STALE,
    RangeRepairJournalWriter,
    RangeRepairTrade,
)
from src.market_data.storage import SqliteKlineStore, SqliteRangeBarStore
from src.market_data.warmup.gap_detector import interval_to_ms
from src.market_data.warmup.service import KlineWarmupService
from src.order_management import LegSyncStatus, MasterFollowerExecutionPolicy, MultiExchangeOrderCoordinator, PositionPlanStatus, RepositoryDuplicateOrderGuard, SqliteOrderJournalStore, SqlitePositionPlanStore
from src.order_management.position_plan.models import LegRole
from src.order_management.models import ExchangeOrderResult, OrderIntentStatus
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.reconciliation.service import LiveStateReconciliationService
from src.order_management.safety import (
    RecoveryExitOrderValidator,
    filter_orders_for_position_scope,
)
from src.platform import create_account_client, create_execution_client
from src.platform.account.events import AccountEvent
from src.platform.account.ports import AccountClient
from src.platform.config import ProjectEnvConfig, get_project_env_config
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, InstrumentRule, Order, OrderStatus, Position, PositionMode, PositionSide
from src.platform.execution.ports import ExecutionClient
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.runtime.account_config import (
    AccountConfigBootstrapResult,
    AccountConfigEnv,
    bootstrap_account_config,
    load_account_config_env,
    raise_on_failed_account_config,
)
from src.runtime.account_sync import AccountStateSyncService, OrderStateSyncService, RequestThrottle, SyncExchangeContext
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.features import closed_kline_feature, range_aggregate_feature, range_aggregate_unavailable_feature, range_bar_closed_feature
from src.runtime.feature_pipeline import TradeDerivedFeaturePipeline
from src.runtime.heartbeat import RuntimeHeartbeatService
from src.runtime.position_mode_gate import (
    fetch_position_mode_statuses,
    resolve_position_mode_requirements,
)
from src.runtime.market_features import (
    dispatch_market_feature_event,
    resolve_market_feature_observers,
)
from src.runtime.models import RuntimeHealth, RuntimeMode, RuntimePhase
from src.runtime.range_backfill_supervisor import RangeBackfillSupervisor, RangeBackfillSupervisorConfig
from src.runtime.range_micro_repair_supervisor import (
    RangeMicroRepairSupervisor,
)
from src.runtime.range_repair_bootstrap import RangeRepairBootstrapService
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher
from src.runtime.requirements import StrategyRuntimeRequirements, resolve_strategy_runtime_requirements
from src.runtime.startup_catchup import (
    StartupCatchupConfig,
    StartupCatchupDecision,
    _check_price_guard,
    _deviation_pct,
    evaluate_startup_catchup_eligibility,
)
from src.runtime.startup_feature_backfill import (
    resolve_startup_feature_backfill_providers,
)
from src.runtime.strategy_positions import (
    StrategyPositionSnapshotIndex,
    resolve_strategy_position_snapshot_index,
)
from src.runtime.strategy_host import StrategyHost
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.recovery.service import RecoveryExchangeContext, RuntimeRecoveryService
from src.runtime.recovery.models import RecoveryReport
from src.runtime.tasks import ClosedBarScheduler, ProducerHealthMonitor, ProducerSupervisor
from src.runtime.tasks.scheduler import closed_bar_open_time_ms
from src.signals import TradeSignal
from src.signals.models import SignalAction
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)
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


@dataclass(frozen=True)
class _BackgroundWriteItem:
    description: str
    write: Callable[[], None]
    on_error: Callable[[BaseException], None] | None = None


class _BackgroundWriteQueue:
    """Small bounded thread writer for live non-critical persistence."""

    _STOP = object()

    def __init__(self, *, name: str, max_pending: int = 1000) -> None:
        if max_pending <= 0:
            raise ValueError("max_pending must be positive")
        self.name = name
        self.max_pending = int(max_pending)
        self._queue: queue.Queue[object] = queue.Queue(maxsize=self.max_pending)
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stopping = False
        self.submitted = 0
        self.written = 0
        self.dropped = 0
        self.failures = 0
        self._drop_warn_every = 100
        self._last_drop_warned_at = 0

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopping = False
            self._thread = threading.Thread(
                target=self._run,
                name=self.name,
                daemon=True,
            )
            self._thread.start()

    def submit(self, item: _BackgroundWriteItem) -> bool:
        self.start()
        with self._lock:
            if self._stopping:
                self.dropped += 1
                self._warn_drop(reason="stopping")
                return False
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self.dropped += 1
                self._warn_drop(reason="queue_full_evicted_oldest")
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                self.dropped += 1
                self._warn_drop(reason="queue_full_double_fail")
                return False
        self.submitted += 1
        return True

    def _warn_drop(self, *, reason: str) -> None:
        """Log a warning on first drop and every Nth drop thereafter."""
        if self.dropped == 1 or self.dropped - self._last_drop_warned_at >= self._drop_warn_every:
            self._last_drop_warned_at = self.dropped
            logger.warning(
                "Background write queue dropped item | name=%s reason=%s "
                "dropped=%s submitted=%s written=%s failures=%s pending=%s",
                self.name,
                reason,
                self.dropped,
                self.submitted,
                self.written,
                self.failures,
                self.pending_count,
            )

    def stop(self, *, flush: bool = True, timeout: float = 5.0) -> None:
        with self._lock:
            if not flush:
                while True:
                    try:
                        self._queue.get_nowait()
                        self._queue.task_done()
                    except queue.Empty:
                        break
            self._stopping = True
            thread = self._thread
        if thread is None:
            return
        if not thread.is_alive():
            with self._lock:
                if self._thread is thread:
                    self._thread = None
            return
        try:
            self._queue.put(self._STOP, timeout=max(0.1, timeout))
        except queue.Full:
            return
        thread.join(timeout=max(0.0, timeout))
        if not thread.is_alive():
            with self._lock:
                if self._thread is thread:
                    self._thread = None

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is self._STOP:
                    return
                if not isinstance(item, _BackgroundWriteItem):
                    self.dropped += 1
                    continue
                try:
                    item.write()
                    self.written += 1
                except BaseException as exc:
                    self.failures += 1
                    if item.on_error is not None:
                        try:
                            item.on_error(exc)
                        except BaseException:
                            pass
            finally:
                self._queue.task_done()


class LiveRuntimeError(RuntimeError):
    pass


# ── Fatal error classification markers ──
FATAL_STARTUP_ERROR_MARKERS = (
    "closed-kline warmup loaded insufficient records",
    "closed-kline warmup did not catch up",
    "startup snapshot is required before live trading",
    "startup reconciliation missing exchange snapshots",
    "startup reconciliation failed",
    "runtime recovery failed",
    "strategy position mode requirement failed",
    "live preflight/smoke report gate failed",
    "direct-live trading requires aether_required_live_strategy",
    "live strategy does not match required launch target",
    "private_credentials",
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
        injected_strategy_host = self.services.get("strategy_host")
        self._strategy_host = (
            injected_strategy_host
            if injected_strategy_host is not None
            else StrategyHost(app_context.strategy)
        )
        self._project_env: ProjectEnvConfig = self.services.get("project_env_config") or get_project_env_config()
        self._account_config_env: AccountConfigEnv | None = None
        self._account_config_new_entries_blocked: bool = False
        self._account_config_apply_writes: bool = False
        self._account_config_results: tuple[AccountConfigBootstrapResult, ...] = ()
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
        self._live_persistence_writer = self.services.get("live_persistence_writer")
        self._persistence_alert_loop: asyncio.AbstractEventLoop | None = None
        self._fixed_time_trade_bar_builder_compat = self.services.get(
            "fixed_time_trade_bar_builder"
        )
        self._trade_footprint_builder_compat = self.services.get("trade_footprint_builder")
        self._range_footprint_builder_compat = self.services.get("range_footprint_builder")
        injected_trade_pipeline = self.services.get(
            "trade_derived_feature_pipeline"
        )
        self._trade_derived_feature_pipeline = (
            injected_trade_pipeline
            if injected_trade_pipeline is not None
            else TradeDerivedFeaturePipeline(
                strategy=self.context.strategy,
                emit_feature=self.process_market_feature,
                fixed_time_trade_bar_builder=self._fixed_time_trade_bar_builder_compat,
                trade_footprint_builder=self._trade_footprint_builder_compat,
                range_footprint_builder=self._range_footprint_builder_compat,
            )
        )
        self._range_checkpoint_store = self.services.get("range_checkpoint_store")
        self._range_checkpoint_writer = self.services.get("range_checkpoint_writer")
        self._range_repair_journal_store = self.services.get(
            "range_repair_journal_store"
        )
        self._range_repair_journal_writer = self.services.get(
            "range_repair_journal_writer"
        )
        self._range_repair_bootstrap_service = self.services.get(
            "range_repair_bootstrap_service"
        )
        self._range_repair_journal_bucket_ms: int | None = None
        self._range_repair_checkpoint_last_trade_ts_ms: int | None = None
        self._range_repair_first_live_submitted = False
        self._range_repair_journal_finalize_submitted = False
        self._range_repair_journal_append_failure_warned = False
        self._producer_monitor: ProducerHealthMonitor = self.services.get("producer_monitor") or ProducerHealthMonitor()
        self._producer_supervisor: ProducerSupervisor = self.services.get("producer_supervisor") or ProducerSupervisor(
            monitor=self._producer_monitor,
            stale_after_ms=self.runtime_config.producer_stale_timeout_ms,
        )
        self._closed_bar_interval = self.requirements.closed_kline.interval if self.requirements.closed_kline.enabled else self.runtime_config.closed_bar_interval
        self._closed_bar_buffer_ms = self.requirements.closed_kline.close_buffer_ms if self.requirements.closed_kline.close_buffer_ms is not None else self.runtime_config.closed_bar_buffer_ms
        self._closed_bar_retry_interval_ms = self.requirements.closed_kline.retry_interval_ms if self.requirements.closed_kline.retry_interval_ms is not None else self.runtime_config.closed_bar_retry_interval_ms
        self._closed_bar_missing_alert_after_ms = self.requirements.closed_kline.missing_alert_after_ms if self.requirements.closed_kline.missing_alert_after_ms is not None else self.runtime_config.closed_bar_missing_alert_after_ms
        self._closed_bar_interval_ms = interval_to_ms(self._closed_bar_interval)
        self._range_pct = self.requirements.range_bars.range_pct if self.requirements.range_bars.enabled else self.runtime_config.range_pct
        self._range_aggregate_interval = self.requirements.range_bars.aggregate_interval if self.requirements.range_bars.enabled else self._closed_bar_interval
        self._closed_bar_scheduler: ClosedBarScheduler = self.services.get("closed_bar_scheduler") or ClosedBarScheduler(
            interval_ms=self._closed_bar_interval_ms,
            close_buffer_ms=self._closed_bar_buffer_ms,
            retry_interval_ms=self._closed_bar_retry_interval_ms,
            missing_alert_after_ms=self._closed_bar_missing_alert_after_ms,
        )
        self._rangebar_trust_start_bucket_ms: int | None = None
        self._initial_range_bucket_ms: int | None = None
        self._initial_range_recovery: RangeCheckpointRecovery | None = None
        self._range_bars_by_bucket: dict[int, list[RangeBar]] = {}
        self._range_repaired_complete_buckets: set[int] = set()
        self._range_bars_bucket_prune_count: int = 3
        self._last_range_checkpoint_submit_ms = 0
        self._range_bars_since_checkpoint = 0
        self._range_checkpoint_snapshot_warned = False
        self._range_builder_reset_at_bucket_ms: int | None = None
        self._intent_factory = self.services.get("intent_factory") or LiveOrderIntentFactory(
            strategy_id=self.app_config.strategy,
            target_exchanges=self.app_config.exchanges,
        )
        self._last_snapshot: PlatformSnapshot | None = self.services.get("snapshot")
        self._last_snapshots: tuple[PlatformSnapshot, ...] = ()
        self._last_account_snapshot_log_state: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}
        self._last_account_snapshot_log_ms: dict[tuple[str, str], int] = {}
        self._account_snapshot_log_keepalive_seconds = _account_snapshot_log_keepalive_seconds_from_env(self._project_env)
        self._last_market_queue_full_log_ms = 0
        self._last_market_queue_full_alert_ms = 0
        self._last_market_queue_backlog_log_ms = 0
        self._last_live_data_path_log_ms = 0
        self._latest_fixed_time_trade_bar_open_time_ms: int | None = None
        self._market_queue_backlog_warn_threshold = self._project_env.get_int("AETHER_MARKET_QUEUE_BACKLOG_WARN_THRESHOLD", 500)
        self._market_queue_drain_batch_size = self._project_env.get_int("AETHER_MARKET_QUEUE_DRAIN_BATCH_SIZE", 1000)
        self._last_trade_health_update_ms = 0
        self._range_context_degraded_buckets: dict[int, str] = {}
        self._executed_range_aggregate_buckets: set[tuple[str, str, int]] = set()
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
        self._range_speed_warmup_excluded_previous = False
        self._startup_catchup_range_observed = False
        self._range_speed_complete_history = 0
        self._range_speed_min_periods = 0
        self._range_backfill_supervisor = self.services.get("range_backfill_supervisor")
        self._startup_feature_backfill_providers = self.services.get(
            "startup_feature_backfill_providers"
        )
        self._feature_backfill_providers_resolved = False
        self._range_micro_repair_supervisor = self.services.get(
            "range_micro_repair_supervisor"
        )
        self._range_speed_history_refresher = self.services.get("range_speed_history_refresher")

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
            await self._stop_range_speed_background_services()
            await self._stop_sync_tasks()
            await self._stop_producers()
            await self._stop_live_persistence_writer()
            await self._stop_range_repair_journal_writer()
            await self._stop_range_checkpoint_writer()
            await self.context.alerts.stop()

    async def start(self) -> RuntimeHealth:
        self._set_health(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)
        return self._health

    async def stop(self) -> RuntimeHealth:
        self._stop_event.set()
        await self._stop_range_speed_background_services()
        await self._stop_producers()
        await self._stop_live_persistence_writer()
        await self._stop_range_repair_journal_writer()
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
                self._maybe_log_live_data_path_stats()
                return
        signals = await self._call_strategy_market_event(event)
        await self._execute_signals(signals, source=event.event_type.value, event_time_ms=event_ms)
        self._maybe_log_live_data_path_stats()

    async def process_market_feature(self, event: MarketFeatureEvent) -> None:
        self.stats.feature_events_seen += 1
        if event.type_value == "fixed_time_trade_bar" and isinstance(event.data, dict):
            open_ms = event.data.get("open_time_ms")
            if isinstance(open_ms, int):
                self._latest_fixed_time_trade_bar_open_time_ms = open_ms
        # Track closed bar open times for heartbeat diagnostics.
        hb = getattr(self, "_heartbeat_service", None)
        if hb is not None and event.type_value == "closed_kline":
            open_ms = event.data.get("open_time_ms") if isinstance(event.data, dict) else None
            if isinstance(open_ms, int):
                hb.note_closed_bar(open_ms)
        signals = await dispatch_market_feature_event(self.context.strategy, event)
        await self._execute_signals(signals, source=event.type_value, event_time_ms=event.event_time_ms, metadata={"feature_type": event.type_value})
        self._maybe_log_live_data_path_stats()

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
                "range_exit_triggered=False range_exit_reason=None range_exit_peak_r=None "
                "range_exit_current_r=None range_exit_giveback_frac=None "
                f"position=None position_side=None position_engine=None position_stop=None close={closed_kline.close} "
                f"close_buffer_ms={self._closed_bar_buffer_ms}"
            )
            return

        actions = audit.get("actions") or []
        logger.info(
            "4H decision completed | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s decision=%s actions=%s selected_engine=%s selected_side=%s risk_mult=%s quality_mult=%s micro_action=%s micro_scale=%s micro_aligned=%s micro_contra=%s range_available=%s range_status=%s range_bar_count=%s range_min_required=%s range_imbalance=%s range_taker_buy_ratio=%s range_close_pos=%s range_micro_return_pct=%s range_exit_triggered=%s range_exit_reason=%s range_exit_peak_r=%s range_exit_current_r=%s range_exit_giveback_frac=%s position=%s position_side=%s position_engine=%s position_stop=%s close=%s close_buffer_ms=%s",
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
            audit.get("range_exit_triggered"),
            audit.get("range_exit_reason"),
            audit.get("range_exit_peak_r"),
            audit.get("range_exit_current_r"),
            audit.get("range_exit_giveback_frac"),
            audit.get("position_in_pos"),
            audit.get("position_side"),
            audit.get("position_engine"),
            audit.get("position_stop"),
            closed_kline.close,
            self._closed_bar_buffer_ms,
        )
        engine_diag_text = audit.get("engine_diag_text")
        if isinstance(engine_diag_text, str) and engine_diag_text.strip():
            logger.info(
                "4H engine diagnostics | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s\n%s",
                self.app_config.symbol,
                self._closed_bar_interval,
                audit.get("bar_open_time_ms"),
                audit.get("bar_close_time_ms"),
                engine_diag_text,
            )

    async def poll_closed_bar_once(self, *, now_ms: int | None = None) -> list[MarketFeatureEvent]:
        now = int(time.time() * 1000) if now_ms is None else now_ms
        open_time_ms = self._closed_bar_scheduler.due_closed_bar(now)
        if open_time_ms is None:
            return []
        rows = await self.context.data.fetch_klines(
            interval=self._closed_bar_interval,
            limit=10,
            start_time_ms=open_time_ms,
            end_time_ms=open_time_ms,
            use_cache=False,
            oldest_first=True,
        )
        closed_rows = [row for row in rows if row.is_closed and row.open_time_ms == open_time_ms]
        if not closed_rows:
            should_alert = getattr(self._closed_bar_scheduler, "should_alert_missing", None)
            if callable(should_alert) and should_alert(open_time_ms, now):
                close_time_ms = open_time_ms + self._closed_bar_interval_ms
                self.context.alerts.emit(
                    AppAlert(
                        subject="AetherEdge closed bar missing",
                        severity="error",
                        content=(
                            f"symbol={self.app_config.symbol}\n"
                            f"interval={self._closed_bar_interval}\n"
                            f"open_time_ms={open_time_ms}\n"
                            f"close_time_ms={close_time_ms}\n"
                            f"now_ms={now}\n"
                            f"missing_after_ms={self._closed_bar_missing_alert_after_ms}\n"
                        ),
                    )
                )
                logger.error(
                    "Closed bar missing after retry window | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s now_ms=%s",
                    self.app_config.symbol,
                    self._closed_bar_interval,
                    open_time_ms,
                    close_time_ms,
                    now,
                )
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
        self._finalize_range_repair_journal(
            bucket_start_ms=open_time_ms,
            finalized_at_ms=now,
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
        self._refresh_range_micro_repair_coverage(open_time_ms)
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
                    coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
                )
                await self.process_market_feature(unavailable)
                features.append(unavailable)
                self._log_4h_decision_summary(open_time_ms=open_time_ms, closed_kline=closed_kline)
                self._persist_closed_kline(closed_kline)
                return features

        best_range_bar_count = 0
        min_range_bars = self._get_min_range_bars()
        range_aggregates = self._load_range_aggregates_for_bucket(open_time_ms)
        if range_aggregates:
            best_range_bar_count = max((int(aggregate.bar_count) for aggregate in range_aggregates), default=0)
            if (
                not is_mid_bucket_restart
                or (
                    self._initial_range_recovery is None
                    and best_range_bar_count >= min_range_bars
                )
            ):
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
                self._persist_closed_kline(closed_kline)
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
                coverage_status=self._range_coverage_for_bucket(
                    open_time_ms
                ).coverage_status,
                missing_gap_ms=self._range_coverage_for_bucket(
                    open_time_ms
                ).missing_gap_ms,
                range_recovered_from_checkpoint=self._range_coverage_for_bucket(
                    open_time_ms
                ).recovered_from_checkpoint,
                range_checkpoint_age_ms=self._range_coverage_for_bucket(
                    open_time_ms
                ).checkpoint_age_ms,
            )
            await self.process_market_feature(unavailable)
            features.append(unavailable)
            self._log_4h_decision_summary(open_time_ms=open_time_ms, closed_kline=closed_kline)
            self._persist_closed_kline(closed_kline)
            return features

        if (
            self._initial_range_recovery is not None
            and not range_aggregates
            and self.requirements.range_bars.enabled
            and self.requirements.trades.enabled
        ):
            coverage = self._range_coverage_for_bucket(open_time_ms)
            unavailable = range_aggregate_unavailable_feature(
                symbol=self.app_config.symbol,
                exchange=self.app_config.data_exchange,
                timeframe=self._range_aggregate_interval,
                range_pct=self._range_pct,
                bucket_start_ms=open_time_ms,
                bucket_end_ms=open_time_ms
                + self._closed_bar_interval_ms
                - 1,
                reference_price=closed_kline.close,
                reason="no_completed_range_bars",
                coverage_status=coverage.coverage_status,
                missing_gap_ms=coverage.missing_gap_ms,
                range_recovered_from_checkpoint=coverage.recovered_from_checkpoint,
                range_checkpoint_age_ms=coverage.checkpoint_age_ms,
            )
            await self.process_market_feature(unavailable)
            features.append(unavailable)

        self._log_4h_decision_summary(open_time_ms=open_time_ms, closed_kline=closed_kline)
        self._persist_closed_kline(closed_kline)
        return features

    def _persist_closed_kline(self, kline: MarketKline) -> None:
        """Queue one confirmed live closed kline after decisions complete."""

        def write() -> None:
            repository = self.services.get("kline_store")
            if repository is None:
                repository = SqliteKlineStore(
                    self.runtime_config.market_data_db_path
                )
                self.services["kline_store"] = repository
            repository.save([kline])

        self._submit_live_persistence_write(
            description="closed_kline",
            write=write,
            on_error=lambda exc: self._on_closed_kline_persist_error(
                kline, exc
            ),
        )

    def _on_closed_kline_persist_error(
        self, kline: MarketKline, exc: BaseException
    ) -> None:
        try:
            logger.exception(
                "Failed to persist live closed kline | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s",
                kline.symbol,
                kline.interval,
                kline.open_time_ms,
                kline.close_time_ms,
            )
            self._emit_alert_threadsafe(
                AppAlert(
                    subject="AetherEdge closed kline persistence failed",
                    severity="error",
                    content=(
                        f"symbol={kline.symbol}\n"
                        f"interval={kline.interval}\n"
                        f"open_time_ms={kline.open_time_ms}\n"
                        f"close_time_ms={kline.close_time_ms}\n"
                        f"error={type(exc).__name__}:{exc}\n"
                    ),
                )
            )
        except Exception:
            logger.exception(
                "Failed to emit closed kline persistence alert | symbol=%s interval=%s open_time_ms=%s",
                kline.symbol,
                kline.interval,
                kline.open_time_ms,
            )

    def _persist_range_bar(self, bar: RangeBar) -> None:
        """Queue one closed range bar after feature dispatch."""

        def write() -> None:
            self._get_range_bar_store().save([bar])

        self._submit_live_persistence_write(
            description="range_bar",
            write=write,
            on_error=lambda exc: self._on_range_bar_persist_error(bar, exc),
        )

    def _on_range_bar_persist_error(
        self, bar: RangeBar, exc: BaseException
    ) -> None:
        logger.exception(
            "Failed to persist live range bar | symbol=%s range_pct=%s bar_id=%s start_time_ms=%s end_time_ms=%s",
            bar.symbol,
            bar.range_pct,
            bar.bar_id,
            bar.start_time_ms,
            bar.end_time_ms,
        )
        self._emit_alert_threadsafe(
            AppAlert(
                subject="AetherEdge range bar persistence failed",
                severity="warning",
                content=(
                    f"symbol={bar.symbol}\n"
                    f"range_pct={bar.range_pct}\n"
                    f"bar_id={bar.bar_id}\n"
                    f"start_time_ms={bar.start_time_ms}\n"
                    f"end_time_ms={bar.end_time_ms}\n"
                    f"error={type(exc).__name__}:{exc}\n"
                ),
            )
        )

    def _persist_completed_range_aggregate(
        self,
        aggregate: RangeBarAggregate,
        *,
        coverage_status: str,
        missing_gap_ms: int,
    ) -> None:
        def write() -> None:
            self._get_range_checkpoint_store().save_completed_aggregate(
                exchange=self.app_config.data_exchange.value,
                aggregate=aggregate,
                coverage_status=coverage_status,
                missing_gap_ms=missing_gap_ms,
                completed_at_ms=int(time.time() * 1000),
            )

        self._submit_live_persistence_write(
            description="completed_range_aggregate",
            write=write,
            on_error=lambda exc: self._on_completed_range_aggregate_persist_error(
                aggregate, exc
            ),
        )

    def _on_completed_range_aggregate_persist_error(
        self, aggregate: RangeBarAggregate, exc: BaseException
    ) -> None:
        logger.warning(
            "Failed to persist completed range aggregate | symbol=%s range_pct=%s bucket_start_ms=%s error=%s",
            aggregate.symbol,
            aggregate.range_pct,
            aggregate.bucket_start_ms,
            exc,
        )

    def _load_range_aggregates_for_bucket(self, bucket_start_ms: int) -> list[RangeBarAggregate]:
        rows = self._range_bar_rows_for_bucket(bucket_start_ms)
        if not rows:
            return []
        aggregates = self._get_range_bar_aggregator().aggregate(rows, bucket_ms=self._closed_bar_interval_ms)
        return [aggregate for aggregate in aggregates if aggregate.bucket_start_ms == bucket_start_ms]

    def _range_bar_rows_for_bucket(
        self, bucket_start_ms: int
    ) -> list[RangeBar]:
        # ── Micro-repair complete: force DB load, ignore partial memory ──
        if bucket_start_ms in getattr(self, "_range_repaired_complete_buckets", set()):
            rows = self._get_range_bar_store().load(
                symbol=self.app_config.symbol,
                range_pct=str(self._range_pct),
                time_range=TimeRange(
                    bucket_start_ms,
                    bucket_start_ms + self._closed_bar_interval_ms - 1,
                ),
            )
            if rows:
                if not hasattr(self, "_range_bars_by_bucket"):
                    self._range_bars_by_bucket = {}
                self._range_bars_by_bucket[bucket_start_ms] = list(rows)
                self._prune_range_bars_by_bucket(current_bucket=bucket_start_ms)
                logger.info(
                    "Range bar rows loaded from repaired DB | bucket_start_ms=%s "
                    "row_count=%s",
                    bucket_start_ms,
                    len(rows),
                )
                return list(rows)
            # DB load returned empty — do NOT fall back to partial memory.
            logger.warning(
                "Range repaired complete bucket has no DB rows | "
                "bucket_start_ms=%s repaired_complete=True fallback=unavailable",
                bucket_start_ms,
            )
            return []

        range_bars_by_bucket = getattr(self, "_range_bars_by_bucket", {})
        if bucket_start_ms in range_bars_by_bucket:
            memory_rows = list(range_bars_by_bucket[bucket_start_ms])
            if memory_rows or not self._range_store_fallback_allowed(
                bucket_start_ms
            ):
                return memory_rows

        rows = self._get_range_bar_store().load(
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            time_range=TimeRange(
                bucket_start_ms,
                bucket_start_ms + self._closed_bar_interval_ms - 1,
            ),
        )
        if rows:
            if not hasattr(self, "_range_bars_by_bucket"):
                self._range_bars_by_bucket = {}
            self._range_bars_by_bucket[bucket_start_ms] = list(rows)
            self._prune_range_bars_by_bucket(current_bucket=bucket_start_ms)
        return list(rows)

    def _range_store_fallback_allowed(self, bucket_start_ms: int) -> bool:
        if bucket_start_ms not in getattr(self, "_range_bars_by_bucket", {}):
            return True
        return (
            getattr(self, "_initial_range_bucket_ms", None)
            == bucket_start_ms
            and getattr(self, "_initial_range_recovery", None) is not None
        )

    async def _emit_range_aggregates(self, aggregates: Sequence[RangeBarAggregate]) -> list[MarketFeatureEvent]:
        events: list[MarketFeatureEvent] = []
        for aggregate in aggregates:
            key = (aggregate.symbol, str(aggregate.range_pct), int(aggregate.bucket_start_ms))
            if key in self._executed_range_aggregate_buckets:
                logger.warning(
                    "Duplicate range aggregate skipped | symbol=%s range_pct=%s bucket_start_ms=%s",
                    aggregate.symbol,
                    aggregate.range_pct,
                    aggregate.bucket_start_ms,
                )
                continue
            self._executed_range_aggregate_buckets.add(key)
            coverage = self._range_coverage_for_bucket(
                aggregate.bucket_start_ms
            )
            event = range_aggregate_feature(
                aggregate,
                exchange=self.app_config.data_exchange,
                timeframe=self._range_aggregate_interval,
                coverage_status=coverage.coverage_status,
                missing_gap_ms=coverage.missing_gap_ms,
                range_recovered_from_checkpoint=coverage.recovered_from_checkpoint,
                range_checkpoint_age_ms=coverage.checkpoint_age_ms,
            )
            self.stats.range_aggregates_created += 1
            await self.process_market_feature(event)
            events.append(event)
            self._persist_completed_range_aggregate(
                aggregate,
                coverage_status=coverage.coverage_status,
                missing_gap_ms=coverage.missing_gap_ms,
            )
        return events

    async def emit_range_aggregate_for_bucket(self, bucket_start_ms: int) -> list[MarketFeatureEvent]:
        return await self._emit_range_aggregates(self._load_range_aggregates_for_bucket(bucket_start_ms))

    async def _startup(self) -> None:
        logger.info("Live runtime startup phase started")
        self._initialize_rangebar_trust_window()
        self._set_health(RuntimePhase.WARMING_UP, healthy=True)
        await self._bootstrap_account_config_if_enabled()
        await self._check_strategy_position_mode_requirements()
        await self._run_warmup()
        loaded_range_speed_history = await self._warmup_range_speed_history()
        if (
            self._range_speed_min_periods > 0
            and loaded_range_speed_history < self._range_speed_min_periods
        ):
            logger.warning(
                "V10A range-speed history insufficient; live runtime continues | complete_history=%s min_periods=%s missing=%s",
                loaded_range_speed_history,
                self._range_speed_min_periods,
                self._range_speed_min_periods - loaded_range_speed_history,
            )
        await self._check_startup_feature_backfills()
        self._set_health(RuntimePhase.CATCHING_UP, healthy=True, warmup_complete=True)
        snapshots = await self._run_recovery()
        # ── Post-recovery: re-check account config if entries were blocked ──
        if self._account_config_new_entries_blocked:
            await self._recheck_account_config_after_recovery()
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
        await self._finish_range_speed_warmup_after_catchup()
        # ── Start heartbeat service ──
        self._heartbeat_service.start(
            runtime_id=f"{self.app_config.strategy}::{self.app_config.symbol}",
        )
        self._start_range_speed_background_services()
        self._set_health(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)
        logger.info("Live runtime startup phase completed")

    async def _check_startup_feature_backfills(self) -> None:
        providers = self._get_startup_feature_backfill_providers()
        if not providers:
            return

        results: dict[str, Mapping[str, Any]] = {}
        for provider in providers:
            name = str(provider.name)
            try:
                result = await self._invoke_provider_method(
                    provider,
                    "check_and_launch",
                )
            except Exception as exc:
                logger.warning(
                    "Startup feature backfill provider failed | "
                    "provider=%s error=%s",
                    name,
                    exc,
                )
                result = await self._provider_failure_result(
                    provider,
                    exc,
                )
            results[name] = dict(result)
            await self._publish_feature_backfill_events(
                provider,
                result,
            )
            logger.info(
                "Startup feature backfill audit | "
                "provider=%s result=%s",
                name,
                result,
            )

        self._set_health(
            self._health.phase,
            metadata={
                **dict(self._health.metadata),
                "feature_backfill_results": results,
            },
        )

    def _get_startup_feature_backfill_providers(
        self,
    ) -> tuple[object, ...]:
        if self._feature_backfill_providers_resolved:
            return tuple(
                self._startup_feature_backfill_providers or ()
            )
        if self._startup_feature_backfill_providers is None:
            self._startup_feature_backfill_providers = (
                resolve_startup_feature_backfill_providers(
                    self.context.strategy
                )
            )
        else:
            self._startup_feature_backfill_providers = tuple(
                self._startup_feature_backfill_providers
            )
        self._feature_backfill_providers_resolved = True
        return tuple(self._startup_feature_backfill_providers)

    async def _invoke_provider_method(
        self,
        provider: object,
        method_name: str,
        *args: object,
    ) -> Any:
        method = getattr(provider, method_name)
        if inspect.iscoroutinefunction(method):
            return await method(*args)
        result = await asyncio.to_thread(method, *args)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _provider_failure_result(
        self,
        provider: object,
        exc: BaseException,
    ) -> Mapping[str, Any]:
        mapper = getattr(provider, "failure_result", None)
        if callable(mapper):
            mapped = await self._invoke_provider_method(
                provider,
                "failure_result",
                exc,
            )
            if isinstance(mapped, Mapping):
                return dict(mapped)
        return {
            "action": "none",
            "reason": "provider_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }

    async def _publish_feature_backfill_events(
        self,
        provider: object,
        result: Mapping[str, Any],
    ) -> None:
        events = await self._invoke_provider_method(
            provider,
            "market_feature_events",
            result,
        )
        for event in tuple(events or ()):
            if not isinstance(event, MarketFeatureEvent):
                raise TypeError(
                    "feature backfill provider returned a non-market "
                    f"event: {type(event).__name__}"
                )
            await self.process_market_feature(event)

    async def _check_strategy_position_mode_requirements(
        self,
    ) -> None:
        try:
            requirements = resolve_position_mode_requirements(
                self.context.strategy
            )
        except Exception as exc:
            raise LiveRuntimeError(
                "strategy position mode requirement failed | "
                f"invalid_requirement={type(exc).__name__}: {exc}"
            ) from exc
        if not requirements:
            return

        strategy_id = str(
            getattr(
                getattr(self.context.strategy, "config", None),
                "strategy_id",
                self.app_config.strategy,
            )
        )
        audit: dict[str, Any] = {
            "strategy": strategy_id,
            "symbol": self.app_config.symbol,
            "ok": True,
            "requirements": [],
            "source": "startup_hard_gate",
        }
        failures: list[str] = []
        for requirement in requirements:
            statuses = await fetch_position_mode_statuses(
                exchanges=requirement.exchanges,
                symbol=self.app_config.symbol,
                account_clients=self._get_account_clients(),
                source="startup_hard_gate",
            )
            requirement_ok = bool(statuses) and all(
                status.mode == requirement.required_mode.value
                for status in statuses
            )
            requirement_audit = {
                "required_mode": requirement.required_mode.value,
                "requirement_source": requirement.source,
                "ok": requirement_ok,
                "exchanges": [
                    status.audit(requirement.required_mode)
                    for status in statuses
                ],
            }
            audit["requirements"].append(requirement_audit)
            audit["ok"] = bool(audit["ok"]) and requirement_ok
            if not statuses:
                failures.append(
                    "position_mode_requirement_has_no_exchanges"
                )

            for status in statuses:
                status_ok = (
                    status.mode == requirement.required_mode.value
                )
                log_args = (
                    strategy_id,
                    status.exchange.value,
                    status.symbol,
                    requirement.required_mode.value,
                    status.mode,
                    status.error,
                )
                if status_ok:
                    logger.info(
                        "Strategy position mode validated | "
                        "strategy=%s exchange=%s symbol=%s "
                        "required_mode=%s actual_mode=%s "
                        "source=startup_hard_gate error=%s",
                        *log_args,
                    )
                    continue
                logger.error(
                    "Strategy position mode validation failed | "
                    "strategy=%s exchange=%s symbol=%s "
                    "required_mode=%s actual_mode=%s "
                    "source=startup_hard_gate error=%s",
                    *log_args,
                )
                failures.append(
                    f"{status.exchange.value}={status.mode}"
                )

        self._set_health(
            self._health.phase,
            metadata={
                **dict(self._health.metadata),
                "position_mode_requirements": audit,
            },
        )

        if failures:
            raise LiveRuntimeError(
                "strategy position mode requirement failed | "
                f"strategy={strategy_id} symbol={self.app_config.symbol} "
                f"issues={failures}"
            )

    async def _bootstrap_account_config_if_enabled(self) -> None:
        if self.runtime_config.mode is not RuntimeMode.LIVE_RUNTIME:
            return

        project_env = self._project_env
        live_trading = project_env.get_bool("AETHER_LIVE_TRADING", False)
        require_leverage = live_trading and not self.app_config.dry_run
        env = load_account_config_env(
            exchanges=self.app_config.exchanges,
            symbol=self.app_config.symbol,
            environ=project_env.values,
            require_leverage=require_leverage,
        )
        self._account_config_env = env
        if env.missing_leverage:
            logger.warning(
                "Account config leverage env missing; skipping exchanges | exchanges=%s dry_run=%s live_trading=%s",
                ",".join(exchange.value for exchange in env.missing_leverage),
                self.app_config.dry_run,
                live_trading,
            )
        if not env.targets:
            return

        apply_writes = (not self.app_config.dry_run) and (
            live_trading or _all_exchange_sandbox(self.app_config.exchanges, project_env)
        )
        self._account_config_apply_writes = apply_writes
        results = await bootstrap_account_config(
            targets=env.targets,
            account_clients=self._get_account_clients(),
            execution_clients=self._get_execution_clients(),
            apply=apply_writes,
            dry_run=self.app_config.dry_run,
            fail_on_error=require_leverage,
        )
        # Store results for downstream inspection
        self._account_config_results = tuple(results)

        for result in results:
            log = logger.info if result.ok else logger.warning
            log(
                "Account config bootstrap result | exchange=%s symbol=%s applied=%s verified=%s reason=%s error=%s",
                result.exchange.value,
                result.symbol,
                result.applied,
                result.verified,
                result.reason,
                result.error,
            )
        if require_leverage:
            raise_on_failed_account_config(results)

        # Check if any exchange has existing exposure that blocked config verification.
        # These are non-fatal — runtime starts but new entries are blocked.
        _EXPOSURE_BLOCKED = {"existing_exposure_config_unverified", "existing_exposure_config_mismatch"}
        exposure_blocked = [r for r in results if r.reason in _EXPOSURE_BLOCKED]
        if exposure_blocked:
            self._account_config_new_entries_blocked = True
            for blocked in exposure_blocked:
                severity = "critical" if blocked.reason == "existing_exposure_config_mismatch" else "warning"
                logger.log(
                    logging.CRITICAL if severity == "critical" else logging.WARNING,
                    "Account config: existing exposure detected — new entries blocked | "
                    "exchange=%s symbol=%s reason=%s positions=%s orders=%s stop_orders=%s",
                    blocked.exchange.value,
                    blocked.symbol,
                    blocked.reason,
                    len(blocked.active_positions),
                    len(blocked.open_orders),
                    len(blocked.open_stop_orders),
                )
                self.context.alerts.emit(
                    AppAlert(
                        subject=f"AetherEdge account config: {blocked.reason}",
                        severity=severity,
                        content=(
                            f"exchange={blocked.exchange.value}\n"
                            f"symbol={blocked.symbol}\n"
                            f"reason={blocked.reason}\n"
                            f"positions={len(blocked.active_positions)}\n"
                            f"open_orders={len(blocked.open_orders)}\n"
                            f"stop_orders={len(blocked.open_stop_orders)}\n"
                            f"new_entries_blocked=True\n"
                        ),
                    )
                )

    async def _recheck_account_config_after_recovery(self) -> None:
        """After recovery, if all positions are now flat, re-run account config
        bootstrap to try to clear the entry block."""
        env = self._account_config_env
        if env is None or not env.targets:
            return

        # Check if any exchange still has positions
        account_clients = self._get_account_clients()
        execution_clients = self._get_execution_clients()

        still_has_exposure = False
        for target in env.targets:
            account = next((a for a in account_clients if a.exchange == target.exchange), None)
            execution = next((e for e in execution_clients if e.exchange == target.exchange), None)
            if account is None or execution is None:
                continue
            try:
                positions = await account.fetch_positions()
                open_orders = await execution.fetch_open_orders()
                open_stop_orders = await execution.fetch_open_stop_orders()
                if positions or open_orders or open_stop_orders:
                    still_has_exposure = True
                    break
            except Exception:
                still_has_exposure = True
                break

        if still_has_exposure:
            logger.info(
                "Post-recovery account config re-check: exposure still exists, entries remain blocked"
            )
            return

        # All flat — re-run bootstrap
        logger.info("Post-recovery account config re-check: all flat, re-running bootstrap")
        try:
            results = await bootstrap_account_config(
                targets=env.targets,
                account_clients=account_clients,
                execution_clients=execution_clients,
                apply=self._account_config_apply_writes,
                dry_run=self.app_config.dry_run,
                fail_on_error=False,
            )
            all_verified = all(r.verified for r in results)
            if all_verified:
                self._account_config_new_entries_blocked = False
                logger.info("Post-recovery account config verified — new entries re-enabled")
                self.context.alerts.emit(
                    AppAlert(
                        subject="AetherEdge account config verified after recovery",
                        severity="info",
                        content="All exchanges verified after positions closed. New entries re-enabled.",
                    )
                )
            else:
                logger.warning(
                    "Post-recovery account config re-check: not all verified | results=%s",
                    [r.detail() for r in results],
                )
        except Exception as exc:
            logger.warning("Post-recovery account config re-check failed: %s", exc)
            self.context.alerts.emit(
                AppAlert(
                    subject="AetherEdge post-recovery account config re-check failed",
                    severity="warning",
                    content=(
                        f"error={exc}\n"
                        f"new_entries_blocked=True (entries remain blocked until next restart)\n"
                    ),
                )
            )

    def _initialize_rangebar_trust_window(self) -> None:
        if not self.requirements.range_bars.enabled or not self.requirements.trades.enabled:
            self._rangebar_trust_start_bucket_ms = None
            return
        now_ms = int(time.time() * 1000)
        current_bucket = (now_ms // self._closed_bar_interval_ms) * self._closed_bar_interval_ms
        self._initial_range_bucket_ms = current_bucket
        store = self._get_range_checkpoint_store()
        recovery = store.recover_current_bucket(
            exchange=self.app_config.data_exchange.value,
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            bucket_start_ms=current_bucket,
            now_ms=now_ms,
            max_age_for_recovered_minor_ms=self.runtime_config.range_checkpoint_max_age_for_recovered_minor_ms,
            max_age_for_restore_ms=self.runtime_config.range_checkpoint_max_age_for_restore_ms,
        )
        rows = self._get_range_bar_store().load(
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            time_range=TimeRange(
                current_bucket,
                current_bucket + self._closed_bar_interval_ms - 1,
            ),
        )
        self._range_bars_by_bucket[current_bucket] = list(rows)
        if recovery.checkpoint is not None:
            try:
                self._range_bar_builder = RangeBarBuilder.restore_state(
                    recovery.checkpoint.builder_state
                )
            except (KeyError, TypeError, ValueError) as exc:
                logger.warning(
                    "Range builder checkpoint restore failed; current bucket disabled | symbol=%s bucket_start_ms=%s error=%s",
                    self.app_config.symbol,
                    current_bucket,
                    exc,
                )
                recovery = RangeCheckpointRecovery(
                    coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
                    checkpoint=None,
                    checkpoint_age_ms=recovery.checkpoint_age_ms,
                    missing_gap_ms=recovery.missing_gap_ms,
                    recovered_from_checkpoint=False,
                )
        self._initial_range_recovery = recovery
        self._range_builder_reset_at_bucket_ms = (
            current_bucket + self._closed_bar_interval_ms
        )
        if recovery.coverage_status == RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value:
            self._rangebar_trust_start_bucket_ms = current_bucket
        else:
            self._rangebar_trust_start_bucket_ms = current_bucket + self._closed_bar_interval_ms
        configure_coverage = getattr(
            self.context.strategy, "configure_range_coverage", None
        )
        if callable(configure_coverage):
            configure_coverage(
                degraded_fast_margin=self.runtime_config.degraded_fast_margin
            )
        self._get_range_checkpoint_writer().start()
        self._launch_range_micro_repair_subprocess(recovery)
        logger.info(
            "Rangebar checkpoint recovery initialized | symbol=%s interval=%s now_ms=%s current_bucket_ms=%s trust_start_bucket_ms=%s coverage_status=%s checkpoint_age_ms=%s recovered=%s missing_gap_ms=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            now_ms,
            current_bucket,
            self._rangebar_trust_start_bucket_ms,
            recovery.coverage_status,
            recovery.checkpoint_age_ms,
            recovery.recovered_from_checkpoint,
            recovery.missing_gap_ms,
        )

    def _launch_range_micro_repair_subprocess(
        self,
        recovery: RangeCheckpointRecovery,
    ) -> None:
        result = self._get_range_repair_bootstrap_service().start_if_needed(
            recovery,
            initial_bucket_ms=self._initial_range_bucket_ms,
        )
        self._range_repair_journal_store = result.journal_store
        self._range_repair_journal_writer = result.journal_writer
        self._range_micro_repair_supervisor = (
            result.micro_repair_supervisor
        )
        if result.journal_bucket_start_ms is not None:
            self._range_repair_journal_bucket_ms = (
                result.journal_bucket_start_ms
            )
            self._range_repair_checkpoint_last_trade_ts_ms = (
                result.checkpoint_last_trade_ts_ms
            )
            self._range_repair_first_live_submitted = False
            self._range_repair_journal_finalize_submitted = False
            self._range_repair_journal_append_failure_warned = False

    async def _warmup_range_speed_history(self) -> int:
        if not self.requirements.range_bars.enabled:
            return 0
        warmup = getattr(
            self.context.strategy, "warmup_range_speed_history", None
        )
        if not callable(warmup):
            return 0
        now_ms = int(time.time() * 1000)
        current_bucket = (
            now_ms // self._closed_bar_interval_ms
        ) * self._closed_bar_interval_ms
        catchup_config = self.runtime_config.startup_catchup
        within_catchup_window = (
            catchup_config.enabled
            and now_ms - current_bucket
            <= catchup_config.fresh_open_window_seconds * 1000
        )
        self._range_speed_warmup_excluded_previous = within_catchup_window
        before_bucket_end_ms = (
            current_bucket - 1
            if within_catchup_window
            else current_bucket + self._closed_bar_interval_ms - 1
        )
        limit = int(
            getattr(
                getattr(self.context.strategy, "config", None),
                "entry_filters",
                None,
            ).range_speed_rolling_window_bars
        )
        rows = await asyncio.to_thread(
            self._get_range_checkpoint_store().load_complete_history,
            exchange=self.app_config.data_exchange.value,
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            before_bucket_end_ms=before_bucket_end_ms,
            limit=limit,
        )
        loaded = warmup([row.rf_bar_count for row in rows])
        min_periods = int(
            self.context.strategy.config.entry_filters.range_speed_min_periods
        )
        self._range_speed_complete_history = int(
            getattr(
                getattr(self.context.strategy, "range_speed_tracker", None),
                "complete_history_count",
                loaded,
            )
        )
        self._range_speed_min_periods = min_periods
        log = logger.info if loaded >= min_periods else logger.warning
        log(
            "V10A range-speed history warmup | complete_history=%s min_periods=%s available=%s",
            loaded,
            min_periods,
            loaded >= min_periods,
        )
        return int(loaded)

    async def _finish_range_speed_warmup_after_catchup(self) -> None:
        if (
            not self._range_speed_warmup_excluded_previous
            or self._startup_catchup_range_observed
        ):
            return
        warmup = getattr(
            self.context.strategy, "warmup_range_speed_history", None
        )
        if not callable(warmup):
            return
        now_ms = int(time.time() * 1000)
        current_bucket = (
            now_ms // self._closed_bar_interval_ms
        ) * self._closed_bar_interval_ms
        previous_end = current_bucket - 1
        rows = await asyncio.to_thread(
            self._get_range_checkpoint_store().load_complete_history,
            exchange=self.app_config.data_exchange.value,
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            before_bucket_end_ms=current_bucket,
            limit=1,
        )
        if rows and rows[-1].bucket_end_ms == previous_end:
            count = warmup([rows[-1].rf_bar_count])
            self._range_speed_complete_history += int(count)

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
        if not resolve_market_feature_observers(self.context.strategy):
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
        # ── Check strategy recovery blocking state ────────────────────────
        strategy = self.context.strategy
        if getattr(strategy, "recovery_blocking_manual_required", False):
            alerts = getattr(strategy, "recovery_alerts", [])
            raise LiveRuntimeError(
                f"runtime recovery blocking manual required: "
                f"alerts={alerts}"
            )
        # ── Pre-execution postcondition: must have coverage plan ───────────
        self._validate_recovery_protection_postcondition(report)

        # ── Separate stop signals from other recovery signals ──────────────
        _STOP_ACTIONS = {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}
        stop_signals = [s for s in report.strategy_signals if s.action in _STOP_ACTIONS]
        other_signals = [s for s in report.strategy_signals if s.action not in _STOP_ACTIONS]

        if stop_signals:
            # ── Execute stop signals with failure detection ────────────────
            failed_before = self.stats.failed_intents
            partial_before = self.stats.partial_failures
            await self._execute_signals(
                stop_signals,
                source="recovery",
                event_time_ms=None,
                metadata={"feature_type": "recovery"},
            )
            if self.stats.failed_intents > failed_before:
                raise LiveRuntimeError(
                    "recovery stop placement failed: all target exchanges rejected the stop order"
                )
            if self.stats.partial_failures > partial_before:
                raise LiveRuntimeError(
                    "recovery stop placement partially failed: some target exchanges rejected the stop order"
                )
            # ── Post-execution validation: stop must now exist on exchange ──
            await self._validate_post_execution_stop_protection()

        # ── Execute remaining recovery signals (non-stop) ─────────────────
        if other_signals:
            await self._execute_signals(
                other_signals,
                source="recovery",
                event_time_ms=None,
                metadata={"feature_type": "recovery"},
            )

        # ── All checks passed — safe to log completion ────────────────────
        logger.info(
            "Runtime recovery completed | snapshots=%s strategy_signals=%s issues=%s",
            len(report.snapshots),
            len(report.strategy_signals),
            len(report.issues),
        )
        if report.snapshots:
            self._last_snapshots = tuple(report.snapshots)
            self._last_snapshot = report.snapshots[0]  # backward-compat for on_start / legacy consumers
        if not self._last_snapshots:
            raise LiveRuntimeError("recovery completed without a startup snapshot")
        return self._last_snapshots

    def _validate_recovery_protection_postcondition(self, report: RecoveryReport) -> None:
        """Verify every active exchange position has protective stop coverage.

        After strategy recovery, for every active position on the master exchange
        (or any open leg), one of the following MUST be true:

        1. A bot-owned valid protective stop already exists on the exchange.
        2. The recovery generated a PLACE_STOP_LOSS signal.
        3. The strategy marked recovery_blocking_manual_required (checked earlier).

        If none of these hold, the runtime MUST NOT proceed — an active position
        without a protective stop is an unacceptable risk.
        """
        strategy = self.context.strategy
        # Already fatal via blocking check — skip postcondition.
        if getattr(strategy, "recovery_blocking_manual_required", False):
            return

        position_index = self._strategy_position_index()
        active_strategy_positions = position_index.active
        if not active_strategy_positions:
            return

        converter = NativeQuantityConverter()
        validator = RecoveryExitOrderValidator(quantity_converter=converter)

        # Collect PLACE_STOP_LOSS signals from recovery for fast lookup.
        place_stop_scopes: dict[str, set[str | None]] = {}
        for signal in report.strategy_signals:
            if signal.action not in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
                continue
            signal_position_id = _signal_position_id(signal)
            if signal.metadata:
                targets = signal.metadata.get("target_exchanges", [])
                if isinstance(targets, (list, tuple)):
                    for t in targets:
                        exchange_scope = str(t).strip().lower()
                        place_stop_scopes.setdefault(exchange_scope, set()).add(signal_position_id)

        master_exchange_str = self.app_config.data_exchange.value

        for strategy_position in active_strategy_positions:
            if not _strategy_position_requires_protective_stop(
                strategy_position
            ):
                continue
            market_profile = get_market_profile(strategy_position.symbol)
            relevant_exchanges = {
                master_exchange_str,
                *_strategy_position_active_exchanges(strategy_position),
            }
            for snapshot in report.snapshots:
                exchange_name = snapshot.balance.exchange
                exchange_str = (
                    exchange_name.value
                    if hasattr(exchange_name, "value")
                    else str(exchange_name)
                )
                if exchange_str not in relevant_exchanges:
                    continue

                matching_positions = _exchange_positions_matching_strategy_position(
                    getattr(snapshot, "positions", ()) or (),
                    strategy_position,
                )
                if len(matching_positions) > 1:
                    _raise_ambiguous_exchange_positions(
                        context="recovery protection postcondition failed",
                        strategy_position=strategy_position,
                        exchange=exchange_str,
                        ambiguous_count=len(matching_positions),
                    )
                if not matching_positions:
                    continue

                active_pos = matching_positions[0]
                canonical_stop_price = strategy_position.stop_price
                expected_native_quantity = _strategy_position_native_quantity(
                    strategy_position=strategy_position,
                    active_pos=active_pos,
                    exchange=exchange_name,
                    market_profile=market_profile,
                    converter=converter,
                    logical_position_count=len(active_strategy_positions),
                )
                position_side = _position_side_for_strategy_position(
                    strategy_position,
                    active_pos,
                )

                if canonical_stop_price is not None and position_side is not None:
                    try:
                        open_stop_orders = (
                            getattr(snapshot, "open_stop_orders", ()) or ()
                        )
                        if len(active_strategy_positions) > 1:
                            open_stop_orders = filter_orders_for_position_scope(
                                open_stop_orders,
                                position_id=strategy_position.position_id,
                                known_order_ids=_strategy_position_stop_order_ids(
                                    strategy_position
                                ),
                            )
                        validation = validator.validate_stop_orders(
                            exchange=exchange_name,
                            symbol=strategy_position.symbol,
                            strategy_id=strategy_position.strategy_id,
                            position_id=strategy_position.position_id,
                            position_side=position_side,
                            position_mode=snapshot.position_mode,
                            current_position_native_quantity=expected_native_quantity,
                            canonical_stop_price=canonical_stop_price,
                            open_stop_orders=open_stop_orders,
                            open_orders=getattr(snapshot, "open_orders", ()) or (),
                            market_profile=market_profile,
                            instrument_rule=snapshot.instrument_rule,
                        )
                        if validation.should_keep_existing_stop:
                            continue
                    except Exception:
                        pass

                if _place_stop_scope_covers(
                    place_stop_scopes,
                    exchange=exchange_str,
                    position_id=strategy_position.position_id,
                    logical_position_count=len(active_strategy_positions),
                ):
                    continue

                open_stop_orders = getattr(snapshot, "open_stop_orders", ()) or ()
                raise LiveRuntimeError(
                    "recovery protection postcondition failed: "
                    "active position without bot-owned valid stop or recovery stop signal | "
                    f"strategy_position_id={strategy_position.position_id} "
                    f"exchange={exchange_str} "
                    f"symbol={strategy_position.symbol} "
                    f"position_side={strategy_position.side.value} "
                    f"position_qty={active_pos.quantity} "
                    f"open_stop_orders={len(open_stop_orders)} "
                    f"bot_owned_valid_stop=false "
                    f"place_stop_signal=false "
                    f"canonical_stop_price={canonical_stop_price} "
                    f"strategy_recovery_blocking_manual_required=false"
                )

    async def _validate_post_execution_stop_protection(self) -> None:
        """After executing PLACE_STOP_LOSS signals, verify the stop orders
        actually exist on every exchange that holds an active position.

        Fetches fresh exchange state (positions + open stop orders) and
        re-runs ``RecoveryExitOrderValidator``.  Every protected exchange
        MUST satisfy ``should_keep_existing_stop`` — a valid, bot-owned
        protective stop must be live on the exchange right now.
        """
        active_strategy_positions = self._strategy_position_index().active
        if not active_strategy_positions:
            return

        converter = NativeQuantityConverter()
        validator = RecoveryExitOrderValidator(quantity_converter=converter)

        execution_clients = self._get_execution_clients()
        account_clients = self._get_account_clients()
        exec_by_exchange = {c.exchange: c for c in execution_clients}
        acct_by_exchange = {c.exchange: c for c in account_clients}

        master_exchange_str = self.app_config.data_exchange.value
        exchange_state: dict[
            ExchangeName,
            tuple[
                Sequence[Position],
                Sequence[Order],
                PositionMode,
                InstrumentRule | None,
            ],
        ] = {}

        for strategy_position in active_strategy_positions:
            if not _strategy_position_requires_protective_stop(
                strategy_position
            ):
                continue
            canonical_stop_price = strategy_position.stop_price
            if canonical_stop_price is None:
                raise LiveRuntimeError(
                    "post-execution stop validation failed: no canonical stop price available | "
                    f"strategy_position_id={strategy_position.position_id}"
                )

            market_profile = get_market_profile(strategy_position.symbol)
            relevant_exchanges = {
                master_exchange_str,
                *_strategy_position_active_exchanges(strategy_position),
            }
            for exchange in self.app_config.exchanges:
                exchange_str = exchange.value
                if exchange_str not in relevant_exchanges:
                    continue

                exec_client = exec_by_exchange.get(exchange)
                acct_client = acct_by_exchange.get(exchange)
                if exec_client is None or acct_client is None:
                    continue

                if exchange not in exchange_state:
                    try:
                        positions = tuple(await acct_client.fetch_positions() or ())
                        open_stop_orders = tuple(
                            await exec_client.fetch_open_stop_orders() or ()
                        )
                        instrument_rule = (
                            await _fetch_execution_instrument_rule(exec_client)
                        )
                    except Exception as exc:
                        raise LiveRuntimeError(
                            "post-execution stop validation failed: cannot fetch exchange state | "
                            f"exchange={exchange_str} error={exc}"
                        ) from exc
                    try:
                        mode = await acct_client.fetch_position_mode()
                    except Exception:
                        mode = PositionMode.ONE_WAY
                    exchange_state[exchange] = (
                        positions,
                        open_stop_orders,
                        mode,
                        instrument_rule,
                    )

                (
                    positions,
                    open_stop_orders,
                    mode,
                    instrument_rule,
                ) = exchange_state[exchange]
                matching_positions = _exchange_positions_matching_strategy_position(
                    positions,
                    strategy_position,
                )
                if len(matching_positions) > 1:
                    _raise_ambiguous_exchange_positions(
                        context="post-execution stop validation failed",
                        strategy_position=strategy_position,
                        exchange=exchange_str,
                        ambiguous_count=len(matching_positions),
                    )
                if not matching_positions:
                    continue

                active_pos = matching_positions[0]
                position_side = _position_side_for_strategy_position(
                    strategy_position,
                    active_pos,
                )
                if position_side is None:
                    raise LiveRuntimeError(
                        "post-execution stop validation failed: unresolved position side | "
                        f"strategy_position_id={strategy_position.position_id} "
                        f"symbol={strategy_position.symbol} exchange={exchange_str}"
                    )
                expected_native_quantity = _strategy_position_native_quantity(
                    strategy_position=strategy_position,
                    active_pos=active_pos,
                    exchange=exchange,
                    market_profile=market_profile,
                    converter=converter,
                    logical_position_count=len(active_strategy_positions),
                )

                validation = validator.validate_stop_orders(
                    exchange=exchange,
                    symbol=strategy_position.symbol,
                    strategy_id=strategy_position.strategy_id,
                    position_id=strategy_position.position_id,
                    position_side=position_side,
                    position_mode=mode,
                    current_position_native_quantity=expected_native_quantity,
                    canonical_stop_price=canonical_stop_price,
                    open_stop_orders=(
                        filter_orders_for_position_scope(
                            open_stop_orders,
                            position_id=strategy_position.position_id,
                            known_order_ids=_strategy_position_stop_order_ids(
                                strategy_position
                            ),
                        )
                        if len(active_strategy_positions) > 1
                        else open_stop_orders
                    ),
                    open_orders=(),
                    market_profile=market_profile,
                    instrument_rule=instrument_rule,
                )

                if not validation.should_keep_existing_stop:
                    raise LiveRuntimeError(
                        "post-execution stop validation failed: "
                        "active position still without bot-owned valid stop "
                        "after recovery stop placement | "
                        f"strategy_position_id={strategy_position.position_id} "
                        f"exchange={exchange_str} "
                        f"symbol={strategy_position.symbol} "
                        f"position_side={strategy_position.side.value} "
                        f"position_qty={active_pos.quantity} "
                        f"canonical_stop_price={canonical_stop_price} "
                        f"valid_bot_stops={len(validation.valid_bot_owned_orders)} "
                        f"invalid_bot_stops={len(validation.invalid_bot_owned_orders)} "
                        f"unknown_stops={len(validation.unknown_exit_orders)} "
                        f"primary_reason={validation.primary_invalid_reason} "
                        f"detail_reason={validation.primary_invalid_detail_reason} "
                        f"effective_expected_stop_price={validation.effective_expected_stop_price} "
                        f"price_tick={validation.price_tick}"
                    )

                logger.info(
                    "Post-execution stop protection validated | "
                    "position_id=%s exchange=%s valid_bot_stops=%s",
                    strategy_position.position_id,
                    exchange_str,
                    len(validation.valid_bot_owned_orders),
                )

    async def _call_on_start(self, snapshot: PlatformSnapshot) -> None:
        signals = await self._strategy_host.on_start(snapshot)
        self.stats.on_start_called = True
        logger.info("Strategy on_start completed | signals=%s", len(signals or ()))
        await self._execute_signals(signals or (), source="on_start", event_time_ms=None)

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
        2. Strategy exposes one or more active logical position snapshots
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

        # 2 & 3. Strategy-internal logical positions / pending entry
        strategy = self.context.strategy
        if self._strategy_position_index().active:
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
        signals: list[TradeSignal] = []
        for event in events:
            signals.extend(
                await dispatch_market_feature_event(self.context.strategy, event)
            )
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
        rows = self._range_bar_rows_for_bucket(bucket_start_ms)
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
                coverage_status=self._range_coverage_for_bucket(
                    aggregate.bucket_start_ms
                ).coverage_status,
            )
            coverage = self._range_coverage_for_bucket(
                aggregate.bucket_start_ms
            )
            self._persist_completed_range_aggregate(
                aggregate,
                coverage_status=coverage.coverage_status,
                missing_gap_ms=coverage.missing_gap_ms,
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
        self._startup_catchup_range_observed = bool(range_events)
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
            event_time_ms=candidate_open,
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
                        on_transient_failure=(
                            self._on_market_producer_transient_failure
                        ),
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

    def _on_market_producer_transient_failure(
        self, name: str, exc: BaseException
    ) -> None:
        if name != "trades":
            return
        event_ms = int(time.time() * 1000)
        bucket_start_ms = (
            event_ms // self._closed_bar_interval_ms
        ) * self._closed_bar_interval_ms
        self._mark_range_context_degraded_bucket(
            bucket_start_ms=bucket_start_ms,
            reason="producer_failed",
            event_time_ms=event_ms,
        )
        logger.warning(
            "Range repair journal invalidated by transient trade stream "
            "failure | bucket_start_ms=%s error=%s",
            bucket_start_ms,
            exc,
        )

    def _start_sync_tasks(self) -> list[asyncio.Task]:
        tasks: list[asyncio.Task] = []
        if self.requirements.account_state.poll_enabled:
            tasks.append(asyncio.create_task(self._get_account_sync_service().run_periodic(self._stop_event)))
        if self.requirements.order_state.poll_when_position_enabled:
            tasks.append(asyncio.create_task(self._get_order_sync_service().run_periodic(self._stop_event)))
            tasks.append(asyncio.create_task(self._periodic_follower_close_check(self._stop_event)))
        # Heartbeat periodic task
        tasks.append(asyncio.create_task(self._heartbeat_service.run_periodic(self._stop_event)))
        if self._get_startup_feature_backfill_providers():
            tasks.append(
                asyncio.create_task(
                    self._periodic_feature_readiness_refresh(
                        self._stop_event
                    )
                )
            )
        return tasks

    async def _periodic_feature_readiness_refresh(
        self, stop_event: asyncio.Event
    ) -> None:
        providers = self._get_startup_feature_backfill_providers()
        intervals = {
            str(provider.name): max(
                10.0,
                float(provider.poll_interval_seconds),
            )
            for provider in providers
        }
        if not intervals:
            return
        tick_seconds = min(intervals.values())
        last_polled = {
            str(provider.name): 0.0 for provider in providers
        }
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=tick_seconds,
                )
                continue
            except asyncio.TimeoutError:
                pass

            now = time.monotonic()
            for provider in providers:
                name = str(provider.name)
                if (
                    now - last_polled[name]
                    < intervals[name]
                ):
                    continue
                last_polled[name] = now
                try:
                    result = await self._invoke_provider_method(
                        provider,
                        "poll_readiness",
                    )
                except Exception as exc:
                    logger.warning(
                        "Feature readiness provider failed | "
                        "provider=%s error=%s",
                        name,
                        exc,
                    )
                    result = await self._provider_failure_result(
                        provider,
                        exc,
                    )
                await self._publish_feature_backfill_events(
                    provider,
                    result,
                )
                self._record_feature_backfill_result(
                    name,
                    result,
                )

    def _record_feature_backfill_result(
        self,
        name: str,
        result: Mapping[str, Any],
    ) -> None:
        metadata = dict(self._health.metadata)
        results = dict(
            metadata.get("feature_backfill_results", {})
        )
        results[name] = dict(result)
        metadata["feature_backfill_results"] = results
        self._set_health(
            self._health.phase,
            metadata=metadata,
        )

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
                # Read persistent follower_close_generation from plan metadata.
                # This survives restarts and prevents replay of exhausted generations.
                plan_meta = dict(plan.metadata or {})
                current_gen = int(plan_meta.get("follower_close_generation", 0))
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
                            "follower_close_generation": current_gen,
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

    def _has_account_config_entry_block(self) -> bool:
        """Return True when account config verification was blocked by existing
        exposure, preventing new position entries until resolved."""
        return self._account_config_new_entries_blocked

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
        journal_status = {
            "market_queue_dropped_trade": JOURNAL_INVALID_DROPPED_TRADE,
            "market_queue_drain_incomplete_before_closed_bar": (
                JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE
            ),
            "producer_stale": JOURNAL_INVALID_PRODUCER_STALE,
            "producer_failed": JOURNAL_INVALID_PRODUCER_FAILED,
        }.get(reason)
        if journal_status is not None:
            self._invalidate_range_repair_journal(
                bucket_start_ms=bucket_start_ms,
                status=journal_status,
                reason=reason,
                dropped_trades=(
                    1 if reason == "market_queue_dropped_trade" else 0
                ),
            )
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
        signals = await self._strategy_host.on_account_event(event)
        await self._execute_signals(signals or (), source=f"account:{event.exchange.value}", event_time_ms=event.event_time_ms)

    async def _on_account_snapshot_synced(self, snapshot: PlatformSnapshot, sync_type: str) -> None:
        snapshots = [
            existing
            for existing in self._last_snapshots
            if existing.balance.exchange != snapshot.balance.exchange
        ]
        snapshots.append(snapshot)
        self._last_snapshots = tuple(snapshots)
        if snapshot.balance.exchange == self.app_config.data_exchange:
            self._last_snapshot = snapshot

        await self._strategy_host.on_account_snapshot(snapshot)

        exchange = snapshot.balance.exchange.value
        key = (exchange, sync_type)
        state = (snapshot.balance.available, snapshot.balance.total)
        previous_state = self._last_account_snapshot_log_state.get(key)
        now_ms = int(time.monotonic() * 1000)
        self._last_account_snapshot_log_state[key] = state

        if previous_state is None:
            self._last_account_snapshot_log_ms[key] = now_ms
            logger.info(
                "Strategy account snapshot refreshed | exchange=%s sync_type=%s available=%s total=%s reason=first_snapshot",
                exchange,
                sync_type,
                snapshot.balance.available,
                snapshot.balance.total,
            )
            return

        if state != previous_state:
            self._last_account_snapshot_log_ms[key] = now_ms
            logger.info(
                "Strategy account snapshot refreshed | exchange=%s sync_type=%s available=%s total=%s reason=balance_changed previous_available=%s previous_total=%s",
                exchange,
                sync_type,
                snapshot.balance.available,
                snapshot.balance.total,
                previous_state[0],
                previous_state[1],
            )
            return

        keepalive_seconds = self._account_snapshot_log_keepalive_seconds
        last_info_ms = self._last_account_snapshot_log_ms[key]
        if keepalive_seconds > 0 and now_ms - last_info_ms >= keepalive_seconds * 1000:
            self._last_account_snapshot_log_ms[key] = now_ms
            logger.info(
                "Strategy account snapshot refreshed | exchange=%s sync_type=%s available=%s total=%s reason=keepalive_unchanged keepalive_seconds=%g",
                exchange,
                sync_type,
                snapshot.balance.available,
                snapshot.balance.total,
                keepalive_seconds,
            )
            return

        logger.debug(
            "Account snapshot unchanged | exchange=%s sync_type=%s available=%s total=%s",
            exchange,
            sync_type,
            snapshot.balance.available,
            snapshot.balance.total,
        )

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
        await self._dispatch_trade_derived_features(trade)
        if not self.requirements.range_bars.enabled:
            return
        builder = self._get_range_bar_builder()
        trade_time_ms = _event_time_ms(trade)
        if (
            trade_time_ms is not None
            and self._range_builder_reset_at_bucket_ms is not None
            and trade_time_ms >= self._range_builder_reset_at_bucket_ms
        ):
            discard_active = getattr(builder, "discard_active_bar", None)
            if callable(discard_active):
                discard_active()
            else:
                builder = self._get_range_bar_builder()
                if hasattr(builder, "active"):
                    builder.active = None
            logger.info(
                "Discarded restart-spanning active range bar at first clean bucket | bucket_start_ms=%s",
                self._range_builder_reset_at_bucket_ms,
            )
            self._range_builder_reset_at_bucket_ms = None
        closed = builder.on_trade(trade)
        if closed:
            for bar in closed:
                bucket_start = (
                    bar.end_time_ms // self._closed_bar_interval_ms
                ) * self._closed_bar_interval_ms
                self._range_bars_by_bucket.setdefault(
                    bucket_start, []
                ).append(bar)
                self._range_bars_since_checkpoint += 1
                self.stats.range_bars_closed += 1
                self._prune_range_bars_by_bucket(current_bucket=bucket_start)
                await self.process_market_feature(range_bar_closed_feature(bar, exchange=trade.exchange))
                self._persist_range_bar(bar)
        self._submit_range_checkpoint_if_due(trade)
        self._append_range_repair_trade(trade)

    async def _dispatch_trade_derived_features(
        self, trade: MarketTrade
    ) -> None:
        await self._trade_derived_feature_pipeline.process_trade(trade)

    @property
    def _fixed_time_trade_bar_builder(self):
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            return pipeline.fixed_time_trade_bar_builder
        return getattr(self, "_fixed_time_trade_bar_builder_compat", None)

    @_fixed_time_trade_bar_builder.setter
    def _fixed_time_trade_bar_builder(self, value) -> None:
        self._fixed_time_trade_bar_builder_compat = value
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            pipeline.fixed_time_trade_bar_builder = value

    @property
    def _trade_footprint_builder(self):
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            return pipeline.trade_footprint_builder
        return getattr(self, "_trade_footprint_builder_compat", None)

    @_trade_footprint_builder.setter
    def _trade_footprint_builder(self, value) -> None:
        self._trade_footprint_builder_compat = value
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            pipeline.trade_footprint_builder = value

    @property
    def _range_footprint_builder(self):
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            return pipeline.range_footprint_builder
        return getattr(self, "_range_footprint_builder_compat", None)

    @_range_footprint_builder.setter
    def _range_footprint_builder(self, value) -> None:
        self._range_footprint_builder_compat = value
        pipeline = getattr(self, "_trade_derived_feature_pipeline", None)
        if isinstance(pipeline, TradeDerivedFeaturePipeline):
            pipeline.range_footprint_builder = value

    def _submit_range_checkpoint_if_due(self, trade: MarketTrade) -> bool:
        now_ms = int(time.time() * 1000)
        interval_due = (
            now_ms - self._last_range_checkpoint_submit_ms
            >= self.runtime_config.range_checkpoint_interval_ms
        )
        bars_due = (
            self._range_bars_since_checkpoint
            >= self.runtime_config.range_checkpoint_every_closed_bars
        )
        if not interval_due and not bars_due:
            return False
        snapshot_state = getattr(
            self._get_range_bar_builder(), "snapshot_state", None
        )
        if not callable(snapshot_state):
            if not self._range_checkpoint_snapshot_warned:
                logger.warning(
                    "Range checkpoint disabled: builder has no snapshot_state()"
                )
                self._range_checkpoint_snapshot_warned = True
            return False
        trade_time_ms = _event_time_ms(trade)
        if trade_time_ms is None:
            return False
        bucket_start_ms = (
            trade_time_ms // self._closed_bar_interval_ms
        ) * self._closed_bar_interval_ms
        bucket_end_ms = (
            bucket_start_ms + self._closed_bar_interval_ms - 1
        )
        bars = self._range_bar_rows_for_bucket(bucket_start_ms)
        aggregates = self._get_range_bar_aggregator().aggregate(
            bars, bucket_ms=self._closed_bar_interval_ms
        )
        aggregate = next(
            (
                row
                for row in aggregates
                if row.bucket_start_ms == bucket_start_ms
            ),
            None,
        )
        coverage = self._range_coverage_for_bucket(bucket_start_ms)
        checkpoint = RangeBuilderCheckpoint(
            exchange=trade.exchange.value,
            symbol=trade.symbol,
            range_pct=str(self._range_pct),
            bucket_start_ms=bucket_start_ms,
            bucket_end_ms=bucket_end_ms,
            last_trade_id=trade.trade_id,
            last_trade_ts_ms=trade_time_ms,
            last_ws_recv_ts_ms=now_ms,
            range_bar_count=len(bars),
            aggregate=aggregate_snapshot(aggregate),
            builder_state=dict(snapshot_state()),
            coverage_status=coverage.coverage_status,
            missing_gap_ms=coverage.missing_gap_ms,
            checkpoint_updated_at_ms=now_ms,
        )
        accepted = self._get_range_checkpoint_writer().submit(checkpoint)
        if accepted:
            self._last_range_checkpoint_submit_ms = now_ms
            self._range_bars_since_checkpoint = 0
        self._prune_range_bars_by_bucket(current_bucket=bucket_start_ms)
        return accepted

    def _prune_range_bars_by_bucket(self, *, current_bucket: int) -> None:
        """Drop entries for buckets far behind *current_bucket*.

        Keeps the current bucket plus the N most recent closed-bar buckets
        so range-aggregate generation and checkpoint submission still have
        access to a small recent window. This prevents unbounded memory
        growth in long-running sessions.
        """
        keep = max(1, int(getattr(self, "_range_bars_bucket_prune_count", 3)))
        bucket_keys = sorted(self._range_bars_by_bucket.keys(), reverse=True)

        # Always keep the current bucket and the `keep` most recent earlier
        # buckets.  The prune target is everything older than that.
        if len(bucket_keys) <= keep + 1:
            return

        # current_bucket might not be the latest key yet if it hasn't been
        # populated; treat the most-recent key as the effective "latest".
        latest_key = bucket_keys[0] if bucket_keys else current_bucket
        threshold = latest_key - (keep * self._closed_bar_interval_ms)

        stale = [k for k in bucket_keys if k < threshold]
        # Never remove the current or latest bucket
        stale = [k for k in stale if k < current_bucket]
        for k in stale:
            del self._range_bars_by_bucket[k]

    async def _call_strategy_market_event(self, event: MarketEvent) -> Sequence[TradeSignal]:
        return await self._strategy_host.on_market_event(event)

    def _trade_events_are_range_only(self) -> bool:
        strategy = self.context.strategy
        raw_flag = getattr(strategy, "raw_trade_callbacks_enabled", None)
        if raw_flag is False:
            return True

        cfg = getattr(strategy, "config", None)
        strategy_id = getattr(cfg, "strategy_id", "")
        if str(strategy_id).lower().startswith(("eth_lf_portfolio_v9c", "eth_lf_portfolio_v9e")):
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
            # ── Entry guard: block new OPEN signals while account config
            #     is not verified due to existing exposure. ──
            _EXPOSURE_INCREASING_ACTIONS = {
                SignalAction.OPEN_LONG, SignalAction.OPEN_SHORT,
            }
            if signal.action in _EXPOSURE_INCREASING_ACTIONS:
                if self._has_account_config_entry_block():
                    logger.warning(
                        "Blocking new entry — account config not verified due to existing exposure | action=%s source=%s",
                        signal.action.value,
                        source,
                    )
                    self.context.alerts.emit(
                        AppAlert(
                            subject="AetherEdge entry blocked: account config unverified",
                            severity="warning",
                            content=(
                                f"action={signal.action.value}\n"
                                f"source={source}\n"
                                f"reason=account_config_existing_exposure\n"
                            ),
                        )
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
            if self.requirements.order_state.post_submit_sync_enabled:
                logger.info("Post-submit order sync started | action=%s source=%s", signal.action.value, source)
                await self._get_order_sync_service().sync_once(sync_type="post_submit", priority=True)
            self._record_order_results(results)
            self._save_order_results(signal, results)
            self._check_follower_close_failure(signal, results)
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

    async def _validate_order_results_before_journal(
        self,
        *,
        intent,
        results: Sequence[ExchangeOrderResult],
    ) -> Sequence[ExchangeOrderResult]:
        if intent.signal.action not in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            return results
        return await self._verify_stop_order_results(signal=intent.signal, results=results)

    async def _verify_stop_order_results(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
    ) -> Sequence[ExchangeOrderResult]:
        successful = [result for result in results if result.ok]
        if not successful:
            return results

        position_index = self._strategy_position_index()
        strategy_position = _strategy_position_for_stop_signal(position_index, signal)
        if strategy_position is None:
            if not position_index.active:
                return results
            return [
                self._stop_post_check_failed_result(
                    result,
                    reason="ambiguous_strategy_position_scope",
                    metadata={
                        "post_check": "stop_order_exchange_verification",
                        "active_strategy_positions": len(position_index.active),
                    },
                )
                if result.ok
                else result
                for result in results
            ]

        canonical_stop_price = signal.trigger_price or strategy_position.stop_price
        if canonical_stop_price is None:
            return [
                self._stop_post_check_failed_result(
                    result,
                    reason="missing_canonical_stop_price",
                    metadata={"post_check": "stop_order_exchange_verification"},
                )
                if result.ok
                else result
                for result in results
            ]

        execution_by_exchange = {client.exchange: client for client in self._get_execution_clients()}
        account_by_exchange = {client.exchange: client for client in self._get_account_clients()}
        strategy_id = strategy_position.strategy_id
        position_id = strategy_position.position_id
        market_profile = get_market_profile(strategy_position.symbol)
        converter = NativeQuantityConverter()
        validator = RecoveryExitOrderValidator(quantity_converter=converter)

        verified: list[ExchangeOrderResult] = []
        for result in results:
            if not result.ok:
                verified.append(result)
                continue
            if not result.order_id and not result.client_order_id:
                verified.append(
                    self._stop_post_check_failed_result(
                        result,
                        reason="missing_exchange_stop_order_id",
                        metadata={"post_check": "stop_order_exchange_verification"},
                    )
                )
                continue
            if result.status not in {OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED, OrderStatus.FILLED}:
                verified.append(
                    self._stop_post_check_failed_result(
                        result,
                        reason="stop_order_status_not_live",
                        metadata={
                            "post_check": "stop_order_exchange_verification",
                            "status": None if result.status is None else result.status.value,
                        },
                    )
                )
                continue

            exchange = result.exchange
            exec_client = execution_by_exchange.get(exchange)
            acct_client = account_by_exchange.get(exchange)
            if exec_client is None or acct_client is None:
                verified.append(
                    self._stop_post_check_failed_result(
                        result,
                        reason="missing_exchange_client_for_stop_post_check",
                        metadata={"post_check": "stop_order_exchange_verification"},
                    )
                )
                continue

            try:
                instrument_rule = await _fetch_execution_instrument_rule(
                    exec_client
                )
            except Exception as exc:
                verified.append(
                    self._stop_post_check_failed_result(
                        result,
                        reason="stop_post_check_instrument_rule_fetch_failed",
                        metadata={
                            "post_check": "stop_order_exchange_verification",
                            "exchange": exchange.value,
                            "fetch_error": str(exc),
                        },
                    )
                )
                continue

            # ── Retry loop: exchange state may be briefly stale ──
            attempts = _stop_post_check_attempts_from_env(self._project_env)
            delay = _stop_post_check_delay_from_env(self._project_env)

            for attempt in range(1, attempts + 1):
                try:
                    positions = await acct_client.fetch_positions()
                    open_stop_orders = await exec_client.fetch_open_stop_orders()
                except Exception as exc:
                    if attempt < attempts:
                        logger.warning(
                            "Stop post-check fetch failed; retrying | exchange=%s attempt=%s attempts=%s error=%s",
                            exchange.value,
                            attempt,
                            attempts,
                            exc,
                        )
                        await asyncio.sleep(delay)
                        continue
                    verified.append(
                        self._stop_post_check_failed_result(
                            result,
                            reason="stop_post_check_fetch_failed",
                            metadata={
                                "post_check": "stop_order_exchange_verification",
                                "fetch_error": str(exc),
                                "stop_post_check_attempts": attempt,
                            },
                        )
                    )
                    break

                matching_positions = _exchange_positions_matching_strategy_position(
                    positions or (),
                    strategy_position,
                )
                if len(matching_positions) > 1:
                    verified.append(
                        self._stop_post_check_failed_result(
                            result,
                            reason="ambiguous_exchange_position_scope",
                            metadata={
                                "post_check": "stop_order_exchange_verification",
                                "strategy_position_id": strategy_position.position_id,
                                "symbol": strategy_position.symbol,
                                "side": strategy_position.side.value,
                                "exchange": exchange.value,
                                "ambiguous_count": len(matching_positions),
                            },
                        )
                    )
                    break
                if not matching_positions:
                    if attempt < attempts:
                        logger.warning(
                            "Stop post-check missing exchange position; retrying | "
                            "exchange=%s attempt=%s attempts=%s",
                            exchange.value,
                            attempt,
                            attempts,
                        )
                        await asyncio.sleep(delay)
                        continue
                    verified.append(
                        self._stop_post_check_failed_result(
                            result,
                            reason="stop_post_check_failed:missing_exchange_position",
                            metadata={
                                "post_check": "stop_order_exchange_verification",
                                "stop_post_check_attempts": attempt,
                                "invalid_reason": "missing_exchange_position",
                            },
                        )
                    )
                    break

                active_pos = matching_positions[0]
                position_side = _position_side_for_strategy_position(
                    strategy_position,
                    active_pos,
                )
                native_qty = _strategy_position_native_quantity(
                    strategy_position=strategy_position,
                    active_pos=active_pos,
                    exchange=exchange,
                    market_profile=market_profile,
                    converter=converter,
                    logical_position_count=len(position_index.active),
                    scoped_base_quantity=signal.quantity,
                )

                if position_side is None or native_qty <= 0:
                    verified.append(result)
                    break

                try:
                    position_mode = await acct_client.fetch_position_mode()
                except Exception:
                    position_mode = PositionMode.ONE_WAY

                validation = validator.validate_stop_orders(
                    exchange=exchange,
                    symbol=strategy_position.symbol,
                    strategy_id=strategy_id,
                    position_id=position_id,
                    position_side=position_side,
                    position_mode=position_mode,
                    current_position_native_quantity=native_qty,
                    canonical_stop_price=canonical_stop_price,
                    open_stop_orders=open_stop_orders or (),
                    open_orders=(),
                    market_profile=market_profile,
                    instrument_rule=instrument_rule,
                )
                if validation.should_keep_existing_stop:
                    validation_fields = validation.diagnostic_fields(
                        action="keep_existing_stop"
                    )
                    confirmed_stop_price = validation.confirmed_stop_price
                    exchange_position_metadata = _exchange_position_metadata(
                        active_pos=active_pos,
                        exchange=exchange,
                        symbol=strategy_position.symbol,
                        market_profile=market_profile,
                        converter=converter,
                    )
                    verified.append(
                        ExchangeOrderResult(
                            exchange=result.exchange,
                            ok=result.ok,
                            order_id=result.order_id,
                            client_order_id=result.client_order_id,
                            status=result.status,
                            side=result.side,
                            quantity=result.quantity,
                            filled_quantity=result.filled_quantity,
                            avg_fill_price=result.avg_fill_price,
                            fee=result.fee,
                            fee_asset=result.fee_asset,
                            raw={
                                **dict(result.raw),
                                "stop_post_check_attempts": attempt,
                                **validation_fields,
                                "confirmed_stop_price": (
                                    None
                                    if confirmed_stop_price is None
                                    else str(confirmed_stop_price)
                                ),
                                **exchange_position_metadata,
                            },
                        )
                    )
                    logger.info(
                        "Stop order post-check verified | exchange=%s position_qty=%s canonical_stop_price=%s effective_expected_stop_price=%s actual_exchange_stop_price=%s price_tick=%s open_stop_orders=%s attempts=%s",
                        exchange.value,
                        native_qty,
                        canonical_stop_price,
                        validation.effective_expected_stop_price,
                        confirmed_stop_price,
                        validation.price_tick,
                        len(open_stop_orders or ()),
                        attempt,
                    )
                    break

                if attempt < attempts:
                    reason_hint = validation.primary_invalid_reason or "missing_bot_owned_stop"
                    logger.warning(
                        "Stop post-check not verified yet; retrying | exchange=%s attempt=%s attempts=%s invalid_category=%s invalid_detail_reason=%s",
                        exchange.value,
                        attempt,
                        attempts,
                        reason_hint,
                        validation.primary_invalid_detail_reason,
                    )
                    await asyncio.sleep(delay)
                    continue

                # ── All retry attempts exhausted → fail ──────────────────
                reason = validation.primary_invalid_reason or "missing_bot_owned_stop"
                detail_reason = (
                    validation.primary_invalid_detail_reason or reason
                )
                validation_fields = validation.diagnostic_fields(
                    action="fail_post_check"
                )
                logger.critical(
                    "Stop order post-check failed after %s attempts | exchange=%s position_qty=%s canonical_stop_price=%s effective_expected_stop_price=%s actual_exchange_stop_price=%s price_tick=%s price_difference=%s open_stop_orders=%s invalid_category=%s invalid_detail_reason=%s",
                    attempt,
                    exchange.value,
                    native_qty,
                    canonical_stop_price,
                    validation.effective_expected_stop_price,
                    validation_fields.get("actual_exchange_stop_price"),
                    validation.price_tick,
                    validation_fields.get("price_difference"),
                    len(open_stop_orders or ()),
                    reason,
                    detail_reason,
                )
                self.context.alerts.emit(
                    AppAlert(
                        subject="AetherEdge stop order post-check failed",
                        severity="critical",
                        content=(
                            f"exchange={exchange.value}\n"
                            f"symbol={self.app_config.symbol}\n"
                            f"position_qty={native_qty}\n"
                            f"canonical_stop_price={canonical_stop_price}\n"
                            f"effective_expected_stop_price={validation.effective_expected_stop_price}\n"
                            f"actual_exchange_stop_price={validation_fields.get('actual_exchange_stop_price')}\n"
                            f"price_tick={validation.price_tick}\n"
                            f"price_difference={validation_fields.get('price_difference')}\n"
                            f"open_stop_orders={len(open_stop_orders or ())}\n"
                            f"invalid_category={reason}\n"
                            f"invalid_detail_reason={detail_reason}\n"
                            f"order_id={validation_fields.get('existing_order_id')}\n"
                            f"client_order_id={validation_fields.get('existing_client_order_id')}\n"
                            f"stop_post_check_attempts={attempt}\n"
                        ),
                    )
                )
                verified.append(
                    self._stop_post_check_failed_result(
                        result,
                        reason=f"stop_post_check_failed:{reason}",
                        metadata={
                            "post_check": "stop_order_exchange_verification",
                            "stop_post_check_attempts": attempt,
                            "position_qty": str(native_qty),
                            "desired_stop": str(canonical_stop_price),
                            "open_stop_orders": len(open_stop_orders or ()),
                            "invalid_reason": reason,
                            **validation_fields,
                        },
                    )
                )
        return tuple(verified)

    def _stop_post_check_failed_result(
        self,
        result: ExchangeOrderResult,
        *,
        reason: str,
        metadata: Mapping[str, Any],
    ) -> ExchangeOrderResult:
        return ExchangeOrderResult(
            exchange=result.exchange,
            ok=False,
            order_id=result.order_id,
            client_order_id=result.client_order_id,
            status=result.status,
            side=result.side,
            quantity=result.quantity,
            filled_quantity=result.filled_quantity,
            avg_fill_price=result.avg_fill_price,
            fee=result.fee,
            fee_asset=result.fee_asset,
            error=reason,
            raw={**dict(result.raw), **dict(metadata)},
        )

    def _save_order_results(self, signal: TradeSignal, results: Sequence[ExchangeOrderResult]) -> None:
        save_order = getattr(self.context.state_store, "save_order", None)
        if not callable(save_order):
            return
        is_stop = signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}
        for result in results:
            if not result.ok:
                continue
            if result.raw.get("execution_outcome") == "skipped_non_executable_quantity":
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
        follow_up = await self._strategy_host.on_order_results(
            signal=signal,
            results=results,
            source=source,
            event_time_ms=event_time_ms,
        )
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
        event_ms = int(time.time() * 1000)
        bucket_start_ms = (
            event_ms // self._closed_bar_interval_ms
        ) * self._closed_bar_interval_ms
        if any(item.status.value == "failed" for item in unhealthy):
            self._mark_range_context_degraded_bucket(
                bucket_start_ms=bucket_start_ms,
                reason="producer_failed",
                event_time_ms=event_ms,
            )
        elif any(item.status.value == "stale" for item in unhealthy):
            self._mark_range_context_degraded_bucket(
                bucket_start_ms=bucket_start_ms,
                reason="producer_stale",
                event_time_ms=event_ms,
            )
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
            path = self._project_env.get("AETHER_ORDER_JOURNAL_DB", "data/state/aether_order_journal.sqlite3")
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
                post_result_validator=self._validate_order_results_before_journal,
            )
        return self._order_coordinator

    def _get_recovery_service(self):
        if self._recovery_service == "__default__":
            clients = self._get_execution_clients()
            accounts = self._get_account_clients()
            config_env = self._resolved_account_config_env()
            contexts = []
            for account, execution in zip(accounts, clients, strict=False):
                target = config_env.target_for(account.exchange)
                contexts.append(
                    RecoveryExchangeContext(
                        account=account,
                        execution=execution,
                        state_store=self.context.state_store,
                        leverage_margin_mode=(
                            config_env.margin_mode
                            if target is None
                            else target.margin_mode
                        ),
                    )
                )
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

        # ── Inject legacy stop adoptions from strategy recovery ───────
        legacy_adoptions: list[dict[str, Any]] = getattr(
            self.context.strategy, "_legacy_adoptions", []
        ) or []
        if legacy_adoptions:
            from src.order_management.reconciliation.models import (
                ReconciliationAction,
            )

            now_ms = int(time.time() * 1000)
            for adoption in legacy_adoptions:
                action = ReconciliationAction(
                    action_type="adopt_legacy_stop_reference",
                    target=(
                        f"leg:{adoption['position_id']}:"
                        f"{adoption['exchange']}"
                    ),
                    detail={
                        "position_id": adoption["position_id"],
                        "exchange": adoption["exchange"],
                        "stop_order_id": adoption["stop_order_id"],
                        "stop_client_order_id": adoption[
                            "stop_client_order_id"
                        ],
                        "effective_stop_price": adoption[
                            "effective_stop_price"
                        ],
                        "canonical_theoretical_stop_price": adoption[
                            "canonical_theoretical_stop_price"
                        ],
                        "resolution_status": adoption[
                            "resolution_status"
                        ],
                        "adopted_at_ms": now_ms,
                    },
                )
                # Apply legacy adoption directly to the store before
                # reconciliation runs (so reconciliation sees the
                # corrected state).
                service._apply_actions([action], self.app_config.symbol)
                logger.warning(
                    "Startup recovery: legacy stop adopted | "
                    "position_id=%s exchange=%s stop_order_id=%s "
                    "effective_stop_price=%s",
                    adoption["position_id"],
                    adoption["exchange"],
                    adoption["stop_order_id"],
                    adoption["effective_stop_price"],
                )
            # Clear the list so it is not re-applied on subsequent calls.
            self.context.strategy._legacy_adoptions = []

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
        verdict = (
            report.verdict.value
            if hasattr(report.verdict, "value")
            else str(report.verdict)
        )
        if not report.ok:
            logger.error(
                "Startup reconciliation failed | verdict=%s issues=%s",
                verdict,
                report.issues,
            )
            raise LiveRuntimeError(
                "startup reconciliation failed: "
                f"verdict={verdict} issues={list(report.issues)}"
            )
        if (
            verdict == "pass_with_cleanup"
            or report.stale_plans_closed > 0
            or report.fake_order_refs_found
        ):
            logger.info(
                "Startup reconciliation passed with cleanup | "
                "verdict=%s stale_plans_closed=%s fake_refs=%s",
                verdict,
                report.stale_plans_closed,
                len(report.fake_order_refs_found),
            )
        else:
            logger.info(
                "Startup reconciliation passed | verdict=%s",
                verdict,
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
        config_env = self._resolved_account_config_env()
        contexts: list[SyncExchangeContext] = []
        for exchange in self.app_config.exchanges:
            target = config_env.target_for(exchange)
            contexts.append(
                SyncExchangeContext(
                    account=account_by_exchange[exchange],
                    execution=execution_by_exchange[exchange],
                    state_store=self.context.state_store,
                    leverage_margin_mode=(
                        config_env.margin_mode
                        if target is None
                        else target.margin_mode
                    ),
                    expected_leverage=(
                        None if target is None else target.leverage
                    ),
                )
            )
        return tuple(contexts)

    def _resolved_account_config_env(self) -> AccountConfigEnv:
        if self._account_config_env is None:
            self._account_config_env = load_account_config_env(
                exchanges=self.app_config.exchanges,
                symbol=self.app_config.symbol,
                environ=self._project_env.values,
                require_leverage=False,
            )
        return self._account_config_env

    def _get_account_sync_service(self):
        if self._account_sync_service is None:
            self._account_sync_service = AccountStateSyncService(
                contexts=self._get_sync_contexts(),
                config=self.requirements.account_state,
                alert_sink=self.context.alerts,
                throttle=self._request_sync_throttle,
                snapshot_callback=self._on_account_snapshot_synced,
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
        if self._strategy_position_index().active:
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

    def _strategy_position_index(self) -> StrategyPositionSnapshotIndex:
        return resolve_strategy_position_snapshot_index(
            self.context.strategy,
            legacy_strategy_id=self.app_config.strategy,
            legacy_symbol=self.app_config.symbol,
            legacy_base_quantity=Decimal("0"),
        )

    def _get_live_persistence_writer(self) -> _BackgroundWriteQueue:
        if self._live_persistence_writer is None:
            max_pending = int(
                getattr(
                    getattr(self, "runtime_config", None),
                    "background_queue_maxsize",
                    1000,
                )
            )
            self._live_persistence_writer = _BackgroundWriteQueue(
                name="live-persistence-writer",
                max_pending=max_pending,
            )
            self.services["live_persistence_writer"] = (
                self._live_persistence_writer
            )
        return self._live_persistence_writer

    def _submit_live_persistence_write(
        self,
        *,
        description: str,
        write: Callable[[], None],
        on_error: Callable[[BaseException], None] | None = None,
    ) -> bool:
        try:
            self._persistence_alert_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        writer = self._get_live_persistence_writer()
        accepted = writer.submit(
            _BackgroundWriteItem(
                description=description,
                write=write,
                on_error=on_error,
            )
        )
        if not accepted:
            logger.warning(
                "Live persistence write dropped | description=%s pending=%s dropped=%s",
                description,
                writer.pending_count,
                writer.dropped,
            )
        return accepted

    async def _stop_live_persistence_writer(
        self, *, flush: bool = True
    ) -> None:
        writer = getattr(self, "_live_persistence_writer", None)
        if writer is None:
            return
        stop = getattr(writer, "stop", None)
        if not callable(stop):
            return
        if isinstance(writer, _BackgroundWriteQueue):
            await asyncio.to_thread(stop, flush=flush)
            return
        result = stop(flush=flush)
        if inspect.isawaitable(result):
            await result

    def _emit_alert_threadsafe(self, alert: AppAlert) -> None:
        try:
            loop = getattr(self, "_persistence_alert_loop", None)
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(self.context.alerts.emit, alert)
                return
            self.context.alerts.emit(alert)
        except Exception:
            logger.exception(
                "Failed to emit background persistence alert | subject=%s",
                alert.subject,
            )

    def _maybe_log_live_data_path_stats(self) -> None:
        now_ms = int(time.time() * 1000)
        last_ms = int(getattr(self, "_last_live_data_path_log_ms", 0) or 0)
        interval_seconds = (
            getattr(self, "_project_env", None)
            and self._project_env.get_int(
                "AETHER_LIVE_DATA_PATH_STATS_INTERVAL_SECONDS", 1800
            )
        ) or 1800
        if last_ms and now_ms - last_ms < interval_seconds * 1000:
            return
        self._last_live_data_path_log_ms = now_ms
        interval_ms = int(getattr(self, "_closed_bar_interval_ms", 0) or 0)
        current_bucket = (
            (now_ms // interval_ms) * interval_ms
            if interval_ms > 0
            else None
        )
        range_bars_by_bucket = getattr(self, "_range_bars_by_bucket", {})
        current_bucket_count = (
            len(range_bars_by_bucket.get(current_bucket, ()))
            if current_bucket is not None
            else None
        )
        mf_audit = self._mf_observer_audit()
        writer = getattr(self, "_live_persistence_writer", None)
        pending = (
            writer.pending_count
            if isinstance(writer, _BackgroundWriteQueue)
            else None
        )
        writer_dropped = (
            writer.dropped
            if isinstance(writer, _BackgroundWriteQueue)
            else None
        )
        writer_failures = (
            writer.failures
            if isinstance(writer, _BackgroundWriteQueue)
            else None
        )
        writer_written = (
            writer.written
            if isinstance(writer, _BackgroundWriteQueue)
            else None
        )
        writer_submitted = (
            writer.submitted
            if isinstance(writer, _BackgroundWriteQueue)
            else None
        )
        logger.info(
            "Live data path stats | market_events_seen=%s feature_events_seen=%s latest_fixed_time_trade_bar_open_time_ms=%s mf_tradebar_count=%s mf_range_footprint_count=%s current_range_bucket_start_ms=%s range_bars_by_bucket_current_count=%s live_persistence_pending=%s live_persistence_dropped=%s live_persistence_failures=%s live_persistence_written=%s live_persistence_submitted=%s",
            getattr(getattr(self, "stats", None), "market_events_seen", None),
            getattr(getattr(self, "stats", None), "feature_events_seen", None),
            getattr(
                self,
                "_latest_fixed_time_trade_bar_open_time_ms",
                None,
            ),
            mf_audit.get("tradebar_count"),
            mf_audit.get("range_footprint_count"),
            current_bucket,
            current_bucket_count,
            pending,
            writer_dropped,
            writer_failures,
            writer_written,
            writer_submitted,
        )

    def _mf_observer_audit(self) -> Mapping[str, Any]:
        try:
            observers = resolve_market_feature_observers(
                self.context.strategy
            )
        except Exception as exc:
            logger.debug("MF observer audit unavailable | error=%s", exc)
            return {}
        for observer in observers:
            audit = getattr(observer, "audit", None)
            if not callable(audit):
                continue
            try:
                data = audit()
            except Exception as exc:
                logger.debug("MF observer audit failed | error=%s", exc)
                continue
            if isinstance(data, Mapping) and (
                "tradebar_count" in data
                or "range_footprint_count" in data
            ):
                return data
        return {}

    def _get_position_plan_store(self):
        if self._position_plan_store is None:
            path = self._project_env.get("AETHER_POSITION_PLAN_DB", "data/state/aether_position_plan.sqlite3")
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

    def _get_range_checkpoint_store(self) -> SqliteRangeCheckpointStore:
        if self._range_checkpoint_store is None:
            self._range_checkpoint_store = SqliteRangeCheckpointStore(
                self.runtime_config.range_checkpoint_db_path
            )
        return self._range_checkpoint_store

    def _get_range_checkpoint_writer(self) -> RangeCheckpointWriter:
        if self._range_checkpoint_writer is None:
            loop = asyncio.get_running_loop()

            def on_error(exc: BaseException) -> None:
                logger.warning("Range checkpoint write failed | error=%s", exc)
                loop.call_soon_threadsafe(
                    self.context.alerts.emit,
                    AppAlert(
                        subject="AetherEdge range checkpoint write failed",
                        content=str(exc),
                        severity="warning",
                    ),
                )

            self._range_checkpoint_writer = RangeCheckpointWriter(
                self._get_range_checkpoint_store(),
                max_pending=self.runtime_config.range_checkpoint_writer_max_pending,
                on_error=on_error,
            )
        return self._range_checkpoint_writer

    def _get_range_repair_bootstrap_service(
        self,
    ) -> RangeRepairBootstrapService:
        if self._range_repair_bootstrap_service is None:
            self._range_repair_bootstrap_service = (
                RangeRepairBootstrapService(
                    runtime_config=self.runtime_config,
                    exchange=self.app_config.data_exchange.value,
                    symbol=self.app_config.symbol,
                    range_pct=str(self._range_pct),
                    closed_bar_interval_ms=self._closed_bar_interval_ms,
                    checkpoint_store=self._get_range_checkpoint_store(),
                    emit_alert=self.context.alerts.emit,
                    journal_store=self._range_repair_journal_store,
                    journal_writer=self._range_repair_journal_writer,
                    micro_repair_supervisor=(
                        self._range_micro_repair_supervisor
                    ),
                    clock_ms=lambda: int(time.time() * 1000),
                )
            )
        return self._range_repair_bootstrap_service

    def _get_range_repair_journal_writer(
        self,
    ) -> RangeRepairJournalWriter:
        if self._range_repair_journal_writer is None:
            service = self._get_range_repair_bootstrap_service()
            self._range_repair_journal_writer = (
                service.get_journal_writer()
            )
            self._range_repair_journal_store = (
                service.get_journal_store()
            )
        return self._range_repair_journal_writer

    def _append_range_repair_trade(self, trade: MarketTrade) -> None:
        bucket_start_ms = self._range_repair_journal_bucket_ms
        checkpoint_ts = self._range_repair_checkpoint_last_trade_ts_ms
        writer = self._range_repair_journal_writer
        trade_time_ms = _event_time_ms(trade)
        if (
            writer is None
            or bucket_start_ms is None
            or checkpoint_ts is None
            or trade_time_ms is None
            or trade.exchange != self.app_config.data_exchange
            or getattr(trade.source, "value", str(trade.source))
            != "websocket"
            or trade_time_ms <= checkpoint_ts
            or trade_time_ms < bucket_start_ms
            or trade_time_ms
            >= bucket_start_ms + self._closed_bar_interval_ms
        ):
            return
        now_ms = int(time.time() * 1000)
        if self._range_repair_journal_finalize_submitted:
            self._invalidate_range_repair_journal(
                bucket_start_ms=bucket_start_ms,
                status=JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
                reason="live trade arrived after journal finalize",
            )
        if not self._range_repair_first_live_submitted:
            accepted = writer.submit_first_live(
                exchange=trade.exchange.value,
                symbol=trade.symbol,
                range_pct=str(self._range_pct),
                bucket_start_ms=bucket_start_ms,
                trade_time_ms=trade_time_ms,
                trade_id=trade.trade_id,
                recorded_at_ms=now_ms,
            )
            if accepted:
                self._range_repair_first_live_submitted = True
                logger.info(
                    "range_repair_first_live_trade_recorded | symbol=%s "
                    "exchange=%s bucket_start_ms=%s "
                    "first_live_trade_ts_ms=%s first_live_trade_id=%s",
                    trade.symbol,
                    trade.exchange.value,
                    bucket_start_ms,
                    trade_time_ms,
                    trade.trade_id,
                )
        source = getattr(trade.source, "value", str(trade.source))
        side = getattr(trade.side, "value", str(trade.side))
        accepted = writer.submit_trade(
            RangeRepairTrade(
                exchange=trade.exchange.value,
                symbol=trade.symbol,
                range_pct=str(self._range_pct),
                bucket_start_ms=bucket_start_ms,
                trade_time_ms=trade_time_ms,
                event_time_ms=trade.event_time_ms,
                trade_id=trade.trade_id,
                raw_symbol=trade.raw_symbol,
                side=side,
                price=str(trade.price),
                quantity=str(trade.quantity),
                source=source,
                created_at_ms=now_ms,
            )
        )
        if (
            not accepted
            and not self._range_repair_journal_append_failure_warned
        ):
            self._range_repair_journal_append_failure_warned = True
            logger.warning(
                "Range repair journal trade dropped | symbol=%s "
                "exchange=%s bucket_start_ms=%s trade_time_ms=%s",
                trade.symbol,
                trade.exchange.value,
                bucket_start_ms,
                trade_time_ms,
            )
            self.context.alerts.emit(
                AppAlert(
                    subject="AetherEdge range repair journal trade dropped",
                    content=(
                        f"symbol={trade.symbol}\n"
                        f"bucket_start_ms={bucket_start_ms}\n"
                        f"trade_time_ms={trade_time_ms}"
                    ),
                    severity="warning",
                )
            )

    def _invalidate_range_repair_journal(
        self,
        *,
        bucket_start_ms: int,
        status: str,
        reason: str,
        dropped_trades: int = 0,
    ) -> None:
        if (
            self._range_repair_journal_writer is None
            or self._range_repair_journal_bucket_ms != bucket_start_ms
        ):
            return
        self._range_repair_journal_writer.submit_invalidation(
            exchange=self.app_config.data_exchange.value,
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            bucket_start_ms=bucket_start_ms,
            status=status,
            last_error=reason,
            dropped_trades=dropped_trades,
        )

    def _finalize_range_repair_journal(
        self, *, bucket_start_ms: int, finalized_at_ms: int
    ) -> None:
        writer = self._range_repair_journal_writer
        if (
            writer is None
            or self._range_repair_journal_bucket_ms != bucket_start_ms
        ):
            return
        writer.submit_finalize(
            exchange=self.app_config.data_exchange.value,
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            bucket_start_ms=bucket_start_ms,
            finalized_at_ms=finalized_at_ms,
        )
        self._range_repair_journal_finalize_submitted = True
        logger.info(
            "range_repair_journal_finalized | symbol=%s exchange=%s "
            "bucket_start_ms=%s finalized_at_ms=%s",
            self.app_config.symbol,
            self.app_config.data_exchange.value,
            bucket_start_ms,
            finalized_at_ms,
        )

    def _start_range_speed_background_services(self) -> None:
        if not self.requirements.range_bars.enabled:
            return
        try:
            if self.runtime_config.range_micro_repair_enabled:
                self._get_range_micro_repair_supervisor().start_monitor(
                    stop_event=self._stop_event
                )
        except Exception as exc:
            logger.warning(
                "Range micro repair supervisor initialization failed | error=%s",
                exc,
            )
        try:
            if self.runtime_config.range_backfill_enabled:
                supervisor = self._get_range_backfill_supervisor()
                supervisor.start_monitor(
                    stop_event=self._stop_event,
                    symbol=self.app_config.symbol,
                    exchange=self.app_config.data_exchange.value,
                    range_pct=str(self._range_pct),
                    bucket_interval=self._closed_bar_interval,
                )
        except Exception as exc:
            logger.warning("Range backfill supervisor initialization failed | error=%s", exc)

        try:
            if self.runtime_config.range_speed_refresh_enabled:
                refresher = self._get_range_speed_history_refresher()
                refresher.start(self._stop_event)
        except Exception as exc:
            logger.warning("Range speed history refresher initialization failed | error=%s", exc)

    def _get_range_backfill_supervisor(self) -> RangeBackfillSupervisor:
        if self._range_backfill_supervisor is None:
            repo_root = Path(__file__).resolve().parents[2]
            self._range_backfill_supervisor = RangeBackfillSupervisor(
                RangeBackfillSupervisorConfig(
                    enabled=self.runtime_config.range_backfill_enabled,
                    required_buckets=self.runtime_config.range_backfill_required_buckets,
                    lookback_buckets=self.runtime_config.range_backfill_lookback_buckets,
                    max_buckets_per_cycle=self.runtime_config.range_backfill_max_buckets_per_cycle,
                    max_days_per_cycle=self.runtime_config.range_backfill_max_days_per_cycle,
                    sleep_seconds=self.runtime_config.range_backfill_sleep_seconds,
                    heartbeat_stale_seconds=self.runtime_config.range_backfill_heartbeat_stale_seconds,
                    restart_cooldown_seconds=self.runtime_config.range_backfill_restart_cooldown_seconds,
                    archive_publish_lag_hours=self.runtime_config.range_backfill_archive_publish_lag_hours,
                    failure_cooldown_seconds=self.runtime_config.range_repair_failure_cooldown_seconds,
                    archive_not_ready_cooldown_seconds=self.runtime_config.range_repair_archive_not_ready_cooldown_seconds,
                    daily_retry_after_utc_hour=self.runtime_config.range_repair_daily_retry_after_utc_hour,
                    monitor_seconds=self.runtime_config.range_backfill_monitor_seconds,
                    status_path=Path(self.runtime_config.range_backfill_status_path),
                    lock_path=Path(self.runtime_config.range_backfill_lock_path),
                    low_priority=self.runtime_config.range_backfill_low_priority,
                    chunksize=self.runtime_config.range_backfill_chunksize,
                    raw_root=Path(self.runtime_config.range_backfill_raw_root),
                    market_db_path=Path(self.runtime_config.market_data_db_path),
                    checkpoint_db_path=Path(self.runtime_config.range_checkpoint_db_path),
                    save_raw_trades=self.runtime_config.range_backfill_save_raw_trades,
                    chunk_sleep_seconds=self.runtime_config.range_backfill_chunk_sleep_seconds,
                    max_seconds_per_cycle=self.runtime_config.range_backfill_max_seconds_per_cycle,
                    max_trades_per_cycle=self.runtime_config.range_backfill_max_trades_per_cycle,
                    repo_root=repo_root,
                )
            )
        return self._range_backfill_supervisor

    def _get_range_micro_repair_supervisor(
        self,
    ) -> RangeMicroRepairSupervisor:
        if self._range_micro_repair_supervisor is None:
            self._range_micro_repair_supervisor = (
                self._get_range_repair_bootstrap_service()
                .get_micro_repair_supervisor()
            )
        return self._range_micro_repair_supervisor

    def _get_range_speed_history_refresher(self) -> RangeSpeedHistoryRefresher:
        if self._range_speed_history_refresher is None:
            self._range_speed_history_refresher = RangeSpeedHistoryRefresher(
                strategy=self.context.strategy,
                store=self._get_range_checkpoint_store(),
                symbol=self.app_config.symbol,
                exchange=self.app_config.data_exchange.value,
                range_pct=str(self._range_pct),
                bucket_interval=self._closed_bar_interval,
                refresh_seconds=self.runtime_config.range_speed_refresh_seconds,
                warning_seconds=self.runtime_config.range_speed_status_warning_seconds,
                backfill_enabled=self.runtime_config.range_backfill_enabled,
                status_path=self.runtime_config.range_backfill_status_path,
            )
        return self._range_speed_history_refresher

    async def _stop_range_speed_background_services(self) -> None:
        refresher = self._range_speed_history_refresher
        if refresher is not None:
            stop = getattr(refresher, "stop", None)
            if callable(stop):
                result = stop()
                if asyncio.iscoroutine(result):
                    await result
        supervisor = self._range_backfill_supervisor
        if supervisor is not None:
            stop_async = getattr(supervisor, "stop_async", None)
            if callable(stop_async):
                await stop_async()
            else:
                stop = getattr(supervisor, "stop", None)
                if callable(stop):
                    await asyncio.to_thread(stop)
        micro_supervisor = self._range_micro_repair_supervisor
        if micro_supervisor is not None:
            stop_async = getattr(micro_supervisor, "stop_async", None)
            if callable(stop_async):
                await stop_async()

    async def _stop_range_checkpoint_writer(self) -> None:
        writer = self._range_checkpoint_writer
        if writer is None:
            return
        stop = getattr(writer, "stop", None)
        if callable(stop):
            await asyncio.to_thread(stop, flush=True)

    async def _stop_range_repair_journal_writer(self) -> None:
        writer = self._range_repair_journal_writer
        if writer is None:
            return
        stop = getattr(writer, "stop", None)
        if callable(stop):
            await asyncio.to_thread(stop, flush=True)

    def _range_coverage_for_bucket(
        self, bucket_start_ms: int
    ) -> RangeCheckpointRecovery:
        if (
            self._initial_range_bucket_ms == bucket_start_ms
            and self._initial_range_recovery is not None
        ):
            return self._initial_range_recovery
        return RangeCheckpointRecovery(
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            checkpoint=None,
            checkpoint_age_ms=None,
            missing_gap_ms=0,
            recovered_from_checkpoint=False,
        )

    def _refresh_range_micro_repair_coverage(
        self, bucket_start_ms: int
    ) -> None:
        """Adopt a repaired DB aggregate without replaying live trades."""

        if (
            self._initial_range_bucket_ms != bucket_start_ms
            or self._initial_range_recovery is None
            or self._initial_range_recovery.coverage_status
            == RangeCoverageStatus.COMPLETE.value
        ):
            return
        bucket_end_ms = (
            bucket_start_ms + self._closed_bar_interval_ms - 1
        )
        completed = self._get_range_checkpoint_store().load_completed_aggregate(
            exchange=self.app_config.data_exchange.value,
            symbol=self.app_config.symbol,
            range_pct=str(self._range_pct),
            bucket_end_ms=bucket_end_ms,
        )
        if (
            completed is None
            or completed.coverage_status
            != RangeCoverageStatus.COMPLETE.value
        ):
            return
        self._initial_range_recovery = RangeCheckpointRecovery(
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            checkpoint=None,
            checkpoint_age_ms=None,
            missing_gap_ms=0,
            recovered_from_checkpoint=True,
        )
        self._rangebar_trust_start_bucket_ms = bucket_start_ms
        self._range_context_degraded_buckets.pop(bucket_start_ms, None)
        # ── Force canonical rows to come from repaired DB, not partial
        #     in-memory rows that were collected before micro repair finished.
        self._range_repaired_complete_buckets.add(bucket_start_ms)
        self._range_bars_by_bucket.pop(bucket_start_ms, None)
        logger.info(
            "Range micro repair COMPLETE aggregate adopted | symbol=%s "
            "exchange=%s bucket_start_ms=%s bucket_end_ms=%s "
            "cleared_partial_memory_rows=True repaired_complete=True",
            self.app_config.symbol,
            self.app_config.data_exchange.value,
            bucket_start_ms,
            bucket_end_ms,
        )

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


async def _fetch_execution_instrument_rule(
    execution: ExecutionClient,
) -> InstrumentRule | None:
    """Read the bound rule through the public execution facade when exposed."""

    fetch_rule = getattr(execution, "fetch_instrument_rule", None)
    if not callable(fetch_rule):
        return None
    value = fetch_rule()
    if inspect.isawaitable(value):
        value = await value
    return value if isinstance(value, InstrumentRule) else None


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


def _stop_post_check_attempts_from_env(project_env: ProjectEnvConfig) -> int:
    """Parse ``AETHER_STOP_POST_CHECK_ATTEMPTS`` safely, clamping to >= 1."""
    raw = project_env.get("AETHER_STOP_POST_CHECK_ATTEMPTS", "").strip()
    if not raw:
        return 3
    try:
        value = int(raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid stop post-check env value; using default | env=%s raw=%r default=3",
            "AETHER_STOP_POST_CHECK_ATTEMPTS",
            project_env.get("AETHER_STOP_POST_CHECK_ATTEMPTS", ""),
        )
        return 3
    return max(1, value)


def _account_snapshot_log_keepalive_seconds_from_env(project_env: ProjectEnvConfig) -> float:
    """Parse account snapshot INFO keepalive seconds, where zero disables it."""
    raw = project_env.get("AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS", "").strip()
    if not raw:
        return 3600
    try:
        value = float(raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid account snapshot log keepalive env value; using default | env=%s raw=%r default=3600",
            "AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS",
            project_env.get("AETHER_ACCOUNT_SNAPSHOT_LOG_KEEPALIVE_SECONDS", ""),
        )
        return 3600
    return max(0.0, value)


def _stop_post_check_delay_from_env(project_env: ProjectEnvConfig) -> float:
    """Parse ``AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS`` safely, clamping to >= 0.0."""
    raw = project_env.get("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", "").strip()
    if not raw:
        return 0.5
    try:
        value = float(raw)
    except (ValueError, TypeError):
        logger.warning(
            "Invalid stop post-check env value; using default | env=%s raw=%r default=0.5",
            "AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS",
            project_env.get("AETHER_STOP_POST_CHECK_RETRY_DELAY_SECONDS", ""),
        )
        return 0.5
    return max(0.0, value)


def _all_exchange_sandbox(exchanges: Sequence[ExchangeName], project_env: ProjectEnvConfig) -> bool:
    if not exchanges:
        return False
    return all(
        project_env.get_bool(f"{exchange.value.upper()}_SANDBOX", project_env.get_bool("SANDBOX", False))
        for exchange in exchanges
    )


def _single_active_exchange_position_or_none_for_legacy(
    positions: Sequence[Position],
) -> Position | None:
    active = tuple(position for position in positions if position.quantity != 0)
    return active[0] if len(active) == 1 else None


# Backward-compatible import for pre-R004 tests. Runtime paths use the
# explicitly named helper above, which rejects ambiguous multi-position input.
_first_active_position = _single_active_exchange_position_or_none_for_legacy


def _active_exchange_positions(
    positions: Sequence[Position],
) -> tuple[Position, ...]:
    return tuple(position for position in positions if position.quantity != 0)


def _exchange_positions_matching_strategy_position(
    positions: Sequence[Position],
    strategy_position: StrategyPositionSnapshot,
) -> tuple[Position, ...]:
    candidates = tuple(
        position
        for position in _active_exchange_positions(positions)
        if position.symbol == strategy_position.symbol
    )
    if strategy_position.side is StrategyPositionSide.LONG:
        return tuple(
            position
            for position in candidates
            if _exchange_position_matches_long(position)
        )
    if strategy_position.side is StrategyPositionSide.SHORT:
        return tuple(
            position
            for position in candidates
            if _exchange_position_matches_short(position)
        )
    if strategy_position.side in {
        StrategyPositionSide.BOTH,
        StrategyPositionSide.UNKNOWN,
    }:
        return candidates
    return ()


def _exchange_position_matches_long(position: Position) -> bool:
    if position.side is PositionSide.LONG:
        return True
    if position.side is PositionSide.SHORT:
        return False
    return position.quantity > 0


def _exchange_position_matches_short(position: Position) -> bool:
    if position.side is PositionSide.SHORT:
        return True
    if position.side is PositionSide.LONG:
        return False
    return position.quantity < 0


def _position_side_for_strategy_position(
    strategy_position: StrategyPositionSnapshot,
    exchange_position: Position,
) -> PositionSide | None:
    if strategy_position.side is StrategyPositionSide.LONG:
        return PositionSide.LONG
    if strategy_position.side is StrategyPositionSide.SHORT:
        return PositionSide.SHORT
    side = _position_side_from_quantity(exchange_position.quantity)
    if side is not None:
        return side
    if exchange_position.side in {PositionSide.LONG, PositionSide.SHORT}:
        return exchange_position.side
    return None


def _strategy_position_native_quantity(
    *,
    strategy_position: StrategyPositionSnapshot,
    active_pos: Position,
    exchange: ExchangeName,
    market_profile,
    converter: NativeQuantityConverter,
    logical_position_count: int,
    scoped_base_quantity: Decimal | None = None,
) -> Decimal:
    if scoped_base_quantity is not None and scoped_base_quantity > 0:
        try:
            return converter.convert_quantity(
                exchange=exchange,
                symbol=strategy_position.symbol,
                base_quantity=scoped_base_quantity,
                market_profile=market_profile,
            ).native_quantity
        except Exception as exc:
            raise LiveRuntimeError(
                "stop protection validation failed: strategy quantity conversion failed | "
                f"strategy_position_id={strategy_position.position_id} "
                f"symbol={strategy_position.symbol} exchange={exchange.value} error={exc}"
            ) from exc
    if logical_position_count <= 1:
        return abs(active_pos.quantity)
    if strategy_position.base_quantity > 0:
        try:
            return converter.convert_quantity(
                exchange=exchange,
                symbol=strategy_position.symbol,
                base_quantity=strategy_position.base_quantity,
                market_profile=market_profile,
            ).native_quantity
        except Exception as exc:
            raise LiveRuntimeError(
                "stop protection validation failed: strategy quantity conversion failed | "
                f"strategy_position_id={strategy_position.position_id} "
                f"symbol={strategy_position.symbol} exchange={exchange.value} error={exc}"
            ) from exc
    raise LiveRuntimeError(
        "stop protection validation failed: missing scoped strategy quantity | "
        f"strategy_position_id={strategy_position.position_id} "
        f"symbol={strategy_position.symbol} side={strategy_position.side.value} "
        f"exchange={exchange.value} active_strategy_positions={logical_position_count}"
    )


def _raise_ambiguous_exchange_positions(
    *,
    context: str,
    strategy_position: StrategyPositionSnapshot,
    exchange: str,
    ambiguous_count: int,
) -> None:
    raise LiveRuntimeError(
        f"{context}: ambiguous exchange positions | "
        f"strategy_position_id={strategy_position.position_id} "
        f"symbol={strategy_position.symbol} side={strategy_position.side.value} "
        f"exchange={exchange} ambiguous_count={ambiguous_count}"
    )


def _strategy_position_for_stop_signal(
    index: StrategyPositionSnapshotIndex,
    signal: TradeSignal,
) -> StrategyPositionSnapshot | None:
    position_id = _signal_position_id(signal)
    if position_id is not None:
        matches = tuple(
            snapshot
            for snapshot in index.by_position_id(position_id)
            if snapshot.status is StrategyPositionStatus.ACTIVE
        )
        return matches[0] if len(matches) == 1 else None

    side = {
        SignalAction.PLACE_STOP_LOSS_LONG: StrategyPositionSide.LONG,
        SignalAction.PLACE_STOP_LOSS_SHORT: StrategyPositionSide.SHORT,
    }.get(signal.action)
    if side is not None:
        matches = tuple(
            snapshot
            for snapshot in index.by_symbol_side(signal.symbol, side)
            if snapshot.status is StrategyPositionStatus.ACTIVE
        )
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            return None

    # Legacy signals may not carry a position_id. This fallback is safe only
    # when the strategy exposes exactly one active logical position.
    return index.single_active_or_none_for_legacy()


def _signal_position_id(signal: TradeSignal) -> str | None:
    if not signal.metadata:
        return None
    value = signal.metadata.get("position_id")
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _strategy_position_active_exchanges(
    snapshot: StrategyPositionSnapshot,
) -> frozenset[str]:
    value = snapshot.metadata.get("active_exchanges")
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, (list, tuple, set, frozenset)):
        values = value
    else:
        return frozenset()
    return frozenset(
        normalized
        for item in values
        if (normalized := str(item).strip().lower())
    )


def _strategy_position_requires_protective_stop(
    snapshot: StrategyPositionSnapshot,
) -> bool:
    value = snapshot.metadata.get("protective_stop_required", True)
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"false", "0", "no", "n", ""}:
        return False
    return True


def _strategy_position_stop_order_ids(
    snapshot: StrategyPositionSnapshot,
) -> tuple[str, ...]:
    raw = snapshot.metadata.get("stop_order_ids", ())
    if isinstance(raw, str):
        values = (raw,)
    elif isinstance(raw, (list, tuple, set, frozenset)):
        values = raw
    else:
        values = ()
    return tuple(
        normalized
        for value in values
        if (normalized := str(value or "").strip())
    )


def _place_stop_scope_covers(
    scopes: Mapping[str, set[str | None]],
    *,
    exchange: str,
    position_id: str | None,
    logical_position_count: int,
) -> bool:
    exchange_scopes = scopes.get(exchange, set())
    if position_id is not None and position_id in exchange_scopes:
        return True
    return None in exchange_scopes and logical_position_count <= 1


def _position_side_from_quantity(quantity: Decimal) -> PositionSide | None:
    if quantity > 0:
        return PositionSide.LONG
    if quantity < 0:
        return PositionSide.SHORT
    return None


def _exchange_position_metadata(
    *,
    active_pos: Position,
    exchange: ExchangeName,
    symbol: str,
    market_profile,
    converter: NativeQuantityConverter,
) -> dict[str, Any]:
    native_qty = abs(active_pos.quantity)
    side = _position_side_from_quantity(active_pos.quantity)
    if side is None and active_pos.side in {PositionSide.LONG, PositionSide.SHORT}:
        side = active_pos.side
    metadata: dict[str, Any] = {
        "exchange_position_source": "stop_post_check",
        "exchange_position_side": None if side is None else side.value,
        "exchange_position_native_quantity": native_qty,
        "exchange_position_entry_price": active_pos.entry_price,
    }
    try:
        metadata["exchange_position_base_quantity"] = converter.native_to_base_quantity(
            exchange=exchange,
            symbol=symbol,
            native_quantity=native_qty,
            market_profile=market_profile,
        )
    except Exception as exc:
        logger.warning(
            "Stop post-check exchange position quantity conversion failed | exchange=%s symbol=%s native_quantity=%s error=%s",
            exchange.value,
            symbol,
            native_qty,
            exc,
        )
        metadata["exchange_position_base_quantity_convert_error"] = str(exc)
    return metadata


def _position_side_label(position: Position) -> str:
    side = _position_side_from_quantity(position.quantity)
    if side is PositionSide.LONG:
        return "long"
    if side is PositionSide.SHORT:
        return "short"
    return "flat"


async def _jittered_sleep(stop_event: asyncio.Event, interval_seconds: float) -> None:
    import random
    jitter = random.uniform(0, min(5.0, interval_seconds * 0.1))
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds + jitter)
    except asyncio.TimeoutError:
        pass
