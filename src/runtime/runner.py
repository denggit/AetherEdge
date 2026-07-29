from __future__ import annotations

from src.app.alerts import AppAlert
from src.market_data.events import MarketFeatureEvent
from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketEvent
from src.runtime.models import RuntimeHealth, RuntimeMode, RuntimePhase
from src.runtime.market_data.runtime import MarketDataRuntime
from src.runtime.module import CapabilityId
from src.runtime.shutdown_coordinator import RuntimeShutdownCoordinator

from src.runtime.components import (
    COMPONENT_TYPES,
    AccountComponent,
    CatchupComponent,
    ClosedBarComponent,
    LifecycleComponent,
    MarketEventsComponent,
    OrderResultsComponent,
    PersistenceComponent,
    RangeRuntimeComponent,
    RecoveryComponent,
    SignalExecutionComponent,
    StartupComponent,
    WiringComponent,
)
from src.runtime.context import RuntimeContext
from src.runtime.services import RuntimeServiceBundle, RuntimeServices
from src.runtime.persistence import (
    BackgroundWriteItem as _BackgroundWriteItem,
    BackgroundWriteQueue as _BackgroundWriteQueue,
)
from src.runtime.startup_feature_backfill import (
    resolve_startup_feature_backfill_providers,
)
from src.runtime.live_helpers import _account_snapshot_log_keepalive_seconds_from_env, _active_exchange_positions, _all_exchange_sandbox, _event_time_ms, _exchange_position_matches_long, _exchange_position_matches_short, _exchange_position_metadata, _exchange_positions_matching_strategy_position, _fetch_execution_instrument_rule, _first_active_position, _is_trade_at_or_before, _jittered_sleep, _place_stop_scope_covers, _position_side_for_strategy_position, _position_side_from_quantity, _position_side_label, _raise_ambiguous_exchange_positions, _signal_position_id, _single_active_exchange_position_or_none_for_legacy, _stop_post_check_attempts_from_env, _stop_post_check_delay_from_env, _strategy_position_active_exchanges, _strategy_position_for_stop_signal, _strategy_position_native_quantity, _strategy_position_requires_protective_stop, _strategy_position_stop_order_ids
from src.runtime.live_types import (
    FATAL_STARTUP_ERROR_MARKERS,
    LiveRuntimeError,
    LiveRuntimeStats,
    MarketQueueDrainResult,
    StartupPreviewState,
    _is_fatal_startup_error,
    logger,
)


_FATAL_ALERT_FLUSH_TIMEOUT_SECONDS = 2.0


async def _flush_fatal_runtime_alert(alerts) -> None:
    flush = getattr(alerts, "flush", None)
    if not callable(flush):
        return
    try:
        if not await flush(timeout_seconds=_FATAL_ALERT_FLUSH_TIMEOUT_SECONDS):
            logger.warning(
                "Fatal runtime alert flush timed out | timeout_seconds=%s",
                _FATAL_ALERT_FLUSH_TIMEOUT_SECONDS,
            )
    except Exception:
        logger.exception("Fatal runtime alert flush failed")


class _RunnerCompatibilityFacade(
    AccountComponent,
    CatchupComponent,
    ClosedBarComponent,
    LifecycleComponent,
    MarketEventsComponent,
    OrderResultsComponent,
    PersistenceComponent,
    RecoveryComponent,
    RangeRuntimeComponent,
    SignalExecutionComponent,
    StartupComponent,
    WiringComponent,
):
    """Static compatibility surface for legacy tests and integrations."""

    def _named_component(self, name: str, component_type: type):
        components = self.__dict__.get("_runtime_components")
        if components is None:
            return self
        return components[component_type]

    def _compat_override(self, name: str, default):
        return self.__dict__.get(name, default)

    def _runtime_service_bundle(self) -> RuntimeServiceBundle:
        bundle = self.__dict__.get("service_bundle")
        if isinstance(bundle, RuntimeServiceBundle):
            return bundle
        legacy = RuntimeServices.coerce(self.__dict__.get("services"))
        bundle = RuntimeServiceBundle.from_legacy_boundary(legacy)
        self.runtime_services = legacy
        self.service_bundle = bundle
        return bundle

class LiveRuntimeRunner(_RunnerCompatibilityFacade):
    """Thin lifecycle orchestrator over typed domain runtime components."""

    def __init__(self, *args, **kwargs) -> None:
        runtime_context = RuntimeContext()
        self.__dict__ = runtime_context.__dict__
        components = {
            component_type: component_type(runtime_context)
            for component_type in COMPONENT_TYPES
        }
        self._runtime_components = components
        self.wiring = components[WiringComponent]
        self.lifecycle = components[LifecycleComponent]
        self.market_events = components[MarketEventsComponent]
        self.closed_bar = components[ClosedBarComponent]
        self.signal_execution = components[SignalExecutionComponent]
        self.account_runtime = components[AccountComponent]
        self.recovery = components[RecoveryComponent]
        self.startup = components[StartupComponent]
        self.catchup = components[CatchupComponent]
        self.order_results = components[OrderResultsComponent]
        self.persistence = components[PersistenceComponent]
        self.market_data_lifecycle = components[RangeRuntimeComponent]
        self._bind_component_ports(components)
        self.wiring.initialize(*args, **kwargs)

    def _bind_component_ports(self, components: dict[type, object]) -> None:
        """Bind the explicit callable ports used across domain components."""

        self._runtime_service_bundle = (
            _RunnerCompatibilityFacade._runtime_service_bundle.__get__(
                self,
                LiveRuntimeRunner,
            )
        )
        account = components[AccountComponent]
        catchup = components[CatchupComponent]
        lifecycle = components[LifecycleComponent]
        market = components[MarketEventsComponent]
        orders = components[OrderResultsComponent]
        persistence = components[PersistenceComponent]
        range_runtime = components[RangeRuntimeComponent]
        recovery = components[RecoveryComponent]
        signals = components[SignalExecutionComponent]
        startup = components[StartupComponent]

        self._execute_signals = signals._execute_signals
        self._get_account_clients = account._get_account_clients
        self._get_account_sync_service = account._get_account_sync_service
        self._get_order_sync_service = account._get_order_sync_service
        self._get_sync_contexts = account._get_sync_contexts
        self._has_account_config_entry_block = (
            account._has_account_config_entry_block
        )
        self._has_unresolved_follower_close = (
            account._has_unresolved_follower_close
        )
        self._periodic_follower_close_check = (
            account._periodic_follower_close_check
        )
        self._recheck_account_config_after_recovery = (
            startup._recheck_account_config_after_recovery
        )
        self._resolved_account_config_env = (
            account._resolved_account_config_env
        )
        self._run_reconciliation = account._run_reconciliation
        self._strategy_position_index = account._strategy_position_index

        self._get_min_range_bars = catchup._get_min_range_bars
        self._call_on_start = catchup._call_on_start
        self._evaluate_startup_catchup_once = (
            catchup._evaluate_startup_catchup_once
        )

        self._all_producers_done = lifecycle._all_producers_done
        self._raise_on_unhealthy_producer = (
            lifecycle._raise_on_unhealthy_producer
        )
        self._set_health = lifecycle._set_health

        self._enqueue_market_event = market._enqueue_market_event
        self._mark_range_context_degraded_bucket = (
            market._mark_range_context_degraded_bucket
        )
        self._raise_on_unhealthy_market_data = (
            market._raise_on_unhealthy_market_data
        )
        self._trade_integrity_tracker = market._trade_integrity_tracker
        self._order_book_integrity_tracker = (
            market._order_book_integrity_tracker
        )

        self._get_execution_clients = orders._get_execution_clients
        self._get_order_coordinator = orders._get_order_coordinator
        self._get_order_journal = orders._get_order_journal
        self._validate_order_results_before_journal = (
            signals._validate_order_results_before_journal
        )
        self._process_order_result_feedback = (
            orders._process_order_result_feedback
        )
        self._save_order_results = orders._save_order_results
        self._verify_stop_order_results = orders._verify_stop_order_results

        self._emit_alert_threadsafe = persistence._emit_alert_threadsafe
        self._get_market_data_persistence = (
            persistence._get_market_data_persistence
        )
        self._get_market_feature_pipeline = (
            persistence._get_market_feature_pipeline
        )
        self._get_position_plan_store = (
            persistence._get_position_plan_store
        )
        self._maybe_log_live_data_path_stats = (
            persistence._maybe_log_live_data_path_stats
        )
        self._on_live_persistence_write_rejected = (
            persistence._on_live_persistence_write_rejected
        )

        self._get_live_kline_store = range_runtime._get_live_kline_store
        self._get_range_repair_bootstrap_service = (
            range_runtime._get_range_repair_bootstrap_service
        )
        self._require_range_module = range_runtime._require_range_module
        self._start_range_speed_background_services = (
            range_runtime._start_range_speed_background_services
        )

        self._run_recovery = recovery._run_recovery
        self._strategy_range_speed_history_provider = (
            recovery._strategy_range_speed_history_provider
        )
        self._strategy_capabilities = recovery._strategy_capabilities
        self._strategy_pending_work_provider = (
            recovery._strategy_pending_work_provider
        )
        self._strategy_startup_preview_provider = (
            recovery._strategy_startup_preview_provider
        )

        self._bootstrap_account_config_if_enabled = (
            startup._bootstrap_account_config_if_enabled
        )
        self._check_strategy_position_mode_requirements = (
            startup._check_strategy_position_mode_requirements
        )
        self._finish_range_speed_warmup_after_catchup = (
            startup._finish_range_speed_warmup_after_catchup
        )
        self._initialize_rangebar_trust_window = (
            startup._initialize_rangebar_trust_window
        )
        self._run_warmup = startup._run_warmup
        self._warmup_range_speed_history = (
            startup._warmup_range_speed_history
        )

        self._on_range_bar_persist_error = (
            components[ClosedBarComponent]._on_range_bar_persist_error
        )
        self._on_completed_range_aggregate_persist_error = (
            components[
                ClosedBarComponent
            ]._on_completed_range_aggregate_persist_error
        )

        self.process_market_event = LiveRuntimeRunner.process_market_event.__get__(
            self,
            LiveRuntimeRunner,
        )
        self.process_market_feature = (
            LiveRuntimeRunner.process_market_feature.__get__(
                self,
                LiveRuntimeRunner,
            )
        )

    def attach_market_data_runtime(
        self,
        runtime: MarketDataRuntime,
        capabilities: frozenset[CapabilityId],
    ) -> None:
        market_state = self.runtime_state.market
        if market_state.runtime is not None:
            raise RuntimeError("market data runtime is already attached")
        market_state.runtime = runtime
        market_state.capabilities = capabilities
        market_state.modules_managed = True
        # Explicitly listed compatibility fields for older integrations.
        self._market_data_runtime = runtime
        self._market_data_capabilities = capabilities
        self._market_modules_managed = True

    async def enqueue_market_event(self, event: MarketEvent) -> None:
        await self._named_component(
            "market_events",
            MarketEventsComponent,
        )._enqueue_market_event(event)

    async def handle_dropped_trade(self, event: MarketEvent) -> None:
        await self._named_component(
            "market_events",
            MarketEventsComponent,
        )._handle_market_data_trade_drop(event)

    async def _prepare_market_data_modules(self) -> None:
        market_state = self.runtime_state.market
        runtime = market_state.runtime
        if runtime is not None:
            await runtime.prepare(market_state.capabilities)

    async def _start_market_data_modules(self) -> None:
        runtime = self.runtime_state.market.runtime
        if runtime is not None:
            await runtime.start_prepared()

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
            await self._prepare_market_data_modules()
            await self._compat_override(
                "_startup",
                self.lifecycle._run_startup_sequence,
            )()
            await self._start_market_data_modules()
            self._producer_tasks = self._compat_override(
                "_start_producers",
                self.lifecycle._start_producers,
            )()
            self._sync_tasks = self._compat_override(
                "_start_sync_tasks",
                self.lifecycle._start_sync_tasks,
            )()
            await self._compat_override(
                "_consume_market_events",
                self.market_events._consume_market_events,
            )(max_market_events=max_market_events)
            self._compat_override(
                "_set_health",
                self.lifecycle._set_health,
            )(RuntimePhase.STOPPED, healthy=self._health.healthy)
            logger.info("Live runtime stopped | stats=%s", self.stats)
            return self.stats
        except Exception as exc:
            self.stats.errors += 1
            self._compat_override(
                "_set_health",
                self.lifecycle._set_health,
            )(RuntimePhase.ERROR, healthy=False, error=str(exc))
            logger.exception("Live runtime error")
            fatal_alert = AppAlert(
                subject="AetherEdge live runtime error",
                content=str(exc),
                severity="error",
            )
            queued = self.context.alerts.emit(fatal_alert)
            await _flush_fatal_runtime_alert(self.context.alerts)
            if not queued:
                if self.context.alerts.emit(fatal_alert):
                    await _flush_fatal_runtime_alert(self.context.alerts)
                else:
                    queue = getattr(self.context.alerts, "_queue", None)
                    logger.critical(
                        "Fatal runtime alert could not be queued after bounded drain | "
                        "subject=%s queue_size=%s maxsize=%s sent=%s failed=%s dropped=%s",
                        fatal_alert.subject,
                        queue.qsize() if queue is not None else "unknown",
                        getattr(queue, "maxsize", "unknown"),
                        getattr(self.context.alerts, "sent", "unknown"),
                        getattr(self.context.alerts, "failed", "unknown"),
                        getattr(self.context.alerts, "dropped", "unknown"),
                    )
            raise
        finally:
            await self._run_finally_shutdown()

    async def start(self) -> RuntimeHealth:
        lifecycle = self._named_component("lifecycle", LifecycleComponent)
        self._compat_override(
            "_set_health",
            lifecycle._set_health,
        )(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)
        return self._health

    async def stop(self) -> RuntimeHealth:
        self._stop_event.set()
        await self._explicit_stop_shutdown()
        lifecycle = self._named_component("lifecycle", LifecycleComponent)
        self._compat_override(
            "_set_health",
            lifecycle._set_health,
        )(RuntimePhase.STOPPED, healthy=True)
        return self._health

    async def _run_finally_shutdown(self) -> None:
        market_data_lifecycle = self._named_component(
            "market_data_lifecycle",
            COMPONENT_TYPES[-1],
        )
        lifecycle = self._named_component("lifecycle", LifecycleComponent)
        persistence = self._named_component(
            "persistence",
            PersistenceComponent,
        )
        await self._shutdown_coordinator.execute(
            (
                self._compat_override(
                    "_stop_market_data_modules",
                    market_data_lifecycle._stop_market_data_modules,
                ),
                self._compat_override(
                    "_stop_sync_tasks",
                    lifecycle._stop_sync_tasks,
                ),
                self._compat_override(
                    "_stop_producers",
                    lifecycle._stop_producers,
                ),
                self._compat_override(
                    "_stop_live_persistence_writer",
                    persistence._stop_live_persistence_writer,
                ),
                self.context.alerts.stop,
            )
        )

    async def _explicit_stop_shutdown(self) -> None:
        coordinator = getattr(
            self,
            "_shutdown_coordinator",
            RuntimeShutdownCoordinator,
        )
        market_data_lifecycle = self._named_component(
            "market_data_lifecycle",
            COMPONENT_TYPES[-1],
        )
        lifecycle = self._named_component("lifecycle", LifecycleComponent)
        persistence = self._named_component(
            "persistence",
            PersistenceComponent,
        )
        await coordinator.execute(
            (
                self._compat_override(
                    "_stop_market_data_modules",
                    market_data_lifecycle._stop_market_data_modules,
                ),
                self._compat_override(
                    "_stop_producers",
                    lifecycle._stop_producers,
                ),
                self._compat_override(
                    "_stop_live_persistence_writer",
                    persistence._stop_live_persistence_writer,
                ),
            )
        )

    async def health(self) -> RuntimeHealth:
        return self._health

    async def process_market_event(self, event: MarketEvent) -> None:
        await self._named_component(
            "market_events",
            MarketEventsComponent,
        )._process_market_event(event)

    async def process_market_feature(self, event: MarketFeatureEvent) -> None:
        await self._named_component(
            "market_events",
            MarketEventsComponent,
        )._process_market_feature_event(event)

    async def process_account_event(self, event: AccountEvent) -> None:
        await self._named_component(
            "account_runtime",
            AccountComponent,
        )._process_account_event(event)

    async def _startup(self) -> None:
        await self.lifecycle._run_startup_sequence()

__all__ = ["LiveRuntimeRunner", "LiveRuntimeStats", "LiveRuntimeError", "_is_fatal_startup_error"]
