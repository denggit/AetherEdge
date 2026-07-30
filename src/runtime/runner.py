from __future__ import annotations

from src.app.alerts import AppAlert
from src.market_data.events import MarketFeatureEvent
from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketEvent
from src.runtime.assembly import configure_runtime_composition
from src.runtime.compat.runner_state import LegacyRunnerStateFacade
from src.runtime.components import (
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
from src.runtime.live_helpers import (
    _account_snapshot_log_keepalive_seconds_from_env,
    _active_exchange_positions,
    _all_exchange_sandbox,
    _event_time_ms,
    _exchange_position_matches_long,
    _exchange_position_matches_short,
    _exchange_position_metadata,
    _exchange_positions_matching_strategy_position,
    _fetch_execution_instrument_rule,
    _first_active_position,
    _is_trade_at_or_before,
    _jittered_sleep,
    _place_stop_scope_covers,
    _position_side_for_strategy_position,
    _position_side_from_quantity,
    _position_side_label,
    _raise_ambiguous_exchange_positions,
    _signal_position_id,
    _single_active_exchange_position_or_none_for_legacy,
    _stop_post_check_attempts_from_env,
    _stop_post_check_delay_from_env,
    _strategy_position_active_exchanges,
    _strategy_position_for_stop_signal,
    _strategy_position_native_quantity,
    _strategy_position_requires_protective_stop,
    _strategy_position_stop_order_ids,
)
from src.runtime.live_types import (
    LiveRuntimeError,
    LiveRuntimeStats,
    _is_fatal_startup_error,
    logger,
)
from src.runtime.market_data.runtime import MarketDataRuntime
from src.runtime.models import RuntimeHealth, RuntimePhase
from src.runtime.module import CapabilityId
from src.runtime.persistence import (
    BackgroundWriteItem as _BackgroundWriteItem,
    BackgroundWriteQueue as _BackgroundWriteQueue,
)
from src.runtime.ports import WiringPorts
from src.runtime.services import RuntimeServiceBundle
from src.runtime.shutdown_coordinator import RuntimeShutdownCoordinator


_FATAL_ALERT_FLUSH_TIMEOUT_SECONDS = 2.0


async def _flush_fatal_runtime_alert(alerts) -> None:
    flush = getattr(alerts, "flush", None)
    if not callable(flush):
        return
    try:
        completed = await flush(
            timeout_seconds=_FATAL_ALERT_FLUSH_TIMEOUT_SECONDS
        )
        if not completed:
            logger.warning(
                "Fatal runtime alert flush timed out | timeout_seconds=%s",
                _FATAL_ALERT_FLUSH_TIMEOUT_SECONDS,
            )
    except Exception:
        logger.exception("Fatal runtime alert flush failed")


class LiveRuntimeRunner(LegacyRunnerStateFacade):
    """Lifecycle facade over explicitly composed domain components."""

    def __init__(self, *args, **kwargs) -> None:
        runtime_context = RuntimeContext()
        self.context = runtime_context
        wiring_ports = WiringPorts(
            process_market_feature=lambda *a, **k: (
                self.process_market_feature(*a, **k)
            ),
            get_market_data_persistence=lambda: (
                self.persistence._get_market_data_persistence()
            ),
            get_range_repair_bootstrap_service=lambda: (
                self.market_data_lifecycle
                ._get_range_repair_bootstrap_service()
            ),
            strategy_range_speed_history_provider=lambda: (
                self.recovery._strategy_range_speed_history_provider()
            ),
            on_range_bar_persist_error=lambda *a, **k: (
                self.closed_bar._on_range_bar_persist_error(*a, **k)
            ),
            on_completed_range_aggregate_persist_error=lambda *a, **k: (
                self.closed_bar
                ._on_completed_range_aggregate_persist_error(*a, **k)
            ),
            on_live_persistence_write_rejected=lambda *a, **k: (
                self.persistence._on_live_persistence_write_rejected(*a, **k)
            ),
        )
        self.wiring = WiringComponent(runtime_context, wiring_ports)
        self.wiring.initialize(*args, **kwargs)
        self.lifecycle = LifecycleComponent(runtime_context)
        self.market_events = MarketEventsComponent(runtime_context)
        self.closed_bar = ClosedBarComponent(runtime_context)
        self.signal_execution = SignalExecutionComponent(runtime_context)
        self.account_runtime = AccountComponent(runtime_context)
        self.recovery = RecoveryComponent(runtime_context)
        self.startup = StartupComponent(runtime_context)
        self.catchup = CatchupComponent(runtime_context)
        self.order_results = OrderResultsComponent(runtime_context)
        self.persistence = PersistenceComponent(runtime_context)
        self.market_data_lifecycle = RangeRuntimeComponent(runtime_context)
        self._runtime_components = {
            WiringComponent: self.wiring,
            LifecycleComponent: self.lifecycle,
            MarketEventsComponent: self.market_events,
            ClosedBarComponent: self.closed_bar,
            SignalExecutionComponent: self.signal_execution,
            AccountComponent: self.account_runtime,
            RecoveryComponent: self.recovery,
            StartupComponent: self.startup,
            CatchupComponent: self.catchup,
            OrderResultsComponent: self.order_results,
            PersistenceComponent: self.persistence,
            RangeRuntimeComponent: self.market_data_lifecycle,
        }
        configure_runtime_composition(self, self.wiring, runtime_context)

    def _runtime_service_bundle(self) -> RuntimeServiceBundle:
        return self.service_bundle

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
        self._market_data_runtime = runtime
        self._market_data_capabilities = capabilities
        self._market_modules_managed = True

    async def enqueue_market_event(self, event: MarketEvent) -> None:
        await self.market_events._enqueue_market_event(event)

    async def handle_dropped_trade(self, event: MarketEvent) -> None:
        await self.market_events._handle_market_data_trade_drop(event)

    async def _prepare_market_data_modules(self) -> None:
        market_state = self.runtime_state.market
        if market_state.runtime is not None:
            await market_state.runtime.prepare(market_state.capabilities)

    async def _start_market_data_modules(self) -> None:
        runtime = self.runtime_state.market.runtime
        if runtime is not None:
            await runtime.start_prepared()

    async def run(
        self,
        *,
        max_market_events: int | None = None,
    ) -> LiveRuntimeStats:
        logger.info(
            "Live runtime starting | symbol=%s strategy=%s exchanges=%s "
            "data_exchange=%s dry_run=%s max_market_events=%s",
            self.app_config.symbol,
            self.app_config.strategy,
            ",".join(exchange.value for exchange in self.app_config.exchanges),
            self.app_config.data_exchange.value,
            self.app_config.dry_run,
            max_market_events,
        )
        logger.info(
            "Market queue settings | maxsize=%s backlog_warn_threshold=%s "
            "drain_batch_size=%s full_alert_cooldown_seconds=%s",
            self._market_queue.maxsize,
            self._market_queue_backlog_warn_threshold,
            self._market_queue_drain_batch_size,
            300,
        )
        self.app_context.alerts.start()
        try:
            await self._prepare_market_data_modules()
            await self.lifecycle._run_startup_sequence()
            await self._start_market_data_modules()
            self.lifecycle._start_producers()
            self.lifecycle._start_sync_tasks()
            await self.market_events._consume_market_events(
                max_market_events=max_market_events
            )
            self.lifecycle._set_health(
                RuntimePhase.STOPPED,
                healthy=self.lifecycle._health.healthy,
            )
            logger.info("Live runtime stopped | stats=%s", self.stats)
            return self.stats
        except Exception as exc:
            self.stats.errors += 1
            self.lifecycle._set_health(
                RuntimePhase.ERROR,
                healthy=False,
                error=str(exc),
            )
            logger.exception("Live runtime error")
            await self._emit_fatal_runtime_alert(exc)
            raise
        finally:
            await self._run_finally_shutdown()

    async def _emit_fatal_runtime_alert(self, exc: Exception) -> None:
        fatal_alert = AppAlert(
            subject="AetherEdge live runtime error",
            content=str(exc),
            severity="error",
        )
        queued = self.app_context.alerts.emit(fatal_alert)
        await _flush_fatal_runtime_alert(self.app_context.alerts)
        if queued:
            return
        if self.app_context.alerts.emit(fatal_alert):
            await _flush_fatal_runtime_alert(self.app_context.alerts)
            return
        queue = getattr(self.app_context.alerts, "_queue", None)
        logger.critical(
            "Fatal runtime alert could not be queued after bounded drain | "
            "subject=%s queue_size=%s maxsize=%s sent=%s failed=%s dropped=%s",
            fatal_alert.subject,
            queue.qsize() if queue is not None else "unknown",
            getattr(queue, "maxsize", "unknown"),
            getattr(self.app_context.alerts, "sent", "unknown"),
            getattr(self.app_context.alerts, "failed", "unknown"),
            getattr(self.app_context.alerts, "dropped", "unknown"),
        )

    async def start(self) -> RuntimeHealth:
        self.lifecycle._set_health(
            RuntimePhase.RUNNING,
            healthy=True,
            warmup_complete=True,
            caught_up=True,
        )
        return self._health

    async def stop(self) -> RuntimeHealth:
        self._stop_event.set()
        await self._explicit_stop_shutdown()
        self.lifecycle._set_health(RuntimePhase.STOPPED, healthy=True)
        return self._health

    async def _run_finally_shutdown(self) -> None:
        await self._shutdown_coordinator.execute(
            (
                self.market_data_lifecycle._stop_market_data_modules,
                self.lifecycle._stop_sync_tasks,
                self.lifecycle._stop_producers,
                self.persistence._stop_live_persistence_writer,
                self.app_context.alerts.stop,
            )
        )

    async def _explicit_stop_shutdown(self) -> None:
        coordinator = getattr(
            self,
            "_shutdown_coordinator",
            RuntimeShutdownCoordinator,
        )
        await coordinator.execute(
            (
                self.market_data_lifecycle._stop_market_data_modules,
                self.lifecycle._stop_producers,
                self.persistence._stop_live_persistence_writer,
            )
        )

    async def health(self) -> RuntimeHealth:
        return self._health

    async def process_market_event(self, event: MarketEvent) -> None:
        await self.market_events._process_market_event(event)

    async def process_market_feature(self, event: MarketFeatureEvent) -> None:
        await self.market_events._process_market_feature_event(event)

    async def process_account_event(self, event: AccountEvent) -> None:
        await self.account_runtime._process_account_event(event)

    async def _startup(self) -> None:
        await self.lifecycle._run_startup_sequence()


__all__ = [
    "LiveRuntimeRunner",
    "LiveRuntimeStats",
    "LiveRuntimeError",
    "_is_fatal_startup_error",
]
