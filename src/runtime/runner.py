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
    RecoveryComponent,
    SignalExecutionComponent,
    StartupComponent,
    WiringComponent,
)
from src.runtime.components.base import RuntimeSharedState
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


def _compatibility_component_methods() -> dict[str, type]:
    methods: dict[str, type] = {}
    for component_type in COMPONENT_TYPES:
        for name, value in component_type.__dict__.items():
            if name == "initialize" or not (
                callable(value) or isinstance(value, property)
            ):
                continue
            existing = methods.get(name)
            if existing is not None:
                raise RuntimeError(
                    "runtime component method conflict | "
                    f"method={name} components="
                    f"{existing.__name__},{component_type.__name__}"
                )
            methods[name] = component_type
    return methods


_COMPATIBILITY_COMPONENT_METHODS = _compatibility_component_methods()


class _ComponentMethod:
    """Explicit class/instance descriptor for a small legacy surface."""

    def __init__(self, component_type: type, name: str) -> None:
        self.component_type = component_type
        self.name = name

    def __get__(self, instance, owner=None):
        if instance is None:
            return getattr(self.component_type, self.name)
        components = instance._ensure_runtime_components()
        return getattr(components[self.component_type], self.name)


class _RunnerCompatibilityFacade:
    """Legacy test/integration surface; formal orchestration uses named parts."""

    def _ensure_runtime_state(self) -> RuntimeSharedState:
        state = self.__dict__.get("_runtime_state")
        if state is None:
            state = RuntimeSharedState()
            object.__setattr__(self, "_runtime_state", state)
        return state

    def _ensure_runtime_components(self):
        components = self.__dict__.get("_runtime_components")
        if components is None:
            components = {
                component_type: component_type(self)
                for component_type in COMPONENT_TYPES
            }
            object.__setattr__(self, "_runtime_components", components)
        return components

    def _compat_override(self, name: str, default):
        return self.__dict__.get(name, default)

    def _runtime_component_override(self, name: str):
        if name in self.__dict__:
            return True, self.__dict__[name]
        return False, None

    def _named_component(self, name: str, component_type: type):
        component = self.__dict__.get(name)
        if component is not None:
            return component
        return self._ensure_runtime_components()[component_type]

    def __getattr__(self, name: str):
        state_values = self._ensure_runtime_state().__dict__
        if name in state_values:
            return state_values[name]
        component_type = _COMPATIBILITY_COMPONENT_METHODS.get(name)
        if component_type is None:
            raise AttributeError(name)
        return getattr(self._ensure_runtime_components()[component_type], name)

    def __setattr__(self, name: str, value: object) -> None:
        state = self._ensure_runtime_state()
        if name == "_market_data_runtime":
            state.market.runtime = value  # type: ignore[assignment]
        elif name == "_market_data_capabilities":
            state.market.capabilities = value  # type: ignore[assignment]
        elif name == "_market_modules_managed":
            state.market.modules_managed = bool(value)
        elif name == "_pipeline_integrity_error":
            state.market.integrity_error = value  # type: ignore[assignment]
        component_type = _COMPATIBILITY_COMPONENT_METHODS.get(name)
        descriptor = (
            None if component_type is None else component_type.__dict__.get(name)
        )
        if isinstance(descriptor, property) and descriptor.fset is not None:
            setattr(self._ensure_runtime_components()[component_type], name, value)
            return
        if (
            component_type is not None
            or name in type(self).__dict__
            or name in {"_runtime_state", "_runtime_components"}
        ):
            object.__setattr__(self, name, value)
            return
        setattr(self._ensure_runtime_state(), name, value)

    _log_4h_decision_summary = _ComponentMethod(
        ClosedBarComponent, "_log_4h_decision_summary"
    )
    _check_startup_feature_backfills = _ComponentMethod(
        LifecycleComponent, "_check_startup_feature_backfills"
    )
    _check_strategy_position_mode_requirements = _ComponentMethod(
        StartupComponent, "_check_strategy_position_mode_requirements"
    )
    _execute_signals = _ComponentMethod(
        SignalExecutionComponent, "_execute_signals"
    )
    _build_signal_feedback_request = _ComponentMethod(
        SignalExecutionComponent, "_build_signal_feedback_request"
    )
    _bootstrap_account_config_if_enabled = _ComponentMethod(
        StartupComponent, "_bootstrap_account_config_if_enabled"
    )
    _get_recovery_service = _ComponentMethod(
        RecoveryComponent, "_get_recovery_service"
    )
    _get_sync_contexts = _ComponentMethod(
        AccountComponent, "_get_sync_contexts"
    )
    _get_position_plan_store = _ComponentMethod(
        PersistenceComponent, "_get_position_plan_store"
    )


class LiveRuntimeRunner(_RunnerCompatibilityFacade):
    """Thin lifecycle orchestrator over typed domain runtime components."""

    def __init__(self, *args, **kwargs) -> None:
        object.__setattr__(self, "_runtime_state", RuntimeSharedState())
        components = {
            component_type: component_type(self)
            for component_type in COMPONENT_TYPES
        }
        object.__setattr__(self, "_runtime_components", components)
        object.__setattr__(self, "wiring", components[WiringComponent])
        object.__setattr__(self, "lifecycle", components[LifecycleComponent])
        object.__setattr__(self, "market_events", components[MarketEventsComponent])
        object.__setattr__(self, "closed_bar", components[ClosedBarComponent])
        object.__setattr__(self, "signal_execution", components[SignalExecutionComponent])
        object.__setattr__(self, "account_runtime", components[AccountComponent])
        object.__setattr__(self, "recovery", components[RecoveryComponent])
        object.__setattr__(self, "startup", components[StartupComponent])
        object.__setattr__(self, "catchup", components[CatchupComponent])
        object.__setattr__(self, "order_results", components[OrderResultsComponent])
        object.__setattr__(self, "persistence", components[PersistenceComponent])
        object.__setattr__(self, "market_data_lifecycle", components[COMPONENT_TYPES[-1]])
        self.wiring.initialize(*args, **kwargs)

    def attach_market_data_runtime(
        self,
        runtime: MarketDataRuntime,
        capabilities: frozenset[CapabilityId],
    ) -> None:
        market_state = self._ensure_runtime_state().market
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
        market_state = self._ensure_runtime_state().market
        runtime = market_state.runtime
        if runtime is not None:
            await runtime.prepare(market_state.capabilities)

    async def _start_market_data_modules(self) -> None:
        runtime = self._ensure_runtime_state().market.runtime
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
            )(RuntimePhase.STOPPED, healthy=True)
            logger.info("Live runtime stopped | stats=%s", self.stats)
            return self.stats
        except Exception as exc:
            self.stats.errors += 1
            self._compat_override(
                "_set_health",
                self.lifecycle._set_health,
            )(RuntimePhase.ERROR, healthy=False, error=str(exc))
            logger.exception("Live runtime error")
            self.context.alerts.emit(AppAlert(subject="AetherEdge live runtime error", content=str(exc), severity="error"))
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
