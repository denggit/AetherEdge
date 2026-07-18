from __future__ import annotations

from src.app.alerts import AppAlert
from src.market_data.events import MarketFeatureEvent
from src.platform.account.events import AccountEvent
from src.platform.data.models import MarketEvent
from src.runtime.models import RuntimeHealth, RuntimeMode, RuntimePhase
from src.runtime.market_data.runtime import MarketDataRuntime
from src.runtime.module import CapabilityId
from src.runtime.shutdown_coordinator import RuntimeShutdownCoordinator

from src.runtime.components import COMPONENT_TYPES
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


_METHOD_COMPONENTS = {
    name: component_type
    for component_type in COMPONENT_TYPES
    for name, value in component_type.__dict__.items()
    if name != "initialize" and (callable(value) or isinstance(value, property))
}
_COMPONENT_CLASS_PATCH_DEFAULTS: dict[str, object] = {}


class _RunnerMeta(type):
    def __getattr__(cls, name: str):
        component_type = _METHOD_COMPONENTS.get(name)
        if component_type is None:
            raise AttributeError(name)
        return getattr(component_type, name)

    def __setattr__(cls, name: str, value: object) -> None:
        component_type = _METHOD_COMPONENTS.get(name)
        if component_type is not None and name not in cls.__dict__:
            # Preserve class-level patching used by operational smoke tests
            # and legacy integrations after method ownership moved to typed
            # components.
            _COMPONENT_CLASS_PATCH_DEFAULTS.setdefault(
                name,
                getattr(component_type, name),
            )
            setattr(component_type, name, value)
            return
        super().__setattr__(name, value)

    def __delattr__(cls, name: str) -> None:
        component_type = _METHOD_COMPONENTS.get(name)
        original = _COMPONENT_CLASS_PATCH_DEFAULTS.pop(name, None)
        if component_type is not None and original is not None:
            setattr(component_type, name, original)
            return
        super().__delattr__(name)


class LiveRuntimeRunner(metaclass=_RunnerMeta):
    """Thin lifecycle orchestrator over typed domain runtime components."""

    def __init__(self, *args, **kwargs) -> None:
        components = self._ensure_runtime_components()
        wiring_type = COMPONENT_TYPES[0]
        components[wiring_type].initialize(*args, **kwargs)

    def _ensure_runtime_components(self):
        components = self.__dict__.get("_runtime_components")
        if components is None:
            components = {component_type: component_type(self) for component_type in COMPONENT_TYPES}
            object.__setattr__(self, "_runtime_components", components)
        return components

    def __getattr__(self, name: str):
        component_type = _METHOD_COMPONENTS.get(name)
        if component_type is None:
            raise AttributeError(name)
        components = self._ensure_runtime_components()
        return getattr(components[component_type], name)

    def __setattr__(self, name: str, value: object) -> None:
        components = self.__dict__.get("_runtime_components")
        component_type = _METHOD_COMPONENTS.get(name)
        descriptor = (
            None if component_type is None else component_type.__dict__.get(name)
        )
        if isinstance(descriptor, property) and descriptor.fset is not None:
            components = self._ensure_runtime_components()
            setattr(components[component_type], name, value)
            return
        object.__setattr__(self, name, value)

    def attach_market_data_runtime(
        self,
        runtime: MarketDataRuntime,
        capabilities: frozenset[CapabilityId],
    ) -> None:
        if self._market_data_runtime is not None:
            raise RuntimeError("market data runtime is already attached")
        self._market_data_runtime = runtime
        self._market_data_capabilities = capabilities
        self._market_modules_managed = True

    async def enqueue_market_event(self, event: MarketEvent) -> None:
        await self._enqueue_market_event(event)

    async def _prepare_market_data_modules(self) -> None:
        runtime = getattr(self, "_market_data_runtime", None)
        if runtime is not None:
            await runtime.prepare(self._market_data_capabilities)

    async def _start_market_data_modules(self) -> None:
        runtime = getattr(self, "_market_data_runtime", None)
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
            await self._startup()
            await self._start_market_data_modules()
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
            await self._run_finally_shutdown()

    async def start(self) -> RuntimeHealth:
        self._set_health(RuntimePhase.RUNNING, healthy=True, warmup_complete=True, caught_up=True)
        return self._health

    async def stop(self) -> RuntimeHealth:
        self._stop_event.set()
        await self._explicit_stop_shutdown()
        self._set_health(RuntimePhase.STOPPED, healthy=True)
        return self._health

    async def _run_finally_shutdown(self) -> None:
        await self._shutdown_coordinator.execute(
            (
                self._stop_market_data_modules,
                self._stop_sync_tasks,
                self._stop_producers,
                self._stop_live_persistence_writer,
                self.context.alerts.stop,
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
                self._stop_market_data_modules,
                self._stop_producers,
                self._stop_live_persistence_writer,
            )
        )

    async def health(self) -> RuntimeHealth:
        return self._health

    async def process_market_event(self, event: MarketEvent) -> None:
        await self._process_market_event(event)

    async def process_market_feature(self, event: MarketFeatureEvent) -> None:
        await self._process_market_feature_event(event)

    async def process_account_event(self, event: AccountEvent) -> None:
        await self._process_account_event(event)

    async def _startup(self) -> None:
        await self._run_startup_sequence()

__all__ = ["LiveRuntimeRunner", "LiveRuntimeStats", "LiveRuntimeError", "_is_fatal_startup_error"]
