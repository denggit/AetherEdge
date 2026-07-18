from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Callable, Mapping, Sequence
from src.market_data.events import MarketFeatureEvent
from src.runtime.models import RuntimeHealth, RuntimeMode, RuntimePhase
from src.runtime.startup_feature_backfill import (
    resolve_startup_feature_backfill_providers,
)
from src.runtime.sync_lifecycle import RuntimeSyncLifecycle, SyncTaskFactory
from src.runtime.startup_phase_coordinator import RuntimeStartupPhasePlan

from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent


class LifecycleComponent(RuntimeComponent):
    async def _run_startup_sequence(self) -> None:
        self._strategy_capabilities()
        logger.info("Live runtime startup phase started")
        await self._startup_phase_coordinator.execute(
            RuntimeStartupPhasePlan(
                initialize_rangebar_trust_window=(
                    self._initialize_rangebar_trust_window
                ),
                enter_warming_up=self._enter_startup_warming_up,
                bootstrap_account_config=(
                    self._bootstrap_account_config_if_enabled
                ),
                check_position_mode=(
                    self._check_strategy_position_mode_requirements
                ),
                run_warmup=self._run_warmup,
                warmup_range_speed_history=self._warmup_range_speed_history,
                handle_range_speed_history_result=(
                    self._handle_startup_range_speed_history_result
                ),
                check_feature_backfills=(
                    self._check_startup_feature_backfills
                ),
                enter_catching_up=self._enter_startup_catching_up,
                run_recovery=self._run_recovery,
                run_post_recovery_checks=(
                    self._run_startup_post_recovery_checks
                ),
                run_reconciliation=self._run_reconciliation,
                call_strategy_on_start=self._call_on_start,
                evaluate_startup_catchup=self._evaluate_startup_catchup_once,
                finish_range_speed_warmup=(
                    self._finish_range_speed_warmup_after_catchup
                ),
                start_heartbeat=self._start_runtime_heartbeat,
                start_range_speed_background_services=(
                    self._start_range_speed_background_services
                ),
                enter_running=self._enter_startup_running,
            )
        )
        logger.info("Live runtime startup phase completed")

    def _enter_startup_warming_up(self) -> None:
        self._set_health(RuntimePhase.WARMING_UP, healthy=True)

    def _enter_startup_catching_up(self) -> None:
        self._set_health(
            RuntimePhase.CATCHING_UP,
            healthy=True,
            warmup_complete=True,
        )

    def _enter_startup_running(self) -> None:
        self._set_health(
            RuntimePhase.RUNNING,
            healthy=True,
            warmup_complete=True,
            caught_up=True,
        )

    def _handle_startup_range_speed_history_result(
        self,
        loaded_range_speed_history: int,
    ) -> None:
        warmup = getattr(self, "_range_speed_warmup", None)
        if warmup is not None:
            warmup.warn_if_insufficient(loaded_range_speed_history)

    async def _run_startup_post_recovery_checks(
        self,
        snapshots: tuple[object, ...],
    ) -> None:
        if self._account_config_new_entries_blocked:
            await self._recheck_account_config_after_recovery()

    def _start_runtime_heartbeat(self) -> None:
        self._heartbeat_service.start(
            runtime_id=f"{self.app_config.strategy}::{self.app_config.symbol}",
        )

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

    def _start_producers(self) -> list[asyncio.Task]:
        tasks: list[asyncio.Task] = []
        if (
            not getattr(self, "_market_modules_managed", False)
            and self.requirements.trades.enabled
            and self.requirements.trades.stream_enabled
        ):
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
        if (
            not getattr(self, "_market_modules_managed", False)
            and self.requirements.order_book.enabled
            and self.requirements.order_book.stream_enabled
        ):
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
        task_factories: list[SyncTaskFactory] = []
        if self.requirements.account_state.poll_enabled:
            task_factories.append(
                lambda: self._get_account_sync_service().run_periodic(
                    self._stop_event
                )
            )
        if self.requirements.order_state.poll_when_position_enabled:
            task_factories.append(
                lambda: self._get_order_sync_service().run_periodic(
                    self._stop_event
                )
            )
            task_factories.append(
                lambda: self._periodic_follower_close_check(self._stop_event)
            )
        # Heartbeat periodic task
        task_factories.append(
            lambda: self._heartbeat_service.run_periodic(self._stop_event)
        )
        if self._get_startup_feature_backfill_providers():
            task_factories.append(
                lambda: self._periodic_feature_readiness_refresh(
                    self._stop_event
                )
            )
        tasks = self._sync_lifecycle.start(task_factories)
        self._sync_tasks = tasks
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

    async def _stop_producers(self) -> None:
        for task in self._producer_tasks:
            task.cancel()
        if self._producer_tasks:
            await asyncio.gather(*self._producer_tasks, return_exceptions=True)
        self._producer_tasks = []

    async def _stop_sync_tasks(self) -> None:
        await self._sync_lifecycle.stop()
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
        self._health = self._runtime_health_state.update(
            phase,
            healthy=healthy,
            warmup_complete=warmup_complete,
            caught_up=caught_up,
            last_market_event_time_ms=last_market_event_time_ms,
            error=error,
            metadata=metadata,
        )
