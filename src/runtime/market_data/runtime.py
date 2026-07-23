from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from src.runtime.module import CapabilityId
from src.runtime.module import ModuleHealth, ModuleHost
from src.runtime.registry import DependencyResolver, ModuleRegistry, RuntimePlan
from src.runtime.market_data.pipeline_plan import ResolvedMarketPipelinePlan
from src.runtime.market_data.processor import MarketEventProcessor


class MarketDataRuntimeError(RuntimeError):
    pass


@dataclass(frozen=True)
class MarketDataRuntimeState:
    plan: RuntimePlan | None
    started_module_ids: tuple[str, ...]
    health: tuple[ModuleHealth, ...]


class MarketDataRuntime:
    """Resolve and own the one demand-driven market-data module graph."""

    def __init__(
        self,
        *,
        registry: ModuleRegistry,
        logger: Any | None = None,
        event_processor: MarketEventProcessor | None = None,
        pipeline_plan: ResolvedMarketPipelinePlan | None = None,
    ) -> None:
        self._registry = registry
        self._resolver = DependencyResolver(registry)
        self._logger = logger
        self._event_processor = event_processor
        self._pipeline_plan = pipeline_plan
        self._plan: RuntimePlan | None = None
        self._host: ModuleHost | None = None
        self._consumer_modules = ()
        self._source_modules = ()
        self._error: BaseException | None = None
        self._supervisor_stop = asyncio.Event()
        self._supervisor_task: asyncio.Task[None] | None = None
        self._failure_event = asyncio.Event()

    def plan(
        self,
        requested: Iterable[CapabilityId | str],
    ) -> RuntimePlan:
        return self._resolver.resolve(requested)

    async def start(
        self,
        requested: Iterable[CapabilityId | str] | RuntimePlan,
    ) -> RuntimePlan:
        plan = await self.prepare(requested)
        await self.start_prepared()
        return plan

    async def prepare(
        self,
        requested: Iterable[CapabilityId | str] | RuntimePlan,
    ) -> RuntimePlan:
        if self._host is not None:
            raise RuntimeError("market data runtime is already prepared")
        plan = (
            requested
            if isinstance(requested, RuntimePlan)
            else self.plan(requested)
        )
        modules = self._registry.instantiate(plan)
        if self._event_processor is not None:
            by_id = {
                module.module_id: module
                for module in modules
                if callable(getattr(module, "process_trade", None))
            }
            ordered = tuple(
                by_id[module_id]
                for module_id in (
                    ()
                    if self._pipeline_plan is None
                    else self._pipeline_plan.ordered_trade_module_ids
                )
                if module_id in by_id
            )
            self._event_processor.set_trade_modules(ordered)
        host = ModuleHost(modules)
        source_by_id = {module.module_id: module for module in modules}
        self._source_modules = tuple(
            source_by_id[module_id]
            for module_id in ("trade-stream", "order-book-stream")
            if module_id in source_by_id
        )
        self._consumer_modules = tuple(
            module for module in modules if module not in self._source_modules
        )
        self._log_plan(plan)
        try:
            await host.prepare()
        except BaseException as exc:
            self._error = exc
            self._plan = None
            self._host = None
            raise
        self._plan = plan
        self._host = host
        return plan

    async def start_prepared(self) -> None:
        host = self._host
        if host is None:
            raise RuntimeError("market data runtime is not prepared")
        try:
            await host.start(self._consumer_modules)
            if self._event_processor is not None:
                await self._event_processor.start()
            await host.start(self._source_modules)
        except BaseException as exc:
            self._error = exc
            if self._event_processor is not None:
                try:
                    await self._event_processor.stop()
                except BaseException:
                    pass
            await host.stop()
            self._host = None
            self._plan = None
            raise
        self._supervisor_stop.clear()
        self._failure_event.clear()
        if host.modules or self._event_processor is not None:
            self._supervisor_task = asyncio.create_task(
                self._supervise(),
                name="market-data-supervisor",
            )
        else:
            self._supervisor_task = None
        plan = self._plan
        assert plan is not None
        self._log("Started modules | modules=%s", plan.module_ids)

    async def stop(self) -> None:
        host = self._host
        self._host = None
        self._plan = None
        supervisor = self._supervisor_task
        self._supervisor_task = None
        errors: list[BaseException] = []
        processor = self._event_processor
        controls_blocked_separately = False
        if processor is not None:
            stop_controls = getattr(processor, "stop_accepting_controls", None)
            if callable(stop_controls):
                stop_controls()
                controls_blocked_separately = True
            else:
                processor.stop_accepting()
        try:
            if host is not None:
                try:
                    await host.stop(self._source_modules)
                except BaseException as exc:
                    errors.append(exc)
            if processor is not None:
                if controls_blocked_separately:
                    processor.stop_accepting()
                try:
                    await processor.stop()
                except BaseException as exc:
                    errors.append(exc)
            if host is not None:
                try:
                    await host.stop()
                except BaseException as exc:
                    errors.append(exc)
        finally:
            self._supervisor_stop.set()
            if supervisor is not None:
                await asyncio.gather(supervisor, return_exceptions=True)
        if errors:
            self._error = errors[0]
            detail = " | ".join(f"{type(exc).__name__}: {exc}" for exc in errors)
            raise MarketDataRuntimeError(f"market data shutdown failed | {detail}") from errors[0]

    def raise_if_failed(self) -> None:
        if self._error is not None:
            raise MarketDataRuntimeError(
                "market data runtime failed | "
                f"error={type(self._error).__name__}: {self._error}"
            ) from self._error
        if self._event_processor is not None:
            try:
                self._event_processor.raise_if_failed()
            except BaseException as exc:
                self._error = exc
                raise MarketDataRuntimeError(
                    "market event processor failed | "
                    f"error={type(exc).__name__}: {exc}"
                ) from exc
        host = self._host
        if host is None:
            return
        unhealthy = tuple(item for item in host.health() if not item.healthy)
        if not unhealthy:
            return
        detail = "; ".join(
            f"{item.module_id}:{item.state.value}:{item.detail}"
            for item in unhealthy
        )
        error = MarketDataRuntimeError(
            f"market data module unhealthy | {detail}"
        )
        self._error = error
        if self._logger is not None:
            self._logger.error("Market data runtime failed | %s", detail)
        raise error

    async def wait_failed(self) -> None:
        await self._failure_event.wait()
        self.raise_if_failed()

    async def _supervise(self) -> None:
        while not self._supervisor_stop.is_set():
            try:
                await asyncio.wait_for(
                    self._supervisor_stop.wait(),
                    timeout=0.05,
                )
                continue
            except TimeoutError:
                pass
            try:
                self.raise_if_failed()
            except MarketDataRuntimeError:
                self._failure_event.set()
                return

    def state(self) -> MarketDataRuntimeState:
        host = self._host
        return MarketDataRuntimeState(
            plan=self._plan,
            started_module_ids=(
                () if host is None else host.started_module_ids
            ),
            health=(() if host is None else host.health()),
        )

    @property
    def supervisor_task(self) -> asyncio.Task[None] | None:
        return self._supervisor_task

    def _log_plan(self, plan: RuntimePlan) -> None:
        self._log(
            "Requested capabilities | capabilities=%s",
            tuple(str(value) for value in sorted(plan.requested)),
        )
        self._log(
            "Resolved dependencies | capabilities=%s modules=%s",
            tuple(str(value) for value in sorted(plan.resolved)),
            plan.module_ids,
        )
        self._log("Skipped modules | modules=%s", plan.skipped_module_ids)
        self._log(
            "Shared modules | capabilities=%s",
            tuple(str(value) for value in sorted(plan.shared_capabilities)),
        )

    def _log(self, message: str, *args: object) -> None:
        if self._logger is not None:
            self._logger.info(message, *args)


__all__ = [
    "MarketDataRuntime",
    "MarketDataRuntimeError",
    "MarketDataRuntimeState",
]
