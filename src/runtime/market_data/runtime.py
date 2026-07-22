from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from src.runtime.module import CapabilityId
from src.runtime.module import ModuleHealth, ModuleHost
from src.runtime.registry import DependencyResolver, ModuleRegistry, RuntimePlan
from src.runtime.market_data.pipeline_plan import ResolvedMarketPipelinePlan
from src.runtime.market_data.processor import MarketEventProcessor


class RuntimeLog(Protocol):
    def info(self, message: str, *args: object) -> None: ...

    def error(self, message: str, *args: object) -> None: ...


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
        logger: RuntimeLog | None = None,
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
                    else self._pipeline_plan.enabled_module_ids
                )
                if module_id in by_id
            )
            self._event_processor.set_trade_modules(ordered)
        host = ModuleHost(modules)
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
            if self._event_processor is not None:
                await self._event_processor.start()
            await host.start()
        except BaseException as exc:
            self._error = exc
            if self._event_processor is not None:
                await self._event_processor.stop()
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
        self._supervisor_stop.set()
        supervisor = self._supervisor_task
        self._supervisor_task = None
        try:
            if host is not None:
                await host.stop()
        finally:
            if self._event_processor is not None:
                await self._event_processor.stop()
            if supervisor is not None:
                await asyncio.gather(supervisor, return_exceptions=True)

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
    "RuntimeLog",
]
