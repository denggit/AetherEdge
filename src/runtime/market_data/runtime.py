from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from src.runtime.module import CapabilityId
from src.runtime.module import ModuleHealth, ModuleHost
from src.runtime.registry import DependencyResolver, ModuleRegistry, RuntimePlan
from src.runtime.market_data.dispatcher import DispatcherBarrierResult


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
    ) -> None:
        self._registry = registry
        self._resolver = DependencyResolver(registry)
        self._logger = logger
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
            await host.start()
        except BaseException as exc:
            self._error = exc
            self._host = None
            self._plan = None
            raise
        self._supervisor_stop.clear()
        self._failure_event.clear()
        self._supervisor_task = asyncio.create_task(
            self._supervise(),
            name="market-data-supervisor",
        )
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
            if supervisor is not None:
                await asyncio.gather(supervisor, return_exceptions=True)

    async def drain_through(
        self,
        cutoff_time_ms: int,
        *,
        timeout_seconds: float,
    ) -> DispatcherBarrierResult:
        host = self._host
        if host is None:
            return DispatcherBarrierResult(
                cutoff_time_ms=cutoff_time_ms,
                pending=0,
                completed=True,
            )
        for module in host.modules:
            drain = getattr(module, "drain_through", None)
            if callable(drain):
                result = await drain(
                    cutoff_time_ms,
                    timeout_seconds=timeout_seconds,
                )
                if result is not None:
                    return result
        return DispatcherBarrierResult(
            cutoff_time_ms=cutoff_time_ms,
            pending=0,
            completed=True,
        )

    def raise_if_failed(self) -> None:
        if self._error is not None:
            raise MarketDataRuntimeError(
                "market data runtime failed | "
                f"error={type(self._error).__name__}: {self._error}"
            ) from self._error
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
