from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol

from src.runtime.module import CapabilityId
from src.runtime.module import ModuleHealth, ModuleHost
from src.runtime.registry import DependencyResolver, ModuleRegistry, RuntimePlan


class RuntimeLog(Protocol):
    def info(self, message: str, *args: object) -> None: ...


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
        except BaseException:
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
        await host.start()
        plan = self._plan
        assert plan is not None
        self._log("Started modules | modules=%s", plan.module_ids)

    async def stop(self) -> None:
        host = self._host
        self._host = None
        self._plan = None
        if host is not None:
            await host.stop()

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


__all__ = ["MarketDataRuntime", "MarketDataRuntimeState", "RuntimeLog"]
