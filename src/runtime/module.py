from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, order=True)
class CapabilityId:
    """Stable public identifier used to connect module outputs and inputs."""

    value: str

    def __post_init__(self) -> None:
        normalized = self.value.strip().lower()
        if not normalized or "." not in normalized:
            raise ValueError(
                "capability id must be a non-empty dotted identifier"
            )
        object.__setattr__(self, "value", normalized)

    def __str__(self) -> str:
        return self.value


class ModuleState(str, Enum):
    CREATED = "created"
    PREPARED = "prepared"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass(frozen=True)
class ModuleHealth:
    module_id: str
    state: ModuleState
    healthy: bool = True
    detail: str | None = None
    background_tasks: int = 0
    metadata: tuple[tuple[str, str], ...] = ()


@runtime_checkable
class RuntimeModule(Protocol):
    module_id: str
    provides: frozenset[CapabilityId]
    requires: frozenset[CapabilityId]

    async def prepare(self) -> None: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    def health(self) -> ModuleHealth: ...


@runtime_checkable
class WarmupCapable(Protocol):
    async def warmup(self) -> None: ...


@runtime_checkable
class RepairCapable(Protocol):
    async def repair(self) -> None: ...


class ModuleLifecycleError(RuntimeError):
    pass


HealthObserver = Callable[[ModuleHealth], None]


@dataclass
class ModuleHost:
    """Own the lifecycle of an already resolved, unique module sequence."""

    modules: Sequence[RuntimeModule]
    observe_health: HealthObserver | None = None
    _prepared: list[RuntimeModule] = field(default_factory=list, init=False)
    _started: list[RuntimeModule] = field(default_factory=list, init=False)

    async def prepare(self) -> None:
        if self._prepared:
            return
        try:
            for module in self.modules:
                await module.prepare()
                self._prepared.append(module)
                self._observe(module)
                if isinstance(module, WarmupCapable):
                    await module.warmup()
                if isinstance(module, RepairCapable):
                    await module.repair()
        except BaseException as exc:
            await self._raise_startup_error(module, exc)

    async def start(self) -> None:
        if not self._prepared:
            await self.prepare()
        try:
            for module in self.modules:
                await module.start()
                self._started.append(module)
                self._observe(module)
        except BaseException as exc:
            await self._raise_startup_error(module, exc)

    async def stop(self) -> None:
        errors: list[str] = []
        for module in reversed(self._prepared):
            try:
                await module.stop()
                self._observe(module)
            except BaseException as exc:
                errors.append(
                    f"{module.module_id}={type(exc).__name__}: {exc}"
                )
        self._started.clear()
        self._prepared.clear()
        if errors:
            raise ModuleLifecycleError(
                "module shutdown failed | " + " | ".join(errors)
            )

    def health(self) -> tuple[ModuleHealth, ...]:
        return tuple(module.health() for module in self._started)

    @property
    def started_module_ids(self) -> tuple[str, ...]:
        return tuple(module.module_id for module in self._started)

    def _observe(self, module: RuntimeModule) -> None:
        if self.observe_health is not None:
            self.observe_health(module.health())

    async def _raise_startup_error(
        self,
        module: RuntimeModule,
        exc: BaseException,
    ) -> None:
        shutdown_error: BaseException | None = None
        try:
            await self.stop()
        except BaseException as stop_exc:
            shutdown_error = stop_exc
        shutdown_detail = (
            ""
            if shutdown_error is None
            else " | shutdown_error="
            f"{type(shutdown_error).__name__}: {shutdown_error}"
        )
        raise ModuleLifecycleError(
            f"module startup failed | module={module.module_id} | "
            f"error={type(exc).__name__}: {exc}{shutdown_detail}"
        ) from exc


async def run_lifecycle_step(step: Callable[[], Awaitable[None]]) -> None:
    """Typed callback adapter for small lifecycle-only orchestration steps."""

    await step()


__all__ = [
    "CapabilityId",
    "ModuleHealth",
    "ModuleHost",
    "ModuleLifecycleError",
    "ModuleState",
    "RepairCapable",
    "RuntimeModule",
    "WarmupCapable",
]
