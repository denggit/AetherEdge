from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.runtime.module import CapabilityId
from src.runtime.services import RuntimeServices

if TYPE_CHECKING:
    from src.runtime.market_data.runtime import MarketDataRuntime


@dataclass
class MarketRuntimeState:
    runtime: MarketDataRuntime | None = None
    capabilities: frozenset[CapabilityId] = frozenset()
    modules_managed: bool = False
    integrity_error: BaseException | None = None


@dataclass
class AccountRuntimeState:
    startup_snapshot_loaded: bool = False
    last_event_time_ms: int | None = None


@dataclass
class ExecutionRuntimeState:
    accepting_signals: bool = True
    orders_in_flight: int = 0


@dataclass
class ClosedBarRuntimeState:
    active_open_time_ms: int | None = None
    last_completed_open_time_ms: int | None = None


@dataclass
class RangeRuntimeState:
    degraded_windows: dict[int, str] = field(default_factory=dict)


@dataclass
class OperationalRuntimeState:
    stopping: bool = False
    startup_complete: bool = False


@dataclass
class RuntimeSharedState:
    """Domain state groups plus temporary attributes for legacy components."""

    market: MarketRuntimeState = field(default_factory=MarketRuntimeState)
    account: AccountRuntimeState = field(default_factory=AccountRuntimeState)
    execution: ExecutionRuntimeState = field(default_factory=ExecutionRuntimeState)
    closed_bar: ClosedBarRuntimeState = field(default_factory=ClosedBarRuntimeState)
    range: RangeRuntimeState = field(default_factory=RangeRuntimeState)
    operational: OperationalRuntimeState = field(
        default_factory=OperationalRuntimeState
    )


class RuntimeComponent:
    """Domain component backed by an explicit state context, not Runner state."""

    def __init__(self, owner: object) -> None:
        object.__setattr__(self, "_owner", owner)
        object.__setattr__(self, "_state", owner._ensure_runtime_state())

    def __getattribute__(self, name: str):
        if name in {"_owner", "_state", "__class__", "__dict__"}:
            return object.__getattribute__(self, name)
        owner = object.__getattribute__(self, "_owner")
        override_reader = getattr(owner, "_runtime_component_override", None)
        if callable(override_reader):
            found, value = override_reader(name)
            if found:
                return value
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            state = object.__getattribute__(self, "_state")
            state_values = object.__getattribute__(state, "__dict__")
            if name in state_values:
                return state_values[name]
            return getattr(owner, name)

    def __getattr__(self, name: str):
        state = object.__getattribute__(self, "_state")
        state_values = object.__getattribute__(state, "__dict__")
        if name in state_values:
            return state_values[name]
        return getattr(object.__getattribute__(self, "_owner"), name)

    def __setattr__(self, name: str, value: object) -> None:
        if name in {"_owner", "_state"}:
            object.__setattr__(self, name, value)
            return
        descriptor = type(self).__dict__.get(name)
        if isinstance(descriptor, property) and descriptor.fset is not None:
            object.__setattr__(self, name, value)
            return
        state = object.__getattribute__(self, "_state")
        if name == "_market_data_runtime":
            state.market.runtime = value  # type: ignore[assignment]
        elif name == "_market_data_capabilities":
            state.market.capabilities = value  # type: ignore[assignment]
        elif name == "_market_modules_managed":
            state.market.modules_managed = bool(value)
        elif name == "_pipeline_integrity_error":
            state.market.integrity_error = value  # type: ignore[assignment]
        setattr(state, name, value)

    def service_dependencies(self) -> RuntimeServices:
        state = object.__getattribute__(self, "_state")
        services = state.__dict__.get("runtime_services")
        if not isinstance(services, RuntimeServices):
            services = RuntimeServices.coerce(state.__dict__.get("services"))
            object.__setattr__(state, "runtime_services", services)
            object.__setattr__(state, "services", services)
        return services

    @property
    def market_state(self) -> MarketRuntimeState:
        return object.__getattribute__(self, "_state").market


__all__ = [
    "AccountRuntimeState",
    "ClosedBarRuntimeState",
    "ExecutionRuntimeState",
    "MarketRuntimeState",
    "OperationalRuntimeState",
    "RangeRuntimeState",
    "RuntimeComponent",
    "RuntimeSharedState",
]
