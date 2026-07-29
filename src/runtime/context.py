from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.runtime.module import CapabilityId

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
class RuntimeState:
    market: MarketRuntimeState = field(default_factory=MarketRuntimeState)
    account: AccountRuntimeState = field(default_factory=AccountRuntimeState)
    execution: ExecutionRuntimeState = field(
        default_factory=ExecutionRuntimeState
    )
    closed_bar: ClosedBarRuntimeState = field(
        default_factory=ClosedBarRuntimeState
    )
    range: RangeRuntimeState = field(default_factory=RangeRuntimeState)
    operational: OperationalRuntimeState = field(
        default_factory=OperationalRuntimeState
    )


@dataclass
class RuntimeContext:
    """Explicit object shared by runtime components at composition time."""

    state: RuntimeState = field(default_factory=RuntimeState)

    def __post_init__(self) -> None:
        self.runtime_context = self
        self.runtime_state = self.state


__all__ = [
    "AccountRuntimeState",
    "ClosedBarRuntimeState",
    "ExecutionRuntimeState",
    "MarketRuntimeState",
    "OperationalRuntimeState",
    "RangeRuntimeState",
    "RuntimeContext",
    "RuntimeState",
]
