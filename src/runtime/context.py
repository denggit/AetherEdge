from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from src.runtime.module import CapabilityId

if TYPE_CHECKING:
    from src.app import AppConfig, AppContext
    from src.runtime.config import LiveRuntimeConfig
    from src.runtime.live_types import LiveRuntimeStats
    from src.runtime.market_data.range_config import RangeRuntimeConfig
    from src.runtime.market_data.runtime import MarketDataRuntime
    from src.runtime.requirements import StrategyRuntimeRequirements


@dataclass(slots=True)
class MarketRuntimeState:
    runtime: MarketDataRuntime | None = None
    capabilities: frozenset[CapabilityId] = frozenset()
    modules_managed: bool = False
    integrity_error: BaseException | None = None


@dataclass(slots=True)
class AccountRuntimeState:
    startup_snapshot_loaded: bool = False
    last_event_time_ms: int | None = None


@dataclass(slots=True)
class ExecutionRuntimeState:
    accepting_signals: bool = True
    orders_in_flight: int = 0


@dataclass(slots=True)
class ClosedBarRuntimeState:
    active_open_time_ms: int | None = None
    last_completed_open_time_ms: int | None = None


@dataclass(slots=True)
class RangeRuntimeState:
    degraded_windows: dict[int, str] = field(default_factory=dict)


@dataclass(slots=True)
class OperationalRuntimeState:
    stopping: bool = False
    startup_complete: bool = False
    account_config_new_entries_blocked: bool = False


@dataclass(slots=True)
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


@dataclass(slots=True)
class RuntimeQueues:
    market: asyncio.Queue[Any] | None = None
    market_event_available: asyncio.Event | None = None
    latest_state_mailbox: Any = None


@dataclass(slots=True)
class RuntimeSignals:
    stop_event: asyncio.Event | None = None


@dataclass(slots=True)
class RuntimeLifecycleResources:
    health_state: Any = None
    heartbeat_service: Any = None
    shutdown_coordinator: Any = None
    startup_phase_coordinator: Any = None
    last_snapshot: Any = None
    last_snapshots: tuple[Any, ...] = ()


@dataclass(slots=True)
class RuntimeStatistics:
    live: LiveRuntimeStats | None = None


@dataclass(slots=True)
class RuntimeResources:
    queues: RuntimeQueues = field(default_factory=RuntimeQueues)
    signals: RuntimeSignals = field(default_factory=RuntimeSignals)
    lifecycle: RuntimeLifecycleResources = field(
        default_factory=RuntimeLifecycleResources
    )
    statistics: RuntimeStatistics = field(default_factory=RuntimeStatistics)


@dataclass(slots=True)
class RuntimeContext:
    """Typed shared state; unknown runtime fields cannot be added."""

    app_config: AppConfig | None = None
    app_context: AppContext | None = None
    runtime_config: LiveRuntimeConfig | None = None
    range_config: RangeRuntimeConfig | None = None
    requirements: StrategyRuntimeRequirements | None = None
    state: RuntimeState = field(default_factory=RuntimeState)
    resources: RuntimeResources = field(default_factory=RuntimeResources)


__all__ = [
    "AccountRuntimeState",
    "ClosedBarRuntimeState",
    "ExecutionRuntimeState",
    "MarketRuntimeState",
    "OperationalRuntimeState",
    "RangeRuntimeState",
    "RuntimeContext",
    "RuntimeLifecycleResources",
    "RuntimeQueues",
    "RuntimeResources",
    "RuntimeSignals",
    "RuntimeState",
    "RuntimeStatistics",
]
