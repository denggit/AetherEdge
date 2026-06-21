from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping


class RuntimeMode(str, Enum):
    LEGACY_APP = "legacy_app"
    LIVE_RUNTIME = "live_runtime"


class RuntimePhase(str, Enum):
    CREATED = "created"
    WARMING_UP = "warming_up"
    CATCHING_UP = "catching_up"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass(frozen=True)
class RuntimeHealth:
    phase: RuntimePhase
    warmup_complete: bool = False
    caught_up: bool = False
    last_market_event_time_ms: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
