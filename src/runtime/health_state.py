from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.runtime.models import RuntimeHealth, RuntimePhase


class RuntimeHealthState:
    """Own the current immutable runtime health snapshot."""

    def __init__(self, initial: RuntimeHealth) -> None:
        self._current = initial

    @property
    def current(self) -> RuntimeHealth:
        return self._current

    def update(
        self,
        phase: RuntimePhase,
        *,
        healthy: bool | None = None,
        warmup_complete: bool | None = None,
        caught_up: bool | None = None,
        last_market_event_time_ms: int | None = None,
        error: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> RuntimeHealth:
        current = self._current
        updated = RuntimeHealth(
            phase=phase,
            healthy=current.healthy if healthy is None else healthy,
            warmup_complete=(
                current.warmup_complete
                if warmup_complete is None
                else warmup_complete
            ),
            caught_up=(
                current.caught_up if caught_up is None else caught_up
            ),
            last_market_event_time_ms=(
                current.last_market_event_time_ms
                if last_market_event_time_ms is None
                else last_market_event_time_ms
            ),
            error=current.error if error is None else error,
            metadata=dict(
                current.metadata if metadata is None else metadata
            ),
        )
        self._current = updated
        return updated


__all__ = ["RuntimeHealthState"]
