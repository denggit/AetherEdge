from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum


class ProducerStatus(str, Enum):
    RUNNING = "running"
    STALE = "stale"
    FAILED = "failed"
    STOPPED = "stopped"


@dataclass(frozen=True)
class ProducerHealth:
    name: str
    status: ProducerStatus
    last_event_time_ms: int | None = None
    last_heartbeat_time_ms: int | None = None
    error: str | None = None


class ProducerHealthMonitor:
    """Track liveness for market-data producers so runtime cannot fake-live."""

    def __init__(self, *, now_ms_fn=None) -> None:
        self._now_ms_fn = now_ms_fn or (lambda: int(time.time() * 1000))
        self._health: dict[str, ProducerHealth] = {}

    def mark_running(self, name: str) -> ProducerHealth:
        now = self._now_ms_fn()
        health = ProducerHealth(name=name, status=ProducerStatus.RUNNING, last_heartbeat_time_ms=now)
        self._health[name] = health
        return health

    def heartbeat(self, name: str) -> ProducerHealth:
        current = self._health.get(name) or ProducerHealth(name=name, status=ProducerStatus.RUNNING)
        health = ProducerHealth(
            name=name,
            status=ProducerStatus.RUNNING,
            last_event_time_ms=current.last_event_time_ms,
            last_heartbeat_time_ms=self._now_ms_fn(),
            error=None,
        )
        self._health[name] = health
        return health

    def record_event(self, name: str) -> ProducerHealth:
        now = self._now_ms_fn()
        health = ProducerHealth(name=name, status=ProducerStatus.RUNNING, last_event_time_ms=now, last_heartbeat_time_ms=now)
        self._health[name] = health
        return health

    def fail(self, name: str, error: BaseException | str) -> ProducerHealth:
        current = self._health.get(name) or ProducerHealth(name=name, status=ProducerStatus.RUNNING)
        health = ProducerHealth(
            name=name,
            status=ProducerStatus.FAILED,
            last_event_time_ms=current.last_event_time_ms,
            last_heartbeat_time_ms=self._now_ms_fn(),
            error=str(error),
        )
        self._health[name] = health
        return health

    def stop(self, name: str) -> ProducerHealth:
        current = self._health.get(name) or ProducerHealth(name=name, status=ProducerStatus.RUNNING)
        health = ProducerHealth(
            name=name,
            status=ProducerStatus.STOPPED,
            last_event_time_ms=current.last_event_time_ms,
            last_heartbeat_time_ms=self._now_ms_fn(),
            error=current.error,
        )
        self._health[name] = health
        return health

    def mark_stale(self, *, stale_after_ms: int) -> list[ProducerHealth]:
        now = self._now_ms_fn()
        stale: list[ProducerHealth] = []
        for name, current in list(self._health.items()):
            if current.status is not ProducerStatus.RUNNING:
                continue
            candidates = [value for value in (current.last_event_time_ms, current.last_heartbeat_time_ms) if value is not None]
            last_seen = max(candidates) if candidates else None
            if last_seen is None or now - last_seen > stale_after_ms:
                health = ProducerHealth(
                    name=name,
                    status=ProducerStatus.STALE,
                    last_event_time_ms=current.last_event_time_ms,
                    last_heartbeat_time_ms=current.last_heartbeat_time_ms,
                    error=f"producer stale for {now - (last_seen or now)}ms",
                )
                self._health[name] = health
                stale.append(health)
        return stale

    def snapshot(self) -> tuple[ProducerHealth, ...]:
        return tuple(self._health.values())

    def unhealthy(self) -> tuple[ProducerHealth, ...]:
        return tuple(item for item in self._health.values() if item.status in {ProducerStatus.FAILED, ProducerStatus.STALE})
