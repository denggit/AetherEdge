from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable
from typing import TypeVar

from src.runtime.tasks.health import ProducerHealthMonitor, ProducerStatus
from src.utils.log import get_logger

T = TypeVar("T")

logger = get_logger(__name__)


class ProducerSupervisor:
    """Run producer streams while reflecting failures into ProducerHealth."""

    def __init__(self, *, monitor: ProducerHealthMonitor | None = None, stale_after_ms: int = 60_000) -> None:
        if stale_after_ms <= 0:
            raise ValueError("stale_after_ms must be positive")
        self.monitor = monitor or ProducerHealthMonitor()
        self.stale_after_ms = stale_after_ms

    async def run_stream(self, *, name: str, stream: AsyncIterable[T], on_item: Callable[[T], Awaitable[None]]) -> None:
        self.monitor.mark_running(name)
        logger.info("Producer stream running | name=%s", name)
        try:
            async for item in stream:
                self.monitor.record_event(name)
                await on_item(item)
            self.monitor.stop(name)
            logger.info("Producer stream stopped | name=%s", name)
        except Exception as exc:
            self.monitor.fail(name, exc)
            logger.exception("Producer stream failed | name=%s", name)
            raise

    def check(self):
        self.monitor.mark_stale(stale_after_ms=self.stale_after_ms)
        return self.monitor.unhealthy()

    @property
    def failed_or_stale(self):
        self.check()
        return tuple(item for item in self.monitor.snapshot() if item.status in {ProducerStatus.FAILED, ProducerStatus.STALE})
