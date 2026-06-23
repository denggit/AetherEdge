from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable
from typing import TypeVar

from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

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

    async def run_resilient_stream(
        self,
        *,
        name: str,
        stream_factory: Callable[[], AsyncIterable[T]],
        on_item: Callable[[T], Awaitable[None]],
        restart_delay_seconds: float = 1.0,
        max_restarts: int | None = None,
    ) -> None:
        """Run a market-data stream and rebuild it after transient failures.

        This is intentionally opt-in so callers that want fail-fast producer
        semantics can keep using :meth:`run_stream`.
        """

        restarts = 0
        while True:
            self.monitor.mark_running(name)
            logger.info("Producer stream running | name=%s restart_count=%s", name, restarts)
            try:
                async for item in stream_factory():
                    self.monitor.record_event(name)
                    await on_item(item)
                self.monitor.stop(name)
                logger.info("Producer stream stopped | name=%s", name)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not _is_transient_stream_error(exc):
                    self.monitor.fail(name, exc)
                    logger.exception("Producer stream failed | name=%s", name)
                    raise
                if max_restarts is not None and restarts >= max_restarts:
                    self.monitor.fail(name, exc)
                    logger.exception("Producer stream restart limit exceeded | name=%s restarts=%s", name, restarts)
                    raise
                restarts += 1
                delay = _restart_delay(restart_delay_seconds, restarts)
                self.monitor.heartbeat(name)
                logger.warning(
                    "Producer stream transient failure; restarting | name=%s restart_count=%s delay_seconds=%.2f error=%s",
                    name,
                    restarts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

    def check(self):
        self.monitor.mark_stale(stale_after_ms=self.stale_after_ms)
        return self.monitor.unhealthy()

    @property
    def failed_or_stale(self):
        self.check()
        return tuple(item for item in self.monitor.snapshot() if item.status in {ProducerStatus.FAILED, ProducerStatus.STALE})


TRANSIENT_STREAM_EXCEPTIONS = (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
    asyncio.TimeoutError,
    OSError,
)


def _is_transient_stream_error(exc: BaseException) -> bool:
    return isinstance(exc, TRANSIENT_STREAM_EXCEPTIONS)


def _restart_delay(base_delay_seconds: float, restart_count: int) -> float:
    return min(float(base_delay_seconds) * (2 ** min(max(restart_count - 1, 0), 6)), 60.0)
