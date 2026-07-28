from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

from src.platform.data.models import MarketFullOrderBook
from src.platform.data.rest.ports import FullOrderBookSnapshotFetcher


Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]


class FullOrderBookStream(Protocol):
    async def stream_full_order_book(
        self,
    ) -> AsyncIterator[MarketFullOrderBook]:
        ...


class FullOrderBookPollingError(RuntimeError):
    pass


class FullOrderBookPollingStream:
    def __init__(
        self,
        *,
        fetcher: FullOrderBookSnapshotFetcher,
        symbol: str,
        depth: int = 5000,
        poll_interval_seconds: float = 3.0,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        if type(depth) is not int or not 1 <= depth <= 5000:
            raise ValueError(
                "full order book depth must be between 1 and 5000"
            )
        if (
            isinstance(poll_interval_seconds, bool)
            or float(poll_interval_seconds) < 1.0
        ):
            raise ValueError(
                "full order book poll interval must be at least 1 second"
            )
        self._fetcher = fetcher
        self._symbol = symbol
        self.depth = depth
        self.poll_interval_seconds = float(poll_interval_seconds)
        self._clock = clock
        self._sleep = sleep
        self._stopped = False
        self.duplicate_snapshot_count = 0
        self.requests_started = 0

    async def stream_full_order_book(
        self,
    ) -> AsyncIterator[MarketFullOrderBook]:
        self._stopped = False
        last_event_time_ms: int | None = None
        while not self._stopped:
            started_at = self._clock()
            self.requests_started += 1
            try:
                snapshot = await self._fetcher.fetch_full_order_book(
                    symbol=self._symbol,
                    depth=self.depth,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise FullOrderBookPollingError(
                    "full order book polling failed | "
                    f"symbol={self._symbol} "
                    f"depth={self.depth} "
                    f"poll_interval_seconds={self.poll_interval_seconds} "
                    f"error={type(exc).__name__}: {exc}"
                ) from exc
            if snapshot.event_time_ms == last_event_time_ms:
                self.duplicate_snapshot_count += 1
            else:
                last_event_time_ms = snapshot.event_time_ms
                yield snapshot
            if self._stopped:
                break
            elapsed = max(0.0, self._clock() - started_at)
            await self._sleep(
                max(0.0, self.poll_interval_seconds - elapsed)
            )

    async def stop(self) -> None:
        self._stopped = True


__all__ = [
    "FullOrderBookPollingError",
    "FullOrderBookPollingStream",
    "FullOrderBookStream",
]
