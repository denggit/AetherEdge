from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class QueueStats:
    enqueued: int = 0
    processed: int = 0
    dropped: int = 0


class AsyncTaskQueue:
    """Bounded async queue for non-critical runtime work."""

    def __init__(self, *, name: str, maxsize: int = 1000, drop_oldest: bool = True) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.name = name
        self.drop_oldest = drop_oldest
        self.stats = QueueStats()
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize=maxsize)

    async def put(self, item: object) -> None:
        if self._queue.full() and self.drop_oldest:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self.stats.dropped += 1
            except asyncio.QueueEmpty:
                pass
        await self._queue.put(item)
        self.stats.enqueued += 1

    async def get(self) -> object:
        return await self._queue.get()

    def task_done(self) -> None:
        self._queue.task_done()
        self.stats.processed += 1

    async def drain(self) -> None:
        await self._queue.join()

    def qsize(self) -> int:
        return self._queue.qsize()
