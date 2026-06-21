from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from src.runtime.tasks.queues import AsyncTaskQueue


class BackgroundWorker:
    """Worker that consumes non-critical tasks off the main tick/order path."""

    def __init__(self, *, queue: AsyncTaskQueue, handler: Callable[[object], Awaitable[None]]) -> None:
        self.queue = queue
        self.handler = handler
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop_event.set()
        await self.queue.drain()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            item = await self.queue.get()
            try:
                await self.handler(item)
            finally:
                self.queue.task_done()
