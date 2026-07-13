from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence


SyncTaskFactory = Callable[[], Awaitable[None]]


class RuntimeSyncLifecycle:
    """Own creation and cancellation of the runtime sync task group."""

    def __init__(self) -> None:
        self._tasks: list[asyncio.Task[None]] = []

    @property
    def tasks(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(self._tasks)

    def start(
        self,
        task_factories: Sequence[SyncTaskFactory],
    ) -> list[asyncio.Task[None]]:
        tasks: list[asyncio.Task[None]] = []
        for factory in task_factories:
            tasks.append(asyncio.create_task(factory()))
        self._tasks = tasks
        return tasks

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []


__all__ = ["RuntimeSyncLifecycle", "SyncTaskFactory"]
