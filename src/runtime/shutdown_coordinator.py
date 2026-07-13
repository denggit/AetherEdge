from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence


ShutdownStep = Callable[[], Awaitable[None]]


class RuntimeShutdownCoordinator:
    """Await shutdown steps sequentially in the supplied order."""

    @staticmethod
    async def execute(
        steps: Sequence[ShutdownStep],
    ) -> None:
        for step in steps:
            await step()


__all__ = [
    "RuntimeShutdownCoordinator",
    "ShutdownStep",
]
