from __future__ import annotations

from typing import Protocol

from src.runtime.models import RuntimeHealth


class RuntimeServicePort(Protocol):
    async def start(self) -> RuntimeHealth:
        ...

    async def stop(self) -> RuntimeHealth:
        ...

    async def health(self) -> RuntimeHealth:
        ...


class BackgroundTaskQueue(Protocol):
    async def put(self, item: object) -> None:
        ...

    async def drain(self) -> None:
        ...
