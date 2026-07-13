from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Callable

from src.runtime.persistence import BackgroundWriteItem, BackgroundWriteQueue


@dataclass(frozen=True)
class RuntimePersistenceMetrics:
    pending_count: int | None
    dropped: int | None
    failures: int | None
    written: int | None
    submitted: int | None


class RuntimePersistenceService:
    """Own the generic live persistence writer lifecycle."""

    def __init__(
        self,
        *,
        writer: object | None = None,
        max_pending: int = 1000,
        writer_name: str = "live-persistence-writer",
    ) -> None:
        self._writer = writer
        self._max_pending = max_pending
        self._writer_name = writer_name

    def get_writer(self) -> object:
        if self._writer is None:
            self._writer = BackgroundWriteQueue(
                name=self._writer_name,
                max_pending=self._max_pending,
            )
        return self._writer

    def submit(
        self,
        *,
        description: str,
        write: Callable[[], None],
        on_error: Callable[[BaseException], None] | None = None,
    ) -> bool:
        writer = self.get_writer()
        item = BackgroundWriteItem(
            description=description,
            write=write,
            on_error=on_error,
        )
        return writer.submit(item)  # type: ignore[attr-defined, no-any-return]

    async def stop(self, *, flush: bool = True) -> None:
        writer = self._writer
        if writer is None:
            return
        stop = getattr(writer, "stop", None)
        if not callable(stop):
            return
        if isinstance(writer, BackgroundWriteQueue):
            await asyncio.to_thread(stop, flush=flush)
            return
        result = stop(flush=flush)
        if inspect.isawaitable(result):
            await result

    def metrics(self) -> RuntimePersistenceMetrics:
        writer = self._writer
        if not isinstance(writer, BackgroundWriteQueue):
            return RuntimePersistenceMetrics(
                pending_count=None,
                dropped=None,
                failures=None,
                written=None,
                submitted=None,
            )
        return RuntimePersistenceMetrics(
            pending_count=writer.pending_count,
            dropped=writer.dropped,
            failures=writer.failures,
            written=writer.written,
            submitted=writer.submitted,
        )


__all__ = [
    "RuntimePersistenceMetrics",
    "RuntimePersistenceService",
]
