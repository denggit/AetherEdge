from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class AppAlert:
    subject: str
    content: str
    severity: str = "warning"


class AlertSink(Protocol):
    async def send(self, alert: AppAlert) -> None:
        ...


class NoopAlertSink:
    async def send(self, alert: AppAlert) -> None:
        return None


class EmailAlertSink:
    async def send(self, alert: AppAlert) -> None:
        from src.utils.email_sender import send_email

        result = send_email(subject=alert.subject, content=alert.content, content_type="plain")
        if inspect.isawaitable(result):
            await result


class AsyncAlertDispatcher:
    """Non-blocking alert dispatcher.

    Main data/strategy/execution tasks call ``emit`` without waiting for email IO.
    """

    def __init__(self, sink: AlertSink | None = None, *, maxsize: int = 100) -> None:
        self._sink = sink or NoopAlertSink()
        self._queue: asyncio.Queue[AppAlert] = asyncio.Queue(maxsize=maxsize)
        self._worker: asyncio.Task | None = None
        self.sent = 0
        self.dropped = 0

    def start(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._worker is None:
            return
        self._worker.cancel()
        try:
            await self._worker
        except asyncio.CancelledError:
            pass

    def emit(self, alert: AppAlert) -> None:
        try:
            self._queue.put_nowait(alert)
        except asyncio.QueueFull:
            self.dropped += 1

    async def _run(self) -> None:
        while True:
            alert = await self._queue.get()
            try:
                await self._sink.send(alert)
                self.sent += 1
            finally:
                self._queue.task_done()
