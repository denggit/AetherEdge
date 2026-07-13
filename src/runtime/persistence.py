from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from typing import Callable

from src.utils.log import get_logger


logger = get_logger(__name__)


@dataclass(frozen=True)
class BackgroundWriteItem:
    description: str
    write: Callable[[], None]
    on_error: Callable[[BaseException], None] | None = None


class BackgroundWriteQueue:
    """Small bounded thread writer for live non-critical persistence."""

    _STOP = object()

    def __init__(self, *, name: str, max_pending: int = 1000) -> None:
        if max_pending <= 0:
            raise ValueError("max_pending must be positive")
        self.name = name
        self.max_pending = int(max_pending)
        self._queue: queue.Queue[object] = queue.Queue(maxsize=self.max_pending)
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stopping = False
        self.submitted = 0
        self.written = 0
        self.dropped = 0
        self.failures = 0
        self._drop_warn_every = 100
        self._last_drop_warned_at = 0

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopping = False
            self._thread = threading.Thread(
                target=self._run,
                name=self.name,
                daemon=True,
            )
            self._thread.start()

    def submit(self, item: BackgroundWriteItem) -> bool:
        self.start()
        with self._lock:
            if self._stopping:
                self.dropped += 1
                self._warn_drop(reason="stopping")
                return False
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                self.dropped += 1
                self._warn_drop(reason="queue_full_evicted_oldest")
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(item)
            except queue.Full:
                self.dropped += 1
                self._warn_drop(reason="queue_full_double_fail")
                return False
        self.submitted += 1
        return True

    def _warn_drop(self, *, reason: str) -> None:
        """Log a warning on first drop and every Nth drop thereafter."""
        if (
            self.dropped == 1
            or self.dropped - self._last_drop_warned_at >= self._drop_warn_every
        ):
            self._last_drop_warned_at = self.dropped
            logger.warning(
                "Background write queue dropped item | name=%s reason=%s "
                "dropped=%s submitted=%s written=%s failures=%s pending=%s",
                self.name,
                reason,
                self.dropped,
                self.submitted,
                self.written,
                self.failures,
                self.pending_count,
            )

    def stop(self, *, flush: bool = True, timeout: float = 5.0) -> None:
        with self._lock:
            if not flush:
                while True:
                    try:
                        self._queue.get_nowait()
                        self._queue.task_done()
                    except queue.Empty:
                        break
            self._stopping = True
            thread = self._thread
        if thread is None:
            return
        if not thread.is_alive():
            with self._lock:
                if self._thread is thread:
                    self._thread = None
            return
        try:
            self._queue.put(self._STOP, timeout=max(0.1, timeout))
        except queue.Full:
            return
        thread.join(timeout=max(0.0, timeout))
        if not thread.is_alive():
            with self._lock:
                if self._thread is thread:
                    self._thread = None

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is self._STOP:
                    return
                if not isinstance(item, BackgroundWriteItem):
                    self.dropped += 1
                    continue
                try:
                    item.write()
                    self.written += 1
                except BaseException as exc:
                    self.failures += 1
                    if item.on_error is not None:
                        try:
                            item.on_error(exc)
                        except BaseException:
                            pass
            finally:
                self._queue.task_done()


__all__ = [
    "BackgroundWriteItem",
    "BackgroundWriteQueue",
]
