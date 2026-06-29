from __future__ import annotations

import queue
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from src.market_data.models import TimeRange
from src.market_data.storage import SqliteTradeStore
from src.platform.data.models import MarketTrade


@dataclass(frozen=True)
class RealtimeTradeRecorderConfig:
    db_path: str | Path = "data/market_data/aether_market_data.sqlite3"
    batch_size: int = 1_000
    flush_interval_ms: int = 1_000
    queue_maxsize: int = 50_000
    busy_timeout_ms: int = 100


class RealtimeTradeRecorder:
    """Non-blocking realtime trade writer for the internal trade store."""

    def __init__(
        self,
        config: RealtimeTradeRecorderConfig | None = None,
        *,
        store: SqliteTradeStore | None = None,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        self.config = config or RealtimeTradeRecorderConfig()
        self.store = store or SqliteTradeStore(self.config.db_path, busy_timeout_ms=self.config.busy_timeout_ms)
        self.on_error = on_error
        self._queue: queue.Queue[MarketTrade] = queue.Queue(maxsize=max(1, int(self.config.queue_maxsize)))
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.submitted = 0
        self.dropped = 0
        self.written = 0
        self.failures = 0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="realtime-trade-recorder", daemon=True)
        self._thread.start()

    def submit(self, trade: MarketTrade) -> bool:
        try:
            self._queue.put_nowait(trade)
            self.submitted += 1
            return True
        except queue.Full:
            self.dropped += 1
            return False

    def stop(self, *, flush: bool = True, timeout: float = 5.0) -> None:
        if not flush:
            while True:
                try:
                    self._queue.get_nowait()
                    self._queue.task_done()
                except queue.Empty:
                    break
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, timeout))

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    def _run(self) -> None:
        batch: list[MarketTrade] = []
        flush_interval = max(0.001, self.config.flush_interval_ms / 1000)
        next_flush = time.monotonic() + flush_interval
        while not self._stop.is_set() or not self._queue.empty():
            timeout = max(0.0, next_flush - time.monotonic())
            try:
                item = self._queue.get(timeout=timeout)
                batch.append(item)
                self._queue.task_done()
            except queue.Empty:
                pass
            if len(batch) >= self.config.batch_size or (batch and time.monotonic() >= next_flush):
                self._flush(batch)
                batch = []
                next_flush = time.monotonic() + flush_interval
        if batch:
            self._flush(batch)

    def _flush(self, batch: list[MarketTrade]) -> None:
        try:
            self.written += self.store.save(batch)
            times = sorted(ts for trade in batch if (ts := _trade_time_ms(trade)) is not None)
            if times:
                self.store.mark_coverage(
                    symbol=batch[0].symbol,
                    time_range=TimeRange(times[0], times[-1]),
                    source="realtime",
                )
        except sqlite3.OperationalError as exc:
            self.failures += 1
            if self.on_error is not None:
                self.on_error(exc)
        except BaseException as exc:  # noqa: BLE001 - recorder must not crash runtime
            self.failures += 1
            if self.on_error is not None:
                self.on_error(exc)


def _trade_time_ms(trade: MarketTrade) -> int | None:
    return trade.trade_time_ms if trade.trade_time_ms is not None else trade.event_time_ms
