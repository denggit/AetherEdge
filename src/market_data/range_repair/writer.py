from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Callable, Sequence

from src.market_data.range_repair.models import (
    JOURNAL_INVALID_DROPPED_TRADE,
    JOURNAL_INVALID_QUEUE_OVERFLOW,
    JOURNAL_INVALID_WRITER_ERROR,
    RangeRepairTrade,
)
from src.market_data.range_repair.store import (
    SqliteRangeRepairJournalStore,
    _decimal_text,
    _now_ms,
)


@dataclass(frozen=True)
class _WriterCommand:
    kind: str
    payload: object


class RangeRepairJournalWriter:
    """Non-blocking bounded journal writer for the live process."""

    def __init__(
            self,
            store: SqliteRangeRepairJournalStore,
            *,
            max_pending: int = 20_000,
            flush_interval_ms: int = 500,
            batch_size: int = 1_000,
            retention_hours: int = 12,
            on_error: Callable[[BaseException], None] | None = None,
            on_invalidated: (
                    Callable[[tuple[str, str, str, int], str, str], None] | None
            ) = None,
    ) -> None:
        if max_pending <= 0 or batch_size <= 0:
            raise ValueError("journal writer limits must be positive")
        self.store = store
        self.max_pending = int(max_pending)
        self.flush_interval_ms = max(1, int(flush_interval_ms))
        self.batch_size = int(batch_size)
        self.retention_hours = min(12, max(1, int(retention_hours)))
        self.on_error = on_error
        self.on_invalidated = on_invalidated
        self._commands: deque[_WriterCommand] = deque()
        self._invalidations: dict[
            tuple[str, str, str, int], tuple[str, str, int, int]
        ] = {}
        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None
        self._stopping = False
        self._pending_trades = 0
        self._disabled_keys: set[tuple[str, str, str, int]] = set()
        self.written = 0
        self.dropped = 0
        self.failures = 0

    def start(self) -> None:
        with self._condition:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopping = False
            self._thread = threading.Thread(
                target=self._run,
                name="range-repair-journal-writer",
                daemon=True,
            )
            self._thread.start()

    def submit_open(self, **payload: object) -> bool:
        return self._submit_control("open", dict(payload))

    def submit_first_live(self, **payload: object) -> bool:
        return self._submit_control("first_live", dict(payload))

    def submit_trade(self, trade: RangeRepairTrade) -> bool:
        key = _trade_key(trade)
        with self._condition:
            if self._stopping:
                self.dropped += 1
                self._add_invalidation(
                    key,
                    JOURNAL_INVALID_DROPPED_TRADE,
                    "journal writer stopping",
                    dropped=1,
                )
                return False
            if key in self._disabled_keys:
                self.dropped += 1
                return False
            if self._pending_trades >= self.max_pending:
                self.dropped += 1
                self._add_invalidation(
                    key,
                    JOURNAL_INVALID_QUEUE_OVERFLOW,
                    "journal writer queue overflow",
                    dropped=1,
                )
                self._condition.notify()
                return False
            self._commands.append(_WriterCommand("trade", trade))
            self._pending_trades += 1
            self._condition.notify()
            return True

    def submit_invalidation(
            self,
            *,
            exchange: str,
            symbol: str,
            range_pct: str,
            bucket_start_ms: int,
            status: str,
            last_error: str,
            dropped_trades: int = 0,
            writer_failures: int = 0,
    ) -> bool:
        key = (
            str(exchange).lower(),
            symbol,
            _decimal_text(range_pct),
            int(bucket_start_ms),
        )
        with self._condition:
            self._add_invalidation(
                key,
                status,
                last_error,
                dropped=max(0, int(dropped_trades)),
                failures=max(0, int(writer_failures)),
            )
            self._condition.notify()
        return True

    def submit_finalize(self, **payload: object) -> bool:
        return self._submit_control("finalize", dict(payload))

    def stop(self, *, flush: bool = True, timeout: float = 10.0) -> None:
        with self._condition:
            if not flush:
                self._commands.clear()
                self._invalidations.clear()
                self._pending_trades = 0
            self._stopping = True
            self._condition.notify_all()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, timeout))

    @property
    def pending_count(self) -> int:
        with self._condition:
            return self._pending_trades

    def _submit_control(self, kind: str, payload: dict[str, object]) -> bool:
        with self._condition:
            if self._stopping:
                return False
            self._commands.append(_WriterCommand(kind, payload))
            self._condition.notify()
            return True

    def _add_invalidation(
            self,
            key: tuple[str, str, str, int],
            status: str,
            error: str,
            *,
            dropped: int = 0,
            failures: int = 0,
    ) -> None:
        existing = self._invalidations.get(key)
        self._disabled_keys.add(key)
        if existing is None:
            self._invalidations[key] = (
                status,
                error,
                dropped,
                failures,
            )
            return
        old_status, old_error, old_dropped, old_failures = existing
        self._invalidations[key] = (
            old_status,
            old_error or error,
            old_dropped + dropped,
            old_failures + failures,
        )

    def _run(self) -> None:
        while True:
            invalidations = {}
            command = None
            with self._condition:
                while (
                        not self._commands
                        and not self._invalidations
                        and not self._stopping
                ):
                    self._condition.wait()
                if (
                        not self._commands
                        and not self._invalidations
                        and self._stopping
                ):
                    return
                if self._invalidations and not (
                        self._commands and self._commands[0].kind == "open"
                ):
                    invalidations = self._invalidations
                    self._invalidations = {}
                if self._commands:
                    if (
                            self._commands[0].kind == "trade"
                            and len(self._commands) < self.batch_size
                            and not self._stopping
                    ):
                        self._condition.wait(
                            timeout=self.flush_interval_ms / 1000
                        )
                    command = self._commands.popleft()
                    if command.kind == "trade":
                        self._pending_trades -= 1
            for key, values in invalidations.items():
                status, error, dropped, failures = values
                self._safe_invalidate(
                    key,
                    status=status,
                    error=error,
                    dropped=dropped,
                    failures=failures,
                )
            if command is None:
                continue
            if command.kind == "trade":
                trades = [command.payload]
                with self._condition:
                    while (
                            self._commands
                            and self._commands[0].kind == "trade"
                            and len(trades) < self.batch_size
                    ):
                        trades.append(self._commands.popleft().payload)
                        self._pending_trades -= 1
                self._write_trade_batch(trades)
            else:
                self._run_control(command)

    def _run_control(self, command: _WriterCommand) -> None:
        payload = dict(command.payload)
        try:
            if command.kind == "open":
                cutoff = _now_ms() - self.retention_hours * 60 * 60_000
                self.store.cleanup(older_than_ms=cutoff)
                self.store.open_bucket(**payload)
            elif command.kind == "first_live":
                self.store.record_first_live_trade(**payload)
            elif command.kind == "finalize":
                state = self.store.finalize(**payload)
                if state is not None:
                    cutoff = (
                            int(state.finalized_at_ms or _now_ms())
                            - self.retention_hours * 60 * 60_000
                    )
                    self.store.cleanup(older_than_ms=cutoff)
        except BaseException as exc:
            self.failures += 1
            key = _payload_key(payload)
            if command.kind == "open":
                try:
                    self.store.open_bucket(**payload)
                except BaseException:
                    pass
            self._safe_invalidate(
                key,
                status=JOURNAL_INVALID_WRITER_ERROR,
                error=f"{type(exc).__name__}:{exc}",
                failures=1,
            )
            self._notify_error(exc)

    def _write_trade_batch(self, rows: Sequence[object]) -> None:
        trades = [row for row in rows if isinstance(row, RangeRepairTrade)]
        if not trades:
            return
        try:
            self.written += self.store.append_trades(trades)
        except BaseException as exc:
            self.failures += 1
            for key in {_trade_key(row) for row in trades}:
                self._safe_invalidate(
                    key,
                    status=JOURNAL_INVALID_WRITER_ERROR,
                    error=f"{type(exc).__name__}:{exc}",
                    failures=1,
                )
            self._notify_error(exc)

    def _safe_invalidate(
            self,
            key: tuple[str, str, str, int],
            *,
            status: str,
            error: str,
            dropped: int = 0,
            failures: int = 0,
    ) -> None:
        try:
            invalidated = self.store.invalidate(
                exchange=key[0],
                symbol=key[1],
                range_pct=key[2],
                bucket_start_ms=key[3],
                status=status,
                last_error=error,
                dropped_trades=dropped,
                writer_failures=failures,
            )
            if invalidated and self.on_invalidated is not None:
                self.on_invalidated(key, status, error)
        except BaseException as exc:
            self.failures += 1
            self._notify_error(exc)

    def _notify_error(self, exc: BaseException) -> None:
        if self.on_error is None:
            return
        try:
            self.on_error(exc)
        except BaseException:
            pass


def _trade_key(row: RangeRepairTrade) -> tuple[str, str, str, int]:
    return (
        str(row.exchange).lower(),
        row.symbol,
        _decimal_text(row.range_pct),
        int(row.bucket_start_ms),
    )


def _payload_key(
        payload: dict[str, object],
) -> tuple[str, str, str, int]:
    return (
        str(payload["exchange"]).lower(),
        str(payload["symbol"]),
        _decimal_text(payload["range_pct"]),
        int(payload["bucket_start_ms"]),
    )
