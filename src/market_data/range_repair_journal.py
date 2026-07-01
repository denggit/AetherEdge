from __future__ import annotations

import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

DEFAULT_RANGE_REPAIR_JOURNAL_DB = (
    "data/state/range_repair_trade_journal.sqlite3"
)

JOURNAL_OPEN = "journal_open"
JOURNAL_FINALIZED = "journal_finalized"
JOURNAL_INVALID_DROPPED_TRADE = "journal_invalid_dropped_trade"
JOURNAL_INVALID_WRITER_ERROR = "journal_invalid_writer_error"
JOURNAL_INVALID_QUEUE_OVERFLOW = "journal_invalid_queue_overflow"
JOURNAL_INVALID_PRODUCER_STALE = "journal_invalid_producer_stale"
JOURNAL_INVALID_PRODUCER_FAILED = "journal_invalid_producer_failed"
JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE = (
    "journal_invalid_market_queue_drain_incomplete"
)


def journal_status_is_invalid(status: str) -> bool:
    return str(status).startswith("journal_invalid_")


@dataclass(frozen=True)
class RangeRepairTrade:
    exchange: str
    symbol: str
    range_pct: str
    bucket_start_ms: int
    trade_time_ms: int
    event_time_ms: int | None
    trade_id: str | None
    raw_symbol: str
    side: str
    price: str
    quantity: str
    source: str
    created_at_ms: int


@dataclass(frozen=True)
class RangeRepairJournalState:
    exchange: str
    symbol: str
    range_pct: str
    bucket_start_ms: int
    bucket_end_ms: int
    checkpoint_last_trade_ts_ms: int | None
    checkpoint_last_trade_id: str | None
    first_live_trade_ts_ms: int | None
    first_live_trade_id: str | None
    first_live_trade_recorded_at_ms: int | None
    last_journal_trade_ts_ms: int | None
    journal_trade_count: int
    dropped_trades: int
    writer_failures: int
    finalized: bool
    finalized_at_ms: int | None
    status: str
    last_error: str | None
    updated_at_ms: int

    @property
    def valid_for_repair(self) -> bool:
        return (
            self.finalized
            and self.status == JOURNAL_FINALIZED
            and self.first_live_trade_ts_ms is not None
            and self.dropped_trades == 0
            and self.writer_failures == 0
        )


class SqliteRangeRepairJournalStore:
    """Short-lived recovery journal; never writes market-data raw tables."""

    def __init__(
        self,
        path: str | Path = DEFAULT_RANGE_REPAIR_JOURNAL_DB,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def open_bucket(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        bucket_end_ms: int,
        checkpoint_last_trade_ts_ms: int | None,
        checkpoint_last_trade_id: str | None,
        updated_at_ms: int,
    ) -> None:
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT checkpoint_last_trade_ts_ms, first_live_trade_ts_ms,
                       finalized
                FROM range_repair_journal_state
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO range_repair_journal_state (
                    exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                    checkpoint_last_trade_ts_ms, checkpoint_last_trade_id,
                    status, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, symbol, range_pct, bucket_start_ms)
                DO UPDATE SET
                    bucket_end_ms=excluded.bucket_end_ms,
                    checkpoint_last_trade_ts_ms=CASE
                        WHEN range_repair_journal_state.checkpoint_last_trade_ts_ms
                             IS NULL
                        THEN excluded.checkpoint_last_trade_ts_ms
                        WHEN excluded.checkpoint_last_trade_ts_ms IS NULL
                        THEN range_repair_journal_state.checkpoint_last_trade_ts_ms
                        ELSE MIN(
                            range_repair_journal_state.checkpoint_last_trade_ts_ms,
                            excluded.checkpoint_last_trade_ts_ms
                        )
                    END,
                    checkpoint_last_trade_id=CASE
                        WHEN range_repair_journal_state.checkpoint_last_trade_ts_ms
                             IS NULL
                          OR (
                            excluded.checkpoint_last_trade_ts_ms IS NOT NULL
                            AND excluded.checkpoint_last_trade_ts_ms
                                < range_repair_journal_state.checkpoint_last_trade_ts_ms
                          )
                        THEN excluded.checkpoint_last_trade_id
                        ELSE range_repair_journal_state.checkpoint_last_trade_id
                    END,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                    int(bucket_end_ms),
                    checkpoint_last_trade_ts_ms,
                    checkpoint_last_trade_id,
                    JOURNAL_OPEN,
                    int(updated_at_ms),
                ),
            )
            if (
                existing is not None
                and existing[1] is not None
                and not bool(existing[2])
                and checkpoint_last_trade_ts_ms is not None
                and existing[0] is not None
                and int(checkpoint_last_trade_ts_ms) > int(existing[0])
            ):
                conn.execute(
                    """
                    UPDATE range_repair_journal_state
                    SET status=?,
                        dropped_trades=dropped_trades+1,
                        last_error=?,
                        updated_at_ms=?
                    WHERE exchange=? AND symbol=? AND range_pct=?
                      AND bucket_start_ms=?
                    """,
                    (
                        JOURNAL_INVALID_DROPPED_TRADE,
                        "multiple recovery gaps in one bucket are unsupported",
                        int(updated_at_ms),
                        str(exchange).lower(),
                        symbol,
                        _decimal_text(range_pct),
                        int(bucket_start_ms),
                    ),
                )

    def record_first_live_trade(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        trade_time_ms: int,
        trade_id: str | None,
        recorded_at_ms: int,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE range_repair_journal_state
                SET first_live_trade_ts_ms=?,
                    first_live_trade_id=?,
                    first_live_trade_recorded_at_ms=?,
                    updated_at_ms=?
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                  AND first_live_trade_ts_ms IS NULL
                  AND finalized=0
                  AND status=?
                """,
                (
                    int(trade_time_ms),
                    trade_id,
                    int(recorded_at_ms),
                    int(recorded_at_ms),
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                    JOURNAL_OPEN,
                ),
            )
        return int(cursor.rowcount or 0) > 0

    def append_trades(self, rows: Sequence[RangeRepairTrade]) -> int:
        if not rows:
            return 0
        grouped: dict[tuple[str, str, str, int], list[RangeRepairTrade]] = {}
        for row in rows:
            key = (
                str(row.exchange).lower(),
                row.symbol,
                _decimal_text(row.range_pct),
                int(row.bucket_start_ms),
            )
            grouped.setdefault(key, []).append(row)
        inserted_total = 0
        with self._connect() as conn:
            for key, bucket_rows in grouped.items():
                state = conn.execute(
                    """
                    SELECT finalized, status
                    FROM range_repair_journal_state
                    WHERE exchange=? AND symbol=? AND range_pct=?
                      AND bucket_start_ms=?
                    """,
                    key,
                ).fetchone()
                if state is None:
                    raise RuntimeError(
                        f"repair journal state missing for bucket={key[3]}"
                    )
                if bool(state[0]):
                    conn.execute(
                        """
                        UPDATE range_repair_journal_state
                        SET status=?,
                            dropped_trades=dropped_trades+?,
                            last_error=?,
                            updated_at_ms=?
                        WHERE exchange=? AND symbol=? AND range_pct=?
                          AND bucket_start_ms=?
                        """,
                        (
                            JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
                            len(bucket_rows),
                            "live trade arrived after journal finalized",
                            _now_ms(),
                            *key,
                        ),
                    )
                    continue
                if journal_status_is_invalid(str(state[1])):
                    continue
                before = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO range_repair_trades (
                        exchange, symbol, range_pct, bucket_start_ms,
                        trade_time_ms, event_time_ms, trade_id, raw_symbol,
                        side, price, quantity, source, created_at_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [_trade_params(row) for row in bucket_rows],
                )
                inserted = conn.total_changes - before
                inserted_total += inserted
                if inserted:
                    conn.execute(
                        """
                        UPDATE range_repair_journal_state
                        SET last_journal_trade_ts_ms=CASE
                                WHEN last_journal_trade_ts_ms IS NULL
                                THEN ?
                                ELSE MAX(last_journal_trade_ts_ms, ?)
                            END,
                            journal_trade_count=journal_trade_count+?,
                            updated_at_ms=?
                        WHERE exchange=? AND symbol=? AND range_pct=?
                          AND bucket_start_ms=?
                        """,
                        (
                            max(row.trade_time_ms for row in bucket_rows),
                            max(row.trade_time_ms for row in bucket_rows),
                            inserted,
                            _now_ms(),
                            *key,
                        ),
                    )
        return inserted_total

    def invalidate(
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
        updated_at_ms: int | None = None,
    ) -> bool:
        if not journal_status_is_invalid(status):
            raise ValueError(f"journal invalid status required, got {status}")
        timestamp = _now_ms() if updated_at_ms is None else int(updated_at_ms)
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE range_repair_journal_state
                SET status=CASE
                        WHEN status LIKE 'journal_invalid_%' THEN status
                        ELSE ?
                    END,
                    dropped_trades=dropped_trades+?,
                    writer_failures=writer_failures+?,
                    last_error=?,
                    updated_at_ms=?
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                """,
                (
                    status,
                    max(0, int(dropped_trades)),
                    max(0, int(writer_failures)),
                    last_error,
                    timestamp,
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            )
        return int(cursor.rowcount or 0) > 0

    def finalize(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        finalized_at_ms: int,
    ) -> RangeRepairJournalState | None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE range_repair_journal_state
                SET finalized=1,
                    finalized_at_ms=?,
                    status=CASE
                        WHEN status LIKE 'journal_invalid_%' THEN status
                        WHEN dropped_trades > 0
                        THEN ?
                        WHEN writer_failures > 0
                        THEN ?
                        ELSE ?
                    END,
                    updated_at_ms=?
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                """,
                (
                    int(finalized_at_ms),
                    JOURNAL_INVALID_DROPPED_TRADE,
                    JOURNAL_INVALID_WRITER_ERROR,
                    JOURNAL_FINALIZED,
                    int(finalized_at_ms),
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            )
        return self.load_state(
            exchange=exchange,
            symbol=symbol,
            range_pct=range_pct,
            bucket_start_ms=bucket_start_ms,
        )

    def load_state(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
    ) -> RangeRepairJournalState | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT exchange, symbol, range_pct, bucket_start_ms,
                       bucket_end_ms, checkpoint_last_trade_ts_ms,
                       checkpoint_last_trade_id, first_live_trade_ts_ms,
                       first_live_trade_id, first_live_trade_recorded_at_ms,
                       last_journal_trade_ts_ms, journal_trade_count,
                       dropped_trades, writer_failures, finalized,
                       finalized_at_ms, status, last_error, updated_at_ms
                FROM range_repair_journal_state
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            ).fetchone()
        return None if row is None else _state_from_row(row)

    def load_trades(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[RangeRepairTrade]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, range_pct, bucket_start_ms,
                       trade_time_ms, event_time_ms, trade_id, raw_symbol,
                       side, price, quantity, source, created_at_ms
                FROM range_repair_trades
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                  AND trade_time_ms BETWEEN ? AND ?
                ORDER BY trade_time_ms ASC, trade_id ASC
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                    int(start_time_ms),
                    int(end_time_ms),
                ),
            ).fetchall()
        return [_trade_from_row(row) for row in rows]

    def cleanup(self, *, older_than_ms: int) -> tuple[int, int]:
        with self._connect() as conn:
            before = conn.total_changes
            conn.execute(
                """
                DELETE FROM range_repair_trades
                WHERE (exchange, symbol, range_pct, bucket_start_ms) IN (
                    SELECT exchange, symbol, range_pct, bucket_start_ms
                    FROM range_repair_journal_state
                    WHERE bucket_end_ms < ?
                )
                """,
                (int(older_than_ms),),
            )
            trades_deleted = conn.total_changes - before
            before = conn.total_changes
            conn.execute(
                """
                DELETE FROM range_repair_journal_state
                WHERE bucket_end_ms < ?
                """,
                (int(older_than_ms),),
            )
            states_deleted = conn.total_changes - before
        return trades_deleted, states_deleted

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS range_repair_trades (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    bucket_start_ms INTEGER NOT NULL,
                    trade_time_ms INTEGER NOT NULL,
                    event_time_ms INTEGER,
                    trade_id TEXT NOT NULL DEFAULT '',
                    raw_symbol TEXT,
                    side TEXT NOT NULL,
                    price TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (
                        exchange, symbol, range_pct, bucket_start_ms,
                        trade_time_ms, trade_id, price, quantity, side
                    )
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_range_repair_trades_time
                ON range_repair_trades(
                    exchange, symbol, range_pct, bucket_start_ms, trade_time_ms
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS range_repair_journal_state (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    bucket_start_ms INTEGER NOT NULL,
                    bucket_end_ms INTEGER NOT NULL,
                    checkpoint_last_trade_ts_ms INTEGER,
                    checkpoint_last_trade_id TEXT,
                    first_live_trade_ts_ms INTEGER,
                    first_live_trade_id TEXT,
                    first_live_trade_recorded_at_ms INTEGER,
                    last_journal_trade_ts_ms INTEGER,
                    journal_trade_count INTEGER NOT NULL DEFAULT 0,
                    dropped_trades INTEGER NOT NULL DEFAULT 0,
                    writer_failures INTEGER NOT NULL DEFAULT 0,
                    finalized INTEGER NOT NULL DEFAULT 0,
                    finalized_at_ms INTEGER,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (
                        exchange, symbol, range_pct, bucket_start_ms
                    )
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn


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


def _trade_params(row: RangeRepairTrade) -> tuple[object, ...]:
    return (
        str(row.exchange).lower(),
        row.symbol,
        _decimal_text(row.range_pct),
        int(row.bucket_start_ms),
        int(row.trade_time_ms),
        row.event_time_ms,
        str(row.trade_id or ""),
        row.raw_symbol,
        row.side,
        row.price,
        row.quantity,
        row.source,
        int(row.created_at_ms),
    )


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


def _trade_from_row(row: Sequence[object]) -> RangeRepairTrade:
    return RangeRepairTrade(
        exchange=str(row[0]),
        symbol=str(row[1]),
        range_pct=str(row[2]),
        bucket_start_ms=int(row[3]),
        trade_time_ms=int(row[4]),
        event_time_ms=None if row[5] is None else int(row[5]),
        trade_id=None if not str(row[6] or "") else str(row[6]),
        raw_symbol=str(row[7] or ""),
        side=str(row[8]),
        price=str(row[9]),
        quantity=str(row[10]),
        source=str(row[11]),
        created_at_ms=int(row[12]),
    )


def _state_from_row(row: Sequence[object]) -> RangeRepairJournalState:
    return RangeRepairJournalState(
        exchange=str(row[0]),
        symbol=str(row[1]),
        range_pct=str(row[2]),
        bucket_start_ms=int(row[3]),
        bucket_end_ms=int(row[4]),
        checkpoint_last_trade_ts_ms=(
            None if row[5] is None else int(row[5])
        ),
        checkpoint_last_trade_id=(
            None if row[6] is None else str(row[6])
        ),
        first_live_trade_ts_ms=(
            None if row[7] is None else int(row[7])
        ),
        first_live_trade_id=None if row[8] is None else str(row[8]),
        first_live_trade_recorded_at_ms=(
            None if row[9] is None else int(row[9])
        ),
        last_journal_trade_ts_ms=(
            None if row[10] is None else int(row[10])
        ),
        journal_trade_count=int(row[11]),
        dropped_trades=int(row[12]),
        writer_failures=int(row[13]),
        finalized=bool(row[14]),
        finalized_at_ms=None if row[15] is None else int(row[15]),
        status=str(row[16]),
        last_error=None if row[17] is None else str(row[17]),
        updated_at_ms=int(row[18]),
    )


def _decimal_text(value: object) -> str:
    from decimal import Decimal

    return format(Decimal(str(value)).normalize(), "f")


def _now_ms() -> int:
    return int(time.time() * 1000)
