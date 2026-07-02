from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Sequence

from src.market_data.range_repair.models import (
    DEFAULT_RANGE_REPAIR_JOURNAL_DB,
    JOURNAL_FINALIZED,
    JOURNAL_INVALID_DROPPED_TRADE,
    JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
    JOURNAL_INVALID_WRITER_ERROR,
    JOURNAL_OPEN,
    RangeRepairJournalState,
    RangeRepairTrade,
    journal_status_is_invalid,
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
