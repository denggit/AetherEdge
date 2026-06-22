from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from src.market_data.models import RangeBar, TimeRange


class SqliteRangeBarStore:
    """SQLite repository for reusable trade-derived range bars."""

    def __init__(self, path: str | Path = "data/market_data/aether_market_data.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save(self, rows: Sequence[RangeBar]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO range_bars (
                    symbol, range_pct, bar_id, start_time_ms, end_time_ms,
                    open, high, low, close, volume, buy_notional, sell_notional, trade_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, range_pct, bar_id) DO UPDATE SET
                    start_time_ms=excluded.start_time_ms,
                    end_time_ms=excluded.end_time_ms,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    buy_notional=excluded.buy_notional,
                    sell_notional=excluded.sell_notional,
                    trade_count=excluded.trade_count
                """,
                [_range_bar_params(row) for row in rows],
            )
        return len(rows)


    def replace_range(self, *, symbol: str, range_pct: str, time_range: TimeRange, rows: Sequence[RangeBar]) -> int:
        """Replace all range bars whose end time falls inside ``time_range``.

        Current-bucket warmup can rebuild a bucket from a more complete trade
        set after restart/catch-up. A plain upsert can leave stale higher
        ``bar_id`` rows behind when the rebuilt bucket contains fewer bars, so
        bucket rebuilds need delete-then-insert semantics.
        """
        pct = _normalize_decimal_text(Decimal(str(range_pct)))
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM range_bars
                WHERE symbol = ? AND range_pct = ? AND end_time_ms BETWEEN ? AND ?
                """,
                (symbol, pct, time_range.start_time_ms, time_range.end_time_ms),
            )
            if rows:
                conn.executemany(
                    """
                    INSERT INTO range_bars (
                        symbol, range_pct, bar_id, start_time_ms, end_time_ms,
                        open, high, low, close, volume, buy_notional, sell_notional, trade_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, range_pct, bar_id) DO UPDATE SET
                        start_time_ms=excluded.start_time_ms,
                        end_time_ms=excluded.end_time_ms,
                        open=excluded.open,
                        high=excluded.high,
                        low=excluded.low,
                        close=excluded.close,
                        volume=excluded.volume,
                        buy_notional=excluded.buy_notional,
                        sell_notional=excluded.sell_notional,
                        trade_count=excluded.trade_count
                    """,
                    [_range_bar_params(row) for row in rows],
                )
        return len(rows)

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange) -> list[RangeBar]:
        pct = _normalize_decimal_text(Decimal(str(range_pct)))
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, range_pct, bar_id, start_time_ms, end_time_ms,
                       open, high, low, close, volume, buy_notional, sell_notional, trade_count
                FROM range_bars
                WHERE symbol = ? AND range_pct = ? AND end_time_ms BETWEEN ? AND ?
                ORDER BY end_time_ms ASC, bar_id ASC
                """,
                (symbol, pct, time_range.start_time_ms, time_range.end_time_ms),
            ).fetchall()
        return [_row_to_range_bar(row) for row in rows]

    def latest_end_time_ms(self, *, symbol: str, range_pct: str) -> int | None:
        pct = _normalize_decimal_text(Decimal(str(range_pct)))
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(end_time_ms)
                FROM range_bars
                WHERE symbol = ? AND range_pct = ?
                """,
                (symbol, pct),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS range_bars (
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    bar_id INTEGER NOT NULL,
                    start_time_ms INTEGER NOT NULL,
                    end_time_ms INTEGER NOT NULL,
                    open TEXT NOT NULL,
                    high TEXT NOT NULL,
                    low TEXT NOT NULL,
                    close TEXT NOT NULL,
                    volume TEXT NOT NULL,
                    buy_notional TEXT NOT NULL,
                    sell_notional TEXT NOT NULL,
                    trade_count INTEGER NOT NULL,
                    PRIMARY KEY(symbol, range_pct, bar_id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_range_bars_lookup ON range_bars(symbol, range_pct, end_time_ms)")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn


def _range_bar_params(row: RangeBar) -> tuple[object, ...]:
    return (
        row.symbol,
        _normalize_decimal_text(row.range_pct),
        row.bar_id,
        row.start_time_ms,
        row.end_time_ms,
        _normalize_decimal_text(row.open),
        _normalize_decimal_text(row.high),
        _normalize_decimal_text(row.low),
        _normalize_decimal_text(row.close),
        _normalize_decimal_text(row.volume),
        _normalize_decimal_text(row.buy_notional),
        _normalize_decimal_text(row.sell_notional),
        row.trade_count,
    )


def _row_to_range_bar(row: tuple[object, ...]) -> RangeBar:
    return RangeBar(
        symbol=str(row[0]),
        range_pct=Decimal(str(row[1])),
        bar_id=int(row[2]),
        start_time_ms=int(row[3]),
        end_time_ms=int(row[4]),
        open=Decimal(str(row[5])),
        high=Decimal(str(row[6])),
        low=Decimal(str(row[7])),
        close=Decimal(str(row[8])),
        volume=Decimal(str(row[9])),
        buy_notional=Decimal(str(row[10])),
        sell_notional=Decimal(str(row[11])),
        trade_count=int(row[12]),
    )


def _normalize_decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")
