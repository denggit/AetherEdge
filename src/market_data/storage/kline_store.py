from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from src.market_data.models import TimeRange
from src.platform.data.models import MarketDataSource, MarketKline
from src.platform.exchanges.models import ExchangeName


class SqliteKlineStore:
    """SQLite repository for normalized klines used by internal warmup.

    This store belongs to the internal market-data pipeline. It persists the
    normalized ``MarketKline`` model exposed by ``src.platform.data`` but does
    not call exchange adapters or contain strategy logic.
    """

    def __init__(self, path: str | Path = "data/market_data/aether_market_data.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save(self, rows: Sequence[MarketKline]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO klines (
                    exchange, symbol, raw_symbol, interval, open_time_ms, close_time_ms,
                    open, high, low, close, volume, quote_volume, is_closed, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, symbol, interval, open_time_ms) DO UPDATE SET
                    raw_symbol=excluded.raw_symbol,
                    close_time_ms=excluded.close_time_ms,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    quote_volume=excluded.quote_volume,
                    is_closed=excluded.is_closed,
                    source=excluded.source,
                    raw_json=excluded.raw_json
                """,
                [_kline_params(row) for row in rows],
            )
        return len(rows)

    def load(self, *, symbol: str, interval: str, time_range: TimeRange) -> list[MarketKline]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, raw_symbol, interval, open_time_ms, close_time_ms,
                       open, high, low, close, volume, quote_volume, is_closed, source, raw_json
                FROM klines
                WHERE symbol = ? AND interval = ? AND open_time_ms BETWEEN ? AND ?
                ORDER BY open_time_ms ASC
                """,
                (symbol, interval, time_range.start_time_ms, time_range.end_time_ms),
            ).fetchall()
        return [_row_to_kline(row) for row in rows]

    def latest_time_ms(self, *, symbol: str, interval: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(open_time_ms)
                FROM klines
                WHERE symbol = ? AND interval = ? AND is_closed = 1
                """,
                (symbol, interval),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS klines (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    raw_symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    open_time_ms INTEGER NOT NULL,
                    close_time_ms INTEGER NOT NULL,
                    open TEXT NOT NULL,
                    high TEXT NOT NULL,
                    low TEXT NOT NULL,
                    close TEXT NOT NULL,
                    volume TEXT NOT NULL,
                    quote_volume TEXT,
                    is_closed INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY(exchange, symbol, interval, open_time_ms)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_klines_symbol_interval_time ON klines(symbol, interval, open_time_ms)")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn


def _kline_params(row: MarketKline) -> tuple[object, ...]:
    return (
        row.exchange.value,
        row.symbol,
        row.raw_symbol,
        row.interval,
        row.open_time_ms,
        row.close_time_ms,
        _dec(row.open),
        _dec(row.high),
        _dec(row.low),
        _dec(row.close),
        _dec(row.volume),
        None if row.quote_volume is None else _dec(row.quote_volume),
        1 if row.is_closed else 0,
        row.source.value,
        json.dumps(dict(row.raw), separators=(",", ":"), ensure_ascii=False),
    )


def _row_to_kline(row: tuple[object, ...]) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName(str(row[0])),
        symbol=str(row[1]),
        raw_symbol=str(row[2]),
        interval=str(row[3]),
        open_time_ms=int(row[4]),
        close_time_ms=int(row[5]),
        open=Decimal(str(row[6])),
        high=Decimal(str(row[7])),
        low=Decimal(str(row[8])),
        close=Decimal(str(row[9])),
        volume=Decimal(str(row[10])),
        quote_volume=Decimal(str(row[11])) if row[11] is not None else None,
        is_closed=bool(row[12]),
        source=MarketDataSource(str(row[13])),
        raw=json.loads(str(row[14] or "{}")),
    )


def _dec(value: Decimal) -> str:
    return format(value.normalize(), "f")
