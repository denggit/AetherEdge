from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from src.market_data.models import TimeRange
from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


class SqliteTradeStore:
    """SQLite repository for normalized trades used by internal warmup."""

    def __init__(self, path: str | Path = "data/market_data/aether_market_data.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save(self, rows: Sequence[MarketTrade]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO trades (
                    trade_key, exchange, symbol, raw_symbol, price, quantity, side,
                    trade_id, event_time_ms, trade_time_ms, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_key) DO UPDATE SET
                    raw_json=excluded.raw_json
                """,
                [_trade_params(row) for row in rows],
            )
        return len(rows)

    def load(self, *, symbol: str, time_range: TimeRange) -> list[MarketTrade]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, raw_symbol, price, quantity, side,
                       trade_id, event_time_ms, trade_time_ms, source, raw_json
                FROM trades
                WHERE symbol = ? AND COALESCE(trade_time_ms, event_time_ms) BETWEEN ? AND ?
                ORDER BY COALESCE(trade_time_ms, event_time_ms), trade_key
                """,
                (symbol, time_range.start_time_ms, time_range.end_time_ms),
            ).fetchall()
        return [_row_to_trade(row) for row in rows]

    def latest_time_ms(self, *, symbol: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(COALESCE(trade_time_ms, event_time_ms))
                FROM trades
                WHERE symbol = ?
                """,
                (symbol,),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    trade_key TEXT PRIMARY KEY,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    raw_symbol TEXT NOT NULL,
                    price TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    side TEXT NOT NULL,
                    trade_id TEXT,
                    event_time_ms INTEGER,
                    trade_time_ms INTEGER,
                    source TEXT NOT NULL,
                    raw_json TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades(symbol, trade_time_ms, event_time_ms)")

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)


def _trade_params(row: MarketTrade) -> tuple[object, ...]:
    key = _trade_key(row)
    return (
        key,
        row.exchange.value,
        row.symbol,
        row.raw_symbol,
        _dec(row.price),
        _dec(row.quantity),
        row.side.value,
        row.trade_id,
        row.event_time_ms,
        row.trade_time_ms,
        row.source.value,
        json.dumps(dict(row.raw), separators=(",", ":"), ensure_ascii=False),
    )


def _trade_key(row: MarketTrade) -> str:
    if row.trade_id:
        return f"{row.exchange.value}:{row.symbol}:{row.trade_id}"
    time_ms = row.trade_time_ms if row.trade_time_ms is not None else row.event_time_ms
    return f"{row.exchange.value}:{row.symbol}:{time_ms}:{_dec(row.price)}:{_dec(row.quantity)}:{row.side.value}"


def _row_to_trade(row: tuple[object, ...]) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName(str(row[0])),
        symbol=str(row[1]),
        raw_symbol=str(row[2]),
        price=Decimal(str(row[3])),
        quantity=Decimal(str(row[4])),
        side=TradeSide(str(row[5])),
        trade_id=str(row[6]) if row[6] is not None else None,
        event_time_ms=int(row[7]) if row[7] is not None else None,
        trade_time_ms=int(row[8]) if row[8] is not None else None,
        source=MarketDataSource(str(row[9])),
        raw=json.loads(str(row[10] or "{}")),
    )


def _dec(value: Decimal) -> str:
    return format(value.normalize(), "f")
