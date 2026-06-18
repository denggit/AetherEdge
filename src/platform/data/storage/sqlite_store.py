from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

from src.platform.data.models import (
    MarketDataSource,
    MarketKline,
    MarketOrderBook,
    MarketTrade,
    OrderBookLevel,
    TradeSide,
)
from src.platform.exchanges.models import ExchangeName


class SqliteMarketDataStore:
    """Small local SQLite cache for market data.

    It is intentionally simple: one writer per process is enough for the first
    live runtime. If later multiple processes write concurrently, move this
    behind a dedicated data service.
    """

    def __init__(self, path: str | Path = "data/cache/market_data.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save_klines(self, rows: list[MarketKline]) -> None:
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO klines (
                    exchange, symbol, raw_symbol, interval, open_time_ms, close_time_ms,
                    open, high, low, close, volume, quote_volume, is_closed, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.exchange.value,
                        row.symbol,
                        row.raw_symbol,
                        row.interval,
                        row.open_time_ms,
                        row.close_time_ms,
                        str(row.open),
                        str(row.high),
                        str(row.low),
                        str(row.close),
                        str(row.volume),
                        None if row.quote_volume is None else str(row.quote_volume),
                        1 if row.is_closed else 0,
                        row.source.value,
                        json.dumps(row.raw, separators=(",", ":"), default=str),
                    )
                    for row in rows
                ],
            )

    def load_klines(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketKline]:
        where = ["exchange = ?", "symbol = ?", "interval = ?"]
        params: list[Any] = [exchange.value, symbol, interval]
        if start_time_ms is not None:
            where.append("open_time_ms >= ?")
            params.append(start_time_ms)
        if end_time_ms is not None:
            where.append("open_time_ms <= ?")
            params.append(end_time_ms)
        params.append(limit)
        sql = f"""
            SELECT exchange, symbol, raw_symbol, interval, open_time_ms, close_time_ms,
                   open, high, low, close, volume, quote_volume, is_closed, source, raw_json
            FROM klines
            WHERE {' AND '.join(where)}
            ORDER BY open_time_ms DESC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return list(reversed([_row_to_kline(row) for row in rows]))

    def save_trade(self, trade: MarketTrade) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO trades (
                    exchange, symbol, raw_symbol, trade_id, event_time_ms, trade_time_ms,
                    price, quantity, side, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.exchange.value,
                    trade.symbol,
                    trade.raw_symbol,
                    trade.trade_id,
                    trade.event_time_ms,
                    trade.trade_time_ms,
                    str(trade.price),
                    str(trade.quantity),
                    trade.side.value,
                    trade.source.value,
                    json.dumps(trade.raw, separators=(",", ":"), default=str),
                ),
            )

    def load_trades(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketTrade]:
        where = ["exchange = ?", "symbol = ?"]
        params: list[Any] = [exchange.value, symbol]
        if start_time_ms is not None:
            where.append("trade_time_ms >= ?")
            params.append(start_time_ms)
        if end_time_ms is not None:
            where.append("trade_time_ms <= ?")
            params.append(end_time_ms)
        params.append(limit)
        sql = f"""
            SELECT exchange, symbol, raw_symbol, trade_id, event_time_ms, trade_time_ms,
                   price, quantity, side, source, raw_json
            FROM trades
            WHERE {' AND '.join(where)}
            ORDER BY trade_time_ms DESC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return list(reversed([_row_to_trade(row) for row in rows]))

    def save_order_book(self, order_book: MarketOrderBook) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO order_books (
                    exchange, symbol, raw_symbol, event_time_ms, bids_json, asks_json, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_book.exchange.value,
                    order_book.symbol,
                    order_book.raw_symbol,
                    order_book.event_time_ms,
                    _levels_to_json(order_book.bids),
                    _levels_to_json(order_book.asks),
                    order_book.source.value,
                    json.dumps(order_book.raw, separators=(",", ":"), default=str),
                ),
            )

    def load_order_books(
        self,
        *,
        exchange: ExchangeName,
        symbol: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[MarketOrderBook]:
        where = ["exchange = ?", "symbol = ?"]
        params: list[Any] = [exchange.value, symbol]
        if start_time_ms is not None:
            where.append("event_time_ms >= ?")
            params.append(start_time_ms)
        if end_time_ms is not None:
            where.append("event_time_ms <= ?")
            params.append(end_time_ms)
        params.append(limit)
        sql = f"""
            SELECT exchange, symbol, raw_symbol, event_time_ms, bids_json, asks_json, source, raw_json
            FROM order_books
            WHERE {' AND '.join(where)}
            ORDER BY event_time_ms DESC, id DESC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return list(reversed([_row_to_order_book(row) for row in rows]))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
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
                    PRIMARY KEY (exchange, symbol, interval, open_time_ms)
                );
                CREATE INDEX IF NOT EXISTS idx_klines_lookup
                    ON klines (exchange, symbol, interval, open_time_ms);

                CREATE TABLE IF NOT EXISTS trades (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    raw_symbol TEXT NOT NULL,
                    trade_id TEXT,
                    event_time_ms INTEGER,
                    trade_time_ms INTEGER,
                    price TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    side TEXT NOT NULL,
                    source TEXT NOT NULL,
                    raw_json TEXT NOT NULL,
                    PRIMARY KEY (exchange, raw_symbol, trade_id)
                );
                CREATE INDEX IF NOT EXISTS idx_trades_lookup
                    ON trades (exchange, symbol, trade_time_ms);

                CREATE TABLE IF NOT EXISTS order_books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    raw_symbol TEXT NOT NULL,
                    event_time_ms INTEGER,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL,
                    source TEXT NOT NULL,
                    raw_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_order_books_lookup
                    ON order_books (exchange, symbol, event_time_ms);
                """
            )


def _row_to_kline(row: sqlite3.Row | tuple[Any, ...]) -> MarketKline:
    raw_json = row[14]
    return MarketKline(
        exchange=ExchangeName(row[0]),
        symbol=row[1],
        raw_symbol=row[2],
        interval=row[3],
        open_time_ms=int(row[4]),
        close_time_ms=int(row[5]),
        open=Decimal(row[6]),
        high=Decimal(row[7]),
        low=Decimal(row[8]),
        close=Decimal(row[9]),
        volume=Decimal(row[10]),
        quote_volume=None if row[11] is None else Decimal(row[11]),
        is_closed=bool(row[12]),
        source=MarketDataSource(row[13]),
        raw=json.loads(raw_json) if raw_json else {},
    )


def _row_to_trade(row: sqlite3.Row | tuple[Any, ...]) -> MarketTrade:
    raw_json = row[10]
    return MarketTrade(
        exchange=ExchangeName(row[0]),
        symbol=row[1],
        raw_symbol=row[2],
        trade_id=row[3],
        event_time_ms=None if row[4] is None else int(row[4]),
        trade_time_ms=None if row[5] is None else int(row[5]),
        price=Decimal(row[6]),
        quantity=Decimal(row[7]),
        side=TradeSide(row[8]),
        source=MarketDataSource(row[9]),
        raw=json.loads(raw_json) if raw_json else {},
    )


def _row_to_order_book(row: sqlite3.Row | tuple[Any, ...]) -> MarketOrderBook:
    raw_json = row[7]
    return MarketOrderBook(
        exchange=ExchangeName(row[0]),
        symbol=row[1],
        raw_symbol=row[2],
        event_time_ms=None if row[3] is None else int(row[3]),
        bids=_json_to_levels(row[4]),
        asks=_json_to_levels(row[5]),
        source=MarketDataSource(row[6]),
        raw=json.loads(raw_json) if raw_json else {},
    )


def _json_to_levels(payload: str) -> list[OrderBookLevel]:
    return [
        OrderBookLevel(price=Decimal(str(price)), quantity=Decimal(str(quantity)))
        for price, quantity in json.loads(payload)
    ]


def _levels_to_json(levels: list[OrderBookLevel] | tuple[OrderBookLevel, ...] | Any) -> str:
    return json.dumps([[str(level.price), str(level.quantity)] for level in levels], separators=(",", ":"))
