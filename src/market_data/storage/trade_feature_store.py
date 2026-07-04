from __future__ import annotations

import logging
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from src.market_data.models import (
    FixedTimeTradeBar,
    TimeRange,
    TradeDerivedFeatureCoverage,
    TradeFeatureQuality,
)

logger = logging.getLogger(__name__)


class SqliteTradeFeatureStore:
    """SQLite repository for 1m trade-derived features.

    Stores aggregated bar + footprint features (never raw trades). Schema
    uses WAL + synchronous=NORMAL for write performance.
    """

    def __init__(self, path: str | Path = "data/market_data/aether_market_data.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def upsert_many(self, rows: Sequence[FixedTimeTradeBar]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO tradebar_1m_features (
                    exchange, symbol, timeframe, open_time_ms, close_time_ms,
                    available_time_ms, open, high, low, close,
                    volume, buy_volume, sell_volume,
                    buy_notional, sell_notional,
                    delta_volume, delta_notional, abs_delta_notional,
                    trade_count, large_buy_notional, large_sell_notional,
                    large_trade_count, large_trade_share,
                    quality, source
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
                ON CONFLICT(exchange, symbol, timeframe, open_time_ms) DO UPDATE SET
                    close_time_ms=excluded.close_time_ms,
                    available_time_ms=excluded.available_time_ms,
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    buy_volume=excluded.buy_volume,
                    sell_volume=excluded.sell_volume,
                    buy_notional=excluded.buy_notional,
                    sell_notional=excluded.sell_notional,
                    delta_volume=excluded.delta_volume,
                    delta_notional=excluded.delta_notional,
                    abs_delta_notional=excluded.abs_delta_notional,
                    trade_count=excluded.trade_count,
                    large_buy_notional=excluded.large_buy_notional,
                    large_sell_notional=excluded.large_sell_notional,
                    large_trade_count=excluded.large_trade_count,
                    large_trade_share=excluded.large_trade_share,
                    quality=excluded.quality,
                    source=excluded.source
                """,
                [_tradebar_params(row) for row in rows],
            )
        return len(rows)

    def replace_range(self, time_range: TimeRange, rows: Sequence[FixedTimeTradeBar]) -> int:
        """Replace all bars within a time range with the given rows."""
        if not rows:
            return 0
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM tradebar_1m_features
                WHERE open_time_ms >= ? AND open_time_ms <= ?
                """,
                (time_range.start_time_ms, time_range.end_time_ms),
            )
            conn.executemany(
                """
                INSERT INTO tradebar_1m_features (
                    exchange, symbol, timeframe, open_time_ms, close_time_ms,
                    available_time_ms, open, high, low, close,
                    volume, buy_volume, sell_volume,
                    buy_notional, sell_notional,
                    delta_volume, delta_notional, abs_delta_notional,
                    trade_count, large_buy_notional, large_sell_notional,
                    large_trade_count, large_trade_share,
                    quality, source
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?
                )
                """,
                [_tradebar_params(row) for row in rows],
            )
        return len(rows)

    def mark_coverage(self, *, symbol: str, exchange: str, time_range: TimeRange, complete: bool = True) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO trade_feature_coverage
                    (symbol, exchange, start_time_ms, end_time_ms, complete, updated_at_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    exchange,
                    time_range.start_time_ms,
                    time_range.end_time_ms,
                    1 if complete else 0,
                    _now_ms(),
                ),
            )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load_recent(
        self,
        *,
        symbol: str,
        exchange: str,
        limit: int = 4320,
    ) -> list[FixedTimeTradeBar]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, timeframe, open_time_ms, close_time_ms,
                       available_time_ms, open, high, low, close,
                       volume, buy_volume, sell_volume,
                       buy_notional, sell_notional,
                       delta_volume, delta_notional, abs_delta_notional,
                       trade_count, large_buy_notional, large_sell_notional,
                       large_trade_count, large_trade_share,
                       quality, source
                FROM tradebar_1m_features
                WHERE symbol = ? AND exchange = ?
                ORDER BY open_time_ms DESC
                LIMIT ?
                """,
                (symbol, exchange, max(1, int(limit))),
            ).fetchall()
        bars = [_row_to_tradebar(row) for row in rows]
        bars.reverse()
        return bars

    def load_range(self, *, symbol: str, exchange: str, time_range: TimeRange) -> list[FixedTimeTradeBar]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, timeframe, open_time_ms, close_time_ms,
                       available_time_ms, open, high, low, close,
                       volume, buy_volume, sell_volume,
                       buy_notional, sell_notional,
                       delta_volume, delta_notional, abs_delta_notional,
                       trade_count, large_buy_notional, large_sell_notional,
                       large_trade_count, large_trade_share,
                       quality, source
                FROM tradebar_1m_features
                WHERE symbol = ? AND exchange = ?
                  AND open_time_ms >= ? AND open_time_ms <= ?
                ORDER BY open_time_ms ASC
                """,
                (symbol, exchange, time_range.start_time_ms, time_range.end_time_ms),
            ).fetchall()
        return [_row_to_tradebar(row) for row in rows]

    def latest_complete_close_time_ms(self, *, symbol: str, exchange: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(close_time_ms)
                FROM tradebar_1m_features
                WHERE symbol = ? AND exchange = ?
                """,
                (symbol, exchange),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    # ------------------------------------------------------------------
    # Coverage
    # ------------------------------------------------------------------

    def coverage_scan(
        self,
        *,
        symbol: str,
        exchange: str,
        required_minutes: int = 4320,
        current_day_archive_ready: bool = True,
        extra: dict | None = None,
    ) -> TradeDerivedFeatureCoverage:
        latest = self.latest_complete_close_time_ms(symbol=symbol, exchange=exchange)
        if latest is None:
            return TradeDerivedFeatureCoverage(
                symbol=symbol,
                exchange=exchange,
                required_minutes=required_minutes,
                complete_minutes=0,
                missing_minutes=required_minutes,
                degraded_minutes=0,
                latest_complete_close_time_ms=None,
                first_missing_range=None,
                available=False,
                reason="no_features_stored",
                extra=extra,
            )

        end_ms = latest
        start_ms = end_ms - (required_minutes * 60_000) + 1

        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT open_time_ms, quality
                FROM tradebar_1m_features
                WHERE symbol = ? AND exchange = ?
                  AND open_time_ms >= ? AND open_time_ms <= ?
                ORDER BY open_time_ms ASC
                """,
                (symbol, exchange, start_ms, end_ms),
            ).fetchall()

        stored: dict[int, str] = {int(r[0]): str(r[1]) for r in rows}

        complete = 0
        degraded = 0
        missing = 0
        first_missing: tuple[int, int] | None = None

        bucket = _bucket_start_ms(start_ms)
        end_bucket = _bucket_start_ms(end_ms)
        while bucket <= end_bucket:
            quality = stored.get(bucket)
            if quality is None:
                missing += 1
                if first_missing is None:
                    first_missing = (bucket, bucket + 60_000 - 1)
            elif quality == TradeFeatureQuality.COMPLETE.value:
                complete += 1
            else:
                degraded += 1
            bucket += 60_000

        available = missing == 0

        reason = ""
        if not available:
            parts = []
            if missing > 0:
                parts.append(f"missing={missing}")
            if degraded > 0:
                parts.append(f"degraded={degraded}")
            if not current_day_archive_ready:
                parts.append("current_day_archive_not_ready")
            reason = "; ".join(parts)

        return TradeDerivedFeatureCoverage(
            symbol=symbol,
            exchange=exchange,
            required_minutes=required_minutes,
            complete_minutes=complete,
            missing_minutes=missing,
            degraded_minutes=degraded,
            latest_complete_close_time_ms=latest,
            first_missing_range=first_missing,
            available=available,
            reason=reason,
            extra=extra,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tradebar_1m_features (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL DEFAULT '1m',
                    open_time_ms INTEGER NOT NULL,
                    close_time_ms INTEGER NOT NULL,
                    available_time_ms INTEGER NOT NULL,
                    open TEXT NOT NULL,
                    high TEXT NOT NULL,
                    low TEXT NOT NULL,
                    close TEXT NOT NULL,
                    volume TEXT NOT NULL,
                    buy_volume TEXT NOT NULL,
                    sell_volume TEXT NOT NULL,
                    buy_notional TEXT NOT NULL,
                    sell_notional TEXT NOT NULL,
                    delta_volume TEXT NOT NULL,
                    delta_notional TEXT NOT NULL,
                    abs_delta_notional TEXT NOT NULL,
                    trade_count INTEGER NOT NULL DEFAULT 0,
                    large_buy_notional TEXT NOT NULL,
                    large_sell_notional TEXT NOT NULL,
                    large_trade_count INTEGER NOT NULL DEFAULT 0,
                    large_trade_share TEXT NOT NULL,
                    quality TEXT NOT NULL DEFAULT 'COMPLETE',
                    source TEXT NOT NULL DEFAULT 'trade_derived',
                    PRIMARY KEY (exchange, symbol, timeframe, open_time_ms)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tradebar_1m_close_time
                ON tradebar_1m_features(exchange, symbol, timeframe, close_time_ms)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tradebar_1m_available_time
                ON tradebar_1m_features(exchange, symbol, timeframe, available_time_ms)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_feature_coverage (
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    start_time_ms INTEGER NOT NULL,
                    end_time_ms INTEGER NOT NULL,
                    complete INTEGER NOT NULL DEFAULT 1,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY(symbol, exchange, start_time_ms, end_time_ms)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_trade_feature_coverage_lookup
                ON trade_feature_coverage(symbol, exchange, start_time_ms, end_time_ms)
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn


def _tradebar_params(bar: FixedTimeTradeBar) -> tuple[object, ...]:
    return (
        bar.exchange,
        bar.symbol,
        bar.timeframe,
        bar.open_time_ms,
        bar.close_time_ms,
        bar.available_time_ms,
        _dec(bar.open),
        _dec(bar.high),
        _dec(bar.low),
        _dec(bar.close),
        _dec(bar.volume),
        _dec(bar.buy_volume),
        _dec(bar.sell_volume),
        _dec(bar.buy_notional),
        _dec(bar.sell_notional),
        _dec(bar.delta_volume),
        _dec(bar.delta_notional),
        _dec(bar.abs_delta_notional),
        bar.trade_count,
        _dec(bar.large_buy_notional),
        _dec(bar.large_sell_notional),
        bar.large_trade_count,
        _dec(bar.large_trade_share),
        bar.quality,
        bar.source,
    )


def _row_to_tradebar(row: tuple[object, ...]) -> FixedTimeTradeBar:
    return FixedTimeTradeBar(
        exchange=str(row[0]),
        symbol=str(row[1]),
        timeframe=str(row[2]),
        open_time_ms=int(row[3]),
        close_time_ms=int(row[4]),
        available_time_ms=int(row[5]),
        open=Decimal(str(row[6])),
        high=Decimal(str(row[7])),
        low=Decimal(str(row[8])),
        close=Decimal(str(row[9])),
        volume=Decimal(str(row[10])),
        buy_volume=Decimal(str(row[11])),
        sell_volume=Decimal(str(row[12])),
        buy_notional=Decimal(str(row[13])),
        sell_notional=Decimal(str(row[14])),
        delta_volume=Decimal(str(row[15])),
        delta_notional=Decimal(str(row[16])),
        abs_delta_notional=Decimal(str(row[17])),
        trade_count=int(row[18]),
        large_buy_notional=Decimal(str(row[19])),
        large_sell_notional=Decimal(str(row[20])),
        large_trade_count=int(row[21]),
        large_trade_share=Decimal(str(row[22])),
        quality=str(row[23]),
        source=str(row[24]),
    )


def _dec(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


def _bucket_start_ms(time_ms: int) -> int:
    return (time_ms // 60_000) * 60_000
