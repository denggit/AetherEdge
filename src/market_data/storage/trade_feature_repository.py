from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from src.market_data.models import (
    FixedTimeTradeBar,
    RangeFootprintFeature,
    TimeRange,
    TradeFootprintFeature,
)
from src.market_data.trade_features.coverage_repository import (
    FixedTimeQualityRows,
    RangeCoverageRows,
)

logger = logging.getLogger(__name__)

_ONE_MINUTE_MS = 60_000


@dataclass(frozen=True)
class LargeTradeShareSample:
    open_time_ms: int
    large_trade_share: Decimal | None
    quality: str


class SqliteTradeFeatureRepository:
    """SQLite repository for 1m trade-derived features.

    Two main tables:
    - tradebar_1m_features  (OHLCV + order-flow)
    - trade_footprint_1m_features (footprint metrics)

    Never stores raw trades. Schema uses WAL + synchronous=NORMAL.
    """

    def __init__(self, path: str | Path = "data/market_data/aether_market_data.sqlite3") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    # ==================================================================
    # TradeBar write
    # ==================================================================

    def upsert_tradebars_many(self, rows: Sequence[FixedTimeTradeBar]) -> int:
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

    def replace_range_tradebars(self, time_range: TimeRange, rows: Sequence[FixedTimeTradeBar]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM tradebar_1m_features WHERE open_time_ms >= ? AND open_time_ms <= ?",
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

    # ---- legacy aliases ----
    upsert_many = upsert_tradebars_many
    replace_range = replace_range_tradebars

    # ==================================================================
    # Footprint write
    # ==================================================================

    def upsert_footprints_many(self, rows: Sequence[TradeFootprintFeature]) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO trade_footprint_1m_features (
                    exchange, symbol, timeframe, open_time_ms, close_time_ms,
                    available_time_ms, delta_notional, abs_delta_notional,
                    taker_buy_ratio, close_pos, range_pct, return_pct,
                    fp_max_bucket_abs_delta_pressure,
                    context_available, quality, source
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(exchange, symbol, timeframe, open_time_ms) DO UPDATE SET
                    close_time_ms=excluded.close_time_ms,
                    available_time_ms=excluded.available_time_ms,
                    delta_notional=excluded.delta_notional,
                    abs_delta_notional=excluded.abs_delta_notional,
                    taker_buy_ratio=excluded.taker_buy_ratio,
                    close_pos=excluded.close_pos,
                    range_pct=excluded.range_pct,
                    return_pct=excluded.return_pct,
                    fp_max_bucket_abs_delta_pressure=excluded.fp_max_bucket_abs_delta_pressure,
                    context_available=excluded.context_available,
                    quality=excluded.quality,
                    source=excluded.source
                """,
                [_footprint_params(row) for row in rows],
            )
        return len(rows)

    # ==================================================================
    # Range footprint write
    # ==================================================================

    def upsert_range_footprints_many(
        self, rows: Sequence[RangeFootprintFeature]
    ) -> int:
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO range_footprint_features (
                    exchange, symbol, range_pct, price_step, range_bar_id,
                    range_start_ms, range_end_ms, available_time_ms,
                    fp_max_bucket_abs_delta_pressure,
                    fp_low_bucket_delta_pressure,
                    fp_high_bucket_delta_pressure,
                    fp_delta_pressure,
                    bucket_count, trade_count,
                    context_available, quality, source
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(
                    exchange, symbol, range_pct, price_step, range_bar_id
                ) DO UPDATE SET
                    range_start_ms=excluded.range_start_ms,
                    range_end_ms=excluded.range_end_ms,
                    available_time_ms=excluded.available_time_ms,
                    fp_max_bucket_abs_delta_pressure=
                        excluded.fp_max_bucket_abs_delta_pressure,
                    fp_low_bucket_delta_pressure=
                        excluded.fp_low_bucket_delta_pressure,
                    fp_high_bucket_delta_pressure=
                        excluded.fp_high_bucket_delta_pressure,
                    fp_delta_pressure=excluded.fp_delta_pressure,
                    bucket_count=excluded.bucket_count,
                    trade_count=excluded.trade_count,
                    context_available=excluded.context_available,
                    quality=excluded.quality,
                    source=excluded.source
                """,
                [_range_footprint_params(row) for row in rows],
            )
        return len(rows)

    def delete_range_footprint_window(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: Decimal | str | float,
        price_step: Decimal | str | float,
        start_ms: int,
        end_ms: int,
    ) -> None:
        range_text = _decimal_key(range_pct)
        step_text = _decimal_key(price_step)
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM range_footprint_features
                WHERE exchange = ? AND symbol = ?
                  AND range_pct = ? AND price_step = ?
                  AND available_time_ms BETWEEN ? AND ?
                """,
                (
                    exchange,
                    symbol,
                    range_text,
                    step_text,
                    int(start_ms),
                    int(end_ms),
                ),
            )
            conn.execute(
                """
                DELETE FROM range_footprint_backfill_coverage
                WHERE exchange = ? AND symbol = ?
                  AND range_pct = ? AND price_step = ?
                  AND start_time_ms <= ? AND end_time_ms >= ?
                """,
                (
                    exchange,
                    symbol,
                    range_text,
                    step_text,
                    int(end_ms),
                    int(start_ms),
                ),
            )

    def mark_range_footprint_coverage(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: Decimal | str | float,
        price_step: Decimal | str | float,
        start_ms: int,
        end_ms: int,
        complete: bool,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO range_footprint_backfill_coverage (
                    exchange, symbol, range_pct, price_step,
                    start_time_ms, end_time_ms, complete, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(
                    exchange, symbol, range_pct, price_step,
                    start_time_ms, end_time_ms
                ) DO UPDATE SET
                    complete=excluded.complete,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    exchange,
                    symbol,
                    _decimal_key(range_pct),
                    _decimal_key(price_step),
                    int(start_ms),
                    int(end_ms),
                    1 if complete else 0,
                    _now_ms(),
                ),
            )

    # ==================================================================
    # TradeBar read
    # ==================================================================

    def load_recent_tradebars(
        self, *, symbol: str, exchange: str, limit: int = 4320,
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

    def load_recent_large_trade_shares(
        self,
        *,
        symbol: str,
        exchange: str,
        limit: int,
    ) -> list[LargeTradeShareSample]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT open_time_ms, large_trade_share, quality
                FROM tradebar_1m_features
                WHERE symbol = ? AND exchange = ?
                ORDER BY open_time_ms DESC
                LIMIT ?
                """,
                (symbol, exchange, max(1, int(limit))),
            ).fetchall()
        samples = [
            LargeTradeShareSample(
                open_time_ms=int(row[0]),
                large_trade_share=(
                    None
                    if row[1] is None
                    else Decimal(str(row[1]))
                ),
                quality=str(row[2]),
            )
            for row in rows
        ]
        samples.reverse()
        return samples

    def load_range_tradebars(
        self, *, symbol: str, exchange: str, time_range: TimeRange,
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
                  AND open_time_ms >= ? AND open_time_ms <= ?
                ORDER BY open_time_ms ASC
                """,
                (symbol, exchange, time_range.start_time_ms, time_range.end_time_ms),
            ).fetchall()
        return [_row_to_tradebar(row) for row in rows]

    # ---- legacy aliases ----
    load_recent = load_recent_tradebars
    load_range = load_range_tradebars

    # ==================================================================
    # Footprint read
    # ==================================================================

    def load_recent_footprints(
        self, *, symbol: str, exchange: str, limit: int = 4320,
    ) -> list[TradeFootprintFeature]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, timeframe, open_time_ms, close_time_ms,
                       available_time_ms, delta_notional, abs_delta_notional,
                       taker_buy_ratio, close_pos, range_pct, return_pct,
                       fp_max_bucket_abs_delta_pressure,
                       context_available, quality, source
                FROM trade_footprint_1m_features
                WHERE symbol = ? AND exchange = ?
                ORDER BY open_time_ms DESC
                LIMIT ?
                """,
                (symbol, exchange, max(1, int(limit))),
            ).fetchall()
        result = [_row_to_footprint(row) for row in rows]
        result.reverse()
        return result

    def load_range_footprints(
        self, *, symbol: str, exchange: str, time_range: TimeRange,
    ) -> list[TradeFootprintFeature]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, timeframe, open_time_ms, close_time_ms,
                       available_time_ms, delta_notional, abs_delta_notional,
                       taker_buy_ratio, close_pos, range_pct, return_pct,
                       fp_max_bucket_abs_delta_pressure,
                       context_available, quality, source
                FROM trade_footprint_1m_features
                WHERE symbol = ? AND exchange = ?
                  AND open_time_ms >= ? AND open_time_ms <= ?
                ORDER BY open_time_ms ASC
                """,
                (symbol, exchange, time_range.start_time_ms, time_range.end_time_ms),
            ).fetchall()
        return [_row_to_footprint(row) for row in rows]

    def load_range_footprint_features(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
        time_range: TimeRange | None = None,
        limit: int | None = None,
    ) -> list[RangeFootprintFeature]:
        where = [
            "exchange = ?",
            "symbol = ?",
            "range_pct = ?",
            "price_step = ?",
        ]
        params: list[object] = [
            exchange,
            symbol,
            _decimal_key(range_pct),
            _decimal_key(price_step),
        ]
        if time_range is not None:
            where.append("available_time_ms BETWEEN ? AND ?")
            params.extend(
                [time_range.start_time_ms, time_range.end_time_ms]
            )
        sql = f"""
            SELECT exchange, symbol, range_pct, price_step, range_bar_id,
                   range_start_ms, range_end_ms, available_time_ms,
                   fp_max_bucket_abs_delta_pressure,
                   fp_low_bucket_delta_pressure,
                   fp_high_bucket_delta_pressure,
                   fp_delta_pressure,
                   bucket_count, trade_count,
                   context_available, quality, source
            FROM range_footprint_features
            WHERE {' AND '.join(where)}
            ORDER BY available_time_ms ASC, range_bar_id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(max(1, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_row_to_range_footprint(row) for row in rows]

    def load_latest_range_footprint_context(
        self,
        *,
        symbol: str,
        exchange: str,
        cutoff_ms: int,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
    ) -> RangeFootprintFeature | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT exchange, symbol, range_pct, price_step, range_bar_id,
                       range_start_ms, range_end_ms, available_time_ms,
                       fp_max_bucket_abs_delta_pressure,
                       fp_low_bucket_delta_pressure,
                       fp_high_bucket_delta_pressure,
                       fp_delta_pressure,
                       bucket_count, trade_count,
                       context_available, quality, source
                FROM range_footprint_features
                WHERE exchange = ? AND symbol = ?
                  AND range_pct = ? AND price_step = ?
                  AND available_time_ms <= ?
                  AND quality = 'COMPLETE'
                  AND context_available = 1
                ORDER BY available_time_ms DESC, range_bar_id DESC
                LIMIT 1
                """,
                (
                    exchange,
                    symbol,
                    _decimal_key(range_pct),
                    _decimal_key(price_step),
                    int(cutoff_ms),
                ),
            ).fetchone()
        if row is None:
            return None
        return _row_to_range_footprint(row)

    # ==================================================================
    # Common
    # ==================================================================

    def latest_any_tradebar_close_time_ms(
        self, *, symbol: str, exchange: str
    ) -> int | None:
        return self._time_scalar(
            """
            SELECT MAX(close_time_ms)
            FROM tradebar_1m_features
            WHERE symbol = ? AND exchange = ?
            """,
            symbol=symbol,
            exchange=exchange,
        )

    def latest_any_footprint_close_time_ms(
        self, *, symbol: str, exchange: str
    ) -> int | None:
        return self._time_scalar(
            """
            SELECT MAX(close_time_ms)
            FROM trade_footprint_1m_features
            WHERE symbol = ? AND exchange = ?
            """,
            symbol=symbol,
            exchange=exchange,
        )

    def earliest_any_tradebar_open_time_ms(
        self, *, symbol: str, exchange: str
    ) -> int | None:
        return self._time_scalar(
            """
            SELECT MIN(open_time_ms)
            FROM tradebar_1m_features
            WHERE symbol = ? AND exchange = ?
            """,
            symbol=symbol,
            exchange=exchange,
        )

    def earliest_any_footprint_open_time_ms(
        self, *, symbol: str, exchange: str
    ) -> int | None:
        return self._time_scalar(
            """
            SELECT MIN(open_time_ms)
            FROM trade_footprint_1m_features
            WHERE symbol = ? AND exchange = ?
            """,
            symbol=symbol,
            exchange=exchange,
        )

    def latest_any_range_footprint_available_time_ms(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
    ) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(available_time_ms)
                FROM range_footprint_features
                WHERE symbol = ? AND exchange = ?
                  AND range_pct = ? AND price_step = ?
                """,
                (
                    symbol,
                    exchange,
                    _decimal_key(range_pct),
                    _decimal_key(price_step),
                ),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def load_fixed_time_quality_rows(
        self,
        *,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
    ) -> FixedTimeQualityRows:
        with self._connect() as conn:
            tradebars = conn.execute(
                """
                SELECT open_time_ms, quality
                FROM tradebar_1m_features
                WHERE symbol = ? AND exchange = ?
                  AND open_time_ms >= ? AND open_time_ms <= ?
                ORDER BY open_time_ms ASC
                """,
                (symbol, exchange, int(start_ms), int(end_ms)),
            ).fetchall()
            footprints = conn.execute(
                """
                SELECT open_time_ms, quality, context_available
                FROM trade_footprint_1m_features
                WHERE symbol = ? AND exchange = ?
                  AND open_time_ms >= ? AND open_time_ms <= ?
                ORDER BY open_time_ms ASC
                """,
                (symbol, exchange, int(start_ms), int(end_ms)),
            ).fetchall()
        return FixedTimeQualityRows(
            tradebars=tuple(
                (int(row[0]), str(row[1])) for row in tradebars
            ),
            footprints=tuple(
                (int(row[0]), str(row[1]), bool(row[2]))
                for row in footprints
            ),
        )

    def load_range_coverage_rows(
        self,
        *,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
    ) -> RangeCoverageRows:
        range_text = _decimal_key(range_pct)
        step_text = _decimal_key(price_step)
        with self._connect() as conn:
            feature_rows = conn.execute(
                """
                SELECT available_time_ms, quality, context_available
                FROM range_footprint_features
                WHERE symbol = ? AND exchange = ?
                  AND range_pct = ? AND price_step = ?
                  AND available_time_ms BETWEEN ? AND ?
                ORDER BY available_time_ms ASC
                """,
                (
                    symbol,
                    exchange,
                    range_text,
                    step_text,
                    int(start_ms),
                    int(end_ms),
                ),
            ).fetchall()
            latest_row = conn.execute(
                """
                SELECT MAX(available_time_ms)
                FROM range_footprint_features
                WHERE symbol = ? AND exchange = ?
                  AND range_pct = ? AND price_step = ?
                  AND available_time_ms <= ?
                  AND quality = 'COMPLETE'
                  AND context_available = 1
                """,
                (
                    symbol,
                    exchange,
                    range_text,
                    step_text,
                    int(end_ms),
                ),
            ).fetchone()
            seed_row = conn.execute(
                """
                SELECT MAX(available_time_ms)
                FROM range_footprint_features
                WHERE symbol = ? AND exchange = ?
                  AND range_pct = ? AND price_step = ?
                  AND available_time_ms <= ?
                  AND quality = 'COMPLETE'
                  AND context_available = 1
                """,
                (
                    symbol,
                    exchange,
                    range_text,
                    step_text,
                    int(start_ms),
                ),
            ).fetchone()
            coverage_rows = conn.execute(
                """
                SELECT start_time_ms, end_time_ms
                FROM range_footprint_backfill_coverage
                WHERE symbol = ? AND exchange = ?
                  AND range_pct = ? AND price_step = ?
                  AND complete = 1
                  AND start_time_ms <= ? AND end_time_ms >= ?
                ORDER BY start_time_ms ASC
                """,
                (
                    symbol,
                    exchange,
                    range_text,
                    step_text,
                    int(end_ms),
                    int(start_ms),
                ),
            ).fetchall()

        latest_complete = (
            None
            if latest_row is None or latest_row[0] is None
            else int(latest_row[0])
        )
        context_seed = (
            None
            if seed_row is None or seed_row[0] is None
            else int(seed_row[0])
        )
        return RangeCoverageRows(
            features=tuple(
                (int(row[0]), str(row[1]), bool(row[2]))
                for row in feature_rows
            ),
            latest_complete_time_ms=latest_complete,
            context_seed_time_ms=context_seed,
            complete_intervals=tuple(
                (int(row[0]), int(row[1])) for row in coverage_rows
            ),
            range_pct=range_text,
            price_step=step_text,
        )

    def tradebar_without_footprint_bounds(
        self, *, symbol: str, exchange: str, end_ms: int
    ) -> tuple[int, int] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(tb.open_time_ms), MAX(tb.open_time_ms)
                FROM tradebar_1m_features tb
                LEFT JOIN trade_footprint_1m_features fp
                  ON fp.exchange = tb.exchange
                 AND fp.symbol = tb.symbol
                 AND fp.timeframe = tb.timeframe
                 AND fp.open_time_ms = tb.open_time_ms
                WHERE tb.symbol = ? AND tb.exchange = ?
                  AND tb.close_time_ms <= ?
                  AND fp.open_time_ms IS NULL
                """,
                (symbol, exchange, int(end_ms)),
            ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            return None
        return int(row[0]), int(row[1]) + _ONE_MINUTE_MS - 1

    def degraded_footprint_bounds(
        self, *, symbol: str, exchange: str, end_ms: int
    ) -> tuple[int, int] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MIN(open_time_ms), MAX(open_time_ms)
                FROM trade_footprint_1m_features
                WHERE symbol = ? AND exchange = ?
                  AND close_time_ms <= ?
                  AND (quality != 'COMPLETE' OR context_available != 1)
                """,
                (symbol, exchange, int(end_ms)),
            ).fetchone()
        if row is None or row[0] is None or row[1] is None:
            return None
        return int(row[0]), int(row[1]) + _ONE_MINUTE_MS - 1

    def latest_complete_close_time_ms(self, *, symbol: str, exchange: str) -> int | None:
        """Latest close_time_ms where BOTH tradebar and footprint exist and are COMPLETE."""
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(tb.close_time_ms)
                FROM tradebar_1m_features tb
                INNER JOIN trade_footprint_1m_features fp
                  ON fp.exchange = tb.exchange
                 AND fp.symbol = tb.symbol
                 AND fp.timeframe = tb.timeframe
                 AND fp.open_time_ms = tb.open_time_ms
                WHERE tb.symbol = ? AND tb.exchange = ?
                  AND tb.quality = 'COMPLETE'
                  AND fp.quality = 'COMPLETE'
                  AND fp.context_available = 1
                """,
                (symbol, exchange),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])

    def _time_scalar(
        self, query: str, *, symbol: str, exchange: str
    ) -> int | None:
        with self._connect() as conn:
            row = conn.execute(query, (symbol, exchange)).fetchone()
        if row is None or row[0] is None:
            return None
        return int(row[0])
    # ==================================================================
    # Schema
    # ==================================================================

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
            # Footprint table
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_footprint_1m_features (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL DEFAULT '1m',
                    open_time_ms INTEGER NOT NULL,
                    close_time_ms INTEGER NOT NULL,
                    available_time_ms INTEGER NOT NULL,
                    delta_notional TEXT NOT NULL,
                    abs_delta_notional TEXT NOT NULL,
                    taker_buy_ratio TEXT NOT NULL,
                    close_pos TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    return_pct TEXT NOT NULL,
                    fp_max_bucket_abs_delta_pressure TEXT NOT NULL,
                    context_available INTEGER NOT NULL DEFAULT 1,
                    quality TEXT NOT NULL DEFAULT 'COMPLETE',
                    source TEXT NOT NULL DEFAULT 'trade_derived',
                    PRIMARY KEY (exchange, symbol, timeframe, open_time_ms)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_footprint_1m_close_time
                ON trade_footprint_1m_features(exchange, symbol, timeframe, close_time_ms)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_footprint_1m_available_time
                ON trade_footprint_1m_features(exchange, symbol, timeframe, available_time_ms)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS range_footprint_features (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    price_step TEXT NOT NULL,
                    range_bar_id INTEGER NOT NULL,
                    range_start_ms INTEGER NOT NULL,
                    range_end_ms INTEGER NOT NULL,
                    available_time_ms INTEGER NOT NULL,
                    fp_max_bucket_abs_delta_pressure TEXT NOT NULL,
                    fp_low_bucket_delta_pressure TEXT NOT NULL,
                    fp_high_bucket_delta_pressure TEXT NOT NULL,
                    fp_delta_pressure TEXT NOT NULL,
                    bucket_count INTEGER NOT NULL DEFAULT 0,
                    trade_count INTEGER NOT NULL DEFAULT 0,
                    context_available INTEGER NOT NULL DEFAULT 1,
                    quality TEXT NOT NULL DEFAULT 'COMPLETE',
                    source TEXT NOT NULL DEFAULT 'trade_derived_range_footprint',
                    PRIMARY KEY (
                        exchange, symbol, range_pct, price_step, range_bar_id
                    )
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_range_footprint_available
                ON range_footprint_features(
                    exchange, symbol, range_pct, price_step, available_time_ms
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS range_footprint_backfill_coverage (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    price_step TEXT NOT NULL,
                    start_time_ms INTEGER NOT NULL,
                    end_time_ms INTEGER NOT NULL,
                    complete INTEGER NOT NULL DEFAULT 0,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (
                        exchange, symbol, range_pct, price_step,
                        start_time_ms, end_time_ms
                    )
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_range_footprint_coverage_lookup
                ON range_footprint_backfill_coverage(
                    exchange, symbol, range_pct, price_step,
                    start_time_ms, end_time_ms
                )
                """
            )
            # Coverage marker table
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


# ==================================================================
# Serialisation helpers
# ==================================================================

def _tradebar_params(bar: FixedTimeTradeBar) -> tuple[object, ...]:
    return (
        bar.exchange, bar.symbol, bar.timeframe,
        bar.open_time_ms, bar.close_time_ms, bar.available_time_ms,
        _dec(bar.open), _dec(bar.high), _dec(bar.low), _dec(bar.close),
        _dec(bar.volume), _dec(bar.buy_volume), _dec(bar.sell_volume),
        _dec(bar.buy_notional), _dec(bar.sell_notional),
        _dec(bar.delta_volume), _dec(bar.delta_notional), _dec(bar.abs_delta_notional),
        bar.trade_count,
        _dec(bar.large_buy_notional), _dec(bar.large_sell_notional),
        bar.large_trade_count, _dec(bar.large_trade_share),
        bar.quality, bar.source,
    )


def _row_to_tradebar(row: tuple[object, ...]) -> FixedTimeTradeBar:
    return FixedTimeTradeBar(
        exchange=str(row[0]), symbol=str(row[1]), timeframe=str(row[2]),
        open_time_ms=int(row[3]), close_time_ms=int(row[4]), available_time_ms=int(row[5]),
        open=Decimal(str(row[6])), high=Decimal(str(row[7])),
        low=Decimal(str(row[8])), close=Decimal(str(row[9])),
        volume=Decimal(str(row[10])), buy_volume=Decimal(str(row[11])),
        sell_volume=Decimal(str(row[12])),
        buy_notional=Decimal(str(row[13])), sell_notional=Decimal(str(row[14])),
        delta_volume=Decimal(str(row[15])), delta_notional=Decimal(str(row[16])),
        abs_delta_notional=Decimal(str(row[17])),
        trade_count=int(row[18]),
        large_buy_notional=Decimal(str(row[19])), large_sell_notional=Decimal(str(row[20])),
        large_trade_count=int(row[21]), large_trade_share=Decimal(str(row[22])),
        quality=str(row[23]), source=str(row[24]),
    )


def _footprint_params(fp: TradeFootprintFeature) -> tuple[object, ...]:
    return (
        fp.exchange, fp.symbol, fp.timeframe,
        fp.open_time_ms, fp.close_time_ms, fp.available_time_ms,
        _dec(fp.delta_notional), _dec(fp.abs_delta_notional),
        _dec(fp.taker_buy_ratio), _dec(fp.close_pos),
        _dec(fp.range_pct), _dec(fp.return_pct),
        _dec(fp.fp_max_bucket_abs_delta_pressure),
        1 if fp.context_available else 0,
        fp.quality, fp.source,
    )


def _row_to_footprint(row: tuple[object, ...]) -> TradeFootprintFeature:
    return TradeFootprintFeature(
        exchange=str(row[0]), symbol=str(row[1]), timeframe=str(row[2]),
        open_time_ms=int(row[3]), close_time_ms=int(row[4]), available_time_ms=int(row[5]),
        delta_notional=Decimal(str(row[6])), abs_delta_notional=Decimal(str(row[7])),
        taker_buy_ratio=Decimal(str(row[8])), close_pos=Decimal(str(row[9])),
        range_pct=Decimal(str(row[10])), return_pct=Decimal(str(row[11])),
        fp_max_bucket_abs_delta_pressure=Decimal(str(row[12])),
        context_available=bool(row[13]),
        quality=str(row[14]), source=str(row[15]),
    )


def _range_footprint_params(
    feature: RangeFootprintFeature,
) -> tuple[object, ...]:
    return (
        feature.exchange,
        feature.symbol,
        _decimal_key(feature.range_pct),
        _decimal_key(feature.price_step),
        feature.range_bar_id,
        feature.range_start_ms,
        feature.range_end_ms,
        feature.available_time_ms,
        _dec(feature.fp_max_bucket_abs_delta_pressure),
        _dec(feature.fp_low_bucket_delta_pressure),
        _dec(feature.fp_high_bucket_delta_pressure),
        _dec(feature.fp_delta_pressure),
        feature.bucket_count,
        feature.trade_count,
        1 if feature.context_available else 0,
        feature.quality,
        feature.source,
    )


def _row_to_range_footprint(
    row: tuple[object, ...],
) -> RangeFootprintFeature:
    return RangeFootprintFeature(
        exchange=str(row[0]),
        symbol=str(row[1]),
        range_pct=Decimal(str(row[2])),
        price_step=Decimal(str(row[3])),
        range_bar_id=int(row[4]),
        range_start_ms=int(row[5]),
        range_end_ms=int(row[6]),
        available_time_ms=int(row[7]),
        fp_max_bucket_abs_delta_pressure=Decimal(str(row[8])),
        fp_low_bucket_delta_pressure=Decimal(str(row[9])),
        fp_high_bucket_delta_pressure=Decimal(str(row[10])),
        fp_delta_pressure=Decimal(str(row[11])),
        bucket_count=int(row[12]),
        trade_count=int(row[13]),
        context_available=bool(row[14]),
        quality=str(row[15]),
        source=str(row[16]),
    )


def _dec(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _decimal_key(value: Decimal | str | float) -> str:
    return _dec(Decimal(str(value)))


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)
