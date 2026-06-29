from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from src.market_data.backfill.models import BackfillPlan
from src.market_data.models import RangeCoverageStatus


MIN_VALID_BUCKET_END_MS = 1_640_995_200_000  # 2022-01-01T00:00:00Z


@dataclass(frozen=True)
class BackfillScanner:
    checkpoint_db: str | Path
    market_db: str | Path
    dirty_table: str = "range_backfill_dirty_buckets"

    def scan(
        self,
        *,
        exchange: str,
        symbol: str,
        raw_symbol: str,
        range_pct: str,
        bucket_ms: int,
        required_buckets: int,
        lookback_buckets: int,
        current_time_ms: int,
    ) -> BackfillPlan:
        latest_start = (int(current_time_ms) // int(bucket_ms)) * int(bucket_ms) - int(bucket_ms)
        latest_end = latest_start + int(bucket_ms) - 1
        lookback = max(int(required_buckets), int(lookback_buckets))
        starts_desc = [latest_start - i * int(bucket_ms) for i in range(lookback) if latest_start - i * int(bucket_ms) >= 0]
        required_desc = starts_desc[: max(0, int(required_buckets))]
        required_set = set(required_desc)

        complete_all = self._complete_aggregate_starts(
            exchange=exchange,
            symbol=symbol,
            range_pct=range_pct,
            starts=starts_desc,
        )
        dirty = self._dirty_starts(exchange=exchange, symbol=symbol, range_pct=range_pct, starts=starts_desc)
        coverage_missing = self._incomplete_coverage_starts(symbol=symbol, starts=starts_desc, bucket_ms=bucket_ms)
        bars_present = self._range_bar_bucket_starts(symbol=symbol, range_pct=range_pct, starts=starts_desc, bucket_ms=bucket_ms)

        complete_required = tuple(sorted(required_set & complete_all))
        missing = tuple(start for start in required_desc if start not in complete_all)
        dirty_required = tuple(start for start in required_desc if start in dirty)
        incomplete_required = tuple(start for start in required_desc if start in complete_all and start in coverage_missing and start not in bars_present)

        continuous = 0
        for start in required_desc:
            if start in complete_all and start not in dirty:
                continuous += 1
            else:
                break
        ready = continuous >= int(required_buckets)
        nearest = None
        for start in required_desc:
            if start in set(missing) | set(dirty_required) | set(incomplete_required):
                nearest = start
                break
        reason = "ready" if ready else f"continuous_complete_buckets_from_latest={continuous} required={required_buckets}"
        return BackfillPlan(
            exchange=str(exchange).lower(),
            symbol=symbol,
            raw_symbol=raw_symbol,
            range_pct=_decimal_text(range_pct),
            bucket_ms=int(bucket_ms),
            latest_closed_bucket_start_ms=latest_start,
            latest_closed_bucket_end_ms=latest_end,
            required_bucket_starts=tuple(sorted(required_desc)),
            complete_bucket_starts=complete_required,
            missing_bucket_starts=missing,
            dirty_bucket_starts=dirty_required,
            incomplete_coverage_bucket_starts=incomplete_required,
            continuous_complete_buckets_from_latest=continuous,
            range_speed_ready=ready,
            nearest_missing_bucket_start_ms=nearest,
            reason=reason,
        )

    def _complete_aggregate_starts(self, *, exchange: str, symbol: str, range_pct: str, starts: list[int]) -> set[int]:
        if not starts or not Path(self.checkpoint_db).exists():
            return set()
        with sqlite3.connect(self.checkpoint_db) as conn:
            _configure_read(conn)
            if not _table_exists(conn, "completed_range_aggregates"):
                return set()
            placeholders = ",".join("?" for _ in starts)
            rows = conn.execute(
                f"""
                SELECT bucket_start_ms
                FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND coverage_status = ? AND bucket_start_ms IN ({placeholders})
                  AND bucket_end_ms >= ?
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    RangeCoverageStatus.COMPLETE.value,
                    *starts,
                    MIN_VALID_BUCKET_END_MS,
                ),
            ).fetchall()
        return {int(row[0]) for row in rows}

    def _dirty_starts(self, *, exchange: str, symbol: str, range_pct: str, starts: list[int]) -> set[int]:
        if not starts or not Path(self.checkpoint_db).exists():
            return set()
        with sqlite3.connect(self.checkpoint_db) as conn:
            _configure_read(conn)
            if not _table_exists(conn, self.dirty_table):
                return set()
            placeholders = ",".join("?" for _ in starts)
            rows = conn.execute(
                f"""
                SELECT bucket_start_ms
                FROM {self.dirty_table}
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND bucket_start_ms IN ({placeholders})
                """,
                (str(exchange).lower(), symbol, _decimal_text(range_pct), *starts),
            ).fetchall()
        return {int(row[0]) for row in rows}

    def _range_bar_bucket_starts(self, *, symbol: str, range_pct: str, starts: list[int], bucket_ms: int) -> set[int]:
        if not starts or not Path(self.market_db).exists():
            return set()
        min_start = min(starts)
        max_end = max(starts) + bucket_ms - 1
        with sqlite3.connect(self.market_db) as conn:
            _configure_read(conn)
            if not _table_exists(conn, "range_bars"):
                return set()
            rows = conn.execute(
                """
                SELECT DISTINCT (end_time_ms / ?) * ?
                FROM range_bars
                WHERE symbol = ? AND range_pct = ? AND end_time_ms BETWEEN ? AND ?
                """,
                (bucket_ms, bucket_ms, symbol, _decimal_text(range_pct), min_start, max_end),
            ).fetchall()
        return {int(row[0]) for row in rows}

    def _incomplete_coverage_starts(self, *, symbol: str, starts: list[int], bucket_ms: int) -> set[int]:
        if not starts or not Path(self.market_db).exists():
            return set()
        missing: set[int] = set()
        with sqlite3.connect(self.market_db) as conn:
            _configure_read(conn)
            if not _table_exists(conn, "trade_coverage"):
                return set()
            columns = _table_columns(conn, "trade_coverage")
            for start in starts:
                end = start + bucket_ms - 1
                if "coverage_status" in columns:
                    row = conn.execute(
                        """
                        SELECT 1 FROM trade_coverage
                        WHERE symbol = ? AND start_time_ms <= ? AND end_time_ms >= ?
                          AND coverage_status = 'COMPLETE'
                        LIMIT 1
                        """,
                        (symbol, start, end),
                    ).fetchone()
                else:
                    row = conn.execute(
                        """
                        SELECT 1 FROM trade_coverage
                        WHERE symbol = ? AND start_time_ms <= ? AND end_time_ms >= ?
                        LIMIT 1
                        """,
                        (symbol, start, end),
                    ).fetchone()
                if row is None:
                    missing.add(start)
        return missing


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _configure_read(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA busy_timeout=100")


def _decimal_text(value: Decimal | str) -> str:
    return format(Decimal(str(value)).normalize(), "f")
