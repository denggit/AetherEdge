from __future__ import annotations

import json
import sqlite3
import threading
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src.market_data.models import RangeBarAggregate, RangeCoverageStatus

DEFAULT_RANGE_CHECKPOINT_DB = "data/state/range_builder_checkpoint.sqlite3"
MIN_VALID_COMPLETED_AGGREGATE_MS = 1_700_000_000_000
MICRO_REPAIR_QUEUED = "micro_repair_queued"
MICRO_REPAIR_RUNNING = "micro_repair_running"
MICRO_REPAIR_SUCCESS = "micro_repair_success"
MICRO_REPAIR_FAILED = "micro_repair_failed"
MICRO_REPAIR_SKIPPED = "micro_repair_skipped"


@dataclass(frozen=True)
class RangeBuilderCheckpoint:
    exchange: str
    symbol: str
    range_pct: str
    bucket_start_ms: int
    bucket_end_ms: int
    last_trade_id: str | None
    last_trade_ts_ms: int | None
    last_ws_recv_ts_ms: int | None
    range_bar_count: int
    aggregate: Mapping[str, Any]
    builder_state: Mapping[str, Any]
    coverage_status: str
    missing_gap_ms: int
    checkpoint_updated_at_ms: int

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (
            self.exchange,
            self.symbol,
            _decimal_text(self.range_pct),
            self.bucket_start_ms,
        )


@dataclass(frozen=True)
class RangeCheckpointRecovery:
    coverage_status: str
    checkpoint: RangeBuilderCheckpoint | None
    checkpoint_age_ms: int | None
    missing_gap_ms: int
    recovered_from_checkpoint: bool


@dataclass(frozen=True)
class RangeMicroRepairJob:
    exchange: str
    symbol: str
    range_pct: str
    bucket_start_ms: int
    bucket_end_ms: int
    checkpoint_last_trade_id: str | None
    checkpoint_last_trade_ts_ms: int | None
    builder_state: Mapping[str, Any]
    coverage_status: str
    missing_gap_ms: int
    first_live_trade_ts_ms: int | None = None
    first_live_trade_id: str | None = None
    repair_gap_start_ms: int | None = None
    repair_gap_end_ms: int | None = None
    journal_start_ms: int | None = None
    journal_end_ms: int | None = None
    journal_required: bool = True
    journal_status: str | None = None
    status: str = MICRO_REPAIR_QUEUED
    created_at_ms: int = 0
    updated_at_ms: int = 0
    last_error: str | None = None

    @property
    def key(self) -> tuple[str, str, str, int]:
        return (
            str(self.exchange).lower(),
            self.symbol,
            _decimal_text(self.range_pct),
            self.bucket_start_ms,
        )


@dataclass(frozen=True)
class CompletedRangeAggregate:
    exchange: str
    symbol: str
    range_pct: str
    bucket_start_ms: int
    bucket_end_ms: int
    rf_bar_count: int
    imbalance: str | None
    close_pos: str | None
    taker_buy_ratio: str | None
    micro_return_pct: str | None
    delta_notional_sum: str | None
    notional_sum: str | None
    coverage_status: str
    missing_gap_ms: int
    completed_at_ms: int


class SqliteRangeCheckpointStore:
    """Latest builder checkpoint and completed aggregate history in SQLite."""

    def __init__(self, path: str | Path = DEFAULT_RANGE_CHECKPOINT_DB) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save_checkpoint(self, checkpoint: RangeBuilderCheckpoint) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO range_builder_checkpoints (
                    exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                    last_trade_id, last_trade_ts_ms, last_ws_recv_ts_ms,
                    range_bar_count, aggregate_json, builder_state_json,
                    coverage_status, missing_gap_ms, checkpoint_updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, symbol, range_pct, bucket_start_ms)
                DO UPDATE SET
                    bucket_end_ms=excluded.bucket_end_ms,
                    last_trade_id=excluded.last_trade_id,
                    last_trade_ts_ms=excluded.last_trade_ts_ms,
                    last_ws_recv_ts_ms=excluded.last_ws_recv_ts_ms,
                    range_bar_count=excluded.range_bar_count,
                    aggregate_json=excluded.aggregate_json,
                    builder_state_json=excluded.builder_state_json,
                    coverage_status=excluded.coverage_status,
                    missing_gap_ms=excluded.missing_gap_ms,
                    checkpoint_updated_at_ms=excluded.checkpoint_updated_at_ms
                """,
                (
                    str(checkpoint.exchange).lower(),
                    checkpoint.symbol,
                    _decimal_text(checkpoint.range_pct),
                    checkpoint.bucket_start_ms,
                    checkpoint.bucket_end_ms,
                    checkpoint.last_trade_id,
                    checkpoint.last_trade_ts_ms,
                    checkpoint.last_ws_recv_ts_ms,
                    checkpoint.range_bar_count,
                    _json_dump(checkpoint.aggregate),
                    _json_dump(checkpoint.builder_state),
                    _coverage_value(checkpoint.coverage_status),
                    max(0, checkpoint.missing_gap_ms),
                    checkpoint.checkpoint_updated_at_ms,
                ),
            )

    def load_checkpoint(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
    ) -> RangeBuilderCheckpoint | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                       last_trade_id, last_trade_ts_ms, last_ws_recv_ts_ms,
                       range_bar_count, aggregate_json, builder_state_json,
                       coverage_status, missing_gap_ms, checkpoint_updated_at_ms
                FROM range_builder_checkpoints
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND bucket_start_ms = ?
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    bucket_start_ms,
                ),
            ).fetchone()
        return None if row is None else _checkpoint_from_row(row)

    def recover_current_bucket(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        now_ms: int,
        max_age_for_recovered_minor_ms: int = 60_000,
        max_age_for_restore_ms: int = 300_000,
    ) -> RangeCheckpointRecovery:
        checkpoint = self.load_checkpoint(
            exchange=exchange,
            symbol=symbol,
            range_pct=range_pct,
            bucket_start_ms=bucket_start_ms,
        )
        if checkpoint is None:
            return RangeCheckpointRecovery(
                coverage_status=RangeCoverageStatus.COLD_START_PARTIAL.value,
                checkpoint=None,
                checkpoint_age_ms=None,
                missing_gap_ms=max(0, now_ms - bucket_start_ms),
                recovered_from_checkpoint=False,
            )

        age_ms = max(0, now_ms - checkpoint.checkpoint_updated_at_ms)
        missing_gap_ms = max(0, checkpoint.missing_gap_ms) + age_ms
        if age_ms <= max_age_for_recovered_minor_ms:
            return RangeCheckpointRecovery(
                coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
                checkpoint=checkpoint,
                checkpoint_age_ms=age_ms,
                missing_gap_ms=missing_gap_ms,
                recovered_from_checkpoint=True,
            )
        if age_ms <= max_age_for_restore_ms:
            return RangeCheckpointRecovery(
                coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
                checkpoint=checkpoint,
                checkpoint_age_ms=age_ms,
                missing_gap_ms=missing_gap_ms,
                recovered_from_checkpoint=True,
            )
        return RangeCheckpointRecovery(
            coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
            checkpoint=None,
            checkpoint_age_ms=age_ms,
            missing_gap_ms=missing_gap_ms,
            recovered_from_checkpoint=False,
        )

    def save_completed_aggregate(
        self,
        *,
        exchange: str,
        aggregate: RangeBarAggregate,
        coverage_status: str,
        missing_gap_ms: int = 0,
        completed_at_ms: int,
    ) -> bool:
        if not _completed_aggregate_is_valid(aggregate):
            return False
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO completed_range_aggregates (
                    exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                    rf_bar_count, imbalance, close_pos, taker_buy_ratio,
                    micro_return_pct, delta_notional_sum, notional_sum,
                    coverage_status, missing_gap_ms, completed_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange, symbol, range_pct, bucket_end_ms)
                DO UPDATE SET
                    bucket_start_ms=excluded.bucket_start_ms,
                    rf_bar_count=excluded.rf_bar_count,
                    imbalance=excluded.imbalance,
                    close_pos=excluded.close_pos,
                    taker_buy_ratio=excluded.taker_buy_ratio,
                    micro_return_pct=excluded.micro_return_pct,
                    delta_notional_sum=excluded.delta_notional_sum,
                    notional_sum=excluded.notional_sum,
                    coverage_status=excluded.coverage_status,
                    missing_gap_ms=excluded.missing_gap_ms,
                    completed_at_ms=excluded.completed_at_ms
                """,
                (
                    str(exchange).lower(),
                    aggregate.symbol,
                    _decimal_text(aggregate.range_pct),
                    aggregate.bucket_start_ms,
                    aggregate.bucket_end_ms,
                    aggregate.bar_count,
                    str(aggregate.imbalance),
                    str(aggregate.close_pos),
                    str(aggregate.taker_buy_ratio),
                    str(aggregate.micro_return_pct),
                    str(aggregate.delta_notional_sum),
                    str(aggregate.notional_sum),
                    _coverage_value(coverage_status),
                    max(0, int(missing_gap_ms)),
                    int(completed_at_ms),
                ),
            )
        return True

    def load_completed_aggregate(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_end_ms: int,
    ) -> CompletedRangeAggregate | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                       rf_bar_count, imbalance, close_pos, taker_buy_ratio,
                       micro_return_pct, delta_notional_sum, notional_sum,
                       coverage_status, missing_gap_ms, completed_at_ms
                FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND bucket_end_ms = ?
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_end_ms),
                ),
            ).fetchone()
        return None if row is None else _completed_from_row(row)

    def invalidate_completed_aggregate(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_end_ms: int,
        coverage_status: str,
        missing_gap_ms: int,
        completed_at_ms: int,
    ) -> bool:
        status = _coverage_value(coverage_status)
        if status == RangeCoverageStatus.COMPLETE.value:
            raise ValueError("invalidated aggregate cannot remain COMPLETE")
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE completed_range_aggregates
                SET coverage_status=?,
                    missing_gap_ms=MAX(missing_gap_ms, ?),
                    completed_at_ms=?
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_end_ms=?
                """,
                (
                    status,
                    max(1, int(missing_gap_ms)),
                    int(completed_at_ms),
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_end_ms),
                ),
            )
        return int(cursor.rowcount or 0) > 0

    def enqueue_micro_repair(self, job: RangeMicroRepairJob) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO range_micro_repair_jobs (
                    exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                    checkpoint_last_trade_id, checkpoint_last_trade_ts_ms,
                    builder_state_json, coverage_status, missing_gap_ms,
                    first_live_trade_ts_ms, first_live_trade_id,
                    repair_gap_start_ms, repair_gap_end_ms,
                    journal_start_ms, journal_end_ms, journal_required,
                    journal_status,
                    status, created_at_ms, updated_at_ms, last_error
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
                ON CONFLICT(exchange, symbol, range_pct, bucket_start_ms)
                DO UPDATE SET
                    bucket_end_ms=excluded.bucket_end_ms,
                    checkpoint_last_trade_id=excluded.checkpoint_last_trade_id,
                    checkpoint_last_trade_ts_ms=excluded.checkpoint_last_trade_ts_ms,
                    builder_state_json=excluded.builder_state_json,
                    coverage_status=excluded.coverage_status,
                    missing_gap_ms=excluded.missing_gap_ms,
                    first_live_trade_ts_ms=COALESCE(
                        range_micro_repair_jobs.first_live_trade_ts_ms,
                        excluded.first_live_trade_ts_ms
                    ),
                    first_live_trade_id=COALESCE(
                        range_micro_repair_jobs.first_live_trade_id,
                        excluded.first_live_trade_id
                    ),
                    repair_gap_start_ms=COALESCE(
                        range_micro_repair_jobs.repair_gap_start_ms,
                        excluded.repair_gap_start_ms
                    ),
                    repair_gap_end_ms=COALESCE(
                        range_micro_repair_jobs.repair_gap_end_ms,
                        excluded.repair_gap_end_ms
                    ),
                    journal_start_ms=COALESCE(
                        range_micro_repair_jobs.journal_start_ms,
                        excluded.journal_start_ms
                    ),
                    journal_end_ms=COALESCE(
                        range_micro_repair_jobs.journal_end_ms,
                        excluded.journal_end_ms
                    ),
                    journal_required=excluded.journal_required,
                    journal_status=COALESCE(
                        excluded.journal_status,
                        range_micro_repair_jobs.journal_status
                    ),
                    status=CASE
                        WHEN range_micro_repair_jobs.status = ?
                        THEN range_micro_repair_jobs.status
                        ELSE excluded.status
                    END,
                    updated_at_ms=excluded.updated_at_ms,
                    last_error=excluded.last_error
                """,
                (
                    str(job.exchange).lower(),
                    job.symbol,
                    _decimal_text(job.range_pct),
                    int(job.bucket_start_ms),
                    int(job.bucket_end_ms),
                    job.checkpoint_last_trade_id,
                    job.checkpoint_last_trade_ts_ms,
                    _json_dump(job.builder_state),
                    _coverage_value(job.coverage_status),
                    max(0, int(job.missing_gap_ms)),
                    job.first_live_trade_ts_ms,
                    job.first_live_trade_id,
                    job.repair_gap_start_ms,
                    job.repair_gap_end_ms,
                    job.journal_start_ms,
                    job.journal_end_ms,
                    int(bool(job.journal_required)),
                    job.journal_status,
                    str(job.status),
                    int(job.created_at_ms),
                    int(job.updated_at_ms),
                    job.last_error,
                    MICRO_REPAIR_SUCCESS,
                ),
            )

    def load_micro_repair_job(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
    ) -> RangeMicroRepairJob | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                       checkpoint_last_trade_id, checkpoint_last_trade_ts_ms,
                       builder_state_json, coverage_status, missing_gap_ms,
                       first_live_trade_ts_ms, first_live_trade_id,
                       repair_gap_start_ms, repair_gap_end_ms,
                       journal_start_ms, journal_end_ms, journal_required,
                       journal_status,
                       status, created_at_ms, updated_at_ms, last_error
                FROM range_micro_repair_jobs
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND bucket_start_ms = ?
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            ).fetchone()
        return None if row is None else _micro_repair_job_from_row(row)

    def update_micro_repair_journal(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        first_live_trade_ts_ms: int,
        first_live_trade_id: str | None,
        repair_gap_start_ms: int,
        repair_gap_end_ms: int,
        journal_start_ms: int,
        journal_end_ms: int,
        journal_status: str,
        updated_at_ms: int,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE range_micro_repair_jobs
                SET first_live_trade_ts_ms=?,
                    first_live_trade_id=?,
                    repair_gap_start_ms=?,
                    repair_gap_end_ms=?,
                    journal_start_ms=?,
                    journal_end_ms=?,
                    journal_status=?,
                    updated_at_ms=?
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                """,
                (
                    int(first_live_trade_ts_ms),
                    first_live_trade_id,
                    int(repair_gap_start_ms),
                    int(repair_gap_end_ms),
                    int(journal_start_ms),
                    int(journal_end_ms),
                    journal_status,
                    int(updated_at_ms),
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            )
        return int(cursor.rowcount or 0) > 0

    def update_micro_repair_journal_status(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        journal_status: str,
        updated_at_ms: int,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE range_micro_repair_jobs
                SET journal_status=?, updated_at_ms=?
                WHERE exchange=? AND symbol=? AND range_pct=?
                  AND bucket_start_ms=?
                """,
                (
                    journal_status,
                    int(updated_at_ms),
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            )
        return int(cursor.rowcount or 0) > 0

    def mark_micro_repair_status(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        status: str,
        updated_at_ms: int,
        last_error: str | None = None,
    ) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE range_micro_repair_jobs
                SET status = ?, updated_at_ms = ?, last_error = ?
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND bucket_start_ms = ?
                """,
                (
                    str(status),
                    int(updated_at_ms),
                    last_error,
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    int(bucket_start_ms),
                ),
            )
        return int(cursor.rowcount or 0) > 0

    def load_complete_history(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        before_bucket_end_ms: int,
        limit: int = 1080,
    ) -> list[CompletedRangeAggregate]:
        if limit <= 0:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                       rf_bar_count, imbalance, close_pos, taker_buy_ratio,
                       micro_return_pct, delta_notional_sum, notional_sum,
                       coverage_status, missing_gap_ms, completed_at_ms
                FROM (
                    SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                           rf_bar_count, imbalance, close_pos, taker_buy_ratio,
                           micro_return_pct, delta_notional_sum, notional_sum,
                           coverage_status, missing_gap_ms, completed_at_ms
                    FROM completed_range_aggregates
                    WHERE exchange = ? AND symbol = ? AND range_pct = ?
                      AND coverage_status = ?
                      AND bucket_start_ms >= ?
                      AND bucket_end_ms >= ?
                      AND bucket_end_ms > bucket_start_ms
                      AND bucket_end_ms < ?
                    ORDER BY bucket_end_ms DESC
                    LIMIT ?
                )
                ORDER BY bucket_end_ms ASC
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    RangeCoverageStatus.COMPLETE.value,
                    MIN_VALID_COMPLETED_AGGREGATE_MS,
                    MIN_VALID_COMPLETED_AGGREGATE_MS,
                    before_bucket_end_ms,
                    limit,
                ),
            ).fetchall()
        return [_completed_from_row(row) for row in rows]

    def load_history(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        before_bucket_end_ms: int,
        limit: int = 1080,
    ) -> list[CompletedRangeAggregate]:
        """Load valid aggregate rows regardless of coverage quality."""

        if limit <= 0:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                       rf_bar_count, imbalance, close_pos, taker_buy_ratio,
                       micro_return_pct, delta_notional_sum, notional_sum,
                       coverage_status, missing_gap_ms, completed_at_ms
                FROM (
                    SELECT exchange, symbol, range_pct, bucket_start_ms, bucket_end_ms,
                           rf_bar_count, imbalance, close_pos, taker_buy_ratio,
                           micro_return_pct, delta_notional_sum, notional_sum,
                           coverage_status, missing_gap_ms, completed_at_ms
                    FROM completed_range_aggregates
                    WHERE exchange = ? AND symbol = ? AND range_pct = ?
                      AND bucket_start_ms >= ?
                      AND bucket_end_ms >= ?
                      AND bucket_end_ms > bucket_start_ms
                      AND bucket_end_ms < ?
                    ORDER BY bucket_end_ms DESC
                    LIMIT ?
                )
                ORDER BY bucket_end_ms ASC
                """,
                (
                    str(exchange).lower(),
                    symbol,
                    _decimal_text(range_pct),
                    MIN_VALID_COMPLETED_AGGREGATE_MS,
                    MIN_VALID_COMPLETED_AGGREGATE_MS,
                    before_bucket_end_ms,
                    limit,
                ),
            ).fetchall()
        return [_completed_from_row(row) for row in rows]

    def history_counts(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        before_bucket_end_ms: int | None = None,
    ) -> tuple[int, int]:
        where_end = "" if before_bucket_end_ms is None else " AND bucket_end_ms < ?"
        params: list[object] = [
            RangeCoverageStatus.COMPLETE.value,
            str(exchange).lower(),
            symbol,
            _decimal_text(range_pct),
            MIN_VALID_COMPLETED_AGGREGATE_MS,
            MIN_VALID_COMPLETED_AGGREGATE_MS,
        ]
        if before_bucket_end_ms is not None:
            params.append(before_bucket_end_ms)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*),
                       SUM(CASE WHEN coverage_status = ? THEN 1 ELSE 0 END)
                FROM completed_range_aggregates
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND bucket_start_ms >= ?
                  AND bucket_end_ms >= ?
                  AND bucket_end_ms > bucket_start_ms
                  {where_end}
                """,
                params,
            ).fetchone()
        return int(row[0] or 0), int(row[1] or 0)

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS range_builder_checkpoints (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    bucket_start_ms INTEGER NOT NULL,
                    bucket_end_ms INTEGER NOT NULL,
                    last_trade_id TEXT,
                    last_trade_ts_ms INTEGER,
                    last_ws_recv_ts_ms INTEGER,
                    range_bar_count INTEGER NOT NULL,
                    aggregate_json TEXT NOT NULL,
                    builder_state_json TEXT NOT NULL,
                    coverage_status TEXT NOT NULL,
                    missing_gap_ms INTEGER NOT NULL DEFAULT 0,
                    checkpoint_updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (exchange, symbol, range_pct, bucket_start_ms)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS completed_range_aggregates (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    bucket_start_ms INTEGER NOT NULL,
                    bucket_end_ms INTEGER NOT NULL,
                    rf_bar_count INTEGER NOT NULL,
                    imbalance TEXT,
                    close_pos TEXT,
                    taker_buy_ratio TEXT,
                    micro_return_pct TEXT,
                    delta_notional_sum TEXT,
                    notional_sum TEXT,
                    coverage_status TEXT NOT NULL,
                    missing_gap_ms INTEGER NOT NULL DEFAULT 0,
                    completed_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (exchange, symbol, range_pct, bucket_end_ms)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_completed_range_history
                ON completed_range_aggregates(
                    exchange, symbol, range_pct, coverage_status, bucket_end_ms
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS range_micro_repair_jobs (
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    range_pct TEXT NOT NULL,
                    bucket_start_ms INTEGER NOT NULL,
                    bucket_end_ms INTEGER NOT NULL,
                    checkpoint_last_trade_id TEXT,
                    checkpoint_last_trade_ts_ms INTEGER,
                    builder_state_json TEXT NOT NULL,
                    coverage_status TEXT NOT NULL,
                    missing_gap_ms INTEGER NOT NULL DEFAULT 0,
                    first_live_trade_ts_ms INTEGER,
                    first_live_trade_id TEXT,
                    repair_gap_start_ms INTEGER,
                    repair_gap_end_ms INTEGER,
                    journal_start_ms INTEGER,
                    journal_end_ms INTEGER,
                    journal_required INTEGER NOT NULL DEFAULT 1,
                    journal_status TEXT,
                    status TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    last_error TEXT,
                    PRIMARY KEY (exchange, symbol, range_pct, bucket_start_ms)
                )
                """
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "first_live_trade_ts_ms",
                "INTEGER",
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "first_live_trade_id",
                "TEXT",
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "repair_gap_start_ms",
                "INTEGER",
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "repair_gap_end_ms",
                "INTEGER",
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "journal_start_ms",
                "INTEGER",
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "journal_end_ms",
                "INTEGER",
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "journal_required",
                "INTEGER NOT NULL DEFAULT 1",
            )
            _ensure_column(
                conn,
                "range_micro_repair_jobs",
                "journal_status",
                "TEXT",
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_range_micro_repair_due
                ON range_micro_repair_jobs(status, bucket_end_ms)
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn


class RangeCheckpointWriter:
    """Bounded latest-state writer; submit never performs database I/O."""

    def __init__(
        self,
        store: SqliteRangeCheckpointStore,
        *,
        max_pending: int = 8,
        on_error: Callable[[BaseException], None] | None = None,
    ) -> None:
        if max_pending <= 0:
            raise ValueError("max_pending must be positive")
        self.store = store
        self.max_pending = int(max_pending)
        self.on_error = on_error
        self._pending: OrderedDict[
            tuple[str, str, str, int], RangeBuilderCheckpoint
        ] = OrderedDict()
        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None
        self._stopping = False
        self.replaced = 0
        self.dropped = 0
        self.written = 0
        self.failures = 0

    def start(self) -> None:
        with self._condition:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stopping = False
            self._thread = threading.Thread(
                target=self._run,
                name="range-checkpoint-writer",
                daemon=True,
            )
            self._thread.start()

    def submit(self, checkpoint: RangeBuilderCheckpoint) -> bool:
        """Queue/replace one snapshot and return immediately."""

        with self._condition:
            if self._stopping:
                self.dropped += 1
                return False
            key = checkpoint.key
            if key in self._pending:
                self.replaced += 1
                self._pending[key] = checkpoint
                self._pending.move_to_end(key)
            else:
                if len(self._pending) >= self.max_pending:
                    self._pending.popitem(last=False)
                    self.dropped += 1
                self._pending[key] = checkpoint
            self._condition.notify()
        return True

    def stop(self, *, flush: bool = True, timeout: float = 5.0) -> None:
        with self._condition:
            if not flush:
                self._pending.clear()
            self._stopping = True
            self._condition.notify_all()
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, timeout))

    @property
    def pending_count(self) -> int:
        with self._condition:
            return len(self._pending)

    def _run(self) -> None:
        while True:
            with self._condition:
                while not self._pending and not self._stopping:
                    self._condition.wait()
                if not self._pending and self._stopping:
                    return
                _, checkpoint = self._pending.popitem(last=False)
            try:
                self.store.save_checkpoint(checkpoint)
                self.written += 1
            except BaseException as exc:
                self.failures += 1
                if self.on_error is not None:
                    try:
                        self.on_error(exc)
                    except BaseException:
                        pass


def aggregate_snapshot(aggregate: RangeBarAggregate | None) -> Mapping[str, Any]:
    if aggregate is None:
        return {}
    return {
        "symbol": aggregate.symbol,
        "range_pct": str(aggregate.range_pct),
        "bucket_start_ms": aggregate.bucket_start_ms,
        "bucket_end_ms": aggregate.bucket_end_ms,
        "rf_bar_count": aggregate.bar_count,
        "first_open": str(aggregate.first_open),
        "last_close": str(aggregate.last_close),
        "high": str(aggregate.high),
        "low": str(aggregate.low),
        "buy_notional_sum": str(aggregate.buy_notional_sum),
        "sell_notional_sum": str(aggregate.sell_notional_sum),
        "delta_notional_sum": str(aggregate.delta_notional_sum),
        "notional_sum": str(aggregate.notional_sum),
        "micro_return_pct": str(aggregate.micro_return_pct),
        "imbalance": str(aggregate.imbalance),
        "taker_buy_ratio": str(aggregate.taker_buy_ratio),
        "close_pos": str(aggregate.close_pos),
    }


def _completed_aggregate_is_valid(aggregate: RangeBarAggregate) -> bool:
    bucket_start_ms = int(aggregate.bucket_start_ms)
    bucket_end_ms = int(aggregate.bucket_end_ms)
    return not (
        bucket_start_ms < MIN_VALID_COMPLETED_AGGREGATE_MS
        or bucket_end_ms < MIN_VALID_COMPLETED_AGGREGATE_MS
        or bucket_end_ms <= bucket_start_ms
    )


def _checkpoint_from_row(row: Sequence[object]) -> RangeBuilderCheckpoint:
    return RangeBuilderCheckpoint(
        exchange=str(row[0]),
        symbol=str(row[1]),
        range_pct=str(row[2]),
        bucket_start_ms=int(row[3]),
        bucket_end_ms=int(row[4]),
        last_trade_id=None if row[5] is None else str(row[5]),
        last_trade_ts_ms=None if row[6] is None else int(row[6]),
        last_ws_recv_ts_ms=None if row[7] is None else int(row[7]),
        range_bar_count=int(row[8]),
        aggregate=json.loads(str(row[9])),
        builder_state=json.loads(str(row[10])),
        coverage_status=str(row[11]),
        missing_gap_ms=int(row[12]),
        checkpoint_updated_at_ms=int(row[13]),
    )


def _completed_from_row(row: Sequence[object]) -> CompletedRangeAggregate:
    return CompletedRangeAggregate(
        exchange=str(row[0]),
        symbol=str(row[1]),
        range_pct=str(row[2]),
        bucket_start_ms=int(row[3]),
        bucket_end_ms=int(row[4]),
        rf_bar_count=int(row[5]),
        imbalance=None if row[6] is None else str(row[6]),
        close_pos=None if row[7] is None else str(row[7]),
        taker_buy_ratio=None if row[8] is None else str(row[8]),
        micro_return_pct=None if row[9] is None else str(row[9]),
        delta_notional_sum=None if row[10] is None else str(row[10]),
        notional_sum=None if row[11] is None else str(row[11]),
        coverage_status=str(row[12]),
        missing_gap_ms=int(row[13]),
        completed_at_ms=int(row[14]),
    )


def _micro_repair_job_from_row(row: Sequence[object]) -> RangeMicroRepairJob:
    return RangeMicroRepairJob(
        exchange=str(row[0]),
        symbol=str(row[1]),
        range_pct=str(row[2]),
        bucket_start_ms=int(row[3]),
        bucket_end_ms=int(row[4]),
        checkpoint_last_trade_id=None if row[5] is None else str(row[5]),
        checkpoint_last_trade_ts_ms=None if row[6] is None else int(row[6]),
        builder_state=json.loads(str(row[7])),
        coverage_status=str(row[8]),
        missing_gap_ms=int(row[9]),
        first_live_trade_ts_ms=(
            None if row[10] is None else int(row[10])
        ),
        first_live_trade_id=None if row[11] is None else str(row[11]),
        repair_gap_start_ms=None if row[12] is None else int(row[12]),
        repair_gap_end_ms=None if row[13] is None else int(row[13]),
        journal_start_ms=None if row[14] is None else int(row[14]),
        journal_end_ms=None if row[15] is None else int(row[15]),
        journal_required=bool(row[16]),
        journal_status=None if row[17] is None else str(row[17]),
        status=str(row[18]),
        created_at_ms=int(row[19]),
        updated_at_ms=int(row[20]),
        last_error=None if row[21] is None else str(row[21]),
    )


def _ensure_column(
    conn: sqlite3.Connection,
    table: str,
    column: str,
    definition: str,
) -> None:
    columns = {
        str(row[1])
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in columns:
        conn.execute(
            f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
        )


def _json_dump(value: Mapping[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _coverage_value(value: str | RangeCoverageStatus) -> str:
    raw = value.value if isinstance(value, RangeCoverageStatus) else str(value)
    return RangeCoverageStatus(raw).value


def _decimal_text(value: Decimal | str) -> str:
    return format(Decimal(str(value)).normalize(), "f")
