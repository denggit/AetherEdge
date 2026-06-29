from __future__ import annotations

import sqlite3
from pathlib import Path

from src.market_data.backfill.scanner import BackfillScanner


H4 = 4 * 60 * 60_000
BASE = 1_640_995_200_000 + 200 * H4


def _insert_complete(db: Path, starts: list[int]) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE completed_range_aggregates (
                exchange TEXT, symbol TEXT, range_pct TEXT, bucket_start_ms INTEGER,
                bucket_end_ms INTEGER, rf_bar_count INTEGER, imbalance TEXT, close_pos TEXT,
                taker_buy_ratio TEXT, micro_return_pct TEXT, delta_notional_sum TEXT,
                notional_sum TEXT, coverage_status TEXT, missing_gap_ms INTEGER,
                completed_at_ms INTEGER,
                PRIMARY KEY(exchange, symbol, range_pct, bucket_end_ms)
            )
            """
        )
        for start in starts:
            conn.execute(
                """
                INSERT INTO completed_range_aggregates VALUES (
                    'okx','ETH-USDT-PERP','0.002',?,?,1,NULL,NULL,NULL,NULL,NULL,NULL,'COMPLETE',0,?
                )
                """,
                (start, start + H4 - 1, start + H4),
            )


def _insert_dirty(db: Path, start: int) -> None:
    with sqlite3.connect(db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS range_backfill_dirty_buckets (
                exchange TEXT, symbol TEXT, range_pct TEXT, bucket_start_ms INTEGER,
                bucket_end_ms INTEGER, reason TEXT, updated_at_ms INTEGER,
                PRIMARY KEY(exchange, symbol, range_pct, bucket_start_ms)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO range_backfill_dirty_buckets VALUES (
                'okx','ETH-USDT-PERP','0.002',?,?, 'test', ?
            )
            """,
            (start, start + H4 - 1, start + H4),
        )


def _plan(checkpoint_db: Path, market_db: Path, *, current: int = BASE + 101 * H4 + 1):
    return BackfillScanner(checkpoint_db=checkpoint_db, market_db=market_db).scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        range_pct="0.002",
        bucket_ms=H4,
        required_buckets=100,
        lookback_buckets=180,
        current_time_ms=current,
    )


def test_continuous_100_completed_is_ready(tmp_path: Path) -> None:
    latest = (BASE + 101 * H4 + 1) // H4 * H4 - H4
    _insert_complete(tmp_path / "checkpoint.sqlite3", [latest - i * H4 for i in range(100)])

    plan = _plan(tmp_path / "checkpoint.sqlite3", tmp_path / "market.sqlite3")

    assert plan.range_speed_ready is True
    assert plan.continuous_complete_buckets_from_latest == 100


def test_total_100_with_recent_gap_is_not_ready(tmp_path: Path) -> None:
    latest = (BASE + 101 * H4 + 1) // H4 * H4 - H4
    starts = [latest - i * H4 for i in range(101) if i != 3][:100]
    _insert_complete(tmp_path / "checkpoint.sqlite3", starts)

    plan = _plan(tmp_path / "checkpoint.sqlite3", tmp_path / "market.sqlite3")

    assert plan.range_speed_ready is False
    assert plan.nearest_missing_bucket_start_ms == latest - 3 * H4


def test_pollution_bucket_end_before_2022_is_ignored(tmp_path: Path) -> None:
    _insert_complete(tmp_path / "checkpoint.sqlite3", [0])

    plan = _plan(tmp_path / "checkpoint.sqlite3", tmp_path / "market.sqlite3", current=H4 * 2 + 1)

    assert plan.range_speed_ready is False
    assert 0 in plan.missing_bucket_starts


def test_current_open_bucket_is_not_required(tmp_path: Path) -> None:
    current = BASE + 10 * H4 + 123
    latest_closed = (current // H4) * H4 - H4
    _insert_complete(tmp_path / "checkpoint.sqlite3", [latest_closed])

    plan = BackfillScanner(checkpoint_db=tmp_path / "checkpoint.sqlite3", market_db=tmp_path / "market.sqlite3").scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        range_pct="0.002",
        bucket_ms=H4,
        required_buckets=1,
        lookback_buckets=2,
        current_time_ms=current,
    )

    assert plan.latest_closed_bucket_start_ms == latest_closed
    assert current // H4 * H4 not in plan.required_bucket_starts


def test_dirty_bucket_breaks_continuous_complete(tmp_path: Path) -> None:
    latest = (BASE + 101 * H4 + 1) // H4 * H4 - H4
    checkpoint_db = tmp_path / "checkpoint.sqlite3"
    _insert_complete(checkpoint_db, [latest - i * H4 for i in range(100)])
    _insert_dirty(checkpoint_db, latest - 2 * H4)

    plan = _plan(checkpoint_db, tmp_path / "market.sqlite3")

    assert plan.range_speed_ready is False
    assert plan.continuous_complete_buckets_from_latest == 2
    assert plan.nearest_missing_bucket_start_ms == latest - 2 * H4


def test_missing_trade_coverage_row_does_not_target_complete_aggregate(tmp_path: Path) -> None:
    latest = (BASE + 101 * H4 + 1) // H4 * H4 - H4
    _insert_complete(tmp_path / "checkpoint.sqlite3", [latest])

    plan = BackfillScanner(checkpoint_db=tmp_path / "checkpoint.sqlite3", market_db=tmp_path / "market.sqlite3").scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        range_pct="0.002",
        bucket_ms=H4,
        required_buckets=1,
        lookback_buckets=1,
        current_time_ms=BASE + 101 * H4 + 1,
    )

    assert plan.range_speed_ready is True
    assert plan.incomplete_coverage_bucket_starts == ()
    assert plan.missing_bucket_count == 0
