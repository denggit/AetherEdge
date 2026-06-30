from __future__ import annotations

from decimal import Decimal

from src.market_data.backfill.scanner import RangeBackfillScanner
from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore


def _aggregate(start: int, end: int, count: int) -> RangeBarAggregate:
    return RangeBarAggregate(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bucket_start_ms=start,
        bucket_end_ms=end,
        bar_count=count,
        first_open=Decimal("100"),
        last_close=Decimal("101"),
        high=Decimal("101"),
        low=Decimal("100"),
        buy_notional_sum=Decimal("10"),
        sell_notional_sum=Decimal("5"),
        delta_notional_sum=Decimal("5"),
        notional_sum=Decimal("15"),
    )


def test_scanner_finds_recent_missing_buckets(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    bucket_ms = 4 * 60 * 60_000
    now_ms = 1782835200000
    closed_end = now_ms - 1
    for offset in (0, 2):
        end = closed_end - offset * bucket_ms
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(end - bucket_ms + 1, end, 10 + offset),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end,
        )

    coverage = RangeBackfillScanner(store).scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_interval="4h",
        required_buckets=3,
        lookback_buckets=3,
        now_ms=now_ms,
        direction="recent-to-oldest",
    )

    assert coverage.complete_history == 2
    assert [gap.bucket_end_ms for gap in coverage.missing_buckets] == [closed_end - bucket_ms]


def test_scanner_supports_oldest_direction(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    now_ms = 1782835200000

    coverage = RangeBackfillScanner(store).scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_interval="4h",
        required_buckets=2,
        lookback_buckets=2,
        now_ms=now_ms,
        direction="oldest-to-recent",
    )

    assert coverage.missing_buckets[0].bucket_end_ms < coverage.missing_buckets[1].bucket_end_ms


def test_older_complete_buckets_do_not_mask_recent_required_gap(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    bucket_ms = 4 * 60 * 60_000
    now_ms = 1782835200000
    closed_end = now_ms - 1
    for offset in (0, 1, 3):
        end = closed_end - offset * bucket_ms
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(end - bucket_ms + 1, end, 10 + offset),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end,
        )

    coverage = RangeBackfillScanner(store).scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_interval="4h",
        required_buckets=3,
        lookback_buckets=4,
        now_ms=now_ms,
        direction="recent-to-oldest",
    )

    assert coverage.complete_history == 2
    assert coverage.required_window_complete_count == 2
    assert coverage.required_window_missing_count == 1
    assert coverage.required_window_missing_buckets[0].bucket_end_ms == closed_end - 2 * bucket_ms
    assert coverage.available is False


def test_recent_required_window_all_complete_is_available(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    bucket_ms = 4 * 60 * 60_000
    now_ms = 1782835200000
    closed_end = now_ms - 1
    for offset in (0, 1, 2):
        end = closed_end - offset * bucket_ms
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(end - bucket_ms + 1, end, 10 + offset),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end,
        )

    coverage = RangeBackfillScanner(store).scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_interval="4h",
        required_buckets=3,
        lookback_buckets=5,
        now_ms=now_ms,
    )

    assert coverage.available is True
    assert coverage.has_latest_closed_bucket is True


def test_latest_closed_bucket_missing_is_unavailable(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    bucket_ms = 4 * 60 * 60_000
    now_ms = 1782835200000
    closed_end = now_ms - 1
    for offset in (1, 2, 3):
        end = closed_end - offset * bucket_ms
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(end - bucket_ms + 1, end, 10 + offset),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end,
        )

    coverage = RangeBackfillScanner(store).scan(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct="0.002",
        bucket_interval="4h",
        required_buckets=3,
        lookback_buckets=4,
        now_ms=now_ms,
        direction="recent-to-oldest",
    )

    assert coverage.has_latest_closed_bucket is False
    assert coverage.available is False
