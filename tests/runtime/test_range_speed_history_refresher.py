from __future__ import annotations

from decimal import Decimal
import time

import pytest

from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.backfill.coverage import current_closed_bucket_end_ms
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher


class Strategy:
    def __init__(self) -> None:
        self.values: list[int] = []
        self.calls = 0

    def warmup_range_speed_history(self, values) -> int:
        self.values.extend(values)
        return len(values)

    def replace_range_speed_history(self, values) -> int:
        self.calls += 1
        self.values = list(values)
        return len(self.values)

    def range_speed_history_status(self):
        return {
            "complete_history": len(self.values),
            "min_periods": 3,
            "rolling_window_bars": 100,
            "available": len(self.values) >= 3,
        }


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


@pytest.mark.asyncio
async def test_refresher_calls_replace_range_speed_history(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    bucket_ms = 4 * 60 * 60_000
    closed_end = current_closed_bucket_end_ms(int(time.time() * 1000), "4h")
    for index in range(3):
        end = closed_end - (2 - index) * bucket_ms
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(end - bucket_ms + 1, end, index + 10),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end,
        )
    strategy = Strategy()
    refresher = RangeSpeedHistoryRefresher(
        strategy=strategy,
        store=store,
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        status_path=str(tmp_path / "status.json"),
    )

    status = await refresher.refresh_once()

    assert strategy.values == [10, 11, 12]
    assert status.available is True


@pytest.mark.asyncio
async def test_refresher_recovery_available_without_restart(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    strategy = Strategy()
    refresher = RangeSpeedHistoryRefresher(
        strategy=strategy,
        store=store,
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        warning_seconds=600,
        status_path=str(tmp_path / "status.json"),
    )

    status = await refresher.refresh_once()

    assert status.available is False
    assert strategy.values == []


@pytest.mark.asyncio
async def test_refresher_detects_older_bucket_backfill_when_latest_marker_unchanged(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    bucket_ms = 4 * 60 * 60_000
    closed_end = current_closed_bucket_end_ms(int(time.time() * 1000), "4h")
    for offset, count in ((0, 12), (1, 11)):
        end = closed_end - offset * bucket_ms
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(end - bucket_ms + 1, end, count),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end,
        )
    strategy = Strategy()
    refresher = RangeSpeedHistoryRefresher(
        strategy=strategy,
        store=store,
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        status_path=str(tmp_path / "status.json"),
    )

    first = await refresher.refresh_once()
    old_end = closed_end - 2 * bucket_ms
    store.save_completed_aggregate(
        exchange="okx",
        aggregate=_aggregate(old_end - bucket_ms + 1, old_end, 10),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        completed_at_ms=closed_end + 999,
    )
    second = await refresher.refresh_once()

    assert first.available is False
    assert second.available is True
    assert strategy.values == [10, 11, 12]
    assert strategy.calls == 2

@pytest.mark.asyncio
async def test_refresher_keeps_rolling_complete_history_when_required_window_has_gap(tmp_path) -> None:
    store = SqliteRangeCheckpointStore(tmp_path / "checkpoint.sqlite3")
    bucket_ms = 4 * 60 * 60_000
    closed_end = current_closed_bucket_end_ms(int(time.time() * 1000), "4h")
    # Required window is the latest 3 buckets.  Leave the middle one missing,
    # but keep enough complete buckets in the rolling history for the strategy
    # tracker to remain usable.
    for offset, count in ((0, 13), (2, 11), (3, 10)):
        end = closed_end - offset * bucket_ms
        store.save_completed_aggregate(
            exchange="okx",
            aggregate=_aggregate(end - bucket_ms + 1, end, count),
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            completed_at_ms=end,
        )
    strategy = Strategy()
    refresher = RangeSpeedHistoryRefresher(
        strategy=strategy,
        store=store,
        symbol="ETH-USDT-PERP",
        exchange="okx",
        range_pct="0.002",
        bucket_interval="4h",
        status_path=str(tmp_path / "status.json"),
    )

    status = await refresher.refresh_once()

    assert status.available is False
    assert status.missing_periods == 1
    assert status.first_missing_bucket_end_ms == closed_end - bucket_ms
    assert strategy.values == [10, 11, 13]
    assert status.complete_history == 3

