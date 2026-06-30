from __future__ import annotations

from decimal import Decimal
import time

import pytest

from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.backfill.coverage import current_closed_bucket_end_ms
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher


class EntryFilters:
    range_speed_rolling_window_bars = 100
    range_speed_min_periods = 3


class Config:
    entry_filters = EntryFilters()


class Strategy:
    config = Config()

    def __init__(self) -> None:
        self.values: list[int] = []
        self.range_speed_tracker = type("Tracker", (), {"complete_history_count": 0})()

    def replace_range_speed_history(self, values) -> int:
        self.values = list(values)
        self.range_speed_tracker.complete_history_count = len(self.values)
        return len(self.values)


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
