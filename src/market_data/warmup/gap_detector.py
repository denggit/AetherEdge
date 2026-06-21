from __future__ import annotations

from collections.abc import Iterable

from src.market_data.models import DataGap, MarketDataSet, TimeRange
from src.market_data.ports import KlineRepository
from src.platform.data.models import MarketKline

_INTERVAL_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}


def interval_to_ms(interval: str) -> int:
    """Convert a supported kline interval to milliseconds.

    AetherEdge accepts both exchange-style intervals such as ``4H`` and
    normalized lower-case intervals such as ``4h``. The returned value is used
    by internal warmup/gap detection only; exchange adapters still own any
    venue-specific interval naming.
    """

    if not interval:
        raise ValueError("interval is required")
    normalized = interval.strip().lower()
    if normalized not in _INTERVAL_MS:
        raise ValueError(f"unsupported interval: {interval}")
    return _INTERVAL_MS[normalized]


class KlineGapDetector:
    """Find missing closed kline intervals in the internal local store.

    The detector belongs to the reusable market-data pipeline. It only knows
    about normalized ``MarketKline`` records and does not depend on any strategy
    logic or exchange adapter implementation.
    """

    def __init__(self, repository: KlineRepository) -> None:
        self.repository = repository

    def find_gaps(self, *, symbol: str, dataset: MarketDataSet, time_range: TimeRange, interval: str | None = None) -> list[DataGap]:
        if dataset is not MarketDataSet.KLINES:
            raise ValueError("KlineGapDetector only supports the KLINES dataset")
        if interval is None:
            raise ValueError("interval is required for kline gap detection")

        step_ms = interval_to_ms(interval)
        rows = self.repository.load(symbol=symbol, interval=interval, time_range=time_range)
        closed_open_times = _closed_unique_open_times(rows)
        if not closed_open_times:
            return [DataGap(symbol=symbol, dataset=dataset, time_range=time_range, interval=interval, reason="empty")]

        gaps: list[DataGap] = []
        cursor = time_range.start_time_ms
        for open_time_ms in closed_open_times:
            if open_time_ms < cursor:
                continue
            if open_time_ms > time_range.end_time_ms:
                break
            if open_time_ms > cursor:
                gaps.append(
                    DataGap(
                        symbol=symbol,
                        dataset=dataset,
                        time_range=TimeRange(cursor, min(open_time_ms - step_ms, time_range.end_time_ms)),
                        interval=interval,
                        reason="missing",
                    )
                )
            cursor = max(cursor, open_time_ms + step_ms)

        if cursor <= time_range.end_time_ms:
            gaps.append(
                DataGap(
                    symbol=symbol,
                    dataset=dataset,
                    time_range=TimeRange(cursor, time_range.end_time_ms),
                    interval=interval,
                    reason="missing_tail",
                )
            )
        return gaps


def _closed_unique_open_times(rows: Iterable[MarketKline]) -> list[int]:
    return sorted({row.open_time_ms for row in rows if row.is_closed})
