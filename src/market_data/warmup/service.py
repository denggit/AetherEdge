from __future__ import annotations

from src.market_data.models import MarketDataSet, TimeRange, WarmupRequest, WarmupResult
from src.market_data.ports import KlineRepository
from src.market_data.warmup.gap_detector import KlineGapDetector, interval_to_ms
from src.platform.data.ports import MarketDataFeed


class KlineWarmupService:
    """Backfill reusable local kline data through the platform data facade.

    This service orchestrates the internal market-data pipeline. It reuses the
    existing ``MarketDataFeed`` port for exchange access and ``KlineRepository``
    for local persistence, but it does not contain strategy logic.
    """

    def __init__(
        self,
        *,
        data_feed: MarketDataFeed,
        repository: KlineRepository,
        gap_detector: KlineGapDetector | None = None,
        batch_limit: int = 100,
        max_gap_passes: int = 100,
    ) -> None:
        if batch_limit <= 0:
            raise ValueError("batch_limit must be positive")
        if max_gap_passes <= 0:
            raise ValueError("max_gap_passes must be positive")
        self.data_feed = data_feed
        self.repository = repository
        self.gap_detector = gap_detector or KlineGapDetector(repository)
        self.batch_limit = batch_limit
        self.max_gap_passes = max_gap_passes

    async def warmup(self, request: WarmupRequest) -> WarmupResult:
        if request.dataset is not MarketDataSet.KLINES:
            raise ValueError("KlineWarmupService only supports the KLINES dataset")
        if request.interval is None:
            raise ValueError("interval is required for kline warmup")

        gaps_before = tuple(self.gap_detector.find_gaps(symbol=request.symbol, dataset=request.dataset, time_range=request.time_range, interval=request.interval))
        records_loaded = 0
        gaps_after = gaps_before

        for _ in range(self.max_gap_passes):
            if not gaps_after:
                break

            pass_start_signature = _gap_signature(gaps_after)
            for gap in gaps_after:
                records_loaded += await self._fill_gap(request=request, gap_range=gap.time_range)

            next_gaps = tuple(self.gap_detector.find_gaps(symbol=request.symbol, dataset=request.dataset, time_range=request.time_range, interval=request.interval))
            if not next_gaps:
                gaps_after = next_gaps
                break
            if _gap_signature(next_gaps) == pass_start_signature:
                gaps_after = next_gaps
                break
            gaps_after = next_gaps

        return WarmupResult(
            request=request,
            gaps_before=gaps_before,
            gaps_after=gaps_after,
            records_loaded=records_loaded,
            caught_up=len(gaps_after) == 0,
        )

    async def _fill_gap(self, *, request: WarmupRequest, gap_range: TimeRange) -> int:
        assert request.interval is not None
        step_ms = interval_to_ms(request.interval)
        cursor = gap_range.start_time_ms
        total = 0

        while cursor <= gap_range.end_time_ms:
            rows = await self.data_feed.fetch_klines(
                interval=request.interval,
                limit=self.batch_limit,
                start_time_ms=cursor,
                end_time_ms=gap_range.end_time_ms,
                use_cache=False,
                oldest_first=True,
            )
            closed_rows = [
                row
                for row in rows
                if row.is_closed
                and row.symbol == request.symbol
                and cursor <= row.open_time_ms <= gap_range.end_time_ms
            ]
            if not closed_rows:
                break

            saved = self.repository.save(closed_rows)
            total += saved
            max_open_time = max(row.open_time_ms for row in closed_rows)
            next_cursor = max_open_time + step_ms
            if next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(closed_rows) < self.batch_limit:
                break

        return total


def _gap_signature(gaps) -> tuple[tuple[int, int, str], ...]:
    return tuple(
        (
            gap.time_range.start_time_ms,
            gap.time_range.end_time_ms,
            gap.reason,
        )
        for gap in gaps
    )
