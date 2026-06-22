from __future__ import annotations

from src.market_data.models import DataGap, MarketDataSet, TimeRange, WarmupRequest, WarmupResult
from src.market_data.ports import HistoricalTradeFeed, TradeCoverageRepository, TradeRepository
from src.platform.data.models import MarketTrade


class TradeWarmupService:
    """Backfill historical trades in bounded batches.

    The service uses a protocol-based historical trade feed, so OKX/Binance raw
    adapters stay outside the market-data domain. Coverage tracking lets later
    restarts skip already downloaded intervals instead of reprocessing full
    history.
    """

    def __init__(
        self,
        *,
        data_feed: HistoricalTradeFeed,
        repository: TradeRepository,
        coverage_repository: TradeCoverageRepository,
        batch_limit: int = 1000,
        coverage_source: str = "historical",
    ) -> None:
        if batch_limit <= 0:
            raise ValueError("batch_limit must be positive")
        self.data_feed = data_feed
        self.repository = repository
        self.coverage_repository = coverage_repository
        self.batch_limit = batch_limit
        self.coverage_source = coverage_source

    async def warmup(self, request: WarmupRequest) -> WarmupResult:
        if request.dataset is not MarketDataSet.TRADES:
            raise ValueError("TradeWarmupService only supports the TRADES dataset")

        missing_before = tuple(
            DataGap(symbol=request.symbol, dataset=request.dataset, time_range=item, reason="missing_coverage")
            for item in _subtract_ranges(
                request.time_range,
                self.coverage_repository.coverage_ranges(symbol=request.symbol, time_range=request.time_range, source=self.coverage_source),
            )
        )
        records_loaded = 0
        for gap in missing_before:
            records_loaded += await self._fill_gap(symbol=request.symbol, time_range=gap.time_range)

        gaps_after = tuple(
            DataGap(symbol=request.symbol, dataset=request.dataset, time_range=item, reason="missing_coverage")
            for item in _subtract_ranges(
                request.time_range,
                self.coverage_repository.coverage_ranges(symbol=request.symbol, time_range=request.time_range, source=self.coverage_source),
            )
        )
        return WarmupResult(
            request=request,
            gaps_before=missing_before,
            gaps_after=gaps_after,
            records_loaded=records_loaded,
            caught_up=len(gaps_after) == 0,
        )

    async def _fill_gap(self, *, symbol: str, time_range: TimeRange) -> int:
        cursor = time_range.start_time_ms
        total = 0
        while cursor <= time_range.end_time_ms:
            rows = await self.data_feed.fetch_trades(
                symbol=symbol,
                start_time_ms=cursor,
                end_time_ms=time_range.end_time_ms,
                limit=self.batch_limit,
                oldest_first=True,
            )
            clean_rows = sorted(
                [row for row in rows if row.symbol == symbol and _trade_time_ms(row) is not None and cursor <= _trade_time_ms(row) <= time_range.end_time_ms],
                key=lambda row: (_trade_time_ms(row), row.trade_id or ""),
            )
            if not clean_rows:
                break
            total += self.repository.save(clean_rows)
            min_time = min(_trade_time_ms(row) for row in clean_rows)
            max_time = max(_trade_time_ms(row) for row in clean_rows)
            assert min_time is not None
            assert max_time is not None
            if min_time > cursor and len(clean_rows) >= self.batch_limit:
                # Some exchange adapters can only page recent trades backward.
                # If the first returned batch starts after our requested cursor
                # and is already full, we have not proven the earlier interval
                # is empty. Mark only the interval actually covered by returned
                # rows so the remaining prefix stays visible as a gap.
                self.coverage_repository.mark_coverage(symbol=symbol, time_range=TimeRange(min_time, max_time), source=self.coverage_source)
                break
            self.coverage_repository.mark_coverage(symbol=symbol, time_range=TimeRange(cursor, max_time), source=self.coverage_source)
            if len(clean_rows) < self.batch_limit:
                if max_time < time_range.end_time_ms:
                    self.coverage_repository.mark_coverage(symbol=symbol, time_range=TimeRange(max_time + 1, time_range.end_time_ms), source=self.coverage_source)
                break
            next_cursor = max_time + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor
        return total


def _trade_time_ms(trade: MarketTrade) -> int | None:
    if trade.trade_time_ms is not None:
        return trade.trade_time_ms
    return trade.event_time_ms


def _subtract_ranges(target: TimeRange, covered: list[TimeRange]) -> list[TimeRange]:
    if not covered:
        return [target]
    gaps: list[TimeRange] = []
    cursor = target.start_time_ms
    for item in sorted(covered, key=lambda row: row.start_time_ms):
        if item.end_time_ms < cursor:
            continue
        if item.start_time_ms > target.end_time_ms:
            break
        if item.start_time_ms > cursor:
            gaps.append(TimeRange(cursor, min(item.start_time_ms - 1, target.end_time_ms)))
        cursor = max(cursor, item.end_time_ms + 1)
        if cursor > target.end_time_ms:
            break
    if cursor <= target.end_time_ms:
        gaps.append(TimeRange(cursor, target.end_time_ms))
    return gaps
