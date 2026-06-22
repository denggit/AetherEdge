from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from src.market_data.derived import RangeBarBuilder
from src.market_data.models import MarketDataSet, TimeRange, WarmupRequest
from src.market_data.ports import HistoricalTradeFeed, RangeBarRepository, TradeCoverageRepository, TradeRepository
from src.market_data.warmup.trade_service import TradeWarmupService


@dataclass(frozen=True)
class CurrentRangeBarWarmupResult:
    symbol: str
    time_range: TimeRange
    trades_loaded: int
    trades_available: int
    range_bars_saved: int
    downloaded: bool
    caught_up: bool


class CurrentRangeBarWarmupService:
    """Warm up current open 4H bucket trades into persisted range bars.

    This service is intentionally small: V8 does not need years of range bars for
    live startup; it needs the current signal bucket to be complete after a
    restart. Trades and range bars are persisted, so later restarts can reuse the
    local DB instead of re-downloading covered intervals.
    """

    def __init__(
        self,
        *,
        trade_repository: TradeRepository,
        trade_coverage_repository: TradeCoverageRepository,
        range_bar_repository: RangeBarRepository,
        historical_trade_feed: HistoricalTradeFeed | None,
        range_pct: Decimal,
        contract_value: Decimal,
        batch_limit: int = 1000,
        coverage_source: str = "historical_current_bucket",
    ) -> None:
        self.trade_repository = trade_repository
        self.trade_coverage_repository = trade_coverage_repository
        self.range_bar_repository = range_bar_repository
        self.historical_trade_feed = historical_trade_feed
        self.range_pct = Decimal(str(range_pct))
        self.contract_value = Decimal(str(contract_value))
        self.batch_limit = batch_limit
        self.coverage_source = coverage_source

    async def warmup(self, *, symbol: str, time_range: TimeRange) -> CurrentRangeBarWarmupResult:
        downloaded = False
        trades_loaded = 0
        caught_up = True
        if self.historical_trade_feed is not None:
            service = TradeWarmupService(
                data_feed=self.historical_trade_feed,
                repository=self.trade_repository,
                coverage_repository=self.trade_coverage_repository,
                batch_limit=self.batch_limit,
                coverage_source=self.coverage_source,
            )
            result = await service.warmup(WarmupRequest(symbol=symbol, dataset=MarketDataSet.TRADES, time_range=time_range))
            trades_loaded = result.records_loaded
            downloaded = trades_loaded > 0
            caught_up = result.caught_up
        else:
            covered = self.trade_coverage_repository.coverage_ranges(symbol=symbol, time_range=time_range, source=self.coverage_source)
            caught_up = _covers(time_range, covered)

        trades = self.trade_repository.load(symbol=symbol, time_range=time_range)
        builder = RangeBarBuilder(range_pct=self.range_pct, contract_value=self.contract_value)
        closed = []
        for trade in trades:
            closed.extend(builder.on_trade(trade))
        replace_range = getattr(self.range_bar_repository, "replace_range", None)
        if callable(replace_range):
            saved = replace_range(symbol=symbol, range_pct=str(self.range_pct), time_range=time_range, rows=closed)
        else:
            saved = self.range_bar_repository.save(closed)
        return CurrentRangeBarWarmupResult(
            symbol=symbol,
            time_range=time_range,
            trades_loaded=trades_loaded,
            trades_available=len(trades),
            range_bars_saved=saved,
            downloaded=downloaded,
            caught_up=caught_up,
        )


def _covers(target: TimeRange, covered: list[TimeRange]) -> bool:
    cursor = target.start_time_ms
    for item in sorted(covered, key=lambda row: row.start_time_ms):
        if item.end_time_ms < cursor:
            continue
        if item.start_time_ms > cursor:
            return False
        cursor = max(cursor, item.end_time_ms + 1)
        if cursor > target.end_time_ms:
            return True
    return cursor > target.end_time_ms
