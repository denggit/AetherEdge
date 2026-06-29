from __future__ import annotations

from dataclasses import dataclass

from src.market_data.models import TimeRange
from src.market_data.storage import SqliteTradeStore
from src.platform.data.models import MarketTrade


@dataclass(frozen=True)
class CoverageValidation:
    complete: bool
    trade_count: int
    earliest_trade_ts_ms: int | None
    latest_trade_ts_ms: int | None
    max_gap_ms: int | None
    reason: str


def validate_trade_coverage(
    *,
    trade_store: SqliteTradeStore,
    symbol: str,
    bucket_start_ms: int,
    bucket_end_ms: int,
    edge_tolerance_ms: int = 60_000,
    coverage_max_gap_ms: int = 15 * 60_000,
) -> CoverageValidation:
    bucket = TimeRange(bucket_start_ms, bucket_end_ms)
    for covered in trade_store.coverage_ranges(symbol=symbol, time_range=bucket):
        if covered.start_time_ms <= bucket_start_ms and covered.end_time_ms >= bucket_end_ms:
            return CoverageValidation(True, 0, None, None, None, "coverage_range_complete")

    trades = trade_store.load(symbol=symbol, time_range=bucket)
    times = sorted(_trade_time_ms(trade) for trade in trades if _trade_time_ms(trade) is not None)
    if not times:
        return CoverageValidation(False, 0, None, None, None, "no_trades")
    max_gap = max((b - a for a, b in zip(times, times[1:])), default=0)
    if times[0] > bucket_start_ms + edge_tolerance_ms:
        return CoverageValidation(False, len(times), times[0], times[-1], max_gap, "late_first_trade")
    if times[-1] < bucket_end_ms - edge_tolerance_ms:
        return CoverageValidation(False, len(times), times[0], times[-1], max_gap, "early_last_trade")
    if max_gap > coverage_max_gap_ms:
        return CoverageValidation(False, len(times), times[0], times[-1], max_gap, "max_gap_exceeded")
    return CoverageValidation(True, len(times), times[0], times[-1], max_gap, "trade_span_complete")


def _trade_time_ms(trade: MarketTrade) -> int | None:
    return trade.trade_time_ms if trade.trade_time_ms is not None else trade.event_time_ms
