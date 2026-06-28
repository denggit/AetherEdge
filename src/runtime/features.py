from __future__ import annotations

from decimal import Decimal

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from src.market_data.models import RangeBar, RangeBarAggregate
from src.platform.data.models import MarketKline


def closed_kline_feature(kline: MarketKline) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.CLOSED_KLINE,
        symbol=kline.symbol,
        exchange=kline.exchange,
        timeframe=kline.interval,
        event_time_ms=kline.close_time_ms,
        data={
            "open_time_ms": kline.open_time_ms,
            "close_time_ms": kline.close_time_ms,
            "open": str(kline.open),
            "high": str(kline.high),
            "low": str(kline.low),
            "close": str(kline.close),
            "volume": str(kline.volume),
            "quote_volume": None if kline.quote_volume is None else str(kline.quote_volume),
            "is_closed": kline.is_closed,
        },
    )


def range_bar_closed_feature(bar: RangeBar, *, exchange) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.RANGE_BAR_CLOSED,
        symbol=bar.symbol,
        exchange=exchange,
        timeframe=None,
        event_time_ms=bar.end_time_ms,
        data={
            "range_pct": _d(bar.range_pct),
            "bar_id": bar.bar_id,
            "start_time_ms": bar.start_time_ms,
            "end_time_ms": bar.end_time_ms,
            "open": _d(bar.open),
            "high": _d(bar.high),
            "low": _d(bar.low),
            "close": _d(bar.close),
            "volume": _d(bar.volume),
            "buy_notional": _d(bar.buy_notional),
            "sell_notional": _d(bar.sell_notional),
            "delta_notional": _d(bar.delta_notional),
            "trade_count": bar.trade_count,
        },
    )


def range_aggregate_feature(
    aggregate: RangeBarAggregate,
    *,
    exchange,
    timeframe: str = "4h",
    coverage_status: str = "COMPLETE",
    missing_gap_ms: int = 0,
    range_recovered_from_checkpoint: bool = False,
    range_checkpoint_age_ms: int | None = None,
) -> MarketFeatureEvent:
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.RANGE_AGGREGATE,
        symbol=aggregate.symbol,
        exchange=exchange,
        timeframe=timeframe,
        event_time_ms=aggregate.bucket_end_ms,
        data={
            "range_pct": _d(aggregate.range_pct),
            "bucket_start_ms": aggregate.bucket_start_ms,
            "bucket_end_ms": aggregate.bucket_end_ms,
            "bar_count": aggregate.bar_count,
            "first_open": _d(aggregate.first_open),
            "last_close": _d(aggregate.last_close),
            "high": _d(aggregate.high),
            "low": _d(aggregate.low),
            "buy_notional_sum": _d(aggregate.buy_notional_sum),
            "sell_notional_sum": _d(aggregate.sell_notional_sum),
            "delta_notional_sum": _d(aggregate.delta_notional_sum),
            "notional_sum": _d(aggregate.notional_sum),
            "micro_return_pct": _d(aggregate.micro_return_pct),
            "imbalance": _d(aggregate.imbalance),
            "taker_buy_ratio": _d(aggregate.taker_buy_ratio),
            "close_pos": _d(aggregate.close_pos),
            "coverage_status": coverage_status,
            "missing_gap_ms": max(0, int(missing_gap_ms)),
            "range_recovered_from_checkpoint": bool(
                range_recovered_from_checkpoint
            ),
            "range_checkpoint_age_ms": range_checkpoint_age_ms,
        },
    )


def range_aggregate_unavailable_feature(
    *,
    symbol: str,
    exchange,
    timeframe: str,
    range_pct: Decimal,
    bucket_start_ms: int,
    bucket_end_ms: int,
    reference_price: Decimal,
    reason: str,
    coverage_status: str = "COLD_START_PARTIAL",
    missing_gap_ms: int = 0,
    range_recovered_from_checkpoint: bool = False,
    range_checkpoint_age_ms: int | None = None,
) -> MarketFeatureEvent:
    price = Decimal(str(reference_price))
    return MarketFeatureEvent(
        event_type=MarketFeatureEventType.RANGE_AGGREGATE,
        symbol=symbol,
        exchange=exchange,
        timeframe=timeframe,
        event_time_ms=bucket_end_ms,
        data={
            "range_pct": _d(range_pct),
            "bucket_start_ms": bucket_start_ms,
            "bucket_end_ms": bucket_end_ms,
            "bar_count": 0,
            "first_open": _d(price),
            "last_close": _d(price),
            "high": _d(price),
            "low": _d(price),
            "buy_notional_sum": "0",
            "sell_notional_sum": "0",
            "delta_notional_sum": "0",
            "notional_sum": "0",
            "micro_return_pct": "0",
            "imbalance": "0",
            "taker_buy_ratio": "0",
            "close_pos": "0.5",
            "context_available": False,
            "incomplete": True,
            "reason": reason,
            "coverage_status": coverage_status,
            "missing_gap_ms": max(0, int(missing_gap_ms)),
            "range_recovered_from_checkpoint": bool(
                range_recovered_from_checkpoint
            ),
            "range_checkpoint_age_ms": range_checkpoint_age_ms,
        },
    )


def _d(value: Decimal) -> str:
    return format(value.normalize(), "f")
