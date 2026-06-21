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


def range_aggregate_feature(aggregate: RangeBarAggregate, *, exchange, timeframe: str = "4h") -> MarketFeatureEvent:
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
        },
    )


def _d(value: Decimal) -> str:
    return format(value.normalize(), "f")
