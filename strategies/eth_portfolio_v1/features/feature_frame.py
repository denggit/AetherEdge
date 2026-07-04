from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.market_data.events import MarketFeatureEvent, MarketFeatureEventType
from strategies.eth_portfolio_v1.domain.models import ClosedKlineContext, RangeAggregateContext


def parse_closed_kline(event: MarketFeatureEvent) -> ClosedKlineContext:
    if event.type_value != MarketFeatureEventType.CLOSED_KLINE.value:
        raise ValueError(f"not a closed kline feature: {event.type_value}")
    data = event.data
    return ClosedKlineContext(
        symbol=event.symbol,
        exchange=event.exchange.value,
        timeframe=str(event.timeframe or ""),
        open_time_ms=int(data["open_time_ms"]),
        close_time_ms=int(data["close_time_ms"]),
        open=_d(data["open"]),
        high=_d(data["high"]),
        low=_d(data["low"]),
        close=_d(data["close"]),
        volume=_d(data["volume"]),
        quote_volume=None if data.get("quote_volume") is None else _d(data["quote_volume"]),
    )


def parse_range_aggregate(event: MarketFeatureEvent) -> RangeAggregateContext:
    if event.type_value != MarketFeatureEventType.RANGE_AGGREGATE.value:
        raise ValueError(f"not a range aggregate feature: {event.type_value}")
    data = event.data
    return RangeAggregateContext(
        symbol=event.symbol,
        exchange=event.exchange.value,
        timeframe=str(event.timeframe or ""),
        bucket_start_ms=int(data["bucket_start_ms"]),
        bucket_end_ms=int(data["bucket_end_ms"]),
        range_pct=_d(data["range_pct"]),
        bar_count=int(data["bar_count"]),
        first_open=_d(data["first_open"]),
        last_close=_d(data["last_close"]),
        high=_d(data["high"]),
        low=_d(data["low"]),
        buy_notional_sum=_d(data["buy_notional_sum"]),
        sell_notional_sum=_d(data["sell_notional_sum"]),
        delta_notional_sum=_d(data["delta_notional_sum"]),
        notional_sum=_d(data["notional_sum"]),
        micro_return_pct=_d(data["micro_return_pct"]),
        imbalance=_d(data["imbalance"]),
        taker_buy_ratio=_d(data["taker_buy_ratio"]),
        close_pos=_d(data["close_pos"]),
        coverage_status=str(data.get("coverage_status", "COMPLETE")),
        missing_gap_ms=int(data.get("missing_gap_ms", 0)),
        range_recovered_from_checkpoint=bool(
            data.get("range_recovered_from_checkpoint", False)
        ),
        range_checkpoint_age_ms=(
            None
            if data.get("range_checkpoint_age_ms") is None
            else int(data["range_checkpoint_age_ms"])
        ),
    )


def _d(value: Any) -> Decimal:
    return Decimal(str(value))
