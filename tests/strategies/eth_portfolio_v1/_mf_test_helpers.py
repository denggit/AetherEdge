from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from src.market_data.models import FixedTimeTradeBar, RangeFootprintFeature
from src.platform.exchanges.models import ExchangeName
from src.runtime.features import fixed_time_trade_bar_feature
from strategies.eth_portfolio_v1.domain.mf_data import MfDataBuffer
from strategies.eth_portfolio_v1.domain.mf_signal import MfLowSweepConfig


MINUTE_MS = 60_000
BASE_MS = 1_700_000_000_000
READY = {
    "mf_signal_feature_ready": True,
    "range_footprint_ready": True,
    "tradebar_ready": True,
    "source": "test_readiness_fixture",
}


def config(**overrides) -> MfLowSweepConfig:
    base = MfLowSweepConfig(
        enabled=True,
        position_fraction=Decimal("0.10"),
    )
    return replace(base, **overrides)


def historical_large_shares(
    *,
    value: str = "0.10",
    count: int = 43_200,
) -> tuple[Decimal, ...]:
    return (Decimal(value),) * count


def seed_large_share_history(
    buffer: MfDataBuffer,
    *,
    before_open_time_ms: int,
    value: str = "0.10",
) -> None:
    start = before_open_time_ms - 43_200 * MINUTE_MS
    for index in range(43_200):
        buffer._large_trade_shares.append(
            (start + index * MINUTE_MS, Decimal(value))
        )
    buffer._latest_history_open_time_ms = before_open_time_ms - MINUTE_MS


def closed_tradebar_event(
    item: FixedTimeTradeBar,
    *,
    next_open_price: str = "90",
    next_open_time_ms: int | None = None,
):
    return fixed_time_trade_bar_feature(
        item,
        exchange=ExchangeName.OKX,
        next_open_price=Decimal(next_open_price),
        next_open_time_ms=(
            item.close_time_ms + 1
            if next_open_time_ms is None
            else next_open_time_ms
        ),
    )


def setup_bars(
    *,
    latest_low: str = "89",
    latest_close: str = "89.5",
    latest_high: str = "101",
    latest_large_share: str = "0.90",
    latest_available_time_ms: int | None = None,
) -> list[FixedTimeTradeBar]:
    lows = [
        "100",
        "99",
        "98",
        "97",
        "96",
        "95",
        "90",
        "94",
        "95",
        "96",
        "97",
    ]
    bars: list[FixedTimeTradeBar] = []
    for index, low in enumerate(lows):
        bars.append(
            bar(
                index=index,
                low=low,
                high="102",
                open_price="100",
                close="100",
                large_share="0.10",
            )
        )
    bars.append(
        bar(
            index=len(lows),
            low=latest_low,
            high=latest_high,
            open_price="100",
            close=latest_close,
            large_share=latest_large_share,
            available_time_ms=latest_available_time_ms,
        )
    )
    return bars


def bar(
    *,
    index: int,
    low: str = "99",
    high: str = "101",
    open_price: str = "100",
    close: str = "100",
    large_share: str = "0.10",
    available_time_ms: int | None = None,
) -> FixedTimeTradeBar:
    open_ms = BASE_MS + index * MINUTE_MS
    close_ms = open_ms + MINUTE_MS - 1
    return FixedTimeTradeBar(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        timeframe="1m",
        open_time_ms=open_ms,
        close_time_ms=close_ms,
        available_time_ms=(
            close_ms
            if available_time_ms is None
            else available_time_ms
        ),
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("10"),
        buy_volume=Decimal("6"),
        sell_volume=Decimal("4"),
        buy_notional=Decimal("600"),
        sell_notional=Decimal("400"),
        delta_volume=Decimal("2"),
        delta_notional=Decimal("200"),
        abs_delta_notional=Decimal("200"),
        trade_count=10,
        large_buy_notional=Decimal("90"),
        large_sell_notional=Decimal("10"),
        large_trade_count=2,
        large_trade_share=Decimal(large_share),
    )


def range_footprint(
    *,
    available_time_ms: int,
    pressure: str = "0.80",
    context_available: bool = True,
    quality: str = "COMPLETE",
) -> RangeFootprintFeature:
    return RangeFootprintFeature(
        exchange="okx",
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        price_step=Decimal("1"),
        range_bar_id=20260705000001,
        range_start_ms=available_time_ms - 30_000,
        range_end_ms=available_time_ms,
        available_time_ms=available_time_ms,
        fp_max_bucket_abs_delta_pressure=Decimal(pressure),
        fp_low_bucket_delta_pressure=Decimal("-0.20"),
        fp_high_bucket_delta_pressure=Decimal("0.40"),
        fp_delta_pressure=Decimal("0.10"),
        bucket_count=5,
        trade_count=20,
        context_available=context_available,
        quality=quality,
    )
