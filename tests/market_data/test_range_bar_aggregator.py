from __future__ import annotations

from decimal import Decimal

from src.market_data.derived import RangeBarAggregator
from src.market_data.models import RangeBar

HOUR = 60 * 60_000


def _bar(bar_id: int, end_time_ms: int, open_: str, close: str, buy: str, sell: str) -> RangeBar:
    return RangeBar(
        symbol="ETH-USDT-PERP",
        range_pct=Decimal("0.002"),
        bar_id=bar_id,
        start_time_ms=end_time_ms - 1000,
        end_time_ms=end_time_ms,
        open=Decimal(open_),
        high=Decimal(max(open_, close, key=Decimal)),
        low=Decimal(min(open_, close, key=Decimal)),
        close=Decimal(close),
        volume=Decimal("1"),
        buy_notional=Decimal(buy),
        sell_notional=Decimal(sell),
        trade_count=1,
    )


def test_range_bar_aggregator_groups_by_end_time_bucket():
    agg = RangeBarAggregator()
    rows = [
        _bar(1, HOUR, "1000", "1002", "10", "0"),
        _bar(2, 2 * HOUR, "1002", "1001", "0", "5"),
        _bar(3, 5 * HOUR, "1001", "1004", "20", "10"),
    ]

    out = agg.aggregate(rows, bucket_ms=4 * HOUR)

    assert len(out) == 2
    assert out[0].bucket_start_ms == 0
    assert out[0].bar_count == 2
    assert out[0].first_open == Decimal("1000")
    assert out[0].last_close == Decimal("1001")
    assert out[0].buy_notional_sum == Decimal("10")
    assert out[0].sell_notional_sum == Decimal("5")
    assert out[0].delta_notional_sum == Decimal("5")
    assert out[0].imbalance == Decimal("5") / Decimal("15")
    assert out[0].micro_return_pct == Decimal("0.001")
    assert out[1].bucket_start_ms == 4 * HOUR
