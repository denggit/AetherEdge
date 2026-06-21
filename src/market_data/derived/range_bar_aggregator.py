from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Sequence

from src.market_data.models import RangeBar, RangeBarAggregate


class RangeBarAggregator:
    """Aggregate closed range bars into fixed time buckets.

    Buckets are derived from range-bar ``end_time_ms``. For V8's 4H micro
    context, callers should use ``bucket_ms=4 * 60 * 60_000``.
    """

    def aggregate(self, rows: Sequence[RangeBar], *, bucket_ms: int) -> list[RangeBarAggregate]:
        if bucket_ms <= 0:
            raise ValueError("bucket_ms must be positive")
        buckets: dict[int, list[RangeBar]] = defaultdict(list)
        for row in rows:
            bucket_start = row.end_time_ms - (row.end_time_ms % bucket_ms)
            buckets[bucket_start].append(row)

        aggregates: list[RangeBarAggregate] = []
        for bucket_start in sorted(buckets):
            bucket_rows = sorted(buckets[bucket_start], key=lambda item: (item.end_time_ms, item.bar_id))
            first = bucket_rows[0]
            last = bucket_rows[-1]
            buy_sum = sum((row.buy_notional for row in bucket_rows), Decimal("0"))
            sell_sum = sum((row.sell_notional for row in bucket_rows), Decimal("0"))
            notional_sum = sum((row.notional for row in bucket_rows), Decimal("0"))
            aggregates.append(
                RangeBarAggregate(
                    symbol=first.symbol,
                    range_pct=first.range_pct,
                    bucket_start_ms=bucket_start,
                    bucket_end_ms=bucket_start + bucket_ms - 1,
                    bar_count=len(bucket_rows),
                    first_open=first.open,
                    last_close=last.close,
                    high=max(row.high for row in bucket_rows),
                    low=min(row.low for row in bucket_rows),
                    buy_notional_sum=buy_sum,
                    sell_notional_sum=sell_sum,
                    delta_notional_sum=buy_sum - sell_sum,
                    notional_sum=notional_sum,
                )
            )
        return aggregates
