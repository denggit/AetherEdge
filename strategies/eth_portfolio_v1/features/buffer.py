from __future__ import annotations

from dataclasses import dataclass, field

from strategies.eth_portfolio_v1.domain.models import ClosedKlineContext, RangeAggregateContext


@dataclass
class V8FeatureBuffer:
    closed_klines: dict[int, ClosedKlineContext] = field(default_factory=dict)
    range_aggregates: dict[int, RangeAggregateContext] = field(default_factory=dict)
    evaluated_bars: set[int] = field(default_factory=set)

    def put_kline(self, kline: ClosedKlineContext) -> None:
        self.closed_klines[kline.close_time_ms] = kline

    def put_range_aggregate(self, aggregate: RangeAggregateContext) -> None:
        self.range_aggregates[aggregate.bucket_end_ms] = aggregate

    def ready_times(self) -> list[int]:
        return sorted(set(self.closed_klines).intersection(self.range_aggregates).difference(self.evaluated_bars))

    def mark_evaluated(self, close_time_ms: int) -> None:
        self.evaluated_bars.add(close_time_ms)
