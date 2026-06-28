from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class MarketDataSet(str, Enum):
    """Reusable internal market-data dataset names.

    These names describe AetherEdge's local data pipeline. They are not tied to
    any concrete exchange adapter or strategy implementation.
    """

    KLINES = "klines"
    TRADES = "trades"
    RANGE_BARS = "range_bars"


class RangeCoverageStatus(str, Enum):
    """Quality of trade coverage behind one fixed-time range aggregate."""

    COMPLETE = "COMPLETE"
    COLD_START_PARTIAL = "COLD_START_PARTIAL"
    RECOVERED_DEGRADED_MINOR = "RECOVERED_DEGRADED_MINOR"
    RECOVERED_INCOMPLETE = "RECOVERED_INCOMPLETE"


@dataclass(frozen=True)
class TimeRange:
    """Inclusive millisecond time range used by warmup and storage services."""

    start_time_ms: int
    end_time_ms: int

    def __post_init__(self) -> None:
        if self.start_time_ms < 0 or self.end_time_ms < 0:
            raise ValueError("time range values must be non-negative")
        if self.end_time_ms < self.start_time_ms:
            raise ValueError("end_time_ms must be greater than or equal to start_time_ms")


@dataclass(frozen=True)
class DataGap:
    """A missing local-data interval that should be backfilled."""

    symbol: str
    dataset: MarketDataSet
    time_range: TimeRange
    interval: str | None = None
    reason: str = "missing"

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")


@dataclass(frozen=True)
class WarmupRequest:
    """Request to ensure local market data exists for a time range."""

    symbol: str
    dataset: MarketDataSet
    time_range: TimeRange
    interval: str | None = None

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")


@dataclass(frozen=True)
class WarmupResult:
    """Outcome of a market-data warmup pass."""

    request: WarmupRequest
    gaps_before: tuple[DataGap, ...]
    gaps_after: tuple[DataGap, ...]
    records_loaded: int = 0
    caught_up: bool = False


@dataclass(frozen=True)
class RangeBar:
    """Exchange-agnostic range bar derived from normalized trades."""

    symbol: str
    range_pct: Decimal
    bar_id: int
    start_time_ms: int
    end_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    buy_notional: Decimal
    sell_notional: Decimal
    trade_count: int

    @property
    def notional(self) -> Decimal:
        return self.buy_notional + self.sell_notional

    @property
    def delta_notional(self) -> Decimal:
        return self.buy_notional - self.sell_notional

    @property
    def direction(self) -> int:
        if self.close > self.open:
            return 1
        if self.close < self.open:
            return -1
        return 0

    @property
    def actual_range_pct(self) -> Decimal:
        if self.open == 0:
            return Decimal("0")
        return (self.high - self.low) / self.open

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if self.range_pct <= 0:
            raise ValueError("range_pct must be positive")
        if self.bar_id < 0:
            raise ValueError("bar_id must be non-negative")
        if self.end_time_ms < self.start_time_ms:
            raise ValueError("end_time_ms must be greater than or equal to start_time_ms")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("range bar prices must be positive")
        if self.high < self.low:
            raise ValueError("high must be greater than or equal to low")
        if self.volume < 0 or self.buy_notional < 0 or self.sell_notional < 0:
            raise ValueError("volume and notional values must be non-negative")
        if self.trade_count < 0:
            raise ValueError("trade_count must be non-negative")


@dataclass(frozen=True)
class RangeBarAggregate:
    """Time-bucket aggregate of range bars for strategy feature consumers."""

    symbol: str
    range_pct: Decimal
    bucket_start_ms: int
    bucket_end_ms: int
    bar_count: int
    first_open: Decimal
    last_close: Decimal
    high: Decimal
    low: Decimal
    buy_notional_sum: Decimal
    sell_notional_sum: Decimal
    delta_notional_sum: Decimal
    notional_sum: Decimal

    @property
    def micro_return_pct(self) -> Decimal:
        if self.first_open == 0:
            return Decimal("0")
        return self.last_close / self.first_open - Decimal("1")

    @property
    def imbalance(self) -> Decimal:
        denom = self.buy_notional_sum + self.sell_notional_sum
        if denom == 0:
            return Decimal("0")
        return (self.buy_notional_sum - self.sell_notional_sum) / denom

    @property
    def taker_buy_ratio(self) -> Decimal:
        denom = self.buy_notional_sum + self.sell_notional_sum
        if denom == 0:
            return Decimal("0")
        return self.buy_notional_sum / denom

    @property
    def close_pos(self) -> Decimal:
        span = self.high - self.low
        if span == 0:
            return Decimal("0.5")
        return (self.last_close - self.low) / span

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if self.range_pct <= 0:
            raise ValueError("range_pct must be positive")
        if self.bucket_end_ms < self.bucket_start_ms:
            raise ValueError("bucket_end_ms must be greater than or equal to bucket_start_ms")
        if self.bar_count < 0:
            raise ValueError("bar_count must be non-negative")
