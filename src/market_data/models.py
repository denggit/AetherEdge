from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping


class MarketDataSet(str, Enum):
    """Reusable internal market-data dataset names.

    These names describe AetherEdge's local data pipeline. They are not tied to
    any concrete exchange adapter or strategy implementation.
    """

    KLINES = "klines"
    TRADES = "trades"
    RANGE_BARS = "range_bars"
    TRADE_DERIVED_FEATURES = "trade_derived_features"


class TradeFeatureQuality(str, Enum):
    """Quality of a trade-derived feature bar."""

    COMPLETE = "COMPLETE"
    MISSING_FOOTPRINT_CONTEXT = "MISSING_FOOTPRINT_CONTEXT"
    DEGRADED_LOW_TRADE_COUNT = "DEGRADED_LOW_TRADE_COUNT"
    RECOVERED_FROM_JOURNAL = "RECOVERED_FROM_JOURNAL"


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


@dataclass(frozen=True)
class FixedTimeTradeBar:
    """Exchange-agnostic 1m trade-derived bar (OHLCV + order-flow).

    All values are derived from raw trades — never from kline/OHLCV sources.
    """

    exchange: str
    symbol: str
    timeframe: str = "1m"
    open_time_ms: int = 0
    close_time_ms: int = 0
    available_time_ms: int = 0
    open: Decimal = Decimal("0")
    high: Decimal = Decimal("0")
    low: Decimal = Decimal("0")
    close: Decimal = Decimal("0")
    volume: Decimal = Decimal("0")
    buy_volume: Decimal = Decimal("0")
    sell_volume: Decimal = Decimal("0")
    buy_notional: Decimal = Decimal("0")
    sell_notional: Decimal = Decimal("0")
    delta_volume: Decimal = Decimal("0")
    delta_notional: Decimal = Decimal("0")
    abs_delta_notional: Decimal = Decimal("0")
    trade_count: int = 0
    large_buy_notional: Decimal = Decimal("0")
    large_sell_notional: Decimal = Decimal("0")
    large_trade_count: int = 0
    large_trade_share: Decimal = Decimal("0")
    quality: str = TradeFeatureQuality.COMPLETE.value
    source: str = "trade_derived"

    def __post_init__(self) -> None:
        if not self.exchange:
            raise ValueError("exchange is required")
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.timeframe:
            raise ValueError("timeframe is required")
        if self.open_time_ms < 0 or self.close_time_ms < 0 or self.available_time_ms < 0:
            raise ValueError("time fields must be non-negative")
        if self.available_time_ms < self.close_time_ms:
            raise ValueError("available_time_ms must be >= close_time_ms")
        if self.close_time_ms < self.open_time_ms:
            raise ValueError("close_time_ms must be >= open_time_ms")
        if self.trade_count < 0:
            raise ValueError("trade_count must be non-negative")
        # OHLCV validation
        if self.open <= 0 or self.high <= 0 or self.low <= 0 or self.close <= 0:
            raise ValueError("OHLC must be positive")
        if self.high < self.low:
            raise ValueError("high must be >= low")
        if self.open < self.low or self.open > self.high:
            raise ValueError("open must be within [low, high]")
        if self.close < self.low or self.close > self.high:
            raise ValueError("close must be within [low, high]")
        # Volume / notional
        if self.volume < 0:
            raise ValueError("volume must be non-negative")
        if self.buy_volume < 0 or self.sell_volume < 0:
            raise ValueError("buy/sell volume must be non-negative")
        if self.buy_notional < 0 or self.sell_notional < 0:
            raise ValueError("buy/sell notional must be non-negative")
        if self.large_buy_notional < 0 or self.large_sell_notional < 0:
            raise ValueError("large notional must be non-negative")
        if self.large_trade_count < 0:
            raise ValueError("large_trade_count must be non-negative")
        if self.large_trade_share < 0 or self.large_trade_share > 1:
            raise ValueError("large_trade_share must be within [0, 1]")
        # Delta consistency (allow small float tolerance for Decimal)
        _delta_vol = self.buy_volume - self.sell_volume
        _delta_not = self.buy_notional - self.sell_notional
        _abs_delta = abs(_delta_not)
        if self.delta_volume != _delta_vol:
            raise ValueError(f"delta_volume {self.delta_volume} != buy_vol - sell_vol {_delta_vol}")
        if self.delta_notional != _delta_not:
            raise ValueError(f"delta_notional {self.delta_notional} != buy_not - sell_not {_delta_not}")
        if self.abs_delta_notional != _abs_delta:
            raise ValueError(f"abs_delta_notional {self.abs_delta_notional} != abs(delta_notional) {_abs_delta}")

    @property
    def notional(self) -> Decimal:
        return self.buy_notional + self.sell_notional

    @property
    def taker_buy_ratio(self) -> Decimal:
        denom = self.buy_notional + self.sell_notional
        if denom == 0:
            return Decimal("0")
        return self.buy_notional / denom

    @property
    def return_pct(self) -> Decimal:
        if self.open == 0:
            return Decimal("0")
        return self.close / self.open - Decimal("1")

    @property
    def range_pct(self) -> Decimal:
        if self.open == 0:
            return Decimal("0")
        return (self.high - self.low) / self.open


@dataclass(frozen=True)
class TradeFootprintFeature:
    """Exchange-agnostic 1m footprint/order-flow feature derived from trades.

    This is a lightweight companion to FixedTimeTradeBar containing
    order-flow-specific metrics.
    """

    exchange: str
    symbol: str
    timeframe: str = "1m"
    open_time_ms: int = 0
    close_time_ms: int = 0
    available_time_ms: int = 0
    delta_notional: Decimal = Decimal("0")
    abs_delta_notional: Decimal = Decimal("0")
    taker_buy_ratio: Decimal = Decimal("0")
    close_pos: Decimal = Decimal("0")
    range_pct: Decimal = Decimal("0")
    return_pct: Decimal = Decimal("0")
    fp_max_bucket_abs_delta_pressure: Decimal = Decimal("0")
    context_available: bool = True
    quality: str = TradeFeatureQuality.COMPLETE.value
    source: str = "trade_derived"

    def __post_init__(self) -> None:
        if not self.exchange:
            raise ValueError("exchange is required")
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.timeframe:
            raise ValueError("timeframe is required")
        if self.open_time_ms < 0 or self.close_time_ms < 0 or self.available_time_ms < 0:
            raise ValueError("time fields must be non-negative")
        if self.available_time_ms < self.close_time_ms:
            raise ValueError("available_time_ms must be >= close_time_ms")
        if self.close_time_ms < self.open_time_ms:
            raise ValueError("close_time_ms must be >= open_time_ms")
        # Range checks
        if self.taker_buy_ratio < 0 or self.taker_buy_ratio > 1:
            raise ValueError("taker_buy_ratio must be within [0, 1]")
        if self.close_pos < 0 or self.close_pos > 1:
            raise ValueError("close_pos must be within [0, 1]")
        if self.range_pct < 0:
            raise ValueError("range_pct must be non-negative")
        # Delta consistency
        if self.abs_delta_notional != abs(self.delta_notional):
            raise ValueError("abs_delta_notional must equal abs(delta_notional)")
        # Quality vs context_available invariant
        if self.context_available is False and self.quality == TradeFeatureQuality.COMPLETE.value:
            raise ValueError("quality cannot be COMPLETE when context_available=False")


@dataclass(frozen=True)
class TradeFeatureBackfillTarget:
    """Gap-driven backfill target computed by the coverage scanner."""

    start_ms: int
    end_ms: int
    reason: str = "missing"
    archive_dates: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.start_ms < 0 or self.end_ms < 0:
            raise ValueError("target timestamps must be non-negative")
        if self.end_ms < self.start_ms:
            raise ValueError("end_ms must be >= start_ms")


@dataclass(frozen=True)
class TradeDerivedFeatureCoverage:
    """Coverage scan result for trade-derived 1m features."""

    symbol: str
    exchange: str
    required_minutes: int = 0
    complete_minutes: int = 0
    missing_minutes: int = 0
    degraded_minutes: int = 0
    latest_complete_close_time_ms: int | None = None
    first_missing_range: tuple[int, int] | None = None
    available: bool = False
    reason: str = ""
    extra: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.required_minutes < 0:
            raise ValueError("required_minutes must be non-negative")
        if self.complete_minutes < 0:
            raise ValueError("complete_minutes must be non-negative")
        if self.missing_minutes < 0:
            raise ValueError("missing_minutes must be non-negative")
        if self.degraded_minutes < 0:
            raise ValueError("degraded_minutes must be non-negative")
