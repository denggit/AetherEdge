from __future__ import annotations

from typing import Protocol, Sequence

from src.market_data.models import DataGap, MarketDataSet, RangeBar, RangeBarAggregate, TimeRange, WarmupRequest, WarmupResult
from src.platform.data.models import MarketKline, MarketTrade


class KlineRepository(Protocol):
    """Persistence port for normalized klines in the internal data pipeline."""

    def save(self, rows: Sequence[MarketKline]) -> int:
        ...

    def load(self, *, symbol: str, interval: str, time_range: TimeRange) -> list[MarketKline]:
        ...

    def latest_time_ms(self, *, symbol: str, interval: str) -> int | None:
        ...


class TradeRepository(Protocol):
    """Persistence port for normalized trades in the internal data pipeline."""

    def save(self, rows: Sequence[MarketTrade]) -> int:
        ...

    def load(self, *, symbol: str, time_range: TimeRange) -> list[MarketTrade]:
        ...

    def latest_time_ms(self, *, symbol: str) -> int | None:
        ...


class TradeCoverageRepository(Protocol):
    """Optional coverage tracking for historical trade warmup."""

    def mark_coverage(self, *, symbol: str, time_range: TimeRange, source: str = "historical") -> None:
        ...

    def coverage_ranges(self, *, symbol: str, time_range: TimeRange, source: str = "historical") -> list[TimeRange]:
        ...


class HistoricalTradeProvider(Protocol):
    """Port for one ascending page of normalized historical trades.

    Implementations return the oldest available page inside the inclusive time
    range.  Callers can advance the start time to page forward without knowing
    any exchange-specific cursor format.
    """

    async def fetch_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 100,
    ) -> Sequence[MarketTrade]:
        ...


class HistoricalTradeFeed(HistoricalTradeProvider, Protocol):
    """Legacy warmup port with explicit ordering control."""

    async def fetch_trades(
        self,
        *,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
        oldest_first: bool = True,
    ) -> Sequence[MarketTrade]:
        ...


class RangeBarRepository(Protocol):
    """Persistence port for reusable derived range bars."""

    def save(self, rows: Sequence[RangeBar]) -> int:
        ...

    def load(self, *, symbol: str, range_pct: str, time_range: TimeRange) -> list[RangeBar]:
        ...

    def latest_end_time_ms(self, *, symbol: str, range_pct: str) -> int | None:
        ...


class DataGapDetector(Protocol):
    def find_gaps(self, *, symbol: str, dataset: MarketDataSet, time_range: TimeRange, interval: str | None = None) -> list[DataGap]:
        ...


class WarmupServicePort(Protocol):
    async def warmup(self, request: WarmupRequest) -> WarmupResult:
        ...


class RangeBarBuilderPort(Protocol):
    def on_trade(self, trade: MarketTrade) -> tuple[RangeBar, ...]:
        ...

    def snapshot_open_bar(self) -> RangeBar | None:
        ...


class RangeBarAggregatorPort(Protocol):
    def aggregate(self, rows: Sequence[RangeBar], *, bucket_ms: int) -> list[RangeBarAggregate]:
        ...
