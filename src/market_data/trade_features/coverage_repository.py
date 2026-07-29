from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from src.market_data.models import RangeFootprintFeature


@dataclass(frozen=True)
class FixedTimeQualityRows:
    tradebars: tuple[tuple[int, str], ...]
    footprints: tuple[tuple[int, str, bool], ...]


@dataclass(frozen=True)
class RangeCoverageRows:
    features: tuple[tuple[int, str, bool], ...]
    latest_complete_time_ms: int | None
    context_seed_time_ms: int | None
    complete_intervals: tuple[tuple[int, int], ...]
    range_pct: str
    price_step: str


class TradeFeatureCoverageRepository(Protocol):
    """Raw/query-only persistence port consumed by coverage policy."""

    def load_fixed_time_quality_rows(
        self,
        *,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
    ) -> FixedTimeQualityRows:
        ...

    def load_range_coverage_rows(
        self,
        *,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
        range_pct: Decimal | str | float,
        price_step: Decimal | str | float,
    ) -> RangeCoverageRows:
        ...

    def latest_complete_close_time_ms(
        self, *, symbol: str, exchange: str
    ) -> int | None:
        ...

    def latest_any_tradebar_close_time_ms(
        self, *, symbol: str, exchange: str
    ) -> int | None:
        ...

    def latest_any_footprint_close_time_ms(
        self, *, symbol: str, exchange: str
    ) -> int | None:
        ...

    def latest_any_range_footprint_available_time_ms(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: Decimal | str | float,
        price_step: Decimal | str | float,
    ) -> int | None:
        ...

    def tradebar_without_footprint_bounds(
        self, *, symbol: str, exchange: str, end_ms: int
    ) -> tuple[int, int] | None:
        ...

    def degraded_footprint_bounds(
        self, *, symbol: str, exchange: str, end_ms: int
    ) -> tuple[int, int] | None:
        ...

    def load_latest_range_footprint_context(
        self,
        *,
        symbol: str,
        exchange: str,
        cutoff_ms: int,
        range_pct: Decimal | str | float,
        price_step: Decimal | str | float,
    ) -> RangeFootprintFeature | None:
        ...


__all__ = [
    "FixedTimeQualityRows",
    "RangeCoverageRows",
    "TradeFeatureCoverageRepository",
]
