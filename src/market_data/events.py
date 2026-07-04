from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from src.platform.exchanges.models import ExchangeName


class MarketFeatureEventType(str, Enum):
    CLOSED_KLINE = "closed_kline"
    RANGE_BAR_CLOSED = "range_bar_closed"
    RANGE_AGGREGATE = "range_aggregate"
    FIXED_TIME_TRADE_BAR = "fixed_time_trade_bar"
    TRADE_FOOTPRINT_FEATURE = "trade_footprint_feature"


@dataclass(frozen=True)
class MarketFeatureEvent:
    """Strategy-facing normalized market feature event."""

    event_type: MarketFeatureEventType | str
    symbol: str
    exchange: ExchangeName
    timeframe: str | None
    event_time_ms: int
    data: Mapping[str, Any] = field(default_factory=dict)
    available_time_ms: int | None = None

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if self.event_time_ms < 0:
            raise ValueError("event_time_ms must be non-negative")
        if self.available_time_ms is not None:
            if self.available_time_ms < 0:
                raise ValueError("available_time_ms must be non-negative")
            if self.available_time_ms < self.event_time_ms:
                raise ValueError(
                    "available_time_ms must be greater than or equal to event_time_ms"
                )

    @property
    def type_value(self) -> str:
        return self.event_type.value if isinstance(self.event_type, MarketFeatureEventType) else str(self.event_type)

    @property
    def effective_available_time_ms(self) -> int:
        return (
            self.event_time_ms
            if self.available_time_ms is None
            else self.available_time_ms
        )
