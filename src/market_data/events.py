from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from src.platform.exchanges.models import ExchangeName


class MarketFeatureEventType(str, Enum):
    CLOSED_KLINE = "closed_kline"
    RANGE_BAR_CLOSED = "range_bar_closed"
    RANGE_AGGREGATE = "range_aggregate"


@dataclass(frozen=True)
class MarketFeatureEvent:
    """Strategy-facing normalized market feature event."""

    event_type: MarketFeatureEventType | str
    symbol: str
    exchange: ExchangeName
    timeframe: str | None
    event_time_ms: int
    data: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if self.event_time_ms < 0:
            raise ValueError("event_time_ms must be non-negative")

    @property
    def type_value(self) -> str:
        return self.event_type.value if isinstance(self.event_type, MarketFeatureEventType) else str(self.event_type)
