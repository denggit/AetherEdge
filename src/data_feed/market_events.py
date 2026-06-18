from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from src.exchanges.models import ExchangeName


class MarketTradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    UNKNOWN = "UNKNOWN"


class MarketEventType(str, Enum):
    TRADE = "TRADE"
    CANDLE = "CANDLE"


def require_positive_decimal(value: Decimal, *, field_name: str) -> Decimal:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def require_non_negative_decimal(value: Decimal, *, field_name: str) -> Decimal:
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


@dataclass(frozen=True)
class MarketTradeEvent:
    exchange: ExchangeName
    canonical_symbol: str
    raw_symbol: str
    price: Decimal
    quantity: Decimal
    taker_side: MarketTradeSide
    event_time_ms: int
    trade_time_ms: int | None = None

    # True for Binance aggTrade-like aggregated trade buckets.
    # False for exchange streams that represent one raw trade event.
    is_aggregated: bool = False
    aggregation_window_ms: int | None = None

    # Optional trade id range.  Binance aggTrade exposes first/last trade ids.
    trade_id: str | None = None
    first_trade_id: str | None = None
    last_trade_id: str | None = None

    raw: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_positive_decimal(self.price, field_name="price")
        require_positive_decimal(self.quantity, field_name="quantity")
        if self.is_aggregated and self.aggregation_window_ms is None:
            raise ValueError(
                "aggregation_window_ms is required for aggregated trade events"
            )
        if not self.is_aggregated and self.aggregation_window_ms is not None:
            raise ValueError(
                "aggregation_window_ms must be None for non-aggregated trade events"
            )


@dataclass(frozen=True)
class MarketCandleEvent:
    exchange: ExchangeName
    canonical_symbol: str
    raw_symbol: str
    timeframe: str

    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal

    is_closed: bool

    raw: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_positive_decimal(self.open_price, field_name="open_price")
        require_positive_decimal(self.high_price, field_name="high_price")
        require_positive_decimal(self.low_price, field_name="low_price")
        require_positive_decimal(self.close_price, field_name="close_price")
        require_non_negative_decimal(self.volume, field_name="volume")
        if self.close_time_ms < self.open_time_ms:
            raise ValueError(
                "close_time_ms must be greater than or equal to open_time_ms"
            )
