from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence

from src.platform.exchanges.names import ExchangeName


class MarketEventType(str, Enum):
    KLINE = "kline"
    TICKER = "ticker"
    TRADE = "trade"
    ORDER_BOOK = "order_book"
    ORDER_BOOK_L2 = "order_book_l2"
    FULL_ORDER_BOOK = "full_order_book"
    OPEN_INTEREST = "open_interest"


class MarketDataSource(str, Enum):
    REST = "rest"
    WEBSOCKET = "websocket"


class TradeSide(str, Enum):
    BUY = "buy"
    SELL = "sell"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MarketKline:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal | None = None
    is_closed: bool = True
    source: MarketDataSource = MarketDataSource.REST
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> MarketEventType:
        return MarketEventType.KLINE


@dataclass(frozen=True)
class MarketTicker:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    price: Decimal
    time_ms: int | None = None
    source: MarketDataSource = MarketDataSource.REST
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> MarketEventType:
        return MarketEventType.TICKER


@dataclass(frozen=True)
class MarketTrade:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    price: Decimal
    quantity: Decimal
    side: TradeSide = TradeSide.UNKNOWN
    trade_id: str | None = None
    event_time_ms: int | None = None
    trade_time_ms: int | None = None
    source: MarketDataSource = MarketDataSource.WEBSOCKET
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> MarketEventType:
        return MarketEventType.TRADE


@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    price: Decimal
    quantity: Decimal
    order_count: int | None = None


@dataclass(frozen=True)
class MarketOrderBook:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    bids: Sequence[OrderBookLevel]
    asks: Sequence[OrderBookLevel]
    event_time_ms: int | None = None
    source: MarketDataSource = MarketDataSource.WEBSOCKET
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> MarketEventType:
        return MarketEventType.ORDER_BOOK


@dataclass(frozen=True, slots=True)
class MarketOrderBookL2:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    event_time_ms: int
    sequence_id: int
    previous_sequence_id: int
    depth: int = 400
    source: MarketDataSource = MarketDataSource.WEBSOCKET
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> MarketEventType:
        return MarketEventType.ORDER_BOOK_L2


@dataclass(frozen=True, slots=True)
class MarketFullOrderBook:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    event_time_ms: int
    requested_depth: int
    source: MarketDataSource = MarketDataSource.REST
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> MarketEventType:
        return MarketEventType.FULL_ORDER_BOOK


@dataclass(frozen=True, slots=True)
class MarketOpenInterest:
    exchange: ExchangeName
    symbol: str
    raw_symbol: str
    instrument_type: str
    open_interest_contracts: Decimal
    open_interest_base: Decimal | None
    open_interest_usd: Decimal | None
    event_time_ms: int
    source: MarketDataSource = MarketDataSource.WEBSOCKET
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> MarketEventType:
        return MarketEventType.OPEN_INTEREST


MarketEvent = (
    MarketKline
    | MarketTicker
    | MarketTrade
    | MarketOrderBook
    | MarketOrderBookL2
    | MarketFullOrderBook
    | MarketOpenInterest
)
