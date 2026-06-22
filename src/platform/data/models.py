from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Sequence

from src.platform.exchanges.models import ExchangeName, Kline, Ticker, Trade


class MarketEventType(str, Enum):
    KLINE = "kline"
    TICKER = "ticker"
    TRADE = "trade"
    ORDER_BOOK = "order_book"


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


@dataclass(frozen=True)
class OrderBookLevel:
    price: Decimal
    quantity: Decimal


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


MarketEvent = MarketKline | MarketTicker | MarketTrade | MarketOrderBook


def market_kline_from_exchange(kline: Kline) -> MarketKline:
    return MarketKline(
        exchange=kline.exchange,
        symbol=kline.symbol,
        raw_symbol=kline.raw_symbol,
        interval=kline.interval,
        open_time_ms=kline.open_time_ms,
        close_time_ms=kline.close_time_ms,
        open=kline.open,
        high=kline.high,
        low=kline.low,
        close=kline.close,
        volume=kline.volume,
        quote_volume=kline.quote_volume,
        is_closed=kline.is_closed,
        source=MarketDataSource.REST,
        raw=kline.raw,
    )


def market_ticker_from_exchange(ticker: Ticker) -> MarketTicker:
    return MarketTicker(
        exchange=ticker.exchange,
        symbol=ticker.symbol,
        raw_symbol=ticker.raw_symbol,
        price=ticker.price,
        time_ms=ticker.time_ms,
        source=MarketDataSource.REST,
        raw=ticker.raw,
    )


def market_trade_from_exchange(trade: Trade) -> MarketTrade:
    side = TradeSide.UNKNOWN
    if trade.side is not None:
        if trade.side.value == "buy":
            side = TradeSide.BUY
        elif trade.side.value == "sell":
            side = TradeSide.SELL
    return MarketTrade(
        exchange=trade.exchange,
        symbol=trade.symbol,
        raw_symbol=trade.raw_symbol,
        price=trade.price,
        quantity=trade.quantity,
        side=side,
        trade_id=trade.trade_id,
        event_time_ms=trade.event_time_ms,
        trade_time_ms=trade.trade_time_ms,
        source=MarketDataSource.REST,
        raw=trade.raw,
    )
