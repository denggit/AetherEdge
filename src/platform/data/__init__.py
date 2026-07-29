from __future__ import annotations

from importlib import import_module

from src.platform.data.models import (
    MarketDataSource,
    MarketEvent,
    MarketEventType,
    MarketFullOrderBook,
    MarketKline,
    MarketOpenInterest,
    MarketOrderBook,
    MarketOrderBookL2,
    MarketTicker,
    MarketTrade,
    OrderBookLevel,
    TradeSide,
)

_LAZY_EXPORTS = {
    "MarketDataFeed": ("src.platform.data.ports", "MarketDataFeed"),
    "RestMarketDataFeed": ("src.platform.data.rest_feed", "RestMarketDataFeed"),
    "MarketDataStore": ("src.platform.data.storage", "MarketDataStore"),
    "SqliteMarketDataStore": (
        "src.platform.data.storage",
        "SqliteMarketDataStore",
    ),
    "create_full_order_book_stream": (
        "src.platform.data.factory",
        "create_full_order_book_stream",
    ),
    "create_market_data_feed": (
        "src.platform.data.factory",
        "create_market_data_feed",
    ),
    "create_open_interest_stream": (
        "src.platform.data.factory",
        "create_open_interest_stream",
    ),
    "create_order_book_l2_stream": (
        "src.platform.data.factory",
        "create_order_book_l2_stream",
    ),
}


def __getattr__(name: str):
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(name)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


__all__ = [
    "MarketDataFeed",
    "MarketDataSource",
    "MarketDataStore",
    "MarketEvent",
    "MarketEventType",
    "MarketFullOrderBook",
    "MarketKline",
    "MarketOpenInterest",
    "MarketOrderBook",
    "MarketOrderBookL2",
    "MarketTicker",
    "MarketTrade",
    "OrderBookLevel",
    "RestMarketDataFeed",
    "SqliteMarketDataStore",
    "TradeSide",
    "create_full_order_book_stream",
    "create_market_data_feed",
    "create_open_interest_stream",
    "create_order_book_l2_stream",
]
