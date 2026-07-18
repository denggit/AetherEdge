from __future__ import annotations

from pathlib import Path

from src.platform.data.ports import MarketDataFeed
from src.platform.data.rest_feed import RestMarketDataFeed
from src.platform.data.storage import MarketDataStore, SqliteMarketDataStore
from src.platform.data.websocket import (
    BinanceOrderBookWebSocketFeed,
    BinanceTradeWebSocketFeed,
    OkxOrderBookWebSocketFeed,
    OkxTradeWebSocketFeed,
    OrderBookStream,
    TradeStream,
    WebSocketConnector,
    WebsocketsConnector,
)
from src.platform.exchanges.factory import create_exchange_client, normalize_exchange_name
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.markets import MarketProfile, get_market_profile
from src.platform.exchanges.ports import ExchangeMarketDataClient, HttpClient


def create_market_data_feed(
    exchange: ExchangeName | str,
    *,
    symbol: str | None = None,
    market_profile: MarketProfile | None = None,
    config: ExchangeConfig | None = None,
    exchange_client: ExchangeMarketDataClient | None = None,
    http_client: HttpClient | None = None,
    websocket_connector: WebSocketConnector | None = None,
    enable_trade_stream: bool = True,
    enable_order_book_stream: bool = True,
    store: MarketDataStore | None = None,
    sqlite_path: str | Path | None = None,
    reconnect_streams: bool = True,
    reconnect_delay_seconds: float = 1.0,
    max_reconnects: int | None = None,
) -> MarketDataFeed:
    """Create the single data interface for strategy/runtime code.

    - REST Kline/ticker goes through ExchangeMarketDataClient.
    - WebSocket trade/orderbook goes through small stream adapters.
    - SQLite cache is optional and hidden behind MarketDataStore.
    """

    exchange_name = normalize_exchange_name(exchange)
    profile = market_profile or get_market_profile(symbol)
    symbol = profile.symbol
    cfg = config or ExchangeConfig()
    client = exchange_client or create_exchange_client(exchange_name, cfg, http_client=http_client)
    connector = websocket_connector or WebsocketsConnector()
    data_store = store or (SqliteMarketDataStore(sqlite_path) if sqlite_path is not None else None)
    trade_stream = (
        create_trade_stream(
            exchange_name,
            symbol=symbol,
            config=cfg,
            connector=connector,
            reconnect=reconnect_streams,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
        if enable_trade_stream
        else None
    )
    order_book_stream = (
        create_order_book_stream(
            exchange_name,
            symbol=symbol,
            config=cfg,
            connector=connector,
            reconnect=reconnect_streams,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
        if enable_order_book_stream
        else None
    )
    return RestMarketDataFeed(
        exchange_client=client,
        symbol=symbol,
        market_profile=profile,
        trade_stream=trade_stream,
        order_book_stream=order_book_stream,
        store=data_store,
    )


def create_trade_stream(
    exchange: ExchangeName,
    *,
    symbol: str,
    config: ExchangeConfig,
    connector: WebSocketConnector,
    reconnect: bool,
    reconnect_delay_seconds: float,
    max_reconnects: int | None,
) -> TradeStream:
    if exchange == ExchangeName.OKX:
        return OkxTradeWebSocketFeed(
            symbol=symbol,
            connector=connector,
            sandbox=config.sandbox,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
    if exchange == ExchangeName.BINANCE:
        return BinanceTradeWebSocketFeed(
            symbol=symbol,
            connector=connector,
            sandbox=config.sandbox,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
    raise ValueError(f"Unsupported exchange for trade stream: {exchange.value}")


def create_order_book_stream(
    exchange: ExchangeName,
    *,
    symbol: str,
    config: ExchangeConfig,
    connector: WebSocketConnector,
    reconnect: bool,
    reconnect_delay_seconds: float,
    max_reconnects: int | None,
) -> OrderBookStream:
    if exchange == ExchangeName.OKX:
        return OkxOrderBookWebSocketFeed(
            symbol=symbol,
            connector=connector,
            sandbox=config.sandbox,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
    if exchange == ExchangeName.BINANCE:
        return BinanceOrderBookWebSocketFeed(
            symbol=symbol,
            connector=connector,
            sandbox=config.sandbox,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
    raise ValueError(f"Unsupported exchange for order book stream: {exchange.value}")


__all__ = [
    "create_market_data_feed",
    "create_order_book_stream",
    "create_trade_stream",
]
