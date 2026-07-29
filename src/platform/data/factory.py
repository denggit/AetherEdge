from __future__ import annotations

from pathlib import Path

from src.platform.data.ports import MarketDataFeed
from src.platform.data.rest_feed import RestMarketDataFeed
from src.platform.data.polling import (
    FullOrderBookPollingStream,
    FullOrderBookStream,
)
from src.platform.data.rest import (
    OkxFullOrderBookRestClient,
)
from src.platform.data.storage import MarketDataStore, SqliteMarketDataStore
from src.platform.data.websocket import (
    OkxOpenInterestWebSocketFeed,
    OkxOrderBookL2WebSocketFeed,
    OkxOrderBookWebSocketFeed,
    OkxTradeWebSocketFeed,
    OpenInterestStream,
    OrderBookL2Stream,
    OrderBookStream,
    TradeStream,
    WebSocketConnector,
    WebsocketsConnector,
)
from src.platform.exchanges.factory import create_exchange_client, normalize_exchange_name
from src.platform.exchanges.http import RequestsHttpClient
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.exchanges.okx.public_rest import OkxPublicRestRequester
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
    _require_okx_market_data(exchange_name)
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
        exchange=exchange_name,
        symbol=symbol,
        market_profile=profile,
        kline_fetcher=client,
        ticker_fetcher=client,
        historical_trade_fetcher=client,
        anchored_trade_fetcher=client,
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
    raise ValueError(
        f"OKX is the only supported market-data exchange; got {exchange.value}"
    )


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
    raise ValueError(
        f"OKX is the only supported market-data exchange; got {exchange.value}"
    )


def create_order_book_l2_stream(
    exchange: ExchangeName | str,
    *,
    symbol: str,
    config: ExchangeConfig | None = None,
    connector: WebSocketConnector | None = None,
    reconnect: bool = True,
    reconnect_delay_seconds: float = 1.0,
    max_reconnects: int | None = None,
) -> OrderBookL2Stream:
    exchange_name = normalize_exchange_name(exchange)
    _require_okx_market_data(exchange_name)
    cfg = config or ExchangeConfig()
    return OkxOrderBookL2WebSocketFeed(
        symbol=symbol,
        connector=connector or WebsocketsConnector(),
        sandbox=cfg.sandbox,
        reconnect=reconnect,
        reconnect_delay_seconds=reconnect_delay_seconds,
        max_reconnects=max_reconnects,
    )


def create_full_order_book_stream(
    exchange: ExchangeName | str,
    *,
    symbol: str,
    config: ExchangeConfig | None = None,
    http_client: HttpClient | None = None,
    depth: int = 5000,
    poll_interval_seconds: float = 3.0,
) -> FullOrderBookStream:
    exchange_name = normalize_exchange_name(exchange)
    _require_okx_market_data(exchange_name)
    cfg = config or ExchangeConfig()
    requester = OkxPublicRestRequester(
        config=cfg,
        http_client=http_client or RequestsHttpClient(),
    )
    return FullOrderBookPollingStream(
        fetcher=OkxFullOrderBookRestClient(requester=requester),
        symbol=symbol,
        depth=depth,
        poll_interval_seconds=poll_interval_seconds,
    )


def create_open_interest_stream(
    exchange: ExchangeName | str,
    *,
    symbol: str,
    config: ExchangeConfig | None = None,
    connector: WebSocketConnector | None = None,
    reconnect: bool = True,
    reconnect_delay_seconds: float = 1.0,
    max_reconnects: int | None = None,
) -> OpenInterestStream:
    exchange_name = normalize_exchange_name(exchange)
    _require_okx_market_data(exchange_name)
    cfg = config or ExchangeConfig()
    return OkxOpenInterestWebSocketFeed(
        symbol=symbol,
        connector=connector or WebsocketsConnector(),
        sandbox=cfg.sandbox,
        reconnect=reconnect,
        reconnect_delay_seconds=reconnect_delay_seconds,
        max_reconnects=max_reconnects,
    )


def _require_okx_market_data(exchange: ExchangeName) -> None:
    if exchange != ExchangeName.OKX:
        raise ValueError(
            "OKX is the only supported market-data exchange; "
            f"got {exchange.value}"
        )


__all__ = [
    "create_full_order_book_stream",
    "create_market_data_feed",
    "create_open_interest_stream",
    "create_order_book_l2_stream",
    "create_order_book_stream",
    "create_trade_stream",
]
