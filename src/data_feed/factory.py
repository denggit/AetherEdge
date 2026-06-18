from __future__ import annotations

from src.data_feed.ports import MarketDataFeed
from src.data_feed.rest_feed import RestMarketDataFeed
from src.data_feed.websocket import (
    BinanceTradeWebSocketFeed,
    OkxTradeWebSocketFeed,
    TradeStream,
    WebSocketConnector,
    WebsocketsConnector,
)
from src.exchanges.factory import create_exchange_client, normalize_exchange_name
from src.exchanges.models import ExchangeConfig, ExchangeName
from src.exchanges.ports import ExchangeClient, HttpClient


def create_market_data_feed(
    exchange: ExchangeName | str,
    *,
    symbol: str = "ETH-USDT-PERP",
    config: ExchangeConfig | None = None,
    exchange_client: ExchangeClient | None = None,
    http_client: HttpClient | None = None,
    websocket_connector: WebSocketConnector | None = None,
    enable_trade_stream: bool = True,
) -> MarketDataFeed:
    """Create a market data feed for strategy/runtime code.

    The returned feed supports REST kline/ticker. When enable_trade_stream=True,
    it also supports WebSocket trade/tick streaming.
    """

    exchange_name = normalize_exchange_name(exchange)
    cfg = config or ExchangeConfig()
    client = exchange_client or create_exchange_client(exchange_name, cfg, http_client=http_client)
    trade_stream = _create_trade_stream(
        exchange_name,
        symbol=symbol,
        config=cfg,
        websocket_connector=websocket_connector,
    ) if enable_trade_stream else None
    return RestMarketDataFeed(exchange_client=client, symbol=symbol, trade_stream=trade_stream)


def _create_trade_stream(
    exchange: ExchangeName,
    *,
    symbol: str,
    config: ExchangeConfig,
    websocket_connector: WebSocketConnector | None,
) -> TradeStream:
    connector = websocket_connector or WebsocketsConnector()
    if exchange == ExchangeName.OKX:
        return OkxTradeWebSocketFeed(symbol=symbol, connector=connector, sandbox=config.sandbox)
    if exchange == ExchangeName.BINANCE:
        return BinanceTradeWebSocketFeed(symbol=symbol, connector=connector, sandbox=config.sandbox)
    raise ValueError(f"Unsupported exchange for market data feed: {exchange.value}")
