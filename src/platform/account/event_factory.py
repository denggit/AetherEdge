from __future__ import annotations

from src.platform.account.stream import AccountEventStream
from src.platform.account.websocket import BinanceAccountEventStream, OkxAccountEventStream
from src.platform.data.websocket.connector import WebsocketsConnector
from src.platform.data.websocket.ports import WebSocketConnector
from src.platform.exchanges import ExchangeClient, ExchangeConfig, ExchangeName, create_exchange_client
from src.platform.markets import get_market_profile


def create_account_event_stream(
    exchange: ExchangeName | str,
    *,
    symbol: str | None = None,
    config: ExchangeConfig | None = None,
    exchange_client: ExchangeClient | None = None,
    connector: WebSocketConnector | None = None,
    reconnect: bool = True,
    reconnect_delay_seconds: float = 1.0,
    max_reconnects: int | None = None,
) -> AccountEventStream:
    exchange_name = exchange if isinstance(exchange, ExchangeName) else ExchangeName(str(exchange).strip().lower())
    profile = get_market_profile(symbol)
    resolved_symbol = profile.symbol
    resolved_config = config or ExchangeConfig.from_env(exchange_name)
    resolved_connector = connector or WebsocketsConnector()
    if exchange_name == ExchangeName.OKX:
        return OkxAccountEventStream(
            symbol=resolved_symbol,
            config=resolved_config,
            connector=resolved_connector,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
    if exchange_name == ExchangeName.BINANCE:
        client = exchange_client or create_exchange_client(exchange_name, resolved_config)
        return BinanceAccountEventStream(
            symbol=resolved_symbol,
            config=resolved_config,
            exchange_client=client,
            connector=resolved_connector,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )
    raise ValueError(f"Unsupported exchange: {exchange}")
