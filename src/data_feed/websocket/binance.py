from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping

from src.data_feed.models import MarketTrade, TradeSide
from src.data_feed.websocket.ports import WebSocketConnector
from src.exchanges.models import ExchangeName
from src.exchanges.symbols import to_exchange_symbol

BINANCE_USDM_WS_URL = "wss://fstream.binance.com/ws"
BINANCE_USDM_TESTNET_WS_URL = "wss://stream.binancefuture.com/ws"


class BinanceTradeWebSocketFeed:
    def __init__(
        self,
        *,
        symbol: str,
        connector: WebSocketConnector,
        sandbox: bool = False,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.BINANCE, symbol)
        self._connector = connector
        base_url = BINANCE_USDM_TESTNET_WS_URL if sandbox else BINANCE_USDM_WS_URL
        self._url = f"{base_url}/{self._raw_symbol.lower()}@trade"

    async def stream_trades(self) -> AsyncIterator[MarketTrade]:
        connection = await self._connector.connect(self._url)
        try:
            async for message in connection:
                trade = self._map_message(message)
                if trade is not None:
                    yield trade
        finally:
            await connection.close()

    def _map_message(self, message: str | bytes) -> MarketTrade | None:
        payload = _decode_json(message)
        if payload.get("e") != "trade":
            return None
        return _map_binance_trade(payload, symbol=self._symbol, raw_symbol=self._raw_symbol)


def _map_binance_trade(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        raw_symbol=str(row.get("s") or raw_symbol),
        price=Decimal(str(row["p"])),
        quantity=Decimal(str(row["q"])),
        side=_map_binance_taker_side(row.get("m")),
        trade_id=_optional_str(row.get("t")),
        event_time_ms=_optional_int(row.get("E")),
        trade_time_ms=_optional_int(row.get("T")),
        raw=dict(row),
    )


def _map_binance_taker_side(buyer_is_maker: Any) -> TradeSide:
    if buyer_is_maker is True:
        return TradeSide.SELL
    if buyer_is_maker is False:
        return TradeSide.BUY
    return TradeSide.UNKNOWN


def _decode_json(message: str | bytes) -> Mapping[str, Any]:
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    payload = json.loads(message)
    return payload if isinstance(payload, Mapping) else {}


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
