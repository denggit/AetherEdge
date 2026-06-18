from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping

from src.platform.data.models import MarketOrderBook, MarketTrade, OrderBookLevel, TradeSide
from src.platform.data.websocket.ports import WebSocketConnector
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.symbols import to_exchange_symbol

BINANCE_USDM_WS_URL = "wss://fstream.binance.com/ws"
BINANCE_USDM_TESTNET_WS_URL = "wss://stream.binancefuture.com/ws"


class BinanceTradeWebSocketFeed:
    def __init__(
        self,
        *,
        symbol: str,
        connector: WebSocketConnector,
        sandbox: bool = False,
        reconnect: bool = True,
        reconnect_delay_seconds: float = 1.0,
        max_reconnects: int | None = None,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.BINANCE, symbol)
        self._connector = connector
        self._reconnect = reconnect
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnects = max_reconnects
        base_url = BINANCE_USDM_TESTNET_WS_URL if sandbox else BINANCE_USDM_WS_URL
        self._url = f"{base_url}/{self._raw_symbol.lower()}@aggTrade"

    async def stream_trades(self) -> AsyncIterator[MarketTrade]:
        reconnects = 0
        while True:
            connection = await self._connector.connect(self._url)
            try:
                async for message in connection:
                    trade = self._map_message(message)
                    if trade is not None:
                        yield trade
            finally:
                await connection.close()
            if not self._reconnect or (self._max_reconnects is not None and reconnects >= self._max_reconnects):
                break
            reconnects += 1
            await asyncio.sleep(self._reconnect_delay_seconds)

    def _map_message(self, message: str | bytes) -> MarketTrade | None:
        payload = _decode_json(message)
        if payload.get("e") != "aggTrade":
            return None
        return _map_binance_trade(payload, symbol=self._symbol, raw_symbol=self._raw_symbol)


class BinanceOrderBookWebSocketFeed:
    def __init__(
        self,
        *,
        symbol: str,
        connector: WebSocketConnector,
        sandbox: bool = False,
        depth: int = 5,
        update_ms: int = 100,
        reconnect: bool = True,
        reconnect_delay_seconds: float = 1.0,
        max_reconnects: int | None = None,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.BINANCE, symbol)
        self._connector = connector
        self._reconnect = reconnect
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnects = max_reconnects
        base_url = BINANCE_USDM_TESTNET_WS_URL if sandbox else BINANCE_USDM_WS_URL
        self._url = f"{base_url}/{self._raw_symbol.lower()}@depth{depth}@{update_ms}ms"

    async def stream_order_book(self) -> AsyncIterator[MarketOrderBook]:
        reconnects = 0
        while True:
            connection = await self._connector.connect(self._url)
            try:
                async for message in connection:
                    order_book = self._map_message(message)
                    if order_book is not None:
                        yield order_book
            finally:
                await connection.close()
            if not self._reconnect or (self._max_reconnects is not None and reconnects >= self._max_reconnects):
                break
            reconnects += 1
            await asyncio.sleep(self._reconnect_delay_seconds)

    def _map_message(self, message: str | bytes) -> MarketOrderBook | None:
        payload = _decode_json(message)
        if "b" not in payload or "a" not in payload:
            return None
        return _map_binance_order_book(payload, symbol=self._symbol, raw_symbol=self._raw_symbol)


def _map_binance_trade(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        raw_symbol=str(row.get("s") or raw_symbol),
        price=Decimal(str(row["p"])),
        quantity=Decimal(str(row["q"])),
        side=_map_binance_taker_side(row.get("m")),
        trade_id=_optional_str(row.get("a")),
        event_time_ms=_optional_int(row.get("E")),
        trade_time_ms=_optional_int(row.get("T")),
        raw=dict(row),
    )


def _map_binance_order_book(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> MarketOrderBook:
    return MarketOrderBook(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        raw_symbol=str(row.get("s") or raw_symbol),
        bids=_map_levels(row.get("b") or []),
        asks=_map_levels(row.get("a") or []),
        event_time_ms=_optional_int(row.get("E")),
        raw=dict(row),
    )


def _map_levels(rows: Any) -> list[OrderBookLevel]:
    levels: list[OrderBookLevel] = []
    if not isinstance(rows, list):
        return levels
    for row in rows:
        if isinstance(row, list | tuple) and len(row) >= 2:
            levels.append(OrderBookLevel(price=Decimal(str(row[0])), quantity=Decimal(str(row[1]))))
    return levels


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
