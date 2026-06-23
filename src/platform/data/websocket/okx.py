from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping

from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK

from src.platform.data.models import MarketOrderBook, MarketTrade, OrderBookLevel, TradeSide
from src.platform.data.websocket.ports import WebSocketConnector
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.symbols import to_exchange_symbol
from src.utils.log import get_logger

OKX_PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_DEMO_PUBLIC_WS_URL = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"

logger = get_logger(__name__)

TRANSIENT_WS_EXCEPTIONS = (
    ConnectionClosed,
    ConnectionClosedError,
    ConnectionClosedOK,
    asyncio.TimeoutError,
    OSError,
)


class OkxTradeWebSocketFeed:
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
        self._raw_symbol = to_exchange_symbol(ExchangeName.OKX, symbol)
        self._connector = connector
        self._reconnect = reconnect
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnects = max_reconnects
        self._url = OKX_DEMO_PUBLIC_WS_URL if sandbox else OKX_PUBLIC_WS_URL

    async def stream_trades(self) -> AsyncIterator[MarketTrade]:
        reconnects = 0
        while True:
            connection = None
            try:
                connection = await self._connector.connect(self._url)
                await connection.send(_okx_subscribe_message(channel="trades", inst_id=self._raw_symbol))
                logger.info(
                    "OKX websocket subscribed | channel=trades symbol=%s raw_symbol=%s",
                    self._symbol,
                    self._raw_symbol,
                )
                async for message in connection:
                    trades = self._map_message(message)
                    if trades:
                        reconnects = 0
                    for trade in trades:
                        yield trade
            except TRANSIENT_WS_EXCEPTIONS as exc:
                if not self._reconnect:
                    raise
                if self._max_reconnects is not None and reconnects >= self._max_reconnects:
                    logger.exception(
                        "OKX websocket max reconnects exceeded | channel=trades symbol=%s raw_symbol=%s reconnect_count=%s error=%s",
                        self._symbol,
                        self._raw_symbol,
                        reconnects,
                        exc,
                    )
                    raise
                reconnects += 1
                delay = _reconnect_delay(self._reconnect_delay_seconds, reconnects)
                logger.warning(
                    "OKX websocket disconnected; reconnecting | channel=trades symbol=%s raw_symbol=%s reconnect_count=%s delay_seconds=%.2f error=%s",
                    self._symbol,
                    self._raw_symbol,
                    reconnects,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                continue
            finally:
                if connection is not None:
                    try:
                        await connection.close()
                    except Exception as exc:  # pragma: no cover - defensive close best effort
                        logger.debug(
                            "OKX websocket close failed | channel=trades symbol=%s raw_symbol=%s error=%s",
                            self._symbol,
                            self._raw_symbol,
                            exc,
                        )
            if not self._reconnect:
                break
            reconnects += 1
            if self._max_reconnects is not None and reconnects > self._max_reconnects:
                break
            await asyncio.sleep(_reconnect_delay(self._reconnect_delay_seconds, reconnects))

    def _map_message(self, message: str | bytes) -> list[MarketTrade]:
        payload = _decode_json(message)
        rows = payload.get("data")
        if not isinstance(rows, list):
            return []
        trades: list[MarketTrade] = []
        for row in rows:
            if isinstance(row, Mapping):
                trades.append(_map_okx_trade(row, symbol=self._symbol, raw_symbol=self._raw_symbol))
        return trades


class OkxOrderBookWebSocketFeed:
    def __init__(
        self,
        *,
        symbol: str,
        connector: WebSocketConnector,
        sandbox: bool = False,
        depth_channel: str = "books5",
        reconnect: bool = True,
        reconnect_delay_seconds: float = 1.0,
        max_reconnects: int | None = None,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.OKX, symbol)
        self._connector = connector
        self._reconnect = reconnect
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnects = max_reconnects
        self._url = OKX_DEMO_PUBLIC_WS_URL if sandbox else OKX_PUBLIC_WS_URL
        self._depth_channel = depth_channel

    async def stream_order_book(self) -> AsyncIterator[MarketOrderBook]:
        reconnects = 0
        while True:
            connection = None
            try:
                connection = await self._connector.connect(self._url)
                await connection.send(_okx_subscribe_message(channel=self._depth_channel, inst_id=self._raw_symbol))
                logger.info(
                    "OKX websocket subscribed | channel=%s symbol=%s raw_symbol=%s",
                    self._depth_channel,
                    self._symbol,
                    self._raw_symbol,
                )
                async for message in connection:
                    order_books = self._map_message(message)
                    if order_books:
                        reconnects = 0
                    for order_book in order_books:
                        yield order_book
            except TRANSIENT_WS_EXCEPTIONS as exc:
                if not self._reconnect:
                    raise
                if self._max_reconnects is not None and reconnects >= self._max_reconnects:
                    logger.exception(
                        "OKX websocket max reconnects exceeded | channel=%s symbol=%s raw_symbol=%s reconnect_count=%s error=%s",
                        self._depth_channel,
                        self._symbol,
                        self._raw_symbol,
                        reconnects,
                        exc,
                    )
                    raise
                reconnects += 1
                delay = _reconnect_delay(self._reconnect_delay_seconds, reconnects)
                logger.warning(
                    "OKX websocket disconnected; reconnecting | channel=%s symbol=%s raw_symbol=%s reconnect_count=%s delay_seconds=%.2f error=%s",
                    self._depth_channel,
                    self._symbol,
                    self._raw_symbol,
                    reconnects,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                continue
            finally:
                if connection is not None:
                    try:
                        await connection.close()
                    except Exception as exc:  # pragma: no cover - defensive close best effort
                        logger.debug(
                            "OKX websocket close failed | channel=%s symbol=%s raw_symbol=%s error=%s",
                            self._depth_channel,
                            self._symbol,
                            self._raw_symbol,
                            exc,
                        )
            if not self._reconnect:
                break
            reconnects += 1
            if self._max_reconnects is not None and reconnects > self._max_reconnects:
                break
            await asyncio.sleep(_reconnect_delay(self._reconnect_delay_seconds, reconnects))

    def _map_message(self, message: str | bytes) -> list[MarketOrderBook]:
        payload = _decode_json(message)
        rows = payload.get("data")
        if not isinstance(rows, list):
            return []
        books: list[MarketOrderBook] = []
        for row in rows:
            if isinstance(row, Mapping):
                books.append(_map_okx_order_book(row, symbol=self._symbol, raw_symbol=self._raw_symbol))
        return books


def _okx_subscribe_message(*, channel: str, inst_id: str) -> str:
    return json.dumps(
        {"op": "subscribe", "args": [{"channel": channel, "instId": inst_id}]},
        separators=(",", ":"),
    )


def _reconnect_delay(base_delay_seconds: float, reconnect_count: int) -> float:
    return min(float(base_delay_seconds) * (2 ** min(max(reconnect_count - 1, 0), 6)), 60.0)


def _map_okx_trade(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=str(row.get("instId") or raw_symbol),
        price=Decimal(str(row["px"])),
        quantity=Decimal(str(row["sz"])),
        side=_map_okx_trade_side(row.get("side")),
        trade_id=_optional_str(row.get("tradeId")),
        event_time_ms=_optional_int(row.get("ts")),
        trade_time_ms=_optional_int(row.get("ts")),
        raw=dict(row),
    )


def _map_okx_order_book(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> MarketOrderBook:
    return MarketOrderBook(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=str(row.get("instId") or raw_symbol),
        bids=_map_levels(row.get("bids") or []),
        asks=_map_levels(row.get("asks") or []),
        event_time_ms=_optional_int(row.get("ts")),
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


def _map_okx_trade_side(value: Any) -> TradeSide:
    text = str(value or "").lower()
    if text == "buy":
        return TradeSide.BUY
    if text == "sell":
        return TradeSide.SELL
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
