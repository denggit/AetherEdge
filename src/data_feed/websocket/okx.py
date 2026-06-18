from __future__ import annotations

import json
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping

from src.data_feed.models import MarketTrade, TradeSide
from src.data_feed.websocket.ports import WebSocketConnector
from src.exchanges.models import ExchangeName
from src.exchanges.symbols import to_exchange_symbol

OKX_PUBLIC_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
OKX_DEMO_PUBLIC_WS_URL = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"


class OkxTradeWebSocketFeed:
    def __init__(
        self,
        *,
        symbol: str,
        connector: WebSocketConnector,
        sandbox: bool = False,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.OKX, symbol)
        self._connector = connector
        self._url = OKX_DEMO_PUBLIC_WS_URL if sandbox else OKX_PUBLIC_WS_URL

    async def stream_trades(self) -> AsyncIterator[MarketTrade]:
        connection = await self._connector.connect(self._url)
        try:
            await connection.send(
                json.dumps(
                    {
                        "op": "subscribe",
                        "args": [{"channel": "trades", "instId": self._raw_symbol}],
                    },
                    separators=(",", ":"),
                )
            )
            async for message in connection:
                for trade in self._map_message(message):
                    yield trade
        finally:
            await connection.close()

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
