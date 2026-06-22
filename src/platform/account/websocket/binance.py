from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping

from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.websocket.ports import WebSocketConnector
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, OrderSide, OrderStatus, PositionSide
from src.platform.exchanges.ports import ExchangeClient
from src.platform.exchanges.symbols import to_canonical_symbol, to_exchange_symbol

BINANCE_USER_WS_URL = "wss://fstream.binance.com/ws"
BINANCE_TESTNET_USER_WS_URL = "wss://stream.binancefuture.com/ws"

_BINANCE_ORDER_STATUS = {
    "NEW": OrderStatus.NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.CANCELED,
}


class BinanceAccountEventStream:
    def __init__(
        self,
        *,
        symbol: str,
        config: ExchangeConfig,
        exchange_client: ExchangeClient,
        connector: WebSocketConnector,
        reconnect: bool = True,
        reconnect_delay_seconds: float = 1.0,
        max_reconnects: int | None = None,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.BINANCE, symbol)
        self._config = config
        self._exchange_client = exchange_client
        self._connector = connector
        self._reconnect = reconnect
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnects = max_reconnects
        self._base_url = BINANCE_TESTNET_USER_WS_URL if config.sandbox else BINANCE_USER_WS_URL

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.BINANCE

    @property
    def symbol(self) -> str:
        return self._symbol

    async def stream_events(self) -> AsyncIterator[AccountEvent]:
        reconnects = 0
        listen_key = await self._exchange_client.create_user_stream_listen_key()  # type: ignore[attr-defined]
        while True:
            connection = await self._connector.connect(f"{self._base_url}/{listen_key}")
            try:
                async for message in connection:
                    for event in self._map_message(message):
                        yield event
            finally:
                await connection.close()
            if not self._reconnect or (self._max_reconnects is not None and reconnects >= self._max_reconnects):
                break
            reconnects += 1
            await asyncio.sleep(self._reconnect_delay_seconds)

    def _map_message(self, message: str | bytes) -> list[AccountEvent]:
        payload = _decode_json(message)
        event_type = str(payload.get("e") or "")
        if event_type == "ORDER_TRADE_UPDATE":
            order = payload.get("o")
            return [_map_binance_order_event(order, payload, fallback_symbol=self._symbol)] if isinstance(order, Mapping) else []
        if event_type == "ACCOUNT_UPDATE":
            account = payload.get("a")
            return _map_binance_account_update(account, payload, fallback_symbol=self._symbol) if isinstance(account, Mapping) else []
        if event_type:
            return [AccountEvent(exchange=self.exchange, event_type=AccountEventType.SYSTEM, event_time_ms=_optional_int(payload.get("E")), raw=payload)]
        return []


def _map_binance_order_event(row: Mapping[str, Any], payload: Mapping[str, Any], *, fallback_symbol: str) -> AccountEvent:
    raw_symbol = str(row.get("s") or "")
    return AccountEvent(
        exchange=ExchangeName.BINANCE,
        event_type=AccountEventType.ORDER,
        symbol=_symbol_or_fallback(raw_symbol, fallback_symbol),
        raw_symbol=raw_symbol or None,
        event_time_ms=_optional_int(payload.get("E") or row.get("T")),
        order_id=_optional_str(row.get("i")),
        client_order_id=_optional_str(row.get("c")),
        order_status=_BINANCE_ORDER_STATUS.get(str(row.get("X", "")).upper(), OrderStatus.UNKNOWN),
        side=_map_order_side(row.get("S")),
        position_side=_map_position_side(row.get("ps")),
        price=_first_positive_decimal(row, "ap", "L", "p"),
        quantity=_optional_decimal(row.get("q")),
        filled_quantity=_optional_decimal(row.get("z")),
        raw=row,
    )


def _map_binance_account_update(account: Mapping[str, Any], payload: Mapping[str, Any], *, fallback_symbol: str) -> list[AccountEvent]:
    events: list[AccountEvent] = []
    event_time_ms = _optional_int(payload.get("E"))
    balances = account.get("B")
    if isinstance(balances, list):
        for row in balances:
            if isinstance(row, Mapping):
                events.append(
                    AccountEvent(
                        exchange=ExchangeName.BINANCE,
                        event_type=AccountEventType.BALANCE,
                        event_time_ms=event_time_ms,
                        asset=_optional_str(row.get("a")),
                        balance=_optional_decimal(row.get("wb")),
                        available=_optional_decimal(row.get("cw")),
                        raw=row,
                    )
                )
    positions = account.get("P")
    if isinstance(positions, list):
        for row in positions:
            if isinstance(row, Mapping):
                raw_symbol = str(row.get("s") or "")
                events.append(
                    AccountEvent(
                        exchange=ExchangeName.BINANCE,
                        event_type=AccountEventType.POSITION,
                        symbol=_symbol_or_fallback(raw_symbol, fallback_symbol),
                        raw_symbol=raw_symbol or None,
                        event_time_ms=event_time_ms,
                        position_side=_map_position_side(row.get("ps")),
                        quantity=_optional_decimal(row.get("pa")),
                        price=_optional_decimal(row.get("ep")),
                        raw=row,
                    )
                )
    return events


def _symbol_or_fallback(raw_symbol: str, fallback_symbol: str) -> str:
    if not raw_symbol:
        return fallback_symbol
    try:
        return to_canonical_symbol(ExchangeName.BINANCE, raw_symbol)
    except Exception:
        return fallback_symbol


def _decode_json(message: str | bytes) -> Mapping[str, Any]:
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    payload = json.loads(message)
    return payload if isinstance(payload, Mapping) else {}


def _map_order_side(value: Any) -> OrderSide | None:
    text = str(value or "").upper()
    if text == "BUY":
        return OrderSide.BUY
    if text == "SELL":
        return OrderSide.SELL
    return None


def _map_position_side(value: Any) -> PositionSide | None:
    text = str(value or "").upper()
    if text == "LONG":
        return PositionSide.LONG
    if text == "SHORT":
        return PositionSide.SHORT
    if text == "BOTH":
        return PositionSide.BOTH
    return None


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _first_positive_decimal(row: Mapping[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = _optional_decimal(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)
