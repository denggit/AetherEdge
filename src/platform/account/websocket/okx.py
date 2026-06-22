from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
from decimal import Decimal
from typing import Any, AsyncIterator, Mapping

from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.data.websocket.ports import WebSocketConnector
from src.platform.exchanges.errors import ExchangeConfigError
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, OrderSide, OrderStatus, PositionSide
from src.platform.exchanges.symbols import to_canonical_symbol, to_exchange_symbol

OKX_PRIVATE_WS_URL = "wss://ws.okx.com:8443/ws/v5/private"
OKX_DEMO_PRIVATE_WS_URL = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"

_OKX_ORDER_STATUS = {
    "live": OrderStatus.NEW,
    "partially_filled": OrderStatus.PARTIALLY_FILLED,
    "filled": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELED,
}


class OkxAccountEventStream:
    def __init__(
        self,
        *,
        symbol: str,
        config: ExchangeConfig,
        connector: WebSocketConnector,
        reconnect: bool = True,
        reconnect_delay_seconds: float = 1.0,
        max_reconnects: int | None = None,
    ) -> None:
        self._symbol = symbol
        self._raw_symbol = to_exchange_symbol(ExchangeName.OKX, symbol)
        self._config = config
        self._connector = connector
        self._reconnect = reconnect
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._max_reconnects = max_reconnects
        self._url = OKX_DEMO_PRIVATE_WS_URL if config.sandbox else OKX_PRIVATE_WS_URL

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.OKX

    @property
    def symbol(self) -> str:
        return self._symbol

    async def stream_events(self) -> AsyncIterator[AccountEvent]:
        self._require_credentials()
        reconnects = 0
        while True:
            connection = await self._connector.connect(self._url)
            try:
                await connection.send(_okx_login_message(self._config))
                await connection.send(_okx_subscribe_message(self._raw_symbol))
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
        arg = payload.get("arg") if isinstance(payload.get("arg"), Mapping) else {}
        channel = str(arg.get("channel") or "")
        rows = payload.get("data")
        if not isinstance(rows, list):
            event = str(payload.get("event") or "")
            if event:
                return [AccountEvent(exchange=self.exchange, event_type=AccountEventType.SYSTEM, raw=payload)]
            return []
        events: list[AccountEvent] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            if channel == "orders":
                events.append(_map_okx_order_event(row, fallback_symbol=self._symbol))
            elif channel == "account":
                events.extend(_map_okx_balance_events(row))
            elif channel == "positions":
                events.append(_map_okx_position_event(row, fallback_symbol=self._symbol))
            else:
                events.append(AccountEvent(exchange=self.exchange, event_type=AccountEventType.UNKNOWN, raw=row))
        return events

    def _require_credentials(self) -> None:
        if not self._config.api_key or not self._config.api_secret or not self._config.passphrase:
            raise ExchangeConfigError("OKX private WebSocket requires api_key, api_secret and passphrase")


def _okx_login_message(config: ExchangeConfig) -> str:
    timestamp = str(int(time.time()))
    payload = f"{timestamp}GET/users/self/verify"
    sign = base64.b64encode(hmac.new(config.api_secret.encode(), payload.encode(), hashlib.sha256).digest()).decode()
    return json.dumps(
        {
            "op": "login",
            "args": [
                {
                    "apiKey": config.api_key,
                    "passphrase": config.passphrase,
                    "timestamp": timestamp,
                    "sign": sign,
                }
            ],
        },
        separators=(",", ":"),
    )


def _okx_subscribe_message(raw_symbol: str) -> str:
    return json.dumps(
        {
            "op": "subscribe",
            "args": [
                {"channel": "orders", "instType": "SWAP", "instId": raw_symbol},
                {"channel": "account", "ccy": "USDT"},
                {"channel": "positions", "instType": "SWAP", "instId": raw_symbol},
            ],
        },
        separators=(",", ":"),
    )


def _map_okx_order_event(row: Mapping[str, Any], *, fallback_symbol: str) -> AccountEvent:
    raw_symbol = str(row.get("instId") or "")
    return AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.ORDER,
        symbol=_symbol_or_fallback(raw_symbol, fallback_symbol),
        raw_symbol=raw_symbol or None,
        event_time_ms=_optional_int(row.get("uTime") or row.get("cTime")),
        order_id=_optional_str(row.get("ordId")),
        client_order_id=_optional_str(row.get("clOrdId")),
        order_status=_OKX_ORDER_STATUS.get(str(row.get("state", "")).lower(), OrderStatus.UNKNOWN),
        side=_map_order_side(row.get("side")),
        position_side=_map_position_side(row.get("posSide")),
        price=_first_positive_decimal(row, "avgPx", "fillPx", "px"),
        quantity=_optional_decimal(row.get("sz")),
        filled_quantity=_optional_decimal(row.get("accFillSz")),
        raw=row,
    )


def _map_okx_balance_events(row: Mapping[str, Any]) -> list[AccountEvent]:
    details = row.get("details")
    if not isinstance(details, list):
        details = [row]
    events: list[AccountEvent] = []
    for detail in details:
        if isinstance(detail, Mapping):
            events.append(
                AccountEvent(
                    exchange=ExchangeName.OKX,
                    event_type=AccountEventType.BALANCE,
                    event_time_ms=_optional_int(row.get("uTime")),
                    asset=_optional_str(detail.get("ccy")),
                    balance=_optional_decimal(detail.get("cashBal") or detail.get("eq")),
                    available=_optional_decimal(detail.get("availBal") or detail.get("availEq")),
                    raw=detail,
                )
            )
    return events


def _map_okx_position_event(row: Mapping[str, Any], *, fallback_symbol: str) -> AccountEvent:
    raw_symbol = str(row.get("instId") or "")
    return AccountEvent(
        exchange=ExchangeName.OKX,
        event_type=AccountEventType.POSITION,
        symbol=_symbol_or_fallback(raw_symbol, fallback_symbol),
        raw_symbol=raw_symbol or None,
        event_time_ms=_optional_int(row.get("uTime")),
        position_side=_map_position_side(row.get("posSide")),
        quantity=_optional_decimal(row.get("pos")),
        price=_optional_decimal(row.get("avgPx")),
        raw=row,
    )


def _symbol_or_fallback(raw_symbol: str, fallback_symbol: str) -> str:
    if not raw_symbol:
        return fallback_symbol
    try:
        return to_canonical_symbol(ExchangeName.OKX, raw_symbol)
    except Exception:
        return fallback_symbol


def _decode_json(message: str | bytes) -> Mapping[str, Any]:
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    payload = json.loads(message)
    return payload if isinstance(payload, Mapping) else {}


def _map_order_side(value: Any) -> OrderSide | None:
    text = str(value or "").lower()
    if text == "buy":
        return OrderSide.BUY
    if text == "sell":
        return OrderSide.SELL
    return None


def _map_position_side(value: Any) -> PositionSide | None:
    text = str(value or "").lower()
    if text == "long":
        return PositionSide.LONG
    if text == "short":
        return PositionSide.SHORT
    if text in {"both", "net"}:
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
