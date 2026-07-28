from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from src.platform.data.models import MarketOpenInterest
from src.platform.data.websocket.okx import (
    OKX_DEMO_PUBLIC_WS_URL,
    OKX_PUBLIC_WS_URL,
)
from src.platform.data.websocket.okx_public import (
    OkxPublicWebSocketProtocolError,
    _OkxPublicWebSocketFeed,
    decode_okx_public_message,
)
from src.platform.data.websocket.ports import WebSocketConnector
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.symbols import to_exchange_symbol


_CHANNEL = "open-interest"


class OkxOpenInterestWebSocketFeed:
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
        self._session = _OkxPublicWebSocketFeed[MarketOpenInterest](
            symbol=symbol,
            raw_symbol=self._raw_symbol,
            channel=_CHANNEL,
            url=(
                OKX_DEMO_PUBLIC_WS_URL
                if sandbox
                else OKX_PUBLIC_WS_URL
            ),
            connector=connector,
            reconnect=reconnect,
            reconnect_delay_seconds=reconnect_delay_seconds,
            max_reconnects=max_reconnects,
        )

    async def stream_open_interest(
        self,
    ) -> AsyncIterator[MarketOpenInterest]:
        async for event in self._session.stream(mapper=self._map_message):
            yield event

    async def close(self) -> None:
        await self._session.close()

    def _map_message(
        self,
        message: str | bytes,
    ) -> list[MarketOpenInterest]:
        payload = decode_okx_public_message(
            message,
            channel=_CHANNEL,
            symbol=self._symbol,
            raw_symbol=self._raw_symbol,
        )
        if payload is None:
            return []
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise OkxPublicWebSocketProtocolError(
                "OKX open interest data must be a list"
            )
        events: list[MarketOpenInterest] = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise OkxPublicWebSocketProtocolError(
                    "OKX open interest row must be a mapping"
                )
            if str(row.get("instId") or self._raw_symbol) != self._raw_symbol:
                continue
            instrument_type = str(row.get("instType") or "")
            if not instrument_type:
                raise OkxPublicWebSocketProtocolError(
                    "OKX open interest instType is required"
                )
            events.append(
                MarketOpenInterest(
                    exchange=ExchangeName.OKX,
                    symbol=self._symbol,
                    raw_symbol=self._raw_symbol,
                    instrument_type=instrument_type,
                    open_interest_contracts=_required_decimal(
                        row.get("oi"),
                        "oi",
                    ),
                    open_interest_base=_optional_decimal(row.get("oiCcy")),
                    open_interest_usd=_optional_decimal(row.get("oiUsd")),
                    event_time_ms=_required_int(row.get("ts"), "ts"),
                    raw=dict(row),
                )
            )
        return events


def _required_decimal(value: Any, field: str) -> Decimal:
    if value in (None, ""):
        raise OkxPublicWebSocketProtocolError(
            f"OKX open interest {field} is required"
        )
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OkxPublicWebSocketProtocolError(
            f"invalid OKX open interest {field}: {value!r}"
        ) from exc


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise OkxPublicWebSocketProtocolError(
            f"invalid OKX open interest decimal: {value!r}"
        ) from exc


def _required_int(value: Any, field: str) -> int:
    if value is None or type(value) is bool:
        raise OkxPublicWebSocketProtocolError(
            f"OKX open interest {field} is required"
        )
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise OkxPublicWebSocketProtocolError(
            f"invalid OKX open interest {field}: {value!r}"
        ) from exc


__all__ = ["OkxOpenInterestWebSocketFeed"]
