from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Mapping, Protocol

from src.data_feed.base import MarketDataEvent
from src.data_feed.binance.feed import (
    DEFAULT_BINANCE_CANONICAL_SYMBOL,
    DEFAULT_BINANCE_KLINE_INTERVAL,
    DEFAULT_BINANCE_RAW_SYMBOL,
    binance_default_market_stream_names,
    try_map_binance_market_event,
)

# ---------------------------------------------------------------------------
# Protocol definitions
# ---------------------------------------------------------------------------


class BinanceWebSocketConnection(Protocol):
    def __aiter__(self) -> AsyncIterator[str | bytes]:
        ...


BinanceWebSocketConnector = Callable[[str], Awaitable[BinanceWebSocketConnection]]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_USDM_WS_MARKET_BASE_URL = "wss://fstream.binance.com/market"

# ---------------------------------------------------------------------------
# URL helper
# ---------------------------------------------------------------------------


def build_binance_combined_market_stream_url(
    stream_names: tuple[str, ...],
    *,
    base_url: str = BINANCE_USDM_WS_MARKET_BASE_URL,
) -> str:
    if not stream_names:
        raise ValueError("stream_names must not be empty")

    normalized_streams = []
    for stream in stream_names:
        value = str(stream or "").strip()
        if not value:
            raise ValueError("stream name must not be empty")
        normalized_streams.append(value)

    return f"{base_url.rstrip('/')}/stream?streams={'/'.join(normalized_streams)}"


# ---------------------------------------------------------------------------
# Payload parsing helpers
# ---------------------------------------------------------------------------


def decode_binance_ws_message(message: str | bytes) -> Mapping[str, Any]:
    if isinstance(message, bytes):
        text = message.decode("utf-8")
    else:
        text = message

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Binance websocket JSON payload: {text!r}") from exc

    if not isinstance(payload, Mapping):
        raise ValueError("Binance websocket payload must be a JSON object")

    return payload


# ---------------------------------------------------------------------------
# Combined stream wrapper helper
# ---------------------------------------------------------------------------


def unwrap_binance_combined_stream_payload(
    payload: Mapping[str, Any],
) -> Mapping[str, Any]:
    data = payload.get("data")
    if isinstance(data, Mapping):
        return data
    return payload


# ---------------------------------------------------------------------------
# Feed class
# ---------------------------------------------------------------------------


class BinanceWebSocketMarketDataFeed:
    def __init__(
        self,
        *,
        connector: BinanceWebSocketConnector,
        canonical_symbol: str = DEFAULT_BINANCE_CANONICAL_SYMBOL,
        raw_symbol: str = DEFAULT_BINANCE_RAW_SYMBOL,
        kline_interval: str = DEFAULT_BINANCE_KLINE_INTERVAL,
        base_url: str = BINANCE_USDM_WS_MARKET_BASE_URL,
    ) -> None:
        self._connector = connector
        self._canonical_symbol = canonical_symbol
        self._raw_symbol = raw_symbol
        self._kline_interval = kline_interval
        self._base_url = base_url

    @property
    def canonical_symbol(self) -> str:
        return self._canonical_symbol

    @property
    def raw_symbol(self) -> str:
        return self._raw_symbol

    def stream_names(self) -> tuple[str, str]:
        return binance_default_market_stream_names(
            raw_symbol=self._raw_symbol,
            kline_interval=self._kline_interval,
        )

    def stream_url(self) -> str:
        return build_binance_combined_market_stream_url(
            self.stream_names(),
            base_url=self._base_url,
        )

    def map_payload(self, payload: Mapping[str, Any]) -> MarketDataEvent | None:
        raw_payload = unwrap_binance_combined_stream_payload(payload)
        return try_map_binance_market_event(
            raw_payload,
            canonical_symbol=self._canonical_symbol,
        )

    def map_message(self, message: str | bytes) -> MarketDataEvent | None:
        payload = decode_binance_ws_message(message)
        return self.map_payload(payload)

    async def events(self) -> AsyncIterator[MarketDataEvent]:
        connection = await self._connector(self.stream_url())
        async for message in connection:
            event = self.map_message(message)
            if event is not None:
                yield event
