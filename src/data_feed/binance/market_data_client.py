#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : market_data_client.py
@Description: Binance implementation of MarketDataClientPort.

This class directly wraps Binance public market data APIs
(REST candles + WebSocket aggTrade).  It does NOT depend on any
strategy, monitor, or business logic.

It is the sole Binance market data adapter for live trading.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import aiohttp

from src.data_feed.binance.aiohttp_ws_connector import connect_binance_market_ws
from src.data_feed.binance.feed import (
    binance_agg_trade_stream_name,
    normalize_binance_stream_symbol,
)
from src.data_feed.binance.mappers import (
    map_binance_agg_trade_to_market_trade_snapshot,
    map_binance_rest_kline_to_candle_snapshot,
)
from src.data_feed.binance.websocket_feed import (
    BINANCE_USDM_WS_MARKET_BASE_URL,
    decode_binance_ws_message,
    unwrap_binance_combined_stream_payload,
)
from src.data_feed.market_data_client_port import (
    CandleSnapshot,
    MarketDataClientPort,
    MarketDataEvent,
    MarketTradeSnapshot,
)
from src.utils.log import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_USDM_REST_BASE_URL: str = "https://fapi.binance.com"
BINANCE_KLINES_PATH: str = "/fapi/v1/klines"
BINANCE_WS_RAW_BASE_URL: str = "wss://fstream.binance.com/ws"
_MAX_KLINES_LIMIT: int = 1500

# ---------------------------------------------------------------------------
# Internal low-level REST client
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _BinancePublicRestConfig:
    rest_base_url: str = BINANCE_USDM_REST_BASE_URL
    timeout_seconds: float = 10.0
    connector_limit: int = 10


class _BinancePublicRestClient:
    """Minimal Binance public REST client for fetching klines."""

    def __init__(self, config: _BinancePublicRestConfig) -> None:
        self._config = config
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        if self._session is not None and not self._session.closed:
            return
        connector = aiohttp.TCPConnector(limit=self._config.connector_limit)
        timeout = aiohttp.ClientTimeout(total=self._config.timeout_seconds)
        self._session = aiohttp.ClientSession(connector=connector, timeout=timeout)

    async def close(self) -> None:
        if self._session is None:
            return
        await self._session.close()
        self._session = None

    async def fetch_klines(
        self, *, symbol: str, interval: str, limit: int
    ) -> list[list[Any]]:
        """Fetch raw kline data from Binance public REST.

        Returns the raw JSON array-of-arrays from ``GET /fapi/v1/klines``.
        Binance returns oldest first.
        """
        await self.start()
        if self._session is None:
            raise RuntimeError("Binance REST session is not initialized")

        url = f"{self._config.rest_base_url.rstrip('/')}{BINANCE_KLINES_PATH}"
        params = {"symbol": symbol, "interval": interval, "limit": str(limit)}

        async with self._session.get(url, params=params) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"Binance public klines request failed: "
                    f"HTTP {resp.status} for {url} — {body[:500]}"
                )
            payload = await resp.json()

        if not isinstance(payload, list):
            raise ValueError(
                f"Binance klines payload must be a list, got {type(payload).__name__}"
            )

        return payload


# ---------------------------------------------------------------------------
# BinanceMarketDataClient
# ---------------------------------------------------------------------------


class BinanceMarketDataClient(MarketDataClientPort):
    """Binance implementation of MarketDataClientPort.

    Directly wraps Binance public market data APIs —
    REST klines and WebSocket aggTrade.  Does NOT depend on any
    strategy, monitor, or business logic.

    Parameters
    ----------
    symbol:
        Binance raw symbol, e.g. ``"ETHUSDT"``.
    interval:
        Kline interval string, e.g. ``"15m"``.
    rest_client:
        Optional pre-built REST client (for dependency injection in tests).
    ws_connector:
        Optional WebSocket connector callable (for dependency injection).
    request_timeout_seconds:
        HTTP request timeout in seconds.
    """

    def __init__(
        self,
        *,
        symbol: str,
        interval: str,
        rest_client: _BinancePublicRestClient | None = None,
        ws_connector: Callable[
            [str], Awaitable[Any]
        ] | None = None,
        request_timeout_seconds: float = 10.0,
    ) -> None:
        self._symbol = symbol
        self._interval = interval

        if rest_client is not None:
            self._rest = rest_client
        else:
            self._rest = _BinancePublicRestClient(
                _BinancePublicRestConfig(
                    rest_base_url=BINANCE_USDM_REST_BASE_URL,
                    timeout_seconds=request_timeout_seconds,
                )
            )

        self._ws_connector: Callable[[str], Awaitable[Any]] = (
            ws_connector or connect_binance_market_ws
        )
        self._ws_connection: Any = None
        self._ws_running: bool = False

    # ------------------------------------------------------------------
    # MarketDataClientPort methods
    # ------------------------------------------------------------------

    async def fetch_recent_klines(self, *, limit: int) -> list[CandleSnapshot]:
        """Fetch recent klines from Binance public REST klines API.

        Returns the last *limit* candles mapped to ``CandleSnapshot`` DTOs,
        sorted oldest first.  The last (still-forming) candle may be
        included if Binance returned it — its ``is_closed`` will be
        determined by ``close_time_ms`` vs current time.

        Raises ValueError if *limit* is not positive.
        """
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be positive")
        if limit > _MAX_KLINES_LIMIT:
            raise ValueError(
                f"limit must not exceed {_MAX_KLINES_LIMIT}, got {limit}"
            )

        raw_rows = await self._rest.fetch_klines(
            symbol=self._symbol,
            interval=self._interval,
            limit=limit,
        )

        now_ms = int(time.time() * 1000)

        result: list[CandleSnapshot] = []
        for row in raw_rows:
            try:
                snapshot = map_binance_rest_kline_to_candle_snapshot(
                    row,
                    symbol=self._symbol,
                    interval=self._interval,
                    now_ms=now_ms,
                )
                result.append(snapshot)
            except ValueError as exc:
                logger.warning(
                    "BINANCE_MARKET_DATA_INVALID_KLINE_ROW | symbol=%s row=%s",
                    self._symbol,
                    row,
                )
                raise

        # Sort by open_time_ms to guarantee oldest -> newest ordering
        result.sort(key=lambda c: c.open_time_ms)
        # Take the most recent 'limit' candles, still oldest -> newest
        if len(result) > limit:
            result = result[-limit:]
        return result

    async def stream_market_events(
        self,
        on_event: Callable[[MarketDataEvent], Awaitable[None]],
    ) -> None:
        """Stream market trade events from Binance WebSocket.

        Connects to the Binance aggTrade stream, maps each event to a
        ``MarketTradeSnapshot``, and passes it to *on_event*.

        Does NOT calculate CVD, detect entries, or touch strategy logic.
        CancelledError is re-raised; network errors trigger reconnect.
        """
        self._ws_running = True

        stream_name = binance_agg_trade_stream_name(self._symbol)
        raw_symbol = normalize_binance_stream_symbol(self._symbol)
        ws_url = f"{BINANCE_WS_RAW_BASE_URL.rstrip('/')}/{raw_symbol}@aggTrade"

        while self._ws_running:
            try:
                connection = await self._ws_connector(ws_url)
                self._ws_connection = connection

                logger.info(
                    "BINANCE_MARKET_DATA_WS_CONNECTED | symbol=%s stream=%s",
                    self._symbol,
                    stream_name,
                )

                async for message in connection:
                    if not self._ws_running:
                        break

                    try:
                        payload = decode_binance_ws_message(message)
                    except ValueError:
                        logger.debug(
                            "BINANCE_MARKET_DATA_INVALID_JSON | symbol=%s",
                            self._symbol,
                        )
                        continue

                    # Unwrap combined-stream envelope if present
                    inner = unwrap_binance_combined_stream_payload(payload)

                    event_type = str(inner.get("e") or "")
                    if event_type != "aggTrade":
                        continue

                    try:
                        snapshot = map_binance_agg_trade_to_market_trade_snapshot(
                            inner
                        )
                    except ValueError:
                        logger.warning(
                            "BINANCE_MARKET_DATA_INVALID_TRADE | symbol=%s payload=%s",
                            self._symbol,
                            inner,
                        )
                        continue

                    try:
                        await on_event(snapshot)
                    except asyncio.CancelledError:  # pragma: no cover
                        raise
                    except Exception:  # pragma: no cover
                        logger.warning(
                            "BINANCE_MARKET_DATA_ON_EVENT_ERROR | symbol=%s",
                            self._symbol,
                            exc_info=True,
                        )

            except asyncio.CancelledError:  # pragma: no cover
                raise
            except Exception:  # pragma: no cover
                if not self._ws_running:
                    break
                logger.exception(
                    "BINANCE_MARKET_DATA_WS_DISCONNECTED | symbol=%s retry_in=3s",
                    self._symbol,
                )
                await asyncio.sleep(3)
            finally:
                if self._ws_connection is not None:
                    try:
                        await self._ws_connection.close()
                    except Exception:  # pragma: no cover
                        pass
                    self._ws_connection = None

    async def close(self) -> None:
        """Stop the WebSocket loop and close the REST client.

        Idempotent — safe to call multiple times.
        """
        self._ws_running = False

        if self._ws_connection is not None:
            try:
                await self._ws_connection.close()
            except Exception:  # pragma: no cover
                pass
            self._ws_connection = None

        await self._rest.close()
