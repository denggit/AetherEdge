#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/14
@File       : okx_market_data_client.py
@Description: OKX implementation of MarketDataClientPort.

This class directly wraps OKX market data APIs (REST candles + websocket trades).
It does NOT depend on any strategy, monitor, or business logic.
It is the sole OKX market data adapter for live trading.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import aiohttp

from src.data_feed.market_data_client_port import (
    CandleSnapshot,
    MarketDataClientPort,
    MarketDataEvent,
    MarketTradeSnapshot,
)
from src.utils.log import get_logger

logger = get_logger(__name__)


# ======================================================================
# Internal low-level REST client
# ======================================================================


@dataclass(frozen=True)
class _OkxPublicRestConfig:
    rest_base_url: str = "https://www.okx.com"
    candle_limit: int = 100
    timeout_seconds: float = 10.0
    connector_limit: int = 10


class _OkxPublicRestClient:
    """Minimal OKX public REST client for fetching candles."""

    def __init__(self, config: _OkxPublicRestConfig) -> None:
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

    async def fetch_candles(
        self, *, inst_id: str, bar: str, limit: int
    ) -> list[dict[str, Any]]:
        """Fetch raw candle data from OKX public REST.

        Returns a list of raw candle rows (each a list from the OKX response).
        """
        await self.start()
        if self._session is None:
            raise RuntimeError("OKX REST session is not initialized")
        url = f"{self._config.rest_base_url}/api/v5/market/candles"
        params = {"instId": inst_id, "bar": bar, "limit": str(limit)}
        async with self._session.get(url, params=params) as resp:
            payload = await resp.json()
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX candle API error: {payload}")
        return list(payload.get("data", []))


# ======================================================================
# Internal candle row parser
# ======================================================================


@dataclass(frozen=True)
class _RawCandle:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    confirmed: bool


def _parse_raw_candle(row: list, *, include_live: bool) -> _RawCandle | None:
    """Parse a single OKX candle row into a _RawCandle.

    Returns None if the row is too short or if ``include_live`` is False
    and the candle is not confirmed.
    """
    if len(row) < 6:
        return None
    confirmed = row[8] == "1" if len(row) >= 9 else True
    if not include_live and not confirmed:
        return None
    return _RawCandle(
        ts_ms=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        volume=float(row[5]),
        confirmed=confirmed,
    )


# ======================================================================
# OkxMarketDataClient
# ======================================================================


@dataclass(frozen=True)
class OkxMarketDataClientConfig:
    """Configuration for OkxMarketDataClient.

    All values are exchange-specific and do NOT carry business semantics.
    """

    inst_id: str = "ETH-USDT-SWAP"
    bar: str = "15m"
    rest_base_url: str = "https://www.okx.com"
    ws_public_url: str = "wss://ws.okx.com:8443/ws/v5/public"
    candle_limit: int = 100
    rest_timeout_seconds: float = 10.0
    rest_connector_limit: int = 10
    use_live_candle: bool = True
    ws_ping_interval: int = 20


class OkxMarketDataClient(MarketDataClientPort):
    """OKX implementation of MarketDataClientPort.

    Directly wraps OKX market data APIs — REST candles and websocket trades.
    Does NOT depend on any strategy, monitor, or business logic.
    """

    def __init__(self, config: OkxMarketDataClientConfig) -> None:
        self._config = config
        self._rest = _OkxPublicRestClient(
            _OkxPublicRestConfig(
                rest_base_url=config.rest_base_url,
                candle_limit=config.candle_limit,
                timeout_seconds=config.rest_timeout_seconds,
                connector_limit=config.rest_connector_limit,
            )
        )
        self._bar_interval_ms = self._parse_bar_interval_ms(config.bar)
        self._ws_session: aiohttp.ClientSession | None = None
        self._ws_running: bool = False

    # ------------------------------------------------------------------
    # MarketDataClientPort methods
    # ------------------------------------------------------------------

    async def fetch_recent_klines(self, *, limit: int) -> list[CandleSnapshot]:
        """Fetch recent klines directly from OKX public REST candles API.

        Returns the last *limit* candles mapped to ``CandleSnapshot`` DTOs.
        No BOLL calculation, no strategy updates, no TP processing.
        """
        if limit <= 0:
            raise ValueError("limit must be positive")

        raw_rows = await self._rest.fetch_candles(
            inst_id=self._config.inst_id,
            bar=self._config.bar,
            limit=max(limit, self._config.candle_limit),
        )

        candles: list[_RawCandle] = []
        for row in reversed(raw_rows):  # OKX returns newest first
            parsed = _parse_raw_candle(row, include_live=self._config.use_live_candle)
            if parsed is not None:
                candles.append(parsed)

        # Sort by timestamp ascending
        candles.sort(key=lambda c: c.ts_ms)

        # Take the last *limit*
        candles = candles[-limit:]

        bar_interval_ms = self._bar_interval_ms
        result: list[CandleSnapshot] = []
        for candle in candles:
            result.append(
                CandleSnapshot(
                    open_time_ms=candle.ts_ms,
                    close_time_ms=candle.ts_ms + bar_interval_ms if bar_interval_ms > 0 else candle.ts_ms,
                    open_price=Decimal(str(candle.open)),
                    high_price=Decimal(str(candle.high)),
                    low_price=Decimal(str(candle.low)),
                    close_price=Decimal(str(candle.close)),
                    volume=Decimal(str(candle.volume)),
                    is_closed=candle.confirmed,
                    raw={
                        "inst_id": self._config.inst_id,
                        "bar": self._config.bar,
                    },
                )
            )
        return result

    async def stream_market_events(
        self,
        on_event: Callable[[MarketDataEvent], Awaitable[None]],
    ) -> None:
        """Stream market trade events directly from OKX websocket.

        Opens a websocket connection to the OKX public trades channel.
        Each trade tick is mapped to a ``MarketTradeSnapshot`` and passed
        to *on_event*. No CVD calculation, no entry detection, no strategy.
        """
        self._ws_running = True
        subscribe_msg = {
            "op": "subscribe",
            "args": [{"channel": "trades", "instId": self._config.inst_id}],
        }

        while self._ws_running:
            try:
                timeout = aiohttp.ClientTimeout(total=None)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    self._ws_session = session
                    async with session.ws_connect(
                        self._config.ws_public_url,
                        heartbeat=self._config.ws_ping_interval,
                        autoping=True,
                    ) as ws:
                        await ws.send_json(subscribe_msg)
                        logger.info(
                            "OKX_MARKET_DATA_WS_CONNECTED | inst_id=%s channel=trades",
                            self._config.inst_id,
                        )
                        async for msg in ws:
                            if not self._ws_running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_ws_message(msg.json(), on_event)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception:
                if not self._ws_running:
                    break
                logger.exception(
                    "OKX_MARKET_DATA_WS_DISCONNECTED | inst_id=%s retry_in=3s",
                    self._config.inst_id,
                )
                await asyncio.sleep(3)
            finally:
                self._ws_session = None

    async def close(self) -> None:
        """Stop the websocket loop and close the REST client."""
        self._ws_running = False
        # Cancel the current websocket connection if any
        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()
            self._ws_session = None
        await self._rest.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _handle_ws_message(
        self,
        payload: dict[str, Any],
        on_event: Callable[[MarketDataEvent], Awaitable[None]],
    ) -> None:
        """Parse a single websocket message and emit market events."""
        if "event" in payload:
            logger.info("OKX websocket event: %s", payload)
            return
        if payload.get("arg", {}).get("channel") != "trades":
            return
        for item in payload.get("data", []):
            try:
                snapshot = MarketTradeSnapshot(
                    event_time_ms=int(item["ts"]),
                    price=Decimal(str(item["px"])),
                    qty=Decimal(str(item.get("sz", 0))),
                    side=str(item.get("side", "unknown")),
                    raw={"inst_id": self._config.inst_id},
                )
                await on_event(snapshot)
            except Exception:
                logger.warning(
                    "OKX_MARKET_DATA_INVALID_TICK | payload=%s",
                    item,
                )

    @staticmethod
    def _parse_bar_interval_ms(bar: str) -> int:
        """Parse a bar string like '15m', '1h', '1d' into milliseconds."""
        text = bar.strip().lower()
        if text.endswith("m"):
            return int(text[:-1]) * 60 * 1000
        if text.endswith("h"):
            return int(text[:-1]) * 60 * 60 * 1000
        if text.endswith("d"):
            return int(text[:-1]) * 24 * 60 * 60 * 1000
        raise ValueError(f"Unsupported bar interval: {bar}")
