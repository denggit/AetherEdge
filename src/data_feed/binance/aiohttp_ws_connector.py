#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/14
@File       : aiohttp_ws_connector.py
@Description: Reusable aiohttp WebSocket connector for Binance market data streams.

This module provides an async-iterable connection wrapper and a factory
function that are usable by the Binance data feed layer.  It does NOT:

- map any market events
- read environment variables
- read API keys or secrets
- import broker / execution / strategy modules
- place or cancel orders
"""

from __future__ import annotations

import aiohttp


# ---------------------------------------------------------------------------
# Connection wrapper
# ---------------------------------------------------------------------------


class AiohttpBinanceWsConnection:
    """Async iterator over Binance WebSocket text/binary messages.

    This class implements the :class:`BinanceWebSocketConnection` protocol
    defined in ``src.data_feed.binance.websocket_feed``.
    """

    def __init__(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        session: aiohttp.ClientSession,
    ) -> None:
        self._ws = ws
        self._session = session

    def __aiter__(self) -> "AiohttpBinanceWsConnection":
        return self

    async def __anext__(self) -> str | bytes:
        msg = await self._ws.receive()
        if msg.type == aiohttp.WSMsgType.TEXT:
            return msg.data
        if msg.type == aiohttp.WSMsgType.BINARY:
            return msg.data
        if msg.type == aiohttp.WSMsgType.CLOSED:
            raise StopAsyncIteration
        if msg.type == aiohttp.WSMsgType.ERROR:
            raise ConnectionError(f"WebSocket error: {self._ws.exception()}")
        # CLOSING / CLOSE / other sentinel — end iteration
        raise StopAsyncIteration

    async def close(self) -> None:
        """Close the underlying WebSocket and HTTP session."""
        await self._ws.close()
        await self._session.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


async def connect_binance_market_ws(url: str) -> AiohttpBinanceWsConnection:
    """Open an aiohttp WebSocket connection to *url*.

    Returns an :class:`AiohttpBinanceWsConnection` that can be used as an
    async iterator of raw messages (``str | bytes``).

    Raises ``ConnectionError`` (or aiohttp errors) when the connection fails.
    """
    timeout = aiohttp.ClientTimeout(total=30.0)
    session = aiohttp.ClientSession(timeout=timeout)
    try:
        ws = await session.ws_connect(url)
    except Exception:
        await session.close()
        raise
    return AiohttpBinanceWsConnection(ws, session)
