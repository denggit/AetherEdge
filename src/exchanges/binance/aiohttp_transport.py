#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : aiohttp_transport.py
@Description: Binance aiohttp HTTP transport implementation.

This module provides ``AiohttpBinanceTransport`` — a real HTTP transport
that sends ``BinanceSignedRequest`` objects via aiohttp.

No live wiring.  No factory wiring.  No env reads.  No API key reads.
Not wired into the broker client by default.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import aiohttp

from src.exchanges.binance.signing import BinanceSignedRequest
from src.exchanges.binance.transport import BinanceTransportResponse
from src.exchanges.errors import ExchangeError, ExchangeErrorKind
from src.exchanges.models import ExchangeName


class AiohttpBinanceTransport:
    """aiohttp implementation of BinanceHttpTransport.

    This class performs HTTP I/O only when explicitly instantiated and called.
    It is not wired into live/runtime/factory by default.
    """

    def __init__(
        self,
        *,
        session: aiohttp.ClientSession | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._session = session
        self._timeout_seconds = timeout_seconds

    async def send(self, request: BinanceSignedRequest) -> BinanceTransportResponse:
        url = f"{request.base_url}{request.path}"
        if request.query_string:
            url = f"{url}?{request.query_string}"

        try:
            if self._session is not None:
                return await self._send_with_session(self._session, request, url)

            timeout = aiohttp.ClientTimeout(total=self._timeout_seconds)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                return await self._send_with_session(session, request, url)

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise ExchangeError(
                exchange=ExchangeName.BINANCE,
                kind=ExchangeErrorKind.NETWORK_ERROR,
                message=f"Binance HTTP transport error: {exc}",
                raw={"method": request.method, "path": request.path},
            ) from exc

    async def _send_with_session(
        self,
        session: aiohttp.ClientSession,
        request: BinanceSignedRequest,
        url: str,
    ) -> BinanceTransportResponse:
        async with session.request(
            request.method,
            url,
            headers=dict(request.headers),
        ) as response:
            payload = await self._read_payload(response)
            return BinanceTransportResponse(
                status_code=response.status,
                payload=payload,
                headers=dict(response.headers),
            )

    async def _read_payload(self, response: aiohttp.ClientResponse) -> Any:
        text = await response.text()
        if text == "":
            return {}

        try:
            return json.loads(text)
        except Exception:
            return {"message": text}


__all__ = ["AiohttpBinanceTransport"]
