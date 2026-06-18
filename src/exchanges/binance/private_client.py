#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : private_client.py
@Description: Binance signed REST client with injectable transport.

Wraps ``build_signed_request()`` from ``signing.py`` and dispatches through
an injectable ``BinanceHttpTransport``.  This keeps the client testable with
fake transports and avoids coupling to aiohttp.

No live wiring.  No Trader / strategy / factory wiring.  No env reads.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from src.exchanges.binance.mapper import map_binance_error
from src.exchanges.binance.signing import (
    BINANCE_USDM_BASE_URL,
    BinanceSignedRequest,
    build_signed_request,
)
from src.exchanges.binance.transport import (
    BinanceHttpTransport,
    BinanceTransportResponse,
)


class BinancePrivateClient:
    """Signed Binance USD-M Futures REST client.

    Accepts an injectable *transport* so unit tests can use a fake transport.
    When *transport* is not provided a real ``AiohttpBinanceTransport`` is
    created lazily on ``start()``.

    Parameters
    ----------
    api_key:
        Binance API key.  Never printed in logs.
    api_secret:
        Binance API secret.  Never printed in logs.
    transport:
        Injectable transport implementing ``BinanceHttpTransport``.
        When ``None``, ``start()`` creates a real aiohttp transport.
    base_url:
        Binance USD-M Futures base URL.
    recv_window:
        Binance ``recvWindow`` value in milliseconds.
    timestamp_factory:
        Callable returning a UNIX-millisecond timestamp.
        Injected for deterministic testing.
    """

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        transport: BinanceHttpTransport | None = None,
        transport_factory: Callable[[], BinanceHttpTransport] | None = None,
        base_url: str = BINANCE_USDM_BASE_URL,
        recv_window: int = 5000,
        timestamp_factory: Callable[[], int] | None = None,
    ) -> None:
        if not api_key:
            raise ValueError("api_key must not be empty")
        if not api_secret:
            raise ValueError("api_secret must not be empty")

        self._api_key = api_key
        self._api_secret = api_secret
        self._transport = transport
        self._transport_factory = transport_factory
        self._base_url = base_url
        self._recv_window = recv_window
        self._timestamp_factory = timestamp_factory

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Ensure the transport is ready.

        When no transport was injected, create one via *transport_factory*
        (if provided) or a real ``AiohttpBinanceTransport``.
        """
        if self._transport is not None:
            return
        if self._transport_factory is not None:
            self._transport = self._transport_factory()
            return
        # Lazy import so aiohttp is only loaded when actually needed.
        from src.exchanges.binance.aiohttp_transport import (  # pylint: disable=import-outside-toplevel
            AiohttpBinanceTransport,
        )
        self._transport = AiohttpBinanceTransport()

    async def close(self) -> None:
        """Release transport resources if the transport supports closing."""
        transport = self._transport
        if transport is not None and hasattr(transport, "close"):
            await transport.close()  # type: ignore[union-attr]
        self._transport = None

    # ------------------------------------------------------------------
    # HTTP verbs
    # ------------------------------------------------------------------

    async def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        """Send a signed GET request and return the JSON payload."""
        return await self._request("GET", path, params)

    async def post(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        """Send a signed POST request and return the JSON payload."""
        return await self._request("POST", path, params)

    async def delete(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        """Send a signed DELETE request and return the JSON payload."""
        return await self._request("DELETE", path, params)

    # ------------------------------------------------------------------
    # internal
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        params: Mapping[str, Any] | None,
    ) -> Any:
        """Build a signed request, send it, validate, and return the payload.

        Auto-starts the transport if not already initialised so that callers
        never need to call ``start()`` explicitly.
        """
        if self._transport is None:
            await self.start()
        if self._transport is None:
            raise RuntimeError(
                "BinancePrivateClient transport is not ready — call start() first"
            )

        timestamp_ms = None
        if self._timestamp_factory is not None:
            timestamp_ms = self._timestamp_factory()

        signed: BinanceSignedRequest = build_signed_request(
            method=method,
            path=path,
            params=dict(params or {}),
            api_key=self._api_key,
            api_secret=self._api_secret,
            base_url=self._base_url,
            recv_window=self._recv_window,
            timestamp_ms=timestamp_ms,
        )

        response: BinanceTransportResponse = await self._transport.send(signed)

        self._raise_for_error(response)
        return response.payload

    # ------------------------------------------------------------------
    # error handling
    # ------------------------------------------------------------------

    @staticmethod
    def _raise_for_error(response: BinanceTransportResponse) -> None:
        """Inspect transport response and raise a mapped ExchangeError on failure."""
        # HTTP-level errors
        if response.status_code >= 400:
            payload = (
                response.payload
                if isinstance(response.payload, dict)
                else {"message": str(response.payload)}
            )
            raise map_binance_error(status_code=response.status_code, payload=payload)

        # Binance application-level errors (code + msg in response body)
        if isinstance(response.payload, dict) and "code" in response.payload:
            code = response.payload.get("code")
            if code is not None and code < 0:
                raise map_binance_error(
                    status_code=response.status_code,
                    payload=response.payload,
                )


__all__ = ["BinancePrivateClient"]
