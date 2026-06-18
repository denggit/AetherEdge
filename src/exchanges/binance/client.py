#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : client.py
@Description: Binance broker adapter.

No-arg construction produces a shell that raises UNSUPPORTED_OPERATION on
every method — compatible with the contract established in earlier commits.

When api_key, api_secret, and an injected transport are supplied the client
builds signed REST requests, sends them through the transport, and maps the
responses back into the exchange-agnostic Broker* DTOs.
"""

from __future__ import annotations

from typing import Sequence

from src.exchanges.base import BrokerClient
from src.exchanges.binance.errors import binance_unsupported
from src.exchanges.binance.mapper import (
    BINANCE_ETH_USDT_SYMBOL,
    assert_binance_ethusdt_symbol,
    map_binance_error,
    map_binance_order,
    map_binance_position,
)
from src.exchanges.binance.request_mapper import broker_order_request_to_binance_params
from src.exchanges.binance.signing import (
    BINANCE_USDM_BASE_URL,
    BINANCE_USDM_OPEN_ORDERS_PATH,
    BINANCE_USDM_ORDER_PATH,
    BINANCE_USDM_POSITION_RISK_PATH,
    build_signed_request,
)
from src.exchanges.binance.transport import BinanceHttpTransport, BinanceTransportResponse
from src.exchanges.errors import ExchangeError, ExchangeErrorKind
from src.exchanges.models import (
    BrokerCancelResult,
    BrokerOrder,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerPosition,
    ExchangeName,
)


class BinanceBrokerClient(BrokerClient):
    """Binance broker adapter.

    No-arg construction produces a shell that raises UNSUPPORTED_OPERATION on
    every method.  Pass ``api_key``, ``api_secret``, and ``transport`` to
    enable real (or fake) request dispatching.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        transport: BinanceHttpTransport | None = None,
        base_url: str = BINANCE_USDM_BASE_URL,
        recv_window: int = 5000,
        position_mode: str = "net",
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._transport = transport
        self._base_url = base_url
        self._recv_window = recv_window
        self._position_mode = position_mode

    # ------------------------------------------------------------------
    # BrokerClient port
    # ------------------------------------------------------------------

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.BINANCE

    async def place_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        self._ensure_transport("place_order")

        params = broker_order_request_to_binance_params(
            request,
            position_mode=self._position_mode,
        )

        signed_request = build_signed_request(
            method="POST",
            path=BINANCE_USDM_ORDER_PATH,
            params=params,
            api_key=self._api_key or "",
            api_secret=self._api_secret or "",
            base_url=self._base_url,
            recv_window=self._recv_window,
        )

        response = await self._transport.send(signed_request)  # type: ignore[union-attr]
        self._raise_for_transport_response(response, operation="place_order")

        if not isinstance(response.payload, dict):
            raise map_binance_error(
                status_code=response.status_code,
                payload={"message": "Binance place_order payload is not a dict"},
            )

        order = map_binance_order(response.payload)

        return BrokerOrderResult(
            exchange=ExchangeName.BINANCE,
            symbol=order.symbol,
            ok=True,
            order_id=order.order_id,
            client_order_id=order.client_order_id,
            order=order,
            raw=response.payload,
        )

    async def cancel_order(self, symbol: str, order_id: str) -> BrokerCancelResult:
        self._ensure_transport("cancel_order")
        assert_binance_ethusdt_symbol(symbol)

        if not order_id:
            raise ValueError("order_id must not be empty")

        signed_request = build_signed_request(
            method="DELETE",
            path=BINANCE_USDM_ORDER_PATH,
            params={"symbol": symbol, "orderId": order_id},
            api_key=self._api_key or "",
            api_secret=self._api_secret or "",
            base_url=self._base_url,
            recv_window=self._recv_window,
        )

        response = await self._transport.send(signed_request)  # type: ignore[union-attr]
        self._raise_for_transport_response(response, operation="cancel_order")

        if not isinstance(response.payload, dict):
            raise map_binance_error(
                status_code=response.status_code,
                payload={"message": "Binance cancel_order payload is not a dict"},
            )

        return BrokerCancelResult(
            exchange=ExchangeName.BINANCE,
            symbol=symbol,
            ok=True,
            order_id=str(response.payload.get("orderId")) if response.payload.get("orderId") is not None else order_id,
            client_order_id=str(response.payload.get("clientOrderId")) if response.payload.get("clientOrderId") is not None else None,
            raw=response.payload,
        )

    async def fetch_open_orders(self, symbol: str) -> Sequence[BrokerOrder]:
        self._ensure_transport("fetch_open_orders")
        assert_binance_ethusdt_symbol(symbol)

        signed_request = build_signed_request(
            method="GET",
            path=BINANCE_USDM_OPEN_ORDERS_PATH,
            params={"symbol": symbol},
            api_key=self._api_key or "",
            api_secret=self._api_secret or "",
            base_url=self._base_url,
            recv_window=self._recv_window,
        )

        response = await self._transport.send(signed_request)  # type: ignore[union-attr]
        self._raise_for_transport_response(response, operation="fetch_open_orders")

        if not isinstance(response.payload, list):
            raise map_binance_error(
                status_code=response.status_code,
                payload={"message": "Binance fetch_open_orders payload is not a list"},
            )

        return [map_binance_order(item) for item in response.payload]

    async def fetch_position(self, symbol: str) -> BrokerPosition | None:
        self._ensure_transport("fetch_position")
        assert_binance_ethusdt_symbol(symbol)

        signed_request = build_signed_request(
            method="GET",
            path=BINANCE_USDM_POSITION_RISK_PATH,
            params={"symbol": symbol},
            api_key=self._api_key or "",
            api_secret=self._api_secret or "",
            base_url=self._base_url,
            recv_window=self._recv_window,
        )

        response = await self._transport.send(signed_request)  # type: ignore[union-attr]
        self._raise_for_transport_response(response, operation="fetch_position")

        if not isinstance(response.payload, list):
            raise map_binance_error(
                status_code=response.status_code,
                payload={"message": "Binance fetch_position payload is not a list"},
            )

        positions = [map_binance_position(item) for item in response.payload]
        active_positions = [pos for pos in positions if pos is not None]

        if not active_positions:
            return None

        if len(active_positions) > 1:
            raise ExchangeError(
                exchange=ExchangeName.BINANCE,
                kind=ExchangeErrorKind.UNSUPPORTED_OPERATION,
                message="fetch_position does not support simultaneous LONG and SHORT Binance hedge positions yet",
                raw={"positions": response.payload},
            )

        return active_positions[0]

    # ------------------------------------------------------------------
    # Transport readiness helpers
    # ------------------------------------------------------------------

    def _has_transport(self) -> bool:
        """Return True when all three transport ingredients are present."""
        return bool(self._api_key and self._api_secret and self._transport)

    def _ensure_transport(self, operation: str) -> None:
        """Raise UNSUPPORTED_OPERATION if the transport is not wired."""
        if not self._has_transport():
            raise binance_unsupported(operation)

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def _raise_for_transport_response(
        self,
        response: BinanceTransportResponse,
        *,
        operation: str,
    ) -> None:
        """Inspect transport response and raise a mapped ExchangeError on failure."""
        if response.status_code >= 400:
            payload = (
                response.payload
                if isinstance(response.payload, dict)
                else {"message": str(response.payload)}
            )
            raise map_binance_error(status_code=response.status_code, payload=payload)

        if isinstance(response.payload, dict) and "code" in response.payload and "msg" in response.payload:
            raise map_binance_error(status_code=response.status_code, payload=response.payload)


__all__ = ["BinanceBrokerClient"]
