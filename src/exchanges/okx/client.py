#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/12
@File       : client.py
@Description: OKX BrokerClient adapter.

This module wraps a trader-like object behind the generic BrokerClient port.
It does not instantiate Trader and is not wired into the live execution path.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.execution import order_specs
from src.exchanges.base import BrokerClient
from src.exchanges.errors import ExchangeError, ExchangeErrorKind
from src.exchanges.models import (
    BrokerCancelResult,
    BrokerOrder,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerOrderSide,
    BrokerOrderType,
    BrokerPosition,
    BrokerPositionSide,
    BrokerQuantityUnit,
    ExchangeName,
)
from src.exchanges.okx.mapper import (
    broker_order_from_okx_pending_algo_order,
    broker_order_from_okx_pending_order,
)


class OkxBrokerClient(BrokerClient):
    """OKX adapter for the generic broker port.

    The injected trader provides exchange-agnostic config fields
    (symbol, td_mode, pos_side_mode) and formatting helpers
    (decimal_to_str, price_to_str, extract_order_id, extract_algo_id).

    REST calls are issued through *private_client* directly — the
    broker client no longer tunnels them through Trader.request().
    """

    def __init__(
        self,
        trader: Any,
        *,
        private_client: Any,
        rate_limiter: Any | None = None,
    ) -> None:
        self._trader = trader
        self._client = private_client
        self._rate_limiter = rate_limiter

    async def _request(
        self,
        method: str,
        endpoint: str,
        payload: Any | None = None,
    ) -> dict[str, Any]:
        """Rate-limited REST call through the injected private client."""
        if method.upper() == "POST" and self._rate_limiter is not None:
            await self._rate_limiter.acquire()
        return await self._client.request(method, endpoint, payload)

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.OKX

    async def place_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        self._validate_request_basics(request)

        if request.order_type == BrokerOrderType.STOP_MARKET:
            return await self.place_protective_stop_order(request)

        side = _to_legacy_position_side(request.position_side)
        contracts_text = self._format_decimal(request.quantity)

        if request.order_type == BrokerOrderType.MARKET and not request.reduce_only:
            body = order_specs.build_market_entry_order_body(
                inst_id=request.symbol,
                td_mode=self._td_mode(),
                side=side,
                contracts_text=contracts_text,
                pos_side_mode=self._pos_side_mode(),
            )
            res = await self._request("POST", "/api/v5/trade/order", body)
            return self._order_result(request, res, order_id=self._extract_order_id(res))

        if request.order_type == BrokerOrderType.MARKET and request.reduce_only:
            body = order_specs.build_reduce_only_market_order_body(
                inst_id=request.symbol,
                td_mode=self._td_mode(),
                side=side,
                contracts_text=contracts_text,
                pos_side_mode=self._pos_side_mode(),
            )
            res = await self._request("POST", "/api/v5/trade/order", body)
            return self._order_result(request, res, order_id=self._extract_order_id(res))

        if request.order_type == BrokerOrderType.LIMIT and request.reduce_only:
            if request.price is None:
                raise _exchange_error(
                    ExchangeErrorKind.INVALID_PRICE,
                    "LIMIT reduce-only order requires price",
                )
            body = order_specs.build_reduce_only_tp_order_body(
                inst_id=request.symbol,
                td_mode=self._td_mode(),
                side=side,
                contracts_text=contracts_text,
                price_text=self._format_price(request.price),
                pos_side_mode=self._pos_side_mode(),
                client_order_id=request.client_order_id,
            )
            res = await self._request("POST", "/api/v5/trade/order", body)
            return self._order_result(request, res, order_id=self._extract_order_id(res))

        raise _exchange_error(
            ExchangeErrorKind.UNSUPPORTED_OPERATION,
            f"Unsupported OKX order mapping: {request.order_type.value}",
        )

    async def place_protective_stop_order(
        self,
        request: BrokerOrderRequest,
    ) -> BrokerOrderResult:
        self._validate_request_basics(request)
        if request.trigger_price is None:
            raise _exchange_error(
                ExchangeErrorKind.INVALID_PRICE,
                "STOP_MARKET order requires trigger_price",
            )
        if request.order_type != BrokerOrderType.STOP_MARKET:
            raise _exchange_error(
                ExchangeErrorKind.UNSUPPORTED_OPERATION,
                "protective stop requires STOP_MARKET order type",
            )

        body = order_specs.build_conditional_protective_sl_algo_body(
            inst_id=request.symbol,
            td_mode=self._td_mode(),
            side=_to_legacy_position_side(request.position_side),
            contracts_text=self._format_decimal(request.quantity),
            stop_price_text=self._format_price(request.trigger_price),
            pos_side_mode=self._pos_side_mode(),
        )
        res = await self._request("POST", "/api/v5/trade/order-algo", body)
        return self._order_result(request, res, order_id=self._extract_algo_id(res))

    async def cancel_order(self, symbol: str, order_id: str) -> BrokerCancelResult:
        res = await self._request(
            "POST",
            "/api/v5/trade/cancel-order",
            order_specs.build_cancel_order_body(inst_id=symbol, order_id=order_id),
        )
        return BrokerCancelResult(
            exchange=ExchangeName.OKX,
            symbol=symbol,
            ok=True,
            order_id=order_id,
            raw=res,
        )

    async def cancel_algo_order(
        self,
        symbol: str,
        algo_id: str,
    ) -> BrokerCancelResult:
        res = await self._request(
            "POST",
            "/api/v5/trade/cancel-algos",
            order_specs.build_cancel_algo_body(inst_id=symbol, algo_id=algo_id),
        )
        return BrokerCancelResult(
            exchange=ExchangeName.OKX,
            symbol=symbol,
            ok=True,
            order_id=algo_id,
            raw=res,
        )

    async def fetch_open_orders(self, symbol: str) -> Sequence[BrokerOrder]:
        raw = await self._request(
            "GET",
            f"/api/v5/trade/orders-pending?instId={symbol}",
        )
        raw_orders = _response_data(raw)

        return tuple(
            broker_order_from_okx_pending_order(item)
            for item in raw_orders
            if item.get("instId") == symbol
        )

    async def fetch_algo_orders(self, symbol: str) -> Sequence[BrokerOrder]:
        raw = await self._request(
            "GET",
            f"/api/v5/trade/orders-algo-pending?instId={symbol}&ordType=conditional",
        )
        raw_orders = _response_data(raw)

        return tuple(
            broker_order_from_okx_pending_algo_order(item)
            for item in raw_orders
            if item.get("instId") == symbol
        )

    async def fetch_position(self, symbol: str) -> BrokerPosition | None:
        fetch_position_snapshot = getattr(self._trader, "fetch_position_snapshot", None)
        if not callable(fetch_position_snapshot):
            raise _exchange_error(
                ExchangeErrorKind.UNSUPPORTED_OPERATION,
                "trader does not support fetch_position_snapshot",
            )

        snapshot = await fetch_position_snapshot()
        if snapshot is None:
            return None

        contracts = getattr(snapshot, "contracts", Decimal("0"))
        if not isinstance(contracts, Decimal):
            contracts = Decimal(str(contracts))

        side = getattr(snapshot, "side", None)
        if side is None or contracts <= 0:
            return None

        position_side = _broker_position_side_from_legacy(side)
        avg_entry = getattr(snapshot, "avg_entry_price", None)
        average_entry_price = (
            Decimal(str(avg_entry))
            if avg_entry is not None and str(avg_entry) != ""
            else None
        )

        return BrokerPosition(
            exchange=ExchangeName.OKX,
            symbol=symbol,
            position_side=position_side,
            quantity=contracts,
            quantity_unit=BrokerQuantityUnit.CONTRACTS,
            average_entry_price=average_entry_price,
            raw={"source": "legacy_position_snapshot"},
            metadata={"source": "legacy_position_snapshot"},
        )

    def _validate_request_basics(self, request: BrokerOrderRequest) -> None:
        if request.exchange != ExchangeName.OKX:
            raise _exchange_error(
                ExchangeErrorKind.UNSUPPORTED_OPERATION,
                f"OkxBrokerClient cannot place {request.exchange.value} orders",
            )
        if request.quantity_unit != BrokerQuantityUnit.CONTRACTS:
            raise _exchange_error(
                ExchangeErrorKind.UNSUPPORTED_OPERATION,
                "OkxBrokerClient expects quantity in contracts",
            )
        if request.quantity <= 0:
            raise _exchange_error(
                ExchangeErrorKind.INVALID_ORDER_SIZE,
                "Order quantity must be positive",
            )
        if request.side not in {
            BrokerOrderSide.BUY,
            BrokerOrderSide.SELL,
        }:
            raise _exchange_error(
                ExchangeErrorKind.UNSUPPORTED_OPERATION,
                f"Unsupported order side: {request.side.value}",
            )

    def _order_result(
        self,
        request: BrokerOrderRequest,
        raw: Mapping[str, Any],
        *,
        order_id: str,
    ) -> BrokerOrderResult:
        return BrokerOrderResult(
            exchange=ExchangeName.OKX,
            symbol=request.symbol,
            ok=True,
            order_id=order_id,
            client_order_id=request.client_order_id,
            raw=raw,
        )

    def _extract_order_id(self, res: Mapping[str, Any]) -> str:
        extractor = getattr(self._trader, "extract_order_id", None)
        if callable(extractor):
            try:
                order_id = extractor(res)
                if order_id:
                    return str(order_id)
            except Exception:
                pass
        return _extract_order_id(res)

    def _extract_algo_id(self, res: Mapping[str, Any]) -> str:
        extractor = getattr(self._trader, "extract_algo_id", None)
        if callable(extractor):
            try:
                algo_id = extractor(res)
                if algo_id:
                    return str(algo_id)
            except Exception:
                pass
        return _extract_algo_id(res)

    def _format_decimal(self, value: Decimal) -> str:
        formatter = getattr(self._trader, "decimal_to_str", None)
        if callable(formatter):
            return str(formatter(value))
        return _format_decimal(value)

    def _format_price(self, value: Decimal) -> str:
        formatter = getattr(self._trader, "price_to_str", None)
        if callable(formatter):
            return str(formatter(float(value)))
        return _format_decimal(value)

    def _td_mode(self) -> str:
        return str(getattr(self._trader, "td_mode"))

    def _pos_side_mode(self) -> str:
        return str(getattr(self._trader, "pos_side_mode"))


class OkxBrokerClientNotWired:
    """Compatibility placeholder retained for older skeleton-step imports."""

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.OKX

    def _not_wired(self) -> None:
        raise _exchange_error(
            ExchangeErrorKind.UNSUPPORTED_OPERATION,
            "OKX broker client is not wired into the live path.",
        )


def _to_legacy_position_side(side: BrokerPositionSide) -> str:
    if side == BrokerPositionSide.LONG:
        return "LONG"
    if side == BrokerPositionSide.SHORT:
        return "SHORT"
    raise _exchange_error(
        ExchangeErrorKind.UNSUPPORTED_OPERATION,
        f"Unsupported position side: {side.value}",
    )


def _broker_position_side_from_legacy(side: Any) -> BrokerPositionSide:
    if side == "LONG" or side == BrokerPositionSide.LONG:
        return BrokerPositionSide.LONG
    if side == "SHORT" or side == BrokerPositionSide.SHORT:
        return BrokerPositionSide.SHORT
    if side == "NET" or side == BrokerPositionSide.NET:
        return BrokerPositionSide.NET
    return BrokerPositionSide.UNKNOWN


def _format_decimal(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _extract_order_id(res: Mapping[str, Any]) -> str:
    data = res.get("data")
    if isinstance(data, Sequence) and data:
        item = data[0]
        if isinstance(item, Mapping) and item.get("ordId"):
            return str(item["ordId"])
    raise ExchangeError(
        exchange=ExchangeName.OKX,
        kind=ExchangeErrorKind.EXCHANGE_REJECTED,
        message="Missing OKX order id in response",
        raw=res,
    )


def _extract_algo_id(res: Mapping[str, Any]) -> str:
    data = res.get("data")
    if isinstance(data, Sequence) and data:
        item = data[0]
        if isinstance(item, Mapping):
            algo_id = item.get("algoId") or item.get("ordId")
            if algo_id:
                return str(algo_id)
    raise ExchangeError(
        exchange=ExchangeName.OKX,
        kind=ExchangeErrorKind.EXCHANGE_REJECTED,
        message="Missing OKX order id in response",
        raw=res,
    )


def _response_data(raw: Any) -> Sequence[Mapping[str, Any]]:
    if isinstance(raw, Mapping):
        data = raw.get("data")
        if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
            return tuple(item for item in data if isinstance(item, Mapping))
        return ()
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        return tuple(item for item in raw if isinstance(item, Mapping))
    return ()


def _exchange_error(
    kind: ExchangeErrorKind,
    message: str,
    raw: Mapping[str, Any] | None = None,
) -> ExchangeError:
    return ExchangeError(
        exchange=ExchangeName.OKX,
        kind=kind,
        message=message,
        raw=raw,
    )


__all__ = [
    "OkxBrokerClient",
    "OkxBrokerClientNotWired",
]
