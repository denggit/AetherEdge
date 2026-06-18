#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : trading_client.py
@Description: Binance TradingClientPort adapter.

Implements the ``TradingClientPort`` protocol using ``BinancePrivateClient``
and the pure mappers in ``trading_mappers.py``.

No Trader dependency.  No OKX references.  No live / strategy / monitor
imports.  All Binance-specific logic is contained within
``src/exchanges/binance/*``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.exchanges.binance.private_client import BinancePrivateClient
from src.exchanges.binance.signing import (
    BINANCE_USDM_ALL_ORDERS_PATH,
    BINANCE_USDM_BALANCE_PATH,
    BINANCE_USDM_LEVERAGE_PATH,
    BINANCE_USDM_MARGIN_TYPE_PATH,
    BINANCE_USDM_OPEN_ORDERS_PATH,
    BINANCE_USDM_ORDER_PATH,
    BINANCE_USDM_POSITION_RISK_PATH,
)
from src.exchanges.binance.trading_mappers import (
    map_binance_algo_order_to_snapshot,
    map_binance_balance_to_snapshot,
    map_binance_order_to_snapshot,
    map_binance_order_to_status_snapshot,
    map_binance_position_to_snapshot,
)
from src.exchanges.errors import ExchangeError, ExchangeErrorKind
from src.execution.trading_client_port import (
    AlgoOrderSnapshot,
    BalanceSnapshot,
    CancelResult,
    OrderResult,
    OrderSnapshot,
    OrderStatusSnapshot,
    PositionSnapshot,
    TradingClientPort,
)

# ---------------------------------------------------------------------------
# Binance stop / conditional order types
# ---------------------------------------------------------------------------

_BINANCE_STOP_ORDER_TYPES = frozenset({"STOP_MARKET", "TAKE_PROFIT_MARKET"})

# ---------------------------------------------------------------------------
# Order placement error triage
# ---------------------------------------------------------------------------

# ExchangeError kinds that represent business-level order rejections and
# should be returned as OrderResult(ok=False) rather than re-raised.
_ORDER_REJECTION_KINDS: frozenset[ExchangeErrorKind] = frozenset({
    ExchangeErrorKind.INSUFFICIENT_BALANCE,
    ExchangeErrorKind.INVALID_ORDER_SIZE,
    ExchangeErrorKind.INVALID_PRICE,
    ExchangeErrorKind.REDUCE_ONLY_REJECTED,
    ExchangeErrorKind.EXCHANGE_REJECTED,
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decimal_to_str(value: Decimal) -> str:
    """Convert a Decimal to a string with no precision loss."""
    return format(value.normalize(), "f")


def _to_binance_order_side(port_side: str, reduce_only: bool) -> str:
    """Convert a TradingClientPort side to Binance BUY / SELL.

    Supports both ``LONG``/``SHORT`` and ``BUY``/``SELL`` input.
    """
    port_side = port_side.strip().upper()
    if port_side in ("BUY", "SELL"):
        return port_side
    if port_side == "LONG":
        return "SELL" if reduce_only else "BUY"
    if port_side == "SHORT":
        return "BUY" if reduce_only else "SELL"
    raise ValueError(f"Unrecognised side: {port_side!r}")


def _require_at_least_one_id(
    *,
    order_id: str | None,
    client_order_id: str | None,
    operation: str,
) -> None:
    """Raise ValueError when both *order_id* and *client_order_id* are empty."""
    if not order_id and not client_order_id:
        raise ValueError(
            f"{operation}: at least one of order_id or client_order_id is required"
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class BinanceTradingClient(TradingClientPort):
    """Binance USD-M Futures adapter implementing ``TradingClientPort``.

    Every method that touches the network delegates to the injected
    ``BinancePrivateClient``.  The client is fully testable with a fake
    transport — no real HTTP needs to reach Binance in tests.

    Parameters
    ----------
    symbol:
        Binance trading symbol, e.g. ``ETHUSDT``.
    margin_asset:
        Margin / quote asset, e.g. ``USDT``.
    api_key / api_secret:
        Binance API credentials.  Never logged.
    leverage:
        Target leverage (used by ``configure_instrument``).
    margin_mode:
        Currently only ``isolated`` is supported.
    position_mode:
        Currently only ``net`` (one-way) is supported.
    private_client:
        Optional pre-configured ``BinancePrivateClient`` for testing.
    """

    def __init__(
        self,
        *,
        symbol: str,
        margin_asset: str,
        api_key: str,
        api_secret: str,
        leverage: int,
        margin_mode: str = "isolated",
        position_mode: str = "net",
        private_client: BinancePrivateClient | None = None,
    ) -> None:
        if not symbol:
            raise ValueError("symbol must not be empty")
        if not margin_asset:
            raise ValueError("margin_asset must not be empty")
        if not api_key:
            raise ValueError("api_key must not be empty")
        if not api_secret:
            raise ValueError("api_secret must not be empty")
        if leverage <= 0:
            raise ValueError(f"leverage must be positive, got {leverage}")
        if margin_mode.lower() != "isolated":
            raise ValueError(
                f"Only isolated margin_mode is supported, got {margin_mode!r}"
            )
        if position_mode.lower() != "net":
            raise ValueError(
                f"Only net position_mode is supported, got {position_mode!r}"
            )

        self._symbol = symbol
        self._margin_asset = margin_asset
        self._leverage = leverage
        self._margin_mode = margin_mode
        self._position_mode = position_mode
        self._client = private_client or BinancePrivateClient(
            api_key=api_key,
            api_secret=api_secret,
        )

    # ------------------------------------------------------------------
    # TradingClientPort — configure_instrument
    # ------------------------------------------------------------------

    async def configure_instrument(self) -> None:
        """Set leverage and margin type on Binance.

        Idempotent: if Binance responds with "already set" errors the method
        does not raise.
        """
        await self._set_leverage()
        await self._set_margin_type()

    async def _set_leverage(self) -> None:
        try:
            await self._client.post(
                BINANCE_USDM_LEVERAGE_PATH,
                {
                    "symbol": self._symbol,
                    "leverage": self._leverage,
                },
            )
        except ExchangeError as exc:
            if self._is_already_set_error(exc):
                return
            raise

    async def _set_margin_type(self) -> None:
        try:
            await self._client.post(
                BINANCE_USDM_MARGIN_TYPE_PATH,
                {
                    "symbol": self._symbol,
                    "marginType": self._margin_mode.upper(),
                },
            )
        except ExchangeError as exc:
            if self._is_already_set_error(exc):
                return
            raise

    @staticmethod
    def _is_already_set_error(exc: ExchangeError) -> bool:
        """Return True when *exc* indicates the setting is already applied."""
        raw = exc.raw if isinstance(exc.raw, Mapping) else {}
        payload = (
            raw.get("payload", {})
            if isinstance(raw.get("payload"), Mapping)
            else {}
        )
        code = payload.get("code")
        msg = str(payload.get("msg", "")).lower()
        # -4046: margin type already set
        # -4047: leverage already set
        if code in (-4046, -4047):
            return True
        if "no need to change" in msg:
            return True
        return False

    # ------------------------------------------------------------------
    # TradingClientPort — fetch_balance
    # ------------------------------------------------------------------

    async def fetch_balance(self) -> BalanceSnapshot:
        payload = await self._client.get(BINANCE_USDM_BALANCE_PATH)

        if not isinstance(payload, list):
            raise ExchangeError(
                exchange="BINANCE",
                kind=ExchangeErrorKind.UNKNOWN,
                message="Binance balance response is not a list",
                raw={"payload": payload},
            )

        for entry in payload:
            if isinstance(entry, Mapping) and entry.get("asset") == self._margin_asset:
                return map_binance_balance_to_snapshot(
                    entry, margin_asset=self._margin_asset
                )

        # No entry for margin_asset → zero balance
        return BalanceSnapshot(
            asset=self._margin_asset,
            total=Decimal("0"),
            available=Decimal("0"),
            raw={"payload": payload},
        )

    # ------------------------------------------------------------------
    # TradingClientPort — fetch_position
    # ------------------------------------------------------------------

    async def fetch_position(self) -> PositionSnapshot:
        payload = await self._client.get(
            BINANCE_USDM_POSITION_RISK_PATH,
            {"symbol": self._symbol},
        )

        if not isinstance(payload, list):
            raise ExchangeError(
                exchange="BINANCE",
                kind=ExchangeErrorKind.UNKNOWN,
                message="Binance position response is not a list",
                raw={"payload": payload},
            )

        if not payload:
            return PositionSnapshot(
                side=None,
                qty=Decimal("0"),
                avg_entry_price=None,
                raw={},
            )

        for entry in payload:
            if isinstance(entry, Mapping) and entry.get("symbol") == self._symbol:
                return map_binance_position_to_snapshot(entry)

        return PositionSnapshot(
            side=None,
            qty=Decimal("0"),
            avg_entry_price=None,
            raw={"payload": payload},
        )

    # ------------------------------------------------------------------
    # TradingClientPort — fetch_open_orders
    # ------------------------------------------------------------------

    async def fetch_open_orders(self) -> list[OrderSnapshot]:
        payload = await self._client.get(
            BINANCE_USDM_OPEN_ORDERS_PATH,
            {"symbol": self._symbol},
        )

        if not isinstance(payload, list):
            raise ExchangeError(
                exchange="BINANCE",
                kind=ExchangeErrorKind.UNKNOWN,
                message="Binance open orders response is not a list",
                raw={"payload": payload},
            )

        return [map_binance_order_to_snapshot(item) for item in payload]

    # ------------------------------------------------------------------
    # TradingClientPort — fetch_order_status
    # ------------------------------------------------------------------

    async def fetch_order_status(
        self,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> OrderStatusSnapshot:
        _require_at_least_one_id(
            order_id=order_id,
            client_order_id=client_order_id,
            operation="fetch_order_status",
        )

        params: dict[str, Any] = {"symbol": self._symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id

        payload = await self._client.get(BINANCE_USDM_ORDER_PATH, params)

        if not isinstance(payload, dict):
            raise ExchangeError(
                exchange="BINANCE",
                kind=ExchangeErrorKind.UNKNOWN,
                message="Binance order status response is not a dict",
                raw={"payload": payload},
            )

        return map_binance_order_to_status_snapshot(payload)

    # ------------------------------------------------------------------
    # TradingClientPort — fetch_open_algo_orders
    # ------------------------------------------------------------------

    async def fetch_open_algo_orders(self) -> tuple[AlgoOrderSnapshot, ...]:
        payload = await self._client.get(
            BINANCE_USDM_OPEN_ORDERS_PATH,
            {"symbol": self._symbol},
        )

        if not isinstance(payload, list):
            raise ExchangeError(
                exchange="BINANCE",
                kind=ExchangeErrorKind.UNKNOWN,
                message="Binance open orders response is not a list",
                raw={"payload": payload},
            )

        algo_orders: list[AlgoOrderSnapshot] = []
        for item in payload:
            if not isinstance(item, Mapping):
                continue
            order_type = str(item.get("type", "")).upper()
            if order_type in _BINANCE_STOP_ORDER_TYPES:
                algo_orders.append(map_binance_algo_order_to_snapshot(item))

        return tuple(algo_orders)

    # ------------------------------------------------------------------
    # TradingClientPort — place_market_order
    # ------------------------------------------------------------------

    async def place_market_order(
        self,
        *,
        side: str,
        qty: Decimal,
        reduce_only: bool,
        client_order_id: str,
    ) -> OrderResult:
        if not client_order_id:
            raise ValueError("client_order_id must not be empty")

        binance_side = _to_binance_order_side(side, reduce_only=reduce_only)

        params: dict[str, Any] = {
            "symbol": self._symbol,
            "side": binance_side,
            "type": "MARKET",
            "quantity": _decimal_to_str(qty),
            "reduceOnly": "true" if reduce_only else "false",
            "newClientOrderId": client_order_id,
        }
        if self._position_mode != "net":
            raise ValueError(
                f"position_mode={self._position_mode!r} is not yet supported"
            )

        return await self._place_order(params, client_order_id)

    # ------------------------------------------------------------------
    # TradingClientPort — place_limit_order
    # ------------------------------------------------------------------

    async def place_limit_order(
        self,
        *,
        side: str,
        qty: Decimal,
        price: Decimal,
        reduce_only: bool,
        client_order_id: str,
    ) -> OrderResult:
        if not client_order_id:
            raise ValueError("client_order_id must not be empty")

        binance_side = _to_binance_order_side(side, reduce_only=reduce_only)

        params: dict[str, Any] = {
            "symbol": self._symbol,
            "side": binance_side,
            "type": "LIMIT",
            "quantity": _decimal_to_str(qty),
            "price": _decimal_to_str(price),
            "timeInForce": "GTC",
            "reduceOnly": "true" if reduce_only else "false",
            "newClientOrderId": client_order_id,
        }
        if self._position_mode != "net":
            raise ValueError(
                f"position_mode={self._position_mode!r} is not yet supported"
            )

        return await self._place_order(params, client_order_id)

    # ------------------------------------------------------------------
    # TradingClientPort — place_stop_market_order
    # ------------------------------------------------------------------

    async def place_stop_market_order(
        self,
        *,
        side: str,
        qty: Decimal | None,
        trigger_price: Decimal,
        reduce_only: bool,
        client_order_id: str,
    ) -> OrderResult:
        if not client_order_id:
            raise ValueError("client_order_id must not be empty")
        if qty is None:
            raise ValueError(
                "qty=None is not supported for Binance stop-market orders; "
                "Binance does not have a closePosition equivalent"
            )

        binance_side = _to_binance_order_side(side, reduce_only=reduce_only)

        params: dict[str, Any] = {
            "symbol": self._symbol,
            "side": binance_side,
            "type": "STOP_MARKET",
            "quantity": _decimal_to_str(qty),
            "stopPrice": _decimal_to_str(trigger_price),
            "reduceOnly": "true" if reduce_only else "false",
            "newClientOrderId": client_order_id,
        }
        if self._position_mode != "net":
            raise ValueError(
                f"position_mode={self._position_mode!r} is not yet supported"
            )

        return await self._place_order(params, client_order_id)

    # ------------------------------------------------------------------
    # Order placement shared helper
    # ------------------------------------------------------------------

    async def _place_order(
        self,
        params: dict[str, Any],
        client_order_id: str,
    ) -> OrderResult:
        try:
            payload = await self._client.post(BINANCE_USDM_ORDER_PATH, params)
        except ExchangeError as exc:
            # Only business-level rejections become OrderResult(ok=False).
            # Auth / network / rate-limit / unknown errors must propagate.
            if exc.kind not in _ORDER_REJECTION_KINDS:
                raise
            return OrderResult(
                ok=False,
                client_order_id=client_order_id,
                message=str(exc.message),
                raw=dict(exc.raw) if isinstance(exc.raw, Mapping) else {},
            )

        if not isinstance(payload, dict):
            return OrderResult(
                ok=False,
                client_order_id=client_order_id,
                message="Binance order response is not a dict",
                raw={"payload": payload},
            )

        order_id_raw = payload.get("orderId")
        order_id = str(order_id_raw) if order_id_raw is not None else None

        returned_cid_raw = payload.get("clientOrderId")
        returned_cid = (
            str(returned_cid_raw) if returned_cid_raw is not None else client_order_id
        )

        return OrderResult(
            ok=True,
            order_id=order_id,
            client_order_id=returned_cid,
            message="",
            raw=dict(payload),
        )

    # ------------------------------------------------------------------
    # TradingClientPort — cancel_order
    # ------------------------------------------------------------------

    async def cancel_order(
        self,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> CancelResult:
        _require_at_least_one_id(
            order_id=order_id,
            client_order_id=client_order_id,
            operation="cancel_order",
        )
        return await self._cancel(order_id=order_id, client_order_id=client_order_id)

    # ------------------------------------------------------------------
    # TradingClientPort — cancel_algo_order
    # ------------------------------------------------------------------

    async def cancel_algo_order(
        self,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> CancelResult:
        _require_at_least_one_id(
            order_id=order_id,
            client_order_id=client_order_id,
            operation="cancel_algo_order",
        )
        return await self._cancel(order_id=order_id, client_order_id=client_order_id)

    # ------------------------------------------------------------------
    # Cancel shared helper
    # ------------------------------------------------------------------

    async def _cancel(
        self,
        *,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> CancelResult:
        params: dict[str, Any] = {"symbol": self._symbol}
        if order_id:
            params["orderId"] = order_id
        if client_order_id:
            params["origClientOrderId"] = client_order_id

        try:
            payload = await self._client.delete(BINANCE_USDM_ORDER_PATH, params)
        except ExchangeError as exc:
            # ORDER_NOT_FOUND on cancel is a non-fatal result (consistent with OKX)
            if _is_order_not_found_error(exc):
                return CancelResult(
                    ok=False,
                    order_id=order_id,
                    client_order_id=client_order_id,
                    message=str(exc.message),
                    raw=dict(exc.raw) if isinstance(exc.raw, Mapping) else {},
                )
            raise

        if not isinstance(payload, dict):
            return CancelResult(
                ok=False,
                order_id=order_id,
                client_order_id=client_order_id,
                message="Binance cancel response is not a dict",
                raw={"payload": payload},
            )

        returned_order_id_raw = payload.get("orderId")
        returned_cid_raw = payload.get("clientOrderId")

        return CancelResult(
            ok=True,
            order_id=(
                str(returned_order_id_raw)
                if returned_order_id_raw is not None
                else order_id
            ),
            client_order_id=(
                str(returned_cid_raw)
                if returned_cid_raw is not None
                else client_order_id
            ),
            message="",
            raw=dict(payload),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_order_not_found_error(exc: ExchangeError) -> bool:
    """Return True when *exc* is an ORDER_NOT_FOUND error."""
    if exc.kind == ExchangeErrorKind.ORDER_NOT_FOUND:
        return True
    # Also check for -2011 in the raw payload
    raw = exc.raw if isinstance(exc.raw, Mapping) else {}
    payload = raw.get("payload", {}) if isinstance(raw.get("payload"), Mapping) else {}
    code = payload.get("code")
    return code == -2011


__all__ = ["BinanceTradingClient", "_to_binance_order_side"]
