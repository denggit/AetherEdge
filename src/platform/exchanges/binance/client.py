from __future__ import annotations

import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any, Mapping
from urllib.parse import urlencode

from src.platform.exchanges.errors import ExchangeConfigError, ExchangeMappingError
from src.platform.exchanges.models import (
    Balance,
    CancelOrderRequest,
    ExchangeConfig,
    ExchangeName,
    Kline,
    MarginMode,
    Order,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionSide,
    Ticker,
    TimeInForce,
)
from src.platform.exchanges.ports import HttpClient
from src.platform.exchanges.symbols import to_exchange_symbol

BINANCE_PROD_REST_URL = "https://fapi.binance.com"
BINANCE_TESTNET_REST_URL = "https://demo-fapi.binance.com"

_BINANCE_ORDER_STATUS = {
    "NEW": OrderStatus.NEW,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.CANCELED,
}


class BinanceExchangeClient:
    """Binance USD-M futures adapter behind the unified ExchangeClient port."""

    def __init__(self, *, config: ExchangeConfig, http_client: HttpClient) -> None:
        self._config = config
        self._http = http_client
        self._base_url = BINANCE_TESTNET_REST_URL if config.sandbox else BINANCE_PROD_REST_URL

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.BINANCE

    async def get_server_time_ms(self) -> int:
        payload = await self._request_public("GET", "/fapi/v1/time")
        return int(payload["serverTime"])

    async def fetch_klines(
        self,
        symbol: str,
        *,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        oldest_first: bool = False,
    ) -> list[Kline]:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        params = {
            "symbol": raw_symbol,
            "interval": interval,
            "limit": limit,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
        }
        payload = await self._request_public("GET", "/fapi/v1/klines", params=params)
        return [_map_binance_kline(row, symbol=symbol, raw_symbol=raw_symbol, interval=interval) for row in payload]

    async def fetch_ticker(self, symbol: str) -> Ticker:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_public("GET", "/fapi/v1/ticker/price", params={"symbol": raw_symbol})
        return Ticker(
            exchange=self.exchange,
            symbol=symbol,
            raw_symbol=raw_symbol,
            price=_decimal(payload.get("price"), field="price"),
            time_ms=_optional_int(payload.get("time")),
            raw=payload,
        )

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        payload = await self._request_signed("GET", "/fapi/v3/balance")
        selected = next((item for item in payload if item.get("asset") == asset), None)
        if selected is None:
            return Balance(exchange=self.exchange, asset=asset, total=Decimal("0"), available=Decimal("0"), raw={})
        return Balance(
            exchange=self.exchange,
            asset=asset,
            total=_decimal(selected.get("balance", "0"), field="balance"),
            available=_decimal(selected.get("availableBalance", "0"), field="availableBalance"),
            raw=selected,
        )

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        params: dict[str, Any] = {}
        if symbol is not None:
            params["symbol"] = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_signed("GET", "/fapi/v3/positionRisk", params=params)
        return [_map_binance_position(row, fallback_symbol=symbol) for row in payload]

    async def place_order(self, request: OrderRequest) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        params: dict[str, Any] = {
            "symbol": raw_symbol,
            "side": _map_binance_side(request.side),
            "type": _map_binance_order_type(request.order_type),
            "quantity": _decimal_to_str(request.quantity),
        }
        if request.price is not None:
            params["price"] = _decimal_to_str(request.price)
        if request.client_order_id:
            params["newClientOrderId"] = request.client_order_id
        if request.reduce_only:
            params["reduceOnly"] = "true"
        if request.position_side is not None and request.position_side != PositionSide.BOTH:
            params["positionSide"] = request.position_side.value.upper()
        if request.time_in_force is not None:
            params["timeInForce"] = _map_binance_time_in_force(request.time_in_force)
        elif request.order_type == OrderType.LIMIT:
            params["timeInForce"] = "GTC"

        payload = await self._request_signed("POST", "/fapi/v1/order", params=params)
        return _map_binance_order(payload, symbol=request.symbol, raw_symbol=raw_symbol)

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        params: dict[str, Any] = {"symbol": raw_symbol}
        if request.order_id:
            params["orderId"] = request.order_id
        if request.client_order_id:
            params["origClientOrderId"] = request.client_order_id
        payload = await self._request_signed("DELETE", "/fapi/v1/order", params=params)
        return _map_binance_order(payload, symbol=request.symbol, raw_symbol=raw_symbol, fallback_status=OrderStatus.CANCELED)

    async def _request_public(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        return await self._http.request(
            method,
            f"{self._base_url}{path}",
            params=_clean_params(params),
            timeout_seconds=self._config.timeout_seconds,
        )

    async def _request_signed(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        self._require_credentials()
        signed_params: dict[str, Any] = dict(_clean_params(params))
        signed_params["recvWindow"] = self._config.recv_window_ms
        signed_params["timestamp"] = int(time.time() * 1000)
        query = urlencode(signed_params)
        signed_params["signature"] = hmac.new(
            self._config.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {"X-MBX-APIKEY": self._config.api_key, **dict(self._config.extra_headers)}
        return await self._http.request(
            method,
            f"{self._base_url}{path}",
            params=signed_params,
            headers=headers,
            timeout_seconds=self._config.timeout_seconds,
        )

    def _require_credentials(self) -> None:
        if not self._config.api_key or not self._config.api_secret:
            raise ExchangeConfigError("Binance private API requires api_key and api_secret")


def _map_binance_kline(row: list[Any], *, symbol: str, raw_symbol: str, interval: str) -> Kline:
    if len(row) < 8:
        raise ExchangeMappingError("Binance kline row is too short", payload=row)
    return Kline(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        raw_symbol=raw_symbol,
        interval=interval,
        open_time_ms=int(row[0]),
        close_time_ms=int(row[6]),
        open=_decimal(row[1], field="open"),
        high=_decimal(row[2], field="high"),
        low=_decimal(row[3], field="low"),
        close=_decimal(row[4], field="close"),
        volume=_decimal(row[5], field="volume"),
        quote_volume=_decimal(row[7], field="quote_volume"),
        is_closed=True,
        raw={"row": row},
    )


def _map_binance_position(row: Mapping[str, Any], *, fallback_symbol: str | None = None) -> Position:
    raw_symbol = str(row.get("symbol") or "")
    raw_side = str(row.get("positionSide") or "BOTH").upper()
    side = {
        "LONG": PositionSide.LONG,
        "SHORT": PositionSide.SHORT,
        "BOTH": PositionSide.BOTH,
    }.get(raw_side, PositionSide.BOTH)
    return Position(
        exchange=ExchangeName.BINANCE,
        symbol=fallback_symbol or "ETH-USDT-PERP",
        raw_symbol=raw_symbol,
        side=side,
        quantity=_decimal(row.get("positionAmt", "0"), field="positionAmt"),
        entry_price=_optional_decimal(row.get("entryPrice")),
        unrealized_pnl=_optional_decimal(row.get("unRealizedProfit")),
        leverage=_optional_decimal(row.get("leverage")),
        raw=row,
    )


def _map_binance_order(
    payload: Mapping[str, Any],
    *,
    symbol: str,
    raw_symbol: str,
    fallback_status: OrderStatus = OrderStatus.NEW,
) -> Order:
    return Order(
        exchange=ExchangeName.BINANCE,
        symbol=symbol,
        raw_symbol=raw_symbol,
        order_id=_optional_str(payload.get("orderId")),
        client_order_id=_optional_str(payload.get("clientOrderId")),
        status=_BINANCE_ORDER_STATUS.get(str(payload.get("status", "")).upper(), fallback_status),
        side=_optional_order_side(payload.get("side")),
        order_type=_optional_order_type(payload.get("type")),
        price=_optional_decimal(payload.get("price")),
        quantity=_optional_decimal(payload.get("origQty")),
        filled_quantity=_optional_decimal(payload.get("executedQty")),
        raw=payload,
    )


def _map_binance_side(side: OrderSide) -> str:
    return "BUY" if side == OrderSide.BUY else "SELL"


def _map_binance_order_type(order_type: OrderType) -> str:
    return "MARKET" if order_type == OrderType.MARKET else "LIMIT"


def _map_binance_time_in_force(tif: TimeInForce) -> str:
    return {
        TimeInForce.GTC: "GTC",
        TimeInForce.IOC: "IOC",
        TimeInForce.FOK: "FOK",
        TimeInForce.POST_ONLY: "GTX",
    }[tif]


def _clean_params(params: Mapping[str, Any] | None) -> dict[str, Any]:
    if not params:
        return {}
    return {key: value for key, value in params.items() if value is not None}


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise ExchangeMappingError(f"Invalid decimal for {field}: {value!r}") from exc


def _optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_order_side(value: Any) -> OrderSide | None:
    text = str(value or "").upper()
    if text == "BUY":
        return OrderSide.BUY
    if text == "SELL":
        return OrderSide.SELL
    return None


def _optional_order_type(value: Any) -> OrderType | None:
    text = str(value or "").upper()
    if text == "MARKET":
        return OrderType.MARKET
    if text == "LIMIT":
        return OrderType.LIMIT
    return None


def _decimal_to_str(value: Decimal) -> str:
    return format(value.normalize(), "f")
