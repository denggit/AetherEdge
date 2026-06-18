from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping
from urllib.parse import urlencode

from src.exchanges.errors import ExchangeApiError, ExchangeConfigError, ExchangeMappingError
from src.exchanges.models import (
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
from src.exchanges.ports import HttpClient
from src.exchanges.symbols import to_exchange_symbol

OKX_PROD_REST_URL = "https://www.okx.com"
OKX_DEMO_REST_URL = "https://www.okx.com"

_OKX_ORDER_STATUS = {
    "live": OrderStatus.NEW,
    "partially_filled": OrderStatus.PARTIALLY_FILLED,
    "filled": OrderStatus.FILLED,
    "canceled": OrderStatus.CANCELED,
}


class OkxExchangeClient:
    """OKX USDⓈ perpetual adapter behind the unified ExchangeClient port."""

    def __init__(self, *, config: ExchangeConfig, http_client: HttpClient) -> None:
        self._config = config
        self._http = http_client
        self._base_url = OKX_DEMO_REST_URL if config.sandbox else OKX_PROD_REST_URL

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.OKX

    async def get_server_time_ms(self) -> int:
        payload = await self._request_public("GET", "/api/v5/public/time")
        data = _first_data(payload, "OKX server time")
        return int(data["ts"])

    async def fetch_klines(
        self,
        symbol: str,
        *,
        interval: str,
        limit: int = 100,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> list[Kline]:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        params: dict[str, Any] = {"instId": raw_symbol, "bar": interval, "limit": limit}
        if start_time_ms is not None:
            params["after"] = start_time_ms
        if end_time_ms is not None:
            params["before"] = end_time_ms
        payload = await self._request_public("GET", "/api/v5/market/candles", params=params)
        return [_map_okx_kline(row, symbol=symbol, raw_symbol=raw_symbol, interval=interval) for row in payload.get("data", [])]

    async def fetch_ticker(self, symbol: str) -> Ticker:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_public("GET", "/api/v5/market/ticker", params={"instId": raw_symbol})
        data = _first_data(payload, "OKX ticker")
        return Ticker(
            exchange=self.exchange,
            symbol=symbol,
            raw_symbol=raw_symbol,
            price=_decimal(data.get("last"), field="last"),
            time_ms=_optional_int(data.get("ts")),
            raw=data,
        )

    async def fetch_balance(self, asset: str = "USDT") -> Balance:
        payload = await self._request_private("GET", "/api/v5/account/balance", params={"ccy": asset})
        account = _first_data(payload, "OKX balance")
        details = account.get("details") or []
        selected = next((item for item in details if item.get("ccy") == asset), None)
        if selected is None:
            return Balance(exchange=self.exchange, asset=asset, total=Decimal("0"), available=Decimal("0"), raw=account)
        return Balance(
            exchange=self.exchange,
            asset=asset,
            total=_decimal(selected.get("cashBal", "0"), field="cashBal"),
            available=_decimal(selected.get("availBal", "0"), field="availBal"),
            raw=selected,
        )

    async def fetch_positions(self, symbol: str | None = None) -> list[Position]:
        params: dict[str, Any] = {}
        if symbol is not None:
            params["instId"] = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_private("GET", "/api/v5/account/positions", params=params)
        rows = payload.get("data", [])
        return [_map_okx_position(row, fallback_symbol=symbol) for row in rows]

    async def place_order(self, request: OrderRequest) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        body: dict[str, Any] = {
            "instId": raw_symbol,
            "tdMode": _map_okx_margin_mode(request.margin_mode or self._config.default_margin_mode),
            "side": _map_okx_side(request.side),
            "ordType": _map_okx_order_type(request.order_type, request.time_in_force),
            "sz": _decimal_to_str(request.quantity),
        }
        if request.price is not None:
            body["px"] = _decimal_to_str(request.price)
        if request.client_order_id:
            body["clOrdId"] = request.client_order_id
        if request.reduce_only:
            body["reduceOnly"] = "true"
        if request.position_side is not None and request.position_side != PositionSide.BOTH:
            body["posSide"] = request.position_side.value

        payload = await self._request_private("POST", "/api/v5/trade/order", json_body=body)
        data = _first_data(payload, "OKX place order")
        status = OrderStatus.REJECTED if str(data.get("sCode", "0")) != "0" else OrderStatus.NEW
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=raw_symbol,
            order_id=_optional_str(data.get("ordId")),
            client_order_id=_optional_str(data.get("clOrdId")) or request.client_order_id,
            status=status,
            side=request.side,
            order_type=request.order_type,
            price=request.price,
            quantity=request.quantity,
            raw=data,
        )

    async def cancel_order(self, request: CancelOrderRequest) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        body: dict[str, Any] = {"instId": raw_symbol}
        if request.order_id:
            body["ordId"] = request.order_id
        if request.client_order_id:
            body["clOrdId"] = request.client_order_id
        payload = await self._request_private("POST", "/api/v5/trade/cancel-order", json_body=body)
        data = _first_data(payload, "OKX cancel order")
        status = OrderStatus.REJECTED if str(data.get("sCode", "0")) != "0" else OrderStatus.CANCELED
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=raw_symbol,
            order_id=_optional_str(data.get("ordId")) or request.order_id,
            client_order_id=_optional_str(data.get("clOrdId")) or request.client_order_id,
            status=status,
            raw=data,
        )

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
            params=params,
            timeout_seconds=self._config.timeout_seconds,
        )

    async def _request_private(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> Any:
        self._require_credentials()
        method = method.upper()
        timestamp = _okx_timestamp()
        request_path = path
        if params:
            request_path = f"{path}?{urlencode({k: v for k, v in params.items() if v is not None})}"
        body_text = "" if json_body is None else json.dumps(json_body, separators=(",", ":"))
        sign_payload = f"{timestamp}{method}{request_path}{body_text}"
        signature = base64.b64encode(
            hmac.new(
                self._config.api_secret.encode("utf-8"),
                sign_payload.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")
        headers = {
            "OK-ACCESS-KEY": self._config.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._config.passphrase,
            **dict(self._config.extra_headers),
        }
        if self._config.sandbox:
            headers["x-simulated-trading"] = "1"
        return await self._http.request(
            method,
            f"{self._base_url}{path}",
            params=params,
            json_body=json_body,
            headers=headers,
            timeout_seconds=self._config.timeout_seconds,
        )

    def _require_credentials(self) -> None:
        if not self._config.api_key or not self._config.api_secret or not self._config.passphrase:
            raise ExchangeConfigError("OKX private API requires api_key, api_secret and passphrase")


def _okx_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _map_okx_kline(row: list[Any], *, symbol: str, raw_symbol: str, interval: str) -> Kline:
    if len(row) < 6:
        raise ExchangeMappingError("OKX kline row is too short", payload=row)
    open_time_ms = int(row[0])
    return Kline(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=raw_symbol,
        interval=interval,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms,
        open=_decimal(row[1], field="open"),
        high=_decimal(row[2], field="high"),
        low=_decimal(row[3], field="low"),
        close=_decimal(row[4], field="close"),
        volume=_decimal(row[5], field="volume"),
        quote_volume=_decimal(row[7], field="quote_volume") if len(row) > 7 else None,
        raw={"row": row},
    )


def _map_okx_position(row: Mapping[str, Any], *, fallback_symbol: str | None = None) -> Position:
    raw_symbol = str(row.get("instId") or "")
    symbol = fallback_symbol or "ETH-USDT-PERP"
    raw_side = str(row.get("posSide") or "both").lower()
    side = {
        "long": PositionSide.LONG,
        "short": PositionSide.SHORT,
        "net": PositionSide.BOTH,
        "both": PositionSide.BOTH,
    }.get(raw_side, PositionSide.BOTH)
    return Position(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=raw_symbol,
        side=side,
        quantity=_decimal(row.get("pos", "0"), field="pos"),
        entry_price=_optional_decimal(row.get("avgPx")),
        unrealized_pnl=_optional_decimal(row.get("upl")),
        leverage=_optional_decimal(row.get("lever")),
        raw=row,
    )


def _first_data(payload: Mapping[str, Any], context: str) -> Mapping[str, Any]:
    if str(payload.get("code", "0")) != "0":
        raise ExchangeApiError(f"{context} failed: {payload}", payload=payload)
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise ExchangeMappingError(f"{context} response has no data", payload=payload)
    first = data[0]
    if not isinstance(first, Mapping):
        raise ExchangeMappingError(f"{context} first data item is invalid", payload=payload)
    return first


def _map_okx_side(side: OrderSide) -> str:
    return "buy" if side == OrderSide.BUY else "sell"


def _map_okx_order_type(order_type: OrderType, tif: TimeInForce | None) -> str:
    if tif == TimeInForce.POST_ONLY:
        return "post_only"
    if tif == TimeInForce.FOK:
        return "fok"
    if tif == TimeInForce.IOC:
        return "ioc"
    if order_type == OrderType.MARKET:
        return "market"
    return "limit"


def _map_okx_margin_mode(mode: MarginMode) -> str:
    return "cross" if mode == MarginMode.CROSS else "isolated"


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


def _decimal_to_str(value: Decimal) -> str:
    return format(value.normalize(), "f")
