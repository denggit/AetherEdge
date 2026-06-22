from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping
from urllib.parse import urlencode

from src.platform.exchanges.errors import ExchangeApiError, ExchangeConfigError, ExchangeMappingError
from src.platform.exchanges.models import (
    AmendOrderRequest,
    Balance,
    CancelOrderRequest,
    CancelStopOrderRequest,
    ExchangeConfig,
    ExchangeName,
    InstrumentRule,
    Kline,
    LeverageInfo,
    LeverageRequest,
    MarginMode,
    Order,
    OrderQuery,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionMode,
    PositionSide,
    StopMarketOrderRequest,
    StopOrderQuery,
    Ticker,
    Trade,
    TimeInForce,
    TriggerPriceType,
)
from src.platform.exchanges.ports import HttpClient
from src.platform.exchanges.symbols import to_exchange_symbol

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
        oldest_first: bool = False,
    ) -> list[Kline]:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        params: dict[str, Any] = {"instId": raw_symbol, "bar": _map_okx_interval(interval), "limit": limit}
        if start_time_ms is not None:
            params["after"] = start_time_ms
        if end_time_ms is not None:
            params["before"] = end_time_ms
        payload = await self._request_public("GET", "/api/v5/market/candles", params=params)
        rows = list(payload.get("data", []))
        if oldest_first:
            rows.reverse()
        return [_map_okx_kline(row, symbol=symbol, raw_symbol=raw_symbol, interval=interval) for row in rows]

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

    async def fetch_trades(
        self,
        symbol: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
        oldest_first: bool = True,
    ) -> list[Trade]:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        page_limit = min(max(int(limit or 100), 1), 100)
        max_pages = int(__import__("os").getenv("OKX_HISTORY_TRADES_MAX_PAGES", "200"))
        params: dict[str, Any] = {"instId": raw_symbol, "limit": page_limit}
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursor: str | None = None
        for _ in range(max_pages):
            page_params = dict(params)
            if cursor:
                page_params["after"] = cursor
            payload = await self._request_public("GET", "/api/v5/market/history-trades", params=page_params)
            data = list(payload.get("data", []))
            if not data:
                break
            new_rows = []
            for row in data:
                trade_id = str(row.get("tradeId") or "")
                if trade_id and trade_id in seen:
                    continue
                if trade_id:
                    seen.add(trade_id)
                ts = _optional_int(row.get("ts"))
                if ts is not None and end_time_ms is not None and ts > end_time_ms:
                    continue
                if ts is not None and start_time_ms is not None and ts < start_time_ms:
                    continue
                new_rows.append(row)
            rows.extend(new_rows)
            times = [_optional_int(row.get("ts")) for row in data]
            min_time = min((ts for ts in times if ts is not None), default=None)
            if start_time_ms is not None and min_time is not None and min_time < start_time_ms:
                break
            if len(rows) >= limit:
                break
            cursor = str(data[-1].get("tradeId") or "")
            if not cursor:
                break
        trades = [_map_okx_trade(row, symbol=symbol, raw_symbol=raw_symbol) for row in rows]
        trades.sort(key=lambda row: ((row.trade_time_ms or row.event_time_ms or 0), row.trade_id or ""), reverse=not oldest_first)
        return trades[:limit]

    async def fetch_instrument_rule(self, symbol: str) -> InstrumentRule:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_public(
            "GET",
            "/api/v5/public/instruments",
            params={"instType": "SWAP", "instId": raw_symbol},
        )
        data = _first_data(payload, "OKX instruments")
        return InstrumentRule(
            exchange=self.exchange,
            symbol=symbol,
            raw_symbol=raw_symbol,
            price_tick=_optional_decimal(data.get("tickSz")),
            quantity_step=_optional_decimal(data.get("lotSz")),
            min_quantity=_optional_decimal(data.get("minSz")),
            max_quantity=_optional_decimal(data.get("maxLmtSz") or data.get("maxMktSz")),
            contract_value=_optional_decimal(data.get("ctVal")),
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


    async def place_stop_market_order(self, request: StopMarketOrderRequest) -> Order:
        if request.close_position:
            raise ExchangeConfigError("OKX stop market order requires explicit quantity; close_position is not supported")
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        body: dict[str, Any] = {
            "instId": raw_symbol,
            "tdMode": _map_okx_margin_mode(request.margin_mode or self._config.default_margin_mode),
            "side": _map_okx_side(request.side),
            "ordType": "conditional",
            "sz": _decimal_to_str(request.quantity),
            "slTriggerPx": _decimal_to_str(request.trigger_price),
            "slOrdPx": "-1",
            "slTriggerPxType": _map_okx_trigger_price_type(request.trigger_price_type),
        }
        if request.client_order_id:
            body["algoClOrdId"] = request.client_order_id
        if request.reduce_only:
            body["reduceOnly"] = "true"
        if request.position_side is not None and request.position_side != PositionSide.BOTH:
            body["posSide"] = request.position_side.value

        payload = await self._request_private("POST", "/api/v5/trade/order-algo", json_body=body)
        data = _first_data(payload, "OKX place stop market order")
        status = OrderStatus.REJECTED if str(data.get("sCode", "0")) != "0" else OrderStatus.NEW
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=raw_symbol,
            order_id=_optional_str(data.get("algoId")),
            client_order_id=_optional_str(data.get("algoClOrdId")) or request.client_order_id,
            status=status,
            side=request.side,
            order_type=OrderType.MARKET,
            price=request.trigger_price,
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

    async def amend_order(self, request: AmendOrderRequest) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        body: dict[str, Any] = {"instId": raw_symbol}
        if request.order_id:
            body["ordId"] = request.order_id
        if request.client_order_id:
            body["clOrdId"] = request.client_order_id
        if request.new_quantity is not None:
            body["newSz"] = _decimal_to_str(request.new_quantity)
        if request.new_price is not None:
            body["newPx"] = _decimal_to_str(request.new_price)
        payload = await self._request_private("POST", "/api/v5/trade/amend-order", json_body=body)
        data = _first_data(payload, "OKX amend order")
        status = OrderStatus.REJECTED if str(data.get("sCode", "0")) != "0" else OrderStatus.NEW
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=raw_symbol,
            order_id=_optional_str(data.get("ordId")) or request.order_id,
            client_order_id=_optional_str(data.get("clOrdId")) or request.client_order_id,
            status=status,
            price=request.new_price,
            quantity=request.new_quantity,
            raw=data,
        )

    async def fetch_order_status(self, query: OrderQuery) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, query.symbol)
        params: dict[str, Any] = {"instId": raw_symbol}
        if query.order_id:
            params["ordId"] = query.order_id
        if query.client_order_id:
            params["clOrdId"] = query.client_order_id
        payload = await self._request_private("GET", "/api/v5/trade/order", params=params)
        data = _first_data(payload, "OKX order status")
        return _map_okx_order_row(data, symbol=query.symbol, raw_symbol=raw_symbol)

    async def fetch_open_orders(self, symbol: str) -> list[Order]:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_private("GET", "/api/v5/trade/orders-pending", params={"instId": raw_symbol})
        rows = payload.get("data", [])
        return [
            _map_okx_order_row(row, symbol=symbol, raw_symbol=raw_symbol)
            for row in rows
            if isinstance(row, Mapping)
        ]

    async def cancel_all_orders(self, symbol: str) -> list[Order]:
        orders = await self.fetch_open_orders(symbol)
        canceled: list[Order] = []
        for order in orders:
            if order.order_id or order.client_order_id:
                canceled.append(
                    await self.cancel_order(
                        CancelOrderRequest(
                            symbol=symbol,
                            order_id=order.order_id,
                            client_order_id=order.client_order_id,
                        )
                    )
                )
        return canceled

    async def fetch_stop_order_status(self, query: StopOrderQuery) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, query.symbol)
        params: dict[str, Any] = {"instId": raw_symbol, "ordType": "conditional"}
        if query.stop_order_id:
            params["algoId"] = query.stop_order_id
        if query.client_order_id:
            params["algoClOrdId"] = query.client_order_id
        payload = await self._request_private("GET", "/api/v5/trade/order-algo", params=params)
        data = _first_data(payload, "OKX stop order status")
        return _map_okx_algo_order_row(data, symbol=query.symbol, raw_symbol=raw_symbol)

    async def fetch_open_stop_orders(self, symbol: str) -> list[Order]:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_private(
            "GET",
            "/api/v5/trade/orders-algo-pending",
            params={"instType": "SWAP", "instId": raw_symbol, "ordType": "conditional"},
        )
        rows = payload.get("data", [])
        return [
            _map_okx_algo_order_row(row, symbol=symbol, raw_symbol=raw_symbol)
            for row in rows
            if isinstance(row, Mapping)
        ]

    async def cancel_stop_order(self, request: CancelStopOrderRequest) -> Order:
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        item: dict[str, Any] = {"instId": raw_symbol}
        if request.stop_order_id:
            item["algoId"] = request.stop_order_id
        if request.client_order_id:
            item["algoClOrdId"] = request.client_order_id
        payload = await self._request_private("POST", "/api/v5/trade/cancel-algos", json_body=[item])
        data = _first_data(payload, "OKX cancel stop order")
        status = OrderStatus.REJECTED if str(data.get("sCode", "0")) != "0" else OrderStatus.CANCELED
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=raw_symbol,
            order_id=_optional_str(data.get("algoId")) or request.stop_order_id,
            client_order_id=_optional_str(data.get("algoClOrdId")) or request.client_order_id,
            status=status,
            raw=data,
        )

    async def cancel_all_stop_orders(self, symbol: str) -> list[Order]:
        orders = await self.fetch_open_stop_orders(symbol)
        canceled: list[Order] = []
        for order in orders:
            if order.order_id or order.client_order_id:
                canceled.append(
                    await self.cancel_stop_order(
                        CancelStopOrderRequest(
                            symbol=symbol,
                            stop_order_id=order.order_id,
                            client_order_id=order.client_order_id,
                        )
                    )
                )
        return canceled

    async def fetch_leverage(self, symbol: str, *, margin_mode: MarginMode = MarginMode.CROSS) -> LeverageInfo:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        payload = await self._request_private(
            "GET",
            "/api/v5/account/leverage-info",
            params={"instId": raw_symbol, "mgnMode": _map_okx_margin_mode(margin_mode)},
        )
        data = _first_data(payload, "OKX leverage info")
        return LeverageInfo(
            exchange=self.exchange,
            symbol=symbol,
            raw_symbol=raw_symbol,
            leverage=_optional_decimal(data.get("lever")),
            margin_mode=margin_mode,
            position_side=_optional_position_side(data.get("posSide")),
            raw=data,
        )

    async def set_leverage(self, request: LeverageRequest) -> LeverageInfo:
        raw_symbol = to_exchange_symbol(self.exchange, request.symbol)
        body: dict[str, Any] = {
            "instId": raw_symbol,
            "lever": _decimal_to_str(request.leverage),
            "mgnMode": _map_okx_margin_mode(request.margin_mode),
        }
        if request.position_side is not None and request.position_side != PositionSide.BOTH:
            body["posSide"] = request.position_side.value
        payload = await self._request_private("POST", "/api/v5/account/set-leverage", json_body=body)
        data = _first_data(payload, "OKX set leverage")
        return LeverageInfo(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=raw_symbol,
            leverage=_optional_decimal(data.get("lever")) or request.leverage,
            margin_mode=request.margin_mode,
            position_side=request.position_side,
            raw=data,
        )

    async def set_margin_mode(self, symbol: str, margin_mode: MarginMode) -> Mapping[str, Any]:
        raw_symbol = to_exchange_symbol(self.exchange, symbol)
        return {
            "exchange": self.exchange.value,
            "symbol": symbol,
            "raw_symbol": raw_symbol,
            "margin_mode": margin_mode.value,
            "note": "OKX margin mode is supplied per order as tdMode; no global symbol margin-mode call is needed.",
        }

    async def fetch_position_mode(self) -> PositionMode:
        payload = await self._request_private("GET", "/api/v5/account/config")
        data = _first_data(payload, "OKX account config")
        return PositionMode.HEDGE if str(data.get("posMode", "")).lower() == "long_short_mode" else PositionMode.ONE_WAY

    async def set_position_mode(self, mode: PositionMode) -> PositionMode:
        pos_mode = "long_short_mode" if mode == PositionMode.HEDGE else "net_mode"
        await self._request_private("POST", "/api/v5/account/set-position-mode", json_body={"posMode": pos_mode})
        return mode

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
            "Content-Type": "application/json",
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



def _map_okx_interval(interval: str) -> str:
    value = str(interval).strip()
    upper_map = {"1h": "1H", "2h": "2H", "4h": "4H", "6h": "6H", "12h": "12H"}
    day_map = {"1d": "1D", "2d": "2D", "3d": "3D", "1w": "1W", "1mth": "1M"}
    lowered = value.lower()
    if lowered in upper_map:
        return upper_map[lowered]
    if lowered in day_map:
        return day_map[lowered]
    return value


def _map_okx_trade(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> Trade:
    side_value = str(row.get("side") or "").lower()
    side = OrderSide.BUY if side_value == "buy" else OrderSide.SELL if side_value == "sell" else None
    ts = _optional_int(row.get("ts"))
    return Trade(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=raw_symbol,
        price=_decimal(row.get("px"), field="px"),
        quantity=_decimal(row.get("sz"), field="sz"),
        side=side,
        trade_id=str(row.get("tradeId")) if row.get("tradeId") is not None else None,
        event_time_ms=ts,
        trade_time_ms=ts,
        raw=row,
    )

def _map_okx_kline(row: list[Any], *, symbol: str, raw_symbol: str, interval: str) -> Kline:
    if len(row) < 6:
        raise ExchangeMappingError("OKX kline row is too short", payload=row)
    open_time_ms = int(row[0])
    interval_ms = _okx_interval_to_ms(interval)
    return Kline(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=raw_symbol,
        interval=interval,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + interval_ms - 1,
        open=_decimal(row[1], field="open"),
        high=_decimal(row[2], field="high"),
        low=_decimal(row[3], field="low"),
        close=_decimal(row[4], field="close"),
        volume=_decimal(row[5], field="volume"),
        quote_volume=_decimal(row[7], field="quote_volume") if len(row) > 7 else None,
        is_closed=(str(row[8]) == "1") if len(row) > 8 else True,
        raw={"row": row},
    )


def _okx_interval_to_ms(interval: str) -> int:
    value = str(interval).strip().lower()
    units = (
        ("mth", 30 * 24 * 60 * 60_000),
        ("ms", 1),
        ("m", 60_000),
        ("h", 60 * 60_000),
        ("d", 24 * 60 * 60_000),
        ("w", 7 * 24 * 60 * 60_000),
    )
    for suffix, multiplier in units:
        if value.endswith(suffix):
            num = value[: -len(suffix)] or "1"
            return int(num) * multiplier
    raise ExchangeMappingError(f"Unsupported OKX kline interval={interval}", payload={"interval": interval})


def _map_okx_order_row(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=str(row.get("instId") or raw_symbol),
        order_id=_optional_str(row.get("ordId")),
        client_order_id=_optional_str(row.get("clOrdId")),
        status=_OKX_ORDER_STATUS.get(str(row.get("state", "")).lower(), OrderStatus.UNKNOWN),
        side=_optional_order_side(row.get("side")),
        order_type=_optional_order_type(row.get("ordType")),
        price=_optional_decimal(row.get("px")),
        quantity=_optional_decimal(row.get("sz")),
        filled_quantity=_optional_decimal(row.get("accFillSz")),
        raw=row,
    )



def _map_okx_algo_order_row(row: Mapping[str, Any], *, symbol: str, raw_symbol: str) -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=str(row.get("instId") or raw_symbol),
        order_id=_optional_str(row.get("algoId")),
        client_order_id=_optional_str(row.get("algoClOrdId")),
        status=_OKX_ORDER_STATUS.get(str(row.get("state", "")).lower(), OrderStatus.UNKNOWN),
        side=_optional_order_side(row.get("side")),
        order_type=OrderType.MARKET,
        price=_optional_decimal(row.get("slTriggerPx") or row.get("triggerPx")),
        quantity=_optional_decimal(row.get("sz")),
        filled_quantity=_optional_decimal(row.get("actualSz")),
        raw=row,
    )


def _optional_position_side(value: Any) -> PositionSide | None:
    text = str(value or "").lower()
    if text == "long":
        return PositionSide.LONG
    if text == "short":
        return PositionSide.SHORT
    if text in {"both", "net"}:
        return PositionSide.BOTH
    return None


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


def _map_okx_trigger_price_type(value: TriggerPriceType) -> str:
    return {
        TriggerPriceType.LAST: "last",
        TriggerPriceType.MARK: "mark",
        TriggerPriceType.INDEX: "index",
    }[value]


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise ExchangeMappingError(f"Invalid decimal for {field}: {value!r}") from exc


def _optional_order_side(value: Any) -> OrderSide | None:
    text = str(value or "").lower()
    if text == "buy":
        return OrderSide.BUY
    if text == "sell":
        return OrderSide.SELL
    return None


def _optional_order_type(value: Any) -> OrderType | None:
    text = str(value or "").lower()
    if text == "market":
        return OrderType.MARKET
    if text in {"limit", "post_only", "fok", "ioc"}:
        return OrderType.LIMIT
    return None


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
