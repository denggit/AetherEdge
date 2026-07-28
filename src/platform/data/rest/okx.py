from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from src.platform.data.models import (
    MarketFullOrderBook,
    OrderBookLevel,
)
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.models import ExchangeConfig
from src.platform.exchanges.okx import OKX_BOOKS_FULL_ENDPOINT
from src.platform.exchanges.okx.public_rest import OkxPublicRestRequester
from src.platform.exchanges.ports import HttpClient
from src.platform.exchanges.symbols import to_exchange_symbol


_ENDPOINT = OKX_BOOKS_FULL_ENDPOINT


class OkxFullOrderBookError(RuntimeError):
    pass


class OkxFullOrderBookRestClient:
    def __init__(
        self,
        *,
        requester: OkxPublicRestRequester | None = None,
        config: ExchangeConfig | None = None,
        http_client: HttpClient | None = None,
    ) -> None:
        if requester is None:
            if http_client is None:
                raise TypeError(
                    "http_client is required when requester is not provided"
                )
            requester = OkxPublicRestRequester(
                config=config or ExchangeConfig(),
                http_client=http_client,
            )
        self._requester = requester

    async def fetch_full_order_book(
        self,
        *,
        symbol: str,
        depth: int = 5000,
    ) -> MarketFullOrderBook:
        _validate_depth(depth)
        raw_symbol = to_exchange_symbol(ExchangeName.OKX, symbol)
        payload = await self._requester.request(
            "GET",
            _ENDPOINT,
            params={"instId": raw_symbol, "sz": depth},
        )
        return _map_response(
            payload,
            symbol=symbol,
            raw_symbol=raw_symbol,
            depth=depth,
        )


def _validate_depth(depth: int) -> None:
    if type(depth) is not int or not 1 <= depth <= 5000:
        raise ValueError("full order book depth must be between 1 and 5000")


def _map_response(
    payload: Any,
    *,
    symbol: str,
    raw_symbol: str,
    depth: int,
) -> MarketFullOrderBook:
    if not isinstance(payload, Mapping):
        raise _response_error(
            symbol=symbol,
            raw_symbol=raw_symbol,
            depth=depth,
            code=None,
            message="response is not a mapping",
        )
    code = payload.get("code")
    message = payload.get("msg")
    if str(code) != "0":
        raise _response_error(
            symbol=symbol,
            raw_symbol=raw_symbol,
            depth=depth,
            code=code,
            message=message,
        )
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise _response_error(
            symbol=symbol,
            raw_symbol=raw_symbol,
            depth=depth,
            code=code,
            message="response data is empty",
        )
    row = data[0]
    if not isinstance(row, Mapping):
        raise _response_error(
            symbol=symbol,
            raw_symbol=raw_symbol,
            depth=depth,
            code=code,
            message="response row is invalid",
        )
    try:
        event_time_ms = _required_int(row.get("ts"), "ts")
        bids = _map_rest_levels(row.get("bids"), side="bids")
        asks = _map_rest_levels(row.get("asks"), side="asks")
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise _response_error(
            symbol=symbol,
            raw_symbol=raw_symbol,
            depth=depth,
            code=code,
            message=str(exc),
        ) from exc
    return MarketFullOrderBook(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=raw_symbol,
        bids=tuple(
            sorted(
                bids,
                key=lambda item: item.price,
                reverse=True,
            )[:depth]
        ),
        asks=tuple(
            sorted(asks, key=lambda item: item.price)[:depth]
        ),
        event_time_ms=event_time_ms,
        requested_depth=depth,
        raw={"code": code, "msg": message, "ts": event_time_ms},
    )


def _map_rest_levels(
    value: Any,
    *,
    side: str,
) -> tuple[OrderBookLevel, ...]:
    if not isinstance(value, list):
        raise ValueError(f"OKX full order book {side} must be a list")
    levels: list[OrderBookLevel] = []
    for row in value:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            raise ValueError(
                f"malformed OKX full order book {side} level: {row!r}"
            )
        levels.append(
            OrderBookLevel(
                price=Decimal(str(row[0])),
                quantity=Decimal(str(row[1])),
                order_count=_required_int(row[2], "order_count"),
            )
        )
    return tuple(levels)


def _required_int(value: Any, field: str) -> int:
    if (
        value is None
        or type(value) is bool
        or not isinstance(value, (int, str))
    ):
        raise ValueError(f"OKX full order book {field} is required")
    return int(value)


def _response_error(
    *,
    symbol: str,
    raw_symbol: str,
    depth: int,
    code: object,
    message: object,
) -> OkxFullOrderBookError:
    return OkxFullOrderBookError(
        "OKX full order book request failed | "
        "exchange=okx "
        f"symbol={symbol} "
        f"raw_symbol={raw_symbol} "
        f"endpoint={_ENDPOINT} "
        f"code={code} "
        f"msg={message} "
        f"depth={depth}"
    )


__all__ = [
    "OkxFullOrderBookError",
    "OkxFullOrderBookRestClient",
]
