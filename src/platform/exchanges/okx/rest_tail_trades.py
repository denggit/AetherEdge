from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Mapping
from urllib.parse import urlencode

from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from src.platform.markets import to_canonical_symbol


OKX_HISTORY_TRADES_URL = "https://www.okx.com/api/v5/market/history-trades"
_RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}


class OkxRestTailTradesError(RuntimeError):
    """Controlled failure from the OKX history-trades tail adapter."""


@dataclass(frozen=True)
class OkxRestTailTradesFetcher:
    """Small synchronous fetcher for short, fixed OKX history-trades gaps."""

    symbol: str | None = None
    limit: int = 100
    max_pages: int = 1_000
    sleep_seconds: float = 0.05
    timeout_seconds: float = 15.0
    max_retries: int = 3
    urlopen: Callable[..., object] | None = None

    def __call__(self, raw_symbol: str, start_time_ms: int, end_time_ms: int) -> list[MarketTrade]:
        return fetch_okx_history_trades_tail(
            raw_symbol=raw_symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            symbol=self.symbol,
            limit=self.limit,
            max_pages=self.max_pages,
            sleep_seconds=self.sleep_seconds,
            timeout_seconds=self.timeout_seconds,
            max_retries=self.max_retries,
            urlopen=self.urlopen,
        )


def fetch_okx_history_trades_tail(
    *,
    raw_symbol: str,
    start_time_ms: int,
    end_time_ms: int,
    symbol: str | None = None,
    limit: int = 100,
    max_pages: int = 1_000,
    sleep_seconds: float = 0.05,
    timeout_seconds: float = 15.0,
    max_retries: int = 3,
    urlopen: Callable[..., object] | None = None,
) -> list[MarketTrade]:
    """Fetch and normalize OKX `/api/v5/market/history-trades` for one gap.

    The endpoint paginates backward from recent trades, so this function is
    intentionally bounded by `max_pages` and should only be used for short tail
    gaps near the current trading day.
    """

    start = int(start_time_ms)
    end = int(end_time_ms)
    if end < start:
        return []

    opener = urlopen or urllib.request.urlopen
    page_limit = min(max(1, int(limit)), 100)
    pages = max(0, int(max_pages))
    canonical_symbol = symbol or _canonical_symbol(raw_symbol)
    rows: list[MarketTrade] = []
    seen: set[str] = set()
    cursor: str | None = None

    for page_index in range(pages):
        payload = _request_history_trades_page(
            opener=opener,
            raw_symbol=raw_symbol,
            limit=page_limit,
            cursor=cursor,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            sleep_seconds=sleep_seconds,
        )
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            break

        page_min_ts: int | None = None
        next_cursor = ""
        for item in data:
            if not isinstance(item, Mapping):
                continue
            ts = _optional_int(item.get("ts"))
            if ts is None:
                continue
            page_min_ts = ts if page_min_ts is None else min(page_min_ts, ts)
            next_cursor = str(item.get("tradeId") or next_cursor)
            if ts < start or ts > end:
                continue
            trade = _row_to_market_trade(item, symbol=canonical_symbol, raw_symbol=raw_symbol, ts_ms=ts)
            key = trade.trade_id or f"{trade.trade_time_ms}:{trade.price}:{trade.quantity}:{trade.side.value}"
            if key in seen:
                continue
            seen.add(key)
            rows.append(trade)

        if len(data) < page_limit:
            break
        if page_min_ts is not None and page_min_ts < start:
            break
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
        if page_index < pages - 1 and sleep_seconds > 0:
            time.sleep(float(sleep_seconds))

    rows.sort(key=lambda row: ((row.trade_time_ms or row.event_time_ms or 0), row.trade_id or ""))
    return rows


def _request_history_trades_page(
    *,
    opener: Callable[..., object],
    raw_symbol: str,
    limit: int,
    cursor: str | None,
    timeout_seconds: float,
    max_retries: int,
    sleep_seconds: float,
) -> Mapping[str, Any]:
    params: dict[str, object] = {"instId": raw_symbol, "limit": int(limit)}
    if cursor:
        params["after"] = cursor
    url = f"{OKX_HISTORY_TRADES_URL}?{urlencode(params)}"
    last_error = ""
    attempts = max(1, int(max_retries))
    for attempt in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AetherEdge/range-backfill"})
            with opener(req, timeout=float(timeout_seconds)) as response:  # type: ignore[attr-defined]
                body = response.read()
            payload = json.loads(body.decode("utf-8"))
            if str(payload.get("code", "0")) != "0":
                raise OkxRestTailTradesError(f"OKX history-trades returned code={payload.get('code')} msg={payload.get('msg')}")
            if not isinstance(payload, Mapping):
                raise OkxRestTailTradesError("OKX history-trades payload is not an object")
            return payload
        except urllib.error.HTTPError as exc:
            last_error = f"HTTP {exc.code}: {exc.reason}"
            if attempt >= attempts or int(exc.code) not in _RETRYABLE_HTTP_STATUS:
                raise OkxRestTailTradesError(last_error) from exc
        except OkxRestTailTradesError:
            raise
        except Exception as exc:  # noqa: BLE001 - converted to controlled tail error
            last_error = repr(exc)
            if attempt >= attempts:
                raise OkxRestTailTradesError(last_error) from exc
        if sleep_seconds > 0:
            time.sleep(min(float(sleep_seconds) * (2 ** max(0, attempt - 1)), 5.0))
    raise OkxRestTailTradesError(last_error or "OKX history-trades request failed")


def _row_to_market_trade(row: Mapping[str, Any], *, symbol: str, raw_symbol: str, ts_ms: int) -> MarketTrade:
    side_raw = str(row.get("side") or "").strip().lower()
    side = TradeSide.BUY if side_raw == "buy" else TradeSide.SELL if side_raw == "sell" else TradeSide.UNKNOWN
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol=symbol,
        raw_symbol=str(row.get("instId") or raw_symbol),
        price=Decimal(str(row.get("px"))),
        quantity=Decimal(str(row.get("sz"))),
        side=side,
        trade_id=str(row.get("tradeId")) if row.get("tradeId") not in (None, "") else None,
        event_time_ms=ts_ms,
        trade_time_ms=ts_ms,
        source=MarketDataSource.REST,
        raw=dict(row),
    )


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _canonical_symbol(raw_symbol: str) -> str:
    try:
        return to_canonical_symbol(ExchangeName.OKX, str(raw_symbol))
    except Exception:
        return str(raw_symbol)
