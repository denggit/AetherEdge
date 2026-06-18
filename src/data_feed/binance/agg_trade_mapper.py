from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.data_feed.market_events import MarketTradeEvent, MarketTradeSide
from src.exchanges.models import ExchangeName

BINANCE_AGG_TRADE_AGGREGATION_WINDOW_MS = 100
DEFAULT_BINANCE_CANONICAL_SYMBOL = "ETH-USDT-PERP"
DEFAULT_BINANCE_RAW_SYMBOL = "ETHUSDT"


def map_binance_agg_trade_event(
    payload: Mapping[str, Any],
    *,
    canonical_symbol: str = DEFAULT_BINANCE_CANONICAL_SYMBOL,
) -> MarketTradeEvent:
    raw_symbol = str(payload.get("s") or DEFAULT_BINANCE_RAW_SYMBOL)

    price = _decimal_from_payload(payload, "p")
    quantity = _decimal_from_payload(payload, "q")
    event_time_ms = _int_from_payload(payload, "E")
    trade_time_ms = _int_from_payload(payload, "T")
    trade_id = _optional_str(payload.get("a"))
    first_trade_id = _optional_str(payload.get("f"))
    last_trade_id = _optional_str(payload.get("l"))
    taker_side = _taker_side_from_buyer_market_maker(payload.get("m"))

    return MarketTradeEvent(
        exchange=ExchangeName.BINANCE,
        canonical_symbol=canonical_symbol,
        raw_symbol=raw_symbol,
        price=price,
        quantity=quantity,
        taker_side=taker_side,
        event_time_ms=event_time_ms,
        trade_time_ms=trade_time_ms,
        is_aggregated=True,
        aggregation_window_ms=BINANCE_AGG_TRADE_AGGREGATION_WINDOW_MS,
        trade_id=trade_id,
        first_trade_id=first_trade_id,
        last_trade_id=last_trade_id,
        raw=dict(payload),
    )


def _decimal_from_payload(payload: Mapping[str, Any], key: str) -> Decimal:
    value = payload.get(key)
    if value in {None, ""}:
        raise ValueError(f"Binance aggTrade payload missing {key}")
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"Binance aggTrade payload has invalid decimal {key}={value!r}") from exc


def _int_from_payload(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if value in {None, ""}:
        raise ValueError(f"Binance aggTrade payload missing {key}")
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"Binance aggTrade payload has invalid int {key}={value!r}") from exc


def _optional_str(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _taker_side_from_buyer_market_maker(value: Any) -> MarketTradeSide:
    if value is True:
        return MarketTradeSide.SELL
    if value is False:
        return MarketTradeSide.BUY
    raise ValueError(f"Binance aggTrade payload has invalid m={value!r}")
