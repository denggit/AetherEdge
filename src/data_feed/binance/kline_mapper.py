from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.data_feed.market_events import MarketCandleEvent
from src.exchanges.models import ExchangeName

DEFAULT_BINANCE_CANONICAL_SYMBOL = "ETH-USDT-PERP"
DEFAULT_BINANCE_RAW_SYMBOL = "ETHUSDT"


def map_binance_kline_event(
    payload: Mapping[str, Any],
    *,
    canonical_symbol: str = DEFAULT_BINANCE_CANONICAL_SYMBOL,
) -> MarketCandleEvent:
    kline = payload.get("k")
    if not isinstance(kline, Mapping):
        raise ValueError("Binance kline payload missing k")

    raw_symbol = str(payload.get("s") or kline.get("s") or DEFAULT_BINANCE_RAW_SYMBOL)
    timeframe = str(kline.get("i") or "")
    if not timeframe:
        raise ValueError("Binance kline payload missing k.i")

    return MarketCandleEvent(
        exchange=ExchangeName.BINANCE,
        canonical_symbol=canonical_symbol,
        raw_symbol=raw_symbol,
        timeframe=timeframe,
        open_time_ms=_int_from_mapping(kline, "t"),
        close_time_ms=_int_from_mapping(kline, "T"),
        open_price=_decimal_from_mapping(kline, "o"),
        high_price=_decimal_from_mapping(kline, "h"),
        low_price=_decimal_from_mapping(kline, "l"),
        close_price=_decimal_from_mapping(kline, "c"),
        volume=_decimal_from_mapping(kline, "v"),
        is_closed=_bool_from_mapping(kline, "x"),
        raw=dict(payload),
    )


def _decimal_from_mapping(payload: Mapping[str, Any], key: str) -> Decimal:
    value = payload.get(key)
    if value in {None, ""}:
        raise ValueError(f"Binance kline payload missing k.{key}")
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise ValueError(f"Binance kline payload has invalid decimal k.{key}={value!r}") from exc


def _int_from_mapping(payload: Mapping[str, Any], key: str) -> int:
    value = payload.get(key)
    if value in {None, ""}:
        raise ValueError(f"Binance kline payload missing k.{key}")
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"Binance kline payload has invalid int k.{key}={value!r}") from exc


def _bool_from_mapping(payload: Mapping[str, Any], key: str) -> bool:
    value = payload.get(key)
    if value is True:
        return True
    if value is False:
        return False
    raise ValueError(f"Binance kline payload has invalid bool k.{key}={value!r}")
