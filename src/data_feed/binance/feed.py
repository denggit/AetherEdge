from __future__ import annotations

from typing import Any, Mapping

from src.data_feed.binance.agg_trade_mapper import map_binance_agg_trade_event
from src.data_feed.binance.kline_mapper import map_binance_kline_event
from src.data_feed.market_events import MarketCandleEvent, MarketTradeEvent

# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

BinanceMarketEvent = MarketTradeEvent | MarketCandleEvent

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_BINANCE_RAW_SYMBOL = "ETHUSDT"
DEFAULT_BINANCE_CANONICAL_SYMBOL = "ETH-USDT-PERP"
DEFAULT_BINANCE_KLINE_INTERVAL = "15m"

# ---------------------------------------------------------------------------
# Stream name helpers
# ---------------------------------------------------------------------------


def normalize_binance_stream_symbol(raw_symbol: str) -> str:
    value = str(raw_symbol or "").strip().lower()
    if not value:
        raise ValueError("raw_symbol must not be empty")
    return value


def binance_agg_trade_stream_name(raw_symbol: str = DEFAULT_BINANCE_RAW_SYMBOL) -> str:
    symbol = normalize_binance_stream_symbol(raw_symbol)
    return f"{symbol}@aggTrade"


def binance_kline_stream_name(
    raw_symbol: str = DEFAULT_BINANCE_RAW_SYMBOL,
    interval: str = DEFAULT_BINANCE_KLINE_INTERVAL,
) -> str:
    symbol = normalize_binance_stream_symbol(raw_symbol)
    interval_value = str(interval or "").strip()
    if not interval_value:
        raise ValueError("interval must not be empty")
    return f"{symbol}@kline_{interval_value}"


def binance_default_market_stream_names(
    *,
    raw_symbol: str = DEFAULT_BINANCE_RAW_SYMBOL,
    kline_interval: str = DEFAULT_BINANCE_KLINE_INTERVAL,
) -> tuple[str, str]:
    return (
        binance_agg_trade_stream_name(raw_symbol),
        binance_kline_stream_name(raw_symbol, kline_interval),
    )


# ---------------------------------------------------------------------------
# Payload dispatchers
# ---------------------------------------------------------------------------


def map_binance_market_event(
    payload: Mapping[str, Any],
    *,
    canonical_symbol: str = DEFAULT_BINANCE_CANONICAL_SYMBOL,
) -> BinanceMarketEvent:
    event_type = str(payload.get("e") or "")

    if event_type == "aggTrade":
        return map_binance_agg_trade_event(
            payload,
            canonical_symbol=canonical_symbol,
        )

    if event_type == "kline":
        return map_binance_kline_event(
            payload,
            canonical_symbol=canonical_symbol,
        )

    raise ValueError(f"Unsupported Binance market event type: {event_type!r}")


def try_map_binance_market_event(
    payload: Mapping[str, Any],
    *,
    canonical_symbol: str = DEFAULT_BINANCE_CANONICAL_SYMBOL,
) -> BinanceMarketEvent | None:
    try:
        return map_binance_market_event(
            payload,
            canonical_symbol=canonical_symbol,
        )
    except ValueError:
        return None
