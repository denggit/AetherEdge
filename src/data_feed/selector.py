from __future__ import annotations

from src.data_feed.base import MarketDataFeed
from src.data_feed.binance.adapter import BinanceMarketDataFeed
from src.data_feed.binance.websocket_feed import (
    BinanceWebSocketConnector,
    BinanceWebSocketMarketDataFeed,
)
from src.data_feed.okx.adapter import OkxMarketDataFeed
from src.exchanges.models import ExchangeName

# ---------------------------------------------------------------------------
# Supported runtime symbols / interval — locked to ETH-USDT perpetual
# ---------------------------------------------------------------------------

SUPPORTED_CANONICAL_SYMBOL = "ETH-USDT-PERP"
SUPPORTED_OKX_RAW_SYMBOL = "ETH-USDT-SWAP"
SUPPORTED_BINANCE_RAW_SYMBOL = "ETHUSDT"
SUPPORTED_KLINE_INTERVAL = "15m"


# ---------------------------------------------------------------------------
# Runtime guards
# ---------------------------------------------------------------------------


def _require_supported_canonical_symbol(canonical_symbol: str) -> None:
    if canonical_symbol != SUPPORTED_CANONICAL_SYMBOL:
        raise ValueError(
            f"Only {SUPPORTED_CANONICAL_SYMBOL} is supported by runtime data feed selector, "
            f"got {canonical_symbol!r}"
        )


def _resolve_okx_raw_symbol(raw_symbol: str | None) -> str:
    value = raw_symbol or SUPPORTED_OKX_RAW_SYMBOL
    if value != SUPPORTED_OKX_RAW_SYMBOL:
        raise ValueError(
            f"Only {SUPPORTED_OKX_RAW_SYMBOL} is supported for OKX data feed, "
            f"got {value!r}"
        )
    return value


def _resolve_binance_raw_symbol(raw_symbol: str | None) -> str:
    value = raw_symbol or SUPPORTED_BINANCE_RAW_SYMBOL
    if value != SUPPORTED_BINANCE_RAW_SYMBOL:
        raise ValueError(
            f"Only {SUPPORTED_BINANCE_RAW_SYMBOL} is supported for Binance data feed, "
            f"got {value!r}"
        )
    return value


def _resolve_binance_kline_interval(kline_interval: str) -> str:
    if kline_interval != SUPPORTED_KLINE_INTERVAL:
        raise ValueError(
            f"Only {SUPPORTED_KLINE_INTERVAL} kline interval is supported for Binance data feed, "
            f"got {kline_interval!r}"
        )
    return kline_interval


# ---------------------------------------------------------------------------
# Exchange name normalization
# ---------------------------------------------------------------------------


def normalize_exchange_name(exchange: str | ExchangeName | None) -> ExchangeName:
    if exchange is None:
        return ExchangeName.OKX
    if isinstance(exchange, ExchangeName):
        return exchange

    normalized = str(exchange).strip().lower()
    if normalized == ExchangeName.OKX.value:
        return ExchangeName.OKX
    if normalized == ExchangeName.BINANCE.value:
        return ExchangeName.BINANCE

    raise ValueError(f"Unsupported data feed exchange: {exchange!r}")


# ---------------------------------------------------------------------------
# Market data feed builder
# ---------------------------------------------------------------------------


def build_market_data_feed(
    *,
    exchange: str | ExchangeName | None = None,
    canonical_symbol: str = "ETH-USDT-PERP",
    raw_symbol: str | None = None,
    kline_interval: str = "15m",
    binance_ws_connector: BinanceWebSocketConnector | None = None,
    allow_binance_without_ws_connector: bool = False,
) -> MarketDataFeed:
    exchange_name = normalize_exchange_name(exchange)

    # Reject any non-ETH canonical symbol — only ETH-USDT-PERP is allowed.
    _require_supported_canonical_symbol(canonical_symbol)

    if exchange_name == ExchangeName.OKX:
        return OkxMarketDataFeed(
            canonical_symbol=SUPPORTED_CANONICAL_SYMBOL,
            raw_symbol=_resolve_okx_raw_symbol(raw_symbol),
        )

    if exchange_name == ExchangeName.BINANCE:
        resolved_raw_symbol = _resolve_binance_raw_symbol(raw_symbol)
        resolved_interval = _resolve_binance_kline_interval(kline_interval)

        if binance_ws_connector is not None:
            return BinanceWebSocketMarketDataFeed(
                connector=binance_ws_connector,
                canonical_symbol=SUPPORTED_CANONICAL_SYMBOL,
                raw_symbol=resolved_raw_symbol,
                kline_interval=resolved_interval,
            )

        if allow_binance_without_ws_connector:
            return BinanceMarketDataFeed(
                canonical_symbol=SUPPORTED_CANONICAL_SYMBOL,
                raw_symbol=resolved_raw_symbol,
                kline_interval=resolved_interval,
            )

        raise ValueError(
            "binance_ws_connector is required unless allow_binance_without_ws_connector=True"
        )

    raise ValueError(f"Unsupported data feed exchange: {exchange_name!r}")
