from __future__ import annotations

from typing import Any, Mapping

from src.data_feed.base import MarketDataEvent
from src.data_feed.binance.feed import (
    DEFAULT_BINANCE_CANONICAL_SYMBOL,
    DEFAULT_BINANCE_KLINE_INTERVAL,
    DEFAULT_BINANCE_RAW_SYMBOL,
    binance_default_market_stream_names,
    try_map_binance_market_event,
)
from src.exchanges.models import ExchangeName


class BinanceMarketDataFeed:
    def __init__(
        self,
        *,
        canonical_symbol: str = DEFAULT_BINANCE_CANONICAL_SYMBOL,
        raw_symbol: str = DEFAULT_BINANCE_RAW_SYMBOL,
        kline_interval: str = DEFAULT_BINANCE_KLINE_INTERVAL,
    ) -> None:
        self._canonical_symbol = canonical_symbol
        self._raw_symbol = raw_symbol
        self._kline_interval = kline_interval

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.BINANCE

    @property
    def canonical_symbol(self) -> str:
        return self._canonical_symbol

    @property
    def raw_symbol(self) -> str:
        return self._raw_symbol

    def stream_names(self) -> tuple[str, ...]:
        return binance_default_market_stream_names(
            raw_symbol=self._raw_symbol,
            kline_interval=self._kline_interval,
        )

    def map_message(self, payload: Mapping[str, Any]) -> MarketDataEvent | None:
        return try_map_binance_market_event(
            payload,
            canonical_symbol=self._canonical_symbol,
        )
