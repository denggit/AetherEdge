from __future__ import annotations

from typing import Any, Mapping

from src.data_feed.base import MarketDataEvent
from src.exchanges.models import ExchangeName

DEFAULT_OKX_CANONICAL_SYMBOL = "ETH-USDT-PERP"
DEFAULT_OKX_RAW_SYMBOL = "ETH-USDT-SWAP"


class OkxMarketDataFeed:
    def __init__(
        self,
        *,
        canonical_symbol: str = DEFAULT_OKX_CANONICAL_SYMBOL,
        raw_symbol: str = DEFAULT_OKX_RAW_SYMBOL,
    ) -> None:
        self._canonical_symbol = canonical_symbol
        self._raw_symbol = raw_symbol

    @property
    def exchange(self) -> ExchangeName:
        return ExchangeName.OKX

    @property
    def canonical_symbol(self) -> str:
        return self._canonical_symbol

    @property
    def raw_symbol(self) -> str:
        return self._raw_symbol

    def stream_names(self) -> tuple[str, ...]:
        # Existing OKX live code still owns actual websocket subscription details.
        # This wrapper intentionally returns an empty tuple until the old OKX feed
        # is wrapped in a later live integration step.
        return ()

    def map_message(self, payload: Mapping[str, Any]) -> MarketDataEvent | None:
        # Existing OKX live code still owns raw message parsing.
        # Keep this as a safe no-op shell for selector tests.
        return None
