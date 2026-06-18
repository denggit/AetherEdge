from __future__ import annotations

from src.platform.exchanges.models import ExchangeName
from src.platform.markets.registry import DEFAULT_MARKET_SYMBOL, to_canonical_symbol, to_exchange_symbol

CANONICAL_ETH_USDT_PERP = DEFAULT_MARKET_SYMBOL
OKX_ETH_USDT_SWAP = to_exchange_symbol(ExchangeName.OKX, CANONICAL_ETH_USDT_PERP)
BINANCE_ETH_USDT_PERP = to_exchange_symbol(ExchangeName.BINANCE, CANONICAL_ETH_USDT_PERP)

__all__ = [
    "BINANCE_ETH_USDT_PERP",
    "CANONICAL_ETH_USDT_PERP",
    "OKX_ETH_USDT_SWAP",
    "to_canonical_symbol",
    "to_exchange_symbol",
]
