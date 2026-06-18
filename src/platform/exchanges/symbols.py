from __future__ import annotations

from src.platform.exchanges.models import ExchangeName

CANONICAL_ETH_USDT_PERP = "ETH-USDT-PERP"
OKX_ETH_USDT_SWAP = "ETH-USDT-SWAP"
BINANCE_ETH_USDT_PERP = "ETHUSDT"

_RAW_SYMBOL_BY_EXCHANGE = {
    ExchangeName.OKX: {CANONICAL_ETH_USDT_PERP: OKX_ETH_USDT_SWAP},
    ExchangeName.BINANCE: {CANONICAL_ETH_USDT_PERP: BINANCE_ETH_USDT_PERP},
}

_CANONICAL_SYMBOL_BY_EXCHANGE = {
    exchange: {raw: canonical for canonical, raw in mapping.items()}
    for exchange, mapping in _RAW_SYMBOL_BY_EXCHANGE.items()
}


def to_exchange_symbol(exchange: ExchangeName, canonical_symbol: str) -> str:
    try:
        return _RAW_SYMBOL_BY_EXCHANGE[exchange][canonical_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported symbol mapping: exchange={exchange.value}, symbol={canonical_symbol!r}"
        ) from exc


def to_canonical_symbol(exchange: ExchangeName, raw_symbol: str) -> str:
    try:
        return _CANONICAL_SYMBOL_BY_EXCHANGE[exchange][raw_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported raw symbol mapping: exchange={exchange.value}, raw_symbol={raw_symbol!r}"
        ) from exc
