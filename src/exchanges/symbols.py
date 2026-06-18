#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : symbols.py
@Description: Canonical symbol mapper – maps ETH-USDT-PERP to exchange-specific
              raw symbols (OKX / Binance).

This module is exchange-agnostic at the interface level.  It does NOT import
any live, execution, strategy, or adapter code, and it does NOT read os.environ
or any configuration file.
"""

from __future__ import annotations

from src.exchanges.models import ExchangeName
from src.exchanges.runtime_config import ExchangeRuntimeConfig


SUPPORTED_CANONICAL_SYMBOL = "ETH-USDT-PERP"
OKX_ETH_USDT_PERPETUAL_SYMBOL = "ETH-USDT-SWAP"
BINANCE_ETH_USDT_PERPETUAL_SYMBOL = "ETHUSDT"


def assert_supported_canonical_symbol(canonical_symbol: str) -> None:
    if canonical_symbol != SUPPORTED_CANONICAL_SYMBOL:
        raise ValueError(f"Unsupported canonical symbol: {canonical_symbol}")


def raw_symbol_for_exchange(
    *,
    exchange: ExchangeName,
    canonical_symbol: str,
) -> str:
    assert_supported_canonical_symbol(canonical_symbol)

    if exchange == ExchangeName.OKX:
        return OKX_ETH_USDT_PERPETUAL_SYMBOL
    if exchange == ExchangeName.BINANCE:
        return BINANCE_ETH_USDT_PERPETUAL_SYMBOL

    raise ValueError(f"Unsupported exchange for canonical symbol mapping: {exchange.value}")


def raw_symbol_from_runtime_config(rt_cfg: ExchangeRuntimeConfig) -> str:
    return raw_symbol_for_exchange(
        exchange=rt_cfg.exchange,
        canonical_symbol=rt_cfg.canonical_symbol,
    )
