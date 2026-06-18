#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : runtime_adapter_factory.py
@Description: Exchange-agnostic runtime adapter factory.

This module is the SINGLE dispatch point that maps an exchange name to its
concrete runtime adapter factory.  It does NOT import any exchange-specific
concrete client class — it only imports the per-exchange ``runtime_adapter``
module.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.exchanges.models import ExchangeName
from src.exchanges.runtime_adapters import ExchangeRuntimeAdapters
from src.exchanges.runtime_config import ExchangeRuntimeConfig


def create_exchange_runtime_adapters(
    config: ExchangeRuntimeConfig,
    env: Mapping[str, str],
) -> ExchangeRuntimeAdapters:
    """Create exchange-specific runtime adapters for the configured exchange.

    Parameters
    ----------
    config:
        Canonical exchange runtime configuration.
    env:
        Environment variable mapping for exchange-specific settings.

    Returns
    -------
    ExchangeRuntimeAdapters
        A frozen bundle of exchange-specific adapter instances.

    Raises
    ------
    RuntimeError
        When the exchange is not supported for live trading.
    """
    if config.exchange == ExchangeName.OKX:
        from src.exchanges.okx.runtime_adapter import create_okx_runtime_adapters
        return create_okx_runtime_adapters(config, env)

    if config.exchange == ExchangeName.BINANCE:
        from src.exchanges.binance.runtime_adapter import create_binance_runtime_adapters
        return create_binance_runtime_adapters(config, env)

    raise RuntimeError(
        f"Unsupported exchange for live runtime: {config.exchange}"
    )
