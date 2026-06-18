#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : runtime_adapters.py
@Description: Exchange-agnostic runtime adapters DTO.

This dataclass holds the exchange-specific adapter instances
(market data client, trading client, trader) returned by the
exchange adapter factory.

It belongs to the exchange adapter assembly layer, NOT the live layer.
It does NOT import any exchange-specific concrete class.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.data_feed.market_data_client_port import MarketDataClientPort
from src.execution.trading_client_port import TradingClientPort


@dataclass(frozen=True)
class ExchangeRuntimeAdapters:
    """Frozen bundle of exchange-specific adapter instances.

    Attributes:
        market_data_client: Exchange market data adapter (always present).
        trading_client: Exchange trading adapter, or ``None`` if the
            exchange does not support live order placement.
        trader: Live trader instance (execution facade), or ``None`` if
            the exchange does not support live trading.
    """

    market_data_client: MarketDataClientPort
    trading_client: TradingClientPort | None
    trader: Any | None
