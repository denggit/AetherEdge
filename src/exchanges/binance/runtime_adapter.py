#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : runtime_adapter.py
@Description: Binance runtime adapter — the Binance-specific composition root.

This module is the ONLY place in the codebase that creates Binance concrete
instances (BinancePrivateClient, BinanceTradingClient, BinanceMarketDataClient)
and wires them into ExchangeRuntimeAdapters.

It is gated behind the Binance live preflight check — if the preflight does
not pass, this module raises immediately and never touches the network.

Business / live layers must NOT import or instantiate any Binance* class
directly — they go through the generic exchange adapter factory.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from src.data_feed.binance.market_data_client import BinanceMarketDataClient
from src.exchanges.binance.credentials import resolve_binance_credentials
from src.exchanges.binance.live_preflight import (
    build_binance_live_preflight_report,
    format_binance_live_blocked_message,
)
from src.exchanges.binance.private_client import BinancePrivateClient
from src.exchanges.binance.signing import BINANCE_USDM_BASE_URL
from src.exchanges.binance.trading_client import BinanceTradingClient
from src.exchanges.runtime_adapters import ExchangeRuntimeAdapters
from src.exchanges.runtime_config import ExchangeRuntimeConfig
from src.execution.trader import Trader, TraderRuntimeSettings


def create_binance_runtime_adapters(
    config: ExchangeRuntimeConfig,
    env: Mapping[str, str],
) -> ExchangeRuntimeAdapters:
    """Wire the Binance live runtime adapters from canonical config.

    Creates:
    - BinancePrivateClient (signed REST client)
    - BinanceTradingClient (TradingClientPort adapter)
    - BinanceMarketDataClient (MarketDataClientPort adapter)
    - Trader (execution facade) with injected runtime settings

    Parameters
    ----------
    config:
        Canonical exchange runtime configuration.  Credentials are resolved
        via :func:`resolve_binance_credentials`.
    env:
        Environment variable mapping for Binance-specific infrastructure
        settings (base URL, timeouts, recv window, etc.).

    Returns
    -------
    ExchangeRuntimeAdapters

    Raises
    ------
    RuntimeError
        When the Binance live preflight does not pass (missing / invalid env
        gates, SIGNAL_ONLY=true, etc.).
    ValueError
        When API credentials are incomplete.
    """
    # --- Gate: preflight must pass with orders_globally_enabled=True ---
    report = build_binance_live_preflight_report(
        env,
        orders_globally_enabled=True,
    )
    if not report.ok:
        raise RuntimeError(format_binance_live_blocked_message(report))

    # --- credential resolution ---
    api_key, api_secret = resolve_binance_credentials(config, env)

    # --- private REST client (owned by BinanceTradingClient) ---
    private_client = BinancePrivateClient(
        api_key=api_key,
        api_secret=api_secret,
        base_url=env.get("BINANCE_BASE_URL", BINANCE_USDM_BASE_URL),
        recv_window=int(env.get("BINANCE_RECV_WINDOW", "5000")),
    )

    # --- trading client (TradingClientPort adapter) ---
    trading_client = BinanceTradingClient(
        symbol=config.binance_symbol,
        margin_asset=config.quote_asset,
        api_key=api_key,
        api_secret=api_secret,
        leverage=config.leverage,
        margin_mode=config.margin_mode,
        position_mode=config.position_mode,
        private_client=private_client,
    )

    # --- market data client (MarketDataClientPort adapter, standalone) ---
    market_data_client = BinanceMarketDataClient(
        symbol=config.binance_symbol,
        interval=config.kline_interval,
        request_timeout_seconds=float(
            env.get("BINANCE_PUBLIC_REST_TIMEOUT_SECONDS", "10")
        ),
    )

    # --- trader (execution facade) ---
    trader = Trader(
        settings=TraderRuntimeSettings(
            symbol=config.binance_symbol,
            base_url=env.get("BINANCE_BASE_URL", BINANCE_USDM_BASE_URL),
            td_mode=config.margin_mode,
            pos_side_mode=config.position_mode,
            leverage=str(config.leverage),
            live_trading=True,
            max_live_equity_usdt=float(env.get("MAX_LIVE_EQUITY_USDT", "30")),
            # Binance quantity is in ETH, not contracts — multiplier is 1.
            symbol_allowlist=("ETHUSDT",),
            contract_multiplier=Decimal(
                env.get("BINANCE_CONTRACT_MULTIPLIER", "1")
            ),
            contract_precision=Decimal(
                env.get("BINANCE_CONTRACT_PRECISION", "0.001")
            ),
            min_contracts=Decimal(
                env.get("BINANCE_MIN_CONTRACTS", "0.001")
            ),
        )
    )
    trader.bind_trading_client(trading_client)

    return ExchangeRuntimeAdapters(
        market_data_client=market_data_client,
        trading_client=trading_client,
        trader=trader,
    )
