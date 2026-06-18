#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : runtime_adapter.py
@Description: OKX runtime adapter — the OKX-specific composition root.

This module is the ONLY place in the codebase that creates OKX concrete
instances (OkxPrivateClient, OkxTradingClient, OkxMarketDataClient,
OkxBrokerClient, OkxBrokerSemanticExecutor) and wires them into
ExchangeRuntimeAdapters.

Business / live layers must NOT import or instantiate any Okx* class
directly — they go through the generic exchange adapter factory.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.data_feed.okx_market_data_client import (
    OkxMarketDataClient,
    OkxMarketDataClientConfig,
)
from src.execution.okx_private_client import (
    OkxPrivateClient,
    OkxPrivateClientConfig,
    PrivateWriteRateLimiter,
)
from src.execution.okx_trading_client import OkxTradingClient
from src.execution.trader import Trader, TraderRuntimeSettings
from src.exchanges.okx.client import OkxBrokerClient
from src.exchanges.okx.credentials import resolve_okx_credentials
from src.exchanges.okx.semantic_executor import OkxBrokerSemanticExecutor
from src.exchanges.runtime_adapters import ExchangeRuntimeAdapters
from src.exchanges.runtime_config import ExchangeRuntimeConfig


def create_okx_runtime_adapters(
    config: ExchangeRuntimeConfig,
    env: Mapping[str, str],
) -> ExchangeRuntimeAdapters:
    """Wire the OKX live runtime adapters from canonical config.

    Creates:
    - OkxPrivateClient (private REST client with signing)
    - PrivateWriteRateLimiter (rate limiter for private writes)
    - Trader (execution facade) with injected runtime settings
    - OkxTradingClient (trading adapter, owns OkxPrivateClient)
    - OkxBrokerClient + OkxBrokerSemanticExecutor (legacy broker path)
    - OkxMarketDataClient (market data adapter, standalone)
    - Calls trader.bind_*() to wire all adapters

    Parameters
    ----------
    config:
        Canonical exchange runtime configuration.  Credentials are resolved
        via :func:`resolve_okx_credentials`.
    env:
        Environment variable mapping for OKX-specific infrastructure settings
        (base URL, timeouts, rate-limit, etc.).

    Returns
    -------
    ExchangeRuntimeAdapters

    Raises
    ------
    ValueError
        When API credentials are incomplete.
    """
    # --- credential resolution (with legacy OKX_* fallback) ---
    api_key, secret_key, passphrase = resolve_okx_credentials(config, env)
    if not api_key or not secret_key or not passphrase:
        raise ValueError(
            "OKX API config is incomplete. "
            "Check EXCHANGE_API_KEY/EXCHANGE_API_SECRET/EXCHANGE_API_PASSPHRASE "
            "or legacy OKX_API_KEY/OKX_SECRET_KEY/OKX_PASSPHASE."
        )

    # --- private REST client (owned by OkxTradingClient) ---
    private_client = OkxPrivateClient(
        OkxPrivateClientConfig(
            base_url=env.get("OKX_BASE_URL", "https://www.okx.com"),
            api_key=api_key,
            secret_key=secret_key,
            passphrase=passphrase,
            timeout_seconds=float(env.get("OKX_PRIVATE_REST_TIMEOUT_SECONDS", "10")),
        )
    )
    rate_limiter = PrivateWriteRateLimiter()

    # --- trader (execution facade) ---
    settings = TraderRuntimeSettings(
        symbol=config.okx_inst_id,
        base_url=env.get("OKX_BASE_URL", "https://www.okx.com"),
        td_mode=config.margin_mode,
        pos_side_mode=config.position_mode,
        leverage=str(config.leverage),
        live_trading=True,
        max_live_equity_usdt=float(env.get("MAX_LIVE_EQUITY_USDT", "30")),
    )
    trader = Trader(settings=settings)

    # --- trading client (owns private client) ---
    trading_client = OkxTradingClient(
        trader,
        private_client=private_client,
        rate_limiter=rate_limiter,
    )
    trader.bind_trading_client(trading_client)

    # --- broker semantic executor (legacy, behind feature flags) ---
    broker_client = OkxBrokerClient(
        trader,
        private_client=private_client,
        rate_limiter=rate_limiter,
    )
    broker_semantic_executor = OkxBrokerSemanticExecutor(broker_client)
    trader.bind_broker_semantic_executor(broker_semantic_executor)

    # --- market data client (standalone) ---
    market_data_config = OkxMarketDataClientConfig(
        inst_id=config.okx_inst_id,
        bar=config.kline_interval,
    )
    market_data_client = OkxMarketDataClient(market_data_config)

    return ExchangeRuntimeAdapters(
        market_data_client=market_data_client,
        trading_client=trading_client,
        trader=trader,
    )
