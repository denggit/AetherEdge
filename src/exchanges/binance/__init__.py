#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : __init__.py
@Description: Binance exchange adapter package.

Real broker execution is deferred to a later task.
"""

from src.exchanges.binance.aiohttp_transport import AiohttpBinanceTransport
from src.exchanges.binance.client import BinanceBrokerClient
from src.exchanges.binance.credentials import resolve_binance_credentials
from src.exchanges.binance.mapper import (
    BINANCE_ETH_USDT_SYMBOL,
    assert_binance_ethusdt_symbol,
    map_binance_error,
    map_binance_order,
    map_binance_order_side,
    map_binance_order_status,
    map_binance_order_type,
    map_binance_position,
    map_binance_position_side,
)
from src.exchanges.binance.private_client import BinancePrivateClient
from src.exchanges.binance.request_mapper import (
    BINANCE_ETH_CONTRACT_SIZE_BASE,
    broker_order_request_to_binance_params,
    broker_order_side_to_binance,
    broker_order_type_to_binance,
    broker_position_side_to_binance,
    broker_quantity_to_binance_base_quantity,
)
from src.exchanges.binance.signing import (
    BINANCE_USDM_BASE_URL,
    BINANCE_USDM_TESTNET_BASE_URL,
    BINANCE_USDM_ORDER_PATH,
    BINANCE_USDM_OPEN_ORDERS_PATH,
    BINANCE_USDM_ALL_ORDERS_PATH,
    BINANCE_USDM_POSITION_RISK_PATH,
    BINANCE_USDM_BALANCE_PATH,
    BINANCE_USDM_LEVERAGE_PATH,
    BINANCE_USDM_MARGIN_TYPE_PATH,
    BINANCE_USDM_POSITION_MODE_PATH,
    BinanceSignedRequest,
    binance_api_key_headers,
    build_query_string,
    build_signed_params,
    build_signed_request,
    current_timestamp_ms,
    sign_query_string,
)
from src.exchanges.binance.trading_client import BinanceTradingClient
from src.exchanges.binance.trading_mappers import (
    map_binance_algo_order_to_snapshot,
    map_binance_balance_to_snapshot,
    map_binance_order_to_snapshot,
    map_binance_order_to_status_snapshot,
    map_binance_position_to_snapshot,
    map_binance_side_to_port_side,
    map_binance_status_to_port_status,
)
from src.exchanges.binance.transport import BinanceHttpTransport, BinanceTransportResponse

__all__ = [
    "AiohttpBinanceTransport",
    "BINANCE_ETH_CONTRACT_SIZE_BASE",
    "BINANCE_ETH_USDT_SYMBOL",
    "BINANCE_USDM_BASE_URL",
    "BINANCE_USDM_TESTNET_BASE_URL",
    "BINANCE_USDM_ORDER_PATH",
    "BINANCE_USDM_OPEN_ORDERS_PATH",
    "BINANCE_USDM_ALL_ORDERS_PATH",
    "BINANCE_USDM_POSITION_RISK_PATH",
    "BINANCE_USDM_BALANCE_PATH",
    "BINANCE_USDM_LEVERAGE_PATH",
    "BINANCE_USDM_MARGIN_TYPE_PATH",
    "BINANCE_USDM_POSITION_MODE_PATH",
    "BinanceBrokerClient",
    "BinanceHttpTransport",
    "BinancePrivateClient",
    "BinanceSignedRequest",
    "BinanceTradingClient",
    "BinanceTransportResponse",
    "assert_binance_ethusdt_symbol",
    "binance_api_key_headers",
    "broker_order_request_to_binance_params",
    "broker_order_side_to_binance",
    "broker_order_type_to_binance",
    "broker_position_side_to_binance",
    "broker_quantity_to_binance_base_quantity",
    "build_query_string",
    "build_signed_params",
    "build_signed_request",
    "current_timestamp_ms",
    "map_binance_algo_order_to_snapshot",
    "map_binance_balance_to_snapshot",
    "map_binance_error",
    "map_binance_order",
    "map_binance_order_side",
    "map_binance_order_status",
    "map_binance_order_to_snapshot",
    "map_binance_order_to_status_snapshot",
    "map_binance_order_type",
    "map_binance_position",
    "map_binance_position_side",
    "map_binance_position_to_snapshot",
    "map_binance_side_to_port_side",
    "map_binance_status_to_port_status",
    "resolve_binance_credentials",
    "sign_query_string",
]
