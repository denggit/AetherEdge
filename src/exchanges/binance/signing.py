#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : signing.py
@Description: Binance USD-M Futures signed REST request builder.

Pure functions that build Binance signed requests and related artifacts.
No HTTP calls.  No API keys.  No live / Trader / factory wiring.

This module is the foundation for 16B injected transport client.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from time import time
from typing import Any, Mapping
from urllib.parse import urlencode

# ---------------------------------------------------------------------------
# Binance USD-M Futures endpoint constants
# ---------------------------------------------------------------------------

BINANCE_USDM_BASE_URL = "https://fapi.binance.com"
BINANCE_USDM_TESTNET_BASE_URL = "https://demo-fapi.binance.com"

BINANCE_USDM_ORDER_PATH = "/fapi/v1/order"
BINANCE_USDM_OPEN_ORDERS_PATH = "/fapi/v1/openOrders"
BINANCE_USDM_ALL_ORDERS_PATH = "/fapi/v1/allOrders"
BINANCE_USDM_POSITION_RISK_PATH = "/fapi/v2/positionRisk"
BINANCE_USDM_BALANCE_PATH = "/fapi/v2/balance"
BINANCE_USDM_LEVERAGE_PATH = "/fapi/v1/leverage"
BINANCE_USDM_MARGIN_TYPE_PATH = "/fapi/v1/marginType"
BINANCE_USDM_POSITION_MODE_PATH = "/fapi/v1/positionSide/dual"


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BinanceSignedRequest:
    """A fully-signed Binance REST request, ready for an HTTP transport.

    params, headers, and query_string are excluded from repr to avoid leaking
    signature or API key material into logs.
    """

    method: str
    path: str
    params: Mapping[str, Any] = field(repr=False)
    headers: Mapping[str, str] = field(repr=False)
    base_url: str = BINANCE_USDM_BASE_URL
    query_string: str = field(repr=False, default="")


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------


def build_query_string(params: Mapping[str, Any]) -> str:
    """Serialize *params* into a URL-encoded query string.

    ``None`` values are silently dropped.  Insertion order of the underlying
    dict is preserved so that downstream consumers (and tests) can rely on
    deterministic ordering.
    """
    clean_params: dict[str, Any] = {}
    for key, value in params.items():
        if value is None:
            continue
        clean_params[key] = value
    return urlencode(clean_params)


def sign_query_string(query_string: str, api_secret: str) -> str:
    """Return the HMAC-SHA256 hex digest of *query_string* keyed with *api_secret*."""
    return hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def current_timestamp_ms() -> int:
    """Return the current Unix epoch in milliseconds."""
    return int(time() * 1000)


# ---------------------------------------------------------------------------
# Signed parameter builders
# ---------------------------------------------------------------------------


def build_signed_params(
    params: Mapping[str, Any],
    *,
    api_secret: str,
    timestamp_ms: int | None = None,
    recv_window: int = 5000,
) -> dict[str, Any]:
    """Return a new dict with ``recvWindow``, ``timestamp``, and ``signature``
    added.

    *api_secret* must be non-empty.  The original *params* are never mutated.
    """
    if not api_secret:
        raise ValueError("api_secret must not be empty")

    signed_params = dict(params)
    signed_params["recvWindow"] = recv_window
    signed_params["timestamp"] = (
        current_timestamp_ms() if timestamp_ms is None else timestamp_ms
    )

    query_string = build_query_string(signed_params)
    signed_params["signature"] = sign_query_string(query_string, api_secret)

    return signed_params


def binance_api_key_headers(api_key: str) -> dict[str, str]:
    """Return headers containing the ``X-MBX-APIKEY`` header."""
    if not api_key:
        raise ValueError("api_key must not be empty")
    return {"X-MBX-APIKEY": api_key}


# ---------------------------------------------------------------------------
# Request builder
# ---------------------------------------------------------------------------


def build_signed_request(
    *,
    method: str,
    path: str,
    params: Mapping[str, Any],
    api_key: str,
    api_secret: str,
    base_url: str = BINANCE_USDM_BASE_URL,
    timestamp_ms: int | None = None,
    recv_window: int = 5000,
) -> BinanceSignedRequest:
    """Build a fully-signed :class:`BinanceSignedRequest`.

    The caller is responsible for supplying *api_key* and *api_secret* —
    this function never reads environment variables or config files.
    """
    signed_params = build_signed_params(
        params,
        api_secret=api_secret,
        timestamp_ms=timestamp_ms,
        recv_window=recv_window,
    )
    headers = binance_api_key_headers(api_key)
    query_string = build_query_string(signed_params)

    return BinanceSignedRequest(
        method=method.upper(),
        path=path,
        params=signed_params,
        headers=headers,
        base_url=base_url,
        query_string=query_string,
    )


__all__ = [
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
    "BinanceSignedRequest",
    "binance_api_key_headers",
    "build_query_string",
    "build_signed_params",
    "build_signed_request",
    "current_timestamp_ms",
    "sign_query_string",
]
