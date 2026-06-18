#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : credentials.py
@Description: Binance credential resolution.

Resolves Binance API key and secret from ``ExchangeRuntimeConfig`` and
optional environment overrides.  No other live wiring.
"""

from __future__ import annotations

from typing import Mapping

from src.exchanges.runtime_config import ExchangeRuntimeConfig


def resolve_binance_credentials(
    config: ExchangeRuntimeConfig,
    env: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    """Resolve ``(api_key, api_secret)`` for Binance.

    Priority (highest first)
    ------------------------
    api_key:
        1. ``config.api_key``
        2. ``env["BINANCE_API_KEY"]``
        3. ``env["EXCHANGE_API_KEY"]``

    api_secret:
        1. ``config.api_secret``
        2. ``env["BINANCE_API_SECRET"]``
        3. ``env["EXCHANGE_API_SECRET"]``
    """
    import os as _os

    values: Mapping[str, str] = _os.environ if env is None else env

    api_key = _first_non_empty(
        config.api_key,
        values.get("BINANCE_API_KEY", ""),
        values.get("EXCHANGE_API_KEY", ""),
    )
    api_secret = _first_non_empty(
        config.api_secret,
        values.get("BINANCE_API_SECRET", ""),
        values.get("EXCHANGE_API_SECRET", ""),
    )

    if not api_key:
        raise ValueError(
            "Binance API key not found; set EXCHANGE_API_KEY or BINANCE_API_KEY"
        )
    if not api_secret:
        raise ValueError(
            "Binance API secret not found; set EXCHANGE_API_SECRET or BINANCE_API_SECRET"
        )

    return api_key, api_secret


def _first_non_empty(*candidates: str) -> str:
    """Return the first non-empty string from *candidates*."""
    for candidate in candidates:
        if candidate.strip():
            return candidate.strip()
    return ""


__all__ = ["resolve_binance_credentials"]
