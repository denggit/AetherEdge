#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : credentials.py
@Description: OKX credential resolver — the ONLY place legacy OKX_* env var
              fallback is allowed in the codebase.

This module exists inside the OKX adapter layer because legacy OKX-specific
environment variables (OKX_API_KEY, OKX_SECRET_KEY, OKX_PASSPHRASE, etc.)
are an OKX adapter concern, not a concern for the generic runtime config layer.

Production config should use the unified EXCHANGE_API_* variables.
Legacy OKX_* fallback is retained ONLY for backward compatibility.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.exchanges.runtime_config import ExchangeRuntimeConfig


def resolve_okx_credentials(
    config: ExchangeRuntimeConfig,
    env: Mapping[str, str],
) -> tuple[str, str, str]:
    """Resolve OKX API credentials with unified priority and legacy fallback.

    Priority chain (first non-empty wins):
        api_key:      EXCHANGE_API_KEY         > OKX_API_KEY
        api_secret:   EXCHANGE_API_SECRET      > OKX_SECRET_KEY   > OKX_API_SECRET
        api_passphrase: EXCHANGE_API_PASSPHRASE > OKX_PASSPHASE   > OKX_PASSPHRASE

    Parameters
    ----------
    config:
        Canonical exchange runtime config.  Unified credentials are read
        from ``config.api_key`` / ``config.api_secret`` / ``config.api_passphrase``.
    env:
        Raw environment variable mapping for legacy OKX_* fallback.

    Returns
    -------
    tuple[str, str, str]
        ``(api_key, api_secret, api_passphrase)`` — each is guaranteed to be a
        string (may be empty if no credential is configured).
    """
    api_key = config.api_key or env.get("OKX_API_KEY", "")

    api_secret = (
        config.api_secret
        or env.get("OKX_SECRET_KEY", "")
        or env.get("OKX_API_SECRET", "")
    )

    api_passphrase = (
        config.api_passphrase
        or env.get("OKX_PASSPHASE", "")
        or env.get("OKX_PASSPHRASE", "")
    )

    return api_key, api_secret, api_passphrase
