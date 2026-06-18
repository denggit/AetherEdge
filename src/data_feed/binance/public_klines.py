#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/14
@File       : public_klines.py
@Description: Binance public USD-M Futures Kline REST fetch and parse.

This module provides a minimal, zero-auth client for Binance's public
``GET /fapi/v1/klines`` endpoint.  It does **not**:

* import or use API keys, secrets, or passphrases
* import or use signed request builders
* import or use broker / execution / strategy / order modules
* support any symbol other than ``ETHUSDT``
* support any interval other than ``15m``
* support any exchange other than Binance USD-M Futures (testnet excluded)

It is deliberately narrow — its sole purpose is to seed the signal-only
candle buffer with historical closed klines before the WebSocket takes over.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Sequence

import aiohttp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BINANCE_USDM_PUBLIC_BASE_URL: str = "https://fapi.binance.com"
BINANCE_KLINES_PATH: str = "/fapi/v1/klines"
SUPPORTED_SYMBOL: str = "ETHUSDT"
SUPPORTED_INTERVAL: str = "15m"
MAX_KLINES_LIMIT: int = 1500

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BinancePublicKline:
    """A single parsed kline from the Binance public REST endpoint.

    All numeric fields are ``Decimal`` — no float conversion.
    """

    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal


# ---------------------------------------------------------------------------
# Kline array indices (Binance REST /fapi/v1/klines response format)
# ---------------------------------------------------------------------------

_KL_OPEN_TIME = 0
_KL_OPEN = 1
_KL_HIGH = 2
_KL_LOW = 3
_KL_CLOSE = 4
_KL_VOLUME = 5
_KL_CLOSE_TIME = 6


def _parse_single_kline(raw: Sequence[Any]) -> BinancePublicKline:
    """Parse a single kline array (length >= 12 expected by Binance)."""
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError(f"Expected a kline array, got {type(raw).__name__}")

    if len(raw) < 7:
        raise ValueError(
            f"Kline array too short: expected at least 7 elements, got {len(raw)}"
        )

    return BinancePublicKline(
        open_time_ms=int(raw[_KL_OPEN_TIME]),
        close_time_ms=int(raw[_KL_CLOSE_TIME]),
        open_price=Decimal(str(raw[_KL_OPEN])),
        high_price=Decimal(str(raw[_KL_HIGH])),
        low_price=Decimal(str(raw[_KL_LOW])),
        close_price=Decimal(str(raw[_KL_CLOSE])),
        volume=Decimal(str(raw[_KL_VOLUME])),
    )


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------


def parse_public_klines_payload(
    payload: Sequence[Sequence[Any]],
    *,
    now_ms: int | None = None,
    closed_only: bool = True,
) -> list[BinancePublicKline]:
    """Parse a raw Binance REST klines payload into a deduplicated, sorted list.

    Parameters
    ----------
    payload:
        The raw JSON array-of-arrays returned by ``GET /fapi/v1/klines``.
        Must be a non-empty list.
    now_ms:
        Current time in milliseconds.  Used to determine whether a kline
        is still open (unclosed).  Defaults to ``int(time.time() * 1000)``.
    closed_only:
        When ``True`` (default), filter out any kline whose
        ``close_time_ms > now_ms``, i.e. the current, still-forming candle.

    Returns
    -------
    list[BinancePublicKline]
        Parsed klines:

        * Sorted ascending by ``open_time_ms``.
        * Deduplicated by ``open_time_ms`` (last wins).
        * Filtered to only closed klines when *closed_only* is ``True``.

    Raises
    ------
    ValueError
        If *payload* is not a list, or any kline array is too short.
    """
    if not isinstance(payload, list):
        raise ValueError(
            f"Expected payload to be a list, got {type(payload).__name__!r}"
        )

    if now_ms is None:
        now_ms = int(time.time() * 1000)

    parsed: list[BinancePublicKline] = []
    seen: set[int] = set()

    for raw in payload:
        kline = _parse_single_kline(raw)

        # Filter unclosed candles
        if closed_only and kline.close_time_ms > now_ms:
            continue

        # Dedup by open_time_ms — keep the last occurrence
        if kline.open_time_ms in seen:
            # Replace the previous entry with the same open_time_ms
            for i, existing in enumerate(parsed):
                if existing.open_time_ms == kline.open_time_ms:
                    parsed[i] = kline
                    break
        else:
            seen.add(kline.open_time_ms)
            parsed.append(kline)

    # Sort ascending by open_time_ms
    parsed.sort(key=lambda k: k.open_time_ms)

    return parsed


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def _validate_fetch_args(*, symbol: str, interval: str, limit: int) -> None:
    """Validate the fetch arguments before making an HTTP request."""
    if symbol != SUPPORTED_SYMBOL:
        raise ValueError(
            f"Unsupported symbol: {symbol!r}. "
            f"Only {SUPPORTED_SYMBOL!r} is supported."
        )
    if interval != SUPPORTED_INTERVAL:
        raise ValueError(
            f"Unsupported interval: {interval!r}. "
            f"Only {SUPPORTED_INTERVAL!r} is supported."
        )
    if not isinstance(limit, int) or limit <= 0:
        raise ValueError(
            f"limit must be a positive integer, got {limit!r}"
        )
    if limit > MAX_KLINES_LIMIT:
        raise ValueError(
            f"limit must not exceed {MAX_KLINES_LIMIT}, got {limit}"
        )


async def fetch_public_klines(
    *,
    symbol: str,
    interval: str,
    limit: int,
    base_url: str = BINANCE_USDM_PUBLIC_BASE_URL,
    session_factory: Callable[..., aiohttp.ClientSession] | None = None,
) -> list[BinancePublicKline]:
    """Fetch historical klines from Binance public USD-M Futures REST.

    Parameters
    ----------
    symbol:
        Trading pair symbol, e.g. ``"ETHUSDT"``.  Only ``ETHUSDT`` is accepted.
    interval:
        Kline interval, e.g. ``"15m"``.  Only ``15m`` is accepted.
    limit:
        Number of klines to fetch (max 1500).
    base_url:
        Override the Binance USD-M Futures base URL (for testing).
    session_factory:
        Optional callable that returns an ``aiohttp.ClientSession``.
        Used for dependency injection in tests.

    Returns
    -------
    list[BinancePublicKline]
        Parsed, deduplicated, sorted, closed-only klines.

    Raises
    ------
    ValueError
        If *symbol*, *interval*, or *limit* are invalid, or the response
        body is not a valid klines array.
    RuntimeError
        If the HTTP response is not 2xx.
    """
    _validate_fetch_args(symbol=symbol, interval=interval, limit=limit)

    url = f"{base_url.rstrip('/')}{BINANCE_KLINES_PATH}"
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    _factory = session_factory or aiohttp.ClientSession

    async with _factory() as session:
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise RuntimeError(
                    f"Binance public klines request failed: "
                    f"HTTP {resp.status} for {url} — {body[:500]}"
                )

            payload = await resp.json()

    # Note: Binance REST returns an array that contains the last (possibly
    # unclosed) candle.  parse_public_klines_payload with closed_only=True
    # will filter it out.
    return parse_public_klines_payload(payload, closed_only=True)
