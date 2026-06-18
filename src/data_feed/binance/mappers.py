#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/15
@File       : mappers.py
@Description: Binance-to-MarketDataClientPort mappers.

These functions convert Binance REST kline rows and WebSocket aggTrade
payloads into the unified DTOs defined in ``market_data_client_port.py``:

    * ``CandleSnapshot``
    * ``MarketTradeSnapshot``

They are the ONLY functions that know about Binance raw field names
(``p``, ``q``, ``m``, etc.) and the ONLY place where the ``m -> side``
convention lives.

None of these mappers import any strategy, monitor, or execution module.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping, Sequence

from src.data_feed.market_data_client_port import (
    CandleSnapshot,
    MarketTradeSnapshot,
)


# ---------------------------------------------------------------------------
# REST kline -> CandleSnapshot
# ---------------------------------------------------------------------------

# Binance REST /fapi/v1/klines array indices
_KLINE_OPEN_TIME = 0
_KLINE_OPEN = 1
_KLINE_HIGH = 2
_KLINE_LOW = 3
_KLINE_CLOSE = 4
_KLINE_VOLUME = 5
_KLINE_CLOSE_TIME = 6


def map_binance_rest_kline_to_candle_snapshot(
    raw_row: Sequence[Any],
    *,
    symbol: str,
    interval: str,
    now_ms: int | None = None,
) -> CandleSnapshot:
    """Map a single Binance REST kline row to a ``CandleSnapshot``.

    Parameters
    ----------
    raw_row:
        A single kline array from ``GET /fapi/v1/klines`` (length >= 7).
    symbol:
        The Binance raw symbol (e.g. ``"ETHUSDT"``), stored in ``raw``.
    interval:
        The kline interval (e.g. ``"15m"``), stored in ``raw``.
    now_ms:
        Current time in milliseconds for ``is_closed`` detection.
        Defaults to ``close_time_ms`` (treating the candle as closed).

    Returns
    -------
    CandleSnapshot

    Raises
    ------
    ValueError
        If *raw_row* is too short or has invalid numeric fields.
    """
    if len(raw_row) < 7:
        raise ValueError(
            f"Binance kline row too short: expected >= 7 elements, got {len(raw_row)}"
        )

    open_time_ms = int(raw_row[_KLINE_OPEN_TIME])
    close_time_ms = int(raw_row[_KLINE_CLOSE_TIME])

    if now_ms is None:
        now_ms = close_time_ms

    return CandleSnapshot(
        open_time_ms=open_time_ms,
        close_time_ms=close_time_ms,
        open_price=Decimal(str(raw_row[_KLINE_OPEN])),
        high_price=Decimal(str(raw_row[_KLINE_HIGH])),
        low_price=Decimal(str(raw_row[_KLINE_LOW])),
        close_price=Decimal(str(raw_row[_KLINE_CLOSE])),
        volume=Decimal(str(raw_row[_KLINE_VOLUME])),
        is_closed=close_time_ms <= now_ms,
        raw={
            "symbol": symbol,
            "interval": interval,
            "open_time_ms": open_time_ms,
            "close_time_ms": close_time_ms,
        },
    )


# ---------------------------------------------------------------------------
# WebSocket aggTrade -> MarketTradeSnapshot
# ---------------------------------------------------------------------------


def map_binance_agg_trade_to_market_trade_snapshot(
    payload: Mapping[str, Any],
) -> MarketTradeSnapshot:
    """Map a Binance aggTrade WebSocket event to a ``MarketTradeSnapshot``.

    The ``m`` (buyer-is-maker) field determines the taker side:

    * ``m=True``  → buyer is maker & seller is taker → ``side="SELL"``
    * ``m=False`` → buyer is taker                → ``side="BUY"``
    * ``m`` absent or ``None``                    → ``side=None``

    This interpretation lives **only** here — no strategy or monitor
    should depend on the ``m`` field.

    Parameters
    ----------
    payload:
        A raw Binance aggTrade WebSocket message (already unwrapped from
        the combined-stream envelope).

    Returns
    -------
    MarketTradeSnapshot

    Raises
    ------
    ValueError
        If required fields (``E``, ``p``, ``q``) are missing or invalid.
    """
    # -- event time ----------------------------------------------------------
    e_raw = payload.get("E")
    if e_raw in {None, ""}:
        raise ValueError("Binance aggTrade payload missing E")
    try:
        event_time_ms = int(e_raw)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Binance aggTrade payload has invalid int E={e_raw!r}"
        ) from exc

    # -- price ---------------------------------------------------------------
    p_raw = payload.get("p")
    if p_raw in {None, ""}:
        raise ValueError("Binance aggTrade payload missing p")
    try:
        price = Decimal(str(p_raw))
    except Exception as exc:
        raise ValueError(
            f"Binance aggTrade payload has invalid decimal p={p_raw!r}"
        ) from exc

    # -- quantity ------------------------------------------------------------
    q_raw = payload.get("q")
    if q_raw in {None, ""}:
        raise ValueError("Binance aggTrade payload missing q")
    try:
        qty = Decimal(str(q_raw))
    except Exception as exc:
        raise ValueError(
            f"Binance aggTrade payload has invalid decimal q={q_raw!r}"
        ) from exc

    # -- side from m ---------------------------------------------------------
    side: str | None = None
    m_val = payload.get("m")
    if m_val is True:
        side = "SELL"
    elif m_val is False:
        side = "BUY"

    return MarketTradeSnapshot(
        event_time_ms=event_time_ms,
        price=price,
        qty=qty,
        side=side,
        raw=dict(payload),
    )
