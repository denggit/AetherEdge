#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/14
@File       : market_data_client_port.py
@Description: Market data client port — the final exchange-facing market data interface.

This module defines the Protocol that every exchange market data adapter must satisfy.
It does NOT import any concrete exchange implementation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Awaitable, Callable, Mapping, Protocol


@dataclass(frozen=True)
class CandleSnapshot:
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    is_closed: bool
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketTradeSnapshot:
    event_time_ms: int
    price: Decimal
    qty: Decimal
    side: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)


MarketDataEvent = CandleSnapshot | MarketTradeSnapshot


class MarketDataClientPort(Protocol):
    async def fetch_recent_klines(
        self,
        *,
        limit: int,
    ) -> list[CandleSnapshot]:
        ...

    async def stream_market_events(
        self,
        on_event: Callable[[MarketDataEvent], Awaitable[None]],
    ) -> None:
        ...

    async def close(self) -> None:
        ...
