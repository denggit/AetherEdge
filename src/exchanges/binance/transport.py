#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : transport.py
@Description: Binance HTTP transport protocol and response DTO.

Defines the contract that BinanceBrokerClient expects from an injected
transport.  This module does NOT implement a real HTTP transport — it only
declares the interface and the response shape.

No real HTTP.  No env reads.  No live / Trader / factory wiring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol

from src.exchanges.binance.signing import BinanceSignedRequest


@dataclass(frozen=True)
class BinanceTransportResponse:
    """Normalised response returned by any BinanceHttpTransport implementation."""

    status_code: int
    payload: Any
    headers: Mapping[str, str] = field(default_factory=dict)


class BinanceHttpTransport(Protocol):
    """Structural interface for a Binance HTTP transport.

    The transport receives a fully-signed :class:`BinanceSignedRequest` and
    returns a :class:`BinanceTransportResponse`.  Callers must not assume the
    transport performs real I/O — it may be a fake / replay / stub for testing.
    """

    async def send(self, request: BinanceSignedRequest) -> BinanceTransportResponse:
        ...


__all__ = ["BinanceHttpTransport", "BinanceTransportResponse"]
