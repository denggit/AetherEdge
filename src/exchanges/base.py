#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/12
@File       : base.py
@Description: Low-level broker port (BrokerClient ABC).

This is the adapter port that every exchange adapter must implement.
It defines raw order/position operations — no semantic intent, no
strategy-level concepts.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from src.exchanges.errors import ExchangeError
from src.exchanges.models import (
    BrokerCancelResult,
    BrokerOrder,
    BrokerOrderRequest,
    BrokerOrderResult,
    BrokerPosition,
    ExchangeName,
)


class BrokerClient(ABC):
    """Low-level, exchange-agnostic broker interface.

    Implementations translate these generic calls into exchange-specific
    REST / WebSocket requests.  This port MUST NOT reference any strategy,
    trader, or live-worker module.
    """

    @property
    @abstractmethod
    def exchange(self) -> ExchangeName:
        """The exchange this client connects to."""
        ...

    @abstractmethod
    async def place_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        """Place a new order (market, limit, or conditional).

        Raises:
            ExchangeError: When the exchange rejects the request or a
                network/auth error occurs.
        """
        ...

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: str) -> BrokerCancelResult:
        """Cancel an existing order by its exchange-assigned ID.

        Raises:
            ExchangeError: When the cancellation fails.
        """
        ...

    @abstractmethod
    async def fetch_open_orders(self, symbol: str) -> Sequence[BrokerOrder]:
        """Return all currently open orders for *symbol*.

        Returns an empty sequence when there are no open orders.

        Raises:
            ExchangeError: When the exchange request fails.
        """
        ...

    @abstractmethod
    async def fetch_position(self, symbol: str) -> BrokerPosition | None:
        """Return the current position for *symbol*, or ``None``.

        Raises:
            ExchangeError: When the exchange request fails.
        """
        ...
