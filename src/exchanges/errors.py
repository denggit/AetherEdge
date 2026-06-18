#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/12
@File       : errors.py
@Description: Exchange-agnostic error types.

No dependency on any specific exchange module (OKX / Binance / Bybit).
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping

from src.exchanges.models import ExchangeName


class ExchangeErrorKind(str, Enum):
    NETWORK_ERROR = "NETWORK_ERROR"
    AUTH_ERROR = "AUTH_ERROR"
    RATE_LIMITED = "RATE_LIMITED"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    INVALID_ORDER_SIZE = "INVALID_ORDER_SIZE"
    INVALID_PRICE = "INVALID_PRICE"
    ORDER_NOT_FOUND = "ORDER_NOT_FOUND"
    POSITION_NOT_FOUND = "POSITION_NOT_FOUND"
    REDUCE_ONLY_REJECTED = "REDUCE_ONLY_REJECTED"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
    EXCHANGE_REJECTED = "EXCHANGE_REJECTED"
    UNKNOWN = "UNKNOWN"


class ExchangeError(Exception):
    """A generic, exchange-agnostic error.

    Exchange-specific details are stored in ``raw`` so they never leak into
    business-logic control flow.
    """

    def __init__(
        self,
        *,
        exchange: ExchangeName,
        kind: ExchangeErrorKind,
        message: str,
        raw: Mapping[str, Any] | None = None,
    ) -> None:
        self.exchange = exchange
        self.kind = kind
        self.message = message
        self.raw: Mapping[str, Any] = raw if raw is not None else {}
        super().__init__(str(self))

    def __str__(self) -> str:
        return f"[{self.exchange.value}:{self.kind.value}] {self.message}"

    def __repr__(self) -> str:
        return (
            f"ExchangeError(exchange={self.exchange!r}, kind={self.kind!r}, "
            f"message={self.message!r})"
        )
