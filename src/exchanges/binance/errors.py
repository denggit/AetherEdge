#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Author     : Zijun Deng
@Date       : 2026/06/13
@File       : errors.py
@Description: Binance-specific error mapping – skeleton placeholder.

This module maps Binance error codes / HTTP statuses into
``ExchangeError`` instances.  For now only the UNSUPPORTED_OPERATION
helper is available; real error mapping will be added later.
"""

from __future__ import annotations

from src.exchanges.errors import ExchangeError, ExchangeErrorKind
from src.exchanges.models import ExchangeName


def binance_unsupported(operation: str) -> ExchangeError:
    return ExchangeError(
        exchange=ExchangeName.BINANCE,
        kind=ExchangeErrorKind.UNSUPPORTED_OPERATION,
        message=f"Binance adapter operation is not implemented yet: {operation}",
        raw={"operation": operation},
    )


__all__ = ["binance_unsupported"]
