from __future__ import annotations

from enum import Enum


class ExchangeName(str, Enum):
    OKX = "okx"
    BINANCE = "binance"


__all__ = ["ExchangeName"]
