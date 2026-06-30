from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class HistoricalTradeFile:
    exchange: str
    symbol: str
    raw_symbol: str
    date: str
    path: Path
    downloaded: bool = False
