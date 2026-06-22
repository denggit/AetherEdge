from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence

from src.market_data.models import TimeRange
from src.platform.data.models import MarketKline
from src.utils.log import get_logger

logger = get_logger(__name__)


class HistoricalKlineProvider(Protocol):
    """Port for fetching historical closed klines from an external source.

    Implementations translate canonical symbols to exchange-specific raw
    symbols internally and return only closed candles identified by the
    canonical symbol.  This port lives in the market-data domain so
    strategies and runtime code never depend on exchange adapters directly.
    """

    async def fetch_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_open_ms: int,
        end_open_ms: int,
    ) -> Sequence[MarketKline]:
        ...


@dataclass
class BackfillDiagnostics:
    """Structured diagnostics produced by a backfill attempt."""

    symbol: str
    raw_aliases: tuple[str, ...]
    interval: str
    start_open_ms: int
    end_open_ms: int
    start_open_utc: str
    end_open_utc: str
    records_loaded_before: int
    records_loaded_after: int
    min_records: int
    kline_store_class: str
    kline_store_path: str
    provider_used: str
    fetched_records: int
    saved_records: int
    success: bool
    error: str | None = None
