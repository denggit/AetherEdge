from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.market_data.models import RangeBar, RangeBarAggregate
from src.platform.data.models import MarketKline
from src.runtime.persistence_service import RuntimePersistenceService


class RuntimeMarketDataPersistence:
    """Submit live market-data writes without owning runtime orchestration."""

    def __init__(
        self,
        *,
        persistence_service: RuntimePersistenceService,
        kline_store_provider: Callable[[], Any],
        range_bar_store_provider: Callable[[], Any],
        completed_aggregate_store_provider: Callable[[], Any],
        exchange: str,
        clock_ms: Callable[[], int],
    ) -> None:
        self._persistence_service = persistence_service
        self._kline_store_provider = kline_store_provider
        self._range_bar_store_provider = range_bar_store_provider
        self._completed_aggregate_store_provider = (
            completed_aggregate_store_provider
        )
        self._exchange = exchange
        self._clock_ms = clock_ms

    def persist_closed_kline(
        self,
        kline: MarketKline,
        *,
        on_error: Callable[[BaseException], None] | None,
        on_rejected: Callable[[str], None] | None = None,
    ) -> bool:
        description = "closed_kline"

        def write() -> None:
            repository = self._kline_store_provider()
            repository.save([kline])

        accepted = self._persistence_service.submit(
            description=description,
            write=write,
            on_error=on_error,
        )
        if not accepted and on_rejected is not None:
            on_rejected(description)
        return accepted

    def persist_range_bar(
        self,
        bar: RangeBar,
        *,
        on_error: Callable[[BaseException], None] | None,
        on_rejected: Callable[[str], None] | None = None,
    ) -> bool:
        description = "range_bar"

        def write() -> None:
            repository = self._range_bar_store_provider()
            repository.save([bar])

        accepted = self._persistence_service.submit(
            description=description,
            write=write,
            on_error=on_error,
        )
        if not accepted and on_rejected is not None:
            on_rejected(description)
        return accepted

    def persist_completed_range_aggregate(
        self,
        aggregate: RangeBarAggregate,
        *,
        coverage_status: str,
        missing_gap_ms: int,
        on_error: Callable[[BaseException], None] | None,
        on_rejected: Callable[[str], None] | None = None,
    ) -> bool:
        description = "completed_range_aggregate"

        def write() -> None:
            repository = self._completed_aggregate_store_provider()
            repository.save_completed_aggregate(
                exchange=self._exchange,
                aggregate=aggregate,
                coverage_status=coverage_status,
                missing_gap_ms=missing_gap_ms,
                completed_at_ms=self._clock_ms(),
            )

        accepted = self._persistence_service.submit(
            description=description,
            write=write,
            on_error=on_error,
        )
        if not accepted and on_rejected is not None:
            on_rejected(description)
        return accepted


__all__ = ["RuntimeMarketDataPersistence"]
