from __future__ import annotations

from decimal import Decimal
from typing import Any

from src.market_data.models import TradeDerivedFeatureCoverage
from src.market_data.trade_features.coverage_service import (
    TradeFeatureCoverageService,
)
from src.market_data.trade_features.coverage_repository import (
    TradeFeatureCoverageRepository,
)
from src.market_data.trade_features.okx_archive_calendar import (
    safe_okx_archive_end_ms,
)


class LegacyTradeFeatureCoverageAdapter:
    """Composition adapter for legacy callers expecting Store coverage methods."""

    def __init__(
        self,
        repository: TradeFeatureCoverageRepository,
    ) -> None:
        self._repository = repository

    def coverage_scan(
        self,
        *,
        symbol: str,
        exchange: str,
        required_minutes: int = 4320,
        current_day_archive_ready: bool = False,
        reference_end_ms: int | None = None,
        safe_archive_end_ms: int | None = None,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
        extra: dict[str, Any] | None = None,
    ) -> TradeDerivedFeatureCoverage:
        safe_end = (
            globals()["safe_okx_archive_end_ms"]()
            if safe_archive_end_ms is None
            else int(safe_archive_end_ms)
        )
        return TradeFeatureCoverageService(self._repository).scan_window(
            symbol=symbol,
            exchange=exchange,
            required_minutes=required_minutes,
            current_day_archive_ready=current_day_archive_ready,
            reference_end_ms=reference_end_ms,
            safe_archive_end_ms=safe_end,
            range_pct=range_pct,
            price_step=price_step,
            extra=extra,
        )

    def range_footprint_coverage_summary(
        self,
        *,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
    ) -> dict[str, object]:
        return TradeFeatureCoverageService(
            self._repository
        ).summarize_range_window(
            symbol=symbol,
            exchange=exchange,
            start_ms=start_ms,
            end_ms=end_ms,
            range_pct=range_pct,
            price_step=price_step,
        )

    def load_latest_range_footprint_context(self, **kwargs):
        """Expose the one repository query used by the legacy preflight."""

        return self._repository.load_latest_range_footprint_context(**kwargs)

    def _connect(self):
        """Retain the legacy preflight's private SQL compatibility boundary."""

        return self._repository._connect()


class CoverageRepositoryCompatibility:
    """Deprecated mixin retained only by the legacy Store facade."""

    def coverage_scan(self, **kwargs):
        return LegacyTradeFeatureCoverageAdapter(self).coverage_scan(**kwargs)

    def range_footprint_coverage_summary(self, **kwargs):
        return LegacyTradeFeatureCoverageAdapter(
            self
        ).range_footprint_coverage_summary(**kwargs)


__all__ = [
    "CoverageRepositoryCompatibility",
    "LegacyTradeFeatureCoverageAdapter",
]
