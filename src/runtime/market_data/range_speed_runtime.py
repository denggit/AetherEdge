from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.runtime.startup_catchup import StartupCatchupConfig
from src.strategy.ports import RangeSpeedHistoryProvider
from src.utils.log import get_logger


logger = get_logger(__name__)
ProviderFactory = Callable[[], RangeSpeedHistoryProvider | None]
StoreFactory = Callable[[], SqliteRangeCheckpointStore]


@dataclass(frozen=True)
class RangeSpeedWarmupConfig:
    symbol: str
    exchange: str
    range_pct: Decimal
    bucket_interval_ms: int
    startup_catchup: StartupCatchupConfig


class RangeSpeedWarmup:
    """Own startup Range-speed history state and checkpoint reads."""

    def __init__(
        self,
        *,
        config: RangeSpeedWarmupConfig,
        provider: ProviderFactory,
        checkpoint_store: StoreFactory,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.config = config
        self._provider = provider
        self._checkpoint_store = checkpoint_store
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.excluded_previous = False
        self.complete_history = 0
        self.min_periods = 0

    async def warmup(self) -> int:
        provider = self._provider()
        if provider is None:
            return 0
        status = provider.range_speed_history_status()
        now_ms = self._clock_ms()
        current_bucket = self._bucket_start(now_ms)
        catchup = self.config.startup_catchup
        within_catchup_window = (
            catchup.enabled
            and now_ms - current_bucket
            <= catchup.fresh_open_window_seconds * 1000
        )
        self.excluded_previous = within_catchup_window
        before_bucket_end_ms = (
            current_bucket - 1
            if within_catchup_window
            else current_bucket + self.config.bucket_interval_ms - 1
        )
        rows = await asyncio.to_thread(
            self._checkpoint_store().load_complete_history,
            exchange=self.config.exchange,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            before_bucket_end_ms=before_bucket_end_ms,
            limit=int(status["rolling_window_bars"]),
        )
        loaded = provider.warmup_range_speed_history(
            [row.rf_bar_count for row in rows]
        )
        status = provider.range_speed_history_status()
        self.min_periods = int(status["min_periods"])
        self.complete_history = int(status["complete_history"])
        log = logger.info if loaded >= self.min_periods else logger.warning
        log(
            "Range-speed history warmup | complete_history=%s min_periods=%s available=%s",
            loaded,
            self.min_periods,
            loaded >= self.min_periods,
        )
        return int(loaded)

    async def finish_after_catchup(self, *, range_observed: bool) -> None:
        if not self.excluded_previous or range_observed:
            return
        provider = self._provider()
        if provider is None:
            return
        current_bucket = self._bucket_start(self._clock_ms())
        rows = await asyncio.to_thread(
            self._checkpoint_store().load_complete_history,
            exchange=self.config.exchange,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            before_bucket_end_ms=current_bucket,
            limit=1,
        )
        if rows and rows[-1].bucket_end_ms == current_bucket - 1:
            count = provider.warmup_range_speed_history(
                [rows[-1].rf_bar_count]
            )
            self.complete_history += int(count)

    def warn_if_insufficient(self, loaded: int) -> None:
        if self.min_periods > 0 and loaded < self.min_periods:
            logger.warning(
                "Range-speed history insufficient; live runtime continues | complete_history=%s min_periods=%s missing=%s",
                loaded,
                self.min_periods,
                self.min_periods - loaded,
            )

    def _bucket_start(self, time_ms: int) -> int:
        interval = self.config.bucket_interval_ms
        return (time_ms // interval) * interval


__all__ = ["RangeSpeedWarmup", "RangeSpeedWarmupConfig"]
