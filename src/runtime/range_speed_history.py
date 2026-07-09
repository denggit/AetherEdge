from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from typing import Any

from src.market_data.backfill.coverage import current_closed_bucket_end_ms
from src.market_data.backfill.scanner import RangeBackfillScanner
from src.market_data.backfill.status_store import (
    RangeBackfillStatusStore,
    worker_heartbeat_ms,
    worker_status_is_running,
)
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.utils.log import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class RangeSpeedHistoryStatus:
    symbol: str
    exchange: str
    range_pct: str
    bucket_interval: str
    complete_history: int
    min_periods: int
    missing_periods: int
    rolling_window_bars: int
    available: bool
    latest_complete_bucket_end_ms: int | None
    current_closed_bucket_end_ms: int
    refreshed: bool = False
    first_missing_bucket_start_ms: int | None = None
    first_missing_bucket_end_ms: int | None = None
    first_missing_reason: str | None = None
    first_missing_coverage_status: str | None = None


class RangeSpeedHistoryRefresher:
    def __init__(
        self,
        *,
        strategy: Any,
        store: SqliteRangeCheckpointStore,
        symbol: str,
        exchange: str,
        range_pct: str,
        bucket_interval: str,
        refresh_seconds: float = 60.0,
        warning_seconds: float = 600.0,
        backfill_enabled: bool = True,
        status_path: str = "data/state/range_backfill_status.json",
    ) -> None:
        self.strategy = strategy
        self.store = store
        self.symbol = symbol
        self.exchange = exchange
        self.range_pct = range_pct
        self.bucket_interval = bucket_interval
        self.refresh_seconds = max(1.0, float(refresh_seconds))
        self.warning_seconds = max(1.0, float(warning_seconds))
        self.backfill_enabled = bool(backfill_enabled)
        self.status_store = RangeBackfillStatusStore(status_path)
        self._task: asyncio.Task | None = None
        self._last_marker: tuple[int, int | None, int | None, int | None] | None = None
        self._last_coverage_marker: tuple[int, int] | None = None
        self._last_warning_ms = 0
        self._was_available: bool | None = None
        self.last_status: RangeSpeedHistoryStatus | None = None

    def start(self, stop_event: asyncio.Event) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self.run(stop_event))

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self._task = None

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.refresh_once()
            except Exception as exc:
                logger.warning("Range speed history refresh failed | error=%s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.refresh_seconds)
            except asyncio.TimeoutError:
                pass

    async def refresh_once(self) -> RangeSpeedHistoryStatus:
        now_ms = int(time.time() * 1000)
        closed_end = current_closed_bucket_end_ms(now_ms, self.bucket_interval)
        rolling = self._rolling_window_bars()
        min_periods = self._min_periods()
        coverage = await asyncio.to_thread(
            RangeBackfillScanner(self.store).scan,
            exchange=self.exchange,
            symbol=self.symbol,
            range_pct=self.range_pct,
            bucket_interval=self.bucket_interval,
            required_buckets=min_periods,
            lookback_buckets=rolling,
            now_ms=now_ms,
            direction="oldest-to-recent",
        )
        rows = await asyncio.to_thread(
            self.store.load_complete_history,
            exchange=self.exchange,
            symbol=self.symbol,
            range_pct=self.range_pct,
            before_bucket_end_ms=closed_end + 1,
            limit=rolling,
        )
        latest_end = rows[-1].bucket_end_ms if rows else None
        completed_at_values = [row.completed_at_ms for row in rows]
        marker = (
            len(rows),
            rows[0].bucket_end_ms if rows else None,
            latest_end,
            max(completed_at_values) if completed_at_values else None,
        )
        coverage_marker = (
            coverage.current_closed_bucket_end_ms,
            coverage.required_window_missing_count,
        )
        first_missing = (
            coverage.required_window_missing_buckets[0]
            if coverage.required_window_missing_buckets
            else None
        )
        refreshed = False
        if marker != self._last_marker or coverage_marker != self._last_coverage_marker:
            values = self._history_values_for_strategy(rows)
            replace = getattr(self.strategy, "replace_range_speed_history", None)
            if callable(replace):
                replace(values)
                refreshed = True
            else:
                logger.warning("Strategy has no replace_range_speed_history(); range speed refresh skipped")
            self._last_marker = marker
            self._last_coverage_marker = coverage_marker
        complete = self._complete_history_count(default=len(rows))
        status = RangeSpeedHistoryStatus(
            symbol=self.symbol,
            exchange=self.exchange,
            range_pct=self.range_pct,
            bucket_interval=self.bucket_interval,
            complete_history=complete,
            min_periods=min_periods,
            missing_periods=coverage.required_window_missing_count,
            rolling_window_bars=rolling,
            available=coverage.available,
            latest_complete_bucket_end_ms=latest_end,
            current_closed_bucket_end_ms=closed_end,
            refreshed=refreshed,
            first_missing_bucket_start_ms=(
                None if first_missing is None else first_missing.bucket_start_ms
            ),
            first_missing_bucket_end_ms=(
                None if first_missing is None else first_missing.bucket_end_ms
            ),
            first_missing_reason=None if first_missing is None else first_missing.reason,
            first_missing_coverage_status=(
                None if first_missing is None else first_missing.coverage_status
            ),
        )
        self.last_status = status
        self._log_status_if_needed(status)
        return status

    def _history_values_for_strategy(self, rows) -> list[int]:
        # Keep the strategy-side tracker filled with the complete rolling history we
        # actually have.  Coverage.available is intentionally stricter: it means
        # the latest required buckets are consecutive and is used to trigger
        # backfill/diagnostics.  It must not shrink an otherwise usable 1080-bar
        # rolling history to 99 bars just because one recent bucket is missing.
        return [row.rf_bar_count for row in rows]

    def _log_status_if_needed(self, status: RangeSpeedHistoryStatus) -> None:
        if status.available:
            if self._was_available is False:
                logger.info(
                    "V10A range-speed history recovered; short-speed block available without restart | complete_history=%s min_periods=%s refreshed=%s",
                    status.complete_history,
                    status.min_periods,
                    status.refreshed,
                )
            self._was_available = True
            return
        self._was_available = False
        now = int(time.time() * 1000)
        if now - self._last_warning_ms < self.warning_seconds * 1000:
            return
        self._last_warning_ms = now
        backfill = self.status_store.read() or {}
        backfill_running = worker_status_is_running(backfill)
        backfill_pid = backfill.get("pid") if backfill_running else None
        logger.warning(
            "V10A range-speed history still insufficient; live runtime continues | "
            "symbol=%s exchange=%s range_pct=%s interval=%s complete_history=%s "
            "min_periods=%s missing_periods=%s rolling_window_bars=%s available=%s "
            "latest_complete_bucket_end_ms=%s current_closed_bucket_end_ms=%s "
            "backfill_enabled=%s backfill_process_running=%s backfill_pid=%s "
            "backfill_mode=%s backfill_direction=%s backfill_last_heartbeat_ms=%s "
            "backfill_last_completed_bucket_end_ms=%s backfill_last_error=%s "
            "first_missing_bucket_start_ms=%s first_missing_bucket_end_ms=%s "
            "first_missing_reason=%s first_missing_coverage_status=%s "
            "next_check_seconds=%s",
            status.symbol,
            status.exchange,
            status.range_pct,
            status.bucket_interval,
            status.complete_history,
            status.min_periods,
            status.missing_periods,
            status.rolling_window_bars,
            status.available,
            status.latest_complete_bucket_end_ms,
            status.current_closed_bucket_end_ms,
            self.backfill_enabled,
            backfill_running,
            backfill_pid,
            backfill.get("mode"),
            backfill.get("direction"),
            worker_heartbeat_ms(backfill),
            backfill.get("last_completed_bucket_end_ms"),
            backfill.get("last_error"),
            status.first_missing_bucket_start_ms,
            status.first_missing_bucket_end_ms,
            status.first_missing_reason,
            status.first_missing_coverage_status,
            int(self.refresh_seconds),
        )

    def _entry_filters(self):
        return getattr(getattr(self.strategy, "config", None), "entry_filters", None)

    def _rolling_window_bars(self) -> int:
        return int(getattr(self._entry_filters(), "range_speed_rolling_window_bars", 1080))

    def _min_periods(self) -> int:
        return int(getattr(self._entry_filters(), "range_speed_min_periods", 100))

    def _complete_history_count(self, *, default: int) -> int:
        tracker = getattr(self.strategy, "range_speed_tracker", None)
        if tracker is None:
            return default
        return int(getattr(tracker, "complete_history_count", default))
