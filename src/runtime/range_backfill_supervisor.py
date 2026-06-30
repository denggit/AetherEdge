from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import subprocess
import sys

from src.market_data.backfill.scanner import RangeBackfillScanner
from src.market_data.backfill.status_store import (
    RangeBackfillStatusStore,
    now_ms,
    process_id_exists,
    worker_heartbeat_ms,
)
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.utils.log import get_logger

logger = get_logger(__name__)

REASON_AVAILABLE = "available"
REASON_INSUFFICIENT_HISTORY = "insufficient_history"
REASON_ARCHIVE_GAP_BACKFILLING = "archive_gap_backfilling"
REASON_ARCHIVE_GAP_NO_PROGRESS = "archive_gap_no_progress"
REASON_ARCHIVE_GAP_PARTIAL_NO_PROGRESS = "archive_gap_partial_no_progress"
REASON_CURRENT_DAY_ARCHIVE_NOT_READY = "current_day_archive_not_ready"
REASON_CURRENT_DAY_GAP_TOO_LARGE = "current_day_gap_too_large"
REASON_REPAIR_FAILED_COOLDOWN = "repair_failed_cooldown"
REASON_STALE_WORKER_MISSING = "stale_worker_missing"
DEFERRED_REPAIR_REASONS = frozenset(
    {
        REASON_ARCHIVE_GAP_NO_PROGRESS,
        REASON_ARCHIVE_GAP_PARTIAL_NO_PROGRESS,
        REASON_CURRENT_DAY_ARCHIVE_NOT_READY,
        REASON_REPAIR_FAILED_COOLDOWN,
    }
)


@dataclass(frozen=True)
class RangeBackfillSupervisorConfig:
    enabled: bool = True
    required_buckets: int = 100
    lookback_buckets: int = 160
    max_buckets_per_cycle: int = 6
    max_days_per_cycle: int = 1
    sleep_seconds: float = 30.0
    heartbeat_stale_seconds: int = 180
    restart_cooldown_seconds: int = 300
    failure_cooldown_seconds: int = 3600
    archive_not_ready_cooldown_seconds: int = 21600
    daily_retry_after_utc_hour: int = 1
    monitor_seconds: float = 60.0
    status_path: Path = Path("data/state/range_backfill_status.json")
    lock_path: Path = Path("data/state/range_backfill.lock")
    low_priority: bool = True
    chunksize: int = 50_000
    raw_root: Path = Path("data/okx/raw/trades")
    market_db_path: Path = Path("data/market_data/aether_market_data.sqlite3")
    checkpoint_db_path: Path = Path("data/state/range_builder_checkpoint.sqlite3")
    save_raw_trades: bool = False
    chunk_sleep_seconds: float = 0.1
    max_seconds_per_cycle: float = 30.0
    max_trades_per_cycle: int = 300_000
    repo_root: Path = Path(".")


class RangeBackfillSupervisor:
    def __init__(self, config: RangeBackfillSupervisorConfig) -> None:
        self.config = config
        self.status_store = RangeBackfillStatusStore(config.status_path)
        self.process: subprocess.Popen | None = None
        self._last_start_ms = 0
        self._stdout_handle = None
        self._monitor_task: asyncio.Task | None = None

    def start_monitor(
        self,
        *,
        stop_event: asyncio.Event,
        symbol: str,
        exchange: str,
        range_pct: str,
        bucket_interval: str,
    ) -> None:
        if self._monitor_task is not None and not self._monitor_task.done():
            return
        self._monitor_task = asyncio.create_task(
            self._monitor_loop(
                stop_event=stop_event,
                symbol=symbol,
                exchange=exchange,
                range_pct=range_pct,
                bucket_interval=bucket_interval,
            )
        )

    async def stop_async(self, *, timeout_seconds: float = 2.0) -> None:
        task = self._monitor_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._monitor_task = None
        await asyncio.to_thread(self.stop, timeout_seconds=timeout_seconds)

    def start_if_needed(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: str,
        bucket_interval: str,
        complete_history: int,
        min_periods: int,
        max_target_end_ms: int | None = None,
    ) -> bool:
        if not self.config.enabled or int(complete_history) >= int(min_periods):
            return False
        if self._local_process_running():
            return False
        if self._status_shows_running_worker():
            return False
        if self._persisted_retry_is_deferred():
            return False
        if self._in_restart_cooldown():
            return False
        try:
            command = self._build_command(
                symbol=symbol,
                exchange=exchange,
                range_pct=range_pct,
                bucket_interval=bucket_interval,
                max_target_end_ms=max_target_end_ms,
            )
            log_path = self.config.repo_root / "logs" / "range_backfill_worker.out"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            self._stdout_handle = log_path.open("a", encoding="utf-8")
            self.process = subprocess.Popen(
                command,
                cwd=str(self.config.repo_root),
                stdout=self._stdout_handle,
                stderr=subprocess.STDOUT,
                shell=False,
            )
            self._last_start_ms = now_ms()
            self.status_store.patch(
                range_speed_available=False,
                range_speed_reason=REASON_ARCHIVE_GAP_BACKFILLING,
                next_retry_after_ms=None,
            )
            logger.warning(
                "Range backfill worker started | pid=%s mode=live direction=recent-to-oldest",
                self.process.pid,
            )
            return True
        except Exception as exc:
            retry_after_ms = now_ms() + max(0, int(self.config.failure_cooldown_seconds)) * 1000
            self.status_store.patch(
                running=False,
                phase="failed",
                range_speed_available=False,
                range_speed_reason=REASON_REPAIR_FAILED_COOLDOWN,
                next_retry_after_ms=retry_after_ms,
                last_error=str(exc),
                exit_code=1,
            )
            logger.warning("Range backfill worker failed to start | error=%s", exc)
            return False

    async def _monitor_loop(
        self,
        *,
        stop_event: asyncio.Event,
        symbol: str,
        exchange: str,
        range_pct: str,
        bucket_interval: str,
    ) -> None:
        while not stop_event.is_set():
            try:
                coverage = await asyncio.to_thread(
                    self._scan_coverage,
                    symbol=symbol,
                    exchange=exchange,
                    range_pct=range_pct,
                    bucket_interval=bucket_interval,
                )
                archive_max_target_end_ms = _archive_complete_max_target_end_ms()
                reason = self._coverage_reason(
                    coverage,
                    archive_max_target_end_ms=archive_max_target_end_ms,
                )
                worker_running = self.running
                deferred_reason = self._persisted_retry_reason()
                if reason != REASON_AVAILABLE and deferred_reason is not None:
                    reason = deferred_reason
                elif (
                    reason == REASON_ARCHIVE_GAP_BACKFILLING
                    and not worker_running
                    and self._in_restart_cooldown()
                ):
                    reason = REASON_REPAIR_FAILED_COOLDOWN
                self._write_coverage_status(
                    coverage,
                    reason=reason,
                    archive_max_target_end_ms=archive_max_target_end_ms,
                )
                if reason == REASON_ARCHIVE_GAP_BACKFILLING:
                    self.start_if_needed(
                        symbol=symbol,
                        exchange=exchange,
                        range_pct=range_pct,
                        bucket_interval=bucket_interval,
                        complete_history=coverage.required_window_complete_count,
                        min_periods=coverage.required_buckets,
                        max_target_end_ms=archive_max_target_end_ms,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Range backfill supervisor monitor failed | error=%s", exc)
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=max(1.0, float(self.config.monitor_seconds)),
                )
            except asyncio.TimeoutError:
                pass

    def _scan_coverage(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: str,
        bucket_interval: str,
    ):
        scanner = RangeBackfillScanner(
            SqliteRangeCheckpointStore(self.config.checkpoint_db_path)
        )
        return scanner.scan(
            exchange=exchange,
            symbol=symbol,
            range_pct=range_pct,
            bucket_interval=bucket_interval,
            required_buckets=self.config.required_buckets,
            lookback_buckets=self.config.lookback_buckets,
            direction="recent-to-oldest",
        )

    def stop(self, *, timeout_seconds: float = 2.0) -> None:
        process = self.process
        if process is None:
            self._close_stdout()
            return
        if process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=max(0.0, timeout_seconds))
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
        self.process = None
        self._close_stdout()

    @property
    def running(self) -> bool:
        return self._local_process_running() or self._status_shows_running_worker()

    @property
    def pid(self) -> int | None:
        if self.process is not None and self.process.poll() is None:
            return int(self.process.pid)
        status = self.status_store.read()
        if status and status.get("running"):
            raw = status.get("pid")
            return None if raw is None else int(raw)
        return None

    def _build_command(
        self,
        *,
        symbol: str,
        exchange: str,
        range_pct: str,
        bucket_interval: str,
        max_target_end_ms: int | None = None,
    ) -> list[str]:
        command = [
            sys.executable,
            "-u",
            "tools/range_backfill_worker.py",
            "--mode",
            "live",
            "--direction",
            "recent-to-oldest",
            "--symbol",
            symbol,
            "--exchange",
            exchange,
            "--range-pct",
            str(range_pct),
            "--bucket-interval",
            bucket_interval,
            "--required-buckets",
            str(self.config.required_buckets),
            "--lookback-buckets",
            str(self.config.lookback_buckets),
            "--max-buckets-per-cycle",
            str(self.config.max_buckets_per_cycle),
            "--max-days-per-cycle",
            str(self.config.max_days_per_cycle),
            "--sleep-seconds",
            str(self.config.sleep_seconds),
            "--chunksize",
            str(self.config.chunksize),
            "--status-path",
            str(self.config.status_path),
            "--lock-path",
            str(self.config.lock_path),
            "--market-db",
            str(self.config.market_db_path),
            "--checkpoint-db",
            str(self.config.checkpoint_db_path),
            "--raw-root",
            str(self.config.raw_root),
            "--low-priority" if self.config.low_priority else "--no-low-priority",
            "--no-once",
            "--save-raw-trades" if self.config.save_raw_trades else "--no-save-raw-trades",
            "--chunk-sleep-seconds",
            str(self.config.chunk_sleep_seconds),
            "--max-seconds-per-cycle",
            str(self.config.max_seconds_per_cycle),
            "--max-trades-per-cycle",
            str(self.config.max_trades_per_cycle),
            "--failure-cooldown-seconds",
            str(self.config.failure_cooldown_seconds),
            "--archive-not-ready-cooldown-seconds",
            str(self.config.archive_not_ready_cooldown_seconds),
            "--daily-retry-after-utc-hour",
            str(self.config.daily_retry_after_utc_hour),
        ]
        if max_target_end_ms is not None:
            command.extend(["--max-target-end-ms", str(int(max_target_end_ms))])
        return command

    def _coverage_reason(self, coverage, *, archive_max_target_end_ms: int) -> str:
        if bool(getattr(coverage, "available", False)):
            return REASON_AVAILABLE
        gaps = tuple(
            getattr(coverage, "required_window_missing_buckets", ())
            or getattr(coverage, "missing_buckets", ())
            or ()
        )
        if not gaps:
            return REASON_INSUFFICIENT_HISTORY
        if any(gap.bucket_end_ms <= archive_max_target_end_ms for gap in gaps):
            return REASON_ARCHIVE_GAP_BACKFILLING
        return REASON_CURRENT_DAY_GAP_TOO_LARGE

    def _write_coverage_status(self, coverage, *, reason: str, archive_max_target_end_ms: int) -> None:
        payload = {
            "range_speed_available": reason == REASON_AVAILABLE,
            "range_speed_reason": reason,
            "complete_after": int(getattr(coverage, "required_window_complete_count", 0)),
            "missing_after": int(getattr(coverage, "required_window_missing_count", 0)),
            "required_buckets": int(
                getattr(coverage, "required_buckets", self.config.required_buckets)
            ),
            "lookback_buckets": int(self.config.lookback_buckets),
            "last_scanned_bucket_end_ms": getattr(
                coverage, "current_closed_bucket_end_ms", None
            ),
            "archive_max_target_end_ms": archive_max_target_end_ms,
            "supervisor_heartbeat_ms": now_ms(),
        }
        if reason == REASON_AVAILABLE:
            payload["next_retry_after_ms"] = None
        self.status_store.patch(**payload)

    def _local_process_running(self) -> bool:
        if self.process is None:
            return False
        if self.process.poll() is None:
            return True
        self.process = None
        self._close_stdout()
        return False

    def _status_shows_running_worker(self) -> bool:
        status = self.status_store.read()
        if not status or not status.get("running"):
            return False
        if process_id_exists(status.get("pid")) is False:
            self.status_store.patch(
                running=False,
                pid=None,
                phase=REASON_STALE_WORKER_MISSING,
                range_speed_available=False,
                range_speed_reason=REASON_STALE_WORKER_MISSING,
                exit_code=0,
                last_error="stale worker pid not found",
                next_retry_after_ms=None,
            )
            return False
        heartbeat = worker_heartbeat_ms(status)
        if heartbeat is None:
            return False
        return now_ms() - int(heartbeat) <= self.config.heartbeat_stale_seconds * 1000

    def _in_restart_cooldown(self) -> bool:
        return self._last_start_ms > 0 and now_ms() - self._last_start_ms < self.config.restart_cooldown_seconds * 1000

    def _persisted_retry_is_deferred(self) -> bool:
        return self._persisted_retry_reason() is not None

    def _persisted_retry_reason(self) -> str | None:
        status = self.status_store.read()
        if not status or status.get("running"):
            return None
        reason = str(status.get("range_speed_reason") or "")
        if reason not in DEFERRED_REPAIR_REASONS:
            return None
        retry_after_ms = status.get("next_retry_after_ms")
        if retry_after_ms is None:
            return None
        try:
            deferred = int(retry_after_ms) > now_ms()
        except (TypeError, ValueError):
            return None
        return reason if deferred else None

    def _close_stdout(self) -> None:
        handle = self._stdout_handle
        self._stdout_handle = None
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass


def _archive_complete_max_target_end_ms(now_ms_value: int | None = None) -> int:
    now = datetime.now(UTC) if now_ms_value is None else datetime.fromtimestamp(int(now_ms_value) / 1000, tz=UTC)
    today_start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    return int(today_start.timestamp() * 1000) - 1
