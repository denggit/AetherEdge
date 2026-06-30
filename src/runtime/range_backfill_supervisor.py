from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys

from src.market_data.backfill.scanner import RangeBackfillScanner
from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.utils.log import get_logger

logger = get_logger(__name__)


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
    monitor_seconds: float = 60.0
    status_path: Path = Path("data/state/range_backfill_status.json")
    lock_path: Path = Path("data/state/range_backfill.lock")
    low_priority: bool = True
    chunksize: int = 50_000
    raw_root: Path = Path("data/okx/raw/trades")
    market_db_path: Path = Path("data/market_data/aether_market_data.sqlite3")
    checkpoint_db_path: Path = Path("data/state/range_builder_checkpoint.sqlite3")
    save_raw_trades: bool = False
    chunk_sleep_seconds: float = 0.05
    max_seconds_per_cycle: float = 120.0
    max_trades_per_cycle: int = 2_000_000
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
    ) -> bool:
        if not self.config.enabled or int(complete_history) >= int(min_periods):
            return False
        if self._local_process_running():
            return False
        if self._status_shows_running_worker():
            return False
        if self._in_restart_cooldown():
            return False
        try:
            command = self._build_command(
                symbol=symbol,
                exchange=exchange,
                range_pct=range_pct,
                bucket_interval=bucket_interval,
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
            logger.warning(
                "Range backfill worker started | pid=%s mode=live direction=recent-to-oldest",
                self.process.pid,
            )
            return True
        except Exception as exc:
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
                if not coverage.available:
                    self.start_if_needed(
                        symbol=symbol,
                        exchange=exchange,
                        range_pct=range_pct,
                        bucket_interval=bucket_interval,
                        complete_history=coverage.required_window_complete_count,
                        min_periods=coverage.required_buckets,
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

    def _build_command(self, *, symbol: str, exchange: str, range_pct: str, bucket_interval: str) -> list[str]:
        return [
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
        ]

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
        heartbeat = status.get("heartbeat_ms")
        if heartbeat is None:
            return False
        return now_ms() - int(heartbeat) <= self.config.heartbeat_stale_seconds * 1000

    def _in_restart_cooldown(self) -> bool:
        return self._last_start_ms > 0 and now_ms() - self._last_start_ms < self.config.restart_cooldown_seconds * 1000

    def _close_stdout(self) -> None:
        handle = self._stdout_handle
        self._stdout_handle = None
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass
