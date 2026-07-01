from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import subprocess
import sys
from typing import Callable

from src.market_data.backfill.status_store import RangeBackfillStatusStore
from src.market_data.range_checkpoint import MICRO_REPAIR_FAILED
from src.utils.log import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class RangeMicroRepairSupervisorConfig:
    enabled: bool = True
    monitor_seconds: float = 30.0
    status_path: Path = Path("data/state/range_micro_repair_status.json")
    lock_path: Path = Path("data/state/range_micro_repair.lock")
    checkpoint_db_path: Path = Path(
        "data/state/range_builder_checkpoint.sqlite3"
    )
    market_db_path: Path = Path(
        "data/market_data/aether_market_data.sqlite3"
    )
    page_limit: int = 100
    max_pages: int = 20
    max_seconds: float = 30.0
    missing_bucket_grace_seconds: int = 120
    repo_root: Path = Path(".")


class RangeMicroRepairSupervisor:
    """Launch a self-contained repair worker without handling trade data."""

    def __init__(
        self,
        config: RangeMicroRepairSupervisorConfig,
        *,
        on_failure: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config
        self.on_failure = on_failure
        self.status_store = RangeBackfillStatusStore(config.status_path)
        self.process: subprocess.Popen | None = None
        self._stdout_handle = None
        self._monitor_task: asyncio.Task | None = None

    def start_monitor(self, *, stop_event: asyncio.Event) -> None:
        if not self.config.enabled:
            return
        if self._monitor_task is not None and not self._monitor_task.done():
            return
        self._monitor_task = asyncio.create_task(self._monitor_loop(stop_event))

    async def stop_async(self) -> None:
        task = self._monitor_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._monitor_task = None
        # The repair process is intentionally independent of live runtime.
        # Do not terminate it when the main process shuts down.
        if self.process is not None and self.process.poll() is not None:
            self.process = None
            self._close_stdout()

    async def _monitor_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                self._refresh_finished_process()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "Range micro repair supervisor monitor failed | error=%s",
                    exc,
                )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=max(1.0, float(self.config.monitor_seconds)),
                )
            except asyncio.TimeoutError:
                pass

    def start_for_recovery(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        bucket_end_ms: int,
        coverage_status: str,
        missing_gap_ms: int,
    ) -> bool:
        if not self.config.enabled or self.running:
            return False
        try:
            log_path = (
                self.config.repo_root
                / "logs"
                / "range_micro_repair_worker.out"
            )
            log_path.parent.mkdir(parents=True, exist_ok=True)
            self._stdout_handle = log_path.open("a", encoding="utf-8")
            popen_kwargs = {
                "cwd": str(self.config.repo_root),
                "stdout": self._stdout_handle,
                "stderr": subprocess.STDOUT,
                "shell": False,
            }
            if sys.platform == "win32":
                popen_kwargs["creationflags"] = getattr(
                    subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0
                )
            self.process = subprocess.Popen(
                self._build_command(
                    exchange=exchange,
                    symbol=symbol,
                    range_pct=range_pct,
                    bucket_start_ms=bucket_start_ms,
                    bucket_end_ms=bucket_end_ms,
                    coverage_status=coverage_status,
                    missing_gap_ms=missing_gap_ms,
                ),
                **popen_kwargs,
            )
            logger.warning(
                "Range micro repair worker started | pid=%s symbol=%s "
                "bucket_start_ms=%s bucket_end_ms=%s",
                self.process.pid,
                symbol,
                bucket_start_ms,
                bucket_end_ms,
            )
            return True
        except Exception as exc:
            logger.warning(
                "Range micro repair worker failed to start | error=%s", exc
            )
            self._notify_failure(f"worker_start_failed:{exc}")
            self.process = None
            self._close_stdout()
            return False

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def _refresh_finished_process(self) -> None:
        if self.process is None or self.process.poll() is None:
            return
        exit_code = int(self.process.returncode or 0)
        status = self.status_store.read() or {}
        repair_status = str(status.get("repair_status") or "")
        if exit_code != 0 or repair_status == MICRO_REPAIR_FAILED:
            reason = str(
                status.get("failure_reason")
                or status.get("last_error")
                or f"worker_exit_code={exit_code}"
            )
            logger.warning(
                "Range micro repair worker failed | exit_code=%s "
                "failure_reason=%s",
                exit_code,
                reason,
            )
            self._notify_failure(reason)
        self.process = None
        self._close_stdout()

    def _build_command(
        self,
        *,
        exchange: str,
        symbol: str,
        range_pct: str,
        bucket_start_ms: int,
        bucket_end_ms: int,
        coverage_status: str,
        missing_gap_ms: int,
    ) -> list[str]:
        return [
            sys.executable,
            "-u",
            "tools/range_micro_repair_worker.py",
            "--exchange",
            exchange,
            "--symbol",
            symbol,
            "--range-pct",
            range_pct,
            "--bucket-start-ms",
            str(bucket_start_ms),
            "--bucket-end-ms",
            str(bucket_end_ms),
            "--coverage-status",
            str(coverage_status),
            "--missing-gap-ms",
            str(missing_gap_ms),
            "--checkpoint-db",
            str(self.config.checkpoint_db_path),
            "--market-db",
            str(self.config.market_db_path),
            "--status-path",
            str(self.config.status_path),
            "--lock-path",
            str(self.config.lock_path),
            "--page-limit",
            str(self.config.page_limit),
            "--max-pages",
            str(self.config.max_pages),
            "--max-seconds",
            str(self.config.max_seconds),
            "--missing-bucket-grace-seconds",
            str(self.config.missing_bucket_grace_seconds),
        ]

    def _close_stdout(self) -> None:
        handle = self._stdout_handle
        self._stdout_handle = None
        if handle is not None:
            try:
                handle.close()
            except Exception:
                pass

    def _notify_failure(self, reason: str) -> None:
        if self.on_failure is None:
            return
        try:
            self.on_failure(str(reason))
        except Exception as exc:
            logger.warning(
                "Range micro repair failure callback failed | error=%s",
                exc,
            )
