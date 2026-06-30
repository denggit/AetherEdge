from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from src.market_data.backfill.scanner import BackfillScanner
from src.market_data.backfill.service import BackfillService
from src.market_data.warmup.gap_detector import interval_to_ms
from src.platform.exchanges.okx.rest_tail_trades import OkxRestTailTradesFetcher

logger = logging.getLogger(__name__)


@dataclass
class RangeBackfillWorker:
    exchange: str = "okx"
    symbol: str = "ETH-USDT-PERP"
    raw_symbol: str = "ETH-USDT-SWAP"
    range_pct: str = "0.002"
    bucket_interval: str = "4h"
    required_buckets: int = 100
    lookback_buckets: int = 180
    raw_root: str | Path = "data/okx/raw"
    market_db: str | Path = "data/market_data/aether_market_data.sqlite3"
    checkpoint_db: str | Path = "data/state/range_builder_checkpoint.sqlite3"
    max_buckets_per_cycle: int = 1
    cycle_sleep_seconds: float = 30.0
    download_sleep_seconds: float = 2.0
    chunksize: int = 300_000
    max_rest_tail_gap_minutes: int = 240
    max_rest_tail_buckets: int = 12
    warning_interval_seconds: float = 600.0
    json_status: str | Path = "data/reports/range_backfill/status.json"
    pid_file: str | Path = "data/run/range_backfill_worker.pid"
    lock_file: str | Path = "data/run/range_backfill_worker.lock"

    def run_once(self) -> dict[str, object]:
        scanner = BackfillScanner(checkpoint_db=self.checkpoint_db, market_db=self.market_db)
        service = BackfillService(
            market_db=self.market_db,
            checkpoint_db=self.checkpoint_db,
            raw_root=self.raw_root,
            chunksize=self.chunksize,
            download_sleep_seconds=self.download_sleep_seconds,
            max_rest_tail_gap_minutes=self.max_rest_tail_gap_minutes,
            max_rest_tail_buckets=self.max_rest_tail_buckets,
            rest_tail_fetcher=OkxRestTailTradesFetcher(symbol=self.symbol),
        )
        plan = scanner.scan(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol=self.raw_symbol,
            range_pct=self.range_pct,
            bucket_ms=interval_to_ms(self.bucket_interval),
            required_buckets=self.required_buckets,
            lookback_buckets=self.lookback_buckets,
            current_time_ms=int(time.time() * 1000),
        )
        result = service.process_plan(plan, max_buckets=self.max_buckets_per_cycle)
        status = {
            "updated_at_ms": int(time.time() * 1000),
            "mode": "once",
            "pid": os.getpid(),
            "plan": plan.to_dict(),
            "result": result.to_dict(),
            "range_speed_ready": plan.range_speed_ready,
            "missing_bucket_count": plan.missing_bucket_count,
            "continuous_complete_buckets_from_latest": plan.continuous_complete_buckets_from_latest,
            "tail_fetch_failed_buckets": list(result.tail_fetch_failed_buckets),
            "archive_errors_count": len(result.archive_errors),
        }
        self.write_status(status)
        return status

    def run_daemon(self, *, stop_after_cycles: int | None = None) -> int:
        last_warning = 0.0
        cycles = 0
        while True:
            try:
                status = self.run_once()
            except Exception as exc:  # noqa: BLE001 - daemon must survive one bad cycle
                logger.exception("range backfill worker cycle failed")
                status = {
                    "updated_at_ms": int(time.time() * 1000),
                    "mode": "daemon",
                    "pid": os.getpid(),
                    "plan": {},
                    "result": {
                        "processed_buckets": 0,
                        "downloaded_days": 0,
                        "imported_trades": 0,
                        "range_bars_saved": 0,
                        "aggregates_upserted": 0,
                        "skipped_buckets": [],
                        "tail_fetch_requested_buckets": [],
                        "tail_fetch_succeeded_buckets": [],
                        "tail_fetch_failed_buckets": [],
                        "tail_fetch_trades_saved": 0,
                        "coverage_validated_buckets": [],
                        "coverage_failed_buckets": [],
                        "archive_errors": [],
                        "tail_errors": [],
                        "locked": False,
                        "errors": [f"{type(exc).__name__}: {exc}"],
                    },
                    "range_speed_ready": False,
                    "missing_bucket_count": None,
                    "continuous_complete_buckets_from_latest": None,
                    "tail_fetch_failed_buckets": [],
                    "archive_errors_count": 0,
                }
                try:
                    self.write_status(status)
                except Exception:
                    logger.exception("range backfill worker failed to write error status")
            plan = status.get("plan", {})
            if isinstance(plan, dict) and not bool(plan.get("range_speed_ready")):
                now = time.monotonic()
                if now - last_warning >= self.warning_interval_seconds:
                    logger.warning(
                        "range-speed backfill not ready | continuous_complete_buckets_from_latest=%s missing_bucket_count=%s nearest_missing_bucket=%s",
                        plan.get("continuous_complete_buckets_from_latest"),
                        plan.get("missing_bucket_count"),
                        plan.get("nearest_missing_bucket_start_ms"),
                    )
                    last_warning = now
            cycles += 1
            if stop_after_cycles is not None and cycles >= stop_after_cycles:
                return 0
            time.sleep(max(0.0, self.cycle_sleep_seconds))

    def write_status(self, status: dict[str, object]) -> None:
        path = Path(self.json_status)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps(status, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
        tmp.replace(path)

    def acquire_single_instance(self) -> "WorkerLock | None":
        return WorkerLock.acquire(Path(self.lock_file), Path(self.pid_file))


class WorkerLock:
    def __init__(self, lock_file: Path, pid_file: Path, fd: int) -> None:
        self.lock_file = lock_file
        self.pid_file = pid_file
        self.fd = fd

    @classmethod
    def acquire(cls, lock_file: Path, pid_file: Path) -> "WorkerLock | None":
        lock_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        if pid_file.exists():
            try:
                pid = int(pid_file.read_text(encoding="utf-8").strip())
            except ValueError:
                pid = 0
            if pid > 0 and pid != os.getpid() and _pid_alive(pid):
                return None
            pid_file.unlink(missing_ok=True)
            lock_file.unlink(missing_ok=True)
        try:
            fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            return None
        os.write(fd, str(os.getpid()).encode("ascii"))
        pid_file.write_text(str(os.getpid()), encoding="utf-8")
        return cls(lock_file, pid_file, fd)

    def release(self) -> None:
        try:
            os.close(self.fd)
        except OSError:
            pass
        self.pid_file.unlink(missing_ok=True)
        self.lock_file.unlink(missing_ok=True)

    def __enter__(self) -> "WorkerLock":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
        self.release()


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        return _windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def _windows_pid_alive(pid: int) -> bool:
    import ctypes

    process_query_limited_information = 0x1000
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
    if not handle:
        return False
    try:
        return True
    finally:
        kernel32.CloseHandle(handle)


def print_summary(status: dict[str, object], *, stream: TextIO) -> None:
    plan = status.get("plan", {})
    result = status.get("result", {})
    stream.write(
        json.dumps(
            {
                "range_speed_ready": status.get("range_speed_ready"),
                "missing_bucket_count": status.get("missing_bucket_count"),
                "continuous_complete_buckets_from_latest": plan.get("continuous_complete_buckets_from_latest") if isinstance(plan, dict) else None,
                "processed_buckets": result.get("processed_buckets") if isinstance(result, dict) else None,
                "locked": result.get("locked") if isinstance(result, dict) else None,
                "tail_fetch_failed_buckets": result.get("tail_fetch_failed_buckets") if isinstance(result, dict) else None,
                "archive_errors_count": len(result.get("archive_errors", [])) if isinstance(result, dict) else None,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        + "\n"
    )
