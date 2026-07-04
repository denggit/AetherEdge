from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.market_data.backfill.status_store import (
    RangeBackfillStatusStore,
    now_ms,
    worker_status_is_running,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import resolve_mf_readiness

logger = logging.getLogger(__name__)

_WORKER_SCRIPT = Path(__file__).resolve().parents[2] / "tools" / "mf_feature_backfill_worker.py"


@dataclass
class _SupervisorState:
    last_restart_ms: int = 0
    last_failure_ms: int = 0
    last_archive_not_ready_ms: int = 0
    consecutive_failures: int = 0


class MfFeatureBackfillSupervisor:
    """Supervisor that ensures 1m trade-derived features are backfilled.

    Scans feature coverage on startup and periodically. When gaps are found
    and no worker is already running, launches the backfill worker as a
    subprocess.

    Does NOT block LF startup. Does NOT generate MF signals.
    """

    def __init__(
        self,
        *,
        symbol: str,
        exchange: str = "okx",
        market_db: str = "data/market_data/aether_market_data.sqlite3",
        status_path: str = "data/state/mf_feature_backfill_status.json",
        lock_path: str = "data/state/mf_feature_backfill.lock",
        global_lock_path: str = "data/state/raw_trade_backfill_global.lock",
        global_status_path: str = "data/state/raw_trade_backfill_global_status.json",
        worker_log_path: str = "logs/mf_feature_backfill_worker.out",
        required_minutes: int = 4320,
        stale_after_seconds: int = 180,
        restart_cooldown_seconds: int = 300,
        failure_cooldown_seconds: int = 3600,
        archive_not_ready_cooldown_seconds: int = 21600,
        max_seconds_per_cycle: float = 60.0,
        raw_root: str = "data/okx/raw/trades",
        contract_value: str = "0.01",
        large_trade_threshold: str = "10000",
    ) -> None:
        self.symbol = symbol
        self.exchange = exchange
        self.market_db = market_db
        self.status_path = Path(status_path)
        self.lock_path = Path(lock_path)
        self.global_lock_path = Path(global_lock_path)
        self.global_status_path = Path(global_status_path)
        self.worker_log_path = Path(worker_log_path)
        self.required_minutes = required_minutes
        self.stale_after_seconds = stale_after_seconds
        self.restart_cooldown_ms = max(0, int(restart_cooldown_seconds)) * 1000
        self.failure_cooldown_ms = max(0, int(failure_cooldown_seconds)) * 1000
        self.archive_not_ready_cooldown_ms = max(0, int(archive_not_ready_cooldown_seconds)) * 1000
        self.max_seconds_per_cycle = max_seconds_per_cycle
        self.raw_root = raw_root
        self.contract_value = contract_value
        self.large_trade_threshold = large_trade_threshold

        self._state = _SupervisorState()
        self._store: SqliteTradeFeatureStore | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def store(self) -> SqliteTradeFeatureStore:
        if self._store is None:
            self._store = SqliteTradeFeatureStore(path=self.market_db)
        return self._store

    def scan_coverage(self) -> dict[str, Any]:
        """Scan current MF feature coverage."""
        readiness = resolve_mf_readiness(
            symbol=self.symbol,
            exchange=self.exchange,
            store=self.store,
            required_minutes=self.required_minutes,
            worker_status_path=str(self.status_path),
            global_lock_path=str(self.global_lock_path),
        )
        return dict(readiness.audit())

    def check_and_launch(self) -> Mapping[str, Any]:
        """Check coverage and launch backfill worker if needed.

        Returns a status dict suitable for logging/monitoring.
        """
        coverage = self.scan_coverage()

        if coverage.get("coverage_ready"):
            return {"action": "none", "reason": "coverage_complete", "coverage": coverage}

        # Check if worker is already running
        if self._worker_running():
            return {"action": "none", "reason": "worker_already_running", "coverage": coverage}

        # Check cooldowns
        now = now_ms()
        if now - self._state.last_restart_ms < self.restart_cooldown_ms:
            return {"action": "none", "reason": "restart_cooldown", "coverage": coverage}

        if now - self._state.last_failure_ms < self.failure_cooldown_ms:
            return {"action": "none", "reason": "failure_cooldown", "coverage": coverage}

        if coverage.get("current_day_archive_not_ready"):
            if now - self._state.last_archive_not_ready_ms < self.archive_not_ready_cooldown_ms:
                return {"action": "none", "reason": "archive_not_ready_cooldown", "coverage": coverage}
            self._state.last_archive_not_ready_ms = now
            return {"action": "none", "reason": "archive_not_ready", "coverage": coverage}

        # Check global lock
        if self.global_lock_path.exists():
            return {"action": "none", "reason": "global_lock_held", "coverage": coverage}

        # Launch worker
        success = self._launch_worker()
        if success:
            self._state.last_restart_ms = now
            return {"action": "launched", "reason": "coverage_gap", "coverage": coverage}
        else:
            self._state.last_failure_ms = now
            self._state.consecutive_failures += 1
            return {"action": "launch_failed", "reason": "subprocess_error", "coverage": coverage}

    def shutdown(self) -> None:
        """Signal the supervisor to stop (no-op for now)."""
        pass

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _worker_running(self) -> bool:
        status = RangeBackfillStatusStore(self.status_path).read()
        if status is None:
            return False
        return worker_status_is_running(
            status,
            stale_after_seconds=self.stale_after_seconds,
        )

    def _launch_worker(self) -> bool:
        worker_log_dir = self.worker_log_path.parent
        worker_log_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            sys.executable,
            str(_WORKER_SCRIPT),
            "--once",
            "--mode", "prebuild",
            "--symbol", self.symbol,
            "--exchange", self.exchange,
            "--market-db", self.market_db,
            "--status-path", str(self.status_path),
            "--lock-path", str(self.lock_path),
            "--global-lock-path", str(self.global_lock_path),
            "--global-status-path", str(self.global_status_path),
            "--raw-root", self.raw_root,
            "--max-seconds-per-cycle", str(self.max_seconds_per_cycle),
            "--contract-value", str(self.contract_value),
            "--large-trade-threshold", str(self.large_trade_threshold),
            "--log-file", str(self.worker_log_path),
        ]

        try:
            with open(self.worker_log_path, "a", encoding="utf-8") as log_handle:
                log_handle.write(f"\n--- Worker launch at {now_ms()} ---\n")
                subprocess.Popen(
                    cmd,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    cwd=str(Path(__file__).resolve().parents[2]),
                )
            logger.info("Launched MF feature backfill worker | cmd=%s", " ".join(cmd))
            return True
        except OSError as exc:
            logger.error("Failed to launch MF feature backfill worker: %s", exc)
            return False
