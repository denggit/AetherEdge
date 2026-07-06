from __future__ import annotations

import logging
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from src.market_data.backfill.coordinator import (
    RawTradeBackfillCoordinator,
)
from src.market_data.backfill.status_store import (
    RangeBackfillStatusStore,
    now_ms,
    worker_status_is_running,
)

logger = logging.getLogger(__name__)

CoverageReader = Callable[[], Mapping[str, Any]]


@dataclass(frozen=True)
class TradeFeatureBackfillConfig:
    symbol: str
    exchange: str
    worker_script: Path
    repository_root: Path
    market_db: str
    status_path: Path
    global_lock_path: Path
    global_status_path: Path
    worker_log_path: Path
    required_minutes: int = 4320
    stale_after_seconds: int = 180
    restart_cooldown_seconds: int = 300
    failure_cooldown_seconds: int = 3600
    max_seconds_per_cycle: float = 60.0
    raw_root: str = "data/okx/raw/trades"
    contract_value: str = "0.01"
    price_bucket_size: str = "1"
    range_footprint_range_pct: str = "0.002"
    range_footprint_price_step: str = "1"
    range_footprint_warmup_days: int = 1
    large_trade_threshold: str = "10000"


@dataclass
class _SupervisorState:
    last_restart_ms: int = 0
    last_failure_ms: int = 0
    consecutive_failures: int = 0


class TradeFeatureBackfillSupervisor:
    """Check trade-derived coverage and launch an injected worker."""

    def __init__(
        self,
        *,
        config: TradeFeatureBackfillConfig,
        coverage_reader: CoverageReader,
    ) -> None:
        self.config = config
        self.coverage_reader = coverage_reader
        self.restart_cooldown_ms = max(
            0, int(config.restart_cooldown_seconds)
        ) * 1000
        self.failure_cooldown_ms = max(
            0, int(config.failure_cooldown_seconds)
        ) * 1000
        self._state = _SupervisorState()

    def scan_coverage(self) -> dict[str, Any]:
        return dict(self.coverage_reader())

    def check_and_launch(self) -> Mapping[str, Any]:
        coverage = self.scan_coverage()
        if coverage.get("coverage_ready"):
            return {
                "action": "none",
                "reason": "coverage_complete",
                "coverage": coverage,
            }
        if self._worker_running():
            return {
                "action": "none",
                "reason": "worker_already_running",
                "coverage": coverage,
            }

        current_ms = now_ms()
        if (
            current_ms - self._state.last_restart_ms
            < self.restart_cooldown_ms
        ):
            return {
                "action": "none",
                "reason": "restart_cooldown",
                "coverage": coverage,
            }
        if (
            current_ms - self._state.last_failure_ms
            < self.failure_cooldown_ms
        ):
            return {
                "action": "none",
                "reason": "failure_cooldown",
                "coverage": coverage,
            }

        coordinator = RawTradeBackfillCoordinator(
            lock_path=self.config.global_lock_path,
            status_path=self.config.global_status_path,
            stale_after_seconds=self.config.stale_after_seconds,
        )
        if coordinator.has_fresh_holder():
            return {
                "action": "none",
                "reason": "global_lock_held",
                "coverage": coverage,
            }

        if self._launch_worker():
            self._state.last_restart_ms = current_ms
            return {
                "action": "launched",
                "reason": "coverage_gap",
                "coverage": coverage,
            }

        self._state.last_failure_ms = current_ms
        self._state.consecutive_failures += 1
        return {
            "action": "launch_failed",
            "reason": "subprocess_error",
            "coverage": coverage,
        }

    def shutdown(self) -> None:
        """No resident child process is owned by this supervisor."""

    def _worker_running(self) -> bool:
        status = RangeBackfillStatusStore(
            self.config.status_path
        ).read()
        if status is None:
            return False
        return worker_status_is_running(
            status,
            stale_after_seconds=self.config.stale_after_seconds,
        )

    def _launch_worker(self) -> bool:
        self.config.worker_log_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        command = [
            sys.executable,
            str(self.config.worker_script),
            "--once",
            "--mode",
            "prebuild",
            "--symbol",
            self.config.symbol,
            "--exchange",
            self.config.exchange,
            "--market-db",
            self.config.market_db,
            "--status-path",
            str(self.config.status_path),
            "--global-lock-path",
            str(self.config.global_lock_path),
            "--global-status-path",
            str(self.config.global_status_path),
            "--raw-root",
            self.config.raw_root,
            "--max-seconds-per-cycle",
            str(self.config.max_seconds_per_cycle),
            "--required-minutes",
            str(max(1, int(self.config.required_minutes))),
            "--contract-value",
            self.config.contract_value,
            "--price-bucket-size",
            self.config.price_bucket_size,
            "--range-footprint-range-pct",
            self.config.range_footprint_range_pct,
            "--range-footprint-price-step",
            self.config.range_footprint_price_step,
            "--range-footprint-warmup-days",
            str(self.config.range_footprint_warmup_days),
            "--large-trade-threshold",
            self.config.large_trade_threshold,
            "--log-file",
            str(self.config.worker_log_path),
        ]
        try:
            with self.config.worker_log_path.open(
                "a",
                encoding="utf-8",
            ) as log_handle:
                log_handle.write(
                    f"\n--- Worker launch at {now_ms()} ---\n"
                )
                subprocess.Popen(
                    command,
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    cwd=str(self.config.repository_root),
                )
            logger.info(
                "Launched trade-feature backfill worker | command=%s",
                " ".join(command),
            )
            return True
        except OSError as exc:
            logger.error(
                "Failed to launch trade-feature backfill worker: %s",
                exc,
            )
            return False


__all__ = [
    "TradeFeatureBackfillConfig",
    "TradeFeatureBackfillSupervisor",
]
