from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import time
from typing import Callable

from src.app.alerts import AppAlert
from src.market_data.models import RangeCoverageStatus
from src.market_data.range_checkpoint import (
    MICRO_REPAIR_FAILED,
    MICRO_REPAIR_QUEUED,
    RangeBuilderCheckpoint,
    RangeCheckpointRecovery,
    RangeMicroRepairJob,
    SqliteRangeCheckpointStore,
)
from src.market_data.range_repair import (
    JOURNAL_OPEN,
    RangeRepairJournalWriter,
    SqliteRangeRepairJournalStore,
)
from src.runtime.config import LiveRuntimeConfig
from src.runtime.range_micro_repair_supervisor import (
    RangeMicroRepairSupervisor,
    RangeMicroRepairSupervisorConfig,
)
from src.utils.log import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class RangeRepairBootstrapResult:
    journal_store: SqliteRangeRepairJournalStore | None
    journal_writer: RangeRepairJournalWriter | None
    micro_repair_supervisor: RangeMicroRepairSupervisor | None
    micro_repair_started: bool
    journal_bucket_start_ms: int | None = None
    checkpoint_last_trade_ts_ms: int | None = None


class RangeRepairBootstrapService:
    """Prepare startup recovery repair without touching live trade flow."""

    def __init__(
        self,
        *,
        runtime_config: LiveRuntimeConfig,
        exchange: str,
        symbol: str,
        range_pct: str,
        closed_bar_interval_ms: int,
        checkpoint_store: SqliteRangeCheckpointStore,
        emit_alert: Callable[[AppAlert], None],
        journal_store: SqliteRangeRepairJournalStore | None = None,
        journal_writer: RangeRepairJournalWriter | None = None,
        micro_repair_supervisor: RangeMicroRepairSupervisor | None = None,
        journal_store_factory=SqliteRangeRepairJournalStore,
        journal_writer_factory=RangeRepairJournalWriter,
        micro_repair_supervisor_factory=RangeMicroRepairSupervisor,
        clock_ms: Callable[[], int] | None = None,
        repo_root: Path | None = None,
    ) -> None:
        self.runtime_config = runtime_config
        self.exchange = str(exchange)
        self.symbol = symbol
        self.range_pct = str(range_pct)
        self.closed_bar_interval_ms = int(closed_bar_interval_ms)
        self.checkpoint_store = checkpoint_store
        self.emit_alert = emit_alert
        self._journal_store = journal_store
        self._journal_writer = journal_writer
        self._micro_repair_supervisor = micro_repair_supervisor
        self._journal_store_factory = journal_store_factory
        self._journal_writer_factory = journal_writer_factory
        self._micro_repair_supervisor_factory = (
            micro_repair_supervisor_factory
        )
        self._clock_ms = clock_ms or _now_ms
        self._repo_root = (
            Path(repo_root)
            if repo_root is not None
            else Path(__file__).resolve().parents[2]
        )

    def start_if_needed(
        self,
        recovery: RangeCheckpointRecovery,
        *,
        initial_bucket_ms: int | None,
    ) -> RangeRepairBootstrapResult:
        if not self.runtime_config.range_micro_repair_enabled:
            return self._result()
        checkpoint = recovery.checkpoint
        repairable = (
            recovery.recovered_from_checkpoint
            and checkpoint is not None
            and checkpoint.last_trade_ts_ms is not None
            and recovery.missing_gap_ms > 0
            and recovery.coverage_status
            != RangeCoverageStatus.COMPLETE.value
            and self.runtime_config.range_repair_journal_enabled
        )
        if not repairable:
            if recovery.missing_gap_ms > 0:
                bucket_start_ms = int(initial_bucket_ms or 0)
                logger.warning(
                    "range_micro_repair_skipped | symbol=%s "
                    "exchange=%s range_pct=%s bucket_start_ms=%s "
                    "bucket_end_ms=%s checkpoint_last_trade_ts_ms=%s "
                    "checkpoint_last_trade_id=%s missing_gap_ms=%s "
                    "coverage_before=%s coverage_after=%s failure_reason=%s",
                    self.symbol,
                    self.exchange,
                    self.range_pct,
                    bucket_start_ms,
                    bucket_start_ms + self.closed_bar_interval_ms - 1,
                    None,
                    None,
                    recovery.missing_gap_ms,
                    recovery.coverage_status,
                    recovery.coverage_status,
                    "missing_checkpoint_or_repair_journal_disabled",
                )
            return self._result()

        job = RangeMicroRepairJob(
            exchange=self.exchange,
            symbol=self.symbol,
            range_pct=self.range_pct,
            bucket_start_ms=checkpoint.bucket_start_ms,
            bucket_end_ms=checkpoint.bucket_end_ms,
            checkpoint_last_trade_id=checkpoint.last_trade_id,
            checkpoint_last_trade_ts_ms=checkpoint.last_trade_ts_ms,
            builder_state=dict(checkpoint.builder_state),
            coverage_status=recovery.coverage_status,
            missing_gap_ms=recovery.missing_gap_ms,
            journal_required=True,
            journal_status=JOURNAL_OPEN,
            status=MICRO_REPAIR_QUEUED,
            created_at_ms=self._clock_ms(),
            updated_at_ms=self._clock_ms(),
        )
        self.checkpoint_store.enqueue_micro_repair(job)
        if not self._start_journal(checkpoint):
            logger.warning(
                "startup_recovery_micro_repair skipped | symbol=%s "
                "bucket_start_ms=%s failure_reason=journal_writer_start_failed",
                checkpoint.symbol,
                checkpoint.bucket_start_ms,
            )
            return self._result()

        supervisor = self.get_micro_repair_supervisor()
        started = supervisor.start_startup_recovery(
            exchange=self.exchange,
            symbol=self.symbol,
            range_pct=self.range_pct,
            bucket_start_ms=checkpoint.bucket_start_ms,
            bucket_end_ms=checkpoint.bucket_end_ms,
            coverage_status=recovery.coverage_status,
            missing_gap_ms=recovery.missing_gap_ms,
        )
        logger.warning(
            "startup_recovery_micro_repair_subprocess_launch | "
            "symbol=%s exchange=%s "
            "range_pct=%s bucket_start_ms=%s bucket_end_ms=%s "
            "checkpoint_last_trade_ts_ms=%s checkpoint_last_trade_id=%s "
            "missing_gap_ms=%s repair_gap_start_ms=%s "
            "repair_gap_end_ms=pending_first_live_trade "
            "coverage_before=%s started=%s",
            self.symbol,
            self.exchange,
            self.range_pct,
            checkpoint.bucket_start_ms,
            checkpoint.bucket_end_ms,
            checkpoint.last_trade_ts_ms,
            checkpoint.last_trade_id,
            recovery.missing_gap_ms,
            int(checkpoint.last_trade_ts_ms) + 1,
            recovery.coverage_status,
            started,
        )
        return self._result(
            micro_repair_started=bool(started),
            journal_bucket_start_ms=checkpoint.bucket_start_ms,
            checkpoint_last_trade_ts_ms=checkpoint.last_trade_ts_ms,
        )

    def get_journal_store(self) -> SqliteRangeRepairJournalStore:
        if self._journal_store is None:
            self._journal_store = self._journal_store_factory(
                self.runtime_config.range_repair_journal_db
            )
        return self._journal_store

    def get_journal_writer(self) -> RangeRepairJournalWriter:
        if self._journal_writer is None:
            loop = asyncio.get_running_loop()

            def on_error(exc: BaseException) -> None:
                logger.warning(
                    "Range repair journal writer failed | error=%s", exc
                )
                loop.call_soon_threadsafe(
                    self.emit_alert,
                    AppAlert(
                        subject="AetherEdge range repair journal failed",
                        content=str(exc),
                        severity="warning",
                    ),
                )

            def on_invalidated(
                key: tuple[str, str, str, int],
                status: str,
                error: str,
            ) -> None:
                exchange, symbol, range_pct, bucket_start_ms = key
                timestamp = self._clock_ms()
                self.checkpoint_store.invalidate_completed_aggregate(
                    exchange=exchange,
                    symbol=symbol,
                    range_pct=range_pct,
                    bucket_end_ms=(
                        bucket_start_ms + self.closed_bar_interval_ms - 1
                    ),
                    coverage_status=(
                        RangeCoverageStatus.RECOVERED_INCOMPLETE.value
                    ),
                    missing_gap_ms=1,
                    completed_at_ms=timestamp,
                )
                self.checkpoint_store.mark_micro_repair_status(
                    exchange=exchange,
                    symbol=symbol,
                    range_pct=range_pct,
                    bucket_start_ms=bucket_start_ms,
                    status=MICRO_REPAIR_FAILED,
                    updated_at_ms=timestamp,
                    last_error=f"{status}:{error}",
                )
                logger.warning(
                    "Range repair journal invalidated; COMPLETE aggregate "
                    "revoked if present | symbol=%s bucket_start_ms=%s "
                    "journal_status=%s error=%s",
                    symbol,
                    bucket_start_ms,
                    status,
                    error,
                )

            self._journal_writer = self._journal_writer_factory(
                self.get_journal_store(),
                max_pending=(
                    self.runtime_config
                    .range_repair_journal_writer_max_pending
                ),
                flush_interval_ms=(
                    self.runtime_config
                    .range_repair_journal_flush_interval_ms
                ),
                batch_size=(
                    self.runtime_config.range_repair_journal_batch_size
                ),
                retention_hours=(
                    self.runtime_config.range_repair_journal_retention_hours
                ),
                on_error=on_error,
                on_invalidated=on_invalidated,
            )
        return self._journal_writer

    def get_micro_repair_supervisor(self) -> RangeMicroRepairSupervisor:
        if self._micro_repair_supervisor is None:
            self._micro_repair_supervisor = (
                self._micro_repair_supervisor_factory(
                    RangeMicroRepairSupervisorConfig(
                        enabled=(
                            self.runtime_config.range_micro_repair_enabled
                        ),
                        monitor_seconds=(
                            self.runtime_config
                            .range_micro_repair_monitor_seconds
                        ),
                        status_path=Path(
                            self.runtime_config
                            .range_micro_repair_status_path
                        ),
                        lock_path=Path(
                            self.runtime_config.range_micro_repair_lock_path
                        ),
                        checkpoint_db_path=Path(
                            self.runtime_config.range_checkpoint_db_path
                        ),
                        market_db_path=Path(
                            self.runtime_config.market_data_db_path
                        ),
                        journal_db_path=Path(
                            self.runtime_config.range_repair_journal_db
                        ),
                        max_gap_ms=(
                            self.runtime_config.range_micro_repair_max_gap_ms
                        ),
                        page_limit=(
                            self.runtime_config.range_micro_repair_page_limit
                        ),
                        max_pages=(
                            self.runtime_config.range_micro_repair_max_pages
                        ),
                        max_seconds=(
                            self.runtime_config.range_micro_repair_max_seconds
                        ),
                        missing_bucket_grace_seconds=(
                            self.runtime_config
                            .range_micro_repair_missing_bucket_grace_seconds
                        ),
                        repo_root=self._repo_root,
                    ),
                    on_failure=lambda reason: self.emit_alert(
                        AppAlert(
                            subject="AetherEdge range micro repair failed",
                            content=str(reason),
                            severity="warning",
                        )
                    ),
                )
            )
        return self._micro_repair_supervisor

    def _start_journal(self, checkpoint: RangeBuilderCheckpoint) -> bool:
        writer = self.get_journal_writer()
        writer.start()
        accepted = writer.submit_open(
            exchange=checkpoint.exchange,
            symbol=checkpoint.symbol,
            range_pct=checkpoint.range_pct,
            bucket_start_ms=checkpoint.bucket_start_ms,
            bucket_end_ms=checkpoint.bucket_end_ms,
            checkpoint_last_trade_ts_ms=checkpoint.last_trade_ts_ms,
            checkpoint_last_trade_id=checkpoint.last_trade_id,
            updated_at_ms=self._clock_ms(),
        )
        if not accepted:
            return False
        logger.info(
            "range_repair_journal_started | symbol=%s exchange=%s "
            "range_pct=%s bucket_start_ms=%s bucket_end_ms=%s "
            "checkpoint_last_trade_ts_ms=%s checkpoint_last_trade_id=%s",
            checkpoint.symbol,
            checkpoint.exchange,
            checkpoint.range_pct,
            checkpoint.bucket_start_ms,
            checkpoint.bucket_end_ms,
            checkpoint.last_trade_ts_ms,
            checkpoint.last_trade_id,
        )
        return True

    def _result(
        self,
        *,
        micro_repair_started: bool = False,
        journal_bucket_start_ms: int | None = None,
        checkpoint_last_trade_ts_ms: int | None = None,
    ) -> RangeRepairBootstrapResult:
        return RangeRepairBootstrapResult(
            journal_store=self._journal_store,
            journal_writer=self._journal_writer,
            micro_repair_supervisor=self._micro_repair_supervisor,
            micro_repair_started=micro_repair_started,
            journal_bucket_start_ms=journal_bucket_start_ms,
            checkpoint_last_trade_ts_ms=checkpoint_last_trade_ts_ms,
        )


def _now_ms() -> int:
    return int(time.time() * 1000)
