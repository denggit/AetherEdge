from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

from src.market_data.models import RangeCoverageStatus
from src.market_data.range_checkpoint import (
    MICRO_REPAIR_FAILED,
    MICRO_REPAIR_SKIPPED,
    MICRO_REPAIR_SUCCESS,
    RangeBucketIntegrityRecord,
    RangeCheckpointRecovery,
)
from src.utils.log import get_logger

logger = get_logger(__name__)


class RangeBucketIntegrityStatus(str, Enum):
    CLEAN = "clean"
    DEGRADED = "degraded"
    REPAIRING = "repairing"
    REPAIRED = "repaired"


@dataclass
class RangeBucketIntegrityState:
    status: RangeBucketIntegrityStatus = RangeBucketIntegrityStatus.CLEAN
    last_issue_revision: int = 0
    repaired_through_revision: int = 0
    repair_started_revision: int | None = None
    reason: str | None = None

    @property
    def complete(self) -> bool:
        return (self.status is RangeBucketIntegrityStatus.CLEAN and self.last_issue_revision == 0) or (
            self.status is RangeBucketIntegrityStatus.REPAIRED
            and self.last_issue_revision <= self.repaired_through_revision
        )


class RangeIntegrityLifecycle:
    def coverage(self, bucket_start_ms: int) -> RangeCheckpointRecovery:
        state = self._bucket_states.get(bucket_start_ms)
        if state is not None and not state.complete:
            return _coverage(RangeCoverageStatus.RECOVERED_INCOMPLETE, gap_ms=1)
        if state is not None and state.repair_started_revision is not None:
            return _coverage(RangeCoverageStatus.COMPLETE, repaired=True)
        if bucket_start_ms == self._initial_bucket_ms and self._initial_recovery is not None:
            return self._initial_recovery
        return _coverage(RangeCoverageStatus.COMPLETE)

    def _bucket_state(self, bucket_start_ms: int) -> RangeBucketIntegrityState:
        if bucket_start_ms not in self._bucket_states:
            self._bucket_states[bucket_start_ms] = RangeBucketIntegrityState()
        return self._bucket_states[bucket_start_ms]

    def mark_degraded(
        self,
        *,
        bucket_start_ms: int,
        reason: str,
        revision: int | None = None,
    ) -> None:
        state = self._bucket_state(bucket_start_ms)
        state.status = RangeBucketIntegrityStatus.DEGRADED
        state.reason = reason
        state.repair_started_revision = None
        if revision is None:
            self._integrity.mark_dropped(bucket_start_ms, reason)
            revision = self._integrity.revision
        state.last_issue_revision = max(state.last_issue_revision, int(revision))
        self._persist_bucket_state(bucket_start_ms, state)
        try:
            self.checkpoint_store.invalidate_completed_aggregate(
                exchange=self.config.exchange.value,
                symbol=self.config.symbol,
                range_pct=str(self.config.range_pct),
                bucket_end_ms=bucket_start_ms + self.config.bucket_interval_ms - 1,
                coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
                missing_gap_ms=1,
                completed_at_ms=self._clock_ms(),
            )
        except BaseException as exc:
            self.mark_failed(exc)
            raise

    def mark_trade_incomplete(
        self, event_time_ms: int, reason: str, revision: int
    ) -> None:
        self.mark_degraded(
            bucket_start_ms=self._bucket_start(event_time_ms),
            reason=reason,
            revision=revision,
        )

    def degraded_reason(self, bucket_start_ms: int) -> str | None:
        state = self._bucket_states.get(bucket_start_ms)
        return None if state is None or state.complete else state.reason

    def bucket_integrity(
        self, bucket_start_ms: int
    ) -> RangeBucketIntegrityState | None:
        state = self._bucket_states.get(bucket_start_ms)
        return None if state is None else replace(state)

    def begin_repair(self, bucket_start_ms: int) -> int:
        state = self._bucket_state(bucket_start_ms)
        state.status = RangeBucketIntegrityStatus.REPAIRING
        state.repair_started_revision = self._integrity.revision
        self._integrity.restore_window(
            bucket_start_ms,
            bucket_start_ms + self.config.bucket_interval_ms - 1,
            last_issue_revision=state.last_issue_revision,
            repaired_through_revision=state.repaired_through_revision,
            reason=state.reason or "range_repair_in_progress",
            complete=False,
        )
        self._persist_bucket_state(bucket_start_ms, state)
        return state.repair_started_revision

    def mark_repaired(self, bucket_start_ms: int, *, through_revision: int) -> bool:
        state = self._bucket_state(bucket_start_ms)
        through = int(through_revision)
        if through < 0 or through > self._integrity.revision:
            raise ValueError("invalid Range repair revision")
        if state.repair_started_revision is None:
            logger.error(
                "Range repair rejected: missing repair token | bucket_start_ms=%s",
                bucket_start_ms,
            )
            return False
        if through != state.repair_started_revision or state.last_issue_revision > through:
            return False
        self._integrity.mark_repaired(
            bucket_start_ms,
            bucket_start_ms + self.config.bucket_interval_ms - 1,
            through_revision=through,
        )
        state.repaired_through_revision = through
        state.status = RangeBucketIntegrityStatus.REPAIRED
        state.reason = None
        self._persist_bucket_state(bucket_start_ms, state)
        return state.complete

    def adopt_repaired_coverage(self, bucket_start_ms: int) -> bool:
        if (
            self._initial_bucket_ms != bucket_start_ms
            or self._initial_recovery is None
            or self._initial_recovery.coverage_status
            == RangeCoverageStatus.COMPLETE.value
        ):
            return False
        bucket_end_ms = bucket_start_ms + self.config.bucket_interval_ms - 1
        completed = self.checkpoint_store.load_completed_aggregate(
            exchange=self.config.exchange.value,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            bucket_end_ms=bucket_end_ms,
        )
        repaired_rows = self._load_store_rows(bucket_start_ms)
        if (
            completed is None
            or completed.coverage_status != RangeCoverageStatus.COMPLETE.value
            or completed.bucket_start_ms != bucket_start_ms
            or completed.bucket_end_ms != bucket_end_ms
            or len(repaired_rows) != completed.rf_bar_count
        ):
            return False
        state = self._bucket_state(bucket_start_ms)
        through = state.repair_started_revision
        if through is None:
            logger.error(
                "Range repair adoption rejected: missing repair token | "
                "bucket_start_ms=%s",
                bucket_start_ms,
            )
            return False
        if state.last_issue_revision > through or not self.mark_repaired(
            bucket_start_ms, through_revision=through
        ):
            return False
        self._initial_recovery = _coverage(
            RangeCoverageStatus.COMPLETE, repaired=True
        )
        self._trust_start_bucket_ms = bucket_start_ms
        self._bars_by_bucket.pop(bucket_start_ms, None)
        return True

    def refresh_repair_state(self, bucket_start_ms: int | None = None) -> bool:
        bucket = (
            self._initial_bucket_ms
            if bucket_start_ms is None
            else int(bucket_start_ms)
        )
        if bucket is None:
            return False
        state = self._bucket_states.get(bucket)
        if state is None or state.status is not RangeBucketIntegrityStatus.REPAIRING:
            return False
        job = self.checkpoint_store.load_micro_repair_job(
            exchange=self.config.exchange.value,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            bucket_start_ms=bucket,
        )
        if job is not None and job.status == MICRO_REPAIR_SUCCESS:
            if self.adopt_repaired_coverage(bucket):
                return True
            completed = self.checkpoint_store.load_completed_aggregate(
                exchange=self.config.exchange.value,
                symbol=self.config.symbol,
                range_pct=str(self.config.range_pct),
                bucket_end_ms=bucket + self.config.bucket_interval_ms - 1,
            )
            if completed is not None:
                self.mark_degraded(
                    bucket_start_ms=bucket,
                    reason="repair_result_validation_failed",
                    revision=state.last_issue_revision,
                )
            return False
        if job is None or job.status in {MICRO_REPAIR_FAILED, MICRO_REPAIR_SKIPPED}:
            self.mark_degraded(
                bucket_start_ms=bucket,
                reason=(
                    "orphan_repairing_state"
                    if job is None
                    else job.last_error or f"repair_job_{job.status}"
                ),
                revision=state.last_issue_revision,
            )
        return False

    def _restore_bucket_integrity(self) -> None:
        self._bucket_states = {
            row.bucket_start_ms: RangeBucketIntegrityState(
                status=(
                    RangeBucketIntegrityStatus.REPAIRED
                    if row.status == "complete"
                    and row.repair_started_revision is not None
                    else RangeBucketIntegrityStatus.CLEAN
                    if row.status == "complete"
                    else RangeBucketIntegrityStatus(row.status)
                ),
                last_issue_revision=row.last_issue_revision,
                repaired_through_revision=row.repaired_through_revision,
                repair_started_revision=row.repair_started_revision,
                reason=row.reason,
            )
            for row in self.checkpoint_store.load_bucket_integrity(
                exchange=self.config.exchange.value,
                symbol=self.config.symbol,
                range_pct=str(self.config.range_pct),
            )
        }
        for bucket_start_ms, state in self._bucket_states.items():
            self._integrity.restore_window(
                bucket_start_ms,
                bucket_start_ms + self.config.bucket_interval_ms - 1,
                last_issue_revision=state.last_issue_revision,
                repaired_through_revision=state.repaired_through_revision,
                reason=state.reason,
                complete=state.complete,
            )
            self._integrity.restore_revision(state.repair_started_revision or 0)
        self._integrity_revision = self._integrity.revision

    def _persist_bucket_state(
        self, bucket_start_ms: int, state: RangeBucketIntegrityState
    ) -> None:
        try:
            self.checkpoint_store.save_bucket_integrity(
                RangeBucketIntegrityRecord(
                    self.config.exchange.value,
                    self.config.symbol,
                    str(self.config.range_pct),
                    bucket_start_ms,
                    state.last_issue_revision,
                    state.repaired_through_revision,
                    state.repair_started_revision,
                    state.status.value,
                    state.reason,
                    self._clock_ms(),
                )
            )
        except BaseException as exc:
            self.mark_failed(exc)
            self._report("range bucket integrity write failed", exc)
            raise


def _coverage(
    status: RangeCoverageStatus,
    *,
    repaired: bool = False,
    gap_ms: int = 0,
) -> RangeCheckpointRecovery:
    return RangeCheckpointRecovery(status.value, None, None, gap_ms, repaired)
