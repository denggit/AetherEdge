from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class BackfillPlan:
    exchange: str
    symbol: str
    raw_symbol: str
    range_pct: str
    bucket_ms: int
    latest_closed_bucket_start_ms: int
    latest_closed_bucket_end_ms: int
    required_bucket_starts: tuple[int, ...]
    complete_bucket_starts: tuple[int, ...]
    missing_bucket_starts: tuple[int, ...]
    dirty_bucket_starts: tuple[int, ...]
    incomplete_coverage_bucket_starts: tuple[int, ...]
    continuous_complete_buckets_from_latest: int
    range_speed_ready: bool
    nearest_missing_bucket_start_ms: int | None
    reason: str

    @property
    def missing_bucket_count(self) -> int:
        return len(set(self.missing_bucket_starts) | set(self.dirty_bucket_starts) | set(self.incomplete_coverage_bucket_starts))

    def to_dict(self) -> dict[str, object]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "raw_symbol": self.raw_symbol,
            "range_pct": self.range_pct,
            "bucket_ms": self.bucket_ms,
            "latest_closed_bucket_start_ms": self.latest_closed_bucket_start_ms,
            "latest_closed_bucket_end_ms": self.latest_closed_bucket_end_ms,
            "required_bucket_starts": list(self.required_bucket_starts),
            "complete_bucket_starts": list(self.complete_bucket_starts),
            "missing_bucket_starts": list(self.missing_bucket_starts),
            "dirty_bucket_starts": list(self.dirty_bucket_starts),
            "incomplete_coverage_bucket_starts": list(self.incomplete_coverage_bucket_starts),
            "continuous_complete_buckets_from_latest": self.continuous_complete_buckets_from_latest,
            "range_speed_ready": self.range_speed_ready,
            "nearest_missing_bucket_start_ms": self.nearest_missing_bucket_start_ms,
            "missing_bucket_count": self.missing_bucket_count,
            "reason": self.reason,
        }


@dataclass
class BackfillResult:
    processed_buckets: int = 0
    downloaded_days: int = 0
    imported_trades: int = 0
    range_bars_saved: int = 0
    aggregates_upserted: int = 0
    skipped_buckets: list[int] = field(default_factory=list)
    tail_fetch_requested_buckets: list[int] = field(default_factory=list)
    tail_fetch_succeeded_buckets: list[int] = field(default_factory=list)
    tail_fetch_failed_buckets: list[int] = field(default_factory=list)
    tail_fetch_trades_saved: int = 0
    coverage_validated_buckets: list[int] = field(default_factory=list)
    coverage_failed_buckets: list[int] = field(default_factory=list)
    archive_errors: list[str] = field(default_factory=list)
    tail_errors: list[str] = field(default_factory=list)
    locked: bool = False
    errors: list[str] = field(default_factory=list)
    # Candidate selection diagnostics (added for tail-cooldown / fallthrough).
    candidate_bucket_count: int = 0
    eligible_historical_bucket_count: int = 0
    eligible_tail_bucket_count: int = 0
    tail_cooldown_buckets: list[int] = field(default_factory=list)
    tail_deferred_buckets: list[int] = field(default_factory=list)
    selected_buckets: list[int] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "processed_buckets": self.processed_buckets,
            "downloaded_days": self.downloaded_days,
            "imported_trades": self.imported_trades,
            "range_bars_saved": self.range_bars_saved,
            "aggregates_upserted": self.aggregates_upserted,
            "skipped_buckets": list(self.skipped_buckets),
            "tail_fetch_requested_buckets": list(self.tail_fetch_requested_buckets),
            "tail_fetch_succeeded_buckets": list(self.tail_fetch_succeeded_buckets),
            "tail_fetch_failed_buckets": list(self.tail_fetch_failed_buckets),
            "tail_fetch_trades_saved": self.tail_fetch_trades_saved,
            "coverage_validated_buckets": list(self.coverage_validated_buckets),
            "coverage_failed_buckets": list(self.coverage_failed_buckets),
            "archive_errors": list(self.archive_errors),
            "tail_errors": list(self.tail_errors),
            "locked": self.locked,
            "errors": list(self.errors),
            "candidate_bucket_count": self.candidate_bucket_count,
            "eligible_historical_bucket_count": self.eligible_historical_bucket_count,
            "eligible_tail_bucket_count": self.eligible_tail_bucket_count,
            "tail_cooldown_buckets": list(self.tail_cooldown_buckets),
            "tail_deferred_buckets": list(self.tail_deferred_buckets),
            "selected_buckets": list(self.selected_buckets),
        }
