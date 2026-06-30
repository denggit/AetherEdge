from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BucketGap:
    bucket_start_ms: int
    bucket_end_ms: int


@dataclass(frozen=True)
class RangeSpeedCoverage:
    symbol: str
    exchange: str
    range_pct: str
    bucket_interval: str
    complete_history: int
    required_buckets: int
    missing_buckets: tuple[BucketGap, ...]
    current_closed_bucket_end_ms: int
    latest_complete_bucket_end_ms: int | None

    @property
    def missing_periods(self) -> int:
        return max(0, self.required_buckets - self.complete_history)

    @property
    def available(self) -> bool:
        return self.complete_history >= self.required_buckets


@dataclass(frozen=True)
class RangeBackfillRequest:
    symbol: str
    exchange: str = "okx"
    raw_symbol: str | None = None
    range_pct: str = "0.002"
    bucket_interval: str = "4h"
    required_buckets: int = 100
    lookback_buckets: int = 160
    max_buckets_per_cycle: int = 6
    max_days_per_cycle: int = 1
    market_db_path: Path = Path("data/market_data/aether_market_data.sqlite3")
    checkpoint_db_path: Path = Path("data/state/range_builder_checkpoint.sqlite3")
    raw_root: Path = Path("data/okx/raw/trades")
    status_path: Path = Path("data/state/range_backfill_status.json")
    lock_path: Path = Path("data/state/range_backfill.lock")
    chunksize: int = 50_000
    mode: str = "prebuild"
    direction: str = "oldest-to-recent"
    allow_download: bool = True
    dry_run: bool = False
    force: bool = False
    sleep_seconds: float = 0.0
    contract_value: str = "1"


@dataclass(frozen=True)
class RangeBackfillSummary:
    symbol: str
    exchange: str
    range_pct: str
    bucket_interval: str
    target_buckets: int
    complete_before: int
    complete_after: int
    missing_before: int
    missing_after: int
    downloaded_files: int = 0
    trades_loaded: int = 0
    range_bars_written: int = 0
    aggregates_written: int = 0
    elapsed_seconds: float = 0.0
    status: str = "ok"
    last_error: str | None = None
