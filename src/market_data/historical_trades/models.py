from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class HistoricalTradeImportSummary:
    requested_buckets: int = 0
    imported_buckets: int = 0
    failed_buckets: int = 0
    skipped_buckets: int = 0
    rows_read: int = 0
    trades_saved: int = 0
    coverage_validated_buckets: int = 0
    coverage_validation_failed_buckets: int = 0
    coverage_validation_failed_examples: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    raw_dates_required: list[str] = field(default_factory=list)
    raw_files_found: list[str] = field(default_factory=list)
    raw_files_downloaded: list[str] = field(default_factory=list)
    raw_files_missing: list[str] = field(default_factory=list)
    raw_files_not_yet_published: list[str] = field(default_factory=list)
    raw_files_skipped_incomplete_day: list[str] = field(default_factory=list)
    raw_manifest_path: str = ""
    would_download_buckets: int = 0
    would_download_trade_count: int = 0
