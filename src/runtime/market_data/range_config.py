from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, TypeVar

from src.runtime.config import _load_defaults, _load_runtime_env


T = TypeVar("T")


@dataclass(frozen=True)
class RangeRuntimeConfig:
    """Startup-only configuration owned by the Range runtime module."""

    range_pct: Decimal = Decimal("0.002")
    checkpoint_db_path: str = "data/state/range_builder_checkpoint.sqlite3"
    checkpoint_interval_ms: int = 1_000
    checkpoint_every_closed_bars: int = 10
    checkpoint_writer_max_pending: int = 8
    checkpoint_max_age_for_recovered_minor_ms: int = 60_000
    checkpoint_max_age_for_restore_ms: int = 300_000
    micro_repair_enabled: bool = True
    micro_repair_max_gap_ms: int = 600_000
    micro_repair_max_seconds: float = 30.0
    micro_repair_max_pages: int = 20
    micro_repair_page_limit: int = 100
    micro_repair_monitor_seconds: float = 30.0
    micro_repair_missing_bucket_grace_seconds: int = 120
    micro_repair_status_path: str = "data/state/range_micro_repair_status.json"
    micro_repair_lock_path: str = "data/state/range_micro_repair.lock"
    repair_journal_enabled: bool = True
    repair_journal_db: str = "data/state/range_repair_trade_journal.sqlite3"
    repair_journal_retention_hours: int = 12
    repair_journal_writer_max_pending: int = 20_000
    repair_journal_flush_interval_ms: int = 500
    repair_journal_batch_size: int = 1_000
    degraded_fast_margin: float = 1.05
    speed_refresh_enabled: bool = True
    speed_refresh_seconds: float = 60.0
    speed_status_warning_seconds: float = 600.0
    backfill_enabled: bool = True
    backfill_required_buckets: int = 100
    backfill_lookback_buckets: int = 160
    backfill_max_buckets_per_cycle: int = 6
    backfill_max_days_per_cycle: int = 1
    backfill_sleep_seconds: float = 30.0
    backfill_heartbeat_stale_seconds: int = 180
    backfill_restart_cooldown_seconds: int = 300
    backfill_archive_publish_lag_hours: float = 8.0
    repair_failure_cooldown_seconds: int = 3_600
    repair_archive_not_ready_cooldown_seconds: int = 21_600
    repair_daily_retry_after_utc_hour: int = 1
    backfill_monitor_seconds: float = 60.0
    backfill_status_path: str = "data/state/range_backfill_status.json"
    backfill_lock_path: str = "data/state/range_backfill.lock"
    backfill_low_priority: bool = True
    backfill_chunksize: int = 50_000
    backfill_raw_root: str = "data/okx/raw/trades"
    backfill_save_raw_trades: bool = False
    backfill_chunk_sleep_seconds: float = 0.1
    backfill_max_seconds_per_cycle: float = 30.0
    backfill_max_trades_per_cycle: int = 300_000
    market_data_db_path: str = "data/market_data/aether_market_data.sqlite3"


def range_runtime_config_from_env(
    *,
    defaults_path: str | Path = "config/aether_defaults.json",
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> RangeRuntimeConfig:
    defaults = _load_defaults(defaults_path)
    env = _load_runtime_env(env_file=env_file, environ=environ)

    def value(
        env_key: str,
        default_key: str,
        fallback: T,
        convert: Callable[[Any], T],
    ) -> T:
        return convert(env.get(env_key, defaults.get(default_key, fallback)))

    boolean = lambda raw: str(raw).strip().lower() in {"1", "true", "yes", "on"}
    return RangeRuntimeConfig(
        range_pct=value("AETHER_RANGE_PCT", "range_pct", "0.002", lambda raw: Decimal(str(raw))),
        checkpoint_db_path=value("AETHER_RANGE_CHECKPOINT_DB", "range_checkpoint_db_path", "data/state/range_builder_checkpoint.sqlite3", str),
        checkpoint_interval_ms=value("AETHER_RANGE_CHECKPOINT_INTERVAL_MS", "range_checkpoint_interval_ms", 1_000, int),
        checkpoint_every_closed_bars=value("AETHER_RANGE_CHECKPOINT_EVERY_CLOSED_BARS", "range_checkpoint_every_closed_bars", 10, int),
        checkpoint_writer_max_pending=value("AETHER_RANGE_CHECKPOINT_WRITER_MAX_PENDING", "range_checkpoint_writer_max_pending", 8, int),
        checkpoint_max_age_for_recovered_minor_ms=value("AETHER_RANGE_CHECKPOINT_MAX_AGE_FOR_RECOVERED_MINOR_MS", "range_checkpoint_max_age_for_recovered_minor_ms", 60_000, int),
        checkpoint_max_age_for_restore_ms=value("AETHER_RANGE_CHECKPOINT_MAX_AGE_FOR_RESTORE_MS", "range_checkpoint_max_age_for_restore_ms", 300_000, int),
        micro_repair_enabled=value("AETHER_RANGE_MICRO_REPAIR_ENABLED", "range_micro_repair_enabled", True, boolean),
        micro_repair_max_gap_ms=value("AETHER_RANGE_MICRO_REPAIR_MAX_GAP_MS", "range_micro_repair_max_gap_ms", 600_000, int),
        micro_repair_max_seconds=value("AETHER_RANGE_MICRO_REPAIR_MAX_SECONDS", "range_micro_repair_max_seconds", 30.0, float),
        micro_repair_max_pages=value("AETHER_RANGE_MICRO_REPAIR_MAX_PAGES", "range_micro_repair_max_pages", 20, int),
        micro_repair_page_limit=value("AETHER_RANGE_MICRO_REPAIR_PAGE_LIMIT", "range_micro_repair_page_limit", 100, int),
        micro_repair_monitor_seconds=value("AETHER_RANGE_MICRO_REPAIR_MONITOR_SECONDS", "range_micro_repair_monitor_seconds", 30.0, float),
        micro_repair_missing_bucket_grace_seconds=value("AETHER_RANGE_MICRO_REPAIR_MISSING_BUCKET_GRACE_SECONDS", "range_micro_repair_missing_bucket_grace_seconds", 120, int),
        micro_repair_status_path=value("AETHER_RANGE_MICRO_REPAIR_STATUS_PATH", "range_micro_repair_status_path", "data/state/range_micro_repair_status.json", str),
        micro_repair_lock_path=value("AETHER_RANGE_MICRO_REPAIR_LOCK_PATH", "range_micro_repair_lock_path", "data/state/range_micro_repair.lock", str),
        repair_journal_enabled=value("AETHER_RANGE_REPAIR_JOURNAL_ENABLED", "range_repair_journal_enabled", True, boolean),
        repair_journal_db=value("AETHER_RANGE_REPAIR_JOURNAL_DB", "range_repair_journal_db", "data/state/range_repair_trade_journal.sqlite3", str),
        repair_journal_retention_hours=value("AETHER_RANGE_REPAIR_JOURNAL_RETENTION_HOURS", "range_repair_journal_retention_hours", 12, int),
        repair_journal_writer_max_pending=value("AETHER_RANGE_REPAIR_JOURNAL_WRITER_MAX_PENDING", "range_repair_journal_writer_max_pending", 20_000, int),
        repair_journal_flush_interval_ms=value("AETHER_RANGE_REPAIR_JOURNAL_FLUSH_INTERVAL_MS", "range_repair_journal_flush_interval_ms", 500, int),
        repair_journal_batch_size=value("AETHER_RANGE_REPAIR_JOURNAL_BATCH_SIZE", "range_repair_journal_batch_size", 1_000, int),
        degraded_fast_margin=value("AETHER_RANGE_DEGRADED_FAST_MARGIN", "degraded_fast_margin", 1.05, float),
        speed_refresh_enabled=value("AETHER_RANGE_SPEED_REFRESH_ENABLED", "range_speed_refresh_enabled", True, boolean),
        speed_refresh_seconds=value("AETHER_RANGE_SPEED_REFRESH_SECONDS", "range_speed_refresh_seconds", 60.0, float),
        speed_status_warning_seconds=value("AETHER_RANGE_SPEED_STATUS_WARNING_SECONDS", "range_speed_status_warning_seconds", 600.0, float),
        backfill_enabled=value("AETHER_RANGE_BACKFILL_ENABLED", "range_backfill_enabled", True, boolean),
        backfill_required_buckets=value("AETHER_RANGE_BACKFILL_REQUIRED_BUCKETS", "range_backfill_required_buckets", 100, int),
        backfill_lookback_buckets=value("AETHER_RANGE_BACKFILL_LOOKBACK_BUCKETS", "range_backfill_lookback_buckets", 160, int),
        backfill_max_buckets_per_cycle=value("AETHER_RANGE_BACKFILL_MAX_BUCKETS_PER_CYCLE", "range_backfill_max_buckets_per_cycle", 6, int),
        backfill_max_days_per_cycle=value("AETHER_RANGE_BACKFILL_MAX_DAYS_PER_CYCLE", "range_backfill_max_days_per_cycle", 1, int),
        backfill_sleep_seconds=value("AETHER_RANGE_BACKFILL_SLEEP_SECONDS", "range_backfill_sleep_seconds", 30.0, float),
        backfill_heartbeat_stale_seconds=value("AETHER_RANGE_BACKFILL_HEARTBEAT_STALE_SECONDS", "range_backfill_heartbeat_stale_seconds", 180, int),
        backfill_restart_cooldown_seconds=value("AETHER_RANGE_BACKFILL_RESTART_COOLDOWN_SECONDS", "range_backfill_restart_cooldown_seconds", 300, int),
        backfill_archive_publish_lag_hours=value("AETHER_RANGE_ARCHIVE_PUBLISH_LAG_HOURS", "range_backfill_archive_publish_lag_hours", 8.0, float),
        repair_failure_cooldown_seconds=value("AETHER_RANGE_REPAIR_FAILURE_COOLDOWN_SECONDS", "range_repair_failure_cooldown_seconds", 3_600, int),
        repair_archive_not_ready_cooldown_seconds=value("AETHER_RANGE_REPAIR_ARCHIVE_NOT_READY_COOLDOWN_SECONDS", "range_repair_archive_not_ready_cooldown_seconds", 21_600, int),
        repair_daily_retry_after_utc_hour=value("AETHER_RANGE_REPAIR_DAILY_RETRY_AFTER_UTC_HOUR", "range_repair_daily_retry_after_utc_hour", 1, int),
        backfill_monitor_seconds=value("AETHER_RANGE_BACKFILL_MONITOR_SECONDS", "range_backfill_monitor_seconds", 60.0, float),
        backfill_status_path=value("AETHER_RANGE_BACKFILL_STATUS_PATH", "range_backfill_status_path", "data/state/range_backfill_status.json", str),
        backfill_lock_path=value("AETHER_RANGE_BACKFILL_LOCK_PATH", "range_backfill_lock_path", "data/state/range_backfill.lock", str),
        backfill_low_priority=value("AETHER_RANGE_BACKFILL_LOW_PRIORITY", "range_backfill_low_priority", True, boolean),
        backfill_chunksize=value("AETHER_RANGE_BACKFILL_CHUNKSIZE", "range_backfill_chunksize", 50_000, int),
        backfill_raw_root=value("AETHER_RANGE_BACKFILL_RAW_ROOT", "range_backfill_raw_root", "data/okx/raw/trades", str),
        backfill_save_raw_trades=value("AETHER_RANGE_BACKFILL_SAVE_RAW_TRADES", "range_backfill_save_raw_trades", False, boolean),
        backfill_chunk_sleep_seconds=value("AETHER_RANGE_BACKFILL_CHUNK_SLEEP_SECONDS", "range_backfill_chunk_sleep_seconds", 0.1, float),
        backfill_max_seconds_per_cycle=value("AETHER_RANGE_BACKFILL_MAX_SECONDS_PER_CYCLE", "range_backfill_max_seconds_per_cycle", 30.0, float),
        backfill_max_trades_per_cycle=value("AETHER_RANGE_BACKFILL_MAX_TRADES_PER_CYCLE", "range_backfill_max_trades_per_cycle", 300_000, int),
        market_data_db_path=value("AETHER_MARKET_DATA_DB", "market_data_db_path", "data/market_data/aether_market_data.sqlite3", str),
    )


__all__ = ["RangeRuntimeConfig", "range_runtime_config_from_env"]
