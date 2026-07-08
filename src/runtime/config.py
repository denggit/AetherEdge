from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from src.app import AppConfig
from src.order_management import MasterFollowerPolicyConfig
from src.platform.config import get_project_env_config, load_env_config
from src.runtime.models import RuntimeMode
from src.runtime.startup_catchup import StartupCatchupConfig


MASTER_FOLLOWER_ENV_KEYS = frozenset(
    {
        "AETHER_MASTER_EXCHANGE",
        "AETHER_FOLLOWER_EXCHANGES",
        "AETHER_ENTRY_DEVIATION_ALERT_PCT",
        "AETHER_FOLLOWER_ENTRY_MAX_ATTEMPTS",
        "AETHER_FOLLOWER_ENTRY_RETRY_DELAY_SECONDS",
        "AETHER_MASTER_ENTRY_MAX_ATTEMPTS",
        "AETHER_MASTER_ENTRY_RETRY_DELAY_SECONDS",
        "AETHER_MASTER_FAIL_MANUAL_GRACE_SECONDS",
        "AETHER_CLOSE_ORPHAN_FOLLOWER_AFTER_GRACE",
        "AETHER_DO_NOT_REJOIN_MID_POSITION_AFTER_FOLLOWER_DESYNC",
    }
)


@dataclass(frozen=True)
class LiveRuntimeConfig:
    """Runtime-domain config layered on top of the existing AppConfig."""

    app: AppConfig
    mode: RuntimeMode = RuntimeMode.LEGACY_APP
    warmup_enabled: bool = True
    background_queue_maxsize: int = 1000
    scheduler_poll_seconds: float = 1.0
    closed_bar_interval: str = "4h"
    closed_bar_buffer_ms: int = 5_000
    closed_bar_retry_interval_ms: int = 5_000
    closed_bar_missing_alert_after_ms: int = 120_000
    range_pct: Decimal = Decimal("0.002")
    range_checkpoint_db_path: str = "data/state/range_builder_checkpoint.sqlite3"
    range_checkpoint_interval_ms: int = 1_000
    range_checkpoint_every_closed_bars: int = 10
    range_checkpoint_writer_max_pending: int = 8
    range_checkpoint_max_age_for_recovered_minor_ms: int = 60_000
    range_checkpoint_max_age_for_restore_ms: int = 300_000
    range_micro_repair_enabled: bool = True
    range_micro_repair_max_gap_ms: int = 600_000
    range_micro_repair_max_seconds: float = 30.0
    range_micro_repair_max_pages: int = 20
    range_micro_repair_page_limit: int = 100
    range_micro_repair_monitor_seconds: float = 30.0
    range_micro_repair_missing_bucket_grace_seconds: int = 120
    range_micro_repair_status_path: str = (
        "data/state/range_micro_repair_status.json"
    )
    range_micro_repair_lock_path: str = "data/state/range_micro_repair.lock"
    range_repair_journal_enabled: bool = True
    range_repair_journal_db: str = (
        "data/state/range_repair_trade_journal.sqlite3"
    )
    range_repair_journal_retention_hours: int = 12
    range_repair_journal_writer_max_pending: int = 20_000
    range_repair_journal_flush_interval_ms: int = 500
    range_repair_journal_batch_size: int = 1_000
    degraded_fast_margin: float = 1.05
    producer_stale_timeout_ms: int = 60_000
    range_speed_refresh_enabled: bool = True
    range_speed_refresh_seconds: float = 60.0
    range_speed_status_warning_seconds: float = 600.0
    range_backfill_enabled: bool = True
    range_backfill_required_buckets: int = 100
    range_backfill_lookback_buckets: int = 160
    range_backfill_max_buckets_per_cycle: int = 6
    range_backfill_max_days_per_cycle: int = 1
    range_backfill_sleep_seconds: float = 30.0
    range_backfill_heartbeat_stale_seconds: int = 180
    range_backfill_restart_cooldown_seconds: int = 300
    range_backfill_archive_publish_lag_hours: float = 8.0
    range_repair_failure_cooldown_seconds: int = 3600
    range_repair_archive_not_ready_cooldown_seconds: int = 21600
    range_repair_daily_retry_after_utc_hour: int = 1
    range_backfill_monitor_seconds: float = 60.0
    range_backfill_status_path: str = "data/state/range_backfill_status.json"
    range_backfill_lock_path: str = "data/state/range_backfill.lock"
    range_backfill_low_priority: bool = True
    range_backfill_chunksize: int = 50_000
    range_backfill_raw_root: str = "data/okx/raw/trades"
    range_backfill_save_raw_trades: bool = False
    range_backfill_chunk_sleep_seconds: float = 0.1
    range_backfill_max_seconds_per_cycle: float = 30.0
    range_backfill_max_trades_per_cycle: int = 300_000
    market_data_db_path: str = "data/market_data/aether_market_data.sqlite3"
    master_follower_policy: MasterFollowerPolicyConfig | None = None
    startup_catchup: StartupCatchupConfig = StartupCatchupConfig()

    @property
    def symbol(self) -> str:
        return self.app.symbol


def runtime_mode_from_env(
    *,
    defaults_path: str | Path = "config/aether_defaults.json",
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> RuntimeMode:
    defaults = _load_defaults(defaults_path)
    env = _load_runtime_env(env_file=env_file, environ=environ)
    value = env.get("AETHER_RUNTIME_MODE", str(defaults.get("runtime_mode", RuntimeMode.LEGACY_APP.value)))
    return RuntimeMode(str(value).strip().lower())


def live_runtime_config_from_app(
    app_config: AppConfig,
    *,
    defaults_path: str | Path = "config/aether_defaults.json",
    env_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> LiveRuntimeConfig:
    defaults = _load_defaults(defaults_path)
    env = _load_runtime_env(env_file=env_file, environ=environ)
    master_follower_env = _master_follower_env(env, env_file=env_file, environ=environ)
    return LiveRuntimeConfig(
        app=app_config,
        mode=RuntimeMode(str(env.get("AETHER_RUNTIME_MODE", defaults.get("runtime_mode", RuntimeMode.LEGACY_APP.value))).strip().lower()),
        warmup_enabled=_bool(env.get("AETHER_WARMUP_ENABLED", defaults.get("warmup_enabled", True))),
        background_queue_maxsize=int(env.get("AETHER_BACKGROUND_QUEUE_MAXSIZE", defaults.get("background_queue_maxsize", 1000))),
        scheduler_poll_seconds=float(env.get("AETHER_SCHEDULER_POLL_SECONDS", defaults.get("scheduler_poll_seconds", 1.0))),
        closed_bar_interval=str(env.get("AETHER_CLOSED_BAR_INTERVAL", defaults.get("closed_bar_interval", "4h"))),
        closed_bar_buffer_ms=int(env.get("AETHER_CLOSED_BAR_BUFFER_MS", defaults.get("closed_bar_buffer_ms", 5_000))),
        closed_bar_retry_interval_ms=int(env.get("AETHER_CLOSED_BAR_RETRY_INTERVAL_MS", defaults.get("closed_bar_retry_interval_ms", 5_000))),
        closed_bar_missing_alert_after_ms=int(env.get("AETHER_CLOSED_BAR_MISSING_ALERT_AFTER_MS", defaults.get("closed_bar_missing_alert_after_ms", 120_000))),
        range_pct=Decimal(str(env.get("AETHER_RANGE_PCT", defaults.get("range_pct", "0.002")))),
        range_checkpoint_db_path=str(
            env.get(
                "AETHER_RANGE_CHECKPOINT_DB",
                defaults.get(
                    "range_checkpoint_db_path",
                    "data/state/range_builder_checkpoint.sqlite3",
                ),
            )
        ),
        range_checkpoint_interval_ms=int(
            env.get(
                "AETHER_RANGE_CHECKPOINT_INTERVAL_MS",
                defaults.get("range_checkpoint_interval_ms", 1_000),
            )
        ),
        range_checkpoint_every_closed_bars=int(
            env.get(
                "AETHER_RANGE_CHECKPOINT_EVERY_CLOSED_BARS",
                defaults.get("range_checkpoint_every_closed_bars", 10),
            )
        ),
        range_checkpoint_writer_max_pending=int(
            env.get(
                "AETHER_RANGE_CHECKPOINT_WRITER_MAX_PENDING",
                defaults.get("range_checkpoint_writer_max_pending", 8),
            )
        ),
        range_checkpoint_max_age_for_recovered_minor_ms=int(
            env.get(
                "AETHER_RANGE_CHECKPOINT_MAX_AGE_FOR_RECOVERED_MINOR_MS",
                defaults.get(
                    "range_checkpoint_max_age_for_recovered_minor_ms", 60_000
                ),
            )
        ),
        range_checkpoint_max_age_for_restore_ms=int(
            env.get(
                "AETHER_RANGE_CHECKPOINT_MAX_AGE_FOR_RESTORE_MS",
                defaults.get("range_checkpoint_max_age_for_restore_ms", 300_000),
            )
        ),
        range_micro_repair_enabled=_bool(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_ENABLED",
                defaults.get("range_micro_repair_enabled", True),
            )
        ),
        range_micro_repair_max_gap_ms=int(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_MAX_GAP_MS",
                defaults.get("range_micro_repair_max_gap_ms", 600_000),
            )
        ),
        range_micro_repair_max_seconds=float(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_MAX_SECONDS",
                defaults.get("range_micro_repair_max_seconds", 30),
            )
        ),
        range_micro_repair_max_pages=int(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_MAX_PAGES",
                defaults.get("range_micro_repair_max_pages", 20),
            )
        ),
        range_micro_repair_page_limit=int(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_PAGE_LIMIT",
                defaults.get("range_micro_repair_page_limit", 100),
            )
        ),
        range_micro_repair_monitor_seconds=float(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_MONITOR_SECONDS",
                defaults.get("range_micro_repair_monitor_seconds", 30),
            )
        ),
        range_micro_repair_missing_bucket_grace_seconds=int(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_MISSING_BUCKET_GRACE_SECONDS",
                defaults.get(
                    "range_micro_repair_missing_bucket_grace_seconds", 120
                ),
            )
        ),
        range_micro_repair_status_path=str(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_STATUS_PATH",
                defaults.get(
                    "range_micro_repair_status_path",
                    "data/state/range_micro_repair_status.json",
                ),
            )
        ),
        range_micro_repair_lock_path=str(
            env.get(
                "AETHER_RANGE_MICRO_REPAIR_LOCK_PATH",
                defaults.get(
                    "range_micro_repair_lock_path",
                    "data/state/range_micro_repair.lock",
                ),
            )
        ),
        range_repair_journal_enabled=_bool(
            env.get(
                "AETHER_RANGE_REPAIR_JOURNAL_ENABLED",
                defaults.get("range_repair_journal_enabled", True),
            )
        ),
        range_repair_journal_db=str(
            env.get(
                "AETHER_RANGE_REPAIR_JOURNAL_DB",
                defaults.get(
                    "range_repair_journal_db",
                    "data/state/range_repair_trade_journal.sqlite3",
                ),
            )
        ),
        range_repair_journal_retention_hours=int(
            env.get(
                "AETHER_RANGE_REPAIR_JOURNAL_RETENTION_HOURS",
                defaults.get(
                    "range_repair_journal_retention_hours", 12
                ),
            )
        ),
        range_repair_journal_writer_max_pending=int(
            env.get(
                "AETHER_RANGE_REPAIR_JOURNAL_WRITER_MAX_PENDING",
                defaults.get(
                    "range_repair_journal_writer_max_pending", 20_000
                ),
            )
        ),
        range_repair_journal_flush_interval_ms=int(
            env.get(
                "AETHER_RANGE_REPAIR_JOURNAL_FLUSH_INTERVAL_MS",
                defaults.get(
                    "range_repair_journal_flush_interval_ms", 500
                ),
            )
        ),
        range_repair_journal_batch_size=int(
            env.get(
                "AETHER_RANGE_REPAIR_JOURNAL_BATCH_SIZE",
                defaults.get("range_repair_journal_batch_size", 1_000),
            )
        ),
        degraded_fast_margin=float(
            env.get(
                "AETHER_RANGE_DEGRADED_FAST_MARGIN",
                defaults.get("degraded_fast_margin", 1.05),
            )
        ),
        producer_stale_timeout_ms=int(env.get("AETHER_PRODUCER_STALE_TIMEOUT_MS", defaults.get("producer_stale_timeout_ms", 60_000))),
        range_speed_refresh_enabled=_bool(env.get("AETHER_RANGE_SPEED_REFRESH_ENABLED", defaults.get("range_speed_refresh_enabled", True))),
        range_speed_refresh_seconds=float(env.get("AETHER_RANGE_SPEED_REFRESH_SECONDS", defaults.get("range_speed_refresh_seconds", 60))),
        range_speed_status_warning_seconds=float(env.get("AETHER_RANGE_SPEED_STATUS_WARNING_SECONDS", defaults.get("range_speed_status_warning_seconds", 600))),
        range_backfill_enabled=_bool(env.get("AETHER_RANGE_BACKFILL_ENABLED", defaults.get("range_backfill_enabled", True))),
        range_backfill_required_buckets=int(env.get("AETHER_RANGE_BACKFILL_REQUIRED_BUCKETS", defaults.get("range_backfill_required_buckets", 100))),
        range_backfill_lookback_buckets=int(env.get("AETHER_RANGE_BACKFILL_LOOKBACK_BUCKETS", defaults.get("range_backfill_lookback_buckets", 160))),
        range_backfill_max_buckets_per_cycle=int(env.get("AETHER_RANGE_BACKFILL_MAX_BUCKETS_PER_CYCLE", defaults.get("range_backfill_max_buckets_per_cycle", 6))),
        range_backfill_max_days_per_cycle=int(env.get("AETHER_RANGE_BACKFILL_MAX_DAYS_PER_CYCLE", defaults.get("range_backfill_max_days_per_cycle", 1))),
        range_backfill_sleep_seconds=float(env.get("AETHER_RANGE_BACKFILL_SLEEP_SECONDS", defaults.get("range_backfill_sleep_seconds", 30))),
        range_backfill_heartbeat_stale_seconds=int(env.get("AETHER_RANGE_BACKFILL_HEARTBEAT_STALE_SECONDS", defaults.get("range_backfill_heartbeat_stale_seconds", 180))),
        range_backfill_restart_cooldown_seconds=int(env.get("AETHER_RANGE_BACKFILL_RESTART_COOLDOWN_SECONDS", defaults.get("range_backfill_restart_cooldown_seconds", 300))),
        range_backfill_archive_publish_lag_hours=float(env.get("AETHER_RANGE_ARCHIVE_PUBLISH_LAG_HOURS", defaults.get("range_backfill_archive_publish_lag_hours", 8.0))),
        range_repair_failure_cooldown_seconds=int(env.get("AETHER_RANGE_REPAIR_FAILURE_COOLDOWN_SECONDS", defaults.get("range_repair_failure_cooldown_seconds", 3600))),
        range_repair_archive_not_ready_cooldown_seconds=int(env.get("AETHER_RANGE_REPAIR_ARCHIVE_NOT_READY_COOLDOWN_SECONDS", defaults.get("range_repair_archive_not_ready_cooldown_seconds", 21600))),
        range_repair_daily_retry_after_utc_hour=int(env.get("AETHER_RANGE_REPAIR_DAILY_RETRY_AFTER_UTC_HOUR", defaults.get("range_repair_daily_retry_after_utc_hour", 1))),
        range_backfill_monitor_seconds=float(env.get("AETHER_RANGE_BACKFILL_MONITOR_SECONDS", defaults.get("range_backfill_monitor_seconds", 60))),
        range_backfill_status_path=str(env.get("AETHER_RANGE_BACKFILL_STATUS_PATH", defaults.get("range_backfill_status_path", "data/state/range_backfill_status.json"))),
        range_backfill_lock_path=str(env.get("AETHER_RANGE_BACKFILL_LOCK_PATH", defaults.get("range_backfill_lock_path", "data/state/range_backfill.lock"))),
        range_backfill_low_priority=_bool(env.get("AETHER_RANGE_BACKFILL_LOW_PRIORITY", defaults.get("range_backfill_low_priority", True))),
        range_backfill_chunksize=int(env.get("AETHER_RANGE_BACKFILL_CHUNKSIZE", defaults.get("range_backfill_chunksize", 50_000))),
        range_backfill_raw_root=str(env.get("AETHER_RANGE_BACKFILL_RAW_ROOT", defaults.get("range_backfill_raw_root", "data/okx/raw/trades"))),
        range_backfill_save_raw_trades=_bool(env.get("AETHER_RANGE_BACKFILL_SAVE_RAW_TRADES", defaults.get("range_backfill_save_raw_trades", False))),
        range_backfill_chunk_sleep_seconds=float(env.get("AETHER_RANGE_BACKFILL_CHUNK_SLEEP_SECONDS", defaults.get("range_backfill_chunk_sleep_seconds", 0.1))),
        range_backfill_max_seconds_per_cycle=float(env.get("AETHER_RANGE_BACKFILL_MAX_SECONDS_PER_CYCLE", defaults.get("range_backfill_max_seconds_per_cycle", 30))),
        range_backfill_max_trades_per_cycle=int(env.get("AETHER_RANGE_BACKFILL_MAX_TRADES_PER_CYCLE", defaults.get("range_backfill_max_trades_per_cycle", 300_000))),
        market_data_db_path=str(env.get("AETHER_MARKET_DATA_DB", defaults.get("market_data_db_path", "data/market_data/aether_market_data.sqlite3"))),
        master_follower_policy=MasterFollowerPolicyConfig.from_env(
            app_exchanges=app_config.exchanges,
            data_exchange=app_config.data_exchange,
            env=master_follower_env,
        ),
        startup_catchup=StartupCatchupConfig.from_mapping(defaults.get("startup_catchup")),
    )


def _load_runtime_env(*, env_file: str | Path | None, environ: Mapping[str, str] | None) -> dict[str, str]:
    if environ is None and env_file is None:
        return dict(get_project_env_config().values)
    if environ is not None and env_file is None:
        # Synthetic environ mappings used by tests should be hermetic: do not
        # inherit the developer's project config.
        return {str(key): str(value) for key, value in environ.items()}
    return dict(load_env_config(env_file, environ=environ))


def _load_defaults(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _master_follower_env(
    env: Mapping[str, str],
    *,
    env_file: str | Path | None,
    environ: Mapping[str, str] | None,
) -> dict[str, str]:
    """Keep injected runtime-config tests from inheriting project role config."""

    values = dict(env)
    if environ is None or env_file is not None:
        return values

    for key in MASTER_FOLLOWER_ENV_KEYS:
        values.pop(key, None)
    values.update({str(key): str(value) for key, value in environ.items() if str(key) in MASTER_FOLLOWER_ENV_KEYS})
    return values
