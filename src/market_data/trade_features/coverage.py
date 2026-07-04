from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from src.market_data.models import TradeDerivedFeatureCoverage
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore


@dataclass(frozen=True)
class MfFeatureReadiness:
    """Aggregated readiness status for MF trade-derived feature pipeline."""

    price_ready: bool = False
    orderflow_ready: bool = False
    footprint_ready: bool = False
    coverage_ready: bool = False
    mf_signal_ready: bool = False

    coverage: TradeDerivedFeatureCoverage | None = None
    worker_running: bool = False
    waiting_for_global_lock: bool = False
    degraded_footprint: bool = False
    current_day_archive_not_ready: bool = False

    def audit(self) -> Mapping[str, Any]:
        return {
            "price_ready": self.price_ready,
            "orderflow_ready": self.orderflow_ready,
            "footprint_ready": self.footprint_ready,
            "coverage_ready": self.coverage_ready,
            "mf_signal_ready": self.mf_signal_ready,
            "coverage": _coverage_audit(self.coverage),
            "worker_running": self.worker_running,
            "waiting_for_global_lock": self.waiting_for_global_lock,
            "degraded_footprint": self.degraded_footprint,
            "current_day_archive_not_ready": self.current_day_archive_not_ready,
        }


def mf_feature_coverage_scan(
    *,
    symbol: str,
    exchange: str,
    store: SqliteTradeFeatureStore,
    required_minutes: int = 4320,
    worker_status_path: str | None = None,
    global_lock_path: str | None = None,
) -> TradeDerivedFeatureCoverage:
    """Scan 1m trade-derived feature coverage from SQLite.

    Does NOT scan raw zip archives. Only reads the feature store.
    """
    current_day_ready = _current_day_archive_ready()
    extra: dict[str, Any] = {"current_day_archive_ready": current_day_ready}

    if worker_status_path:
        extra["worker_status_path"] = worker_status_path
    if global_lock_path:
        extra["global_lock_path"] = global_lock_path

    return store.coverage_scan(
        symbol=symbol,
        exchange=exchange,
        required_minutes=required_minutes,
        current_day_archive_ready=current_day_ready,
        extra=extra,
    )


def resolve_mf_readiness(
    *,
    symbol: str,
    exchange: str,
    store: SqliteTradeFeatureStore,
    required_minutes: int = 4320,
    worker_status_path: str | None = None,
    global_lock_path: str | None = None,
) -> MfFeatureReadiness:
    """Resolve all MF readiness gates.

    In R007, mf_signal_ready is ALWAYS False.
    """
    coverage = mf_feature_coverage_scan(
        symbol=symbol,
        exchange=exchange,
        store=store,
        required_minutes=required_minutes,
        worker_status_path=worker_status_path,
        global_lock_path=global_lock_path,
    )

    price_ready = coverage.complete_minutes > 0
    orderflow_ready = coverage.complete_minutes > 0
    footprint_ready = coverage.complete_minutes > 0 and coverage.degraded_minutes == 0
    coverage_ready = coverage.available

    worker_running = _check_worker_running(worker_status_path) if worker_status_path else False
    waiting_for_global_lock = _check_lock_exists(global_lock_path) if global_lock_path else False
    degraded_fp = coverage.degraded_minutes > 0

    return MfFeatureReadiness(
        price_ready=price_ready,
        orderflow_ready=orderflow_ready,
        footprint_ready=footprint_ready,
        coverage_ready=coverage_ready,
        mf_signal_ready=False,  # R007: NEVER True
        coverage=coverage,
        worker_running=worker_running,
        waiting_for_global_lock=waiting_for_global_lock,
        degraded_footprint=degraded_fp,
        current_day_archive_not_ready=not (
            coverage.extra.get("current_day_archive_ready", True)
            if coverage.extra
            else True
        ),
    )


def _current_day_archive_ready() -> bool:
    """Check if today's UTC+8 daily archive is likely available.

    The daily archive for day D usually becomes available on D+1.
    """
    now_utc = datetime.now(UTC)
    now_cst = now_utc + timedelta(hours=8)
    current_hour_cst = now_cst.hour
    # Archives for the current day are typically not available until
    # well into the next day. We use a conservative threshold.
    return current_hour_cst >= 1  # After 01:00 CST


def _check_worker_running(status_path: str | None) -> bool:
    if not status_path:
        return False
    try:
        import json
        data = json.loads(Path(status_path).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return False
        if not data.get("running"):
            return False
        heartbeat = data.get("worker_heartbeat_ms", 0)
        age_ms = int(time.time() * 1000) - int(heartbeat)
        return age_ms < 180_000  # stale after 3 min
    except (OSError, json.JSONDecodeError, ValueError):
        return False


def _check_lock_exists(lock_path: str | None) -> bool:
    if not lock_path:
        return False
    return Path(lock_path).exists()


def _coverage_audit(coverage: TradeDerivedFeatureCoverage | None) -> Mapping[str, Any] | None:
    if coverage is None:
        return None
    return {
        "required_minutes": coverage.required_minutes,
        "complete_minutes": coverage.complete_minutes,
        "missing_minutes": coverage.missing_minutes,
        "degraded_minutes": coverage.degraded_minutes,
        "latest_complete_close_time_ms": coverage.latest_complete_close_time_ms,
        "first_missing_range": coverage.first_missing_range,
        "available": coverage.available,
        "reason": coverage.reason,
        "extra": coverage.extra,
    }
