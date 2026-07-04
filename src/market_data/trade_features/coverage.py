from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from src.market_data.models import (
    TradeDerivedFeatureCoverage,
    TradeFeatureBackfillTarget,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore

_ONE_MINUTE_MS = 60_000


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
        result: dict[str, Any] = {
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
        return result


# ---------------------------------------------------------------------------
# Coverage scan
# ---------------------------------------------------------------------------

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

    Checks BOTH tradebar_1m_features AND trade_footprint_1m_features.
    Uses conservative current-day archive readiness.
    """
    current_day_ready = _resolve_current_day_archive_ready(symbol=symbol)
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
        mf_signal_ready=False,
        coverage=coverage,
        worker_running=worker_running,
        waiting_for_global_lock=waiting_for_global_lock,
        degraded_footprint=degraded_fp,
        current_day_archive_not_ready=not (
            (coverage.extra or {}).get("current_day_archive_ready", False)
        ),
    )


# ---------------------------------------------------------------------------
# Gap-driven backfill target
# ---------------------------------------------------------------------------

def compute_backfill_target(
    *,
    symbol: str,
    exchange: str,
    store: SqliteTradeFeatureStore,
    required_minutes: int = 4320,
    max_minutes_per_cycle: int = 1440,
    direction: str = "recent-to-oldest",
) -> TradeFeatureBackfillTarget | None:
    """Compute the next backfill target from coverage gaps.

    Priority: always fill the latest gap first (live/crash recovery).
    If no gaps, returns None.
    """
    # Quick scan at a larger window to find all gaps
    coverage = store.coverage_scan(
        symbol=symbol,
        exchange=exchange,
        required_minutes=max(required_minutes, max_minutes_per_cycle),
        current_day_archive_ready=_resolve_current_day_archive_ready(symbol=symbol),
    )

    if coverage.available and coverage.first_missing_range is None:
        return None

    # Use first_missing_range from coverage as the primary target
    if coverage.first_missing_range is not None:
        start_ms, end_ms = coverage.first_missing_range
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=min(end_ms, start_ms + max_minutes_per_cycle * _ONE_MINUTE_MS - 1),
            reason="gap_from_coverage_scan",
        )

    if coverage.latest_complete_close_time_ms is not None:
        # Gap after latest complete: [latest+1min .. now]
        gap_start = coverage.latest_complete_close_time_ms + 1
        gap_end = int(time.time() * 1000)
        if gap_end > gap_start:
            return TradeFeatureBackfillTarget(
                start_ms=gap_start,
                end_ms=min(gap_end, gap_start + max_minutes_per_cycle * _ONE_MINUTE_MS - 1),
                reason="gap_after_latest_complete",
            )

    return None


# ---------------------------------------------------------------------------
# Current-day archive (conservative)
# ---------------------------------------------------------------------------

def _resolve_current_day_archive_ready(*, symbol: str) -> bool:
    """Conservative check: is the OKX daily archive for the *current* day available?

    The OKX daily archive for UTC+8 day D typically becomes available on D+1
    after ~01:00 CST. Before that, HTTP requests to the archive URL return 404.

    This implementation is conservative:
    - If the current UTC+8 hour is < 1, archive is NOT ready.
    - Even at >= 1 CST, we never assume a file exists without successfully
      downloading or finding a local copy. The caller (worker) is responsible
      for confirming download success and reporting `failed_downloads`.
    """
    _ = symbol  # reserved for future per-symbol logic
    now_utc = datetime.now(UTC)
    now_cst = now_utc + timedelta(hours=8)
    current_hour_cst = now_cst.hour
    # Archives are never available before 01:00 CST on D+1.
    # Even after 01:00 they may be delayed; callers must verify.
    return current_hour_cst >= 1


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
        return age_ms < 180_000
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
