from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from src.market_data.models import (
    TradeDerivedFeatureCoverage,
    TradeFeatureBackfillTarget,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore

_ONE_MINUTE_MS = 60_000
_OKX_ARCHIVE_TIMEZONE = timezone(timedelta(hours=8))


@dataclass(frozen=True)
class TradeFeatureReadiness:
    """Aggregated readiness status for the trade-derived feature pipeline."""

    tradebar_ready: bool = False
    fixed_time_footprint_ready: bool = False
    range_footprint_ready: bool = False
    price_ready: bool = False
    orderflow_ready: bool = False
    footprint_ready: bool = False
    coverage_ready: bool = False

    coverage: TradeDerivedFeatureCoverage | None = None
    worker_running: bool = False
    waiting_for_global_lock: bool = False
    degraded_footprint: bool = False
    current_day_archive_not_ready: bool = False

    def audit(self) -> Mapping[str, Any]:
        return {
            "tradebar_ready": self.tradebar_ready,
            "fixed_time_footprint_ready": self.fixed_time_footprint_ready,
            "range_footprint_ready": self.range_footprint_ready,
            "price_ready": self.price_ready,
            "orderflow_ready": self.orderflow_ready,
            "footprint_ready": self.footprint_ready,
            "coverage_ready": self.coverage_ready,
            "coverage": _coverage_audit(self.coverage),
            "worker_running": self.worker_running,
            "waiting_for_global_lock": self.waiting_for_global_lock,
            "degraded_footprint": self.degraded_footprint,
            "current_day_archive_not_ready": self.current_day_archive_not_ready,
        }


def safe_okx_archive_end_ms(now_ms: int | None = None) -> int:
    """Return the last millisecond of the previous complete OKX UTC+8 day."""
    now = (
        datetime.now(UTC)
        if now_ms is None
        else datetime.fromtimestamp(int(now_ms) / 1000, tz=UTC)
    )
    okx_now = now.astimezone(_OKX_ARCHIVE_TIMEZONE)
    current_day_start = datetime(
        okx_now.year,
        okx_now.month,
        okx_now.day,
        tzinfo=_OKX_ARCHIVE_TIMEZONE,
    )
    return int(current_day_start.timestamp() * 1000) - 1


def trade_feature_coverage_scan(
    *,
    symbol: str,
    exchange: str,
    store: SqliteTradeFeatureStore,
    required_minutes: int = 4320,
    worker_status_path: str | None = None,
    global_lock_path: str | None = None,
    reference_end_ms: int | None = None,
    now_ms: int | None = None,
    range_pct: str = "0.002",
    price_step: str = "1",
) -> TradeDerivedFeatureCoverage:
    """Scan 1m and range-footprint coverage at a safe archive edge."""
    safe_end = safe_okx_archive_end_ms(now_ms)
    extra: dict[str, Any] = {
        "current_day_archive_ready": False,
        "safe_archive_end_ms": safe_end,
    }
    if worker_status_path:
        extra["worker_status_path"] = worker_status_path
    if global_lock_path:
        extra["global_lock_path"] = global_lock_path

    return store.coverage_scan(
        symbol=symbol,
        exchange=exchange,
        required_minutes=required_minutes,
        current_day_archive_ready=False,
        reference_end_ms=reference_end_ms,
        safe_archive_end_ms=safe_end,
        range_pct=range_pct,
        price_step=price_step,
        extra=extra,
    )


def resolve_trade_feature_readiness(
    *,
    symbol: str,
    exchange: str,
    store: SqliteTradeFeatureStore,
    required_minutes: int = 4320,
    worker_status_path: str | None = None,
    global_lock_path: str | None = None,
    reference_end_ms: int | None = None,
    now_ms: int | None = None,
    range_pct: str = "0.002",
    price_step: str = "1",
) -> TradeFeatureReadiness:
    """Resolve independent price, order-flow, and footprint readiness gates."""
    coverage = trade_feature_coverage_scan(
        symbol=symbol,
        exchange=exchange,
        store=store,
        required_minutes=required_minutes,
        worker_status_path=worker_status_path,
        global_lock_path=global_lock_path,
        reference_end_ms=reference_end_ms,
        now_ms=now_ms,
        range_pct=range_pct,
        price_step=price_step,
    )
    extra = dict(coverage.extra or {})
    required = coverage.required_minutes
    tradebar_ready = (
        int(extra.get("tradebar_complete_minutes", 0)) == required
        and int(extra.get("missing_tradebar", required)) == 0
        and int(extra.get("degraded_tradebar", required)) == 0
    )
    orderflow_ready = tradebar_ready
    fixed_time_footprint_ready = (
        int(extra.get("footprint_complete_minutes", 0)) == required
        and int(extra.get("missing_footprint", required)) == 0
        and int(extra.get("degraded_footprint", required)) == 0
    )
    range_footprint_ready = bool(extra.get("range_footprint_ready", False))
    coverage_ready = (
        tradebar_ready
        and fixed_time_footprint_ready
        and range_footprint_ready
    )

    return TradeFeatureReadiness(
        tradebar_ready=tradebar_ready,
        fixed_time_footprint_ready=fixed_time_footprint_ready,
        range_footprint_ready=range_footprint_ready,
        price_ready=tradebar_ready,
        orderflow_ready=orderflow_ready,
        footprint_ready=fixed_time_footprint_ready,
        coverage_ready=coverage_ready,
        coverage=coverage,
        worker_running=(
            _check_worker_running(worker_status_path)
            if worker_status_path
            else False
        ),
        waiting_for_global_lock=(
            _check_lock_exists(global_lock_path) if global_lock_path else False
        ),
        degraded_footprint=int(extra.get("degraded_footprint", 0)) > 0,
        current_day_archive_not_ready=True,
    )


def compute_backfill_target(
    *,
    symbol: str,
    exchange: str,
    store: SqliteTradeFeatureStore,
    required_minutes: int = 4320,
    max_minutes_per_cycle: int = 1440,
    direction: str = "recent-to-oldest",
    safe_archive_end_ms: int | None = None,
    now_ms: int | None = None,
    range_pct: str = "0.002",
    price_step: str = "1",
) -> TradeFeatureBackfillTarget | None:
    """Return the next recoverable safe-archive feature gap.

    Existing tradebars missing a footprint and existing degraded footprints
    are repaired before extending coverage beyond the latest stored minute.
    """
    max_minutes = max(1, int(max_minutes_per_cycle))
    required = max(1, int(required_minutes))
    safe_end = (
        safe_okx_archive_end_ms(now_ms)
        if safe_archive_end_ms is None
        else int(safe_archive_end_ms)
    )
    latest_tradebar = store.latest_any_tradebar_close_time_ms(
        symbol=symbol, exchange=exchange
    )
    latest_footprint = store.latest_any_footprint_close_time_ms(
        symbol=symbol, exchange=exchange
    )

    if latest_tradebar is None and latest_footprint is None:
        return TradeFeatureBackfillTarget(
            start_ms=safe_end - max_minutes * _ONE_MINUTE_MS + 1,
            end_ms=safe_end,
            reason="initial_empty_store",
        )

    missing_footprint = store.tradebar_without_footprint_bounds(
        symbol=symbol,
        exchange=exchange,
        end_ms=safe_end,
    )
    if missing_footprint is not None:
        start_ms, end_ms = _bounded_window(
            missing_footprint,
            max_minutes=max_minutes,
            direction=direction,
        )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=min(end_ms, safe_end),
            reason="missing_footprint_for_existing_tradebars",
        )

    degraded_footprint = store.degraded_footprint_bounds(
        symbol=symbol,
        exchange=exchange,
        end_ms=safe_end,
    )
    if degraded_footprint is not None:
        start_ms, end_ms = _bounded_window(
            degraded_footprint,
            max_minutes=max_minutes,
            direction=direction,
        )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=min(end_ms, safe_end),
            reason="degraded_footprint_recompute",
        )

    latest_values = [
        value
        for value in (latest_tradebar, latest_footprint)
        if value is not None and value <= safe_end
    ]
    latest_any = max(latest_values) if latest_values else None
    if latest_any is not None and latest_any < safe_end:
        start_ms = latest_any + 1
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=min(
                safe_end,
                start_ms + max_minutes * _ONE_MINUTE_MS - 1,
            ),
            reason="gap_after_latest",
        )

    coverage = store.coverage_scan(
        symbol=symbol,
        exchange=exchange,
        required_minutes=required,
        current_day_archive_ready=False,
        reference_end_ms=safe_end,
        safe_archive_end_ms=safe_end,
        range_pct=range_pct,
        price_step=price_step,
    )
    if coverage.available:
        return None

    extra = dict(coverage.extra or {})
    incomplete = extra.get("first_incomplete_range")
    if incomplete is None:
        incomplete = coverage.first_missing_range
    if incomplete is None:
        range_start = safe_end - required * _ONE_MINUTE_MS + 1
        range_reason = (
            "degraded_range_footprint_recompute"
            if int(extra.get("degraded_range_footprint_count", 0)) > 0
            else "missing_range_footprint"
        )
        start_ms, end_ms = _bounded_window(
            (range_start, safe_end),
            max_minutes=max_minutes,
            direction=direction,
        )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=end_ms,
            reason=range_reason,
        )
    start_ms, end_ms = (int(incomplete[0]), int(incomplete[1]))
    return TradeFeatureBackfillTarget(
        start_ms=start_ms,
        end_ms=min(
            safe_end,
            end_ms,
            start_ms + max_minutes * _ONE_MINUTE_MS - 1,
        ),
        reason="gap_from_coverage_scan",
    )


def _bounded_window(
    bounds: tuple[int, int], *, max_minutes: int, direction: str
) -> tuple[int, int]:
    first_ms, last_ms = bounds
    span_ms = max_minutes * _ONE_MINUTE_MS
    if str(direction).strip().lower() == "oldest-to-recent":
        return first_ms, min(last_ms, first_ms + span_ms - 1)
    return max(first_ms, last_ms - span_ms + 1), last_ms


def _resolve_current_day_archive_ready(*, symbol: str) -> bool:
    """The current OKX UTC+8 archive day is never considered complete."""
    _ = symbol
    return False


def _check_worker_running(status_path: str | None) -> bool:
    if not status_path:
        return False
    try:
        import json

        data = json.loads(Path(status_path).read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not data.get("running"):
            return False
        heartbeat = data.get("worker_heartbeat_ms", 0)
        return int(time.time() * 1000) - int(heartbeat) < 180_000
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return False


def _check_lock_exists(lock_path: str | None) -> bool:
    return bool(lock_path and Path(lock_path).exists())


def _coverage_audit(
    coverage: TradeDerivedFeatureCoverage | None,
) -> Mapping[str, Any] | None:
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
