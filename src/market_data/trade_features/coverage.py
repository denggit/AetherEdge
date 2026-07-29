from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping

from src.market_data.models import (
    TradeDerivedFeatureCoverage,
    TradeFeatureBackfillTarget,
)
from src.market_data.trade_features.coverage_repository import (
    TradeFeatureCoverageRepository,
)
from src.market_data.trade_features.coverage_service import (
    TradeFeatureCoverageService,
    TradeFeatureReadiness,
)
from src.market_data.trade_features.okx_archive_calendar import (
    safe_okx_archive_end_ms,
)

_ONE_MINUTE_MS = 60_000


def trade_feature_coverage_scan(
    *,
    symbol: str,
    exchange: str,
    store: TradeFeatureCoverageRepository,
    required_minutes: int = 4320,
    worker_status_path: str | None = None,
    global_lock_path: str | None = None,
    reference_end_ms: int | None = None,
    now_ms: int | None = None,
    range_pct: str = "0.002",
    price_step: str = "1",
    archive_publish_lag_hours: float = 8.0,
) -> TradeDerivedFeatureCoverage:
    """Scan 1m and range-footprint coverage at a safe archive edge."""
    return TradeFeatureCoverageService(store).scan(
        symbol=symbol,
        exchange=exchange,
        required_minutes=required_minutes,
        worker_status_path=worker_status_path,
        global_lock_path=global_lock_path,
        reference_end_ms=reference_end_ms,
        now_ms=now_ms,
        range_pct=range_pct,
        price_step=price_step,
        archive_publish_lag_hours=archive_publish_lag_hours,
    )


def resolve_trade_feature_readiness(
    *,
    symbol: str,
    exchange: str,
    store: TradeFeatureCoverageRepository,
    required_minutes: int = 4320,
    worker_status_path: str | None = None,
    global_lock_path: str | None = None,
    reference_end_ms: int | None = None,
    now_ms: int | None = None,
    range_pct: str = "0.002",
    price_step: str = "1",
    archive_publish_lag_hours: float = 8.0,
) -> TradeFeatureReadiness:
    """Resolve independent price, order-flow, and footprint readiness gates."""
    return TradeFeatureCoverageService(store).readiness(
        symbol=symbol,
        exchange=exchange,
        required_minutes=required_minutes,
        worker_status_path=worker_status_path,
        global_lock_path=global_lock_path,
        reference_end_ms=reference_end_ms,
        now_ms=now_ms,
        range_pct=range_pct,
        price_step=price_step,
        archive_publish_lag_hours=archive_publish_lag_hours,
    )


def compute_backfill_target(
    *,
    symbol: str,
    exchange: str,
    store: TradeFeatureCoverageRepository,
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
    normalized_direction = str(direction).strip().lower()
    if normalized_direction not in {
        "oldest-to-recent",
        "recent-to-oldest",
    }:
        raise ValueError(f"unsupported backfill direction: {direction}")
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
        required_start = safe_end - required * _ONE_MINUTE_MS + 1
        if normalized_direction == "oldest-to-recent":
            start_ms = required_start
            end_ms = min(
                safe_end,
                start_ms + max_minutes * _ONE_MINUTE_MS - 1,
            )
        else:
            end_ms = safe_end
            start_ms = max(
                required_start,
                end_ms - max_minutes * _ONE_MINUTE_MS + 1,
            )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=end_ms,
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
            direction=normalized_direction,
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
            direction=normalized_direction,
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

    coverage = TradeFeatureCoverageService(store).scan_window(
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
    incomplete_key = (
        "first_incomplete_range_contiguous"
        if normalized_direction == "oldest-to-recent"
        else "last_incomplete_range_contiguous"
    )
    incomplete = extra.get(incomplete_key)
    if incomplete is None:
        incomplete = extra.get(
            "first_incomplete_range"
            if normalized_direction == "oldest-to-recent"
            else "last_incomplete_range"
        )
    if incomplete is None:
        incomplete = coverage.first_missing_range
    # ------------------------------------------------------------------
    # Range-footprint-specific gap detection.
    #
    # When fixed-time tradebars and footprints are already complete but
    # range-footprint coverage has gaps (missing markers or degraded
    # features), the target must be derived from those gaps so each
    # cycle advances the covered window rather than re-processing the
    # same earliest chunk.
    # ------------------------------------------------------------------
    if incomplete is None:
        _rfp_missing_key = (
            "first_missing_range_footprint_range"
            if normalized_direction == "oldest-to-recent"
            else "last_missing_range_footprint_range"
        )
        _rfp_missing = extra.get(_rfp_missing_key)
        _rfp_degraded_key = (
            "first_degraded_range_footprint_range"
            if normalized_direction == "oldest-to-recent"
            else "last_degraded_range_footprint_range"
        )
        _rfp_degraded = extra.get(_rfp_degraded_key)
        _rfp_gap = (
            _rfp_missing if _rfp_missing is not None else _rfp_degraded
        )
        if _rfp_gap is not None:
            _rfp_start = int(_rfp_gap[0])
            _rfp_end = int(_rfp_gap[1])
            _reason = (
                "missing_range_footprint"
                if _rfp_missing is not None
                else "degraded_range_footprint_recompute"
            )
            start_ms, end_ms = _bounded_window(
                (_rfp_start, _rfp_end),
                max_minutes=max_minutes,
                direction=normalized_direction,
            )
            return TradeFeatureBackfillTarget(
                start_ms=start_ms,
                end_ms=min(safe_end, end_ms),
                reason=_reason,
            )
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
            direction=normalized_direction,
        )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=end_ms,
            reason=range_reason,
        )
    start_ms, end_ms = _bounded_window(
        (int(incomplete[0]), int(incomplete[1])),
        max_minutes=max_minutes,
        direction=normalized_direction,
    )
    return TradeFeatureBackfillTarget(
        start_ms=start_ms,
        end_ms=min(safe_end, end_ms),
        reason="gap_from_coverage_scan",
    )


def latest_range_footprint_context_audit(
    *,
    symbol: str,
    exchange: str,
    store: TradeFeatureCoverageRepository,
    cutoff_ms: int,
    range_pct: str = "0.002",
    price_step: str = "1",
) -> Mapping[str, Any]:
    """Return the latest causal COMPLETE range-footprint context.

    This query is intentionally independent from historical coverage
    markers: callers can ask whether a causal context seed exists for a
    particular cutoff.
    """

    range_text = _decimal_text(range_pct)
    step_text = _decimal_text(price_step)
    try:
        row = store.load_latest_range_footprint_context(
            symbol=symbol,
            exchange=exchange,
            cutoff_ms=int(cutoff_ms),
            range_pct=range_text,
            price_step=step_text,
        )
    except Exception as exc:
        return {
            "range_footprint_context_ready": False,
            "range_footprint_context_cutoff_ms": int(cutoff_ms),
            "latest_range_footprint_context_available_time_ms": None,
            "latest_range_footprint_context_range_bar_id": None,
            "latest_range_footprint_context_pressure": None,
            "range_footprint_context_error": str(exc),
        }
    if row is None:
        return {
            "range_footprint_context_ready": False,
            "range_footprint_context_cutoff_ms": int(cutoff_ms),
            "latest_range_footprint_context_available_time_ms": None,
            "latest_range_footprint_context_range_bar_id": None,
            "latest_range_footprint_context_pressure": None,
        }
    return {
        "range_footprint_context_ready": True,
        "range_footprint_context_cutoff_ms": int(cutoff_ms),
        "latest_range_footprint_context_available_time_ms":
            int(row.available_time_ms),
        "latest_range_footprint_context_range_bar_id":
            int(row.range_bar_id),
        "latest_range_footprint_context_pressure": (
            str(row.fp_max_bucket_abs_delta_pressure)
        ),
    }

def _bounded_window(
    bounds: tuple[int, int], *, max_minutes: int, direction: str
) -> tuple[int, int]:
    first_ms, last_ms = bounds
    span_ms = max_minutes * _ONE_MINUTE_MS
    if str(direction).strip().lower() == "oldest-to-recent":
        return first_ms, min(last_ms, first_ms + span_ms - 1)
    return max(first_ms, last_ms - span_ms + 1), last_ms


def _decimal_text(value: object) -> str:
    return format(Decimal(str(value)).normalize(), "f")
