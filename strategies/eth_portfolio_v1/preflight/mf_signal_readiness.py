from __future__ import annotations

from src.market_data.models import TradeFeatureBackfillTarget
from src.market_data.storage.trade_feature_store import (
    SqliteTradeFeatureStore,
)
from src.market_data.trade_features.coverage import (
    latest_range_footprint_context_audit,
    safe_okx_archive_end_ms,
)


_ONE_MINUTE_MS = 60_000


def compute_mf_signal_backfill_target(
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
    """Return the next gap for portfolio-v1 MF signal inputs."""

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
    range_start = safe_end - required * _ONE_MINUTE_MS + 1
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
    extra = dict(coverage.extra or {})
    if int(extra.get("tradebar_complete_minutes", 0) or 0) <= 0:
        if normalized_direction == "oldest-to-recent":
            start_ms = range_start
            end_ms = min(
                safe_end,
                start_ms + max_minutes * _ONE_MINUTE_MS - 1,
            )
        else:
            end_ms = safe_end
            start_ms = max(
                range_start,
                end_ms - max_minutes * _ONE_MINUTE_MS + 1,
            )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=end_ms,
            reason="initial_empty_tradebar_store",
        )

    if int(extra.get("missing_tradebar", 0) or 0) > 0:
        gap_key = (
            "first_missing_tradebar_range_contiguous"
            if normalized_direction == "oldest-to-recent"
            else "last_missing_tradebar_range_contiguous"
        )
        gap = extra.get(gap_key) or extra.get("first_missing_range")
        if gap is not None:
            start_ms, end_ms = _bounded_window(
                (int(gap[0]), int(gap[1])),
                max_minutes=max_minutes,
                direction=normalized_direction,
            )
            return TradeFeatureBackfillTarget(
                start_ms=start_ms,
                end_ms=min(safe_end, end_ms),
                reason="missing_tradebar",
            )

    degraded_gap = _tradebar_quality_gap(
        store=store,
        symbol=symbol,
        exchange=exchange,
        start_ms=range_start,
        end_ms=safe_end,
        direction=normalized_direction,
    )
    if degraded_gap is not None:
        start_ms, end_ms = _bounded_window(
            degraded_gap,
            max_minutes=max_minutes,
            direction=normalized_direction,
        )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=min(safe_end, end_ms),
            reason="degraded_tradebar_recompute",
        )

    latest_signal_open_ms = (safe_end // _ONE_MINUTE_MS) * _ONE_MINUTE_MS
    context = latest_range_footprint_context_audit(
        symbol=symbol,
        exchange=exchange,
        store=store,
        cutoff_ms=latest_signal_open_ms,
        range_pct=range_pct,
        price_step=price_step,
    )
    if not context.get("range_footprint_context_ready", False):
        end_ms = safe_end
        start_ms = max(
            0,
            end_ms - max_minutes * _ONE_MINUTE_MS + 1,
        )
        return TradeFeatureBackfillTarget(
            start_ms=start_ms,
            end_ms=end_ms,
            reason="missing_range_footprint_context_seed",
        )
    return None


def _bounded_window(
    bounds: tuple[int, int], *, max_minutes: int, direction: str
) -> tuple[int, int]:
    first_ms, last_ms = bounds
    span_ms = max_minutes * _ONE_MINUTE_MS
    if str(direction).strip().lower() == "oldest-to-recent":
        return first_ms, min(last_ms, first_ms + span_ms - 1)
    return max(first_ms, last_ms - span_ms + 1), last_ms


def _tradebar_quality_gap(
    *,
    store: SqliteTradeFeatureStore,
    symbol: str,
    exchange: str,
    start_ms: int,
    end_ms: int,
    direction: str,
) -> tuple[int, int] | None:
    order = "ASC" if direction == "oldest-to-recent" else "DESC"
    try:
        with store._connect() as conn:
            row = conn.execute(
                f"""
                SELECT open_time_ms
                FROM tradebar_1m_features
                WHERE symbol=? AND exchange=?
                  AND open_time_ms>=? AND open_time_ms<=?
                  AND quality!='COMPLETE'
                ORDER BY open_time_ms {order}
                LIMIT 1
                """,
                (symbol, exchange, int(start_ms), int(end_ms)),
            ).fetchone()
    except Exception:
        return None
    if row is None:
        return None
    open_ms = int(row[0])
    return open_ms, open_ms + _ONE_MINUTE_MS - 1


__all__ = ["compute_mf_signal_backfill_target"]
