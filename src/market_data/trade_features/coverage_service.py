from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from src.market_data.models import (
    TradeDerivedFeatureCoverage,
    TradeFeatureQuality,
)
from src.market_data.trade_features.coverage_repository import (
    RangeCoverageRows,
    TradeFeatureCoverageRepository,
)
from src.market_data.trade_features.okx_archive_calendar import (
    format_okx_archive_time,
    safe_okx_archive_end_ms,
)

_ONE_MINUTE_MS = 60_000


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
        coverage_extra = (
            dict(self.coverage.extra or {})
            if self.coverage is not None
            else {}
        )
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
            "current_day_archive_not_ready":
                self.current_day_archive_not_ready,
            "archive_publish_lag_hours": coverage_extra.get(
                "archive_publish_lag_hours"
            ),
            "calendar_safe_archive_end_ms": coverage_extra.get(
                "calendar_safe_archive_end_ms"
            ),
            "safe_archive_end_ms": coverage_extra.get(
                "safe_archive_end_ms"
            ),
            "safe_archive_end_okx": coverage_extra.get(
                "safe_archive_end_okx"
            ),
            "calendar_safe_archive_end_okx": coverage_extra.get(
                "calendar_safe_archive_end_okx"
            ),
            "latest_archive_day_deferred": coverage_extra.get(
                "latest_archive_day_deferred", False
            ),
            "latest_archive_day_deferred_reason": coverage_extra.get(
                "latest_archive_day_deferred_reason"
            ),
        }


class TradeFeatureCoverageService:
    def __init__(
        self,
        repository: TradeFeatureCoverageRepository,
    ) -> None:
        self._repository = repository

    def scan(
        self,
        *,
        symbol: str,
        exchange: str,
        required_minutes: int = 4320,
        worker_status_path: str | None = None,
        global_lock_path: str | None = None,
        reference_end_ms: int | None = None,
        now_ms: int | None = None,
        range_pct: Decimal | str | float = "0.002",
        price_step: Decimal | str | float = "1",
        archive_publish_lag_hours: float = 8.0,
    ) -> TradeDerivedFeatureCoverage:
        lag_hours = max(0.0, float(archive_publish_lag_hours))
        calendar_safe_end = safe_okx_archive_end_ms(
            now_ms,
            archive_publish_lag_hours=0.0,
        )
        safe_end = safe_okx_archive_end_ms(
            now_ms,
            archive_publish_lag_hours=lag_hours,
        )
        extra: dict[str, Any] = {
            "current_day_archive_ready": False,
            "archive_publish_lag_hours": lag_hours,
            "calendar_safe_archive_end_ms": calendar_safe_end,
            "safe_archive_end_ms": safe_end,
            "safe_archive_end_okx": format_okx_archive_time(safe_end),
            "calendar_safe_archive_end_okx": format_okx_archive_time(
                calendar_safe_end
            ),
            "latest_archive_day_deferred":
                calendar_safe_end > safe_end,
            "latest_archive_day_deferred_reason": (
                "archive_publish_lag"
                if calendar_safe_end > safe_end
                else None
            ),
        }
        if worker_status_path:
            extra["worker_status_path"] = worker_status_path
        if global_lock_path:
            extra["global_lock_path"] = global_lock_path
        return self.scan_window(
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

    def scan_window(
        self,
        *,
        symbol: str,
        exchange: str,
        required_minutes: int = 4320,
        current_day_archive_ready: bool = False,
        reference_end_ms: int | None = None,
        safe_archive_end_ms: int | None = None,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
        extra: dict[str, Any] | None = None,
    ) -> TradeDerivedFeatureCoverage:
        required_minutes = max(0, int(required_minutes))
        safe_end = (
            globals()["safe_okx_archive_end_ms"]()
            if safe_archive_end_ms is None
            else int(safe_archive_end_ms)
        )
        end_ms = safe_end if reference_end_ms is None else int(reference_end_ms)
        start_ms = end_ms - required_minutes * _ONE_MINUTE_MS + 1

        latest = self._repository.latest_complete_close_time_ms(
            symbol=symbol,
            exchange=exchange,
        )
        latest_tradebar = (
            self._repository.latest_any_tradebar_close_time_ms(
                symbol=symbol,
                exchange=exchange,
            )
        )
        latest_footprint = (
            self._repository.latest_any_footprint_close_time_ms(
                symbol=symbol,
                exchange=exchange,
            )
        )
        latest_range_footprint = (
            self._repository.latest_any_range_footprint_available_time_ms(
                symbol=symbol,
                exchange=exchange,
                range_pct=range_pct,
                price_step=price_step,
            )
        )
        rows = self._repository.load_fixed_time_quality_rows(
            symbol=symbol,
            exchange=exchange,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        tradebars = dict(rows.tradebars)
        footprints = {
            time_ms: (quality, context_available)
            for time_ms, quality, context_available in rows.footprints
        }
        counts = _fixed_time_counts(
            tradebars=tradebars,
            footprints=footprints,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        range_rows = self._repository.load_range_coverage_rows(
            symbol=symbol,
            exchange=exchange,
            start_ms=start_ms,
            end_ms=end_ms,
            range_pct=range_pct,
            price_step=price_step,
        )
        range_summary = _range_coverage_summary(
            range_rows,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        available = (
            counts["missing"] == 0
            and counts["degraded"] == 0
            and bool(range_summary["range_footprint_ready"])
        )

        reason_parts: list[str] = []
        if latest_tradebar is None and latest_footprint is None:
            reason_parts.append("no_features_stored")
        if counts["missing"] > 0:
            reason_parts.append(f"missing={counts['missing']}")
        if counts["missing_tradebar"] > 0:
            reason_parts.append(
                f"missing_tradebar={counts['missing_tradebar']}"
            )
        if counts["missing_footprint"] > 0:
            reason_parts.append(
                f"missing_footprint={counts['missing_footprint']}"
            )
        if counts["degraded"] > 0:
            reason_parts.append(f"degraded={counts['degraded']}")
            if counts["degraded_tradebar"] > 0:
                reason_parts.append(
                    f"degraded_tradebar={counts['degraded_tradebar']}"
                )
            if counts["degraded_footprint"] > 0:
                reason_parts.append(
                    f"degraded_footprint={counts['degraded_footprint']}"
                )
        missing_range = int(
            range_summary["missing_range_footprint_count"]
        )
        degraded_range = int(
            range_summary["degraded_range_footprint_count"]
        )
        if missing_range > 0:
            reason_parts.append(
                f"missing_range_footprint={missing_range}"
            )
        if degraded_range > 0:
            reason_parts.append(
                f"degraded_range_footprint={degraded_range}"
            )
        if not current_day_archive_ready:
            reason_parts.append("current_day_archive_not_ready")

        audit = dict(extra or {})
        audit.setdefault(
            "current_day_archive_ready",
            current_day_archive_ready,
        )
        audit.update(
            {
                "tradebar_complete_minutes":
                    counts["tradebar_complete"],
                "footprint_complete_minutes":
                    counts["footprint_complete"],
                "missing_tradebar": counts["missing_tradebar"],
                "missing_footprint": counts["missing_footprint"],
                "degraded_tradebar": counts["degraded_tradebar"],
                "degraded_footprint": counts["degraded_footprint"],
                "tradebar_without_footprint":
                    counts["tradebar_without_footprint"],
                "footprint_without_tradebar":
                    counts["footprint_without_tradebar"],
                "latest_any_tradebar_close_time_ms": latest_tradebar,
                "latest_any_footprint_close_time_ms": latest_footprint,
                "latest_any_range_footprint_available_time_ms":
                    latest_range_footprint,
                "safe_archive_end_ms": safe_end,
                "reference_end_ms": end_ms,
                "first_incomplete_range":
                    counts["first_incomplete"],
                "last_incomplete_range": counts["last_incomplete"],
                "first_missing_range_contiguous":
                    counts["first_missing"],
                "last_missing_range_contiguous":
                    counts["last_missing"],
                "first_incomplete_range_contiguous":
                    counts["first_incomplete"],
                "last_incomplete_range_contiguous":
                    counts["last_incomplete"],
                "first_degraded_range_contiguous":
                    counts["first_degraded"],
                "last_degraded_range_contiguous":
                    counts["last_degraded"],
                "first_degraded_footprint_range_contiguous":
                    counts["first_degraded_footprint"],
                "last_degraded_footprint_range_contiguous":
                    counts["last_degraded_footprint"],
                "first_missing_tradebar_range_contiguous":
                    counts["first_missing_tradebar"],
                "last_missing_tradebar_range_contiguous":
                    counts["last_missing_tradebar"],
                "first_missing_footprint_range_contiguous":
                    counts["first_missing_footprint"],
                "last_missing_footprint_range_contiguous":
                    counts["last_missing_footprint"],
                "first_degraded_footprint_range":
                    counts["first_degraded_footprint"],
                "fixed_time_coverage_ready": (
                    counts["missing"] == 0
                    and counts["degraded"] == 0
                ),
                **range_summary,
            }
        )
        return TradeDerivedFeatureCoverage(
            symbol=symbol,
            exchange=exchange,
            required_minutes=required_minutes,
            complete_minutes=counts["complete"],
            missing_minutes=counts["missing"],
            degraded_minutes=counts["degraded"],
            latest_complete_close_time_ms=latest,
            first_missing_range=counts["first_missing"],
            available=available,
            reason="; ".join(reason_parts),
            extra=audit,
        )

    def readiness(
        self,
        *,
        symbol: str,
        exchange: str,
        required_minutes: int = 4320,
        worker_status_path: str | None = None,
        global_lock_path: str | None = None,
        reference_end_ms: int | None = None,
        now_ms: int | None = None,
        range_pct: str = "0.002",
        price_step: str = "1",
        archive_publish_lag_hours: float = 8.0,
    ) -> TradeFeatureReadiness:
        coverage = self.scan(
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
        extra = dict(coverage.extra or {})
        required = coverage.required_minutes
        tradebar_ready = (
            int(extra.get("tradebar_complete_minutes", 0)) == required
            and int(extra.get("missing_tradebar", required)) == 0
            and int(extra.get("degraded_tradebar", required)) == 0
        )
        footprint_ready = (
            int(extra.get("footprint_complete_minutes", 0)) == required
            and int(extra.get("missing_footprint", required)) == 0
            and int(extra.get("degraded_footprint", required)) == 0
        )
        range_ready = bool(extra.get("range_footprint_ready", False))
        return TradeFeatureReadiness(
            tradebar_ready=tradebar_ready,
            fixed_time_footprint_ready=footprint_ready,
            range_footprint_ready=range_ready,
            price_ready=tradebar_ready,
            orderflow_ready=tradebar_ready,
            footprint_ready=footprint_ready,
            coverage_ready=(
                tradebar_ready and footprint_ready and range_ready
            ),
            coverage=coverage,
            worker_running=_check_worker_running(worker_status_path),
            waiting_for_global_lock=_check_lock_exists(global_lock_path),
            degraded_footprint=(
                int(extra.get("degraded_footprint", 0)) > 0
            ),
            current_day_archive_not_ready=True,
        )

    def summarize_range_window(
        self,
        *,
        symbol: str,
        exchange: str,
        start_ms: int,
        end_ms: int,
        range_pct: Decimal | str | float = Decimal("0.002"),
        price_step: Decimal | str | float = Decimal("1"),
    ) -> dict[str, object]:
        rows = self._repository.load_range_coverage_rows(
            symbol=symbol,
            exchange=exchange,
            start_ms=start_ms,
            end_ms=end_ms,
            range_pct=range_pct,
            price_step=price_step,
        )
        return _range_coverage_summary(
            rows,
            start_ms=int(start_ms),
            end_ms=int(end_ms),
        )


def _fixed_time_counts(
    *,
    tradebars: Mapping[int, str],
    footprints: Mapping[int, tuple[str, bool]],
    start_ms: int,
    end_ms: int,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "complete": 0,
        "missing": 0,
        "degraded": 0,
        "missing_tradebar": 0,
        "missing_footprint": 0,
        "degraded_tradebar": 0,
        "degraded_footprint": 0,
        "tradebar_complete": 0,
        "footprint_complete": 0,
        "tradebar_without_footprint": 0,
        "footprint_without_tradebar": 0,
    }
    contiguous: dict[str, dict[str, object]] = {}
    bucket = _bucket_start_ms(start_ms)
    end_bucket = _bucket_start_ms(end_ms)
    while bucket <= end_bucket:
        tradebar_quality = tradebars.get(bucket)
        footprint = footprints.get(bucket)
        tradebar_complete = (
            tradebar_quality == TradeFeatureQuality.COMPLETE.value
        )
        footprint_complete = bool(
            footprint is not None
            and footprint[0] == TradeFeatureQuality.COMPLETE.value
            and footprint[1]
        )
        result["tradebar_complete"] += int(tradebar_complete)
        result["footprint_complete"] += int(footprint_complete)
        result["missing_tradebar"] += int(tradebar_quality is None)
        result["missing_footprint"] += int(footprint is None)
        result["degraded_tradebar"] += int(
            tradebar_quality is not None and not tradebar_complete
        )
        result["degraded_footprint"] += int(
            footprint is not None and not footprint_complete
        )
        result["tradebar_without_footprint"] += int(
            tradebar_quality is not None and footprint is None
        )
        result["footprint_without_tradebar"] += int(
            footprint is not None and tradebar_quality is None
        )
        missing = tradebar_quality is None or footprint is None
        complete = tradebar_complete and footprint_complete
        degraded = (
            tradebar_quality is not None
            and footprint is not None
            and not complete
        )
        for key, active in (
            ("missing", missing),
            ("incomplete", not complete),
            ("degraded", degraded),
            (
                "degraded_footprint",
                footprint is not None and not footprint_complete,
            ),
            ("missing_tradebar", tradebar_quality is None),
            ("missing_footprint", footprint is None),
        ):
            _observe_contiguous_bucket(
                contiguous,
                key,
                active=active,
                bucket=bucket,
            )
        if missing:
            result["missing"] += 1
        elif complete:
            result["complete"] += 1
        else:
            result["degraded"] += 1
        bucket += _ONE_MINUTE_MS

    for key in (
        "missing",
        "incomplete",
        "degraded",
        "degraded_footprint",
        "missing_tradebar",
        "missing_footprint",
    ):
        first, last = _contiguous_bounds(contiguous, key)
        result[f"first_{key}"] = first
        result[f"last_{key}"] = last
    return result


def _range_coverage_summary(
    rows: RangeCoverageRows,
    *,
    start_ms: int,
    end_ms: int,
) -> dict[str, object]:
    complete_count = sum(
        quality == TradeFeatureQuality.COMPLETE.value and context_available
        for _, quality, context_available in rows.features
    )
    degraded_count = len(rows.features) - complete_count
    missing_gaps = _missing_gaps_from_coverage(
        start_ms=start_ms,
        end_ms=end_ms,
        intervals=rows.complete_intervals,
    )
    missing_minutes = sum(
        (gap_end - gap_start + 1 + _ONE_MINUTE_MS - 1)
        // _ONE_MINUTE_MS
        for gap_start, gap_end in missing_gaps
    )
    degraded_gaps: list[tuple[int, int]] = []
    current_start: int | None = None
    for available_time_ms, quality, context_available in rows.features:
        degraded = (
            quality != TradeFeatureQuality.COMPLETE.value
            or not context_available
        )
        if degraded and current_start is None:
            current_start = available_time_ms
        elif not degraded and current_start is not None:
            degraded_gaps.append(
                (current_start, available_time_ms - _ONE_MINUTE_MS)
            )
            current_start = None
    if current_start is not None:
        degraded_gaps.append((current_start, end_ms))
    ready = (
        rows.context_seed_time_ms is not None
        and degraded_count == 0
        and missing_minutes == 0
    )
    if rows.context_seed_time_ms is None and missing_minutes == 0:
        missing_minutes = 1
    return {
        "range_footprint_ready": ready,
        "range_footprint_complete_count": complete_count,
        "missing_range_footprint_count": missing_minutes,
        "degraded_range_footprint_count": degraded_count,
        "latest_range_footprint_available_time_ms":
            rows.latest_complete_time_ms,
        "range_footprint_context_seed_available_time_ms":
            rows.context_seed_time_ms,
        "range_footprint_coverage_marker_present":
            bool(rows.complete_intervals),
        "range_pct": rows.range_pct,
        "price_step": rows.price_step,
        "first_missing_range_footprint_range":
            (missing_gaps[0] if missing_gaps else None),
        "last_missing_range_footprint_range":
            (missing_gaps[-1] if missing_gaps else None),
        "first_degraded_range_footprint_range":
            (degraded_gaps[0] if degraded_gaps else None),
        "last_degraded_range_footprint_range":
            (degraded_gaps[-1] if degraded_gaps else None),
    }


def _missing_gaps_from_coverage(
    *,
    start_ms: int,
    end_ms: int,
    intervals: tuple[tuple[int, int], ...],
) -> list[tuple[int, int]]:
    cursor = int(start_ms)
    gaps: list[tuple[int, int]] = []
    for interval_start, interval_end in sorted(intervals):
        if interval_end < cursor:
            continue
        if interval_start > end_ms:
            break
        if interval_start > cursor:
            gaps.append((cursor, min(end_ms, interval_start - 1)))
        cursor = max(cursor, interval_end + 1)
        if cursor > end_ms:
            break
    if cursor <= end_ms:
        gaps.append((cursor, end_ms))
    return gaps


def _observe_contiguous_bucket(
    state: dict[str, dict[str, object]],
    key: str,
    *,
    active: bool,
    bucket: int,
) -> None:
    entry = state.setdefault(
        key,
        {"current": None, "first": None, "last": None},
    )
    current = entry["current"]
    if active:
        if current is None:
            entry["current"] = [bucket, bucket + _ONE_MINUTE_MS - 1]
        else:
            current[1] = bucket + _ONE_MINUTE_MS - 1
        return
    if current is None:
        return
    bounds = (int(current[0]), int(current[1]))
    if entry["first"] is None:
        entry["first"] = bounds
    entry["last"] = bounds
    entry["current"] = None


def _contiguous_bounds(
    state: dict[str, dict[str, object]],
    key: str,
) -> tuple[tuple[int, int] | None, tuple[int, int] | None]:
    entry = state.get(key)
    if entry is None:
        return None, None
    current = entry["current"]
    if current is not None:
        bounds = (int(current[0]), int(current[1]))
        if entry["first"] is None:
            entry["first"] = bounds
        entry["last"] = bounds
        entry["current"] = None
    return entry["first"], entry["last"]  # type: ignore[return-value]


def _bucket_start_ms(time_ms: int) -> int:
    return (int(time_ms) // _ONE_MINUTE_MS) * _ONE_MINUTE_MS


def _check_worker_running(status_path: str | None) -> bool:
    if not status_path:
        return False
    try:
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
        "latest_complete_close_time_ms":
            coverage.latest_complete_close_time_ms,
        "first_missing_range": coverage.first_missing_range,
        "available": coverage.available,
        "reason": coverage.reason,
        "extra": coverage.extra,
    }


__all__ = [
    "TradeFeatureCoverageService",
    "TradeFeatureReadiness",
]
