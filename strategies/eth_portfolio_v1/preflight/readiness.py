from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.models import TimeRange
from src.market_data.storage.trade_feature_store import (
    SqliteTradeFeatureStore,
)
from src.market_data.trade_features.coverage import (
    latest_range_footprint_context_audit,
    resolve_trade_feature_readiness,
    safe_okx_archive_end_ms,
)


_FOUR_HOURS_MS = 4 * 60 * 60 * 1000
_MINUTE_MS = 60_000
_OKX_ARCHIVE_TIMEZONE = timezone(timedelta(hours=8))


@dataclass(frozen=True)
class PortfolioV1ReadinessResult:
    lf: dict[str, Any]
    mf: dict[str, Any]
    causal: dict[str, Any]
    issues: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not self.issues

    def audit(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "lf_data_readiness": dict(self.lf),
            "mf_data_readiness": dict(self.mf),
            "causal_audit": dict(self.causal),
            "issues": list(self.issues),
        }


@dataclass(frozen=True)
class _RangeFootprintProbe:
    available_time_ms: int
    quality: str
    context_available: bool
    fp_max_bucket_abs_delta_pressure: str | None


class PortfolioV1ReadinessInspector:
    """Read persisted LF/MF evidence and fail when readiness is unproven."""

    def __init__(
        self,
        *,
        symbol: str,
        market_data_db_path: str | Path,
        range_checkpoint_db_path: str | Path,
        exchange: str = "okx",
        range_pct: str = "0.002",
        price_step: str = "1",
        closed_kline_interval: str = "4h",
        lf_min_records: int = 2000,
        range_speed_min_periods: int = 100,
        mf_required_minutes: int = 4320,
        large_share_min_samples: int = 43200,
        large_share_window_days: int = 90,
        lf_max_staleness_ms: int = _FOUR_HOURS_MS + 10 * _MINUTE_MS,
        mf_max_staleness_ms: int = 5 * _MINUTE_MS,
        readiness_mode: Literal[
            "historical_preflight",
            "live_freshness",
        ] = "historical_preflight",
        archive_publish_lag_hours: float = 8.0,
        now_ms: int | None = None,
    ) -> None:
        self.symbol = symbol
        self.market_data_db_path = Path(market_data_db_path)
        self.range_checkpoint_db_path = Path(range_checkpoint_db_path)
        self.exchange = str(exchange).strip().lower()
        self.range_pct = str(range_pct)
        self.price_step = str(price_step)
        self.closed_kline_interval = closed_kline_interval
        self.lf_min_records = max(1, int(lf_min_records))
        self.range_speed_min_periods = max(
            1, int(range_speed_min_periods)
        )
        self.mf_required_minutes = max(1, int(mf_required_minutes))
        self.large_share_min_samples = max(
            1, int(large_share_min_samples)
        )
        self.large_share_window_days = max(
            1, int(large_share_window_days)
        )
        self.lf_max_staleness_ms = max(0, int(lf_max_staleness_ms))
        self.mf_max_staleness_ms = max(0, int(mf_max_staleness_ms))
        if readiness_mode not in {
            "historical_preflight",
            "live_freshness",
        }:
            raise ValueError(
                f"unsupported MF readiness mode: {readiness_mode}"
            )
        self.readiness_mode = readiness_mode
        self.archive_publish_lag_hours = max(
            0.0,
            float(archive_publish_lag_hours),
        )
        self.now_ms = (
            int(now_ms) if now_ms is not None else int(time.time() * 1000)
        )

    def inspect(self) -> PortfolioV1ReadinessResult:
        issues: list[str] = []
        lf = self._inspect_lf(issues)
        mf = self._inspect_mf(issues)
        causal = self._causal_audit(lf=lf, mf=mf, issues=issues)
        return PortfolioV1ReadinessResult(
            lf=lf,
            mf=mf,
            causal=causal,
            issues=tuple(dict.fromkeys(issues)),
        )

    def _inspect_lf(self, issues: list[str]) -> dict[str, Any]:
        audit: dict[str, Any] = {
            "ok": False,
            "market_data_db_path": str(self.market_data_db_path),
            "range_checkpoint_db_path": str(
                self.range_checkpoint_db_path
            ),
            "canonical_exchange": self.exchange,
            "interval": self.closed_kline_interval,
            "min_records": self.lf_min_records,
            "range_speed_min_periods": self.range_speed_min_periods,
        }
        if not self.market_data_db_path.is_file():
            issues.append("lf_market_data_db_missing")
            return audit
        if not self.range_checkpoint_db_path.is_file():
            issues.append("lf_range_checkpoint_db_missing")
            return audit

        try:
            with _readonly_connect(self.market_data_db_path) as conn:
                kline_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM klines
                        WHERE exchange=? AND symbol=? AND interval=?
                          AND is_closed=1
                        """,
                        (
                            self.exchange,
                            self.symbol,
                            self.closed_kline_interval,
                        ),
                    ).fetchone()[0]
                )
                latest_kline = conn.execute(
                    """
                    SELECT exchange, open_time_ms, close_time_ms, source
                    FROM klines
                    WHERE exchange=? AND symbol=? AND interval=?
                      AND is_closed=1
                    ORDER BY close_time_ms DESC LIMIT 1
                    """,
                    (
                        self.exchange,
                        self.symbol,
                        self.closed_kline_interval,
                    ),
                ).fetchone()
                range_row = conn.execute(
                    """
                    SELECT COUNT(*), MAX(end_time_ms)
                    FROM range_bars
                    WHERE symbol=? AND range_pct=?
                    """,
                    (self.symbol, _decimal_text(self.range_pct)),
                ).fetchone()
                future_klines = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM klines
                        WHERE exchange=? AND symbol=? AND interval=?
                          AND is_closed=1 AND close_time_ms>?
                        """,
                        (
                            self.exchange,
                            self.symbol,
                            self.closed_kline_interval,
                            self.now_ms,
                        ),
                    ).fetchone()[0]
                )
        except (sqlite3.Error, TypeError, ValueError) as exc:
            issues.append(f"lf_store_read_failed:{exc}")
            audit["error"] = str(exc)
            return audit

        latest_close = (
            None if latest_kline is None else int(latest_kline[2])
        )
        stale = (
            latest_close is None
            or self.now_ms - latest_close > self.lf_max_staleness_ms
        )
        if kline_count < self.lf_min_records:
            issues.append("lf_closed_kline_warmup_insufficient")
        if latest_kline is None:
            issues.append("lf_closed_kline_missing")
        elif str(latest_kline[0]).lower() != self.exchange:
            issues.append("lf_closed_kline_noncanonical_exchange")
        if stale:
            issues.append("lf_closed_kline_stale")
        if future_klines:
            issues.append("lf_future_closed_kline")

        range_bar_count = int(range_row[0] or 0)
        latest_range_bar_end = (
            None if range_row[1] is None else int(range_row[1])
        )
        if range_bar_count <= 0:
            issues.append("lf_range_bar_store_empty")

        try:
            checkpoint_store = SqliteRangeCheckpointStore(
                self.range_checkpoint_db_path
            )
            range_history = checkpoint_store.load_complete_history(
                exchange=self.exchange,
                symbol=self.symbol,
                range_pct=self.range_pct,
                before_bucket_end_ms=self.now_ms + 1,
                limit=max(
                    self.range_speed_min_periods,
                    1,
                ),
            )
            latest_history = checkpoint_store.load_history(
                exchange=self.exchange,
                symbol=self.symbol,
                range_pct=self.range_pct,
                before_bucket_end_ms=self.now_ms + 1,
                limit=1,
            )
            with _readonly_connect(
                self.range_checkpoint_db_path
            ) as checkpoint_conn:
                future_range_aggregates = int(
                    checkpoint_conn.execute(
                        """
                        SELECT COUNT(*) FROM completed_range_aggregates
                        WHERE exchange=? AND symbol=? AND range_pct=?
                          AND (bucket_end_ms>? OR completed_at_ms>?)
                        """,
                        (
                            self.exchange,
                            self.symbol,
                            _decimal_text(self.range_pct),
                            self.now_ms,
                            self.now_ms,
                        ),
                    ).fetchone()[0]
                )
        except (sqlite3.Error, TypeError, ValueError) as exc:
            issues.append(f"lf_range_checkpoint_read_failed:{exc}")
            range_history = []
            latest_history = []
            future_range_aggregates = 0
            audit["range_checkpoint_error"] = str(exc)

        latest_aggregate = latest_history[-1] if latest_history else None
        aggregate_stale = (
            latest_aggregate is None
            or self.now_ms - latest_aggregate.bucket_end_ms
            > self.lf_max_staleness_ms
        )
        aggregate_causal = bool(
            latest_aggregate is not None
            and latest_aggregate.bucket_end_ms <= self.now_ms
            and latest_aggregate.completed_at_ms <= self.now_ms
        )
        if len(range_history) < self.range_speed_min_periods:
            issues.append("lf_range_speed_warmup_insufficient")
        if latest_aggregate is None:
            issues.append("lf_range_aggregate_missing")
        if aggregate_stale:
            issues.append("lf_range_aggregate_stale")
        if latest_aggregate is not None and (
            latest_aggregate.coverage_status != "COMPLETE"
        ):
            issues.append("lf_range_aggregate_not_complete")
        if latest_aggregate is not None and not aggregate_causal:
            issues.append("lf_range_aggregate_future_available")
        if future_range_aggregates:
            issues.append("lf_future_range_aggregate")

        audit.update(
            {
                "closed_kline_count": kline_count,
                "latest_closed_kline_open_time_ms": (
                    None if latest_kline is None else int(latest_kline[1])
                ),
                "latest_closed_kline_close_time_ms": latest_close,
                "latest_closed_kline_source": (
                    None if latest_kline is None else str(latest_kline[3])
                ),
                "closed_kline_stale": stale,
                "future_closed_kline_count": future_klines,
                "range_bar_count": range_bar_count,
                "latest_range_bar_end_time_ms": latest_range_bar_end,
                "range_speed_complete_periods": len(range_history),
                "latest_range_aggregate_end_time_ms": (
                    None
                    if latest_aggregate is None
                    else latest_aggregate.bucket_end_ms
                ),
                "latest_range_aggregate_available_time_ms": (
                    None
                    if latest_aggregate is None
                    else latest_aggregate.completed_at_ms
                ),
                "latest_range_aggregate_status": (
                    None
                    if latest_aggregate is None
                    else latest_aggregate.coverage_status
                ),
                "range_aggregate_causal_ok": aggregate_causal,
                "future_range_aggregate_count": future_range_aggregates,
            }
        )
        audit["ok"] = not any(
            issue.startswith("lf_") for issue in issues
        )
        return audit

    def _inspect_mf(self, issues: list[str]) -> dict[str, Any]:
        calendar_safe_archive_end = safe_okx_archive_end_ms(
            self.now_ms,
            archive_publish_lag_hours=0.0,
        )
        safe_archive_end = safe_okx_archive_end_ms(
            self.now_ms,
            archive_publish_lag_hours=self.archive_publish_lag_hours,
        )
        latest_archive_day_deferred = (
            calendar_safe_archive_end > safe_archive_end
        )
        audit: dict[str, Any] = {
            "ok": False,
            "market_data_db_path": str(self.market_data_db_path),
            "required_minutes": self.mf_required_minutes,
            "large_share_min_samples": self.large_share_min_samples,
            "mf_freshness_mode": self.readiness_mode,
            "archive_publish_lag_hours": (
                self.archive_publish_lag_hours
            ),
            "calendar_safe_archive_end_ms": (
                calendar_safe_archive_end
            ),
            "safe_archive_end_ms": safe_archive_end,
            "safe_archive_end_okx": _format_okx_time(
                safe_archive_end
            ),
            "calendar_safe_archive_end_okx": _format_okx_time(
                calendar_safe_archive_end
            ),
            "latest_archive_day_deferred": (
                latest_archive_day_deferred
            ),
            "latest_archive_day_deferred_reason": (
                "archive_publish_lag"
                if latest_archive_day_deferred
                else None
            ),
        }
        if not self.market_data_db_path.is_file():
            issues.append("mf_feature_db_missing")
            return audit
        try:
            store = SqliteTradeFeatureStore(
                path=self.market_data_db_path
            )
            if self.readiness_mode == "historical_preflight":
                history_minutes = max(
                    self.mf_required_minutes,
                    self.large_share_min_samples + 1,
                    self.large_share_window_days * 1_440,
                )
                historical_start = (
                    safe_archive_end
                    - history_minutes * _MINUTE_MS
                    + 1
                )
                history_range = TimeRange(
                    historical_start,
                    safe_archive_end,
                )
                bars = store.load_range_tradebars(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    time_range=history_range,
                )
                footprints = store.load_range_footprints(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    time_range=history_range,
                )
                readiness_reference_end = safe_archive_end
            else:
                bars = store.load_recent_tradebars(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    limit=max(
                        self.mf_required_minutes,
                        self.large_share_min_samples + 1,
                    ),
                )
                footprints = store.load_recent_footprints(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    limit=max(2, self.mf_required_minutes),
                )
                readiness_reference_end = (
                    None if not bars else bars[-1].close_time_ms
                )
        except (sqlite3.Error, TypeError, ValueError) as exc:
            issues.append(f"mf_store_read_failed:{exc}")
            audit["error"] = str(exc)
            return audit

        latest_bar = bars[-1] if bars else None
        latest_footprint = footprints[-1] if footprints else None
        signal_time_ms = (
            None
            if latest_bar is None
            else max(
                int(latest_bar.close_time_ms) + 1,
                int(latest_bar.available_time_ms),
            )
        )
        range_feature = None
        range_context_audit: dict[str, Any] = {
            "range_footprint_context_ready": False,
            "latest_range_footprint_context_available_time_ms": None,
        }
        if latest_bar is not None:
            range_feature = self._latest_range_footprint_probe(
                cutoff_ms=latest_bar.open_time_ms
            )
            range_context_audit = dict(
                latest_range_footprint_context_audit(
                    symbol=self.symbol,
                    exchange=self.exchange,
                    store=store,
                    cutoff_ms=latest_bar.open_time_ms,
                    range_pct=self.range_pct,
                    price_step=self.price_step,
                )
            )

        if latest_bar is None:
            issues.append("mf_tradebar_missing")
            readiness_audit: dict[str, Any] = {}
        else:
            readiness = resolve_trade_feature_readiness(
                symbol=self.symbol,
                exchange=self.exchange,
                store=store,
                required_minutes=self.mf_required_minutes,
                reference_end_ms=readiness_reference_end,
                now_ms=self.now_ms,
                range_pct=self.range_pct,
                price_step=self.price_step,
                archive_publish_lag_hours=(
                    self.archive_publish_lag_hours
                ),
            )
            readiness_audit = dict(readiness.audit())
            readiness_audit["historical_tradebar_ready"] = bool(
                readiness_audit.get("tradebar_ready", False)
            )
            readiness_audit[
                "historical_fixed_time_footprint_ready"
            ] = bool(
                readiness_audit.get("fixed_time_footprint_ready", False)
            )
            readiness_audit["historical_range_footprint_ready"] = bool(
                readiness_audit.get("range_footprint_ready", False)
            )
            readiness_audit["historical_coverage_ready"] = bool(
                readiness_audit.get("coverage_ready", False)
            )
            if not readiness_audit.get("tradebar_ready", False):
                issues.append("mf_tradebar_ready_false")

        tradebar_stale = bool(
            latest_bar is None
            or self.now_ms - latest_bar.close_time_ms
            > self.mf_max_staleness_ms
        )
        live_fresh_ready = not tradebar_stale
        if (
            self.readiness_mode == "live_freshness"
            and tradebar_stale
        ):
            issues.append("mf_tradebar_stale")
        if latest_bar is not None and latest_bar.quality != "COMPLETE":
            issues.append("mf_tradebar_degraded")
        fixed_footprint_causal = bool(
            latest_footprint is not None
            and signal_time_ms is not None
            and latest_footprint.available_time_ms <= signal_time_ms
            and latest_footprint.available_time_ms <= self.now_ms
        )
        range_context_ready = bool(
            range_context_audit.get("range_footprint_context_ready")
        )
        if not range_context_ready:
            issues.append("mf_range_footprint_context_ready_false")
            issues.append("mf_range_footprint_ready_false")

        latest_open = (
            None if latest_bar is None else latest_bar.open_time_ms
        )
        window_start = (
            None
            if latest_open is None
            else latest_open
            - self.large_share_window_days * 24 * 60 * _MINUTE_MS
        )
        large_share_samples = sum(
            1
            for bar in bars
            if latest_open is not None
            and window_start is not None
            and window_start <= bar.open_time_ms < latest_open
            and bar.quality == "COMPLETE"
            and bar.large_trade_share is not None
        )
        if large_share_samples < self.large_share_min_samples:
            issues.append("mf_large_share_samples_insufficient")

        missing_fields: list[str] = []
        if latest_bar is not None:
            for field in (
                "open",
                "high",
                "low",
                "close",
                "large_trade_share",
                "available_time_ms",
            ):
                if getattr(latest_bar, field, None) is None:
                    missing_fields.append(field)
        if range_context_ready and (
            range_context_audit.get(
                "latest_range_footprint_context_pressure"
            )
            is None
        ):
            missing_fields.append(
                "range_fp_max_bucket_abs_delta_pressure"
            )
        if missing_fields:
            issues.append("mf_required_fields_missing")

        future_rows = self._mf_future_row_count()
        if future_rows:
            issues.append("mf_future_feature_rows")
        range_causal = bool(
            range_context_ready
            and latest_bar is not None
            and signal_time_ms is not None
            and int(
                range_context_audit.get(
                    "latest_range_footprint_context_available_time_ms"
                )
                or 0
            )
            <= latest_bar.open_time_ms
            and int(
                range_context_audit.get(
                    "latest_range_footprint_context_available_time_ms"
                )
                or 0
            )
            <= signal_time_ms
        )
        if range_context_ready and not range_causal:
            issues.append("mf_range_footprint_future_available")
        large_share_ready = (
            large_share_samples >= self.large_share_min_samples
        )
        mf_signal_feature_ready = bool(
            readiness_audit.get("tradebar_ready", False)
            and large_share_ready
            and range_context_ready
        )
        readiness_audit["mf_signal_feature_ready"] = (
            mf_signal_feature_ready
        )
        readiness_audit["range_footprint_context_ready"] = (
            range_context_ready
        )
        readiness_audit["range_footprint_ready"] = range_context_ready
        readiness_audit["coverage_ready"] = mf_signal_feature_ready
        if not mf_signal_feature_ready:
            issues.append("mf_mf_signal_feature_ready_false")

        audit.update(
            {
                **readiness_audit,
                **range_context_audit,
                "latest_tradebar_open_time_ms": latest_open,
                "latest_tradebar_close_time_ms": (
                    None
                    if latest_bar is None
                    else latest_bar.close_time_ms
                ),
                "latest_tradebar_available_time_ms": (
                    None
                    if latest_bar is None
                    else latest_bar.available_time_ms
                ),
                "latest_fixed_time_footprint_available_time_ms": (
                    None
                    if latest_footprint is None
                    else latest_footprint.available_time_ms
                ),
                "latest_range_footprint_available_time_ms": (
                    None
                    if range_feature is None
                    else range_feature.available_time_ms
                ),
                "latest_range_footprint_context_available_time_ms": (
                    range_context_audit.get(
                        "latest_range_footprint_context_available_time_ms"
                    )
                ),
                "latest_signal_time_ms": signal_time_ms,
                "tradebar_stale": tradebar_stale,
                "live_fresh_ready": live_fresh_ready,
                "safe_archive_end_ms": safe_archive_end,
                "latest_tradebar_age_ms": (
                    None
                    if latest_bar is None
                    else max(
                        0,
                        self.now_ms - latest_bar.close_time_ms,
                    )
                ),
                "large_share_sample_count": large_share_samples,
                "large_share_samples_ready": large_share_ready,
                "missing_required_fields": missing_fields,
                "future_feature_row_count": future_rows,
                "range_footprint_causal_ok": range_causal,
                "fixed_time_footprint_causal_ok": (
                    fixed_footprint_causal
                ),
            }
        )
        audit["ok"] = not any(
            issue.startswith("mf_") for issue in issues
        )
        return audit

    def _causal_audit(
        self,
        *,
        lf: dict[str, Any],
        mf: dict[str, Any],
        issues: list[str],
    ) -> dict[str, Any]:
        lf_close = lf.get("latest_closed_kline_close_time_ms")
        range_available = lf.get(
            "latest_range_aggregate_available_time_ms"
        )
        signal_time = mf.get("latest_signal_time_ms")
        tradebar_available = mf.get(
            "latest_tradebar_available_time_ms"
        )
        footprint_available = mf.get(
            "latest_range_footprint_context_available_time_ms"
        )
        fixed_footprint_available = mf.get(
            "latest_fixed_time_footprint_available_time_ms"
        )
        checks = {
            "lf_closed_bar_not_future": bool(
                lf_close is not None and int(lf_close) <= self.now_ms
            ),
            "lf_range_available_not_future": bool(
                range_available is not None
                and int(range_available) <= self.now_ms
            ),
            "mf_tradebar_available_by_signal": bool(
                tradebar_available is not None
                and signal_time is not None
                and int(tradebar_available) <= int(signal_time)
            ),
            "mf_range_footprint_available_by_signal": bool(
                footprint_available is not None
                and signal_time is not None
                and int(footprint_available) <= int(signal_time)
            ),
            "no_future_feature_rows": (
                int(mf.get("future_feature_row_count") or 0) == 0
            ),
        }
        diagnostic_checks = {
            "mf_fixed_time_footprint_available_by_signal": bool(
                fixed_footprint_available is not None
                and signal_time is not None
                and int(fixed_footprint_available) <= int(signal_time)
            ),
        }
        if not all(checks.values()):
            issues.append("causal_future_violation")
        return {
            "ok": all(checks.values()),
            "decision_time_ms": self.now_ms,
            **checks,
            **diagnostic_checks,
        }

    def _mf_future_row_count(self) -> int:
        try:
            with _readonly_connect(self.market_data_db_path) as conn:
                tradebar = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM tradebar_1m_features
                        WHERE exchange=? AND symbol=?
                          AND (close_time_ms>? OR available_time_ms>?)
                        """,
                        (
                            self.exchange,
                            self.symbol,
                            self.now_ms,
                            self.now_ms,
                        ),
                    ).fetchone()[0]
                )
                footprint = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM trade_footprint_1m_features
                        WHERE exchange=? AND symbol=?
                          AND (close_time_ms>? OR available_time_ms>?)
                        """,
                        (
                            self.exchange,
                            self.symbol,
                            self.now_ms,
                            self.now_ms,
                        ),
                    ).fetchone()[0]
                )
                range_footprint = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM range_footprint_features
                        WHERE exchange=? AND symbol=?
                          AND available_time_ms>?
                        """,
                        (self.exchange, self.symbol, self.now_ms),
                    ).fetchone()[0]
                )
            return tradebar + footprint + range_footprint
        except sqlite3.Error:
            return 1

    def _latest_range_footprint_probe(
        self, *, cutoff_ms: int
    ) -> _RangeFootprintProbe | None:
        try:
            with _readonly_connect(self.market_data_db_path) as conn:
                row = conn.execute(
                    """
                    SELECT available_time_ms, quality, context_available,
                           fp_max_bucket_abs_delta_pressure
                    FROM range_footprint_features
                    WHERE exchange=? AND symbol=?
                      AND range_pct=? AND price_step=?
                      AND available_time_ms<=?
                    ORDER BY available_time_ms DESC, range_bar_id DESC
                    LIMIT 1
                    """,
                    (
                        self.exchange,
                        self.symbol,
                        _decimal_text(self.range_pct),
                        _decimal_text(self.price_step),
                        int(cutoff_ms),
                    ),
                ).fetchone()
        except sqlite3.Error:
            return None
        if row is None:
            return None
        return _RangeFootprintProbe(
            available_time_ms=int(row[0]),
            quality=str(row[1]),
            context_available=bool(row[2]),
            fp_max_bucket_abs_delta_pressure=(
                None if row[3] is None else str(row[3])
            ),
        )


def _readonly_connect(path: Path) -> sqlite3.Connection:
    uri = path.resolve().as_uri() + "?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _decimal_text(value: str) -> str:
    from decimal import Decimal

    return format(Decimal(str(value)).normalize(), "f")


def _format_okx_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(
        int(timestamp_ms) / 1_000,
        tz=UTC,
    ).astimezone(_OKX_ARCHIVE_TIMEZONE).strftime(
        "%Y-%m-%d %H:%M:%S+08"
    )


__all__ = [
    "PortfolioV1ReadinessInspector",
    "PortfolioV1ReadinessResult",
]
