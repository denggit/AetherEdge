#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.storage.trade_feature_store import (  # noqa: E402
    SqliteTradeFeatureStore,
)
from src.market_data.trade_features.coverage import (  # noqa: E402
    latest_range_footprint_context_audit,
    resolve_trade_feature_readiness,
)
from src.market_data.backfill.status_store import now_ms  # noqa: E402
from tools.mf_feature_backfill_worker import run_cycle  # noqa: E402


_DAY_MS = 86_400_000
_OKX_TIMEZONE = timezone(timedelta(hours=8))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prebuild MF trade-derived feature history until ready."
        )
    )
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--exchange", default="okx")
    parser.add_argument(
        "--market-db",
        default="data/market_data/aether_market_data.sqlite3",
    )
    parser.add_argument(
        "--raw-root",
        default="data/okx/raw/trades",
    )
    parser.add_argument(
        "--status-path",
        default="data/state/mf_feature_prebuild_status.json",
    )
    parser.add_argument(
        "--global-lock-path",
        default="data/state/raw_trade_backfill_global.lock",
    )
    parser.add_argument(
        "--global-status-path",
        default=(
            "data/state/raw_trade_backfill_global_status.json"
        ),
    )
    parser.add_argument(
        "--range-footprint-range-pct",
        default="0.002",
    )
    parser.add_argument(
        "--range-footprint-price-step",
        default="1",
    )
    parser.add_argument("--range-footprint-warmup-days", type=int, default=1)
    parser.add_argument("--contract-value", default="0.01")
    parser.add_argument("--price-bucket-size", default="1")
    parser.add_argument("--large-trade-threshold", default="10000")
    parser.add_argument("--target-days", type=int, default=95)
    parser.add_argument(
        "--large-share-min-samples",
        type=int,
        default=43_200,
    )
    parser.add_argument(
        "--large-share-window-days",
        type=int,
        default=90,
    )
    parser.add_argument(
        "--max-minutes-per-cycle",
        type=int,
        default=4320,
    )
    parser.add_argument("--max-days-per-cycle", type=int, default=3)
    parser.add_argument(
        "--max-trades-per-cycle",
        type=int,
        default=20_000_000,
    )
    parser.add_argument(
        "--max-seconds-per-cycle",
        type=float,
        default=1800.0,
    )
    parser.add_argument("--max-cycles", type=int, default=200)
    parser.add_argument("--max-seconds", type=float, default=0.0)
    parser.add_argument("--max-failures", type=int, default=3)
    parser.add_argument(
        "--archive-publish-lag-hours",
        type=float,
        default=8.0,
    )
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    parser.add_argument("--chunk-sleep-seconds", type=float, default=0.0)
    parser.add_argument(
        "--download",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    return parser


def run_prebuild(args: argparse.Namespace) -> int:
    started_at_ms = now_ms()
    started_monotonic = time.monotonic()
    status_path = Path(args.status_path)
    requested_minutes, required_minutes = _required_windows(args)
    cycles = 0
    consecutive_failures = 0
    last_result: Mapping[str, Any] | None = None
    last_readiness: Mapping[str, Any] | None = None
    status = {
        "running": True,
        "started_at_ms": started_at_ms,
        "updated_at_ms": started_at_ms,
        "symbol": args.symbol,
        "exchange": args.exchange,
        "market_db": args.market_db,
        "target_days": int(args.target_days),
        "requested_minutes": requested_minutes,
        "effective_required_minutes": required_minutes,
        "large_share_min_samples": int(
            args.large_share_min_samples
        ),
        "large_share_window_days": int(
            args.large_share_window_days
        ),
        "archive_publish_lag_hours": float(
            args.archive_publish_lag_hours
        ),
        "cycles": 0,
        "ready": False,
        "last_result": None,
        "last_readiness": None,
        "error": False,
        **_progress_snapshot(
            result={},
            readiness={},
            target_days=int(args.target_days),
            elapsed_seconds=0.0,
        ),
    }
    status["archive_publish_lag_hours"] = max(
        0.0,
        float(args.archive_publish_lag_hours),
    )
    _write_status(status_path, status)

    try:
        last_readiness = _readiness_audit(
            args,
            required_minutes=required_minutes,
        )
        status.update(
            _progress_snapshot(
                result={},
                readiness=last_readiness,
                target_days=int(args.target_days),
                elapsed_seconds=(
                    time.monotonic() - started_monotonic
                ),
            )
        )
        status["archive_publish_lag_hours"] = max(
            0.0,
            float(args.archive_publish_lag_hours),
        )
        _write_status(status_path, status)
        if _is_ready(last_readiness):
            return _complete(
                status_path=status_path,
                status=status,
                cycles=0,
                readiness=last_readiness,
                started_monotonic=started_monotonic,
                market_db=args.market_db,
            )

        max_cycles = max(1, int(args.max_cycles))
        max_failures = max(1, int(args.max_failures))
        for cycle in range(1, max_cycles + 1):
            elapsed = time.monotonic() - started_monotonic
            if args.max_seconds > 0 and elapsed >= args.max_seconds:
                return _fail(
                    status_path=status_path,
                    status=status,
                    cycles=cycles,
                    result=last_result,
                    readiness=last_readiness,
                    error_detail="max_seconds_reached",
                )

            cycles = cycle
            try:
                last_result = dict(
                    run_cycle(
                        symbol=args.symbol,
                        exchange=args.exchange,
                        market_db=args.market_db,
                        raw_root=args.raw_root,
                        status_path=args.status_path,
                        global_lock_path=args.global_lock_path,
                        global_status_path=args.global_status_path,
                        mode="prebuild",
                        direction="oldest-to-recent",
                        max_minutes_per_cycle=max(
                            1, int(args.max_minutes_per_cycle)
                        ),
                        max_days_per_cycle=max(
                            1, int(args.max_days_per_cycle)
                        ),
                        max_trades_per_cycle=max(
                            1, int(args.max_trades_per_cycle)
                        ),
                        max_seconds_per_cycle=max(
                            0.0,
                            float(args.max_seconds_per_cycle),
                        ),
                        chunk_sleep_seconds=max(
                            0.0,
                            float(args.chunk_sleep_seconds),
                        ),
                        no_download=not bool(args.download),
                        save_raw_trades=False,
                        contract_value=Decimal(args.contract_value),
                        large_trade_threshold=Decimal(
                            args.large_trade_threshold
                        ),
                        price_bucket_size=Decimal(
                            args.price_bucket_size
                        ),
                        range_footprint_range_pct=Decimal(
                            args.range_footprint_range_pct
                        ),
                        range_footprint_price_step=Decimal(
                            args.range_footprint_price_step
                        ),
                        range_footprint_warmup_days=max(
                            0,
                            int(args.range_footprint_warmup_days),
                        ),
                        required_minutes=required_minutes,
                        archive_publish_lag_hours=max(
                            0.0,
                            float(args.archive_publish_lag_hours),
                        ),
                    )
                )
            except Exception as exc:
                last_result = {
                    "status": "error",
                    "reason": "run_cycle_exception",
                    "error": f"{type(exc).__name__}: {exc}",
                }

            if _cycle_failed(last_result):
                consecutive_failures += 1
            else:
                consecutive_failures = 0

            last_readiness = _readiness_audit(
                args,
                required_minutes=required_minutes,
            )
            elapsed = time.monotonic() - started_monotonic
            progress = _progress_snapshot(
                result=last_result,
                readiness=last_readiness,
                target_days=int(args.target_days),
                elapsed_seconds=elapsed,
            )
            _print_progress(
                cycle=cycle,
                result=last_result,
                readiness=last_readiness,
                elapsed=elapsed,
                progress=progress,
            )
            status.update(
                {
                    "running": True,
                    "updated_at_ms": now_ms(),
                    "cycles": cycle,
                    "ready": _is_ready(last_readiness),
                    "last_result": last_result,
                    "last_readiness": last_readiness,
                    "error": False,
                    **progress,
                    "archive_publish_lag_hours": max(
                        0.0,
                        float(args.archive_publish_lag_hours),
                    ),
                }
            )
            _write_status(status_path, status)

            if _is_ready(last_readiness):
                return _complete(
                    status_path=status_path,
                    status=status,
                    cycles=cycle,
                    readiness=last_readiness,
                    started_monotonic=started_monotonic,
                    market_db=args.market_db,
                )
            if consecutive_failures >= max_failures:
                return _fail(
                    status_path=status_path,
                    status=status,
                    cycles=cycle,
                    result=last_result,
                    readiness=last_readiness,
                    error_detail="max_failures_reached",
                )
            if args.max_seconds > 0 and elapsed >= args.max_seconds:
                return _fail(
                    status_path=status_path,
                    status=status,
                    cycles=cycle,
                    result=last_result,
                    readiness=last_readiness,
                    error_detail="max_seconds_reached",
                )
            if args.sleep_seconds > 0:
                time.sleep(max(0.0, float(args.sleep_seconds)))

        return _fail(
            status_path=status_path,
            status=status,
            cycles=cycles,
            result=last_result,
            readiness=last_readiness,
            error_detail="max_cycles_reached",
        )
    except Exception as exc:
        return _fail(
            status_path=status_path,
            status=status,
            cycles=cycles,
            result=last_result,
            readiness=last_readiness,
            error_detail=f"{type(exc).__name__}: {exc}",
        )


def _required_windows(
    args: argparse.Namespace,
) -> tuple[int, int]:
    requested_minutes = max(1, int(args.target_days)) * 1440
    required_minutes = max(
        requested_minutes,
        max(1, int(args.large_share_min_samples)),
        max(1, int(args.large_share_window_days)) * 1_440,
    )
    return requested_minutes, required_minutes


def _format_okx_time(timestamp_ms: int | None) -> str:
    if timestamp_ms is None:
        return "unknown"
    value = datetime.fromtimestamp(
        int(timestamp_ms) / 1_000,
        tz=UTC,
    ).astimezone(_OKX_TIMEZONE)
    return value.strftime("%Y-%m-%d %H:%M:%S") + "+08"


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    total_seconds = max(0, int(round(seconds)))
    days, remainder = divmod(total_seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, remaining_seconds = divmod(remainder, 60)
    if days:
        return f"{days}d{hours}h"
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{remaining_seconds:02d}s"
    return f"{remaining_seconds}s"


def _progress_snapshot(
    *,
    result: Mapping[str, Any],
    readiness: Mapping[str, Any],
    target_days: int,
    elapsed_seconds: float,
) -> dict[str, Any]:
    readiness_coverage = readiness.get("coverage")
    coverage = (
        readiness_coverage
        if isinstance(readiness_coverage, Mapping)
        else {}
    )
    coverage_extra_value = coverage.get("extra")
    coverage_extra = (
        coverage_extra_value
        if isinstance(coverage_extra_value, Mapping)
        else {}
    )
    result_coverage_value = result.get("coverage_after")
    result_coverage = (
        result_coverage_value
        if isinstance(result_coverage_value, Mapping)
        else {}
    )
    coverage_complete_minutes = _first_int(
        coverage.get("complete_minutes"),
        result_coverage.get("complete_minutes"),
    )
    coverage_missing_minutes = _first_int(
        coverage.get("missing_minutes"),
        result_coverage.get("missing_minutes"),
    )
    safe_end_ms = _first_int(
        result.get("safe_archive_end_ms"),
        result.get("safe_end_ms"),
        coverage_extra.get("safe_archive_end_ms"),
        coverage_extra.get("reference_end_ms"),
    )
    calendar_safe_end_ms = _first_int(
        result.get("calendar_safe_archive_end_ms"),
        coverage_extra.get("calendar_safe_archive_end_ms"),
    )
    archive_publish_lag_hours = _first_float(
        result.get("archive_publish_lag_hours")
    )
    if archive_publish_lag_hours is None:
        archive_publish_lag_hours = _first_float(
            coverage_extra.get("archive_publish_lag_hours")
        )
    processed_through_ms = _first_int(
        result.get("processed_through_ms"),
        coverage.get("latest_complete_close_time_ms"),
    )
    target_end_ms = _first_int(
        result.get("target_end_ms"),
        processed_through_ms,
    )
    total_days = float(max(1, int(target_days)))
    required_start_ms = (
        None
        if safe_end_ms is None
        else safe_end_ms - int(total_days * _DAY_MS) + 1
    )
    completed_days = 0.0
    if (
        required_start_ms is not None
        and processed_through_ms is not None
    ):
        completed_days = min(
            total_days,
            max(
                0.0,
                (
                    processed_through_ms
                    - required_start_ms
                    + 1
                )
                / _DAY_MS,
            ),
        )
    if coverage_complete_minutes is not None:
        completed_days = min(
            total_days,
            max(0.0, coverage_complete_minutes / 1_440),
        )
    remaining_days = max(0.0, total_days - completed_days)
    progress_pct = completed_days / total_days * 100.0
    if (
        coverage_missing_minutes is not None
        and coverage_missing_minutes > 0
        and progress_pct >= 100.0
    ):
        progress_pct = 99.9
        completed_days = total_days * progress_pct / 100.0
        remaining_days = total_days - completed_days
    avg_seconds_per_day = (
        None
        if completed_days <= 0
        else max(0.0, float(elapsed_seconds)) / completed_days
    )
    eta_seconds = (
        None
        if avg_seconds_per_day is None
        else remaining_days * avg_seconds_per_day
    )
    cycle_elapsed = _first_float(result.get("elapsed_seconds"))
    return {
        "progress": (
            f"{completed_days:.2f}/{total_days:.2f}d"
        ),
        "progress_pct": progress_pct,
        "completed_days": completed_days,
        "total_days": total_days,
        "remaining_days": remaining_days,
        "eta": _format_duration(eta_seconds),
        "eta_seconds": eta_seconds,
        "target_okx": _format_okx_time(target_end_ms),
        "safe_end_okx": _format_okx_time(safe_end_ms),
        "calendar_safe_end_okx": _format_okx_time(
            calendar_safe_end_ms
        ),
        "archive_publish_lag_hours": archive_publish_lag_hours,
        "required_start_okx": _format_okx_time(required_start_ms),
        "avg_seconds_per_day": avg_seconds_per_day,
        "cycle_elapsed": cycle_elapsed,
        "bars_written": _first_int(
            result.get("total_bars_written"),
            result.get("tradebars_written"),
        ),
        "footprints_written": _first_int(
            result.get("total_footprints_written"),
            result.get("fixed_footprints_written"),
        ),
        "range_footprints_written": _first_int(
            result.get("range_footprints_written"),
        ),
        "coverage_complete_minutes": coverage_complete_minutes,
        "coverage_missing_minutes": coverage_missing_minutes,
        "processed_through_ms": processed_through_ms,
        "cycle_truncated": bool(
            result.get("cycle_truncated", False)
        ),
    }


def _first_int(*values: Any) -> int | None:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _readiness_audit(
    args: argparse.Namespace,
    *,
    required_minutes: int,
) -> dict[str, Any]:
    store = SqliteTradeFeatureStore(path=args.market_db)
    readiness = resolve_trade_feature_readiness(
        symbol=args.symbol,
        exchange=args.exchange,
        store=store,
        required_minutes=required_minutes,
        range_pct=args.range_footprint_range_pct,
        price_step=args.range_footprint_price_step,
        archive_publish_lag_hours=max(
            0.0,
            float(args.archive_publish_lag_hours),
        ),
    )
    audit = dict(readiness.audit())
    coverage = audit.get("coverage")
    coverage_extra = (
        dict(coverage.get("extra", {}))
        if isinstance(coverage, Mapping)
        else {}
    )
    audit["historical_fixed_time_footprint_ready"] = bool(
        audit.get("fixed_time_footprint_ready", False)
    )
    audit["historical_range_footprint_ready"] = bool(
        audit.get("range_footprint_ready", False)
    )
    audit["historical_coverage_ready"] = bool(
        audit.get("coverage_ready", False)
    )
    large_share_sample_count = int(
        coverage_extra.get("tradebar_complete_minutes", 0) or 0
    )
    large_share_ready = (
        large_share_sample_count
        >= max(1, int(args.large_share_min_samples))
    )
    reference_end_ms = _first_int(
        coverage_extra.get("reference_end_ms"),
        coverage_extra.get("safe_archive_end_ms"),
    )
    context_audit = latest_range_footprint_context_audit(
        symbol=args.symbol,
        exchange=args.exchange,
        store=store,
        cutoff_ms=(
            0
            if reference_end_ms is None
            else (reference_end_ms // 60_000) * 60_000
        ),
        range_pct=args.range_footprint_range_pct,
        price_step=args.range_footprint_price_step,
    )
    range_context_ready = bool(
        context_audit.get("range_footprint_context_ready", False)
    )
    mf_signal_feature_ready = bool(
        audit.get("tradebar_ready", False)
        and large_share_ready
        and range_context_ready
    )
    audit.update(context_audit)
    audit["large_share_sample_count"] = large_share_sample_count
    audit["large_share_min_samples"] = max(
        1, int(args.large_share_min_samples)
    )
    audit["large_share_samples_ready"] = large_share_ready
    audit["range_footprint_context_ready"] = range_context_ready
    audit["range_footprint_ready"] = range_context_ready
    audit["mf_signal_feature_ready"] = mf_signal_feature_ready
    audit["coverage_ready"] = mf_signal_feature_ready
    audit["ready"] = _is_ready(audit)
    return audit


def _is_ready(readiness: Mapping[str, Any]) -> bool:
    return bool(
        readiness.get("tradebar_ready", False)
        and readiness.get("large_share_samples_ready", False)
        and readiness.get("range_footprint_context_ready", False)
        and readiness.get("mf_signal_feature_ready", False)
    )


def _cycle_failed(result: Mapping[str, Any]) -> bool:
    if (
        str(result.get("status", "")).lower() == "deferred"
        and str(result.get("reason", "")).lower()
        == "archive_not_published_yet"
    ):
        return False
    if str(result.get("status", "")).lower() in {
        "error",
        "failed",
        "launch_failed",
    }:
        return True
    if str(result.get("reason", "")).lower() in {
        "download_failures",
        "subprocess_error",
        "run_cycle_exception",
    }:
        return True
    return bool(result.get("failed_downloads"))


def _print_progress(
    *,
    cycle: int,
    result: Mapping[str, Any],
    readiness: Mapping[str, Any],
    elapsed: float,
    progress: Mapping[str, Any],
) -> None:
    cycle_elapsed = progress.get("cycle_elapsed")
    avg_seconds_per_day = progress.get("avg_seconds_per_day")
    print(
        "[prebuild-mf] "
        f"cycle={cycle} "
        f"status={result.get('status', 'unknown')} "
        f"reason={result.get('reason', 'unknown')} "
        f"ready={_is_ready(readiness)} "
        f"tradebar={readiness.get('tradebar_ready', False)} "
        "footprint="
        f"{readiness.get('fixed_time_footprint_ready', False)} "
        f"range_fp={readiness.get('range_footprint_ready', False)} "
        f"target={result.get('target_end_ms', 'pending')} "
        f"target_okx={progress['target_okx']} "
        f"safe_end_okx={progress['safe_end_okx']} "
        "calendar_safe_end_okx="
        f"{progress['calendar_safe_end_okx']} "
        "archive_publish_lag_hours="
        f"{progress['archive_publish_lag_hours']} "
        f"required_start_okx={progress['required_start_okx']} "
        f"completed_days={progress['completed_days']:.2f} "
        f"total_days={progress['total_days']:.2f} "
        f"progress={progress['progress']} "
        f"progress_pct={progress['progress_pct']:.1f}% "
        f"remaining_days={progress['remaining_days']:.2f} "
        f"eta={progress['eta']} "
        "cycle_elapsed="
        f"{'unknown' if cycle_elapsed is None else f'{cycle_elapsed:.1f}s'} "
        "avg_seconds_per_day="
        f"{'unknown' if avg_seconds_per_day is None else f'{avg_seconds_per_day:.1f}'} "
        f"bars_written={progress['bars_written']} "
        f"footprints_written={progress['footprints_written']} "
        "range_footprints_written="
        f"{progress['range_footprints_written']} "
        "coverage_complete_minutes="
        f"{progress['coverage_complete_minutes']} "
        "coverage_missing_minutes="
        f"{progress['coverage_missing_minutes']} "
        f"processed_through_ms={progress['processed_through_ms']} "
        f"cycle_truncated={progress['cycle_truncated']} "
        "archive_not_published_days="
        f"{result.get('archive_not_published_days', [])} "
        f"elapsed={elapsed:.1f}",
        flush=True,
    )


def _complete(
    *,
    status_path: Path,
    status: dict[str, Any],
    cycles: int,
    readiness: Mapping[str, Any],
    started_monotonic: float,
    market_db: str,
) -> int:
    elapsed = time.monotonic() - started_monotonic
    status.update(
        {
            "running": False,
            "updated_at_ms": now_ms(),
            "cycles": cycles,
            "ready": True,
            "last_readiness": dict(readiness),
            "error": False,
        }
    )
    _write_status(status_path, status)
    print(
        "[prebuild-mf] complete "
        f"ready=True cycles={cycles} elapsed={elapsed:.1f} "
        f"market_db={market_db}",
        flush=True,
    )
    return 0


def _fail(
    *,
    status_path: Path,
    status: dict[str, Any],
    cycles: int,
    result: Mapping[str, Any] | None,
    readiness: Mapping[str, Any] | None,
    error_detail: str,
) -> int:
    status.update(
        {
            "running": False,
            "updated_at_ms": now_ms(),
            "cycles": cycles,
            "ready": False,
            "last_result": result,
            "last_readiness": readiness,
            "error": True,
            "error_detail": error_detail,
        }
    )
    _write_status(status_path, status)
    print(
        "[prebuild-mf] failed "
        f"cycles={cycles} reason={error_detail}",
        flush=True,
    )
    return 1


def _write_status(
    path: str | Path,
    payload: Mapping[str, Any],
) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    temporary.write_text(
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    os.replace(temporary, target)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.target_days <= 0:
        print("[prebuild-mf] failed reason=target_days_must_be_positive")
        return 1
    if args.large_share_min_samples <= 0:
        print(
            "[prebuild-mf] failed "
            "reason=large_share_min_samples_must_be_positive"
        )
        return 1
    if args.large_share_window_days <= 0:
        print(
            "[prebuild-mf] failed "
            "reason=large_share_window_days_must_be_positive"
        )
        return 1
    return run_prebuild(args)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "build_parser",
    "main",
    "run_prebuild",
]
