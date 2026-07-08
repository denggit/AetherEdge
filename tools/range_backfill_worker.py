from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import os
from pathlib import Path
import platform
import re
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.models import RangeBackfillRequest
from src.market_data.backfill.lock import RangeBackfillLock
from src.market_data.backfill.coordinator import (
    BACKGROUND_BACKFILL_PRIORITY,
    RawTradeBackfillCoordinator,
)
from src.market_data.backfill.service import RangeBackfillService
from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.market_data.historical_trades.okx_archive import okx_raw_symbol_from_canonical
from src.market_data.historical_trades.okx_archive import okx_archive_date_from_utc_ms

REASON_AVAILABLE = "available"
REASON_ARCHIVE_GAP_BACKFILLING = "archive_gap_backfilling"
REASON_ARCHIVE_GAP_NO_PROGRESS = "archive_gap_no_progress"
REASON_ARCHIVE_GAP_PARTIAL_NO_PROGRESS = "archive_gap_partial_no_progress"
REASON_CURRENT_DAY_ARCHIVE_NOT_READY = "current_day_archive_not_ready"
REASON_REPAIR_FAILED_COOLDOWN = "repair_failed_cooldown"
DAILY_ARCHIVE_BACKFILL_RUNNING = "daily_archive_backfill_running"
DAILY_ARCHIVE_BACKFILL_FAILED = "daily_archive_backfill_failed"
DAILY_ARCHIVE_BACKFILL_SUCCESS = "daily_archive_backfill_success"
_DATE_PATTERN = re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Live range-speed backfill worker.")
    parser.add_argument("--mode", default="live")
    parser.add_argument("--direction", default="recent-to-oldest")
    parser.add_argument("--symbol", default=_env("AETHER_MARKET", "ETH-USDT-PERP"))
    parser.add_argument("--exchange", default=_env("AETHER_DATA_EXCHANGE", "okx"))
    parser.add_argument("--raw-symbol", default=None)
    parser.add_argument("--range-pct", default=_env("AETHER_RANGE_PCT", "0.002"))
    parser.add_argument("--bucket-interval", default=_env("AETHER_CLOSED_BAR_INTERVAL", "4h"))
    parser.add_argument("--required-buckets", type=int, default=int(_env("AETHER_RANGE_BACKFILL_REQUIRED_BUCKETS", "100")))
    parser.add_argument("--lookback-buckets", type=int, default=int(_env("AETHER_RANGE_BACKFILL_LOOKBACK_BUCKETS", "160")))
    parser.add_argument("--max-buckets-per-cycle", type=int, default=int(_env("AETHER_RANGE_BACKFILL_MAX_BUCKETS_PER_CYCLE", "6")))
    parser.add_argument("--max-days-per-cycle", type=int, default=int(_env("AETHER_RANGE_BACKFILL_MAX_DAYS_PER_CYCLE", "1")))
    parser.add_argument("--sleep-seconds", type=float, default=float(_env("AETHER_RANGE_BACKFILL_SLEEP_SECONDS", "30")))
    parser.add_argument("--chunksize", type=int, default=int(_env("AETHER_RANGE_BACKFILL_CHUNKSIZE", "50000")))
    parser.add_argument("--status-path", default=_env("AETHER_RANGE_BACKFILL_STATUS_PATH", "data/state/range_backfill_status.json"))
    parser.add_argument("--lock-path", default=_env("AETHER_RANGE_BACKFILL_LOCK_PATH", "data/state/range_backfill.lock"))
    parser.add_argument("--market-db", default=_env("AETHER_MARKET_DATA_DB", "data/market_data/aether_market_data.sqlite3"))
    parser.add_argument("--checkpoint-db", default=_env("AETHER_RANGE_CHECKPOINT_DB", "data/state/range_builder_checkpoint.sqlite3"))
    parser.add_argument("--raw-root", default=_env("AETHER_RANGE_BACKFILL_RAW_ROOT", "data/okx/raw/trades"))
    parser.add_argument(
        "--global-lock-path",
        default=_env(
            "AETHER_RAW_TRADE_BACKFILL_GLOBAL_LOCK_PATH",
            "data/state/raw_trade_backfill_global.lock",
        ),
    )
    parser.add_argument(
        "--global-status-path",
        default=_env(
            "AETHER_RAW_TRADE_BACKFILL_GLOBAL_STATUS_PATH",
            "data/state/raw_trade_backfill_global_status.json",
        ),
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--no-download", action="store_true")
    parser.add_argument("--low-priority", action=argparse.BooleanOptionalAction, default=_bool(_env("AETHER_RANGE_BACKFILL_LOW_PRIORITY", "true")))
    parser.add_argument("--once", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--save-raw-trades", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--chunk-sleep-seconds", type=float, default=None)
    parser.add_argument("--max-seconds-per-cycle", type=float, default=None)
    parser.add_argument("--max-trades-per-cycle", type=int, default=None)
    parser.add_argument("--max-target-end-ms", type=int, default=None)
    parser.add_argument(
        "--failure-cooldown-seconds",
        type=int,
        default=int(_env("AETHER_RANGE_REPAIR_FAILURE_COOLDOWN_SECONDS", "3600")),
    )
    parser.add_argument(
        "--archive-not-ready-cooldown-seconds",
        type=int,
        default=int(_env("AETHER_RANGE_REPAIR_ARCHIVE_NOT_READY_COOLDOWN_SECONDS", "21600")),
    )
    parser.add_argument(
        "--daily-retry-after-utc-hour",
        type=int,
        default=int(_env("AETHER_RANGE_REPAIR_DAILY_RETRY_AFTER_UTC_HOUR", "1")),
    )
    return parser


def request_from_args(args: argparse.Namespace) -> RangeBackfillRequest:
    mode = str(args.mode).strip().lower()
    return RangeBackfillRequest(
        symbol=args.symbol,
        exchange=args.exchange,
        raw_symbol=args.raw_symbol or okx_raw_symbol_from_canonical(args.symbol),
        range_pct=str(args.range_pct),
        bucket_interval=args.bucket_interval,
        required_buckets=int(args.required_buckets),
        lookback_buckets=int(args.lookback_buckets),
        max_buckets_per_cycle=int(args.max_buckets_per_cycle),
        max_days_per_cycle=int(args.max_days_per_cycle),
        market_db_path=Path(args.market_db),
        checkpoint_db_path=Path(args.checkpoint_db),
        raw_root=Path(args.raw_root),
        status_path=Path(args.status_path),
        lock_path=Path(args.lock_path),
        chunksize=int(args.chunksize),
        mode=args.mode,
        direction=args.direction,
        allow_download=not args.no_download,
        dry_run=bool(args.dry_run),
        force=bool(args.force),
        sleep_seconds=float(args.sleep_seconds),
        save_raw_trades=bool(args.save_raw_trades),
        chunk_sleep_seconds=_mode_float_default(
            args.chunk_sleep_seconds,
            mode=mode,
            live_default=float(_env("AETHER_RANGE_BACKFILL_CHUNK_SLEEP_SECONDS", "0.1")),
            prebuild_default=0.0,
        ),
        max_seconds_per_cycle=_mode_float_default(
            args.max_seconds_per_cycle,
            mode=mode,
            live_default=float(_env("AETHER_RANGE_BACKFILL_MAX_SECONDS_PER_CYCLE", "30")),
            prebuild_default=0.0,
        ),
        max_trades_per_cycle=int(
            _mode_float_default(
                args.max_trades_per_cycle,
                mode=mode,
                live_default=float(_env("AETHER_RANGE_BACKFILL_MAX_TRADES_PER_CYCLE", "300000")),
                prebuild_default=0.0,
            )
        ),
        max_target_end_ms=args.max_target_end_ms,
    )


def maybe_lower_priority(enabled: bool) -> None:
    if not enabled or platform.system().lower().startswith("win"):
        return
    nice = getattr(os, "nice", None)
    if callable(nice):
        nice(10)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    once = resolve_once(args)
    maybe_lower_priority(bool(args.low_priority))
    request = request_from_args(args)
    service = RangeBackfillService(request)
    if args.check_only:
        coverage = service.check_coverage(direction=args.direction)
        print(
            "Range speed coverage | "
            f"complete_history={coverage.complete_history} "
            f"min_periods={coverage.required_buckets} "
            f"missing={coverage.missing_periods} available={coverage.available}",
            flush=True,
        )
        return 0

    status_store = RangeBackfillStatusStore(request.status_path)
    lock = RangeBackfillLock(request.lock_path, status_path=request.status_path)
    if not lock.acquire(mode=request.mode, force=request.force):
        print(f"Range backfill lock busy | path={request.lock_path}", flush=True)
        return 1
    coordinator = RawTradeBackfillCoordinator(
        lock_path=args.global_lock_path,
        status_path=args.global_status_path,
    )
    if not coordinator.try_acquire(
        owner="range_backfill",
        priority=BACKGROUND_BACKFILL_PRIORITY,
        symbol=request.symbol,
        raw_days=request.max_days_per_cycle,
    ):
        holder = coordinator.current_owner() or {}
        status_store.patch(
            running=False,
            phase="global_lock_not_acquired",
            range_speed_available=False,
            range_speed_reason="global_lock_not_acquired",
            global_lock_owner=holder.get("owner"),
            global_lock_pid=holder.get("pid"),
            global_lock_priority=holder.get("priority"),
            worker_heartbeat_ms=now_ms(),
        )
        lock.release()
        print(
            "Range backfill skipped | reason=global_lock_not_acquired "
            f"owner={holder.get('owner')} priority={holder.get('priority')}",
            flush=True,
        )
        return 0

    exit_code = 1
    exit_phase = "failed"
    range_speed_reason: str | None = None
    next_retry_after_ms: int | None = None
    final_summary = None
    try:
        _write_process_status(
            status_store,
            request=request,
            phase="starting",
            running=True,
            exit_code=None,
            range_speed_reason=REASON_ARCHIVE_GAP_BACKFILLING,
        )
        while True:
            summary = service.run_once(
                acquire_lock=False,
                mark_process_finished_on_summary=False,
            )
            print(
                "Range backfill cycle completed | "
                f"status={summary.status} complete_after={summary.complete_after} "
                f"missing_after={summary.missing_after} aggregates_written={summary.aggregates_written} "
                f"target_bucket_start_ms={summary.target_bucket_start_ms} "
                f"target_bucket_end_ms={summary.target_bucket_end_ms} "
                f"selected_archive_dates={list(summary.selected_archive_dates)} "
                f"per_file_min_trade_time_ms={dict(summary.per_file_min_trade_time_ms)} "
                f"per_file_max_trade_time_ms={dict(summary.per_file_max_trade_time_ms)} "
                f"target_trade_count={summary.target_trade_count} "
                f"candidate_range_bars={summary.candidate_range_bars} "
                f"candidate_aggregates={summary.candidate_aggregates} "
                f"filtered_reason_if_zero={summary.filtered_reason_if_zero} "
                f"last_error={summary.last_error}",
                flush=True,
            )
            final_summary = summary
            coordinator.heartbeat()
            if summary.status == "error":
                exit_code = 1
                exit_phase = "failed"
                range_speed_reason = REASON_REPAIR_FAILED_COOLDOWN
                next_retry_after_ms = now_ms() + max(0, int(args.failure_cooldown_seconds)) * 1000
                return exit_code
            if summary.status == "dry_run":
                exit_code = 0
                exit_phase = "completed"
                return exit_code
            if summary.missing_after <= 0:
                exit_code = 0
                exit_phase = "completed"
                range_speed_reason = REASON_AVAILABLE
                return exit_code
            no_progress_reason = _no_progress_reason(
                summary, exchange=request.exchange
            )
            partial_without_progress = _partial_without_progress(summary)
            if (
                summary.status == "no_progress"
                or partial_without_progress
                or no_progress_reason == REASON_CURRENT_DAY_ARCHIVE_NOT_READY
            ):
                exit_code = 0
                exit_phase = "no_progress"
                if no_progress_reason == REASON_CURRENT_DAY_ARCHIVE_NOT_READY:
                    range_speed_reason = no_progress_reason
                elif partial_without_progress:
                    range_speed_reason = REASON_ARCHIVE_GAP_PARTIAL_NO_PROGRESS
                else:
                    range_speed_reason = no_progress_reason or REASON_ARCHIVE_GAP_NO_PROGRESS
                next_retry_after_ms = _next_retry_after_ms(
                    reason=range_speed_reason,
                    failure_cooldown_seconds=int(args.failure_cooldown_seconds),
                    archive_not_ready_cooldown_seconds=int(args.archive_not_ready_cooldown_seconds),
                    daily_retry_after_utc_hour=int(args.daily_retry_after_utc_hour),
                    archive_day=max(_summary_raw_days(summary), default=None),
                )
                print(
                    "Range backfill worker exiting without progress | "
                    f"reason={range_speed_reason} next_retry_after_ms={next_retry_after_ms}",
                    flush=True,
                )
                return exit_code
            if once:
                exit_code = 0 if summary.status in {"ok", "dry_run", "partial", "no_progress", "archive_not_ready"} else 1
                exit_phase = "completed" if exit_code == 0 else "failed"
                return exit_code
            time.sleep(max(0.0, float(args.sleep_seconds)))
    finally:
        try:
            _write_process_status(
                status_store,
                request=request,
                phase=exit_phase,
                running=False,
                exit_code=exit_code,
                range_speed_reason=range_speed_reason,
                next_retry_after_ms=next_retry_after_ms,
                summary=final_summary,
            )
        finally:
            coordinator.release()
            lock.release()


def resolve_once(args: argparse.Namespace) -> bool:
    if args.once is not None:
        return bool(args.once)
    return str(args.mode).strip().lower() != "live"


def _write_process_status(
    status_store: RangeBackfillStatusStore,
    *,
    request: RangeBackfillRequest,
    phase: str,
    running: bool,
    exit_code: int | None,
    range_speed_reason: str | None = None,
    next_retry_after_ms: int | None = None,
    summary=None,
) -> None:
    heartbeat = now_ms()
    if summary is not None and not running:
        aggregates_written = int(getattr(summary, "aggregates_written", 0) or 0)
        complete_improved = int(getattr(summary, "complete_after", 0) or 0) > int(
            getattr(summary, "complete_before", 0) or 0
        )
        if exit_code not in (None, 0):
            repair_status = DAILY_ARCHIVE_BACKFILL_FAILED
        elif aggregates_written > 0 or complete_improved:
            repair_status = DAILY_ARCHIVE_BACKFILL_SUCCESS
        else:
            repair_status = DAILY_ARCHIVE_BACKFILL_FAILED
    elif running:
        repair_status = DAILY_ARCHIVE_BACKFILL_RUNNING
    elif exit_code not in (None, 0):
        repair_status = DAILY_ARCHIVE_BACKFILL_FAILED
    else:
        repair_status = DAILY_ARCHIVE_BACKFILL_SUCCESS
    payload = {
        "mode": request.mode,
        "direction": request.direction,
        "pid": os.getpid(),
        "running": running,
        "phase": phase,
        "worker_heartbeat_ms": heartbeat,
        "heartbeat_ms": heartbeat,
        "symbol": request.symbol,
        "exchange": request.exchange,
        "range_pct": request.range_pct,
        "bucket_interval": request.bucket_interval,
        "required_buckets": request.required_buckets,
        "lookback_buckets": request.lookback_buckets,
        "exit_code": exit_code,
        "repair_status": repair_status,
        "finished_at_ms": now_ms() if not running else None,
    }
    if range_speed_reason is not None:
        payload["range_speed_reason"] = range_speed_reason
        payload["range_speed_available"] = range_speed_reason == REASON_AVAILABLE
    if next_retry_after_ms is not None:
        payload["next_retry_after_ms"] = int(next_retry_after_ms)
    elif running or range_speed_reason == REASON_AVAILABLE:
        payload["next_retry_after_ms"] = None
    if summary is not None:
        payload.update(
            complete_after=int(summary.complete_after),
            missing_after=int(summary.missing_after),
            cycle_status=summary.status,
            missing_raw_days=list(summary.missing_raw_days),
            failed_downloads=list(summary.failed_downloads),
            last_error=summary.last_error,
            target_bucket_start_ms=summary.target_bucket_start_ms,
            target_bucket_end_ms=summary.target_bucket_end_ms,
            selected_archive_dates=list(summary.selected_archive_dates),
            per_file_min_trade_time_ms=dict(summary.per_file_min_trade_time_ms),
            per_file_max_trade_time_ms=dict(summary.per_file_max_trade_time_ms),
            target_trade_count=int(summary.target_trade_count),
            candidate_range_bars=int(summary.candidate_range_bars),
            candidate_aggregates=int(summary.candidate_aggregates),
            aggregates_written=int(summary.aggregates_written),
            filtered_reason_if_zero=summary.filtered_reason_if_zero,
            repair_method=getattr(summary, "repair_method", ""),
            target_window_reached=bool(getattr(summary, "target_window_reached", False)),
            target_bucket_proven_complete=bool(getattr(summary, "target_bucket_proven_complete", False)),
            anchor_last_trade_ts_ms=getattr(summary, "anchor_last_trade_ts_ms", None),
            replay_start_ms=getattr(summary, "replay_start_ms", None),
            replay_end_ms=getattr(summary, "replay_end_ms", None),
            pre_replay_existing_range_bars=int(getattr(summary, "pre_replay_existing_range_bars", 0)),
            generated_range_bars=int(getattr(summary, "generated_range_bars", 0)),
            combined_range_bars=int(getattr(summary, "combined_range_bars", 0)),
        )
    status_store.patch(**payload)


def _no_progress_reason(
    summary,
    *,
    now_ms_value: int | None = None,
    exchange: str = "okx",
) -> str | None:
    failed_days = _summary_raw_days(summary)
    if not failed_days:
        return REASON_ARCHIVE_GAP_NO_PROGRESS if summary.status == "no_progress" else None
    now_value = (
        int(datetime.now(UTC).timestamp() * 1000)
        if now_ms_value is None
        else int(now_ms_value)
    )
    current_source_day = (
        okx_archive_date_from_utc_ms(now_value)
        if str(exchange).strip().lower() == "okx"
        else datetime.fromtimestamp(now_value / 1000, tz=UTC).date()
    )
    if all(day >= current_source_day for day in failed_days):
        return REASON_CURRENT_DAY_ARCHIVE_NOT_READY
    return REASON_ARCHIVE_GAP_NO_PROGRESS if summary.status == "no_progress" else None


def _partial_without_progress(summary) -> bool:
    return (
        summary.status == "partial"
        and int(summary.aggregates_written) == 0
        and int(summary.range_bars_written) == 0
        and int(summary.missing_after) > 0
    )


def _summary_raw_days(summary) -> tuple[date, ...]:
    values = [str(value) for value in summary.missing_raw_days]
    values.extend(str(value) for value in summary.failed_downloads)
    days: list[date] = []
    for value in values:
        for match in _DATE_PATTERN.findall(value):
            try:
                parsed = date.fromisoformat(match)
            except ValueError:
                continue
            if parsed not in days:
                days.append(parsed)
    return tuple(days)


def _next_retry_after_ms(
    *,
    reason: str,
    failure_cooldown_seconds: int,
    archive_not_ready_cooldown_seconds: int,
    daily_retry_after_utc_hour: int,
    now_ms_value: int | None = None,
    archive_day: date | None = None,
) -> int:
    now = _utc_datetime(now_ms_value)
    if reason == REASON_CURRENT_DAY_ARCHIVE_NOT_READY:
        retry_hour = min(23, max(0, int(daily_retry_after_utc_hour)))
        next_day = (
            archive_day + timedelta(days=1)
            if archive_day is not None
            else (now + timedelta(days=1)).date()
        )
        daily_retry = datetime(
            next_day.year,
            next_day.month,
            next_day.day,
            retry_hour,
            tzinfo=UTC,
        )
        cooldown_retry = now + timedelta(seconds=max(0, int(archive_not_ready_cooldown_seconds)))
        retry_at = max(daily_retry, cooldown_retry)
    else:
        retry_at = now + timedelta(seconds=max(0, int(failure_cooldown_seconds)))
    return int(retry_at.timestamp() * 1000)


def _utc_datetime(now_ms_value: int | None = None) -> datetime:
    if now_ms_value is None:
        return datetime.now(UTC)
    return datetime.fromtimestamp(int(now_ms_value) / 1000, tz=UTC)


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _mode_float_default(value, *, mode: str, live_default: float, prebuild_default: float) -> float:
    if value is not None:
        return float(value)
    return float(live_default if mode == "live" else prebuild_default)


if __name__ == "__main__":
    raise SystemExit(main())
