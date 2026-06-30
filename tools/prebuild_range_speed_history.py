from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, time as datetime_time, timedelta
from decimal import Decimal
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.models import RangeBackfillRequest, RangeBackfillSummary
from src.market_data.backfill.coverage import iter_utc_dates, previous_utc_day_start_ms
from src.market_data.backfill.service import RangeBackfillService
from src.market_data.historical_trades.okx_archive import okx_raw_symbol_from_canonical
from src.market_data.range_checkpoint import MIN_VALID_COMPLETED_AGGREGATE_MS
from src.utils.sqlite_backup import backup_sqlite_database

SUSPICIOUS_BUCKET_CUTOFF_MS = MIN_VALID_COMPLETED_AGGREGATE_MS
SQLITE_BACKUP_KEEP = 5


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prebuild completed range-speed history.")
    parser.add_argument("--symbol", default=_env("AETHER_MARKET", "ETH-USDT-PERP"))
    parser.add_argument("--exchange", default=_env("AETHER_DATA_EXCHANGE", "okx"))
    parser.add_argument("--raw-symbol", default=None)
    parser.add_argument("--range-pct", default=_env("AETHER_RANGE_PCT", "0.002"))
    parser.add_argument("--bucket-interval", default=_env("AETHER_CLOSED_BAR_INTERVAL", "4h"))
    parser.add_argument("--buckets", type=int, default=160)
    parser.add_argument("--lookback-buckets", type=int, default=None)
    parser.add_argument("--batch-buckets", type=int, default=6)
    parser.add_argument("--batch-days", type=int, default=2)
    parser.add_argument("--max-cycles", type=int, default=1000)
    parser.add_argument("--progress-seconds", type=float, default=30.0)
    parser.add_argument("--profile-one-bucket", action="store_true")
    parser.add_argument("--max-chunks", type=int, default=0)
    parser.add_argument("--clean-suspicious", action="store_true")
    parser.add_argument("--complete-before-days", type=int, default=1)
    parser.add_argument("--end-before-date", default=None)
    parser.add_argument("--max-target-end-ms", type=int, default=None)
    parser.add_argument("--market-db", default=_env("AETHER_MARKET_DATA_DB", "data/market_data/aether_market_data.sqlite3"))
    parser.add_argument("--checkpoint-db", default=_env("AETHER_RANGE_CHECKPOINT_DB", "data/state/range_builder_checkpoint.sqlite3"))
    parser.add_argument("--raw-root", default=_env("AETHER_RANGE_BACKFILL_RAW_ROOT", "data/okx/raw/trades"))
    parser.add_argument("--status-path", default=_env("AETHER_RANGE_BACKFILL_STATUS_PATH", "data/state/range_backfill_status.json"))
    parser.add_argument("--lock-path", default=_env("AETHER_RANGE_BACKFILL_LOCK_PATH", "data/state/range_backfill.lock"))
    parser.add_argument("--backup-dir", default=_env("AETHER_SQLITE_BACKUP_DIR", "data/state/backups"))
    parser.add_argument("--chunksize", type=int, default=int(_env("AETHER_RANGE_BACKFILL_CHUNKSIZE", "50000")))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--check-raw", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-download", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--save-raw-trades", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--chunk-sleep-seconds", type=float, default=0.0)
    parser.add_argument("--max-seconds-per-cycle", type=float, default=0.0)
    parser.add_argument("--max-trades-per-cycle", type=int, default=0)
    return parser


def request_from_args(args: argparse.Namespace) -> RangeBackfillRequest:
    raw_symbol = args.raw_symbol or okx_raw_symbol_from_canonical(args.symbol)
    target_buckets = 1 if args.profile_one_bucket else int(args.buckets)
    lookback = args.lookback_buckets or target_buckets
    return RangeBackfillRequest(
        symbol=args.symbol,
        exchange=args.exchange,
        raw_symbol=raw_symbol,
        range_pct=str(args.range_pct),
        bucket_interval=args.bucket_interval,
        required_buckets=int(target_buckets),
        lookback_buckets=int(lookback),
        max_buckets_per_cycle=max(1, int(args.batch_buckets)),
        max_days_per_cycle=max(1, int(args.batch_days)),
        market_db_path=Path(args.market_db),
        checkpoint_db_path=Path(args.checkpoint_db),
        raw_root=Path(args.raw_root),
        status_path=Path(args.status_path),
        lock_path=Path(args.lock_path),
        chunksize=int(args.chunksize),
        mode="prebuild",
        direction="oldest-to-recent",
        allow_download=not args.no_download,
        dry_run=bool(args.dry_run),
        force=bool(args.force),
        sleep_seconds=float(args.sleep_seconds),
        save_raw_trades=bool(args.save_raw_trades),
        chunk_sleep_seconds=float(args.chunk_sleep_seconds),
        max_seconds_per_cycle=float(args.max_seconds_per_cycle),
        max_trades_per_cycle=int(args.max_trades_per_cycle),
        max_chunks_per_cycle=max(0, int(args.max_chunks)),
        progress_seconds=max(0.0, float(args.progress_seconds)),
        max_target_end_ms=resolve_max_target_end_ms(args),
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    request = request_from_args(args)
    progress = ProgressPrinter()
    service = RangeBackfillService(request, progress_callback=progress)
    _handle_suspicious_aggregates(args, request)
    if args.check_only:
        coverage = service.check_coverage(direction="oldest-to-recent")
        print("Range speed coverage", flush=True)
        print(
            f"symbol={coverage.symbol} exchange={coverage.exchange} "
            f"range_pct={coverage.range_pct} interval={coverage.bucket_interval}",
            flush=True,
        )
        print(
            f"complete_history={coverage.complete_history} "
            f"min_periods={coverage.required_buckets} "
            f"missing={coverage.missing_periods} available={coverage.available}",
            flush=True,
        )
        if args.check_raw:
            raw = raw_coverage_report(service, coverage=coverage, raw_symbol=request.raw_symbol or okx_raw_symbol_from_canonical(request.symbol))
            print(f"raw_required_days={raw['required']}", flush=True)
            print(f"raw_available_days={raw['available']}", flush=True)
            print(f"raw_missing_days={raw['missing']}", flush=True)
            if raw["missing_days"]:
                print(f"first_missing_raw_day={raw['missing_days'][0]}", flush=True)
        return 0
    print(
        "Range speed prebuild started | "
        f"symbol={request.symbol} exchange={request.exchange} range_pct={request.range_pct} "
        f"target_buckets={request.required_buckets} batch_buckets={request.max_buckets_per_cycle} "
        f"batch_days={request.max_days_per_cycle} max_cycles={args.max_cycles} "
        f"max_target_end_ms={request.max_target_end_ms}",
        flush=True,
    )

    final_summary: RangeBackfillSummary | None = None
    max_cycles = max(1, int(args.max_cycles))
    for cycle in range(1, max_cycles + 1):
        progress.cycle = cycle
        coverage = service.check_coverage(direction="oldest-to-recent")
        print(
            "Coverage before | "
            f"cycle={cycle} complete={coverage.required_window_complete_count} "
            f"missing={coverage.missing_periods} available={str(coverage.available).lower()} "
            f"lookback_missing={len(coverage.lookback_missing_buckets)}",
            flush=True,
        )
        if coverage.available and not coverage.lookback_missing_buckets:
            print(
                "Final summary | "
                f"status=ok complete_after={coverage.required_window_complete_count} "
                "missing_after=0",
                flush=True,
            )
            return 0

        started = time.monotonic()
        summary = service.run_once()
        final_summary = summary
        print_summary(summary, title=f"Batch {cycle} summary")
        print(
            "Batch timing | "
            f"cycle={cycle} elapsed_seconds={time.monotonic() - started:.3f}",
            flush=True,
        )
        if summary.status in {"error", "lock_busy"}:
            print(
                f"Final summary | status={summary.status} last_error={summary.last_error}",
                flush=True,
            )
            return 1
        if summary.status == "no_progress":
            print(
                "No progress | "
                f"missing_raw_days={list(summary.missing_raw_days)} "
                f"failed_downloads={list(summary.failed_downloads)} "
                f"hint={summary.hint}",
                flush=True,
            )
            print_final_summary(summary)
            return 0
        if args.max_chunks > 0:
            print(
                "Max chunks diagnostic complete | "
                f"max_chunks={args.max_chunks}",
                flush=True,
            )
            print_final_summary(summary)
            return 0
        if summary.missing_after <= 0:
            print_final_summary(summary)
            return 0
        if args.sleep_seconds > 0:
            time.sleep(max(0.0, float(args.sleep_seconds)))

    if final_summary is not None:
        print(
            "Final summary | "
            f"status=max_cycles complete_after={final_summary.complete_after} "
            f"missing_after={final_summary.missing_after}",
            flush=True,
        )
    return 0


class ProgressPrinter:
    def __init__(self) -> None:
        self.cycle = 0

    def __call__(self, event: str, payload: Mapping[str, object]) -> None:
        prefix = f"batch={self.cycle} " if self.cycle else ""
        if event == "coverage_checked":
            print(
                "Coverage checked | "
                f"{prefix}complete={payload.get('complete')} missing={payload.get('missing')} "
                f"available={str(payload.get('available')).lower()}",
                flush=True,
            )
        elif event == "gaps_selected":
            print(
                "Selected gaps | "
                f"{prefix}gaps={payload.get('gaps')} first={payload.get('first_bucket_end_ms')} "
                f"last={payload.get('last_bucket_end_ms')}",
                flush=True,
            )
        elif event == "build_window_started":
            print(
                "Batch started | "
                f"{prefix}gaps={payload.get('gaps')} first={payload.get('first_bucket_end_ms')} "
                f"last={payload.get('last_bucket_end_ms')} anchor_start={payload.get('anchor_start_ms')} "
                f"target_end={payload.get('target_end_ms')}",
                flush=True,
            )
        elif event == "ensuring_raw_days":
            print(
                "Ensuring raw days | "
                f"{prefix}days={payload.get('days')} first_day={payload.get('first_day')} "
                f"last_day={payload.get('last_day')}",
                flush=True,
            )
        elif event == "raw_day_ready":
            print(
                "Raw day ready | "
                f"{prefix}day={payload.get('day')} downloaded={str(payload.get('downloaded')).lower()} "
                f"size={payload.get('size')} path={payload.get('path')}",
                flush=True,
            )
        elif event == "raw_day_missing":
            print(
                "Raw day missing | "
                f"{prefix}day={payload.get('day')} url={payload.get('url')} error={payload.get('error')}",
                flush=True,
            )
        elif event == "file_read_started":
            print(
                "Reading raw zip | "
                f"{prefix}day={payload.get('day')} size={payload.get('size')} path={payload.get('path')}",
                flush=True,
            )
        elif event == "chunk_progress":
            print(
                "Chunk progress | "
                f"{prefix}file={payload.get('file')} chunk_index={payload.get('chunk_index')} "
                f"file_chunk_index={payload.get('file_chunk_index')} raw_rows={payload.get('raw_rows')} "
                f"filtered_rows={payload.get('filtered_rows')} valid_trades={payload.get('valid_trades')} "
                f"dropped_rows={payload.get('dropped_rows')} chunk_raw_rows={payload.get('chunk_raw_rows')} "
                f"chunk_filtered_rows={payload.get('chunk_filtered_rows')} "
                f"chunk_valid_trades={payload.get('chunk_valid_trades')} "
                f"range_bars_buffered={payload.get('range_bars_buffered')} "
                f"first_trade_time_ms={payload.get('first_trade_time_ms')} "
                f"last_trade_time_ms={payload.get('last_trade_time_ms')} "
                f"elapsed_seconds={float(payload.get('elapsed_seconds') or 0):.3f}",
                flush=True,
            )
        elif event == "writing_range_bars":
            print(
                "Writing range bars | "
                f"{prefix}rows={payload.get('rows')} start={payload.get('start_time_ms')} "
                f"end={payload.get('end_time_ms')}",
                flush=True,
            )
        elif event == "range_bars_written":
            print(f"Range bars written | {prefix}rows={payload.get('rows')}", flush=True)
        elif event == "writing_aggregates":
            print(f"Writing aggregates | {prefix}rows={payload.get('rows')}", flush=True)
        elif event == "aggregates_written":
            print(f"Aggregates written | {prefix}rows={payload.get('rows')}", flush=True)
        elif event == "file_read_stopped":
            print(
                "Reading raw zip stopped | "
                f"{prefix}day={payload.get('day')} reason={payload.get('reason')} "
                f"first_trade_time_ms={payload.get('first_trade_time_ms')} "
                f"target_end_ms={payload.get('target_end_ms')}",
                flush=True,
            )


def print_summary(summary: RangeBackfillSummary, *, title: str = "Range speed prebuild summary") -> None:
    print(title, flush=True)
    print(f"symbol: {summary.symbol}", flush=True)
    print(f"exchange: {summary.exchange}", flush=True)
    print(f"range_pct: {summary.range_pct}", flush=True)
    print(f"bucket_interval: {summary.bucket_interval}", flush=True)
    print(f"target_buckets: {summary.target_buckets}", flush=True)
    print(f"complete_before: {summary.complete_before}", flush=True)
    print(f"complete_after: {summary.complete_after}", flush=True)
    print(f"missing_before: {summary.missing_before}", flush=True)
    print(f"missing_after: {summary.missing_after}", flush=True)
    print(f"downloaded_files: {summary.downloaded_files}", flush=True)
    print(f"raw_rows: {summary.raw_rows}", flush=True)
    print(f"filtered_rows: {summary.filtered_rows}", flush=True)
    print(f"dropped_rows: {summary.dropped_rows}", flush=True)
    print(f"trades_loaded: {summary.trades_loaded}", flush=True)
    print(f"range_bars_written: {summary.range_bars_written}", flush=True)
    print(f"aggregates_written: {summary.aggregates_written}", flush=True)
    print(f"missing_raw_days: {list(summary.missing_raw_days)}", flush=True)
    print(f"failed_downloads: {list(summary.failed_downloads)}", flush=True)
    print(f"skipped_buckets_due_missing_raw: {summary.skipped_buckets_due_missing_raw}", flush=True)
    print(f"elapsed_seconds: {summary.elapsed_seconds:.3f}", flush=True)
    print(f"status: {summary.status}", flush=True)
    if summary.last_error:
        print(f"last_error: {summary.last_error}", flush=True)
    if summary.hint:
        print(f"hint: {summary.hint}", flush=True)


def print_final_summary(summary: RangeBackfillSummary) -> None:
    print(
        "Final summary | "
        f"status={summary.status} complete_after={summary.complete_after} "
        f"missing_after={summary.missing_after} raw_rows={summary.raw_rows} "
        f"filtered_rows={summary.filtered_rows} trades_loaded={summary.trades_loaded} "
        f"range_bars_written={summary.range_bars_written} "
        f"aggregates_written={summary.aggregates_written}",
        flush=True,
    )


def _handle_suspicious_aggregates(
    args: argparse.Namespace,
    request: RangeBackfillRequest,
) -> None:
    suspicious = suspicious_aggregate_count(request)
    if args.clean_suspicious:
        if suspicious > 0:
            backup_sqlite_database(
                request.checkpoint_db_path,
                backup_dir=Path(args.backup_dir),
                keep=SQLITE_BACKUP_KEEP,
                before_backup=lambda path: print(
                    f"SQLite backup path | source={request.checkpoint_db_path} backup={path}",
                    flush=True,
                ),
            )
        deleted = clean_suspicious_aggregates(request)
        print(
            "Suspicious aggregates cleaned | "
            f"deleted={deleted} cutoff_ms={SUSPICIOUS_BUCKET_CUTOFF_MS}",
            flush=True,
        )
    elif suspicious > 0:
        print(
            "Suspicious aggregates warning | "
            f"count={suspicious} cutoff_ms={SUSPICIOUS_BUCKET_CUTOFF_MS} "
            "run with --clean-suspicious to delete them",
            flush=True,
        )


def suspicious_aggregate_count(request: RangeBackfillRequest) -> int:
    if not request.checkpoint_db_path.exists():
        return 0
    with sqlite3.connect(request.checkpoint_db_path) as conn:
        try:
            row = conn.execute(
                """
                SELECT COUNT(*)
                FROM completed_range_aggregates
                WHERE exchange = ?
                  AND symbol = ?
                  AND range_pct = ?
                  AND (
                      bucket_start_ms < ?
                      OR bucket_end_ms < ?
                      OR bucket_end_ms <= bucket_start_ms
                  )
                """,
                (
                    str(request.exchange).lower(),
                    request.symbol,
                    _decimal_text(request.range_pct),
                    SUSPICIOUS_BUCKET_CUTOFF_MS,
                    SUSPICIOUS_BUCKET_CUTOFF_MS,
                ),
            ).fetchone()
        except sqlite3.OperationalError:
            return 0
    return int(row[0] or 0)


def clean_suspicious_aggregates(request: RangeBackfillRequest) -> int:
    if not request.checkpoint_db_path.exists():
        return 0
    with sqlite3.connect(request.checkpoint_db_path) as conn:
        cursor = conn.execute(
            """
            DELETE FROM completed_range_aggregates
            WHERE exchange = ?
              AND symbol = ?
              AND range_pct = ?
              AND (
                  bucket_start_ms < ?
                  OR bucket_end_ms < ?
                  OR bucket_end_ms <= bucket_start_ms
              )
            """,
            (
                str(request.exchange).lower(),
                request.symbol,
                _decimal_text(request.range_pct),
                SUSPICIOUS_BUCKET_CUTOFF_MS,
                SUSPICIOUS_BUCKET_CUTOFF_MS,
            ),
        )
    return int(cursor.rowcount or 0)


def raw_coverage_report(service: RangeBackfillService, *, coverage, raw_symbol: str) -> dict[str, object]:
    days = []
    for gap in coverage.lookback_missing_buckets:
        anchor = previous_utc_day_start_ms(gap.bucket_start_ms)
        days.extend(day.isoformat() for day in iter_utc_dates(anchor, gap.bucket_end_ms))
    unique_days = tuple(dict.fromkeys(days))
    missing = [
        day
        for day in unique_days
        if not service.archive.local_path(
            raw_symbol=raw_symbol,
            day=date.fromisoformat(day),
        ).exists()
    ]
    return {
        "required": len(unique_days),
        "available": len(unique_days) - len(missing),
        "missing": len(missing),
        "missing_days": missing[:10],
    }


def resolve_max_target_end_ms(args: argparse.Namespace) -> int | None:
    candidates: list[int] = []
    if args.max_target_end_ms is not None:
        candidates.append(int(args.max_target_end_ms))
    if args.end_before_date:
        end_before = date.fromisoformat(str(args.end_before_date))
        candidates.append(_utc_day_start_ms(end_before) - 1)
    complete_before_days = int(args.complete_before_days)
    if complete_before_days >= 0:
        today = datetime.now(UTC).date()
        last_complete_day = today - timedelta(days=complete_before_days)
        candidates.append(_utc_day_start_ms(last_complete_day + timedelta(days=1)) - 1)
    return min(candidates) if candidates else None


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _decimal_text(value: Decimal | str) -> str:
    return format(Decimal(str(value)).normalize(), "f")


def _utc_day_start_ms(day: date) -> int:
    return int(datetime.combine(day, datetime_time.min, tzinfo=UTC).timestamp() * 1000)


if __name__ == "__main__":
    raise SystemExit(main())
