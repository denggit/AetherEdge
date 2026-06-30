from __future__ import annotations

import argparse
import os
from pathlib import Path
import platform
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.models import RangeBackfillRequest
from src.market_data.backfill.lock import RangeBackfillLock
from src.market_data.backfill.service import RangeBackfillService
from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.market_data.historical_trades.okx_archive import okx_raw_symbol_from_canonical


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
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--no-download", action="store_true")
    parser.add_argument("--low-priority", action=argparse.BooleanOptionalAction, default=_bool(_env("AETHER_RANGE_BACKFILL_LOW_PRIORITY", "true")))
    parser.add_argument("--once", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--save-raw-trades", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--chunk-sleep-seconds", type=float, default=None)
    parser.add_argument("--max-seconds-per-cycle", type=float, default=None)
    parser.add_argument("--max-trades-per-cycle", type=int, default=None)
    parser.add_argument("--max-target-end-ms", type=int, default=None)
    return parser


def request_from_args(args: argparse.Namespace) -> RangeBackfillRequest:
    mode = str(args.mode).strip().lower()
    save_raw_trades = args.save_raw_trades
    if save_raw_trades is None:
        save_raw_trades = mode != "live"
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
        save_raw_trades=bool(save_raw_trades),
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
            f"missing={coverage.missing_periods} available={coverage.available}"
        )
        return 0

    status_store = RangeBackfillStatusStore(request.status_path)
    lock = RangeBackfillLock(request.lock_path, status_path=request.status_path)
    if not lock.acquire(mode=request.mode, force=request.force):
        print(f"Range backfill lock busy | path={request.lock_path}")
        return 1

    exit_code = 1
    try:
        _write_process_status(
            status_store,
            request=request,
            phase="starting",
            running=True,
            exit_code=None,
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
                f"last_error={summary.last_error}"
            )
            if summary.status == "error":
                exit_code = 1
                return exit_code
            if summary.status == "dry_run":
                exit_code = 0
                return exit_code
            if summary.missing_after <= 0:
                exit_code = 0
                return exit_code
            if once:
                exit_code = 0 if summary.status in {"ok", "dry_run", "partial", "no_progress"} else 1
                return exit_code
            time.sleep(max(0.0, float(args.sleep_seconds)))
    finally:
        _write_process_status(
            status_store,
            request=request,
            phase="completed" if exit_code == 0 else "failed",
            running=False,
            exit_code=exit_code,
        )
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
) -> None:
    status_store.patch(
        mode=request.mode,
        direction=request.direction,
        pid=os.getpid(),
        running=running,
        phase=phase,
        heartbeat_ms=now_ms(),
        symbol=request.symbol,
        exchange=request.exchange,
        range_pct=request.range_pct,
        bucket_interval=request.bucket_interval,
        required_buckets=request.required_buckets,
        lookback_buckets=request.lookback_buckets,
        exit_code=exit_code,
        finished_at_ms=now_ms() if not running else None,
    )


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
