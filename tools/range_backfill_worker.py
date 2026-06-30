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
from src.market_data.backfill.service import RangeBackfillService
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
    parser.add_argument("--once", action=argparse.BooleanOptionalAction, default=True)
    return parser


def request_from_args(args: argparse.Namespace) -> RangeBackfillRequest:
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
    )


def maybe_lower_priority(enabled: bool) -> None:
    if not enabled or platform.system().lower().startswith("win"):
        return
    nice = getattr(os, "nice", None)
    if callable(nice):
        nice(10)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
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

    while True:
        summary = service.run_once()
        print(
            "Range backfill cycle completed | "
            f"status={summary.status} complete_after={summary.complete_after} "
            f"missing_after={summary.missing_after} aggregates_written={summary.aggregates_written} "
            f"last_error={summary.last_error}"
        )
        if summary.status not in {"ok", "dry_run"}:
            return 1
        if summary.missing_after <= 0 or args.once:
            return 0
        time.sleep(max(0.0, float(args.sleep_seconds)))


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    raise SystemExit(main())
