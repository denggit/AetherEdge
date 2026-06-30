from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.models import RangeBackfillRequest, RangeBackfillSummary
from src.market_data.backfill.service import RangeBackfillService
from src.market_data.historical_trades.okx_archive import okx_raw_symbol_from_canonical


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prebuild completed range-speed history.")
    parser.add_argument("--symbol", default=_env("AETHER_MARKET", "ETH-USDT-PERP"))
    parser.add_argument("--exchange", default=_env("AETHER_DATA_EXCHANGE", "okx"))
    parser.add_argument("--raw-symbol", default=None)
    parser.add_argument("--range-pct", default=_env("AETHER_RANGE_PCT", "0.002"))
    parser.add_argument("--bucket-interval", default=_env("AETHER_CLOSED_BAR_INTERVAL", "4h"))
    parser.add_argument("--buckets", type=int, default=160)
    parser.add_argument("--lookback-buckets", type=int, default=None)
    parser.add_argument("--market-db", default=_env("AETHER_MARKET_DATA_DB", "data/market_data/aether_market_data.sqlite3"))
    parser.add_argument("--checkpoint-db", default=_env("AETHER_RANGE_CHECKPOINT_DB", "data/state/range_builder_checkpoint.sqlite3"))
    parser.add_argument("--raw-root", default=_env("AETHER_RANGE_BACKFILL_RAW_ROOT", "data/okx/raw/trades"))
    parser.add_argument("--status-path", default=_env("AETHER_RANGE_BACKFILL_STATUS_PATH", "data/state/range_backfill_status.json"))
    parser.add_argument("--lock-path", default=_env("AETHER_RANGE_BACKFILL_LOCK_PATH", "data/state/range_backfill.lock"))
    parser.add_argument("--chunksize", type=int, default=int(_env("AETHER_RANGE_BACKFILL_CHUNKSIZE", "50000")))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-download", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    return parser


def request_from_args(args: argparse.Namespace) -> RangeBackfillRequest:
    raw_symbol = args.raw_symbol or okx_raw_symbol_from_canonical(args.symbol)
    lookback = args.lookback_buckets or max(args.buckets, 160)
    return RangeBackfillRequest(
        symbol=args.symbol,
        exchange=args.exchange,
        raw_symbol=raw_symbol,
        range_pct=str(args.range_pct),
        bucket_interval=args.bucket_interval,
        required_buckets=int(args.buckets),
        lookback_buckets=int(lookback),
        max_buckets_per_cycle=int(args.buckets),
        max_days_per_cycle=10_000,
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
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    request = request_from_args(args)
    service = RangeBackfillService(request)
    if args.check_only:
        coverage = service.check_coverage(direction="oldest-to-recent")
        print("Range speed coverage")
        print(
            f"symbol={coverage.symbol} exchange={coverage.exchange} "
            f"range_pct={coverage.range_pct} interval={coverage.bucket_interval}"
        )
        print(
            f"complete_history={coverage.complete_history} "
            f"min_periods={coverage.required_buckets} "
            f"missing={coverage.missing_periods} available={coverage.available}"
        )
        return 0
    summary = service.run_once()
    print_summary(summary)
    return 0 if summary.status in {"ok", "dry_run"} else 1


def print_summary(summary: RangeBackfillSummary) -> None:
    print("Range speed prebuild summary")
    print(f"symbol: {summary.symbol}")
    print(f"exchange: {summary.exchange}")
    print(f"range_pct: {summary.range_pct}")
    print(f"bucket_interval: {summary.bucket_interval}")
    print(f"target_buckets: {summary.target_buckets}")
    print(f"complete_before: {summary.complete_before}")
    print(f"complete_after: {summary.complete_after}")
    print(f"missing_before: {summary.missing_before}")
    print(f"missing_after: {summary.missing_after}")
    print(f"downloaded_files: {summary.downloaded_files}")
    print(f"trades_loaded: {summary.trades_loaded}")
    print(f"range_bars_written: {summary.range_bars_written}")
    print(f"aggregates_written: {summary.aggregates_written}")
    print(f"elapsed_seconds: {summary.elapsed_seconds:.3f}")
    print(f"status: {summary.status}")
    if summary.last_error:
        print(f"last_error: {summary.last_error}")


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


if __name__ == "__main__":
    raise SystemExit(main())
