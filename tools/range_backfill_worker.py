#!/usr/bin/env python
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.worker import RangeBackfillWorker, print_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AetherEdge range-speed backfill worker")
    parser.add_argument("--mode", choices=("daemon", "once"), default="once")
    parser.add_argument("--symbol", default="ETH-USDT-PERP")
    parser.add_argument("--raw-symbol", default="ETH-USDT-SWAP")
    parser.add_argument("--exchange", default="okx")
    parser.add_argument("--range-pct", default="0.002")
    parser.add_argument("--bucket-interval", default="4h")
    parser.add_argument("--required-buckets", type=int, default=100)
    parser.add_argument("--lookback-buckets", type=int, default=180)
    parser.add_argument("--raw-root", default="data/okx/raw")
    parser.add_argument("--market-db", default="data/market_data/aether_market_data.sqlite3")
    parser.add_argument("--checkpoint-db", default="data/state/range_builder_checkpoint.sqlite3")
    parser.add_argument("--max-buckets-per-cycle", type=int, default=1)
    parser.add_argument("--cycle-sleep-seconds", type=float, default=30.0)
    parser.add_argument("--download-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--chunksize", type=int, default=300_000)
    parser.add_argument("--max-rest-tail-gap-minutes", type=int, default=240)
    parser.add_argument("--max-rest-tail-buckets", type=int, default=12)
    parser.add_argument("--warning-interval-seconds", type=float, default=600.0)
    parser.add_argument("--json-status", default="data/reports/range_backfill/status.json")
    parser.add_argument("--pid-file", default="data/run/range_backfill_worker.pid")
    parser.add_argument("--lock-file", default="data/run/range_backfill_worker.lock")
    parser.add_argument("--daemon-test-cycles", type=int, default=None, help=argparse.SUPPRESS)
    return parser


def worker_from_args(args: argparse.Namespace) -> RangeBackfillWorker:
    return RangeBackfillWorker(
        exchange=args.exchange,
        symbol=args.symbol,
        raw_symbol=args.raw_symbol,
        range_pct=args.range_pct,
        bucket_interval=args.bucket_interval,
        required_buckets=args.required_buckets,
        lookback_buckets=args.lookback_buckets,
        raw_root=args.raw_root,
        market_db=args.market_db,
        checkpoint_db=args.checkpoint_db,
        max_buckets_per_cycle=args.max_buckets_per_cycle,
        cycle_sleep_seconds=args.cycle_sleep_seconds,
        download_sleep_seconds=args.download_sleep_seconds,
        chunksize=args.chunksize,
        max_rest_tail_gap_minutes=args.max_rest_tail_gap_minutes,
        max_rest_tail_buckets=args.max_rest_tail_buckets,
        warning_interval_seconds=args.warning_interval_seconds,
        json_status=args.json_status,
        pid_file=args.pid_file,
        lock_file=args.lock_file,
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args(argv)
    worker = worker_from_args(args)
    lock = worker.acquire_single_instance()
    if lock is None:
        print("range_backfill_worker already running")
        return 0
    with lock:
        if args.mode == "once":
            status = worker.run_once()
            print_summary(status, stream=sys.stdout)
            return 0
        return worker.run_daemon(stop_after_cycles=args.daemon_test_cycles)


if __name__ == "__main__":
    raise SystemExit(main())
