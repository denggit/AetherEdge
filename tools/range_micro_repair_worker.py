from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
import os
from pathlib import Path
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.lock import RangeBackfillLock
from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.market_data.micro_repair import RangeMicroRepairRebuildService
from src.market_data.range_checkpoint import (
    MICRO_REPAIR_FAILED,
    MICRO_REPAIR_QUEUED,
    MICRO_REPAIR_RUNNING,
    MICRO_REPAIR_SKIPPED,
    MICRO_REPAIR_SUCCESS,
    RangeMicroRepairJob,
    SqliteRangeCheckpointStore,
)
from src.market_data.storage import SqliteRangeBarStore
from src.platform.data import create_market_data_feed
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.markets import get_market_profile


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Repair one closed degraded range bucket in a subprocess."
    )
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--range-pct", required=True)
    parser.add_argument("--bucket-start-ms", type=int, required=True)
    parser.add_argument("--bucket-end-ms", type=int, required=True)
    parser.add_argument("--coverage-status", required=True)
    parser.add_argument("--missing-gap-ms", type=int, required=True)
    parser.add_argument("--checkpoint-db", required=True)
    parser.add_argument("--market-db", required=True)
    parser.add_argument("--status-path", required=True)
    parser.add_argument("--lock-path", required=True)
    parser.add_argument("--page-limit", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--max-seconds", type=float, default=30.0)
    parser.add_argument("--missing-bucket-grace-seconds", type=int, default=120)
    parser.add_argument("--wait-poll-seconds", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _lower_process_priority()
    status_store = RangeBackfillStatusStore(args.status_path)
    checkpoint_store = SqliteRangeCheckpointStore(args.checkpoint_db)
    job = _capture_repair_job(checkpoint_store, args=args)
    if job is None:
        _write_status(
            status_store,
            status=MICRO_REPAIR_SKIPPED,
            args=args,
            running=False,
            failure_reason="recoverable_checkpoint_not_found",
        )
        return 0

    _write_status(
        status_store,
        status=MICRO_REPAIR_QUEUED,
        args=args,
        running=True,
        job=job,
    )
    print(
        "range_micro_repair_worker_waiting_for_closed_bucket | "
        f"symbol={job.symbol} exchange={job.exchange} "
        f"bucket_start_ms={job.bucket_start_ms} "
        f"bucket_end_ms={job.bucket_end_ms} "
        f"checkpoint_last_trade_ts_ms={job.checkpoint_last_trade_ts_ms}",
        flush=True,
    )
    if not _wait_until_bucket_can_be_repaired(
        checkpoint_store,
        status_store,
        args=args,
        job=job,
    ):
        return 0

    completed = checkpoint_store.load_completed_aggregate(
        exchange=job.exchange,
        symbol=job.symbol,
        range_pct=job.range_pct,
        bucket_end_ms=job.bucket_end_ms,
    )
    if completed is not None and completed.coverage_status == "COMPLETE":
        _mark_and_write(
            checkpoint_store,
            status_store,
            job=job,
            status=MICRO_REPAIR_SKIPPED,
            args=args,
            failure_reason="bucket_already_complete",
        )
        return 0

    lock = RangeBackfillLock(
        args.lock_path,
        status_path=args.status_path,
        stale_after_seconds=max(60, int(args.max_seconds) + 30),
    )
    if not lock.acquire(mode="micro_repair", force=False):
        _write_status(
            status_store,
            status=MICRO_REPAIR_SKIPPED,
            args=args,
            running=False,
            job=job,
            failure_reason="micro_repair_lock_busy",
        )
        return 0

    checkpoint_store.mark_micro_repair_status(
        exchange=job.exchange,
        symbol=job.symbol,
        range_pct=job.range_pct,
        bucket_start_ms=job.bucket_start_ms,
        status=MICRO_REPAIR_RUNNING,
        updated_at_ms=now_ms(),
    )
    _write_status(
        status_store,
        status=MICRO_REPAIR_RUNNING,
        args=args,
        running=True,
        job=job,
    )
    print(
        "range_micro_repair_started | "
        f"symbol={job.symbol} exchange={job.exchange} "
        f"range_pct={job.range_pct} "
        f"bucket_start_ms={job.bucket_start_ms} "
        f"bucket_end_ms={job.bucket_end_ms} "
        f"checkpoint_last_trade_ts_ms={job.checkpoint_last_trade_ts_ms} "
        f"checkpoint_last_trade_id={job.checkpoint_last_trade_id} "
        f"missing_gap_ms={job.missing_gap_ms} "
        f"coverage_before={job.coverage_status}",
        flush=True,
    )
    try:
        exchange = ExchangeName(str(args.exchange).strip().lower())
        profile = get_market_profile(args.symbol)
        contract_value = profile.contract_value(exchange)
        if contract_value is None:
            raise ValueError(
                f"missing contract value for {exchange.value}:{args.symbol}"
            )
        provider = create_market_data_feed(
            exchange,
            symbol=args.symbol,
            config=ExchangeConfig.from_env(exchange),
            enable_trade_stream=False,
            enable_order_book_stream=False,
        )
        service = RangeMicroRepairRebuildService(
            provider=provider,
            range_bar_store=SqliteRangeBarStore(args.market_db),
            checkpoint_store=checkpoint_store,
            contract_value=contract_value,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_seconds=args.max_seconds,
        )
        result = asyncio.run(service.rebuild(job, completed_at_ms=now_ms()))
        checkpoint_store.mark_micro_repair_status(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_start_ms=job.bucket_start_ms,
            status=MICRO_REPAIR_SUCCESS,
            updated_at_ms=now_ms(),
        )
        _write_status(
            status_store,
            status=MICRO_REPAIR_SUCCESS,
            args=args,
            running=False,
            job=job,
            result=result,
        )
        print(
            "range_micro_repair_succeeded | "
            f"symbol={job.symbol} exchange={job.exchange} "
            f"range_pct={job.range_pct} "
            f"bucket_start_ms={job.bucket_start_ms} "
            f"bucket_end_ms={job.bucket_end_ms} "
            f"checkpoint_last_trade_ts_ms={job.checkpoint_last_trade_ts_ms} "
            f"checkpoint_last_trade_id={job.checkpoint_last_trade_id} "
            f"missing_gap_ms={job.missing_gap_ms} "
            f"repair_start_ms={result.repair_start_ms} "
            f"repair_end_ms={result.repair_end_ms} "
            f"rest_pages={result.rest_pages} "
            f"rest_raw_trades={result.rest_raw_trades} "
            f"rest_deduped_trades={result.rest_deduped_trades} "
            f"range_bars_written={result.range_bars_written} "
            f"coverage_before={job.coverage_status} "
            "coverage_after=COMPLETE failure_reason=None",
            flush=True,
        )
        return 0
    except Exception as exc:
        checkpoint_store.mark_micro_repair_status(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_start_ms=job.bucket_start_ms,
            status=MICRO_REPAIR_FAILED,
            updated_at_ms=now_ms(),
            last_error=f"{type(exc).__name__}:{exc}",
        )
        _write_status(
            status_store,
            status=MICRO_REPAIR_FAILED,
            args=args,
            running=False,
            job=job,
            failure_reason=f"{type(exc).__name__}:{exc}",
        )
        print(
            "range_micro_repair_failed | "
            f"symbol={job.symbol} exchange={job.exchange} "
            f"range_pct={job.range_pct} "
            f"bucket_start_ms={job.bucket_start_ms} "
            f"bucket_end_ms={job.bucket_end_ms} "
            f"checkpoint_last_trade_ts_ms={job.checkpoint_last_trade_ts_ms} "
            f"checkpoint_last_trade_id={job.checkpoint_last_trade_id} "
            f"missing_gap_ms={job.missing_gap_ms} "
            f"repair_start_ms={int(job.checkpoint_last_trade_ts_ms or job.bucket_start_ms - 1) + 1} "
            f"repair_end_ms={job.bucket_end_ms} "
            "coverage_after="
            f"{job.coverage_status} "
            f"failure_reason={type(exc).__name__}:{exc}",
            flush=True,
        )
        return 1
    finally:
        lock.release()


def _lower_process_priority() -> None:
    if sys.platform == "win32":
        return
    nice = getattr(os, "nice", None)
    if callable(nice):
        try:
            nice(10)
        except OSError:
            pass


def _capture_repair_job(
    store: SqliteRangeCheckpointStore,
    *,
    args,
) -> RangeMicroRepairJob | None:
    existing = store.load_micro_repair_job(
        exchange=args.exchange,
        symbol=args.symbol,
        range_pct=args.range_pct,
        bucket_start_ms=args.bucket_start_ms,
    )
    checkpoint = store.load_checkpoint(
        exchange=args.exchange,
        symbol=args.symbol,
        range_pct=args.range_pct,
        bucket_start_ms=args.bucket_start_ms,
    )
    captured = None
    if checkpoint is not None and checkpoint.last_trade_ts_ms is not None:
        captured = RangeMicroRepairJob(
            exchange=args.exchange,
            symbol=args.symbol,
            range_pct=args.range_pct,
            bucket_start_ms=int(args.bucket_start_ms),
            bucket_end_ms=int(args.bucket_end_ms),
            checkpoint_last_trade_id=checkpoint.last_trade_id,
            checkpoint_last_trade_ts_ms=checkpoint.last_trade_ts_ms,
            builder_state=dict(checkpoint.builder_state),
            coverage_status=str(args.coverage_status),
            missing_gap_ms=max(0, int(args.missing_gap_ms)),
            status=MICRO_REPAIR_QUEUED,
            created_at_ms=now_ms(),
            updated_at_ms=now_ms(),
        )
    job = _earliest_checkpoint_job(existing, captured)
    if job is None:
        return None
    queued = replace(
        job,
        status=MICRO_REPAIR_QUEUED,
        updated_at_ms=now_ms(),
        last_error=None,
    )
    store.enqueue_micro_repair(queued)
    return queued


def _earliest_checkpoint_job(
    existing: RangeMicroRepairJob | None,
    captured: RangeMicroRepairJob | None,
) -> RangeMicroRepairJob | None:
    if existing is None:
        return captured
    if captured is None:
        return existing
    existing_ts = existing.checkpoint_last_trade_ts_ms
    captured_ts = captured.checkpoint_last_trade_ts_ms
    if existing_ts is None:
        return captured
    if captured_ts is None:
        return existing
    return existing if existing_ts <= captured_ts else captured


def _wait_until_bucket_can_be_repaired(
    checkpoint_store: SqliteRangeCheckpointStore,
    status_store: RangeBackfillStatusStore,
    *,
    args,
    job: RangeMicroRepairJob,
) -> bool:
    grace_ms = max(0, int(args.missing_bucket_grace_seconds)) * 1000
    poll_seconds = max(0.1, float(args.wait_poll_seconds))
    while True:
        completed = checkpoint_store.load_completed_aggregate(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_end_ms=job.bucket_end_ms,
        )
        if completed is not None and completed.coverage_status == "COMPLETE":
            _mark_and_write(
                checkpoint_store,
                status_store,
                job=job,
                status=MICRO_REPAIR_SKIPPED,
                args=args,
                failure_reason="bucket_already_complete",
            )
            return False
        current_ms = now_ms()
        if current_ms > job.bucket_end_ms and (
            completed is not None
            or current_ms >= job.bucket_end_ms + 1 + grace_ms
        ):
            return True
        _write_status(
            status_store,
            status=MICRO_REPAIR_QUEUED,
            args=args,
            running=True,
            job=job,
        )
        time.sleep(poll_seconds)


def _mark_and_write(
    checkpoint_store: SqliteRangeCheckpointStore,
    status_store: RangeBackfillStatusStore,
    *,
    job,
    status: str,
    args,
    failure_reason: str,
) -> None:
    checkpoint_store.mark_micro_repair_status(
        exchange=job.exchange,
        symbol=job.symbol,
        range_pct=job.range_pct,
        bucket_start_ms=job.bucket_start_ms,
        status=status,
        updated_at_ms=now_ms(),
        last_error=failure_reason,
    )
    _write_status(
        status_store,
        status=status,
        args=args,
        running=False,
        job=job,
        failure_reason=failure_reason,
    )


def _write_status(
    store: RangeBackfillStatusStore,
    *,
    status: str,
    args,
    running: bool,
    job=None,
    result=None,
    failure_reason: str | None = None,
) -> None:
    timestamp = now_ms()
    payload = {
        "pid": os.getpid(),
        "running": bool(running),
        "repair_status": status,
        "phase": status,
        "exchange": args.exchange,
        "symbol": args.symbol,
        "range_pct": str(args.range_pct),
        "bucket_start_ms": int(args.bucket_start_ms),
        "worker_heartbeat_ms": timestamp,
        "heartbeat_ms": timestamp,
        "failure_reason": failure_reason,
        "finished_at_ms": None if running else timestamp,
    }
    if job is not None:
        repair_start_ms = (
            int(job.checkpoint_last_trade_ts_ms) + 1
            if job.checkpoint_last_trade_ts_ms is not None
            else int(job.bucket_start_ms)
        )
        payload.update(
            bucket_end_ms=job.bucket_end_ms,
            checkpoint_last_trade_ts_ms=job.checkpoint_last_trade_ts_ms,
            checkpoint_last_trade_id=job.checkpoint_last_trade_id,
            coverage_before=job.coverage_status,
            coverage_after=job.coverage_status,
            missing_gap_ms=job.missing_gap_ms,
            repair_start_ms=repair_start_ms,
            repair_end_ms=job.bucket_end_ms,
            rest_pages=0,
            rest_raw_trades=0,
            rest_deduped_trades=0,
            range_bars_written=0,
            aggregates_written=0,
        )
    if result is not None:
        payload.update(
            repair_start_ms=result.repair_start_ms,
            repair_end_ms=result.repair_end_ms,
            rest_pages=result.rest_pages,
            rest_raw_trades=result.rest_raw_trades,
            rest_deduped_trades=result.rest_deduped_trades,
            range_bars_written=result.range_bars_written,
            aggregates_written=int(result.aggregate_written),
            coverage_after="COMPLETE",
        )
    store.write(payload)


if __name__ == "__main__":
    raise SystemExit(main())
