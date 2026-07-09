from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from decimal import Decimal
import os
from pathlib import Path
import sys
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.market_data.backfill.lock import RangeBackfillLock
from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.market_data.micro_repair import (
    RangeMicroRepairError,
    RangeMicroRepairRebuildService,
    RangeMicroRepairStagingService,
)
from src.market_data.models import RangeCoverageStatus
from src.market_data.range_repair import (
    RangeRepairJournalState,
    SqliteRangeRepairJournalStore,
    journal_status_is_invalid,
)
from src.market_data.range_checkpoint import (
    MICRO_REPAIR_FAILED,
    MICRO_REPAIR_PARTIAL,
    MICRO_REPAIR_PENDING,
    MICRO_REPAIR_QUEUED,
    MICRO_REPAIR_RUNNING,
    MICRO_REPAIR_SKIPPED,
    MICRO_REPAIR_SUCCESS,
    RangeMicroRepairJob,
    RangeMicroRepairStagingState,
    RangeMicroRepairStagingTrade,
    SqliteRangeCheckpointStore,
)
from src.market_data.storage import SqliteRangeBarStore
from src.platform.data import create_market_data_feed
from src.platform.data.models import (
    MarketDataSource,
    MarketTrade,
    TradeSide,
)
from src.platform.exchanges.models import ExchangeConfig, ExchangeName
from src.platform.markets import get_market_profile


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Repair the startup-recovery current range bucket in a subprocess."
        )
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
    parser.add_argument("--journal-db", required=True)
    parser.add_argument("--status-path", required=True)
    parser.add_argument("--lock-path", required=True)
    parser.add_argument("--page-limit", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--max-seconds", type=float, default=30.0)
    parser.add_argument("--max-gap-ms", type=int, default=600_000)
    parser.add_argument("--missing-bucket-grace-seconds", type=int, default=120)
    parser.add_argument("--wait-poll-seconds", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _lower_process_priority()
    status_store = RangeBackfillStatusStore(args.status_path)
    checkpoint_store = SqliteRangeCheckpointStore(args.checkpoint_db)
    journal_store = SqliteRangeRepairJournalStore(args.journal_db)
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
    try:
        ready = _wait_until_bucket_can_be_repaired(
            checkpoint_store,
            journal_store,
            status_store,
            args=args,
            job=job,
        )
    except Exception as exc:
        _mark_and_write(
            checkpoint_store,
            status_store,
            job=job,
            status=MICRO_REPAIR_FAILED,
            args=args,
            failure_reason=f"{type(exc).__name__}:{exc}",
            journal_state=journal_store.load_state(
                exchange=job.exchange,
                symbol=job.symbol,
                range_pct=job.range_pct,
                bucket_start_ms=job.bucket_start_ms,
            ),
        )
        print(
            "range_micro_repair_failed | "
            f"symbol={job.symbol} exchange={job.exchange} "
            f"bucket_start_ms={job.bucket_start_ms} "
            f"failure_reason={type(exc).__name__}:{exc}",
            flush=True,
        )
        return 1
    if ready is None:
        return 0
    job, journal_state = ready

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
            journal_state=journal_state,
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
            journal_state=journal_state,
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
        journal_state=journal_state,
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
        rebuild_service = RangeMicroRepairRebuildService(
            provider=provider,
            range_bar_store=SqliteRangeBarStore(args.market_db),
            checkpoint_store=checkpoint_store,
            contract_value=contract_value,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_seconds=args.max_seconds,
        )
        staging_service = RangeMicroRepairStagingService(
            provider=provider,
            checkpoint_store=checkpoint_store,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_seconds=args.max_seconds,
        )
        journal_records = journal_store.load_trades(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_start_ms=job.bucket_start_ms,
            start_time_ms=int(job.journal_start_ms or 0),
            end_time_ms=int(job.journal_end_ms or job.bucket_end_ms),
        )
        journal_trades = tuple(
            _market_trade_from_journal(row) for row in journal_records
        )

        # Check for existing staging (resume) or start fresh
        existing_staging = checkpoint_store.load_staging(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_start_ms=job.bucket_start_ms,
        )

        result = asyncio.run(
            _run_chunked_repair_async(
                staging_service=staging_service,
                rebuild_service=rebuild_service,
                checkpoint_store=checkpoint_store,
                job=job,
                journal_trades=journal_trades,
                staging=existing_staging,
            )
        )

        if result is None:
            # Partial coverage – save cursor and exit cleanly for next round
            checkpoint_store.mark_micro_repair_status(
                exchange=job.exchange,
                symbol=job.symbol,
                range_pct=job.range_pct,
                bucket_start_ms=job.bucket_start_ms,
                status=MICRO_REPAIR_PARTIAL,
                updated_at_ms=now_ms(),
            )
            updated_staging = checkpoint_store.load_staging(
                exchange=job.exchange,
                symbol=job.symbol,
                range_pct=job.range_pct,
                bucket_start_ms=job.bucket_start_ms,
            )
            _write_status(
                status_store,
                status=MICRO_REPAIR_PARTIAL,
                args=args,
                running=False,
                job=job,
                journal_state=journal_state,
                staging_state=updated_staging,
            )
            print(
                "range_micro_repair_partial | "
                f"symbol={job.symbol} exchange={job.exchange} "
                f"range_pct={job.range_pct} "
                f"bucket_start_ms={job.bucket_start_ms} "
                f"bucket_end_ms={job.bucket_end_ms} "
                f"checkpoint_last_trade_id={job.checkpoint_last_trade_id} "
                f"cursor_trade_id="
                f"{updated_staging.current_oldest_fetched_trade_id if updated_staging else 'unknown'} "
                f"coverage_before={job.coverage_status} "
                "coverage_after=PARTIAL failure_reason=None",
                flush=True,
            )
            return 0

        # Coverage complete – verify journal stability then persist
        post_repair_journal_state = journal_store.load_state(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_start_ms=job.bucket_start_ms,
        )
        if (
            post_repair_journal_state is None
            or not post_repair_journal_state.valid_for_repair
            or post_repair_journal_state.journal_trade_count
            != journal_state.journal_trade_count
            or post_repair_journal_state.updated_at_ms
            != journal_state.updated_at_ms
        ):
            checkpoint_store.invalidate_completed_aggregate(
                exchange=job.exchange,
                symbol=job.symbol,
                range_pct=job.range_pct,
                bucket_end_ms=job.bucket_end_ms,
                coverage_status=(
                    RangeCoverageStatus.RECOVERED_INCOMPLETE.value
                ),
                missing_gap_ms=1,
                completed_at_ms=now_ms(),
            )
            raise RangeMicroRepairError(
                "repair journal changed while micro repair was running"
            )
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
            journal_state=journal_state,
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
            f"repair_gap_start_ms={result.repair_gap_start_ms} "
            f"repair_gap_end_ms={result.repair_gap_end_ms} "
            f"repair_gap_ms={result.repair_gap_ms} "
            f"fetch_mode={result.fetch_mode} "
            f"fallback_reason={result.fallback_reason} "
            f"rest_pages={result.rest_pages} "
            f"rest_raw_trades={result.rest_raw_trades} "
            f"rest_deduped_trades={result.rest_deduped_trades} "
            f"replayed_rest_trades={result.replayed_rest_trades} "
            f"replayed_journal_trades={result.replayed_journal_trades} "
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
            journal_state=journal_state,
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
            f"repair_gap_start_ms={job.repair_gap_start_ms} "
            f"repair_gap_end_ms={job.repair_gap_end_ms} "
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


def _journal_job_fields(
    job: RangeMicroRepairJob,
    state: RangeRepairJournalState,
    *,
    repair_gap_start_ms: int,
    repair_gap_end_ms: int,
    updated_at_ms: int,
) -> dict[str, object]:
    return {
        "first_live_trade_ts_ms": state.first_live_trade_ts_ms,
        "first_live_trade_id": state.first_live_trade_id,
        "repair_gap_start_ms": repair_gap_start_ms,
        "repair_gap_end_ms": repair_gap_end_ms,
        "journal_start_ms": state.first_live_trade_ts_ms,
        "journal_end_ms": job.bucket_end_ms,
        "journal_status": state.status,
        "updated_at_ms": updated_at_ms,
    }


def _wait_until_bucket_can_be_repaired(
    checkpoint_store: SqliteRangeCheckpointStore,
    journal_store: SqliteRangeRepairJournalStore,
    status_store: RangeBackfillStatusStore,
    *,
    args,
    job: RangeMicroRepairJob,
) -> tuple[RangeMicroRepairJob, RangeRepairJournalState] | None:
    grace_ms = max(0, int(args.missing_bucket_grace_seconds)) * 1000
    poll_seconds = max(0.1, float(args.wait_poll_seconds))
    waiting_first_logged = False
    waiting_close_logged = False
    waiting_finalized_logged = False
    finalized_signature = None
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
                journal_state=journal_store.load_state(
                    exchange=job.exchange,
                    symbol=job.symbol,
                    range_pct=job.range_pct,
                    bucket_start_ms=job.bucket_start_ms,
                ),
            )
            return None
        state = journal_store.load_state(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_start_ms=job.bucket_start_ms,
        )
        if state is not None:
            checkpoint_store.update_micro_repair_journal_status(
                exchange=job.exchange,
                symbol=job.symbol,
                range_pct=job.range_pct,
                bucket_start_ms=job.bucket_start_ms,
                journal_status=state.status,
                updated_at_ms=now_ms(),
            )
        if state is not None and journal_status_is_invalid(state.status):
            raise RangeMicroRepairError(
                f"repair journal invalid: status={state.status} "
                f"error={state.last_error}"
            )
        if state is None or state.first_live_trade_ts_ms is None:
            if now_ms() >= job.bucket_end_ms + 1 + grace_ms:
                raise RangeMicroRepairError(
                    "first live trade was not recorded before grace deadline"
                )
            if not waiting_first_logged:
                print(
                    "range_micro_repair_waiting_for_first_live_trade | "
                    f"symbol={job.symbol} exchange={job.exchange} "
                    f"bucket_start_ms={job.bucket_start_ms}",
                    flush=True,
                )
                waiting_first_logged = True
            _write_status(
                status_store,
                status=MICRO_REPAIR_QUEUED,
                args=args,
                running=True,
                job=job,
                journal_state=state,
                waiting_reason="waiting_for_first_live_trade",
            )
            time.sleep(poll_seconds)
            continue
        repair_gap_start_ms = int(job.checkpoint_last_trade_ts_ms or 0) + 1
        repair_gap_end_ms = int(state.first_live_trade_ts_ms) - 1
        repair_gap_ms = max(
            0, repair_gap_end_ms - repair_gap_start_ms + 1
        )
        if repair_gap_ms > int(args.max_gap_ms):
            raise RangeMicroRepairError(
                "real REST repair gap exceeds configured maximum: "
                f"repair_gap_ms={repair_gap_ms} max_gap_ms={args.max_gap_ms}"
            )
        updated_at_ms = now_ms()
        journal_job_fields = _journal_job_fields(
            job,
            state,
            repair_gap_start_ms=repair_gap_start_ms,
            repair_gap_end_ms=repair_gap_end_ms,
            updated_at_ms=updated_at_ms,
        )
        checkpoint_store.update_micro_repair_journal(
            exchange=job.exchange,
            symbol=job.symbol,
            range_pct=job.range_pct,
            bucket_start_ms=job.bucket_start_ms,
            **journal_job_fields,
        )
        job = replace(
            job,
            **journal_job_fields,
        )
        current_ms = now_ms()
        if current_ms <= job.bucket_end_ms:
            if not waiting_close_logged:
                print(
                    "range_micro_repair_waiting_for_bucket_close | "
                    f"symbol={job.symbol} exchange={job.exchange} "
                    f"bucket_start_ms={job.bucket_start_ms} "
                    f"bucket_end_ms={job.bucket_end_ms}",
                    flush=True,
                )
                waiting_close_logged = True
            waiting_reason = "waiting_for_bucket_close"
        elif not state.finalized:
            if current_ms >= job.bucket_end_ms + 1 + grace_ms:
                raise RangeMicroRepairError(
                    "repair journal was not finalized before grace deadline"
                )
            if not waiting_finalized_logged:
                print(
                    "range_micro_repair_waiting_for_journal_finalized | "
                    f"symbol={job.symbol} exchange={job.exchange} "
                    f"bucket_start_ms={job.bucket_start_ms}",
                    flush=True,
                )
                waiting_finalized_logged = True
            waiting_reason = "waiting_for_journal_finalized"
        else:
            if not state.valid_for_repair:
                raise RangeMicroRepairError(
                    f"repair journal is not valid: status={state.status} "
                    f"dropped_trades={state.dropped_trades} "
                    f"writer_failures={state.writer_failures}"
                )
            signature = (
                state.updated_at_ms,
                state.journal_trade_count,
                state.dropped_trades,
                state.writer_failures,
                state.status,
            )
            if finalized_signature != signature:
                finalized_signature = signature
                _write_status(
                    status_store,
                    status=MICRO_REPAIR_QUEUED,
                    args=args,
                    running=True,
                    job=job,
                    journal_state=state,
                    waiting_reason="waiting_for_journal_stability",
                )
                time.sleep(poll_seconds)
                continue
            return job, state
        _write_status(
            status_store,
            status=MICRO_REPAIR_QUEUED,
            args=args,
            running=True,
            job=job,
            journal_state=state,
            waiting_reason=waiting_reason,
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
    journal_state: RangeRepairJournalState | None = None,
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
        journal_state=journal_state,
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
    journal_state: RangeRepairJournalState | None = None,
    waiting_reason: str | None = None,
    failure_reason: str | None = None,
    staging_state: RangeMicroRepairStagingState | None = None,
) -> None:
    timestamp = now_ms()
    payload = _base_status_payload(
        status=status,
        args=args,
        running=running,
        timestamp=timestamp,
        waiting_reason=waiting_reason,
        failure_reason=failure_reason,
    )
    if job is not None:
        payload.update(_status_fields_from_job(job))
    if journal_state is not None:
        payload.update(_status_fields_from_journal_state(journal_state))
    if result is not None:
        payload.update(_status_fields_from_result(result))
    if staging_state is not None:
        payload.update(_status_fields_from_staging(staging_state))
    store.write(payload)


def _base_status_payload(
    *,
    status: str,
    args,
    running: bool,
    timestamp: int,
    waiting_reason: str | None,
    failure_reason: str | None,
) -> dict[str, object]:
    return {
        "pid": os.getpid(),
        "running": bool(running),
        "repair_status": status,
        "phase": status,
        "repair_scope": "startup_recovery_current_bucket",
        "exchange": args.exchange,
        "symbol": args.symbol,
        "range_pct": str(args.range_pct),
        "bucket_start_ms": int(args.bucket_start_ms),
        "worker_heartbeat_ms": timestamp,
        "heartbeat_ms": timestamp,
        "failure_reason": failure_reason,
        "waiting_reason": waiting_reason,
        "finished_at_ms": None if running else timestamp,
        "checkpoint_last_trade_ts_ms": None,
        "checkpoint_last_trade_id": None,
        "first_live_trade_ts_ms": None,
        "first_live_trade_id": None,
        "repair_gap_start_ms": None,
        "repair_gap_end_ms": None,
        "repair_gap_ms": None,
        "journal_start_ms": None,
        "journal_end_ms": None,
        "journal_trade_count": 0,
        "journal_status": None,
        "journal_dropped_trades": 0,
        "journal_writer_failures": 0,
        "rest_pages": 0,
        "rest_raw_trades": 0,
        "rest_deduped_trades": 0,
        "fetch_mode": None,
        "fallback_reason": None,
        "replayed_rest_trades": 0,
        "replayed_journal_trades": 0,
        "range_bars_written": 0,
        "aggregate_written": False,
        "coverage_before": None,
        "coverage_after": None,
    }


def _status_fields_from_job(
    job: RangeMicroRepairJob,
) -> dict[str, object]:
    return {
        "bucket_end_ms": job.bucket_end_ms,
        "checkpoint_last_trade_ts_ms": job.checkpoint_last_trade_ts_ms,
        "checkpoint_last_trade_id": job.checkpoint_last_trade_id,
        "coverage_before": job.coverage_status,
        "coverage_after": job.coverage_status,
        "missing_gap_ms": job.missing_gap_ms,
        "first_live_trade_ts_ms": job.first_live_trade_ts_ms,
        "first_live_trade_id": job.first_live_trade_id,
        "repair_gap_start_ms": job.repair_gap_start_ms,
        "repair_gap_end_ms": job.repair_gap_end_ms,
        "repair_gap_ms": (
            None
            if job.repair_gap_start_ms is None
            or job.repair_gap_end_ms is None
            else max(
                0,
                job.repair_gap_end_ms - job.repair_gap_start_ms + 1,
            )
        ),
        "journal_start_ms": job.journal_start_ms,
        "journal_end_ms": job.journal_end_ms,
        "journal_status": job.journal_status,
        "rest_pages": 0,
        "rest_raw_trades": 0,
        "rest_deduped_trades": 0,
        "range_bars_written": 0,
        "aggregates_written": 0,
    }


def _status_fields_from_journal_state(
    journal_state: RangeRepairJournalState,
) -> dict[str, object]:
    return {
        "first_live_trade_ts_ms": journal_state.first_live_trade_ts_ms,
        "first_live_trade_id": journal_state.first_live_trade_id,
        "journal_trade_count": journal_state.journal_trade_count,
        "journal_status": journal_state.status,
        "journal_dropped_trades": journal_state.dropped_trades,
        "journal_writer_failures": journal_state.writer_failures,
        "journal_finalized": journal_state.finalized,
    }


def _status_fields_from_result(result) -> dict[str, object]:
    return {
        "repair_start_ms": result.repair_start_ms,
        "repair_end_ms": result.repair_end_ms,
        "repair_gap_start_ms": result.repair_gap_start_ms,
        "repair_gap_end_ms": result.repair_gap_end_ms,
        "repair_gap_ms": result.repair_gap_ms,
        "journal_start_ms": result.journal_start_ms,
        "journal_end_ms": result.journal_end_ms,
        "journal_trade_count": result.journal_trade_count,
        "rest_pages": result.rest_pages,
        "rest_raw_trades": result.rest_raw_trades,
        "rest_deduped_trades": result.rest_deduped_trades,
        "fetch_mode": result.fetch_mode,
        "fallback_reason": result.fallback_reason,
        "replayed_rest_trades": result.replayed_rest_trades,
        "replayed_journal_trades": result.replayed_journal_trades,
        "range_bars_written": result.range_bars_written,
        "aggregate_written": bool(result.aggregate_written),
        "aggregates_written": int(result.aggregate_written),
        "coverage_after": "COMPLETE",
    }


def _status_fields_from_staging(
    staging: RangeMicroRepairStagingState,
) -> dict[str, object]:
    return {
        "staging_status": staging.status,
        "staging_fetched_trade_count": staging.fetched_trade_count,
        "staging_rest_pages_total": staging.rest_pages_total,
        "staging_cursor_trade_id": staging.current_oldest_fetched_trade_id,
        "staging_cursor_ts_ms": staging.current_oldest_fetched_ts_ms,
        "staging_coverage_complete": staging.coverage_complete,
    }


def _market_trade_from_journal(row) -> MarketTrade:
    try:
        side = TradeSide(str(row.side))
    except ValueError:
        side = TradeSide.UNKNOWN
    try:
        source = MarketDataSource(str(row.source))
    except ValueError:
        source = MarketDataSource.WEBSOCKET
    return MarketTrade(
        exchange=ExchangeName(str(row.exchange).lower()),
        symbol=row.symbol,
        raw_symbol=row.raw_symbol,
        price=Decimal(str(row.price)),
        quantity=Decimal(str(row.quantity)),
        side=side,
        trade_id=row.trade_id,
        event_time_ms=row.event_time_ms,
        trade_time_ms=row.trade_time_ms,
        source=source,
    )


async def _run_chunked_repair_async(
    *,
    staging_service: RangeMicroRepairStagingService,
    rebuild_service: RangeMicroRepairRebuildService,
    checkpoint_store: SqliteRangeCheckpointStore,
    job: RangeMicroRepairJob,
    journal_trades: tuple[MarketTrade, ...],
    staging: RangeMicroRepairStagingState | None,
):
    """Fetch one chunk via staging; if coverage complete, replay and return result.

    Returns ``None`` when coverage is not yet complete (partial progress saved to
    staging).  Returns the rebuild result when the gap is fully covered.
    """

    chunk_trades, _new_staging, coverage_complete = (
        await staging_service.fetch_chunk(job, staging=staging)
    )

    if not coverage_complete:
        return None

    # Load all accumulated staging trades (may be empty when the gap is naturally
    # devoid of trades — the provider already proved coverage is complete).
    staging_rows = checkpoint_store.load_staging_trades(
        exchange=job.exchange,
        symbol=job.symbol,
        range_pct=job.range_pct,
        bucket_start_ms=job.bucket_start_ms,
    )
    all_staging_trades = tuple(
        _market_trade_from_staging(row) for row in staging_rows
    )

    # Strict coverage verification before writing COMPLETE.
    # Empty staging is valid when the provider proved coverage (e.g. no trades
    # occurred between checkpoint and first live trade).
    if all_staging_trades:
        _assert_full_staging_coverage(all_staging_trades, job)

    result = await rebuild_service.rebuild(
        job,
        journal_trades=journal_trades,
        completed_at_ms=now_ms(),
        rest_gap_trades=all_staging_trades,
    )

    # Clear staging now that repair is complete
    checkpoint_store.clear_staging(
        exchange=job.exchange,
        symbol=job.symbol,
        range_pct=job.range_pct,
        bucket_start_ms=job.bucket_start_ms,
    )

    return result


def _assert_full_staging_coverage(
    trades: tuple[MarketTrade, ...],
    job: RangeMicroRepairJob,
) -> None:
    """Verify staging trades cover the entire gap from checkpoint to first_live.

    Coverage completeness is already proven by the provider (pagination was not
    exhausted or cursor reached the checkpoint).  This function performs a
    defence-in-depth sanity check:

    * Every staging trade must fall within the repair gap time bounds.
    * The oldest trade ID must be reasonably close to the checkpoint.
    * Every staging trade ID must be strictly less than the first live trade ID.
    """

    repair_gap_start = int(job.checkpoint_last_trade_ts_ms or 0) + 1
    repair_gap_end = int(job.first_live_trade_ts_ms or 0) - 1

    for trade in trades:
        ts = int(trade.trade_time_ms or 0)
        if not (repair_gap_start <= ts <= repair_gap_end):
            raise RangeMicroRepairError(
                f"staging trade {trade.trade_id} at ts={ts} outside "
                f"gap [{repair_gap_start}, {repair_gap_end}]"
            )

    # If trade IDs are numeric, verify:
    # 1. The oldest fetched ID does not leave an unreasonable gap vs the
    #    checkpoint.  A gap of 1 ID is expected (the checkpoint trade itself
    #    is not part of the repair gap).  Larger gaps are acceptable when the
    #    provider proved coverage (no trades exist in that span).
    # 2. The newest fetched ID is strictly before the first live trade.
    if job.checkpoint_last_trade_id is not None:
        try:
            checkpoint_id = int(job.checkpoint_last_trade_id)
            numeric_ids = [
                int(str(t.trade_id or "0"))
                for t in trades
                if str(t.trade_id or "").isdigit()
            ]
            if numeric_ids:
                min_id = min(numeric_ids)
                if min_id - checkpoint_id > 100:
                    raise RangeMicroRepairError(
                        f"staging coverage gap too large: min_trade_id={min_id} "
                        f"checkpoint_last_trade_id={checkpoint_id} "
                        f"gap={min_id - checkpoint_id}"
                    )
        except (ValueError, TypeError):
            pass

    if job.first_live_trade_id is not None:
        try:
            first_live_id = int(job.first_live_trade_id)
            for trade in trades:
                tid = str(trade.trade_id or "")
                if tid.isdigit() and int(tid) >= first_live_id:
                    raise RangeMicroRepairError(
                        f"staging trade {tid} >= first_live_trade_id "
                        f"{first_live_id}"
                    )
        except (ValueError, TypeError):
            pass


def _market_trade_from_staging(row: RangeMicroRepairStagingTrade) -> MarketTrade:
    try:
        side = TradeSide(str(row.side))
    except ValueError:
        side = TradeSide.UNKNOWN
    try:
        source = MarketDataSource(str(row.source))
    except ValueError:
        source = MarketDataSource.REST
    return MarketTrade(
        exchange=ExchangeName(str(row.exchange).lower()),
        symbol=row.symbol,
        raw_symbol=row.raw_symbol or row.symbol,
        price=Decimal(str(row.price)),
        quantity=Decimal(str(row.quantity)),
        side=side,
        trade_id=row.trade_id,
        event_time_ms=row.event_time_ms,
        trade_time_ms=row.trade_time_ms,
        source=source,
    )


if __name__ == "__main__":
    raise SystemExit(main())
