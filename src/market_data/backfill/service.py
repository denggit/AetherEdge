from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import time
from typing import Callable, Mapping

from src.market_data.backfill.coverage import iter_utc_dates, previous_utc_day_start_ms
from src.market_data.backfill.lock import RangeBackfillLock
from src.market_data.backfill.models import BucketGap, RangeBackfillRequest, RangeBackfillSummary
from src.market_data.backfill.scanner import RangeBackfillScanner
from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.historical_trades.importer import (
    filter_okx_trade_chunk_by_time,
    iter_trade_csv_chunks,
    normalize_okx_trade_chunk,
)
from src.market_data.historical_trades.okx_archive import (
    OkxHistoricalTradeArchive,
    OkxHistoricalTradeDownloadError,
    iter_okx_archive_dates_for_utc_range,
    okx_archive_date_from_utc_ms,
    okx_daily_trade_url,
    okx_raw_symbol_from_canonical,
)
from src.market_data.models import RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import (
    MIN_VALID_COMPLETED_AGGREGATE_MS,
    SqliteRangeCheckpointStore,
)
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.market_data.warmup.gap_detector import interval_to_ms

ProgressCallback = Callable[[str, Mapping[str, object]], None]


@dataclass(frozen=True)
class _BuildWindowResult:
    downloaded_files: int = 0
    raw_rows: int = 0
    filtered_rows: int = 0
    dropped_rows: int = 0
    trades_loaded: int = 0
    range_bars_written: int = 0
    aggregates_written: int = 0
    resource_limited: bool = False
    missing_raw_days: tuple[str, ...] = ()
    failed_downloads: tuple[str, ...] = ()
    skipped_buckets_due_missing_raw: int = 0
    target_bucket_start_ms: int | None = None
    target_bucket_end_ms: int | None = None
    selected_archive_dates: tuple[str, ...] = ()
    per_file_min_trade_time_ms: tuple[tuple[str, int | None], ...] = ()
    per_file_max_trade_time_ms: tuple[tuple[str, int | None], ...] = ()
    target_trade_count: int = 0
    candidate_range_bars: int = 0
    candidate_aggregates: int = 0
    filtered_reason_if_zero: str | None = None
    repair_method: str = ""
    target_window_reached: bool = False
    target_bucket_proven_complete: bool = False
    anchor_last_trade_ts_ms: int | None = None
    replay_start_ms: int | None = None
    replay_end_ms: int | None = None
    pre_replay_existing_range_bars: int = 0
    generated_range_bars: int = 0
    combined_range_bars: int = 0


class RangeBackfillService:
    def __init__(
        self,
        request: RangeBackfillRequest,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        self.request = request
        self.progress_callback = progress_callback
        self.checkpoint_store = SqliteRangeCheckpointStore(request.checkpoint_db_path)
        self.range_bar_store = SqliteRangeBarStore(request.market_db_path)
        self.trade_store = (
            SqliteTradeStore(
                request.market_db_path,
                save_raw_trades=True,
            )
            if request.save_raw_trades
            else None
        )
        self.status_store = RangeBackfillStatusStore(request.status_path)
        self.archive = OkxHistoricalTradeArchive(request.raw_root)
        self._now_ms_value: int | None = None
        self._raw_day_failures: dict[tuple[str, str], str] = {}
        self._archive_not_ready_days: set[str] = set()

    def _emit(self, event: str, **payload: object) -> None:
        if self.progress_callback is None:
            return
        self.progress_callback(event, payload)

    def check_coverage(self, *, now_ms_value: int | None = None, direction: str | None = None):
        scanner = RangeBackfillScanner(self.checkpoint_store)
        return scanner.scan(
            exchange=self.request.exchange,
            symbol=self.request.symbol,
            range_pct=self.request.range_pct,
            bucket_interval=self.request.bucket_interval,
            required_buckets=self.request.required_buckets,
            lookback_buckets=self.request.lookback_buckets,
            now_ms=now_ms_value,
            max_target_end_ms=self.request.max_target_end_ms,
            direction=direction or self.request.direction,
        )

    def run_once(
        self,
        *,
        now_ms_value: int | None = None,
        acquire_lock: bool = True,
        mark_process_finished_on_summary: bool = True,
    ) -> RangeBackfillSummary:
        self._now_ms_value = now_ms_value
        self._raw_day_failures = {}
        self._archive_not_ready_days = set()
        started = time.monotonic()
        coverage_before = self.check_coverage(now_ms_value=now_ms_value)
        self._emit(
            "coverage_checked",
            complete=coverage_before.required_window_complete_count,
            missing=coverage_before.missing_periods,
            missing_bucket=sum(
                gap.reason == "missing_bucket"
                for gap in coverage_before.required_window_missing_buckets
            ),
            degraded_bucket=len(coverage_before.required_window_degraded_buckets),
            available=coverage_before.available,
            current_closed_bucket_end_ms=coverage_before.current_closed_bucket_end_ms,
        )
        if self._coverage_satisfied_for_mode(coverage_before) or self.request.dry_run:
            return self._finish_summary(
                started=started,
                before=coverage_before,
                downloaded_files=0,
                trades_loaded=0,
                range_bars_written=0,
                aggregates_written=0,
                status="ok" if coverage_before.available else "dry_run",
                update_status=False,
            )

        lock: RangeBackfillLock | None = None
        if acquire_lock:
            lock = RangeBackfillLock(
                self.request.lock_path,
                status_path=self.request.status_path,
            )
            if not lock.acquire(mode=self.request.mode, force=self.request.force):
                return RangeBackfillSummary(
                    symbol=self.request.symbol,
                    exchange=self.request.exchange,
                    range_pct=self.request.range_pct,
                    bucket_interval=self.request.bucket_interval,
                    target_buckets=self.request.required_buckets,
                    complete_before=coverage_before.required_window_complete_count,
                    complete_after=coverage_before.required_window_complete_count,
                    missing_before=coverage_before.missing_periods,
                    missing_after=coverage_before.missing_periods,
                    elapsed_seconds=time.monotonic() - started,
                    status="lock_busy",
                    last_error=f"range backfill lock is held: {self.request.lock_path}",
                )
        try:
            self._mark_cycle_started(
                coverage_before=coverage_before,
                reset_status=acquire_lock,
            )
            return self._run_locked(
                started=started,
                coverage_before=coverage_before,
                mark_process_finished_on_summary=mark_process_finished_on_summary,
            )
        except Exception as exc:
            heartbeat = now_ms()
            self.status_store.patch(
                running=not mark_process_finished_on_summary,
                phase="failed",
                worker_heartbeat_ms=heartbeat,
                heartbeat_ms=heartbeat,
                last_error=str(exc),
                finished_at_ms=now_ms() if mark_process_finished_on_summary else None,
            )
            return self._finish_summary(
                started=started,
                before=coverage_before,
                downloaded_files=0,
                trades_loaded=0,
                range_bars_written=0,
                aggregates_written=0,
                status="error",
                last_error=str(exc),
                update_status=True,
                mark_process_finished=mark_process_finished_on_summary,
            )
        finally:
            if lock is not None:
                lock.release()

    def _mark_cycle_started(self, *, coverage_before, reset_status: bool) -> None:
        heartbeat = now_ms()
        payload = {
            "mode": self.request.mode,
            "direction": self.request.direction,
            "pid": __import__("os").getpid(),
            "running": True,
            "phase": "running_cycle",
            "started_at_ms": now_ms(),
            "worker_heartbeat_ms": heartbeat,
            "heartbeat_ms": heartbeat,
            "symbol": self.request.symbol,
            "exchange": self.request.exchange,
            "range_pct": self.request.range_pct,
            "bucket_interval": self.request.bucket_interval,
            "required_buckets": self.request.required_buckets,
            "lookback_buckets": self.request.lookback_buckets,
            "complete_before": coverage_before.required_window_complete_count,
            "missing_before": coverage_before.missing_periods,
            "missing_bucket": sum(
                gap.reason == "missing_bucket"
                for gap in coverage_before.required_window_missing_buckets
            ),
            "degraded_bucket": len(
                coverage_before.required_window_degraded_buckets
            ),
            "save_raw_trades": self.request.save_raw_trades,
            "chunk_sleep_seconds": self.request.chunk_sleep_seconds,
            "max_seconds_per_cycle": self.request.max_seconds_per_cycle,
            "max_trades_per_cycle": self.request.max_trades_per_cycle,
            "last_error": None,
        }
        if reset_status:
            self.status_store.write(payload)
        else:
            self.status_store.patch(**payload)

    def _run_locked(
        self,
        *,
        started: float,
        coverage_before,
        mark_process_finished_on_summary: bool,
    ) -> RangeBackfillSummary:
        target_gaps = self._select_target_gaps(self._target_gaps(coverage_before))
        if not target_gaps:
            return self._finish_summary(
                started=started,
                before=coverage_before,
                downloaded_files=0,
                trades_loaded=0,
                range_bars_written=0,
                aggregates_written=0,
                status="ok",
                update_status=True,
                mark_process_finished=mark_process_finished_on_summary,
            )
        raw_symbol = self.request.raw_symbol or okx_raw_symbol_from_canonical(self.request.symbol)
        self._emit(
            "gaps_selected",
            gaps=len(target_gaps),
            first_bucket_end_ms=target_gaps[0].bucket_end_ms,
            last_bucket_end_ms=target_gaps[-1].bucket_end_ms,
        )
        results: list[_BuildWindowResult] = []
        # ── Single-gap live mode: try fast paths before full replay ──
        if len(target_gaps) == 1:
            gap = target_gaps[0]
            # Phase 1: existing range_bars fast path
            fast = self._try_complete_from_existing_range_bars(
                gap, coverage_before
            )
            if fast is not None:
                results.append(fast)
            else:
                # Phase 2: checkpoint-anchored targeted replay
                checkpoint_result = (
                    self._try_repair_from_bucket_checkpoint(
                        gap, raw_symbol, started, coverage_before
                    )
                )
                if checkpoint_result is not None:
                    results.append(checkpoint_result)
                else:
                    # Phase 3: full replay fallback
                    results.append(
                        self._run_build_window(
                            gaps=(gap,),
                            raw_symbol=raw_symbol,
                            started=started,
                            coverage_before=coverage_before,
                        )
                    )
        else:
            first = self._run_build_window(
                gaps=target_gaps,
                raw_symbol=raw_symbol,
                started=started,
                coverage_before=coverage_before,
            )
            if self._live_archive_is_not_ready(first):
                results.append(first)
            elif first.missing_raw_days and len(target_gaps) > 1:
                for gap in target_gaps:
                    result = self._run_build_window(
                        gaps=(gap,),
                        raw_symbol=raw_symbol,
                        started=started,
                        coverage_before=coverage_before,
                    )
                    results.append(result)
                    if result.resource_limited:
                        break
            else:
                results.append(first)

        downloaded = sum(result.downloaded_files for result in results)
        raw_rows = sum(result.raw_rows for result in results)
        filtered_rows = sum(result.filtered_rows for result in results)
        dropped_rows = sum(result.dropped_rows for result in results)
        trades_loaded = sum(result.trades_loaded for result in results)
        written_bars = sum(result.range_bars_written for result in results)
        aggregates_written = sum(result.aggregates_written for result in results)
        missing_raw_days = tuple(
            dict.fromkeys(day for result in results for day in result.missing_raw_days)
        )
        failed_downloads = tuple(
            dict.fromkeys(url for result in results for url in result.failed_downloads)
        )
        skipped_buckets = sum(result.skipped_buckets_due_missing_raw for result in results)
        resource_limited = any(result.resource_limited for result in results)
        target_bucket_start_ms = min(
            (
                result.target_bucket_start_ms
                for result in results
                if result.target_bucket_start_ms is not None
            ),
            default=None,
        )
        target_bucket_end_ms = max(
            (
                result.target_bucket_end_ms
                for result in results
                if result.target_bucket_end_ms is not None
            ),
            default=None,
        )
        selected_archive_dates = tuple(
            dict.fromkeys(
                day for result in results for day in result.selected_archive_dates
            )
        )
        per_file_min_trade_time_ms = tuple(
            item
            for result in results
            for item in result.per_file_min_trade_time_ms
        )
        per_file_max_trade_time_ms = tuple(
            item
            for result in results
            for item in result.per_file_max_trade_time_ms
        )
        target_trade_count = sum(result.target_trade_count for result in results)
        candidate_range_bars = sum(result.candidate_range_bars for result in results)
        candidate_aggregates = sum(result.candidate_aggregates for result in results)
        # ── Aggregate new diagnostic fields ──────────────────────────
        repair_method = next(
            (
                result.repair_method
                for result in reversed(results)
                if result.repair_method
            ),
            "",
        )
        target_window_reached = any(
            result.target_window_reached for result in results
        )
        target_bucket_proven_complete = any(
            result.target_bucket_proven_complete for result in results
        )
        anchor_last_trade_ts_ms = next(
            (
                result.anchor_last_trade_ts_ms
                for result in reversed(results)
                if result.anchor_last_trade_ts_ms is not None
            ),
            None,
        )
        replay_start_ms = next(
            (
                result.replay_start_ms
                for result in reversed(results)
                if result.replay_start_ms is not None
            ),
            None,
        )
        replay_end_ms = next(
            (
                result.replay_end_ms
                for result in reversed(results)
                if result.replay_end_ms is not None
            ),
            None,
        )
        pre_replay_existing_range_bars = sum(
            result.pre_replay_existing_range_bars for result in results
        )
        generated_range_bars = sum(
            result.generated_range_bars for result in results
        )
        combined_range_bars = sum(
            result.combined_range_bars for result in results
        )
        filtered_reason_if_zero = next(
            (
                result.filtered_reason_if_zero
                for result in reversed(results)
                if result.filtered_reason_if_zero
            ),
            None,
        )
        archive_not_ready = any(
            self._live_archive_is_not_ready(result) for result in results
        )
        if archive_not_ready and aggregates_written == 0:
            status = "archive_not_ready"
        elif missing_raw_days and aggregates_written == 0 and written_bars == 0:
            status = "no_progress"
        elif missing_raw_days or resource_limited:
            status = "partial"
        elif aggregates_written == 0:
            status = "no_progress"
        else:
            status = "ok"
        hint = (
            "raw OKX trades zip missing; run downloader or remove --no-download"
            if missing_raw_days and not self.request.allow_download
            else None
        )
        last_error = failed_downloads[-1] if failed_downloads else None
        return self._finish_summary(
            started=started,
            before=coverage_before,
            downloaded_files=downloaded,
            raw_rows=raw_rows,
            filtered_rows=filtered_rows,
            dropped_rows=dropped_rows,
            trades_loaded=trades_loaded,
            range_bars_written=written_bars,
            aggregates_written=aggregates_written,
            status=status,
            last_error=last_error,
            missing_raw_days=missing_raw_days,
            failed_downloads=failed_downloads,
            skipped_buckets_due_missing_raw=skipped_buckets,
            hint=hint,
            mark_process_finished=mark_process_finished_on_summary,
            target_bucket_start_ms=target_bucket_start_ms,
            target_bucket_end_ms=target_bucket_end_ms,
            selected_archive_dates=selected_archive_dates,
            per_file_min_trade_time_ms=per_file_min_trade_time_ms,
            per_file_max_trade_time_ms=per_file_max_trade_time_ms,
            target_trade_count=target_trade_count,
            candidate_range_bars=candidate_range_bars,
            candidate_aggregates=candidate_aggregates,
            filtered_reason_if_zero=filtered_reason_if_zero,
            repair_method=repair_method,
            target_window_reached=target_window_reached,
            target_bucket_proven_complete=target_bucket_proven_complete,
            anchor_last_trade_ts_ms=anchor_last_trade_ts_ms,
            replay_start_ms=replay_start_ms,
            replay_end_ms=replay_end_ms,
            pre_replay_existing_range_bars=pre_replay_existing_range_bars,
            generated_range_bars=generated_range_bars,
            combined_range_bars=combined_range_bars,
        )

    # ── Phase 1: existing range_bars fast path ──────────────────────────

    def _try_complete_from_existing_range_bars(
        self,
        gap: BucketGap,
        coverage_before,
    ) -> _BuildWindowResult | None:
        """If range bars already exist in the DB for this bucket AND we can
        prove they are complete (via a checkpoint with COMPLETE coverage),
        write the completed aggregate directly without any archive scan."""
        bucket_ms = interval_to_ms(self.request.bucket_interval)
        rows = self.range_bar_store.load(
            symbol=self.request.symbol,
            range_pct=self.request.range_pct,
            time_range=TimeRange(gap.bucket_start_ms, gap.bucket_end_ms),
        )
        if not rows:
            return None

        # Completeness proof: we need a checkpoint that says COMPLETE and
        # whose last_trade_ts extends to or past the bucket end.
        checkpoint = self.checkpoint_store.load_checkpoint(
            exchange=self.request.exchange,
            symbol=self.request.symbol,
            range_pct=self.request.range_pct,
            bucket_start_ms=gap.bucket_start_ms,
        )
        proven_complete = (
            checkpoint is not None
            and checkpoint.coverage_status == RangeCoverageStatus.COMPLETE.value
            and checkpoint.last_trade_ts_ms is not None
            and (
                checkpoint.last_trade_ts_ms >= gap.bucket_end_ms
                or (
                    checkpoint.checkpoint_updated_at_ms > gap.bucket_end_ms
                )
            )
        )
        if not proven_complete:
            return None

        aggregates = RangeBarAggregator().aggregate(rows, bucket_ms=bucket_ms)
        target_aggregate = next(
            (
                a
                for a in aggregates
                if a.bucket_start_ms == gap.bucket_start_ms
                and a.bucket_end_ms == gap.bucket_end_ms
            ),
            None,
        )
        if target_aggregate is None:
            return None

        completed_at = now_ms()
        self.checkpoint_store.save_completed_aggregate(
            exchange=self.request.exchange,
            aggregate=target_aggregate,
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            missing_gap_ms=0,
            completed_at_ms=completed_at,
        )
        self._emit(
            "existing_range_bars_completed",
            bucket_start_ms=gap.bucket_start_ms,
            bucket_end_ms=gap.bucket_end_ms,
            range_bar_count=len(rows),
            aggregate_bar_count=target_aggregate.bar_count,
        )
        return _BuildWindowResult(
            aggregates_written=1,
            repair_method="existing_range_bars",
            target_window_reached=True,
            target_bucket_proven_complete=True,
            target_bucket_start_ms=gap.bucket_start_ms,
            target_bucket_end_ms=gap.bucket_end_ms,
            candidate_range_bars=len(rows),
            candidate_aggregates=1,
            selected_archive_dates=(),
            raw_rows=0,
            filtered_rows=0,
            trades_loaded=0,
        )

    # ── Phase 2: checkpoint-anchored targeted replay ────────────────────

    def _try_repair_from_bucket_checkpoint(
        self,
        gap: BucketGap,
        raw_symbol: str,
        started: float,
        coverage_before,
    ) -> _BuildWindowResult | None:
        """Restore the RangeBarBuilder from a saved checkpoint and replay
        only the trades between checkpoint.last_trade_ts_ms+1 and the bucket
        end.  This avoids scanning entire archive days before the target."""
        checkpoint = self.checkpoint_store.load_checkpoint(
            exchange=self.request.exchange,
            symbol=self.request.symbol,
            range_pct=self.request.range_pct,
            bucket_start_ms=gap.bucket_start_ms,
        )
        if checkpoint is None:
            return None
        builder_state = getattr(checkpoint, "builder_state", None)
        if not isinstance(builder_state, Mapping) or not builder_state:
            return None
        if checkpoint.last_trade_ts_ms is None:
            return None

        # Checkpoint already covers the entire bucket → try existing bars.
        if checkpoint.last_trade_ts_ms >= gap.bucket_end_ms:
            return self._try_complete_from_existing_range_bars(
                gap, coverage_before
            )

        # Restore builder and compute the replay window.
        try:
            builder = RangeBarBuilder.restore_state(builder_state)
        except (KeyError, TypeError, ValueError) as exc:
            self._emit(
                "checkpoint_restore_failed",
                bucket_start_ms=gap.bucket_start_ms,
                error=str(exc),
            )
            return None

        replay_start_ms = int(checkpoint.last_trade_ts_ms) + 1
        replay_end_ms = gap.bucket_end_ms
        if replay_start_ms > replay_end_ms:
            return None

        # Archive days ONLY for the replay window (no previous-day anchor).
        raw_days = (
            tuple(
                iter_okx_archive_dates_for_utc_range(
                    replay_start_ms, replay_end_ms
                )
            )
            if str(self.request.exchange).strip().lower() == "okx"
            else tuple(iter_utc_dates(replay_start_ms, replay_end_ms))
        )
        selected_archive_dates = tuple(day.isoformat() for day in raw_days)

        self._emit(
            "checkpoint_replay_started",
            bucket_start_ms=gap.bucket_start_ms,
            bucket_end_ms=gap.bucket_end_ms,
            anchor_last_trade_ts_ms=checkpoint.last_trade_ts_ms,
            replay_start_ms=replay_start_ms,
            replay_end_ms=replay_end_ms,
            selected_archive_dates=list(selected_archive_dates),
        )

        # Load existing bars from before the replay window.
        existing_rows = self.range_bar_store.load(
            symbol=self.request.symbol,
            range_pct=self.request.range_pct,
            time_range=TimeRange(gap.bucket_start_ms, gap.bucket_end_ms),
        )
        before_rows = [
            r for r in existing_rows if r.end_time_ms < replay_start_ms
        ]
        before_rows.sort(key=lambda item: (item.end_time_ms, item.bar_id))

        # Ensure raw files for the replay window.
        raw_result = self._ensure_raw_days(
            raw_symbol=raw_symbol,
            days=raw_days,
            skipped_buckets=1,
        )
        if raw_result.missing_raw_days:
            return replace(
                raw_result,
                target_bucket_start_ms=gap.bucket_start_ms,
                target_bucket_end_ms=gap.bucket_end_ms,
                selected_archive_dates=selected_archive_dates,
                repair_method="checkpoint_anchored_replay",
                anchor_last_trade_ts_ms=checkpoint.last_trade_ts_ms,
                replay_start_ms=replay_start_ms,
                replay_end_ms=replay_end_ms,
                pre_replay_existing_range_bars=len(before_rows),
                filtered_reason_if_zero=(
                    "archive_not_ready"
                    if self._live_archive_is_not_ready(raw_result)
                    else "selected_archive_file_missing"
                ),
            )

        # Stream and feed trades from the replay window only.
        replay_time = TimeRange(replay_start_ms, replay_end_ms)
        generated_rows: list = []
        raw_rows = 0
        filtered_rows = 0
        dropped_rows = 0
        trades_loaded = 0
        processed_through_ms: int | None = None
        resource_limited = False
        chunk_index = 0
        per_file_min: dict[str, int | None] = {}
        per_file_max: dict[str, int | None] = {}
        target_trade_count = 0
        max_valid_trade_time_ms = (
            self._now_ms_value if self._now_ms_value is not None else now_ms()
        ) + 86_400_000

        for day in raw_days:
            day_iso = day.isoformat()
            file_path = self.archive.local_path(
                raw_symbol=raw_symbol, day=day
            )
            for chunk in iter_trade_csv_chunks(
                file_path, chunksize=self.request.chunksize
            ):
                chunk_index += 1
                filtered = filter_okx_trade_chunk_by_time(
                    chunk,
                    start_time_ms=replay_start_ms,
                    end_time_ms=replay_end_ms,
                    max_valid_trade_time_ms=max_valid_trade_time_ms,
                )
                raw_rows += filtered.raw_rows
                filtered_rows += filtered.filtered_rows
                observed_times = [
                    v
                    for v in (
                        filtered.first_trade_time_ms,
                        filtered.last_trade_time_ms,
                    )
                    if v is not None
                ]
                if observed_times:
                    chunk_min = min(observed_times)
                    chunk_max = max(observed_times)
                    per_file_min[day_iso] = min(
                        chunk_min,
                        per_file_min.get(day_iso, chunk_min) or chunk_min,
                    )
                    per_file_max[day_iso] = max(
                        chunk_max,
                        per_file_max.get(day_iso, chunk_max) or chunk_max,
                    )
                trades = normalize_okx_trade_chunk(
                    filtered.rows,
                    symbol=self.request.symbol,
                    raw_symbol=raw_symbol,
                    exchange=self.request.exchange,
                    max_valid_trade_time_ms=max_valid_trade_time_ms,
                )
                dropped_rows += filtered.raw_rows - len(trades)
                if trades:
                    trades_loaded += len(trades)
                    target_trade_count += sum(
                        1
                        for t in trades
                        if replay_time.start_time_ms
                        <= int(t.trade_time_ms or t.event_time_ms or -1)
                        <= replay_time.end_time_ms
                    )
                    processed_through_ms = (
                        trades[-1].trade_time_ms
                        or trades[-1].event_time_ms
                        or processed_through_ms
                    )
                for trade in trades:
                    for bar in builder.on_trade(trade):
                        if (
                            replay_time.start_time_ms
                            <= bar.end_time_ms
                            <= replay_time.end_time_ms
                        ):
                            generated_rows.append(bar)

                if (
                    self.request.max_trades_per_cycle > 0
                    and trades_loaded >= self.request.max_trades_per_cycle
                ):
                    resource_limited = True
                    break
                if (
                    self.request.max_seconds_per_cycle > 0
                    and time.monotonic() - started
                    >= self.request.max_seconds_per_cycle
                ):
                    resource_limited = True
                    break
                if self.request.chunk_sleep_seconds > 0:
                    time.sleep(float(self.request.chunk_sleep_seconds))
            if resource_limited:
                break

        # Combine and determine completeness.
        generated_rows.sort(key=lambda item: (item.end_time_ms, item.bar_id))
        combined_rows = before_rows + generated_rows
        combined_rows.sort(key=lambda item: (item.end_time_ms, item.bar_id))

        replay_complete = not resource_limited or (
            processed_through_ms is not None
            and processed_through_ms >= replay_end_ms
        )
        bucket_ms = interval_to_ms(self.request.bucket_interval)
        aggregates = [
            a
            for a in RangeBarAggregator().aggregate(
                combined_rows, bucket_ms=bucket_ms
            )
            if a.bucket_start_ms == gap.bucket_start_ms
            and a.bucket_end_ms == gap.bucket_end_ms
        ]
        aggregates_written = 0
        if aggregates and replay_complete:
            completed_at = now_ms()
            for aggregate in aggregates:
                self.checkpoint_store.save_completed_aggregate(
                    exchange=self.request.exchange,
                    aggregate=aggregate,
                    coverage_status=RangeCoverageStatus.COMPLETE.value,
                    missing_gap_ms=0,
                    completed_at_ms=completed_at,
                )
            aggregates_written = len(aggregates)
            # Write the generated rows to the range bar store.
            if generated_rows:
                self.range_bar_store.replace_range(
                    symbol=self.request.symbol,
                    range_pct=self.request.range_pct,
                    time_range=TimeRange(replay_start_ms, replay_end_ms),
                    rows=generated_rows,
                )

        filtered_reason = (
            _aggregate_zero_reason(
                aggregates=aggregates,
                candidate_aggregates=aggregates,
                bars=combined_rows,
                trades_loaded=trades_loaded,
                target_trade_count=target_trade_count,
                resource_limited=resource_limited,
            )
            if not aggregates
            else None
        )

        self._emit(
            "checkpoint_replay_finished",
            bucket_start_ms=gap.bucket_start_ms,
            bucket_end_ms=gap.bucket_end_ms,
            replay_complete=replay_complete,
            aggregates_written=aggregates_written,
            pre_replay_bars=len(before_rows),
            generated_bars=len(generated_rows),
            combined_bars=len(combined_rows),
        )
        return _BuildWindowResult(
            raw_rows=raw_rows,
            filtered_rows=filtered_rows,
            dropped_rows=dropped_rows,
            trades_loaded=trades_loaded,
            aggregates_written=aggregates_written,
            resource_limited=resource_limited,
            repair_method="checkpoint_anchored_replay",
            target_window_reached=replay_complete or target_trade_count > 0,
            target_bucket_proven_complete=aggregates_written > 0,
            anchor_last_trade_ts_ms=checkpoint.last_trade_ts_ms,
            replay_start_ms=replay_start_ms,
            replay_end_ms=replay_end_ms,
            pre_replay_existing_range_bars=len(before_rows),
            generated_range_bars=len(generated_rows),
            combined_range_bars=len(combined_rows),
            target_bucket_start_ms=gap.bucket_start_ms,
            target_bucket_end_ms=gap.bucket_end_ms,
            selected_archive_dates=selected_archive_dates,
            per_file_min_trade_time_ms=tuple(per_file_min.items()),
            per_file_max_trade_time_ms=tuple(per_file_max.items()),
            target_trade_count=target_trade_count,
            candidate_range_bars=len(combined_rows),
            candidate_aggregates=len(aggregates),
            filtered_reason_if_zero=filtered_reason,
        )

    # ── Phase 3: full replay fallback ──────────────────────────────────

    def _run_build_window(
        self,
        *,
        gaps: tuple[BucketGap, ...],
        raw_symbol: str,
        started: float,
        coverage_before,
    ) -> _BuildWindowResult:
        earliest_start = min(gap.bucket_start_ms for gap in gaps)
        latest_end = max(gap.bucket_end_ms for gap in gaps)
        anchor_start = previous_utc_day_start_ms(earliest_start)
        raw_days = (
            tuple(iter_okx_archive_dates_for_utc_range(anchor_start, latest_end))
            if str(self.request.exchange).strip().lower() == "okx"
            else tuple(iter_utc_dates(anchor_start, latest_end))
        )
        selected_archive_dates = tuple(day.isoformat() for day in raw_days)
        self._emit(
            "build_window_started",
            gaps=len(gaps),
            first_bucket_end_ms=gaps[0].bucket_end_ms,
            last_bucket_end_ms=gaps[-1].bucket_end_ms,
            target_start_ms=earliest_start,
            target_end_ms=latest_end,
            anchor_start_ms=anchor_start,
            target_bucket_start_ms=earliest_start,
            target_bucket_end_ms=latest_end,
            selected_archive_dates=list(selected_archive_dates),
        )
        self.status_store.patch(
            target_bucket_start_ms=earliest_start,
            target_bucket_end_ms=latest_end,
            selected_archive_dates=list(selected_archive_dates),
        )
        self._emit(
            "ensuring_raw_days",
            days=len(raw_days),
            first_day=raw_days[0].isoformat() if raw_days else None,
            last_day=raw_days[-1].isoformat() if raw_days else None,
        )
        raw_result = self._ensure_raw_days(
            raw_symbol=raw_symbol,
            days=raw_days,
            skipped_buckets=len(gaps),
        )
        if raw_result.missing_raw_days:
            return replace(
                raw_result,
                target_bucket_start_ms=earliest_start,
                target_bucket_end_ms=latest_end,
                selected_archive_dates=selected_archive_dates,
                filtered_reason_if_zero=(
                    "archive_not_ready"
                    if self._live_archive_is_not_ready(raw_result)
                    else "selected_archive_file_missing"
                ),
            )

        downloaded = raw_result.downloaded_files
        builder = RangeBarBuilder(
            range_pct=Decimal(str(self.request.range_pct)),
            contract_value=Decimal(str(self.request.contract_value)),
        )
        target_time = TimeRange(min(gap.bucket_start_ms for gap in gaps), latest_end)
        bars = []
        raw_rows = 0
        filtered_rows = 0
        dropped_rows = 0
        trades_loaded = 0
        processed_through_ms: int | None = None
        resource_limited = False
        chunk_index = 0
        last_progress_at = started
        per_file_min: dict[str, int | None] = {}
        per_file_max: dict[str, int | None] = {}
        target_trade_count = 0
        max_valid_trade_time_ms = (self._now_ms_value if self._now_ms_value is not None else now_ms()) + 86_400_000
        for day in raw_days:
            day_iso = day.isoformat()
            file_path = self.archive.local_path(raw_symbol=raw_symbol, day=day)
            self._emit(
                "file_read_started",
                day=day.isoformat(),
                path=str(file_path),
                size=file_path.stat().st_size if file_path.exists() else 0,
            )
            file_chunk_index = 0
            for chunk in iter_trade_csv_chunks(file_path, chunksize=self.request.chunksize):
                chunk_index += 1
                file_chunk_index += 1
                filtered = filter_okx_trade_chunk_by_time(
                    chunk,
                    start_time_ms=anchor_start,
                    end_time_ms=latest_end,
                    max_valid_trade_time_ms=max_valid_trade_time_ms,
                )
                raw_rows += filtered.raw_rows
                filtered_rows += filtered.filtered_rows
                observed_times = [
                    value
                    for value in (
                        filtered.first_trade_time_ms,
                        filtered.last_trade_time_ms,
                    )
                    if value is not None
                ]
                if observed_times:
                    chunk_min = min(observed_times)
                    chunk_max = max(observed_times)
                    per_file_min[day_iso] = min(
                        chunk_min,
                        per_file_min.get(day_iso, chunk_min) or chunk_min,
                    )
                    per_file_max[day_iso] = max(
                        chunk_max,
                        per_file_max.get(day_iso, chunk_max) or chunk_max,
                    )
                trades = normalize_okx_trade_chunk(
                    filtered.rows,
                    symbol=self.request.symbol,
                    raw_symbol=raw_symbol,
                    exchange=self.request.exchange,
                    max_valid_trade_time_ms=max_valid_trade_time_ms,
                )
                dropped_rows += filtered.raw_rows - len(trades)
                if trades:
                    if self.trade_store is not None:
                        self.trade_store.save_trades(trades)
                    trades_loaded += len(trades)
                    target_trade_count += sum(
                        1
                        for trade in trades
                        if target_time.start_time_ms
                        <= int(trade.trade_time_ms or trade.event_time_ms or -1)
                        <= target_time.end_time_ms
                    )
                    processed_through_ms = trades[-1].trade_time_ms or trades[-1].event_time_ms or processed_through_ms
                for trade in trades:
                    for bar in builder.on_trade(trade):
                        if target_time.start_time_ms <= bar.end_time_ms <= target_time.end_time_ms:
                            bars.append(bar)
                heartbeat = now_ms()
                self.status_store.patch(
                    worker_heartbeat_ms=heartbeat,
                    heartbeat_ms=heartbeat,
                    raw_rows=raw_rows,
                    filtered_rows=filtered_rows,
                    dropped_rows=dropped_rows,
                    trades_loaded=trades_loaded,
                )
                if self._should_emit_chunk_progress(
                    chunk_index=chunk_index,
                    last_progress_at=last_progress_at,
                ):
                    last_progress_at = time.monotonic()
                    self._emit(
                        "chunk_progress",
                        file=str(file_path),
                        day=day.isoformat(),
                        chunk_index=chunk_index,
                        file_chunk_index=file_chunk_index,
                        raw_rows=raw_rows,
                        filtered_rows=filtered_rows,
                        valid_trades=trades_loaded,
                        dropped_rows=dropped_rows,
                        chunk_raw_rows=filtered.raw_rows,
                        chunk_filtered_rows=filtered.filtered_rows,
                        chunk_valid_trades=len(trades),
                        chunk_dropped_rows=filtered.raw_rows - len(trades),
                        trades_loaded=trades_loaded,
                        range_bars_buffered=len(bars),
                        first_trade_time_ms=filtered.first_trade_time_ms,
                        last_trade_time_ms=filtered.last_trade_time_ms,
                        elapsed_seconds=time.monotonic() - started,
                    )
                if self.request.chunk_sleep_seconds > 0:
                    time.sleep(float(self.request.chunk_sleep_seconds))
                if (
                    self.request.max_chunks_per_cycle > 0
                    and chunk_index >= self.request.max_chunks_per_cycle
                ):
                    resource_limited = True
                    break
                if (
                    self.request.max_trades_per_cycle > 0
                    and trades_loaded >= self.request.max_trades_per_cycle
                ):
                    resource_limited = True
                    break
                if (
                    self.request.max_seconds_per_cycle > 0
                    and time.monotonic() - started >= self.request.max_seconds_per_cycle
                ):
                    resource_limited = True
                    break
                if (
                    filtered.first_trade_time_ms is not None
                    and filtered.last_trade_time_ms is not None
                    and filtered.first_trade_time_ms <= filtered.last_trade_time_ms
                    and filtered.first_trade_time_ms > latest_end
                ):
                    self._emit(
                        "file_read_stopped",
                        day=day.isoformat(),
                        reason="past_target_end",
                        first_trade_time_ms=filtered.first_trade_time_ms,
                        target_end_ms=latest_end,
                    )
                    break
            if resource_limited:
                break

            self._emit(
                "file_read_completed",
                day=day_iso,
                per_file_min_trade_time_ms=per_file_min.get(day_iso),
                per_file_max_trade_time_ms=per_file_max.get(day_iso),
            )

        bars.sort(key=lambda item: (item.end_time_ms, item.bar_id))
        bucket_ms = interval_to_ms(self.request.bucket_interval)
        target_ends = {gap.bucket_end_ms for gap in gaps}
        complete_through_ms = latest_end if not resource_limited else (processed_through_ms or -1)
        writable_time_range = self._writable_time_range(
            target_time=target_time,
            resource_limited=resource_limited,
            complete_through_ms=complete_through_ms,
        )
        writable_bars = (
            []
            if writable_time_range is None
            else [
                bar
                for bar in bars
                if writable_time_range.start_time_ms
                <= bar.end_time_ms
                <= writable_time_range.end_time_ms
            ]
        )
        written_bars = 0
        if writable_time_range is not None:
            self._emit(
                "writing_range_bars",
                rows=len(writable_bars),
                start_time_ms=writable_time_range.start_time_ms,
                end_time_ms=writable_time_range.end_time_ms,
            )
            written_bars = self.range_bar_store.replace_range(
                symbol=self.request.symbol,
                range_pct=self.request.range_pct,
                time_range=writable_time_range,
                rows=writable_bars,
            )
            self._emit("range_bars_written", rows=written_bars)
        candidate_aggregate_rows = [
            aggregate
            for aggregate in RangeBarAggregator().aggregate(bars, bucket_ms=bucket_ms)
            if aggregate.bucket_end_ms in target_ends
        ]
        aggregates = [
            aggregate
            for aggregate in candidate_aggregate_rows
            if target_time.start_time_ms <= aggregate.bucket_start_ms
            and aggregate.bucket_end_ms <= target_time.end_time_ms
            and aggregate.bucket_end_ms <= coverage_before.current_closed_bucket_end_ms
            and aggregate.bucket_end_ms <= complete_through_ms
            and aggregate.bucket_start_ms >= MIN_VALID_COMPLETED_AGGREGATE_MS
            and aggregate.bucket_end_ms >= MIN_VALID_COMPLETED_AGGREGATE_MS
            and aggregate.bucket_end_ms > aggregate.bucket_start_ms
        ]
        filtered_reason_if_zero = _aggregate_zero_reason(
            aggregates=aggregates,
            candidate_aggregates=candidate_aggregate_rows,
            bars=bars,
            trades_loaded=trades_loaded,
            target_trade_count=target_trade_count,
            resource_limited=resource_limited,
        )
        self._emit(
            "writing_aggregates",
            rows=len(aggregates),
            target_bucket_start_ms=target_time.start_time_ms,
            target_bucket_end_ms=target_time.end_time_ms,
            selected_archive_dates=list(selected_archive_dates),
            per_file_min_trade_time_ms=dict(per_file_min),
            per_file_max_trade_time_ms=dict(per_file_max),
            target_trade_count=target_trade_count,
            candidate_range_bars=len(bars),
            candidate_aggregates=len(candidate_aggregate_rows),
            filtered_reason_if_zero=filtered_reason_if_zero,
        )
        completed_at = now_ms()
        for aggregate in aggregates:
            self.checkpoint_store.save_completed_aggregate(
                exchange=self.request.exchange,
                aggregate=aggregate,
                coverage_status=RangeCoverageStatus.COMPLETE.value,
                missing_gap_ms=0,
                completed_at_ms=completed_at,
            )
        self._emit(
            "aggregates_written",
            rows=len(aggregates),
            filtered_reason_if_zero=filtered_reason_if_zero,
        )
        target_window_reached = target_trade_count > 0
        return _BuildWindowResult(
            downloaded_files=downloaded,
            raw_rows=raw_rows,
            filtered_rows=filtered_rows,
            dropped_rows=dropped_rows,
            trades_loaded=trades_loaded,
            range_bars_written=written_bars,
            aggregates_written=len(aggregates),
            resource_limited=resource_limited,
            target_bucket_start_ms=target_time.start_time_ms,
            target_bucket_end_ms=target_time.end_time_ms,
            selected_archive_dates=selected_archive_dates,
            per_file_min_trade_time_ms=tuple(per_file_min.items()),
            per_file_max_trade_time_ms=tuple(per_file_max.items()),
            target_trade_count=target_trade_count,
            candidate_range_bars=len(bars),
            candidate_aggregates=len(candidate_aggregate_rows),
            filtered_reason_if_zero=filtered_reason_if_zero,
            repair_method="full_replay_fallback",
            target_window_reached=target_window_reached,
            target_bucket_proven_complete=(
                len(aggregates) > 0
                and not resource_limited
            ),
        )

    def _ensure_raw_days(
        self,
        *,
        raw_symbol: str,
        days: tuple[date, ...],
        skipped_buckets: int,
    ) -> _BuildWindowResult:
        downloaded = 0
        missing_days: list[str] = []
        failed_downloads: list[str] = []
        current_archive_day = self._current_archive_date()
        unavailable_archive_days = tuple(
            day
            for day in days
            if day >= current_archive_day
            and not self.archive.local_path(raw_symbol=raw_symbol, day=day).exists()
        )
        if unavailable_archive_days:
            for day in unavailable_archive_days:
                day_iso = day.isoformat()
                failed_url = okx_daily_trade_url(raw_symbol=raw_symbol, day=day)
                self._raw_day_failures[(raw_symbol, day_iso)] = failed_url
                self._archive_not_ready_days.add(day_iso)
                missing_days.append(day_iso)
                failed_downloads.append(failed_url)
                self._emit(
                    "raw_day_missing",
                    day=day_iso,
                    url=failed_url,
                    reason="current_or_future_archive_not_ready",
                )
            return _BuildWindowResult(
                missing_raw_days=tuple(missing_days),
                failed_downloads=tuple(failed_downloads),
                skipped_buckets_due_missing_raw=skipped_buckets,
            )
        for day in days:
            day_iso = day.isoformat()
            cache_key = (raw_symbol, day_iso)
            cached_failure = self._raw_day_failures.get(cache_key)
            if cached_failure is not None:
                missing_days.append(day_iso)
                failed_downloads.append(cached_failure)
                self._emit("raw_day_missing", day=day_iso, url=cached_failure, cached=True)
                break
            try:
                file = self.archive.ensure_daily_file(
                    symbol=self.request.symbol,
                    raw_symbol=raw_symbol,
                    day=day,
                    allow_download=self.request.allow_download,
                )
                downloaded += int(file.downloaded)
                self._emit(
                    "raw_day_ready",
                    day=day_iso,
                    path=str(file.path),
                    downloaded=bool(file.downloaded),
                    size=file.path.stat().st_size if file.path.exists() else 0,
                )
            except FileNotFoundError:
                failed_url = okx_daily_trade_url(raw_symbol=raw_symbol, day=day)
                self._raw_day_failures[cache_key] = failed_url
                missing_days.append(day_iso)
                failed_downloads.append(failed_url)
                self._emit("raw_day_missing", day=day_iso, url=failed_url)
                break
            except OkxHistoricalTradeDownloadError as exc:
                self._raw_day_failures[cache_key] = exc.url
                if (
                    str(self.request.mode).strip().lower() == "live"
                    and exc.status == 404
                    and day >= current_archive_day - timedelta(days=1)
                ):
                    self._archive_not_ready_days.add(day_iso)
                missing_days.append(day_iso)
                failed_downloads.append(exc.url)
                self._emit("raw_day_missing", day=day_iso, url=exc.url, error=str(exc))
                break
        if missing_days:
            return _BuildWindowResult(
                downloaded_files=downloaded,
                missing_raw_days=tuple(missing_days),
                failed_downloads=tuple(failed_downloads),
                skipped_buckets_due_missing_raw=skipped_buckets,
            )
        return _BuildWindowResult(downloaded_files=downloaded)

    def _live_archive_is_not_ready(self, result: _BuildWindowResult) -> bool:
        if str(self.request.mode).strip().lower() != "live" or not result.missing_raw_days:
            return False
        parsed_days: list[date] = []
        for value in result.missing_raw_days:
            try:
                parsed_days.append(date.fromisoformat(value))
            except ValueError:
                return False
        return bool(parsed_days) and all(
            day.isoformat() in self._archive_not_ready_days
            for day in parsed_days
        )

    def _current_archive_date(self) -> date:
        value = self._now_ms_value if self._now_ms_value is not None else now_ms()
        if str(self.request.exchange).strip().lower() == "okx":
            return okx_archive_date_from_utc_ms(int(value))
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC).date()

    def _writable_time_range(
        self,
        *,
        target_time: TimeRange,
        resource_limited: bool,
        complete_through_ms: int,
    ) -> TimeRange | None:
        if not resource_limited:
            return target_time
        covered_end_ms = min(target_time.end_time_ms, int(complete_through_ms))
        if covered_end_ms < target_time.start_time_ms:
            return None
        return TimeRange(target_time.start_time_ms, covered_end_ms)

    def _should_emit_chunk_progress(self, *, chunk_index: int, last_progress_at: float) -> bool:
        if self.progress_callback is None:
            return False
        if chunk_index <= 1 or chunk_index % 10 == 0:
            return True
        interval = max(0.0, float(self.request.progress_seconds))
        return interval > 0 and time.monotonic() - last_progress_at >= interval

    def _coverage_satisfied_for_mode(self, coverage) -> bool:
        if str(self.request.mode).strip().lower() == "prebuild":
            return coverage.available and not coverage.lookback_missing_buckets
        return coverage.available

    def _target_gaps(self, coverage) -> tuple[BucketGap, ...]:
        if str(self.request.mode).strip().lower() == "live":
            return tuple(coverage.required_window_missing_buckets)
        return tuple(coverage.lookback_missing_buckets)

    def _select_target_gaps(self, gaps: tuple[BucketGap, ...]) -> tuple[BucketGap, ...]:
        selected = list(gaps[: max(1, int(self.request.max_buckets_per_cycle))])
        if self.request.max_days_per_cycle <= 0:
            return tuple(sorted(selected, key=lambda item: item.bucket_end_ms))
        allowed_days: set[int] = set()
        limited: list[BucketGap] = []
        for gap in selected:
            day_start = gap.bucket_start_ms - (gap.bucket_start_ms % 86_400_000)
            if day_start not in allowed_days and len(allowed_days) >= self.request.max_days_per_cycle:
                continue
            allowed_days.add(day_start)
            limited.append(gap)
        return tuple(sorted(limited, key=lambda item: item.bucket_end_ms))

    def _finish_summary(
        self,
        *,
        started: float,
        before,
        downloaded_files: int,
        trades_loaded: int,
        range_bars_written: int,
        aggregates_written: int,
        status: str,
        raw_rows: int = 0,
        filtered_rows: int = 0,
        dropped_rows: int = 0,
        last_error: str | None = None,
        update_status: bool = True,
        mark_process_finished: bool = True,
        missing_raw_days: tuple[str, ...] = (),
        failed_downloads: tuple[str, ...] = (),
        skipped_buckets_due_missing_raw: int = 0,
        hint: str | None = None,
        target_bucket_start_ms: int | None = None,
        target_bucket_end_ms: int | None = None,
        selected_archive_dates: tuple[str, ...] = (),
        per_file_min_trade_time_ms: tuple[tuple[str, int | None], ...] = (),
        per_file_max_trade_time_ms: tuple[tuple[str, int | None], ...] = (),
        target_trade_count: int = 0,
        candidate_range_bars: int = 0,
        candidate_aggregates: int = 0,
        filtered_reason_if_zero: str | None = None,
        repair_method: str = "",
        target_window_reached: bool = False,
        target_bucket_proven_complete: bool = False,
        anchor_last_trade_ts_ms: int | None = None,
        replay_start_ms: int | None = None,
        replay_end_ms: int | None = None,
        pre_replay_existing_range_bars: int = 0,
        generated_range_bars: int = 0,
        combined_range_bars: int = 0,
    ) -> RangeBackfillSummary:
        after = self.check_coverage(now_ms_value=self._now_ms_value, direction=self.request.direction)
        summary = RangeBackfillSummary(
            symbol=self.request.symbol,
            exchange=self.request.exchange,
            range_pct=self.request.range_pct,
            bucket_interval=self.request.bucket_interval,
            target_buckets=self.request.required_buckets,
            complete_before=before.required_window_complete_count,
            complete_after=after.required_window_complete_count,
            missing_before=before.missing_periods,
            missing_after=after.missing_periods,
            downloaded_files=downloaded_files,
            raw_rows=raw_rows,
            filtered_rows=filtered_rows,
            dropped_rows=dropped_rows,
            trades_loaded=trades_loaded,
            range_bars_written=range_bars_written,
            aggregates_written=aggregates_written,
            elapsed_seconds=time.monotonic() - started,
            status=status,
            last_error=last_error,
            missing_raw_days=missing_raw_days,
            failed_downloads=failed_downloads,
            skipped_buckets_due_missing_raw=skipped_buckets_due_missing_raw,
            hint=hint,
            target_bucket_start_ms=target_bucket_start_ms,
            target_bucket_end_ms=target_bucket_end_ms,
            selected_archive_dates=selected_archive_dates,
            per_file_min_trade_time_ms=per_file_min_trade_time_ms,
            per_file_max_trade_time_ms=per_file_max_trade_time_ms,
            target_trade_count=target_trade_count,
            candidate_range_bars=candidate_range_bars,
            candidate_aggregates=candidate_aggregates,
            filtered_reason_if_zero=filtered_reason_if_zero,
            repair_method=repair_method,
            target_window_reached=target_window_reached,
            target_bucket_proven_complete=target_bucket_proven_complete,
            anchor_last_trade_ts_ms=anchor_last_trade_ts_ms,
            replay_start_ms=replay_start_ms,
            replay_end_ms=replay_end_ms,
            pre_replay_existing_range_bars=pre_replay_existing_range_bars,
            generated_range_bars=generated_range_bars,
            combined_range_bars=combined_range_bars,
        )
        if update_status:
            if status == "error":
                phase = "failed"
            elif status == "partial":
                phase = "partial"
            elif mark_process_finished and status in {"ok", "dry_run", "no_progress", "archive_not_ready"}:
                phase = "completed"
            else:
                phase = "sleeping"
            heartbeat = now_ms()
            self.status_store.patch(
                running=False if mark_process_finished else True,
                phase=phase,
                cycle_status=status,
                repair_target_status=(
                    "archive_not_ready"
                    if status == "archive_not_ready"
                    else (
                        "degraded_bucket"
                        if getattr(
                            before, "required_window_degraded_buckets", ()
                        )
                        else "missing_bucket"
                    )
                ),
                worker_heartbeat_ms=heartbeat,
                heartbeat_ms=heartbeat,
                complete_after=summary.complete_after,
                missing_after=summary.missing_after,
                downloaded_files=summary.downloaded_files,
                raw_rows=summary.raw_rows,
                filtered_rows=summary.filtered_rows,
                dropped_rows=summary.dropped_rows,
                trades_loaded=summary.trades_loaded,
                range_bars_written=summary.range_bars_written,
                aggregates_written=summary.aggregates_written,
                last_completed_bucket_end_ms=after.latest_complete_bucket_end_ms,
                last_scanned_bucket_end_ms=after.current_closed_bucket_end_ms,
                last_error=last_error,
                missing_raw_days=list(missing_raw_days),
                failed_downloads=list(failed_downloads),
                skipped_buckets_due_missing_raw=skipped_buckets_due_missing_raw,
                hint=hint,
                target_bucket_start_ms=summary.target_bucket_start_ms,
                target_bucket_end_ms=summary.target_bucket_end_ms,
                selected_archive_dates=list(summary.selected_archive_dates),
                per_file_min_trade_time_ms=dict(summary.per_file_min_trade_time_ms),
                per_file_max_trade_time_ms=dict(summary.per_file_max_trade_time_ms),
                target_trade_count=summary.target_trade_count,
                candidate_range_bars=summary.candidate_range_bars,
                candidate_aggregates=summary.candidate_aggregates,
                filtered_reason_if_zero=summary.filtered_reason_if_zero,
                repair_method=summary.repair_method,
                target_window_reached=summary.target_window_reached,
                target_bucket_proven_complete=summary.target_bucket_proven_complete,
                anchor_last_trade_ts_ms=summary.anchor_last_trade_ts_ms,
                replay_start_ms=summary.replay_start_ms,
                replay_end_ms=summary.replay_end_ms,
                pre_replay_existing_range_bars=summary.pre_replay_existing_range_bars,
                generated_range_bars=summary.generated_range_bars,
                combined_range_bars=summary.combined_range_bars,
                exit_code=0 if status in {"ok", "dry_run", "partial", "no_progress", "archive_not_ready"} else 1,
                finished_at_ms=now_ms() if mark_process_finished else None,
            )
        self._emit(
            "summary",
            status=summary.status,
            complete_after=summary.complete_after,
            missing_after=summary.missing_after,
            raw_rows=summary.raw_rows,
            filtered_rows=summary.filtered_rows,
            dropped_rows=summary.dropped_rows,
            trades_loaded=summary.trades_loaded,
            range_bars_written=summary.range_bars_written,
            aggregates_written=summary.aggregates_written,
            target_bucket_start_ms=summary.target_bucket_start_ms,
            target_bucket_end_ms=summary.target_bucket_end_ms,
            selected_archive_dates=list(summary.selected_archive_dates),
            per_file_min_trade_time_ms=dict(summary.per_file_min_trade_time_ms),
            per_file_max_trade_time_ms=dict(summary.per_file_max_trade_time_ms),
            target_trade_count=summary.target_trade_count,
            candidate_range_bars=summary.candidate_range_bars,
            candidate_aggregates=summary.candidate_aggregates,
            filtered_reason_if_zero=summary.filtered_reason_if_zero,
            repair_method=summary.repair_method,
            target_window_reached=summary.target_window_reached,
            target_bucket_proven_complete=summary.target_bucket_proven_complete,
            elapsed_seconds=summary.elapsed_seconds,
        )
        return summary


def _aggregate_zero_reason(
    *,
    aggregates,
    candidate_aggregates,
    bars,
    trades_loaded: int,
    target_trade_count: int,
    resource_limited: bool,
) -> str | None:
    if aggregates:
        return None
    if resource_limited:
        if target_trade_count <= 0:
            return "target_window_not_reached_due_resource_limit"
        return "resource_limit_before_target_complete"
    if trades_loaded <= 0:
        return "no_valid_trades_in_selected_archives"
    if target_trade_count <= 0:
        return "target_window_reached_but_no_trades"
    if not bars:
        return "no_range_bar_closed_in_target_window"
    if not candidate_aggregates:
        return "range_bars_did_not_form_target_aggregate"
    return "candidate_aggregate_filtered_by_coverage_guard"
