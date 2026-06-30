from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import time

from src.market_data.backfill.coverage import iter_utc_dates, previous_utc_day_start_ms
from src.market_data.backfill.lock import RangeBackfillLock
from src.market_data.backfill.models import BucketGap, RangeBackfillRequest, RangeBackfillSummary
from src.market_data.backfill.scanner import RangeBackfillScanner
from src.market_data.backfill.status_store import RangeBackfillStatusStore, now_ms
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.historical_trades.importer import iter_trade_csv_chunks, normalize_okx_trade_chunk
from src.market_data.historical_trades.okx_archive import (
    OkxHistoricalTradeArchive,
    OkxHistoricalTradeDownloadError,
    okx_daily_trade_url,
    okx_raw_symbol_from_canonical,
)
from src.market_data.models import RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.market_data.warmup.gap_detector import interval_to_ms


@dataclass(frozen=True)
class _BuildWindowResult:
    downloaded_files: int = 0
    trades_loaded: int = 0
    range_bars_written: int = 0
    aggregates_written: int = 0
    resource_limited: bool = False
    missing_raw_days: tuple[str, ...] = ()
    failed_downloads: tuple[str, ...] = ()
    skipped_buckets_due_missing_raw: int = 0


class RangeBackfillService:
    def __init__(self, request: RangeBackfillRequest) -> None:
        self.request = request
        self.checkpoint_store = SqliteRangeCheckpointStore(request.checkpoint_db_path)
        self.range_bar_store = SqliteRangeBarStore(request.market_db_path)
        self.trade_store = SqliteTradeStore(request.market_db_path) if request.save_raw_trades else None
        self.status_store = RangeBackfillStatusStore(request.status_path)
        self.archive = OkxHistoricalTradeArchive(request.raw_root)
        self._now_ms_value: int | None = None
        self._raw_day_failures: dict[tuple[str, str], str] = {}

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
        started = time.monotonic()
        coverage_before = self.check_coverage(now_ms_value=now_ms_value)
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
            self.status_store.patch(
                running=not mark_process_finished_on_summary,
                phase="failed",
                heartbeat_ms=now_ms(),
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
        payload = {
            "mode": self.request.mode,
            "direction": self.request.direction,
            "pid": __import__("os").getpid(),
            "running": True,
            "phase": "running_cycle",
            "started_at_ms": now_ms(),
            "heartbeat_ms": now_ms(),
            "symbol": self.request.symbol,
            "exchange": self.request.exchange,
            "range_pct": self.request.range_pct,
            "bucket_interval": self.request.bucket_interval,
            "required_buckets": self.request.required_buckets,
            "lookback_buckets": self.request.lookback_buckets,
            "complete_before": coverage_before.required_window_complete_count,
            "missing_before": coverage_before.missing_periods,
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
        earliest_start = min(gap.bucket_start_ms for gap in target_gaps)
        latest_end = max(gap.bucket_end_ms for gap in target_gaps)
        raw_symbol = self.request.raw_symbol or okx_raw_symbol_from_canonical(self.request.symbol)
        attempts = [target_gaps]
        results: list[_BuildWindowResult] = []
        first = self._run_build_window(
            gaps=target_gaps,
            raw_symbol=raw_symbol,
            started=started,
            coverage_before=coverage_before,
        )
        if first.missing_raw_days and len(target_gaps) > 1:
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
        if missing_raw_days and aggregates_written == 0 and written_bars == 0:
            status = "no_progress"
        elif missing_raw_days or resource_limited:
            status = "partial"
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
        )

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
        raw_result = self._ensure_raw_days(
            raw_symbol=raw_symbol,
            days=tuple(iter_utc_dates(anchor_start, latest_end)),
            skipped_buckets=len(gaps),
        )
        if raw_result.missing_raw_days:
            return raw_result

        downloaded = raw_result.downloaded_files
        builder = RangeBarBuilder(
            range_pct=Decimal(str(self.request.range_pct)),
            contract_value=Decimal(str(self.request.contract_value)),
        )
        target_time = TimeRange(min(gap.bucket_start_ms for gap in gaps), latest_end)
        bars = []
        trades_loaded = 0
        processed_through_ms: int | None = None
        resource_limited = False
        for day in iter_utc_dates(anchor_start, latest_end):
            file_path = self.archive.local_path(raw_symbol=raw_symbol, day=day)
            for chunk in iter_trade_csv_chunks(file_path, chunksize=self.request.chunksize):
                trades = normalize_okx_trade_chunk(
                    chunk,
                    symbol=self.request.symbol,
                    raw_symbol=raw_symbol,
                    exchange=self.request.exchange,
                )
                if trades:
                    if self.trade_store is not None:
                        self.trade_store.save(trades)
                    trades_loaded += len(trades)
                    processed_through_ms = trades[-1].trade_time_ms or trades[-1].event_time_ms or processed_through_ms
                for trade in trades:
                    for bar in builder.on_trade(trade):
                        if target_time.start_time_ms <= bar.end_time_ms <= target_time.end_time_ms:
                            bars.append(bar)
                self.status_store.patch(heartbeat_ms=now_ms(), trades_loaded=trades_loaded)
                if self.request.chunk_sleep_seconds > 0:
                    time.sleep(float(self.request.chunk_sleep_seconds))
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
            if resource_limited:
                break

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
            written_bars = self.range_bar_store.replace_range(
                symbol=self.request.symbol,
                range_pct=self.request.range_pct,
                time_range=writable_time_range,
                rows=writable_bars,
            )
        aggregates = [
            aggregate
            for aggregate in RangeBarAggregator().aggregate(bars, bucket_ms=bucket_ms)
            if aggregate.bucket_end_ms in target_ends
            and aggregate.bucket_end_ms <= coverage_before.current_closed_bucket_end_ms
            and aggregate.bucket_end_ms <= complete_through_ms
        ]
        completed_at = now_ms()
        for aggregate in aggregates:
            self.checkpoint_store.save_completed_aggregate(
                exchange=self.request.exchange,
                aggregate=aggregate,
                coverage_status=RangeCoverageStatus.COMPLETE.value,
                missing_gap_ms=0,
                completed_at_ms=completed_at,
            )
        return _BuildWindowResult(
            downloaded_files=downloaded,
            trades_loaded=trades_loaded,
            range_bars_written=written_bars,
            aggregates_written=len(aggregates),
            resource_limited=resource_limited,
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
        for day in days:
            day_iso = day.isoformat()
            cache_key = (raw_symbol, day_iso)
            cached_failure = self._raw_day_failures.get(cache_key)
            if cached_failure is not None:
                missing_days.append(day_iso)
                failed_downloads.append(cached_failure)
                break
            try:
                file = self.archive.ensure_daily_file(
                    symbol=self.request.symbol,
                    raw_symbol=raw_symbol,
                    day=day,
                    allow_download=self.request.allow_download,
                )
                downloaded += int(file.downloaded)
            except FileNotFoundError:
                failed_url = okx_daily_trade_url(raw_symbol=raw_symbol, day=day)
                self._raw_day_failures[cache_key] = failed_url
                missing_days.append(day_iso)
                failed_downloads.append(failed_url)
                break
            except OkxHistoricalTradeDownloadError as exc:
                self._raw_day_failures[cache_key] = exc.url
                missing_days.append(day_iso)
                failed_downloads.append(exc.url)
                break
        if missing_days:
            return _BuildWindowResult(
                downloaded_files=downloaded,
                missing_raw_days=tuple(missing_days),
                failed_downloads=tuple(failed_downloads),
                skipped_buckets_due_missing_raw=skipped_buckets,
            )
        return _BuildWindowResult(downloaded_files=downloaded)

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
        last_error: str | None = None,
        update_status: bool = True,
        mark_process_finished: bool = True,
        missing_raw_days: tuple[str, ...] = (),
        failed_downloads: tuple[str, ...] = (),
        skipped_buckets_due_missing_raw: int = 0,
        hint: str | None = None,
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
        )
        if update_status:
            if status == "error":
                phase = "failed"
            elif status == "partial":
                phase = "partial"
            elif mark_process_finished and status in {"ok", "dry_run", "no_progress"}:
                phase = "completed"
            else:
                phase = "sleeping"
            self.status_store.patch(
                running=False if mark_process_finished else True,
                phase=phase,
                heartbeat_ms=now_ms(),
                complete_after=summary.complete_after,
                missing_after=summary.missing_after,
                downloaded_files=summary.downloaded_files,
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
                exit_code=0 if status in {"ok", "dry_run", "partial", "no_progress"} else 1,
                finished_at_ms=now_ms() if mark_process_finished else None,
            )
        return summary
