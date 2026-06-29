from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Sequence

from src.market_data.backfill.coverage import validate_trade_coverage
from src.market_data.backfill.models import BackfillPlan, BackfillResult
from src.market_data.backfill.scheduler import select_candidates
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.models import RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import RangeBuilderCheckpoint, SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.platform.data.models import MarketTrade
from src.platform.exchanges.okx.historical_archive import OkxArchiveUnavailableError, OkxHistoricalArchive, daily_trades_zip_path
from src.platform.markets import get_market_profile


RestTailFetcher = Callable[[str, int, int], Sequence[MarketTrade]]


@dataclass
class BackfillService:
    market_db: str | Path
    checkpoint_db: str | Path
    raw_root: str | Path = "data/okx/raw"
    archive: OkxHistoricalArchive | None = None
    chunksize: int = 300_000
    busy_timeout_ms: int = 100
    edge_tolerance_ms: int = 60_000
    coverage_max_gap_ms: int = 15 * 60_000
    download_sleep_seconds: float = 2.0
    max_rest_tail_gap_minutes: int = 240
    max_rest_tail_buckets: int = 12
    tail_cooldown_seconds: int = 600
    rest_tail_fetcher: RestTailFetcher | None = None

    def __post_init__(self) -> None:
        self.market_db = Path(self.market_db)
        self.checkpoint_db = Path(self.checkpoint_db)
        self.raw_root = Path(self.raw_root)
        self.archive = self.archive or OkxHistoricalArchive(sleep_seconds=self.download_sleep_seconds)
        self.trade_store = SqliteTradeStore(self.market_db, busy_timeout_ms=self.busy_timeout_ms)
        self.range_bar_store = SqliteRangeBarStore(self.market_db, busy_timeout_ms=self.busy_timeout_ms)
        self.checkpoint_store = SqliteRangeCheckpointStore(self.checkpoint_db, busy_timeout_ms=self.busy_timeout_ms)
        self.aggregator = RangeBarAggregator()

    def process_plan(
        self,
        plan: BackfillPlan,
        *,
        max_buckets: int = 1,
        now: datetime | None = None,
        tail_cooldown: dict[int, int] | None = None,
    ) -> BackfillResult:
        result = BackfillResult()
        now = now or datetime.now(UTC)
        max_buckets = max(0, int(max_buckets))

        # Build prioritized candidate list with cooldown awareness.
        from src.market_data.backfill.scheduler import TailCooldownTracker

        tracker = TailCooldownTracker(
            cooldown_buckets=dict(tail_cooldown) if tail_cooldown else {},
            cooldown_seconds=self.tail_cooldown_seconds,
        )
        candidates, meta = select_candidates(
            plan=plan,
            max_buckets=max_buckets,
            cooldown_tracker=tracker,
            now=now,
        )

        # Populate result diagnostics.
        result.candidate_bucket_count = int(meta.get("total", 0))
        result.eligible_historical_bucket_count = int(meta.get("historical", 0))
        result.eligible_tail_bucket_count = int(meta.get("tail", 0))
        result.tail_cooldown_buckets = list(meta.get("cooldown", []))  # type: ignore[arg-type]
        result.tail_deferred_buckets = list(meta.get("deferred", []))  # type: ignore[arg-type]

        if not candidates:
            return result

        # Try each candidate in isolation so one failure does not block others.
        # Accumulate successful targets, then batch-rebuild for efficiency.
        successful_targets: list[int] = []
        for candidate in candidates:
            if len(successful_targets) >= max_buckets:
                break
            try:
                self._ensure_raw_trades(
                    plan=plan, targets=[candidate], result=result, now=now
                )
            except sqlite3.OperationalError as exc:
                if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                    result.locked = True
                    result.errors.append(str(exc))
                    return result
                raise
            except Exception as exc:  # noqa: BLE001 - worker daemon must keep running
                result.errors.append(f"{type(exc).__name__}: {exc}")
                _append_unique(result.skipped_buckets, candidate)
                continue

            if candidate in set(result.skipped_buckets):
                # This candidate failed raw-trade acquisition; fall through to next.
                continue

            successful_targets.append(candidate)
            _append_unique(result.selected_buckets, candidate)

        if not successful_targets:
            # Nothing succeeded. If every candidate was a current-day tail bucket,
            # give the operator a clear signal rather than silent zero.
            all_tail = (
                result.eligible_tail_bucket_count > 0
                and result.eligible_historical_bucket_count == 0
                and len([s for s in getattr(plan, "dirty_bucket_starts", ())]) == 0
            )
            if all_tail:
                result.errors.append(
                    "no eligible historical buckets processed; all candidates skipped"
                )
            return result

        # Batch rebuild all successful targets in one replay pass.
        try:
            self._rebuild_targets(
                plan=plan, targets=successful_targets, result=result
            )
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                result.locked = True
                result.errors.append(str(exc))
                return result
            raise
        except Exception as exc:  # noqa: BLE001 - worker daemon must keep running
            result.errors.append(f"{type(exc).__name__}: {exc}")
            return result

        return result

    def _ensure_raw_trades(self, *, plan: BackfillPlan, targets: Sequence[int], result: BackfillResult, now: datetime) -> None:
        target_count = len(set(targets))
        for start in sorted(set(targets)):
            end = start + plan.bucket_ms - 1
            day = datetime.fromtimestamp(start / 1000, tz=UTC).date()
            current_day = now.astimezone(UTC).date()
            if day < current_day:
                before = daily_trades_zip_path(self.raw_root, plan.raw_symbol, day)
                existed_before = before.exists()
                try:
                    meta = self.archive.ensure_daily_trades_zip(  # type: ignore[union-attr]
                        raw_root=self.raw_root,
                        raw_symbol=plan.raw_symbol,
                        day=day,
                        now=now,
                    )
                except OkxArchiveUnavailableError as exc:
                    _append_unique(result.skipped_buckets, start)
                    result.archive_errors.append(str(exc))
                    result.errors.append(str(exc))
                    continue
                except Exception as exc:  # noqa: BLE001 - archive/network/parser errors retry next cycle
                    _append_unique(result.skipped_buckets, start)
                    message = f"archive bucket_start_ms={start}: {type(exc).__name__}: {exc}"
                    result.archive_errors.append(message)
                    result.errors.append(message)
                    continue
                if meta is None:
                    _append_unique(result.skipped_buckets, start)
                    continue
                if not existed_before and getattr(meta, "status", "downloaded") == "downloaded":
                    result.downloaded_days += 1
                    if self.download_sleep_seconds > 0:
                        time.sleep(self.download_sleep_seconds)
                try:
                    result.imported_trades += self._import_zip(path=Path(meta.path), plan=plan, bucket_start_ms=start, bucket_end_ms=end)
                except Exception as exc:  # noqa: BLE001 - bad zip/csv must not kill daemon
                    _append_unique(result.skipped_buckets, start)
                    message = f"archive import bucket_start_ms={start}: {type(exc).__name__}: {exc}"
                    result.archive_errors.append(message)
                    result.errors.append(message)
                continue

            validation = self._validate_bucket_trade_coverage(plan.symbol, start, end, result)
            if validation.complete:
                continue
            if end > plan.latest_closed_bucket_end_ms:
                _append_unique(result.skipped_buckets, start)
                continue
            gap_minutes = max(0, (end - start + 1) // 60_000)
            if self.rest_tail_fetcher is None:
                _append_unique(result.skipped_buckets, start)
                message = f"tail fetch unavailable bucket_start_ms={start} reason={validation.reason}"
                result.tail_errors.append(message)
                result.errors.append(message)
                continue
            if gap_minutes > self.max_rest_tail_gap_minutes or target_count > self.max_rest_tail_buckets:
                _append_unique(result.skipped_buckets, start)
                message = (
                    "tail fetch skipped bucket_start_ms="
                    f"{start} gap_minutes={gap_minutes} max_gap_minutes={self.max_rest_tail_gap_minutes} "
                    f"target_count={target_count} max_buckets={self.max_rest_tail_buckets}"
                )
                result.tail_errors.append(message)
                result.errors.append(message)
                continue
            _append_unique(result.tail_fetch_requested_buckets, start)
            try:
                rows = list(self.rest_tail_fetcher(plan.raw_symbol, start, end))
                saved = self.trade_store.save(rows)
                result.imported_trades += saved
                result.tail_fetch_trades_saved += saved
            except sqlite3.OperationalError:
                raise
            except Exception as exc:  # noqa: BLE001 - tail/network errors retry next cycle
                _append_unique(result.skipped_buckets, start)
                _append_unique(result.tail_fetch_failed_buckets, start)
                message = f"tail fetch bucket_start_ms={start}: {type(exc).__name__}: {exc}"
                result.tail_errors.append(message)
                result.errors.append(message)
                continue
            validation = self._validate_bucket_trade_coverage(plan.symbol, start, end, result)
            if validation.complete:
                _append_unique(result.tail_fetch_succeeded_buckets, start)
            else:
                _append_unique(result.skipped_buckets, start)
                _append_unique(result.tail_fetch_failed_buckets, start)
                message = f"tail fetch incomplete bucket_start_ms={start} reason={validation.reason}"
                result.tail_errors.append(message)
                result.errors.append(message)

    def _import_zip(self, *, path: Path, plan: BackfillPlan, bucket_start_ms: int, bucket_end_ms: int) -> int:
        saved = 0
        for chunk in self.archive.iter_daily_trades_zip(  # type: ignore[union-attr]
            path,
            raw_symbol=plan.raw_symbol,
            symbol=plan.symbol,
            chunksize=self.chunksize,
        ):
            rows = [
                trade
                for trade in chunk
                if (ts := _trade_time_ms(trade)) is not None and bucket_start_ms <= ts <= bucket_end_ms
            ]
            saved += self.trade_store.save(rows)
        return saved

    def _rebuild_targets(self, *, plan: BackfillPlan, targets: Sequence[int], result: BackfillResult) -> None:
        target_set = set(targets)
        anchor_start, builder = self._builder_from_anchor(plan=plan, first_target=min(targets))
        replay_end = max(targets) + plan.bucket_ms - 1
        trades = self.trade_store.load(symbol=plan.symbol, time_range=TimeRange(anchor_start, replay_end))
        closed: list[RangeBar] = []
        for trade in trades:
            closed.extend(builder.on_trade(trade))
        if closed:
            target_bars = [
                bar
                for bar in closed
                if (bar.end_time_ms // plan.bucket_ms) * plan.bucket_ms in target_set
            ]
            result.range_bars_saved += self.range_bar_store.save(target_bars)

        persisted = self.range_bar_store.load(
            symbol=plan.symbol,
            range_pct=plan.range_pct,
            time_range=TimeRange(min(targets), max(targets) + plan.bucket_ms - 1),
        )
        aggregates = self.aggregator.aggregate(persisted, bucket_ms=plan.bucket_ms)
        by_bucket = {aggregate.bucket_start_ms: aggregate for aggregate in aggregates}
        for bucket_start in sorted(target_set):
            if (
                bucket_start in set(plan.complete_bucket_starts)
                and bucket_start not in set(plan.dirty_bucket_starts)
                and bucket_start not in set(plan.incomplete_coverage_bucket_starts)
            ):
                continue
            bucket_end = bucket_start + plan.bucket_ms - 1
            validation = self._validate_bucket_trade_coverage(plan.symbol, bucket_start, bucket_end, result)
            if not validation.complete:
                _append_unique(result.skipped_buckets, bucket_start)
                continue
            aggregate = by_bucket.get(bucket_start)
            if aggregate is None:
                _append_unique(result.skipped_buckets, bucket_start)
                continue
            self.trade_store.mark_coverage(
                symbol=plan.symbol,
                time_range=TimeRange(bucket_start, bucket_end),
                source="historical",
                coverage_status=RangeCoverageStatus.COMPLETE.value,
            )
            self._upsert_completed_aggregate(
                exchange=plan.exchange,
                aggregate=aggregate,
                completed_at_ms=int(time.time() * 1000),
            )
            self._clear_dirty_bucket(
                exchange=plan.exchange,
                symbol=plan.symbol,
                range_pct=plan.range_pct,
                bucket_start_ms=bucket_start,
            )
            result.aggregates_upserted += 1
            result.processed_buckets += 1

    def _builder_from_anchor(self, *, plan: BackfillPlan, first_target: int) -> tuple[int, RangeBarBuilder]:
        checkpoint = self._load_prior_clean_checkpoint(plan=plan, before_bucket_start=first_target)
        if checkpoint is not None:
            return max(0, (checkpoint.last_trade_ts_ms or checkpoint.bucket_end_ms) + 1), RangeBarBuilder.restore_state(checkpoint.builder_state)
        day_start = _utc_day_start_ms(first_target)
        profile = get_market_profile(plan.symbol)
        contract_value = profile.contract_value(plan.exchange) or Decimal("1")
        builder = RangeBarBuilder(range_pct=plan.range_pct, contract_value=contract_value)
        seed_bars = self.range_bar_store.load(
            symbol=plan.symbol,
            range_pct=plan.range_pct,
            time_range=TimeRange(day_start, max(day_start, first_target - 1)),
        )
        builder.seed_from_bars(seed_bars)
        return day_start, builder

    def _load_prior_clean_checkpoint(self, *, plan: BackfillPlan, before_bucket_start: int) -> RangeBuilderCheckpoint | None:
        with sqlite3.connect(self.checkpoint_db, timeout=max(self.busy_timeout_ms / 1000, 0.0)) as conn:
            conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            row = conn.execute(
                """
                SELECT bucket_start_ms
                FROM range_builder_checkpoints
                WHERE exchange = ? AND symbol = ? AND range_pct = ?
                  AND bucket_start_ms < ? AND coverage_status = ?
                ORDER BY bucket_start_ms DESC
                LIMIT 1
                """,
                (plan.exchange, plan.symbol, _decimal_text(plan.range_pct), before_bucket_start, RangeCoverageStatus.COMPLETE.value),
            ).fetchone()
        if row is None:
            return None
        return self.checkpoint_store.load_checkpoint(
            exchange=plan.exchange,
            symbol=plan.symbol,
            range_pct=plan.range_pct,
            bucket_start_ms=int(row[0]),
        )

    def _upsert_completed_aggregate(self, *, exchange: str, aggregate: RangeBarAggregate, completed_at_ms: int) -> None:
        self.checkpoint_store.save_completed_aggregate(
            exchange=exchange,
            aggregate=aggregate,
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            missing_gap_ms=0,
            completed_at_ms=completed_at_ms,
        )

    def _clear_dirty_bucket(self, *, exchange: str, symbol: str, range_pct: str, bucket_start_ms: int) -> None:
        with sqlite3.connect(self.checkpoint_db, timeout=max(self.busy_timeout_ms / 1000, 0.0)) as conn:
            conn.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
            if not _table_exists(conn, "range_backfill_dirty_buckets"):
                return
            conn.execute(
                "DELETE FROM range_backfill_dirty_buckets WHERE exchange=? AND symbol=? AND range_pct=? AND bucket_start_ms=?",
                (str(exchange).lower(), symbol, _decimal_text(range_pct), bucket_start_ms),
            )

    def _validate_bucket_trade_coverage(self, symbol: str, start: int, end: int, result: BackfillResult):
        validation = validate_trade_coverage(
            trade_store=self.trade_store,
            symbol=symbol,
            bucket_start_ms=start,
            bucket_end_ms=end,
            edge_tolerance_ms=self.edge_tolerance_ms,
            coverage_max_gap_ms=self.coverage_max_gap_ms,
        )
        if validation.complete:
            _append_unique(result.coverage_validated_buckets, start)
        else:
            _append_unique(result.coverage_failed_buckets, start)
        return validation


def _recent_unique(values: Sequence[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _append_unique(values: list[int], value: int) -> None:
    if value not in values:
        values.append(value)


def _utc_day_start_ms(ts_ms: int) -> int:
    day = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).date()
    return int(datetime.combine(day, datetime.min.time(), tzinfo=UTC).timestamp() * 1000)


def _trade_time_ms(trade: MarketTrade) -> int | None:
    return trade.trade_time_ms if trade.trade_time_ms is not None else trade.event_time_ms


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
    return row is not None


def _decimal_text(value: Decimal | str) -> str:
    return format(Decimal(str(value)).normalize(), "f")
