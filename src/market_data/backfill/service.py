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
from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.models import RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange
from src.market_data.range_checkpoint import RangeBuilderCheckpoint, SqliteRangeCheckpointStore
from src.market_data.storage import SqliteRangeBarStore, SqliteTradeStore
from src.platform.data.models import MarketTrade
from src.platform.exchanges.okx.historical_archive import OkxHistoricalArchive, daily_trades_zip_path
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

    def process_plan(self, plan: BackfillPlan, *, max_buckets: int = 1, now: datetime | None = None) -> BackfillResult:
        result = BackfillResult()
        targets = _recent_unique(
            [
                *plan.missing_bucket_starts,
                *plan.dirty_bucket_starts,
                *plan.incomplete_coverage_bucket_starts,
            ]
        )[: max(0, int(max_buckets))]
        if not targets:
            return result

        try:
            self._ensure_raw_trades(plan=plan, targets=targets, result=result, now=now or datetime.now(UTC))
            rebuild_targets = [target for target in targets if target not in set(result.skipped_buckets)]
            if rebuild_targets:
                self._rebuild_targets(plan=plan, targets=rebuild_targets, result=result)
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                result.locked = True
                result.errors.append(str(exc))
                return result
            raise
        return result

    def _ensure_raw_trades(self, *, plan: BackfillPlan, targets: Sequence[int], result: BackfillResult, now: datetime) -> None:
        for start in sorted(set(targets)):
            end = start + plan.bucket_ms - 1
            day = datetime.fromtimestamp(start / 1000, tz=UTC).date()
            current_day = now.astimezone(UTC).date()
            if day < current_day:
                before = daily_trades_zip_path(self.raw_root, plan.raw_symbol, day)
                existed_before = before.exists()
                meta = self.archive.ensure_daily_trades_zip(  # type: ignore[union-attr]
                    raw_root=self.raw_root,
                    raw_symbol=plan.raw_symbol,
                    day=day,
                    now=now,
                )
                if meta is None:
                    result.skipped_buckets.append(start)
                    continue
                if not existed_before:
                    result.downloaded_days += 1
                    if self.download_sleep_seconds > 0:
                        time.sleep(self.download_sleep_seconds)
                result.imported_trades += self._import_zip(path=Path(meta.path), plan=plan, bucket_start_ms=start, bucket_end_ms=end)
                continue

            if not self._bucket_has_trade_coverage(plan.symbol, start, end):
                gap_minutes = max(0, (end - start + 1) // 60_000)
                if (
                    self.rest_tail_fetcher is not None
                    and gap_minutes <= self.max_rest_tail_gap_minutes
                    and len(targets) <= self.max_rest_tail_buckets
                ):
                    rows = list(self.rest_tail_fetcher(plan.raw_symbol, start, end))
                    result.imported_trades += self.trade_store.save(rows)
                else:
                    result.skipped_buckets.append(start)

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
            bucket_end = bucket_start + plan.bucket_ms - 1
            validation = validate_trade_coverage(
                trade_store=self.trade_store,
                symbol=plan.symbol,
                bucket_start_ms=bucket_start,
                bucket_end_ms=bucket_end,
                edge_tolerance_ms=self.edge_tolerance_ms,
                coverage_max_gap_ms=self.coverage_max_gap_ms,
            )
            if not validation.complete:
                result.skipped_buckets.append(bucket_start)
                continue
            aggregate = by_bucket.get(bucket_start)
            if aggregate is None:
                result.skipped_buckets.append(bucket_start)
                continue
            self.trade_store.mark_coverage(
                symbol=plan.symbol,
                time_range=TimeRange(bucket_start, bucket_end),
                source="historical",
            )
            self._upsert_completed_aggregate(
                exchange=plan.exchange,
                aggregate=aggregate,
                completed_at_ms=int(time.time() * 1000),
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

    def _bucket_has_trade_coverage(self, symbol: str, start: int, end: int) -> bool:
        return any(
            item.start_time_ms <= start and item.end_time_ms >= end
            for item in self.trade_store.coverage_ranges(symbol=symbol, time_range=TimeRange(start, end))
        )


def _recent_unique(values: Sequence[int]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _utc_day_start_ms(ts_ms: int) -> int:
    day = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).date()
    return int(datetime.combine(day, datetime.min.time(), tzinfo=UTC).timestamp() * 1000)


def _trade_time_ms(trade: MarketTrade) -> int | None:
    return trade.trade_time_ms if trade.trade_time_ms is not None else trade.event_time_ms


def _decimal_text(value: Decimal | str) -> str:
    return format(Decimal(str(value)).normalize(), "f")
