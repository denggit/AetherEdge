from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, MutableMapping, MutableSet, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

from src.market_data.derived import RangeBarAggregator, RangeBarBuilder
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import (
    RangeBar,
    RangeBarAggregate,
    RangeCoverageStatus,
    TimeRange,
)
from src.market_data.range_checkpoint import (
    RangeBuilderCheckpoint,
    RangeCheckpointRecovery,
    RangeCheckpointWriter,
    SqliteRangeCheckpointStore,
    aggregate_snapshot,
)
from src.market_data.storage import SqliteRangeBarStore
from src.platform.data.models import MarketTrade
from src.platform.exchanges.models import ExchangeName
from src.runtime.capabilities import FEATURE_RANGE_BARS, MARKET_TRADES
from src.runtime.features import range_aggregate_feature, range_bar_closed_feature
from src.runtime.market_data.integrity import TradeDataIntegrityTracker
from src.runtime.market_data.range_integrity import (
    DegradedBucketView,
    RangeBucketIntegrityState,
    RepairedBucketView,
)
from src.runtime.module import CapabilityId, ModuleHealth, ModuleState
from src.utils.log import get_logger

if TYPE_CHECKING:
    from src.runtime.market_data.range_background import RangeBackgroundServices
    from src.runtime.market_data.range_repair_journal import RangeRepairJournalSession
    from src.runtime.market_data.range_speed_runtime import RangeSpeedWarmup
    from src.runtime.range_repair_bootstrap import RangeRepairBootstrapService


logger = get_logger(__name__)
FeaturePublisher = Callable[[MarketFeatureEvent], Awaitable[None]]
ClockMs = Callable[[], int]
ErrorReporter = Callable[[str, BaseException], None]
BarErrorReporter = Callable[[RangeBar, BaseException], None]
AggregateErrorReporter = Callable[[RangeBarAggregate, BaseException], None]


class RangeBarBuilderPort(Protocol):
    def on_trade(self, trade: MarketTrade) -> Sequence[RangeBar]: ...

    def snapshot_state(self) -> dict[str, object]: ...

    def discard_active_bar(self) -> None: ...


class RangeBarStorePort(Protocol):
    def load(
        self,
        *,
        symbol: str,
        range_pct: str,
        time_range: TimeRange,
    ) -> list[RangeBar]: ...


class RangeBarPersistence(Protocol):
    def persist_range_bar(
        self,
        bar: RangeBar,
        *,
        on_error: Callable[[BaseException], None] | None,
        on_rejected: Callable[[str], None] | None = None,
    ) -> bool: ...

    def persist_completed_range_aggregate(
        self,
        aggregate: RangeBarAggregate,
        *,
        coverage_status: str,
        missing_gap_ms: int,
        on_error: Callable[[BaseException], None] | None,
        on_rejected: Callable[[str], None] | None = None,
    ) -> bool: ...


@dataclass(frozen=True)
class RangeBarModuleConfig:
    symbol: str
    exchange: ExchangeName
    range_pct: Decimal
    contract_value: Decimal
    bucket_interval_ms: int
    aggregate_interval: str
    min_bars: int = 1
    checkpoint_db_path: str = (
        "data/state/range_builder_checkpoint.sqlite3"
    )
    checkpoint_interval_ms: int = 1_000
    checkpoint_every_closed_bars: int = 10
    checkpoint_writer_max_pending: int = 8
    checkpoint_max_age_for_recovered_minor_ms: int = 60_000
    checkpoint_max_age_for_restore_ms: int = 300_000
    retained_closed_buckets: int = 3

    def __post_init__(self) -> None:
        if self.bucket_interval_ms <= 0:
            raise ValueError("bucket interval must be positive")
        if self.range_pct <= 0:
            raise ValueError("range pct must be positive")
        if self.contract_value <= 0:
            raise ValueError("contract value must be positive")
        if self.min_bars < 0:
            raise ValueError("minimum range bars cannot be negative")


class RangeBarModule:
    """Own Range building, recovery, checkpointing and aggregate state."""

    module_id = "range-bars"
    provides = frozenset({FEATURE_RANGE_BARS})
    requires = frozenset({MARKET_TRADES})
    dispatch_priority = 400

    def __init__(
        self,
        *,
        config: RangeBarModuleConfig,
        publish: FeaturePublisher | None = None,
        persistence: RangeBarPersistence | None = None,
        builder: RangeBarBuilderPort | None = None,
        bar_store: RangeBarStorePort | None = None,
        aggregator: RangeBarAggregator | None = None,
        checkpoint_store: SqliteRangeCheckpointStore | None = None,
        checkpoint_writer: RangeCheckpointWriter | None = None,
        clock_ms: ClockMs | None = None,
        on_error: ErrorReporter | None = None,
        on_bar_persist_error: BarErrorReporter | None = None,
        on_aggregate_persist_error: AggregateErrorReporter | None = None,
        on_rejected: Callable[[str], None] | None = None,
        integrity: TradeDataIntegrityTracker | None = None,
    ) -> None:
        self.config = config
        self._publish = publish
        self._persistence = persistence
        self._builder = builder
        self._bar_store = bar_store
        self._aggregator = aggregator
        self._checkpoint_store = checkpoint_store
        self._checkpoint_writer = checkpoint_writer
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._on_error = on_error
        self._on_bar_persist_error = on_bar_persist_error
        self._on_aggregate_persist_error = on_aggregate_persist_error
        self._on_rejected = on_rejected
        self._integrity = integrity or TradeDataIntegrityTracker()
        self._integrity_revision = self._integrity.revision
        self._state = ModuleState.CREATED
        self._error: BaseException | None = None
        self._bars_by_bucket: dict[int, list[RangeBar]] = {}
        self._emitted_aggregate_buckets: set[tuple[str, str, int]] = set()
        self._bucket_states: dict[int, RangeBucketIntegrityState] = {}
        self._degraded_bucket_view = DegradedBucketView(self)
        self._repaired_bucket_view = RepairedBucketView(self)
        self._repair_started_revision_by_bucket: dict[int, int] = {}
        self._initial_bucket_ms: int | None = None
        self._initial_recovery: RangeCheckpointRecovery | None = None
        self._trust_start_bucket_ms: int | None = None
        self._builder_reset_at_bucket_ms: int | None = None
        self._last_checkpoint_submit_ms = 0
        self._bars_since_checkpoint = 0
        self._snapshot_warning_emitted = False
        self._background: RangeBackgroundServices | None = None
        self._repair_journal: RangeRepairJournalSession | None = None
        self._speed_warmup: RangeSpeedWarmup | None = None
        self._stop_event: asyncio.Event | None = None
        self._repair_bootstrap: Callable[[], RangeRepairBootstrapService] | None = None
        self.bars_closed = self.builder_bars_closed = 0
        self.aggregates_created = 0
        self.bars_suppressed = 0

    async def prepare(self) -> None:
        self._ensure_resources()
        self._state = ModuleState.PREPARED

    async def warmup(self) -> None:
        self.initialize_recovery()
        if self._speed_warmup is not None:
            await self._speed_warmup.warmup()

    async def repair(self) -> None:
        self.repair_now()

    def initialize_recovery(self) -> RangeCheckpointRecovery:
        self._ensure_resources()
        now_ms = self._clock_ms()
        bucket = self._bucket_start(now_ms)
        self._initial_bucket_ms = bucket
        recovery = self.checkpoint_store.recover_current_bucket(
            exchange=self.config.exchange.value,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            bucket_start_ms=bucket,
            now_ms=now_ms,
            max_age_for_recovered_minor_ms=(
                self.config.checkpoint_max_age_for_recovered_minor_ms
            ),
            max_age_for_restore_ms=(
                self.config.checkpoint_max_age_for_restore_ms
            ),
        )
        self._bars_by_bucket[bucket] = self._load_store_rows(bucket)
        if recovery.checkpoint is not None:
            try:
                self._builder = RangeBarBuilder.restore_state(
                    recovery.checkpoint.builder_state
                )
            except (KeyError, TypeError, ValueError) as exc:
                self._report("range checkpoint restore failed", exc)
                recovery = RangeCheckpointRecovery(
                    coverage_status=(
                        RangeCoverageStatus.RECOVERED_INCOMPLETE.value
                    ),
                    checkpoint=None,
                    checkpoint_age_ms=recovery.checkpoint_age_ms,
                    missing_gap_ms=recovery.missing_gap_ms,
                    recovered_from_checkpoint=False,
                )
        self._initial_recovery = recovery
        self._builder_reset_at_bucket_ms = bucket + self.config.bucket_interval_ms
        self._trust_start_bucket_ms = (
            bucket
            if recovery.coverage_status
            == RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value
            else bucket + self.config.bucket_interval_ms
        )
        return recovery

    async def start(self) -> None:
        self.checkpoint_writer.start()
        if self._background is not None and self._stop_event is not None:
            self._background.start(self._stop_event)
        self._state = ModuleState.RUNNING

    async def stop(self) -> None:
        if self._background is not None:
            await self._background.stop()
        if self._repair_journal is not None:
            await self._repair_journal.stop()
        writer = self._checkpoint_writer
        if writer is not None:
            stop = getattr(writer, "stop", None)
            if callable(stop):
                await asyncio.to_thread(stop, flush=True)
        self._state = ModuleState.STOPPED

    def configure_support(
        self,
        *,
        background: RangeBackgroundServices,
        repair_journal: RangeRepairJournalSession,
        speed_warmup: RangeSpeedWarmup,
        stop_event: asyncio.Event,
        repair_bootstrap: Callable[[], RangeRepairBootstrapService],
    ) -> None:
        self._background = background
        self._repair_journal = repair_journal
        self._speed_warmup = speed_warmup
        self._stop_event = stop_event
        self._repair_bootstrap = repair_bootstrap

    def configure_integrity(
        self,
        tracker: TradeDataIntegrityTracker,
    ) -> None:
        self._integrity = tracker
        self._integrity_revision = tracker.revision

    def repair_now(self) -> None:
        if (
            self._repair_bootstrap is None
            or self._repair_journal is None
            or self._initial_recovery is None
        ):
            return
        if self._initial_bucket_ms is not None:
            self._repair_started_revision_by_bucket[
                self._initial_bucket_ms
            ] = self._integrity.revision
        result = self._repair_bootstrap().start_if_needed(
            self._initial_recovery,
            initial_bucket_ms=self._initial_bucket_ms,
        )
        self._repair_journal.activate(result)
        if self._background is not None:
            self._background.micro_repair_supervisor = (
                result.micro_repair_supervisor
            )

    @property
    def background(self) -> RangeBackgroundServices:
        if self._background is None:
            raise RuntimeError("Range background services are not configured")
        return self._background

    @property
    def repair_journal(self) -> RangeRepairJournalSession:
        if self._repair_journal is None:
            raise RuntimeError("Range repair journal is not configured")
        return self._repair_journal

    @property
    def speed_warmup(self) -> RangeSpeedWarmup:
        if self._speed_warmup is None:
            raise RuntimeError("Range speed warmup is not configured")
        return self._speed_warmup

    def health(self) -> ModuleHealth:
        return ModuleHealth(
            module_id=self.module_id,
            state=self._state,
            healthy=self._error is None,
            detail=(
                None
                if self._error is None
                else f"{type(self._error).__name__}: {self._error}"
            ),
            metadata=(
                ("bars_closed", str(self.bars_closed)),
                ("aggregates_created", str(self.aggregates_created)),
                ("cached_buckets", str(len(self._bars_by_bucket))),
                ("events_dropped", "0"),
                (
                    "data_complete",
                    str(not any(not state.complete for state in self._bucket_states.values())).lower(),
                ),
                ("bars_suppressed", str(self.bars_suppressed)),
            ),
        )

    async def process_trade(self, trade: MarketTrade) -> None:
        builder = self.builder
        trade_time_ms = _trade_time_ms(trade)
        if self._integrity.revision != self._integrity_revision:
            issues = self._integrity.issues_since(self._integrity_revision)
            if issues:
                builder.discard_active_bar()
                for issue in issues:
                    self.mark_degraded(
                        bucket_start_ms=self._bucket_start(issue.event_time_ms),
                        reason=issue.reason,
                        revision=issue.revision,
                    )
            self._integrity_revision = self._integrity.revision
        reset_at = self._builder_reset_at_bucket_ms
        if trade_time_ms is not None and reset_at is not None and trade_time_ms >= reset_at:
            builder.discard_active_bar()
            self._builder_reset_at_bucket_ms = None
        for bar in builder.on_trade(trade):
            self.builder_bars_closed += 1
            bucket = self._bucket_start(bar.end_time_ms)
            invalid_reason = self._integrity.invalid_reason(
                int(bar.start_time_ms),
                int(bar.end_time_ms),
            ) or self.degraded_reason(bucket)
            if invalid_reason is not None:
                self.mark_degraded(
                    bucket_start_ms=bucket,
                    reason=invalid_reason,
                )
                self.bars_suppressed += 1
                continue
            # -- bar is trusted from this point --
            self._bars_by_bucket.setdefault(bucket, []).append(bar)
            self._bars_since_checkpoint += 1
            self.bars_closed += 1
            self._prune(current_bucket=bucket)
            if self._publish is not None:
                await self._publish(range_bar_closed_feature(bar, exchange=trade.exchange))
            if self._persistence is not None:
                self._persistence.persist_range_bar(
                    bar,
                    on_error=lambda exc, value=bar: self._report_bar_error(
                        value, exc
                    ),
                    on_rejected=self._on_rejected,
                )
        self.submit_checkpoint_if_due(trade)

    def _bucket_is_degraded(self, bucket_start_ms: int) -> bool:
        state = self._bucket_states.get(bucket_start_ms)
        return state is not None and not state.complete

    def aggregates_for_bucket(self, bucket_start_ms: int) -> list[RangeBarAggregate]:
        if self._bucket_is_degraded(bucket_start_ms):
            return []
        rows = self.rows_for_bucket(bucket_start_ms)
        if not rows:
            return []
        return [
            aggregate
            for aggregate in self.aggregator.aggregate(
                rows,
                bucket_ms=self.config.bucket_interval_ms,
            )
            if aggregate.bucket_start_ms == bucket_start_ms
        ]

    def build_aggregate_events(self, bucket_start_ms: int) -> list[MarketFeatureEvent]:
        events: list[MarketFeatureEvent] = []
        for aggregate in self.aggregates_for_bucket(bucket_start_ms):
            if aggregate.bar_count < self.config.min_bars:
                continue
            events.append(self._aggregate_event(aggregate))
            self._persist_aggregate(aggregate)
        return events

    async def emit_aggregate_for_bucket(
        self,
        bucket_start_ms: int,
    ) -> list[MarketFeatureEvent]:
        return await self.emit_aggregates(
            self.aggregates_for_bucket(bucket_start_ms)
        )

    async def emit_aggregates(
        self,
        aggregates: Sequence[RangeBarAggregate],
    ) -> list[MarketFeatureEvent]:
        events: list[MarketFeatureEvent] = []
        for aggregate in aggregates:
            key = (
                aggregate.symbol,
                str(aggregate.range_pct),
                int(aggregate.bucket_start_ms),
            )
            if key in self._emitted_aggregate_buckets:
                continue
            event = self._aggregate_event(aggregate)
            await self._publish(event)
            self._emitted_aggregate_buckets.add(key)
            events.append(event)
            self.aggregates_created += 1
            self._persist_aggregate(aggregate)
        return events

    def rows_for_bucket(self, bucket_start_ms: int) -> list[RangeBar]:
        # Never return rows for degraded buckets that haven't been repaired.
        if self._bucket_is_degraded(bucket_start_ms):
            return []
        state = self._bucket_states.get(bucket_start_ms)
        if state is not None and state.complete and state.repaired_through_revision > 0:
            rows = self._load_store_rows(bucket_start_ms)
            if rows:
                self._bars_by_bucket[bucket_start_ms] = rows
                self._prune(current_bucket=bucket_start_ms)
            return rows
        if bucket_start_ms in self._bars_by_bucket:
            memory_rows = list(self._bars_by_bucket[bucket_start_ms])
            if memory_rows or not self._store_fallback_allowed(bucket_start_ms):
                return memory_rows
        rows = self._load_store_rows(bucket_start_ms)
        if rows:
            self._bars_by_bucket[bucket_start_ms] = rows
            self._prune(current_bucket=bucket_start_ms)
        return rows

    def coverage(self, bucket_start_ms: int) -> RangeCheckpointRecovery:
        state = self._bucket_states.get(bucket_start_ms)
        if state is not None and state.complete and state.repaired_through_revision > 0:
            return RangeCheckpointRecovery(
                coverage_status=RangeCoverageStatus.COMPLETE.value,
                checkpoint=None,
                checkpoint_age_ms=None,
                missing_gap_ms=0,
                recovered_from_checkpoint=True,
            )
        if bucket_start_ms == self._initial_bucket_ms and self._initial_recovery is not None:
            return self._initial_recovery
        if self._bucket_is_degraded(bucket_start_ms):
            return RangeCheckpointRecovery(
                coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
                checkpoint=None,
                checkpoint_age_ms=None,
                missing_gap_ms=0,
                recovered_from_checkpoint=False,
            )
        return RangeCheckpointRecovery(
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            checkpoint=None,
            checkpoint_age_ms=None,
            missing_gap_ms=0,
            recovered_from_checkpoint=False,
        )

    def _bucket_state(self, bucket_start_ms: int) -> RangeBucketIntegrityState:
        if bucket_start_ms not in self._bucket_states:
            self._bucket_states[bucket_start_ms] = RangeBucketIntegrityState()
        return self._bucket_states[bucket_start_ms]

    def mark_degraded(
        self,
        *,
        bucket_start_ms: int,
        reason: str,
        revision: int | None = None,
    ) -> None:
        state = self._bucket_state(bucket_start_ms)
        state.reason = reason
        observed_revision = self._integrity.revision if revision is None else int(revision)
        if state.complete:
            state.last_issue_revision = max(
                state.last_issue_revision + 1,
                state.repaired_through_revision + 1,
                observed_revision,
            )
        else:
            state.last_issue_revision = max(
                state.last_issue_revision,
                observed_revision,
            )

    def degraded_reason(self, bucket_start_ms: int) -> str | None:
        state = self._bucket_states.get(bucket_start_ms)
        return None if state is None or state.complete else state.reason

    @property
    def trust_start_bucket_ms(self) -> int | None:
        return self._trust_start_bucket_ms

    @trust_start_bucket_ms.setter
    def trust_start_bucket_ms(self, value: int | None) -> None:
        self._trust_start_bucket_ms = value

    @property
    def initial_bucket_ms(self) -> int | None:
        return self._initial_bucket_ms

    @initial_bucket_ms.setter
    def initial_bucket_ms(self, value: int | None) -> None:
        self._initial_bucket_ms = value

    @property
    def initial_recovery(self) -> RangeCheckpointRecovery | None:
        return self._initial_recovery

    @initial_recovery.setter
    def initial_recovery(self, value: RangeCheckpointRecovery | None) -> None:
        self._initial_recovery = value

    @property
    def bars_by_bucket(self) -> dict[int, list[RangeBar]]:
        return self._bars_by_bucket

    @property
    def degraded_buckets(self) -> MutableMapping[int, str]:
        return self._degraded_bucket_view

    @property
    def repaired_complete_buckets(self) -> MutableSet[int]:
        return self._repaired_bucket_view

    @property
    def last_checkpoint_submit_ms(self) -> int:
        return self._last_checkpoint_submit_ms

    @last_checkpoint_submit_ms.setter
    def last_checkpoint_submit_ms(self, value: int) -> None:
        self._last_checkpoint_submit_ms = int(value)

    @property
    def bars_since_checkpoint(self) -> int:
        return self._bars_since_checkpoint

    @bars_since_checkpoint.setter
    def bars_since_checkpoint(self, value: int) -> None:
        self._bars_since_checkpoint = int(value)

    def adopt_repaired_coverage(self, bucket_start_ms: int) -> bool:
        if (
            self._initial_bucket_ms != bucket_start_ms
            or self._initial_recovery is None
            or self._initial_recovery.coverage_status
            == RangeCoverageStatus.COMPLETE.value
        ):
            return False
        completed = self.checkpoint_store.load_completed_aggregate(
            exchange=self.config.exchange.value,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            bucket_end_ms=(
                bucket_start_ms + self.config.bucket_interval_ms - 1
            ),
        )
        if (
            completed is None
            or completed.coverage_status
            != RangeCoverageStatus.COMPLETE.value
        ):
            return False
        state = self._bucket_state(bucket_start_ms)
        state.last_issue_revision = max(state.last_issue_revision, 1)
        repair_started_revision = self._repair_started_revision_by_bucket.pop(
            bucket_start_ms,
            self._integrity.revision,
        )
        through_rev = repair_started_revision
        if self._integrity.revision == repair_started_revision:
            through_rev = max(through_rev, state.last_issue_revision)
        state.repaired_through_revision = max(
            state.repaired_through_revision,
            through_rev,
        )
        self._integrity.mark_repaired(
            bucket_start_ms,
            bucket_start_ms + self.config.bucket_interval_ms - 1,
            through_revision=through_rev,
        )
        if not state.complete:
            return False
        state.reason = None
        self._initial_recovery = RangeCheckpointRecovery(
            coverage_status=RangeCoverageStatus.COMPLETE.value,
            checkpoint=None,
            checkpoint_age_ms=None,
            missing_gap_ms=0,
            recovered_from_checkpoint=True,
        )
        self._trust_start_bucket_ms = bucket_start_ms
        self._bars_by_bucket.pop(bucket_start_ms, None)
        return True

    def submit_checkpoint_if_due(self, trade: MarketTrade) -> bool:
        now_ms = self._clock_ms()
        interval_due = (
            now_ms - self._last_checkpoint_submit_ms
            >= self.config.checkpoint_interval_ms
        )
        bars_due = (
            self._bars_since_checkpoint
            >= self.config.checkpoint_every_closed_bars
        )
        if not interval_due and not bars_due:
            return False
        snapshot = getattr(self.builder, "snapshot_state", None)
        if not callable(snapshot):
            self._snapshot_warning_emitted = True
            return False
        trade_time_ms = _trade_time_ms(trade)
        if trade_time_ms is None:
            return False
        bucket = self._bucket_start(trade_time_ms)
        # Never persist a normal checkpoint for a degraded, unrepaired bucket.
        if self._bucket_is_degraded(bucket):
            return False
        bars = self.rows_for_bucket(bucket)
        aggregate = next(iter(self.aggregates_for_bucket(bucket)), None)
        coverage = self.coverage(bucket)
        accepted = self.checkpoint_writer.submit(
            RangeBuilderCheckpoint(
                exchange=trade.exchange.value,
                symbol=trade.symbol,
                range_pct=str(self.config.range_pct),
                bucket_start_ms=bucket,
                bucket_end_ms=bucket + self.config.bucket_interval_ms - 1,
                last_trade_id=trade.trade_id,
                last_trade_ts_ms=trade_time_ms,
                last_ws_recv_ts_ms=now_ms,
                range_bar_count=len(bars),
                aggregate=aggregate_snapshot(aggregate),
                builder_state=dict(snapshot()),
                coverage_status=coverage.coverage_status,
                missing_gap_ms=coverage.missing_gap_ms,
                checkpoint_updated_at_ms=now_ms,
            )
        )
        if accepted:
            self._last_checkpoint_submit_ms = now_ms
            self._bars_since_checkpoint = 0
        self._prune(current_bucket=bucket)
        return accepted

    def prune(self, *, current_bucket: int) -> None:
        self._prune(current_bucket=current_bucket)

    @property
    def builder(self) -> RangeBarBuilderPort:
        self._ensure_resources()
        assert self._builder is not None
        return self._builder

    @builder.setter
    def builder(self, value: RangeBarBuilderPort | None) -> None:
        self._builder = value

    @property
    def aggregator(self) -> RangeBarAggregator:
        self._ensure_resources()
        assert self._aggregator is not None
        return self._aggregator

    @aggregator.setter
    def aggregator(self, value: RangeBarAggregator | None) -> None:
        self._aggregator = value

    @property
    def checkpoint_store(self) -> SqliteRangeCheckpointStore:
        self._ensure_resources()
        assert self._checkpoint_store is not None
        return self._checkpoint_store

    @property
    def checkpoint_writer(self) -> RangeCheckpointWriter:
        self._ensure_resources()
        assert self._checkpoint_writer is not None
        return self._checkpoint_writer

    @property
    def bar_store(self) -> RangeBarStorePort:
        self._ensure_resources()
        assert self._bar_store is not None
        return self._bar_store

    @bar_store.setter
    def bar_store(self, value: RangeBarStorePort | None) -> None:
        self._bar_store = value
    def _ensure_resources(self) -> None:
        if self._builder is None:
            self._builder = RangeBarBuilder(
                range_pct=self.config.range_pct,
                contract_value=self.config.contract_value,
            )
        if self._bar_store is None:
            self._bar_store = SqliteRangeBarStore()
        if self._aggregator is None:
            self._aggregator = RangeBarAggregator()
        if self._checkpoint_store is None:
            self._checkpoint_store = SqliteRangeCheckpointStore(
                self.config.checkpoint_db_path
            )
        if self._checkpoint_writer is None:
            assert self._checkpoint_store is not None
            self._checkpoint_writer = RangeCheckpointWriter(
                self._checkpoint_store,
                max_pending=self.config.checkpoint_writer_max_pending,
                on_error=lambda exc: self._report(
                    "range checkpoint write failed", exc
                ),
            )

    def _aggregate_event(self, aggregate: RangeBarAggregate) -> MarketFeatureEvent:
        coverage = self.coverage(aggregate.bucket_start_ms)
        return range_aggregate_feature(
            aggregate,
            exchange=self.config.exchange,
            timeframe=self.config.aggregate_interval,
            coverage_status=coverage.coverage_status,
            missing_gap_ms=coverage.missing_gap_ms,
            range_recovered_from_checkpoint=coverage.recovered_from_checkpoint,
            range_checkpoint_age_ms=coverage.checkpoint_age_ms,
        )

    def _persist_aggregate(self, aggregate: RangeBarAggregate) -> None:
        coverage = self.coverage(aggregate.bucket_start_ms)
        self._persistence.persist_completed_range_aggregate(
            aggregate,
            coverage_status=coverage.coverage_status,
            missing_gap_ms=coverage.missing_gap_ms,
            on_error=lambda exc, value=aggregate: (
                self._on_aggregate_persist_error(value, exc)
                if self._on_aggregate_persist_error is not None
                else self._report("range aggregate persistence failed", exc)
            ),
            on_rejected=self._on_rejected,
        )

    def _load_store_rows(self, bucket_start_ms: int) -> list[RangeBar]:
        self._ensure_resources()
        assert self._bar_store is not None
        return list(
            self._bar_store.load(
                symbol=self.config.symbol,
                range_pct=str(self.config.range_pct),
                time_range=TimeRange(
                    bucket_start_ms,
                    bucket_start_ms + self.config.bucket_interval_ms - 1,
                ),
            )
        )

    def _store_fallback_allowed(self, bucket_start_ms: int) -> bool:
        return (
            bucket_start_ms not in self._bars_by_bucket
            or (
                bucket_start_ms == self._initial_bucket_ms
                and self._initial_recovery is not None
            )
        )

    def _prune(self, *, current_bucket: int) -> None:
        keep = max(1, self.config.retained_closed_buckets)
        keys = sorted(self._bars_by_bucket, reverse=True)
        if len(keys) <= keep + 1:
            return
        latest = keys[0]
        threshold = latest - keep * self.config.bucket_interval_ms
        for key in (value for value in keys if value < threshold and value < current_bucket):
            del self._bars_by_bucket[key]

    def _bucket_start(self, time_ms: int) -> int:
        return (time_ms // self.config.bucket_interval_ms) * self.config.bucket_interval_ms

    def _handle_dispatch_error(self, _module_id: str, exc: BaseException) -> None:
        self._error = exc
        self._state = ModuleState.ERROR
        self._report("range trade dispatch failed", exc)

    def _report(self, message: str, exc: BaseException) -> None:
        logger.warning("%s | error=%s", message, exc)
        if self._on_error is not None:
            self._on_error(message, exc)

    def _report_bar_error(self, bar: RangeBar, exc: BaseException) -> None:
        if self._on_bar_persist_error is not None:
            self._on_bar_persist_error(bar, exc)
            return
        self._report(f"range bar persistence failed: {bar.bar_id}", exc)


def _trade_time_ms(trade: MarketTrade) -> int | None:
    return trade.trade_time_ms or trade.event_time_ms


__all__ = [
    "RangeBarBuilderPort",
    "RangeBarModule",
    "RangeBarModuleConfig",
    "RangeBarPersistence",
    "RangeBarStorePort",
    "RangeBucketIntegrityState",
]
