from __future__ import annotations

import time
from typing import Any, Callable, Mapping, Sequence
from src.app.alerts import AppAlert
from src.market_data.events import MarketFeatureEvent
from src.market_data.models import MarketDataSet, RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange, WarmupRequest
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.runtime.features import closed_kline_feature, range_aggregate_unavailable_feature
from src.runtime.strategy_diagnostics import log_closed_bar_decision
from src.strategy.ports import (
    RangeSpeedHistoryProvider,
    StrategyDecisionAuditProvider,
    StrategyPendingWorkProvider,
    StrategyRecoveryStatus,
    StrategyRecoveryStatusProvider,
    StrategyStartupPreviewProvider,
    StrategyStopAdoptionProvider,
)

from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent


class ClosedBarComponent(RuntimeComponent):
    def _log_4h_decision_summary(self, *, open_time_ms: int, closed_kline: MarketKline) -> None:
        strategy = self.context.strategy
        declared = any(
            "decision_audit" in cls.__dict__
            for cls in type(strategy).__mro__
        )
        audit = (
            strategy.decision_audit()
            if declared and isinstance(strategy, StrategyDecisionAuditProvider)
            else None
        )

        log_closed_bar_decision(
            audit=audit,
            symbol=self.app_config.symbol,
            interval=self._closed_bar_interval,
            close_buffer_ms=self._closed_bar_buffer_ms,
            open_time_ms=open_time_ms,
            closed_kline=closed_kline,
        )

    async def poll_closed_bar_once(
        self,
        *,
        now_ms: int | None = None,
        _health_prechecked: bool = False,
    ) -> list[MarketFeatureEvent]:
        if not _health_prechecked:
            self._raise_on_unhealthy_market_data()
            self._raise_on_unhealthy_producer()
        now = int(time.time() * 1000) if now_ms is None else now_ms
        due = await self._fetch_due_closed_kline(now)
        if due is None:
            return []
        open_time_ms, closed_kline = due
        if not await self._drain_before_closed_bar(
            open_time_ms,
            closed_kline,
        ):
            return []
        features = await self._emit_closed_kline_feature(
            open_time_ms,
            closed_kline,
            finalized_at_ms=now,
        )
        if not self.requirements.range_bars.enabled:
            return self._finish_closed_bar_decision(
                open_time_ms,
                closed_kline,
                features,
            )
        features.extend(
            await self._closed_bar_range_features(
                open_time_ms,
                closed_kline,
            )
        )
        return self._finish_closed_bar_decision(
            open_time_ms,
            closed_kline,
            features,
        )

    async def _fetch_due_closed_kline(
        self,
        now_ms: int,
    ) -> tuple[int, MarketKline] | None:
        open_time_ms = self._closed_bar_scheduler.due_closed_bar(now_ms)
        if open_time_ms is None:
            return None
        rows = await self.context.data.fetch_klines(
            interval=self._closed_bar_interval,
            limit=10,
            start_time_ms=open_time_ms,
            end_time_ms=open_time_ms,
            use_cache=False,
            oldest_first=True,
        )
        closed_rows = [
            row
            for row in rows
            if row.is_closed and row.open_time_ms == open_time_ms
        ]
        if not closed_rows:
            self._alert_missing_closed_bar(open_time_ms, now_ms)
            return None
        return open_time_ms, closed_rows[-1]

    def _alert_missing_closed_bar(
        self,
        open_time_ms: int,
        now_ms: int,
    ) -> None:
        should_alert = getattr(
            self._closed_bar_scheduler,
            "should_alert_missing",
            None,
        )
        if not callable(should_alert) or not should_alert(open_time_ms, now_ms):
            return
        close_time_ms = open_time_ms + self._closed_bar_interval_ms
        self.context.alerts.emit(
            AppAlert(
                subject="AetherEdge closed bar missing",
                severity="error",
                content=(
                    f"symbol={self.app_config.symbol}\n"
                    f"interval={self._closed_bar_interval}\n"
                    f"open_time_ms={open_time_ms}\n"
                    f"close_time_ms={close_time_ms}\n"
                    f"now_ms={now_ms}\n"
                    f"missing_after_ms={self._closed_bar_missing_alert_after_ms}\n"
                ),
            )
        )
        logger.error(
            "Closed bar missing after retry window | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s now_ms=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            open_time_ms,
            close_time_ms,
            now_ms,
        )

    async def _drain_before_closed_bar(
        self,
        open_time_ms: int,
        closed_kline: MarketKline,
    ) -> bool:
        try:
            result = await self._drain_market_events_before_closed_bar(
                closed_bar_close_time_ms=closed_kline.close_time_ms,
            )
        except Exception as exc:
            self._mark_range_context_degraded_bucket(
                bucket_start_ms=open_time_ms,
                reason="market_data_barrier_failed",
                event_time_ms=closed_kline.close_time_ms,
            )
            self.market_state.integrity_error = exc
            self._alert_closed_bar_integrity_failure(
                closed_kline=closed_kline,
                reason=f"{type(exc).__name__}: {exc}",
                result=MarketQueueDrainResult(
                    processed=0,
                    deferred=0,
                    examined=0,
                    queue_size_before=self._market_queue.qsize(),
                    queue_size_after=self._market_queue.qsize(),
                    duration_ms=0,
                    hit_event_limit=False,
                    hit_time_limit=False,
                    pipeline_completed=False,
                    pipeline_pending=0,
                ),
            )
            raise
        tracker = self._trade_integrity_tracker()
        invalid_reason = (
            None
            if tracker is None or not self.requirements.trades.enabled
            else tracker.invalid_reason(
                open_time_ms,
                closed_kline.close_time_ms,
            )
        )
        if invalid_reason is not None:
            self._mark_range_context_degraded_bucket(
                bucket_start_ms=open_time_ms,
                reason="trade_data_incomplete_before_closed_bar",
                event_time_ms=closed_kline.close_time_ms,
            )
            self._alert_closed_bar_integrity_failure(
                closed_kline=closed_kline,
                reason=invalid_reason,
                result=result,
            )
            return False
        if not (result.hit_event_limit or result.hit_time_limit):
            return True
        self._mark_range_context_degraded_bucket(
            bucket_start_ms=open_time_ms,
            reason="market_queue_drain_incomplete_before_closed_bar",
            event_time_ms=closed_kline.close_time_ms,
        )
        logger.warning(
            "Market queue drain incomplete before closed-bar decision | close_time_ms=%s processed=%s deferred=%s examined=%s queue_size_before=%s queue_size_after=%s hit_event_limit=%s hit_time_limit=%s",
            closed_kline.close_time_ms,
            result.processed,
            result.deferred,
            result.examined,
            result.queue_size_before,
            result.queue_size_after,
            result.hit_event_limit,
            result.hit_time_limit,
        )
        self.context.alerts.emit(
            AppAlert(
                subject="AetherEdge closed-bar trade barrier incomplete",
                severity="error",
                content=(
                    f"symbol={self.app_config.symbol}\n"
                    f"close_time_ms={closed_kline.close_time_ms}\n"
                    f"pipeline_completed={result.pipeline_completed}\n"
                    f"pipeline_pending={result.pipeline_pending}\n"
                    f"queue_size_after={result.queue_size_after}\n"
                ),
            )
        )
        self.market_state.integrity_error = LiveRuntimeError(
            "closed-bar Trade pipeline barrier incomplete | "
            f"close_time_ms={closed_kline.close_time_ms} "
            f"pending={result.pipeline_pending}"
        )
        return False

    def _alert_closed_bar_integrity_failure(
        self,
        *,
        closed_kline: MarketKline,
        reason: str,
        result: MarketQueueDrainResult,
    ) -> None:
        logger.error(
            "Closed-bar decision suppressed for incomplete Trade data | "
            "close_time_ms=%s reason=%s pipeline_pending=%s",
            closed_kline.close_time_ms,
            reason,
            result.pipeline_pending,
        )
        self.context.alerts.emit(
            AppAlert(
                subject="AetherEdge closed-bar Trade data incomplete",
                severity="error",
                content=(
                    f"symbol={self.app_config.symbol}\n"
                    f"close_time_ms={closed_kline.close_time_ms}\n"
                    f"reason={reason}\n"
                    f"pipeline_pending={result.pipeline_pending}\n"
                ),
            )
        )

    async def _emit_closed_kline_feature(
        self,
        open_time_ms: int,
        closed_kline: MarketKline,
        *,
        finalized_at_ms: int,
    ) -> list[MarketFeatureEvent]:
        self._finalize_range_repair_journal(
            bucket_start_ms=open_time_ms,
            finalized_at_ms=finalized_at_ms,
        )
        event = closed_kline_feature(closed_kline)
        self.stats.closed_klines_seen += 1
        await self.process_market_feature(event)
        mark_emitted = getattr(self._closed_bar_scheduler, "mark_emitted", None)
        if callable(mark_emitted):
            mark_emitted(open_time_ms)
        else:
            self._closed_bar_scheduler.last_emitted_open_time_ms = open_time_ms
        self._refresh_range_micro_repair_coverage(open_time_ms)
        return [event]

    async def _closed_bar_range_features(
        self,
        open_time_ms: int,
        closed_kline: MarketKline,
    ) -> list[MarketFeatureEvent]:
        degraded = await self._degraded_range_context_feature(
            open_time_ms,
            closed_kline,
        )
        if degraded is not None:
            return [degraded]
        is_mid_bucket_restart = (
            self.requirements.trades.enabled
            and self._rangebar_trust_start_bucket_ms is not None
            and open_time_ms < self._rangebar_trust_start_bucket_ms
        )
        min_range_bars = self._get_min_range_bars()
        range_aggregates = self._load_range_aggregates_for_bucket(open_time_ms)
        best_range_bar_count = max(
            (int(aggregate.bar_count) for aggregate in range_aggregates),
            default=0,
        )
        aggregates_usable = range_aggregates and (
            not is_mid_bucket_restart
            or (
                self._initial_range_recovery is None
                and best_range_bar_count >= min_range_bars
            )
        )
        if aggregates_usable:
            if is_mid_bucket_restart:
                self._log_loaded_mid_bucket_range_aggregate(
                    open_time_ms,
                    best_range_bar_count,
                    min_range_bars,
                )
            return await self._emit_range_aggregates(range_aggregates)
        unavailable = await self._unavailable_range_context_feature(
            open_time_ms,
            closed_kline,
            is_mid_bucket_restart=is_mid_bucket_restart,
            has_range_aggregates=bool(range_aggregates),
            best_range_bar_count=best_range_bar_count,
            min_range_bars=min_range_bars,
        )
        return [] if unavailable is None else [unavailable]

    async def _degraded_range_context_feature(
        self,
        open_time_ms: int,
        closed_kline: MarketKline,
    ) -> MarketFeatureEvent | None:
        if (
            not self.requirements.trades.enabled
            or open_time_ms not in self._range_context_degraded_buckets
        ):
            return None
        reason = self._range_context_degraded_buckets.get(
            open_time_ms,
            "range_context_degraded",
        )
        logger.warning(
            "4H range context unavailable diagnostics | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s reason=%s trust_start_bucket_ms=%s queue_size=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            open_time_ms,
            open_time_ms + self._closed_bar_interval_ms - 1,
            reason,
            self._rangebar_trust_start_bucket_ms,
            self._market_queue.qsize(),
        )
        unavailable = range_aggregate_unavailable_feature(
            symbol=self.app_config.symbol,
            exchange=self.app_config.data_exchange,
            timeframe=self._range_aggregate_interval,
            range_pct=self._range_pct,
            bucket_start_ms=open_time_ms,
            bucket_end_ms=open_time_ms + self._closed_bar_interval_ms - 1,
            reference_price=closed_kline.close,
            reason=reason,
            coverage_status=RangeCoverageStatus.RECOVERED_INCOMPLETE.value,
        )
        await self.process_market_feature(unavailable)
        return unavailable

    def _log_loaded_mid_bucket_range_aggregate(
        self,
        open_time_ms: int,
        best_range_bar_count: int,
        min_range_bars: int,
    ) -> None:
        logger.info(
            "Range aggregate loaded despite mid-bucket restart | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s trust_start_bucket_ms=%s range_bar_count=%s min_range_bars=%s",
            self.app_config.symbol,
            self._closed_bar_interval,
            open_time_ms,
            open_time_ms + self._closed_bar_interval_ms - 1,
            self._rangebar_trust_start_bucket_ms,
            best_range_bar_count,
            min_range_bars,
        )

    async def _unavailable_range_context_feature(
        self,
        open_time_ms: int,
        closed_kline: MarketKline,
        *,
        is_mid_bucket_restart: bool,
        has_range_aggregates: bool,
        best_range_bar_count: int,
        min_range_bars: int,
    ) -> MarketFeatureEvent | None:
        if is_mid_bucket_restart:
            logger.warning(
                "Range aggregate unavailable after store load due to mid-bucket live trade collection | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s trust_start_bucket_ms=%s range_bar_count=%s min_range_bars=%s",
                self.app_config.symbol,
                self._closed_bar_interval,
                open_time_ms,
                open_time_ms + self._closed_bar_interval_ms - 1,
                self._rangebar_trust_start_bucket_ms,
                best_range_bar_count,
                min_range_bars,
            )
            reason = "live_trade_collection_started_mid_bucket"
            logger.warning(
                "4H range context unavailable diagnostics | symbol=%s interval=%s bucket_start_ms=%s bucket_end_ms=%s reason=%s trust_start_bucket_ms=%s queue_size=%s",
                self.app_config.symbol,
                self._closed_bar_interval,
                open_time_ms,
                open_time_ms + self._closed_bar_interval_ms - 1,
                reason,
                self._rangebar_trust_start_bucket_ms,
                self._market_queue.qsize(),
            )
        elif (
            self._initial_range_recovery is not None
            and not has_range_aggregates
            and self.requirements.trades.enabled
        ):
            reason = "no_completed_range_bars"
        else:
            return None
        coverage = self._range_coverage_for_bucket(open_time_ms)
        unavailable = range_aggregate_unavailable_feature(
            symbol=self.app_config.symbol,
            exchange=self.app_config.data_exchange,
            timeframe=self._range_aggregate_interval,
            range_pct=self._range_pct,
            bucket_start_ms=open_time_ms,
            bucket_end_ms=open_time_ms + self._closed_bar_interval_ms - 1,
            reference_price=closed_kline.close,
            reason=reason,
            coverage_status=coverage.coverage_status,
            missing_gap_ms=coverage.missing_gap_ms,
            range_recovered_from_checkpoint=coverage.recovered_from_checkpoint,
            range_checkpoint_age_ms=coverage.checkpoint_age_ms,
        )
        await self.process_market_feature(unavailable)
        return unavailable

    def _finish_closed_bar_decision(
        self,
        open_time_ms: int,
        closed_kline: MarketKline,
        features: list[MarketFeatureEvent],
    ) -> list[MarketFeatureEvent]:
        self._log_4h_decision_summary(
            open_time_ms=open_time_ms,
            closed_kline=closed_kline,
        )
        self._persist_closed_kline(closed_kline)
        return features

    def _persist_closed_kline(self, kline: MarketKline) -> None:
        """Queue one confirmed live closed kline after decisions complete."""

        self._get_market_data_persistence().persist_closed_kline(
            kline,
            on_error=lambda exc: self._on_closed_kline_persist_error(
                kline, exc
            ),
            on_rejected=self._on_live_persistence_write_rejected,
        )

    def _on_closed_kline_persist_error(
        self, kline: MarketKline, exc: BaseException
    ) -> None:
        try:
            logger.exception(
                "Failed to persist live closed kline | symbol=%s interval=%s open_time_ms=%s close_time_ms=%s",
                kline.symbol,
                kline.interval,
                kline.open_time_ms,
                kline.close_time_ms,
            )
            self._emit_alert_threadsafe(
                AppAlert(
                    subject="AetherEdge closed kline persistence failed",
                    severity="error",
                    content=(
                        f"symbol={kline.symbol}\n"
                        f"interval={kline.interval}\n"
                        f"open_time_ms={kline.open_time_ms}\n"
                        f"close_time_ms={kline.close_time_ms}\n"
                        f"error={type(exc).__name__}:{exc}\n"
                    ),
                )
            )
        except Exception:
            logger.exception(
                "Failed to emit closed kline persistence alert | symbol=%s interval=%s open_time_ms=%s",
                kline.symbol,
                kline.interval,
                kline.open_time_ms,
            )

    def _persist_range_bar(self, bar: RangeBar) -> None:
        """Queue one closed range bar after feature dispatch."""

        self._get_market_data_persistence().persist_range_bar(
            bar,
            on_error=lambda exc: self._on_range_bar_persist_error(bar, exc),
            on_rejected=self._on_live_persistence_write_rejected,
        )

    def _on_range_bar_persist_error(
        self, bar: RangeBar, exc: BaseException
    ) -> None:
        logger.exception(
            "Failed to persist live range bar | symbol=%s range_pct=%s bar_id=%s start_time_ms=%s end_time_ms=%s",
            bar.symbol,
            bar.range_pct,
            bar.bar_id,
            bar.start_time_ms,
            bar.end_time_ms,
        )
        self._emit_alert_threadsafe(
            AppAlert(
                subject="AetherEdge range bar persistence failed",
                severity="warning",
                content=(
                    f"symbol={bar.symbol}\n"
                    f"range_pct={bar.range_pct}\n"
                    f"bar_id={bar.bar_id}\n"
                    f"start_time_ms={bar.start_time_ms}\n"
                    f"end_time_ms={bar.end_time_ms}\n"
                    f"error={type(exc).__name__}:{exc}\n"
                ),
            )
        )

    def _persist_completed_range_aggregate(
        self,
        aggregate: RangeBarAggregate,
        *,
        coverage_status: str,
        missing_gap_ms: int,
    ) -> None:
        self._get_market_data_persistence().persist_completed_range_aggregate(
            aggregate,
            coverage_status=coverage_status,
            missing_gap_ms=missing_gap_ms,
            on_error=lambda exc: self._on_completed_range_aggregate_persist_error(
                aggregate, exc
            ),
            on_rejected=self._on_live_persistence_write_rejected,
        )

    def _on_completed_range_aggregate_persist_error(
        self, aggregate: RangeBarAggregate, exc: BaseException
    ) -> None:
        logger.warning(
            "Failed to persist completed range aggregate | symbol=%s range_pct=%s bucket_start_ms=%s error=%s",
            aggregate.symbol,
            aggregate.range_pct,
            aggregate.bucket_start_ms,
            exc,
        )

    def _load_range_aggregates_for_bucket(self, bucket_start_ms: int) -> list[RangeBarAggregate]:
        return self._require_range_module().aggregates_for_bucket(
            bucket_start_ms
        )

    def _range_bar_rows_for_bucket(
        self, bucket_start_ms: int
    ) -> list[RangeBar]:
        return self._require_range_module().rows_for_bucket(bucket_start_ms)

    def _range_store_fallback_allowed(self, bucket_start_ms: int) -> bool:
        module = self._require_range_module()
        return (
            bucket_start_ms not in module.bars_by_bucket
            or (
                module.initial_bucket_ms == bucket_start_ms
                and module.initial_recovery is not None
            )
        )

    async def _emit_range_aggregates(self, aggregates: Sequence[RangeBarAggregate]) -> list[MarketFeatureEvent]:
        module = self._require_range_module()
        before = module.aggregates_created
        events = await module.emit_aggregates(aggregates)
        self.stats.range_aggregates_created += (
            module.aggregates_created - before
        )
        return events

    async def emit_range_aggregate_for_bucket(self, bucket_start_ms: int) -> list[MarketFeatureEvent]:
        return await self._emit_range_aggregates(
            self._load_range_aggregates_for_bucket(bucket_start_ms)
        )
