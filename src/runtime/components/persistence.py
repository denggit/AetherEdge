from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Mapping, Sequence
from src.app.alerts import AppAlert
from src.order_management import LegSyncStatus, MasterFollowerExecutionPolicy, MultiExchangeOrderCoordinator, PositionPlanStatus, RepositoryDuplicateOrderGuard, SqliteOrderJournalStore, SqlitePositionPlanStore
from src.runtime.market_data_persistence import RuntimeMarketDataPersistence
from src.runtime.persistence_service import RuntimePersistenceService
from src.runtime.market_features import MarketFeaturePipeline

from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent
from src.runtime.persistence import BackgroundWriteQueue
_BackgroundWriteQueue = BackgroundWriteQueue


class PersistenceComponent(RuntimeComponent):
    def _get_runtime_persistence_service(self) -> RuntimePersistenceService:
        service = getattr(self, "_runtime_persistence_service", None)
        if service is None:
            max_pending = int(
                getattr(
                    getattr(self, "runtime_config", None),
                    "background_queue_maxsize",
                    1000,
                )
            )
            service = RuntimePersistenceService(
                writer=getattr(self, "_live_persistence_writer", None),
                max_pending=max_pending,
                writer_name="live-persistence-writer",
            )
            self._runtime_persistence_service = service
            self.service_dependencies().runtime_persistence_service = service
        return service

    def _get_market_data_persistence(self) -> RuntimeMarketDataPersistence:
        try:
            self._persistence_alert_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        persistence = getattr(self, "_market_data_persistence", None)
        if persistence is None:
            services = self.service_dependencies()
            persistence = services.market_data_persistence
            if persistence is None:
                persistence = RuntimeMarketDataPersistence(
                    persistence_service=self._get_runtime_persistence_service(),
                    kline_store_provider=self._get_live_kline_store,
                    range_bar_store_provider=self._get_range_bar_store,
                    completed_aggregate_store_provider=(
                        self._get_range_checkpoint_store
                    ),
                    exchange=self.app_config.data_exchange.value,
                    clock_ms=lambda: int(time.time() * 1000),
                )
                services.market_data_persistence = persistence
            self._market_data_persistence = persistence
        return persistence

    def _get_live_persistence_writer(self) -> _BackgroundWriteQueue:
        writer = self._get_runtime_persistence_service().get_writer()
        self._live_persistence_writer = writer
        self.service_dependencies().live_persistence_writer = writer
        return writer  # type: ignore[return-value]

    def _on_live_persistence_write_rejected(self, description: str) -> None:
        metrics = self._get_runtime_persistence_service().metrics()
        logger.warning(
            "Live persistence write dropped | description=%s pending=%s dropped=%s",
            description,
            metrics.pending_count,
            metrics.dropped,
        )

    def _submit_live_persistence_write(
        self,
        *,
        description: str,
        write: Callable[[], None],
        on_error: Callable[[BaseException], None] | None = None,
    ) -> bool:
        try:
            self._persistence_alert_loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        service = self._get_runtime_persistence_service()
        self._get_live_persistence_writer()
        accepted = service.submit(
            description=description,
            write=write,
            on_error=on_error,
        )
        if not accepted:
            metrics = service.metrics()
            logger.warning(
                "Live persistence write dropped | description=%s pending=%s dropped=%s",
                description,
                metrics.pending_count,
                metrics.dropped,
            )
        return accepted

    async def _stop_live_persistence_writer(
        self, *, flush: bool = True
    ) -> None:
        service = getattr(self, "_runtime_persistence_service", None)
        if service is None and getattr(self, "_live_persistence_writer", None) is None:
            return
        await self._get_runtime_persistence_service().stop(flush=flush)

    def _emit_alert_threadsafe(self, alert: AppAlert) -> None:
        try:
            loop = getattr(self, "_persistence_alert_loop", None)
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(self.context.alerts.emit, alert)
                return
            self.context.alerts.emit(alert)
        except Exception:
            logger.exception(
                "Failed to emit background persistence alert | subject=%s",
                alert.subject,
            )

    def _maybe_log_live_data_path_stats(self) -> None:
        now_ms = int(time.time() * 1000)
        last_ms = int(getattr(self, "_last_live_data_path_log_ms", 0) or 0)
        interval_seconds = (
            getattr(self, "_project_env", None)
            and self._project_env.get_int(
                "AETHER_LIVE_DATA_PATH_STATS_INTERVAL_SECONDS", 1800
            )
        ) or 1800
        if last_ms and now_ms - last_ms < interval_seconds * 1000:
            return
        self._last_live_data_path_log_ms = now_ms
        interval_ms = int(getattr(self, "_closed_bar_interval_ms", 0) or 0)
        current_bucket = (
            (now_ms // interval_ms) * interval_ms
            if interval_ms > 0
            else None
        )
        range_bars_by_bucket = (
            {}
            if getattr(self, "_range_module", None) is None
            else self._range_module.bars_by_bucket
        )
        current_bucket_count = (
            len(range_bars_by_bucket.get(current_bucket, ()))
            if current_bucket is not None
            else None
        )
        mf_audit = self._mf_observer_audit()
        persistence_metrics = self._get_runtime_persistence_service().metrics()
        pending = persistence_metrics.pending_count
        writer_dropped = persistence_metrics.dropped
        writer_failures = persistence_metrics.failures
        writer_written = persistence_metrics.written
        writer_submitted = persistence_metrics.submitted
        logger.info(
            "Live data path stats | market_events_seen=%s feature_events_seen=%s latest_fixed_time_trade_bar_open_time_ms=%s mf_tradebar_count=%s mf_range_footprint_count=%s current_range_bucket_start_ms=%s range_bars_by_bucket_current_count=%s live_persistence_pending=%s live_persistence_dropped=%s live_persistence_failures=%s live_persistence_written=%s live_persistence_submitted=%s",
            getattr(getattr(self, "stats", None), "market_events_seen", None),
            getattr(getattr(self, "stats", None), "feature_events_seen", None),
            getattr(
                self,
                "_latest_fixed_time_trade_bar_open_time_ms",
                None,
            ),
            mf_audit.get("tradebar_count"),
            mf_audit.get("range_footprint_count"),
            current_bucket,
            current_bucket_count,
            pending,
            writer_dropped,
            writer_failures,
            writer_written,
            writer_submitted,
        )

    def _get_market_feature_pipeline(self) -> MarketFeaturePipeline:
        pipeline = getattr(self, "_market_feature_pipeline", None)
        if pipeline is None:
            pipeline = MarketFeaturePipeline(self.context.strategy)
            self._market_feature_pipeline = pipeline
        return pipeline

    def _mf_observer_audit(self) -> Mapping[str, Any]:
        try:
            observers = self._get_market_feature_pipeline().resolve_observers()
        except Exception as exc:
            logger.debug("MF observer audit unavailable | error=%s", exc)
            return {}
        for observer in observers:
            audit = getattr(observer, "audit", None)
            if not callable(audit):
                continue
            try:
                data = audit()
            except Exception as exc:
                logger.debug("MF observer audit failed | error=%s", exc)
                continue
            if isinstance(data, Mapping) and (
                "tradebar_count" in data
                or "range_footprint_count" in data
            ):
                return data
        return {}

    def _get_position_plan_store(self):
        if self._position_plan_store is None:
            path = self._project_env.get("AETHER_POSITION_PLAN_DB", "data/state/aether_position_plan.sqlite3")
            self._position_plan_store = SqlitePositionPlanStore(path)
        return self._position_plan_store
