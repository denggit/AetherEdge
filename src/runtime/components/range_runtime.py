from __future__ import annotations

import time
from src.market_data.models import MarketDataSet, RangeBar, RangeBarAggregate, RangeCoverageStatus, TimeRange, WarmupRequest
from src.market_data.range_checkpoint import (
    RangeCheckpointRecovery,
    RangeCheckpointWriter,
    SqliteRangeCheckpointStore,
)
from src.market_data.range_repair import (
    JOURNAL_INVALID_DROPPED_TRADE,
    JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
    JOURNAL_INVALID_PRODUCER_FAILED,
    JOURNAL_INVALID_PRODUCER_STALE,
    RangeRepairJournalWriter,
)
from src.market_data.storage import SqliteKlineStore
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.runtime.market_data.range_module import (
    RangeBarModule,
    RangeBarModuleConfig,
)
from src.runtime.market_data.range_repair_journal import (
    RangeRepairJournalConfig,
    RangeRepairJournalSession,
)
from src.runtime.range_backfill_supervisor import RangeBackfillSupervisor
from src.runtime.range_micro_repair_supervisor import RangeMicroRepairSupervisor
from src.runtime.range_repair_bootstrap import RangeRepairBootstrapService
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher
from src.runtime.market_data.range_background import (
    RangeBackgroundServices,
    range_background_config,
)

from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent


class RangeRuntimeComponent(RuntimeComponent):
    def _get_range_bar_builder(self):
        return self._require_range_module().builder

    def _get_live_kline_store(self):
        services = self.service_dependencies()
        repository = services.kline_store
        if repository is None:
            repository = SqliteKlineStore(
                self.range_config.market_data_db_path
            )
            services.kline_store = repository
        return repository

    def _get_range_bar_store(self):
        return self._require_range_module().bar_store

    def _get_range_bar_aggregator(self):
        return self._require_range_module().aggregator

    def _get_range_checkpoint_store(self) -> SqliteRangeCheckpointStore:
        return self._require_range_module().checkpoint_store

    def _get_range_checkpoint_writer(self) -> RangeCheckpointWriter:
        return self._require_range_module().checkpoint_writer

    def _get_range_repair_bootstrap_service(
        self,
    ) -> RangeRepairBootstrapService:
        if self._range_repair_bootstrap_service is None:
            self._range_repair_bootstrap_service = (
                RangeRepairBootstrapService(
                    range_config=self.range_config,
                    exchange=self.app_config.data_exchange.value,
                    symbol=self.app_config.symbol,
                    range_pct=str(self._range_pct),
                    closed_bar_interval_ms=self._closed_bar_interval_ms,
                    checkpoint_store=self._get_range_checkpoint_store(),
                    emit_alert=self.context.alerts.emit,
                    journal_store=self._require_range_repair_journal().store,
                    journal_writer=self._require_range_repair_journal().writer,
                    micro_repair_supervisor=(
                        None
                        if self._range_background is None
                        else self._range_background.micro_repair_supervisor
                    ),
                    clock_ms=lambda: int(time.time() * 1000),
                )
            )
        return self._range_repair_bootstrap_service

    def _get_range_repair_journal_writer(
        self,
    ) -> RangeRepairJournalWriter:
        journal = self._require_range_repair_journal()
        if journal.writer is None:
            service = self._get_range_repair_bootstrap_service()
            journal.set_resources(
                writer=service.get_journal_writer(),
                store=service.get_journal_store(),
            )
        assert journal.writer is not None
        return journal.writer

    def _append_range_repair_trade(self, trade: MarketTrade) -> None:
        self._require_range_repair_journal().append(trade)

    def _invalidate_range_repair_journal(
        self,
        *,
        bucket_start_ms: int,
        status: str,
        reason: str,
        dropped_trades: int = 0,
    ) -> None:
        journal = self._range_repair_journal
        if journal is None:
            return
        journal.invalidate(
            bucket_start_ms=bucket_start_ms,
            status=status,
            reason=reason,
            dropped_trades=dropped_trades,
        )

    def _finalize_range_repair_journal(
        self,
        *,
        bucket_start_ms: int,
        finalized_at_ms: int,
    ) -> None:
        journal = self._range_repair_journal
        if journal is None:
            return
        journal.finalize(
            bucket_start_ms=bucket_start_ms,
            finalized_at_ms=finalized_at_ms,
        )

    def _start_range_speed_background_services(self) -> None:
        if getattr(self, "_market_modules_managed", False):
            return
        background = self._range_background
        if background is not None:
            background.start(self._stop_event)

    def _get_range_backfill_supervisor(self) -> RangeBackfillSupervisor:
        background = self._require_range_background()
        return background.get_backfill_supervisor()

    def _get_range_micro_repair_supervisor(
        self,
    ) -> RangeMicroRepairSupervisor:
        return self._require_range_background().get_micro_repair_supervisor()

    def _get_range_speed_history_refresher(self) -> RangeSpeedHistoryRefresher:
        return self._require_range_background().get_speed_refresher()

    async def _stop_market_data_modules(self) -> None:
        runtime = getattr(self, "_market_data_runtime", None)
        if runtime is not None:
            await runtime.stop()
            return
        module = self._range_module
        if module is None:
            return
        await module.stop()

    def _range_coverage_for_bucket(
        self, bucket_start_ms: int
    ) -> RangeCheckpointRecovery:
        return self._require_range_module().coverage(bucket_start_ms)

    def _refresh_range_micro_repair_coverage(
        self, bucket_start_ms: int
    ) -> None:
        module = self._range_module
        if module is None:
            return
        if not module.adopt_repaired_coverage(bucket_start_ms):
            return
        bucket_end_ms = bucket_start_ms + self._closed_bar_interval_ms - 1
        logger.info(
            "Range micro repair COMPLETE aggregate adopted | symbol=%s "
            "exchange=%s bucket_start_ms=%s bucket_end_ms=%s "
            "cleared_partial_memory_rows=True repaired_complete=True",
            self.app_config.symbol,
            self.app_config.data_exchange.value,
            bucket_start_ms,
            bucket_end_ms,
        )

    def _require_range_module(self) -> RangeBarModule:
        module = getattr(self, "_range_module", None)
        if module is None:
            raise LiveRuntimeError("Range capability is not enabled")
        return module

    def _require_range_background(self) -> RangeBackgroundServices:
        background = self._range_background
        if background is None:
            raise LiveRuntimeError("Range capability is not enabled")
        return background

    def _require_range_repair_journal(self) -> RangeRepairJournalSession:
        journal = self._range_repair_journal
        if journal is None:
            raise LiveRuntimeError("Range capability is not enabled")
        return journal

    @property
    def _rangebar_trust_start_bucket_ms(self) -> int | None:
        module = self._range_module
        return None if module is None else module.trust_start_bucket_ms

    @_rangebar_trust_start_bucket_ms.setter
    def _rangebar_trust_start_bucket_ms(self, value: int | None) -> None:
        self._require_range_module().trust_start_bucket_ms = value

    @property
    def _initial_range_bucket_ms(self) -> int | None:
        module = self._range_module
        return None if module is None else module.initial_bucket_ms

    @_initial_range_bucket_ms.setter
    def _initial_range_bucket_ms(self, value: int | None) -> None:
        self._require_range_module().initial_bucket_ms = value

    @property
    def _initial_range_recovery(self) -> RangeCheckpointRecovery | None:
        module = self._range_module
        return None if module is None else module.initial_recovery

    @_initial_range_recovery.setter
    def _initial_range_recovery(
        self,
        value: RangeCheckpointRecovery | None,
    ) -> None:
        self._require_range_module().initial_recovery = value

    @property
    def _range_bars_by_bucket(self) -> dict[int, list[RangeBar]]:
        return self._require_range_module().bars_by_bucket

    @property
    def _range_repair_journal_store(self):
        journal = self._range_repair_journal
        return None if journal is None else journal.store

    @_range_repair_journal_store.setter
    def _range_repair_journal_store(self, value) -> None:
        self._require_range_repair_journal().store = value

    @property
    def _range_repair_journal_writer(self):
        journal = self._range_repair_journal
        return None if journal is None else journal.writer

    @_range_repair_journal_writer.setter
    def _range_repair_journal_writer(self, value) -> None:
        self._require_range_repair_journal().writer = value

    @property
    def _last_range_checkpoint_submit_ms(self) -> int:
        return self._require_range_module().last_checkpoint_submit_ms

    @_last_range_checkpoint_submit_ms.setter
    def _last_range_checkpoint_submit_ms(self, value: int) -> None:
        self._require_range_module().last_checkpoint_submit_ms = value

    @property
    def _range_bars_since_checkpoint(self) -> int:
        return self._require_range_module().bars_since_checkpoint

    @_range_bars_since_checkpoint.setter
    def _range_bars_since_checkpoint(self, value: int) -> None:
        self._require_range_module().bars_since_checkpoint = value

    @property
    def _range_speed_min_periods(self) -> int:
        warmup = getattr(self, "_range_speed_warmup", None)
        return (
            int(self.__dict__.get("_range_speed_min_periods_compat", 0))
            if warmup is None
            else warmup.min_periods
        )

    @_range_speed_min_periods.setter
    def _range_speed_min_periods(self, value: int) -> None:
        warmup = getattr(self, "_range_speed_warmup", None)
        if warmup is None:
            self.__dict__["_range_speed_min_periods_compat"] = int(value)
        else:
            warmup.min_periods = int(value)

    @property
    def _range_speed_complete_history(self) -> int:
        warmup = getattr(self, "_range_speed_warmup", None)
        return (
            int(self.__dict__.get("_range_speed_complete_history_compat", 0))
            if warmup is None
            else warmup.complete_history
        )

    @_range_speed_complete_history.setter
    def _range_speed_complete_history(self, value: int) -> None:
        warmup = getattr(self, "_range_speed_warmup", None)
        if warmup is None:
            self.__dict__["_range_speed_complete_history_compat"] = int(value)
        else:
            warmup.complete_history = int(value)

    @property
    def _range_speed_warmup_excluded_previous(self) -> bool:
        warmup = getattr(self, "_range_speed_warmup", None)
        return (
            bool(self.__dict__.get("_range_speed_excluded_compat", False))
            if warmup is None
            else warmup.excluded_previous
        )

    @_range_speed_warmup_excluded_previous.setter
    def _range_speed_warmup_excluded_previous(self, value: bool) -> None:
        warmup = getattr(self, "_range_speed_warmup", None)
        if warmup is None:
            self.__dict__["_range_speed_excluded_compat"] = bool(value)
        else:
            warmup.excluded_previous = bool(value)

    @property
    def _range_bar_builder(self):
        module = getattr(self, "_range_module", None)
        if module is None:
            return self.__dict__.get("_range_bar_builder_compat")
        return module.builder

    @_range_bar_builder.setter
    def _range_bar_builder(self, value) -> None:
        module = getattr(self, "_range_module", None)
        if module is None:
            self.__dict__["_range_bar_builder_compat"] = value
        else:
            module.builder = value

    @property
    def _range_bar_store(self):
        module = getattr(self, "_range_module", None)
        if module is None:
            return self.__dict__.get("_range_bar_store_compat")
        return module.bar_store

    @_range_bar_store.setter
    def _range_bar_store(self, value) -> None:
        module = getattr(self, "_range_module", None)
        if module is None:
            self.__dict__["_range_bar_store_compat"] = value
        else:
            module.bar_store = value

    @property
    def _range_bar_aggregator(self):
        module = getattr(self, "_range_module", None)
        if module is None:
            return self.__dict__.get("_range_bar_aggregator_compat")
        return module.aggregator

    @_range_bar_aggregator.setter
    def _range_bar_aggregator(self, value) -> None:
        module = getattr(self, "_range_module", None)
        if module is None:
            self.__dict__["_range_bar_aggregator_compat"] = value
        else:
            module.aggregator = value
