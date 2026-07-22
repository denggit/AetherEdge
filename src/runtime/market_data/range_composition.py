from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from src.market_data.models import RangeBar, RangeBarAggregate
from src.market_data.range_checkpoint import (
    RangeCheckpointWriter,
    SqliteRangeCheckpointStore,
)
from src.platform.exchanges.models import ExchangeName
from src.runtime.market_data.range_background import (
    RangeBackgroundServices,
    range_background_config,
)
from src.runtime.market_data.range_config import RangeRuntimeConfig
from src.runtime.market_data.range_module import (
    AggregateErrorReporter,
    BarErrorReporter,
    FeaturePublisher,
    RangeBarModule,
    RangeBarModuleConfig,
    RangeBarPersistence,
)
from src.runtime.market_data.range_repair_journal import (
    RangeRepairJournalConfig,
    RangeRepairJournalSession,
)
from src.runtime.market_data.range_speed_runtime import (
    ProviderFactory,
    RangeSpeedWarmup,
    RangeSpeedWarmupConfig,
)
from src.runtime.range_backfill_supervisor import RangeBackfillSupervisor
from src.runtime.range_micro_repair_supervisor import RangeMicroRepairSupervisor
from src.runtime.range_repair_bootstrap import RangeRepairBootstrapService
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher
from src.runtime.startup_catchup import StartupCatchupConfig


@dataclass(frozen=True)
class RangeModuleOverrides:
    module: RangeBarModule | None = None
    bar_builder: object | None = None
    bar_store: object | None = None
    bar_aggregator: object | None = None
    checkpoint_store: SqliteRangeCheckpointStore | None = None
    checkpoint_writer: RangeCheckpointWriter | None = None
    repair_journal_store: object | None = None
    repair_journal_writer: object | None = None
    backfill_supervisor: RangeBackfillSupervisor | None = None
    micro_repair_supervisor: RangeMicroRepairSupervisor | None = None
    speed_history_refresher: RangeSpeedHistoryRefresher | None = None


@dataclass(frozen=True)
class RangeModuleComposition:
    symbol: str
    exchange: ExchangeName
    range_pct: Decimal
    contract_value: Decimal
    bucket_interval: str
    bucket_interval_ms: int
    aggregate_interval: str
    min_bars: int
    runtime_config: RangeRuntimeConfig
    startup_catchup: StartupCatchupConfig
    publish: FeaturePublisher
    persistence: RangeBarPersistence
    stop_event: asyncio.Event
    speed_provider: ProviderFactory
    repair_bootstrap: Callable[[], RangeRepairBootstrapService]
    emit_alert: Callable[[object], None]
    repo_root: Path
    on_error: Callable[[str, BaseException], None] | None = None
    on_bar_persist_error: BarErrorReporter | None = None
    on_aggregate_persist_error: AggregateErrorReporter | None = None
    on_rejected: Callable[[str], None] | None = None
    overrides: RangeModuleOverrides = RangeModuleOverrides()

    def build(self) -> RangeBarModule:
        module = self.overrides.module or RangeBarModule(
            config=RangeBarModuleConfig(
                symbol=self.symbol,
                exchange=self.exchange,
                range_pct=self.range_pct,
                contract_value=self.contract_value,
                bucket_interval_ms=self.bucket_interval_ms,
                aggregate_interval=self.aggregate_interval,
                min_bars=self.min_bars,
                checkpoint_db_path=self.runtime_config.checkpoint_db_path,
                checkpoint_interval_ms=self.runtime_config.checkpoint_interval_ms,
                checkpoint_every_closed_bars=(
                    self.runtime_config.checkpoint_every_closed_bars
                ),
                checkpoint_writer_max_pending=(
                    self.runtime_config.checkpoint_writer_max_pending
                ),
                checkpoint_max_age_for_recovered_minor_ms=(
                    self.runtime_config.checkpoint_max_age_for_recovered_minor_ms
                ),
                checkpoint_max_age_for_restore_ms=(
                    self.runtime_config.checkpoint_max_age_for_restore_ms
                ),
            ),
            publish=self.publish,
            persistence=self.persistence,
            builder=self.overrides.bar_builder,
            bar_store=self.overrides.bar_store,
            aggregator=self.overrides.bar_aggregator,
            checkpoint_store=self.overrides.checkpoint_store,
            checkpoint_writer=self.overrides.checkpoint_writer,
            on_error=self.on_error,
            on_bar_persist_error=self.on_bar_persist_error,
            on_aggregate_persist_error=self.on_aggregate_persist_error,
            on_rejected=self.on_rejected,
        )
        repair_journal = RangeRepairJournalSession(
            config=RangeRepairJournalConfig(
                symbol=self.symbol,
                exchange=self.exchange,
                range_pct=self.range_pct,
                bucket_interval_ms=self.bucket_interval_ms,
            ),
            emit_alert=self.emit_alert,
            store=self.overrides.repair_journal_store,
            writer=self.overrides.repair_journal_writer,
        )
        speed_warmup = RangeSpeedWarmup(
            config=RangeSpeedWarmupConfig(
                symbol=self.symbol,
                exchange=self.exchange.value,
                range_pct=self.range_pct,
                bucket_interval_ms=self.bucket_interval_ms,
                startup_catchup=self.startup_catchup,
            ),
            provider=self.speed_provider,
            checkpoint_store=lambda: module.checkpoint_store,
        )
        background = RangeBackgroundServices(
            config=range_background_config(
                self.runtime_config,
                symbol=self.symbol,
                exchange=self.exchange.value,
                range_pct=self.range_pct,
                bucket_interval=self.bucket_interval,
                repo_root=self.repo_root,
            ),
            checkpoint_store=lambda: module.checkpoint_store,
            provider=self.speed_provider,
            micro_repair_factory=lambda: (
                self.repair_bootstrap().get_micro_repair_supervisor()
            ),
            backfill_supervisor=self.overrides.backfill_supervisor,
            micro_repair_supervisor=self.overrides.micro_repair_supervisor,
            speed_refresher=self.overrides.speed_history_refresher,
        )
        module.configure_support(
            background=background,
            repair_journal=repair_journal,
            speed_warmup=speed_warmup,
            stop_event=self.stop_event,
            repair_bootstrap=self.repair_bootstrap,
        )
        return module


__all__ = [
    "RangeModuleComposition",
    "RangeModuleOverrides",
]
