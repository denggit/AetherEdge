from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, fields
from typing import Protocol

from src.market_data.ports import (
    HistoricalTradeFeed,
    KlineRepository,
    RangeBarAggregatorPort,
    RangeBarBuilderPort,
    RangeBarRepository,
    TradeRepository,
)
from src.market_data.range_checkpoint import (
    RangeCheckpointWriter,
    SqliteRangeCheckpointStore,
)
from src.market_data.range_repair import (
    RangeRepairJournalWriter,
    SqliteRangeRepairJournalStore,
)
from src.order_management.journal import SqliteOrderJournalStore
from src.order_management.ports import (
    OrderCoordinatorPort,
    PositionPlanStorePort,
)
from src.order_management.reconciliation.service import (
    LiveStateReconciliationService,
)
from src.platform.account.ports import AccountClient
from src.platform.config import ProjectEnvConfig
from src.platform.execution.ports import ExecutionClient
from src.platform.snapshot import PlatformSnapshot
from src.runtime.account_sync import (
    AccountStateSyncService,
    OrderStateSyncService,
    RequestThrottle,
)
from src.runtime.feature_pipeline import TradeFeatureRuntimeConfig
from src.runtime.health_state import RuntimeHealthState
from src.runtime.heartbeat import RuntimeHeartbeatService
from src.runtime.market_data.integrity import (
    OrderBookDataIntegrityTracker,
    TradeDataIntegrityTracker,
)
from src.runtime.market_data.processor import MarketEventProcessor
from src.runtime.market_data.range_module import RangeBarModule
from src.runtime.market_data_persistence import RuntimeMarketDataPersistence
from src.runtime.market_features import MarketFeaturePipeline
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.persistence import BackgroundWriteItem
from src.runtime.persistence_service import RuntimePersistenceService
from src.runtime.range_backfill_supervisor import RangeBackfillSupervisor
from src.runtime.range_micro_repair_supervisor import (
    RangeMicroRepairSupervisor,
)
from src.runtime.range_repair_bootstrap import RangeRepairBootstrapService
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher
from src.runtime.reconciliation_coordinator import (
    RuntimeReconciliationCoordinator,
)
from src.runtime.recovery import RuntimeRecoveryService
from src.runtime.recovery_coordinator import RuntimeRecoveryCoordinator
from src.runtime.requirements import StrategyRuntimeRequirements
from src.runtime.shutdown_coordinator import RuntimeShutdownCoordinator
from src.runtime.signal_execution_service import (
    RuntimeSignalExecutionService,
)
from src.runtime.startup_feature_backfill import (
    StartupFeatureBackfillProvider,
)
from src.runtime.startup_phase_coordinator import (
    RuntimeStartupPhaseCoordinator,
)
from src.runtime.strategy_host import StrategyHost
from src.runtime.sync_lifecycle import RuntimeSyncLifecycle
from src.runtime.sync_services import RuntimeSyncServiceRegistry
from src.runtime.tasks import (
    ClosedBarScheduler,
    ProducerHealthMonitor,
    ProducerSupervisor,
)


class RuntimeWarmupService(Protocol):
    def warmup(self):
        ...


RuntimeWarmupFactory = Callable[[], RuntimeWarmupService]
RuntimeWarmupDependency = RuntimeWarmupService | RuntimeWarmupFactory


class PersistenceWriter(Protocol):
    def submit(self, item: BackgroundWriteItem) -> bool:
        ...

    def stop(self, *, flush: bool = True):
        ...


class _DefaultRuntimeService:
    pass


DEFAULT_RUNTIME_SERVICE = _DefaultRuntimeService()


@dataclass
class RuntimeServices:
    """Legacy constructor DTO.

    The live composition boundary copies this DTO into independent domain
    service groups. Runtime components never retain or proxy this object.
    """

    strategy_host: object | None = None
    market_feature_pipeline: object | None = None
    project_env_config: object | None = None
    runtime_requirements: object | None = None
    sync_lifecycle: object | None = None
    order_journal: object | None = None
    position_plan_store: object | None = None
    order_coordinator: object | None = None
    account_sync_service: object | None = None
    order_sync_service: object | None = None
    sync_service_registry: object | None = None
    signal_execution_service: object | None = None
    request_sync_throttle: object | None = None
    recovery_service: object = DEFAULT_RUNTIME_SERVICE
    recovery_coordinator: object | None = None
    reconciliation_service: object = DEFAULT_RUNTIME_SERVICE
    reconciliation_coordinator: object | None = None
    live_persistence_writer: object | None = None
    runtime_persistence_service: object | None = None
    market_data_persistence: object | None = None
    trade_feature_config: object | None = None
    producer_monitor: object | None = None
    producer_supervisor: object | None = None
    closed_bar_scheduler: object | None = None
    intent_factory: object | None = None
    snapshot: object | None = None
    runtime_health_state: object | None = None
    heartbeat_service: object | None = None
    shutdown_coordinator: object | None = None
    startup_phase_coordinator: object | None = None
    startup_feature_backfill_providers: object | None = None
    account_clients: object | None = None
    execution_clients: object | None = None
    kline_store: object | None = None
    warmup_services: object | None = None
    warmup_service: object | None = None
    historical_trade_feed: object | None = None
    trade_store: object | None = None
    trade_data_integrity_tracker: object | None = None
    order_book_data_integrity_tracker: object | None = None
    market_event_processor: object | None = None
    range_bar_module: object | None = None
    range_bar_store: object | None = None
    range_bar_builder: object | None = None
    range_bar_aggregator: object | None = None
    range_checkpoint_store: object | None = None
    range_checkpoint_writer: object | None = None
    range_repair_journal_store: object | None = None
    range_repair_journal_writer: object | None = None
    range_repair_bootstrap_service: object | None = None
    range_backfill_supervisor: object | None = None
    range_micro_repair_supervisor: object | None = None
    range_speed_history_refresher: object | None = None

    @classmethod
    def from_legacy_mapping(
        cls,
        values: Mapping[str, object] | None,
    ) -> RuntimeServices:
        if values is None:
            return cls()
        known = {item.name for item in fields(cls)}
        unknown = sorted(set(values) - known)
        if unknown:
            raise KeyError(
                "unknown runtime service field(s): " + ", ".join(unknown)
            )
        return cls(**dict(values))

    @classmethod
    def coerce(
        cls,
        value: RuntimeServices | Mapping[str, object] | None,
    ) -> RuntimeServices:
        return value if isinstance(value, cls) else cls.from_legacy_mapping(value)


RuntimeServicesInput = RuntimeServices | Mapping[str, object] | None


@dataclass
class MarketRuntimeServices:
    kline_store: KlineRepository | None = None
    warmup_services: Sequence[RuntimeWarmupDependency] | None = None
    warmup_service: RuntimeWarmupDependency | None = None
    historical_trade_feed: HistoricalTradeFeed | None = None
    trade_store: TradeRepository | None = None
    market_feature_pipeline: MarketFeaturePipeline | None = None
    trade_feature_config: TradeFeatureRuntimeConfig | None = None
    producer_monitor: ProducerHealthMonitor | None = None
    producer_supervisor: ProducerSupervisor | None = None
    closed_bar_scheduler: ClosedBarScheduler | None = None
    market_event_processor: MarketEventProcessor | None = None
    trade_data_integrity_tracker: TradeDataIntegrityTracker | None = None
    order_book_data_integrity_tracker: (
        OrderBookDataIntegrityTracker | None
    ) = None


@dataclass
class AccountRuntimeServices:
    clients: Sequence[AccountClient] | None = None
    account_sync_service: AccountStateSyncService | None = None
    order_sync_service: OrderStateSyncService | None = None
    sync_service_registry: RuntimeSyncServiceRegistry | None = None
    request_sync_throttle: RequestThrottle | None = None
    project_env_config: ProjectEnvConfig | None = None


@dataclass
class ExecutionRuntimeServices:
    clients: Sequence[ExecutionClient] | None = None
    strategy_host: StrategyHost | None = None
    runtime_requirements: StrategyRuntimeRequirements | None = None
    order_journal: SqliteOrderJournalStore | None = None
    position_plan_store: PositionPlanStorePort | None = None
    order_coordinator: OrderCoordinatorPort | None = None
    signal_execution_service: RuntimeSignalExecutionService | None = None
    intent_factory: LiveOrderIntentFactory | None = None


@dataclass
class PersistenceRuntimeServices:
    runtime: RuntimePersistenceService | None = None
    market_data: RuntimeMarketDataPersistence | None = None
    writer: PersistenceWriter | None = None


@dataclass
class RecoveryRuntimeServices:
    recovery_service: (
        RuntimeRecoveryService | _DefaultRuntimeService
    ) = DEFAULT_RUNTIME_SERVICE
    recovery_coordinator: RuntimeRecoveryCoordinator | None = None
    reconciliation_service: (
        LiveStateReconciliationService | _DefaultRuntimeService
    ) = DEFAULT_RUNTIME_SERVICE
    reconciliation_coordinator: RuntimeReconciliationCoordinator | None = None
    snapshot: PlatformSnapshot | None = None


@dataclass
class LifecycleRuntimeServices:
    sync_lifecycle: RuntimeSyncLifecycle | None = None
    runtime_health_state: RuntimeHealthState | None = None
    heartbeat_service: RuntimeHeartbeatService | None = None
    shutdown_coordinator: RuntimeShutdownCoordinator | None = None
    startup_phase_coordinator: RuntimeStartupPhaseCoordinator | None = None
    startup_feature_backfill_providers: (
        Sequence[StartupFeatureBackfillProvider] | None
    ) = None


@dataclass
class RangeRuntimeServices:
    module: RangeBarModule | None = None
    bar_store: RangeBarRepository | None = None
    bar_builder: RangeBarBuilderPort | None = None
    bar_aggregator: RangeBarAggregatorPort | None = None
    checkpoint_store: SqliteRangeCheckpointStore | None = None
    checkpoint_writer: RangeCheckpointWriter | None = None
    repair_journal_store: SqliteRangeRepairJournalStore | None = None
    repair_journal_writer: RangeRepairJournalWriter | None = None
    repair_bootstrap_service: RangeRepairBootstrapService | None = None
    backfill_supervisor: RangeBackfillSupervisor | None = None
    micro_repair_supervisor: RangeMicroRepairSupervisor | None = None
    speed_history_refresher: RangeSpeedHistoryRefresher | None = None


@dataclass
class RuntimeServiceBundle:
    market: MarketRuntimeServices
    execution: ExecutionRuntimeServices
    account: AccountRuntimeServices
    persistence: PersistenceRuntimeServices
    recovery: RecoveryRuntimeServices
    lifecycle: LifecycleRuntimeServices
    range: RangeRuntimeServices

    @classmethod
    def from_legacy_boundary(
        cls,
        services: RuntimeServices,
    ) -> RuntimeServiceBundle:
        """Copy legacy constructor values once into isolated service groups."""

        return cls(
            market=MarketRuntimeServices(
                kline_store=services.kline_store,
                warmup_services=services.warmup_services,
                warmup_service=services.warmup_service,
                historical_trade_feed=services.historical_trade_feed,
                trade_store=services.trade_store,
                market_feature_pipeline=services.market_feature_pipeline,
                trade_feature_config=services.trade_feature_config,
                producer_monitor=services.producer_monitor,
                producer_supervisor=services.producer_supervisor,
                closed_bar_scheduler=services.closed_bar_scheduler,
                market_event_processor=services.market_event_processor,
                trade_data_integrity_tracker=(
                    services.trade_data_integrity_tracker
                ),
                order_book_data_integrity_tracker=(
                    services.order_book_data_integrity_tracker
                ),
            ),
            execution=ExecutionRuntimeServices(
                clients=services.execution_clients,
                strategy_host=services.strategy_host,
                runtime_requirements=services.runtime_requirements,
                order_journal=services.order_journal,
                position_plan_store=services.position_plan_store,
                order_coordinator=services.order_coordinator,
                signal_execution_service=services.signal_execution_service,
                intent_factory=services.intent_factory,
            ),
            account=AccountRuntimeServices(
                clients=services.account_clients,
                account_sync_service=services.account_sync_service,
                order_sync_service=services.order_sync_service,
                sync_service_registry=services.sync_service_registry,
                request_sync_throttle=services.request_sync_throttle,
                project_env_config=services.project_env_config,
            ),
            persistence=PersistenceRuntimeServices(
                runtime=services.runtime_persistence_service,
                market_data=services.market_data_persistence,
                writer=services.live_persistence_writer,
            ),
            recovery=RecoveryRuntimeServices(
                recovery_service=services.recovery_service,
                recovery_coordinator=services.recovery_coordinator,
                reconciliation_service=services.reconciliation_service,
                reconciliation_coordinator=services.reconciliation_coordinator,
                snapshot=services.snapshot,
            ),
            lifecycle=LifecycleRuntimeServices(
                sync_lifecycle=services.sync_lifecycle,
                runtime_health_state=services.runtime_health_state,
                heartbeat_service=services.heartbeat_service,
                shutdown_coordinator=services.shutdown_coordinator,
                startup_phase_coordinator=services.startup_phase_coordinator,
                startup_feature_backfill_providers=(
                    services.startup_feature_backfill_providers
                ),
            ),
            range=RangeRuntimeServices(
                module=services.range_bar_module,
                bar_store=services.range_bar_store,
                bar_builder=services.range_bar_builder,
                bar_aggregator=services.range_bar_aggregator,
                checkpoint_store=services.range_checkpoint_store,
                checkpoint_writer=services.range_checkpoint_writer,
                repair_journal_store=services.range_repair_journal_store,
                repair_journal_writer=services.range_repair_journal_writer,
                repair_bootstrap_service=(
                    services.range_repair_bootstrap_service
                ),
                backfill_supervisor=services.range_backfill_supervisor,
                micro_repair_supervisor=(
                    services.range_micro_repair_supervisor
                ),
                speed_history_refresher=(
                    services.range_speed_history_refresher
                ),
            ),
        )


__all__ = [
    "DEFAULT_RUNTIME_SERVICE",
    "AccountRuntimeServices",
    "ExecutionRuntimeServices",
    "LifecycleRuntimeServices",
    "MarketRuntimeServices",
    "PersistenceRuntimeServices",
    "RangeRuntimeServices",
    "RecoveryRuntimeServices",
    "RuntimeServiceBundle",
    "RuntimeServices",
    "RuntimeServicesInput",
]
