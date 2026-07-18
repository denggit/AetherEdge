from __future__ import annotations

import asyncio
from pathlib import Path
from decimal import Decimal
from typing import Callable, Sequence
from src.app import AppConfig, AppContext
from src.market_data.warmup.gap_detector import interval_to_ms
from src.platform.account.ports import AccountClient
from src.platform.config import ProjectEnvConfig, get_project_env_config
from src.platform.data.models import MarketEvent, MarketEventType, MarketKline, MarketOrderBook, MarketTicker, MarketTrade
from src.platform.execution.ports import ExecutionClient
from src.platform.markets import get_market_profile
from src.platform.snapshot import PlatformSnapshot
from src.runtime.account_config import (
    AccountConfigBootstrapResult,
    AccountConfigEnv,
    bootstrap_account_config,
    load_account_config_env,
    raise_on_failed_account_config,
)
from src.runtime.account_sync import AccountStateSyncService, OrderStateSyncService, RequestThrottle, SyncExchangeContext
from src.runtime.config import LiveRuntimeConfig, live_runtime_config_from_app
from src.runtime.feature_pipeline import (
    TradeDerivedFeaturePipeline,
    TradeFeatureRuntimeConfig,
)
from src.runtime.health_state import RuntimeHealthState
from src.runtime.heartbeat import RuntimeHeartbeatService
from src.runtime.market_data.dispatcher import BoundedOrderedEventDispatcher
from src.runtime.market_data.range_module import RangeBarModule
from src.runtime.market_data.range_config import (
    RangeRuntimeConfig,
    range_runtime_config_from_env,
)
from src.runtime.market_data.range_repair_journal import RangeRepairJournalSession
from src.runtime.persistence_service import RuntimePersistenceService
from src.runtime.market_features import MarketFeaturePipeline
from src.runtime.models import RuntimeHealth, RuntimeMode, RuntimePhase
from src.runtime.market_data.range_background import RangeBackgroundServices
from src.runtime.market_data.range_composition import (
    RangeModuleComposition,
    RangeModuleOverrides,
)
from src.runtime.market_data.range_speed_runtime import RangeSpeedWarmup
from src.runtime.market_data.runtime import MarketDataRuntime
from src.runtime.module import CapabilityId
from src.runtime.requirements import (
    resolve_strategy_runtime_requirements,
    validate_strategy_runtime_requirements,
)
from src.runtime.strategy_capabilities import (
    StrategyCapabilityError,
    StrategyContractError,
    ValidatedStrategyCapabilities,
    validate_dynamic_strategy_capabilities,
    validate_strategy_capabilities,
)
from src.runtime.recovery_coordinator import (
    RuntimeRecoveryCoordinator,
    RuntimeRecoveryPlan,
)
from src.runtime.reconciliation_coordinator import (
    RuntimeReconciliationCoordinator,
    RuntimeReconciliationPlan,
)
from src.runtime.shutdown_coordinator import RuntimeShutdownCoordinator
from src.runtime.signal_execution_service import (
    RuntimeSignalExecutionPlan,
    RuntimeSignalExecutionRequest,
    RuntimeSignalExecutionService,
)
from src.runtime.startup_phase_coordinator import (
    RuntimeStartupPhaseCoordinator,
    RuntimeStartupPhasePlan,
)
from src.runtime.startup_catchup import (
    StartupCatchupConfig,
    StartupCatchupDecision,
    _check_price_guard,
    _deviation_pct,
)
from src.runtime.strategy_host import StrategyHost
from src.runtime.sync_lifecycle import RuntimeSyncLifecycle, SyncTaskFactory
from src.runtime.sync_services import RuntimeSyncServiceRegistry
from src.runtime.services import RuntimeServices, RuntimeServicesInput
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.tasks import ClosedBarScheduler, ProducerHealthMonitor, ProducerSupervisor

from src.runtime.live_helpers import _account_snapshot_log_keepalive_seconds_from_env
from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent


class WiringComponent(RuntimeComponent):
    def initialize(
        self,
        *,
        app_config: AppConfig,
        app_context: AppContext,
        runtime_config: LiveRuntimeConfig | None = None,
        range_config: RangeRuntimeConfig | None = None,
        managed_market_modules: bool = False,
        services: RuntimeServicesInput = None,
    ) -> None:
        self.app_config = app_config
        self.runtime_config = runtime_config or live_runtime_config_from_app(app_config)
        self.range_config = range_config or range_runtime_config_from_env()
        self._market_data_runtime: MarketDataRuntime | None = None
        self._market_data_capabilities: frozenset[CapabilityId] = frozenset()
        self._market_modules_managed = bool(managed_market_modules)
        self.context = app_context
        self.runtime_services = RuntimeServices.coerce(services)
        self.services = self.runtime_services
        self._initialize_strategy_runtime()
        self._initialize_execution_runtime()
        self._initialize_market_runtime()
        self._initialize_operational_state()
        self._initialize_range_runtime()

    def _initialize_strategy_runtime(self) -> None:
        injected_strategy_host = self.runtime_services.strategy_host
        self._strategy_host = (
            injected_strategy_host
            if injected_strategy_host is not None
            else StrategyHost(self.context.strategy)
        )
        injected_market_feature_pipeline = (
            self.runtime_services.market_feature_pipeline
        )
        self._market_feature_pipeline = (
            injected_market_feature_pipeline
            if injected_market_feature_pipeline is not None
            else MarketFeaturePipeline(self.context.strategy)
        )
        self._project_env: ProjectEnvConfig = (
            self.runtime_services.project_env_config or get_project_env_config()
        )
        self._account_config_env: AccountConfigEnv | None = None
        self._account_config_new_entries_blocked: bool = False
        self._account_config_apply_writes: bool = False
        self._account_config_results: tuple[AccountConfigBootstrapResult, ...] = ()
        if self.runtime_services.runtime_requirements is not None:
            self.requirements = validate_strategy_runtime_requirements(
                self.runtime_services.runtime_requirements
            )
        else:
            self.requirements = resolve_strategy_runtime_requirements(
                self.context.strategy,
                fallback_data_streams=self.app_config.data_streams,
            )
        self._validated_strategy_capabilities: (
            ValidatedStrategyCapabilities | None
        ) = None
        self.stats = LiveRuntimeStats()
        self._market_queue: asyncio.Queue[MarketEvent] = asyncio.Queue(
            maxsize=self.app_config.market_queue_maxsize
        )
        self._stop_event = asyncio.Event()
        self._producer_tasks: list[asyncio.Task] = []
        self._sync_tasks: list[asyncio.Task] = []
        injected_sync_lifecycle = self.runtime_services.sync_lifecycle
        self._sync_lifecycle = (
            injected_sync_lifecycle
            if injected_sync_lifecycle is not None
            else RuntimeSyncLifecycle()
        )
        self.runtime_services.sync_lifecycle = self._sync_lifecycle

    def _initialize_execution_runtime(self) -> None:
        self._execution_clients: tuple[ExecutionClient, ...] | None = None
        self._account_clients: tuple[AccountClient, ...] | None = None
        self._order_journal = self.runtime_services.order_journal
        self._position_plan_store = self.runtime_services.position_plan_store
        self._order_coordinator = self.runtime_services.order_coordinator
        self._account_sync_service = self.runtime_services.account_sync_service
        self._order_sync_service = self.runtime_services.order_sync_service
        injected_sync_service_registry = self.runtime_services.sync_service_registry
        self._sync_service_registry = (
            injected_sync_service_registry
            if injected_sync_service_registry is not None
            else RuntimeSyncServiceRegistry(
                account_service=self._account_sync_service,
                order_service=self._order_sync_service,
            )
        )
        self.runtime_services.sync_service_registry = self._sync_service_registry
        self._account_sync_service = getattr(
            self._sync_service_registry,
            "account_service",
            None,
        )
        self._order_sync_service = getattr(
            self._sync_service_registry,
            "order_service",
            None,
        )
        injected_signal_execution_service = (
            self.runtime_services.signal_execution_service
        )
        self._signal_execution_service = (
            injected_signal_execution_service
            if injected_signal_execution_service is not None
            else RuntimeSignalExecutionService()
        )
        self.runtime_services.signal_execution_service = (
            self._signal_execution_service
        )
        self._request_sync_throttle = (
            self.runtime_services.request_sync_throttle
            or RequestThrottle(min_interval_seconds=0.25)
        )
        self._recovery_service = self.runtime_services.recovery_service
        injected_recovery_coordinator = self.runtime_services.recovery_coordinator
        self._recovery_coordinator = (
            injected_recovery_coordinator
            if injected_recovery_coordinator is not None
            else RuntimeRecoveryCoordinator()
        )
        self.runtime_services.recovery_coordinator = self._recovery_coordinator
        self._reconciliation_service = self.runtime_services.reconciliation_service
        injected_reconciliation_coordinator = (
            self.runtime_services.reconciliation_coordinator
        )
        self._reconciliation_coordinator = (
            injected_reconciliation_coordinator
            if injected_reconciliation_coordinator is not None
            else RuntimeReconciliationCoordinator()
        )
        self.runtime_services.reconciliation_coordinator = (
            self._reconciliation_coordinator
        )

    def _initialize_market_runtime(self) -> None:
        self._live_persistence_writer = self.runtime_services.live_persistence_writer
        injected_persistence_service = (
            self.runtime_services.runtime_persistence_service
        )
        self._runtime_persistence_service = (
            injected_persistence_service
            if injected_persistence_service is not None
            else RuntimePersistenceService(
                writer=self._live_persistence_writer,
                max_pending=int(self.runtime_config.background_queue_maxsize),
                writer_name="live-persistence-writer",
            )
        )
        self.runtime_services.runtime_persistence_service = (
            self._runtime_persistence_service
        )
        self._persistence_alert_loop: asyncio.AbstractEventLoop | None = None
        self._fixed_time_trade_bar_builder_compat = (
            self.runtime_services.fixed_time_trade_bar_builder
        )
        self._trade_footprint_builder_compat = (
            self.runtime_services.trade_footprint_builder
        )
        self._range_footprint_builder_compat = (
            self.runtime_services.range_footprint_builder
        )
        injected_trade_pipeline = (
            self.runtime_services.trade_derived_feature_pipeline
        )
        self._trade_feature_config = TradeFeatureRuntimeConfig.from_strategy(
            self.context.strategy
        )
        self.runtime_services.trade_feature_config = self._trade_feature_config
        self._trade_derived_feature_pipeline = (
            injected_trade_pipeline
            if injected_trade_pipeline is not None
            else TradeDerivedFeaturePipeline(
                config=(
                    TradeFeatureRuntimeConfig()
                    if self._market_modules_managed
                    else self._trade_feature_config
                ),
                emit_feature=self.process_market_feature,
                fixed_time_trade_bar_builder=self._fixed_time_trade_bar_builder_compat,
                trade_footprint_builder=self._trade_footprint_builder_compat,
                range_footprint_builder=self._range_footprint_builder_compat,
            )
        )
        self._market_data_persistence = (
            self.runtime_services.market_data_persistence
        )
        self._get_market_data_persistence()
        self._range_repair_bootstrap_service = (
            self.runtime_services.range_repair_bootstrap_service
        )
        self._producer_monitor: ProducerHealthMonitor = (
            self.runtime_services.producer_monitor or ProducerHealthMonitor()
        )
        self._producer_supervisor: ProducerSupervisor = (
            self.runtime_services.producer_supervisor
            or ProducerSupervisor(
            monitor=self._producer_monitor,
            stale_after_ms=self.runtime_config.producer_stale_timeout_ms,
            )
        )
        self._closed_bar_interval = self.requirements.closed_kline.interval if self.requirements.closed_kline.enabled else self.runtime_config.closed_bar_interval
        self._closed_bar_buffer_ms = self.requirements.closed_kline.close_buffer_ms if self.requirements.closed_kline.close_buffer_ms is not None else self.runtime_config.closed_bar_buffer_ms
        self._closed_bar_retry_interval_ms = self.requirements.closed_kline.retry_interval_ms if self.requirements.closed_kline.retry_interval_ms is not None else self.runtime_config.closed_bar_retry_interval_ms
        self._closed_bar_missing_alert_after_ms = self.requirements.closed_kline.missing_alert_after_ms if self.requirements.closed_kline.missing_alert_after_ms is not None else self.runtime_config.closed_bar_missing_alert_after_ms
        self._closed_bar_interval_ms = interval_to_ms(self._closed_bar_interval)
        self._range_pct = self.requirements.range_bars.range_pct if self.requirements.range_bars.enabled else self.range_config.range_pct
        self._range_aggregate_interval = self.requirements.range_bars.aggregate_interval if self.requirements.range_bars.enabled else self._closed_bar_interval
        self._range_module: RangeBarModule | None = None
        self._range_repair_journal: RangeRepairJournalSession | None = None

    def _initialize_operational_state(self) -> None:
        self._closed_bar_scheduler: ClosedBarScheduler = (
            self.runtime_services.closed_bar_scheduler
            or ClosedBarScheduler(
            interval_ms=self._closed_bar_interval_ms,
            close_buffer_ms=self._closed_bar_buffer_ms,
            retry_interval_ms=self._closed_bar_retry_interval_ms,
            missing_alert_after_ms=self._closed_bar_missing_alert_after_ms,
            )
        )
        self._intent_factory = (
            self.runtime_services.intent_factory
            or LiveOrderIntentFactory(
            strategy_id=self.app_config.strategy,
            target_exchanges=self.app_config.exchanges,
            )
        )
        self._last_snapshot: PlatformSnapshot | None = (
            self.runtime_services.snapshot
        )
        self._last_snapshots: tuple[PlatformSnapshot, ...] = ()
        self._last_account_snapshot_log_state: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}
        self._last_account_snapshot_log_ms: dict[tuple[str, str], int] = {}
        self._account_snapshot_log_keepalive_seconds = _account_snapshot_log_keepalive_seconds_from_env(self._project_env)
        self._last_market_queue_full_log_ms = 0
        self._last_market_queue_full_alert_ms = 0
        self._last_market_queue_backlog_log_ms = 0
        self._last_live_data_path_log_ms = 0
        self._latest_fixed_time_trade_bar_open_time_ms: int | None = None
        self._market_queue_backlog_warn_threshold = self._project_env.get_int("AETHER_MARKET_QUEUE_BACKLOG_WARN_THRESHOLD", 500)
        self._market_queue_drain_batch_size = self._project_env.get_int("AETHER_MARKET_QUEUE_DRAIN_BATCH_SIZE", 1000)
        self._last_trade_health_update_ms = 0
        self._follower_close_alert_last_ms: dict[str, int] = {}
        initial_health = RuntimeHealth(
            phase=RuntimePhase.CREATED,
            warmup_complete=not self.runtime_config.warmup_enabled,
            caught_up=not self.runtime_config.warmup_enabled,
            metadata={"runtime_mode": self.runtime_config.mode.value, "strategy": self.app_config.strategy},
        )
        injected_health_state = self.runtime_services.runtime_health_state
        self._runtime_health_state = (
            injected_health_state
            if injected_health_state is not None
            else RuntimeHealthState(initial_health)
        )
        self.runtime_services.runtime_health_state = self._runtime_health_state
        self._health = self._runtime_health_state.current
        injected_heartbeat_service = self.runtime_services.heartbeat_service
        self._heartbeat_service = (
            injected_heartbeat_service
            if injected_heartbeat_service is not None
            else RuntimeHeartbeatService()
        )
        self.runtime_services.heartbeat_service = self._heartbeat_service
        injected_shutdown_coordinator = (
            self.runtime_services.shutdown_coordinator
        )
        self._shutdown_coordinator = (
            injected_shutdown_coordinator
            if injected_shutdown_coordinator is not None
            else RuntimeShutdownCoordinator()
        )
        self.runtime_services.shutdown_coordinator = self._shutdown_coordinator
        injected_startup_phase_coordinator = (
            self.runtime_services.startup_phase_coordinator
        )
        self._startup_phase_coordinator = (
            injected_startup_phase_coordinator
            if injected_startup_phase_coordinator is not None
            else RuntimeStartupPhaseCoordinator()
        )
        self.runtime_services.startup_phase_coordinator = (
            self._startup_phase_coordinator
        )
        self._startup_catchup_decision: StartupCatchupDecision | None = None
        self._startup_catchup_evaluated = False
        self._startup_catchup_range_observed = False
        self._range_speed_warmup: RangeSpeedWarmup | None = None
        self._startup_feature_backfill_providers = (
            self.runtime_services.startup_feature_backfill_providers
        )
        self._feature_backfill_providers_resolved = False
        self._range_background: RangeBackgroundServices | None = None

    def _initialize_range_runtime(self) -> None:
        if self.requirements.range_bars.enabled:
            market_profile = getattr(
                self.context.data,
                "market_profile",
                get_market_profile(self.app_config.symbol),
            )
            contract_value = (
                market_profile.contract_value(self.app_config.data_exchange)
                or Decimal("1")
            )
            self._range_module = RangeModuleComposition(
                symbol=self.app_config.symbol,
                exchange=self.app_config.data_exchange,
                range_pct=self._range_pct,
                contract_value=contract_value,
                bucket_interval=self._closed_bar_interval,
                bucket_interval_ms=self._closed_bar_interval_ms,
                aggregate_interval=self._range_aggregate_interval,
                min_bars=self.requirements.range_bars.min_bars,
                runtime_config=self.range_config,
                startup_catchup=self.runtime_config.startup_catchup,
                dispatcher=(
                    self.runtime_services.range_trade_dispatcher
                    or BoundedOrderedEventDispatcher(
                        maxsize=max(1, self.app_config.market_queue_maxsize),
                    )
                ),
                publish=self.process_market_feature,
                persistence=self._get_market_data_persistence(),
                stop_event=self._stop_event,
                speed_provider=self._strategy_range_speed_history_provider,
                repair_bootstrap=self._get_range_repair_bootstrap_service,
                emit_alert=getattr(
                    getattr(self.context, "alerts", None),
                    "emit",
                    lambda _alert: None,
                ),
                repo_root=Path(__file__).resolve().parents[3],
                on_error=lambda message, exc: logger.warning(
                    "%s | error=%s", message, exc
                ),
                on_bar_persist_error=self._on_range_bar_persist_error,
                on_aggregate_persist_error=(
                    self._on_completed_range_aggregate_persist_error
                ),
                on_rejected=self._on_live_persistence_write_rejected,
                overrides=RangeModuleOverrides(
                    module=self.runtime_services.range_bar_module,
                    bar_builder=self.runtime_services.range_bar_builder,
                    bar_store=self.runtime_services.range_bar_store,
                    bar_aggregator=self.runtime_services.range_bar_aggregator,
                    checkpoint_store=(
                        self.runtime_services.range_checkpoint_store
                    ),
                    checkpoint_writer=(
                        self.runtime_services.range_checkpoint_writer
                    ),
                    repair_journal_store=(
                        self.runtime_services.range_repair_journal_store
                    ),
                    repair_journal_writer=(
                        self.runtime_services.range_repair_journal_writer
                    ),
                    backfill_supervisor=(
                        self.runtime_services.range_backfill_supervisor
                    ),
                    micro_repair_supervisor=(
                        self.runtime_services.range_micro_repair_supervisor
                    ),
                    speed_history_refresher=(
                        self.runtime_services.range_speed_history_refresher
                    ),
                ),
            ).build()
            self.runtime_services.range_bar_module = self._range_module
            self._range_repair_journal = self._range_module.repair_journal
            self._range_speed_warmup = self._range_module.speed_warmup
            self._range_background = self._range_module.background
