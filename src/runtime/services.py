from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields


DEFAULT_RUNTIME_SERVICE = object()


@dataclass
class RuntimeServices:
    """Explicit runtime dependency container.

    Production code consumes named attributes. ``from_legacy_mapping`` exists
    only at the pre-refactor constructor boundary.
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

    # Range-only compatibility inputs.  They are consumed by the Range
    # composition boundary, never by the generic runtime orchestrator.
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
    _source: RuntimeServices

    kline_store = property(
        lambda self: self._source.kline_store,
        lambda self, value: setattr(self._source, "kline_store", value),
    )
    warmup_services = property(
        lambda self: self._source.warmup_services,
        lambda self, value: setattr(self._source, "warmup_services", value),
    )
    warmup_service = property(
        lambda self: self._source.warmup_service,
        lambda self, value: setattr(self._source, "warmup_service", value),
    )
    market_event_processor = property(
        lambda self: self._source.market_event_processor,
        lambda self, value: setattr(
            self._source, "market_event_processor", value
        ),
    )
    trade_data_integrity_tracker = property(
        lambda self: self._source.trade_data_integrity_tracker
    )
    order_book_data_integrity_tracker = property(
        lambda self: self._source.order_book_data_integrity_tracker
    )


@dataclass
class AccountRuntimeServices:
    _source: RuntimeServices

    clients = property(lambda self: self._source.account_clients)


@dataclass
class ExecutionRuntimeServices:
    _source: RuntimeServices

    clients = property(lambda self: self._source.execution_clients)


@dataclass
class PersistenceRuntimeServices:
    _source: RuntimeServices

    runtime = property(
        lambda self: self._source.runtime_persistence_service,
        lambda self, value: setattr(
            self._source, "runtime_persistence_service", value
        ),
    )
    market_data = property(
        lambda self: self._source.market_data_persistence,
        lambda self, value: setattr(
            self._source, "market_data_persistence", value
        ),
    )
    writer = property(
        lambda self: self._source.live_persistence_writer,
        lambda self, value: setattr(
            self._source, "live_persistence_writer", value
        ),
    )


@dataclass
class RangeRuntimeServices:
    _source: RuntimeServices

    module = property(lambda self: self._source.range_bar_module)


@dataclass
class RecoveryRuntimeServices:
    _source: RuntimeServices


@dataclass
class LifecycleRuntimeServices:
    _source: RuntimeServices


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
    ) -> "RuntimeServiceBundle":
        return cls(
            market=MarketRuntimeServices(services),
            execution=ExecutionRuntimeServices(services),
            account=AccountRuntimeServices(services),
            persistence=PersistenceRuntimeServices(services),
            recovery=RecoveryRuntimeServices(services),
            lifecycle=LifecycleRuntimeServices(services),
            range=RangeRuntimeServices(services),
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
