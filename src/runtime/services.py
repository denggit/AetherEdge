from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, fields


DEFAULT_RUNTIME_SERVICE = object()


@dataclass
class RuntimeServices:
    """Explicit runtime dependency container.

    Production code consumes named attributes.  ``from_legacy_mapping`` and the
    item accessors exist only for the pre-refactor constructor/test boundary and
    can be removed once external callers have migrated.
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
    fixed_time_trade_bar_builder: object | None = None
    trade_footprint_builder: object | None = None
    range_footprint_builder: object | None = None
    trade_derived_feature_pipeline: object | None = None
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

    # Range-only compatibility inputs.  They are consumed by the Range
    # composition boundary, never by the generic runtime orchestrator.
    range_bar_module: object | None = None
    range_trade_dispatcher: object | None = None
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

    # Thin mapping compatibility for existing external callers and tests.
    def __getitem__(self, key: str) -> object:
        if key not in self:
            raise KeyError(key)
        return getattr(self, key)

    def __setitem__(self, key: str, value: object) -> None:
        if key not in {item.name for item in fields(self)}:
            raise KeyError(key)
        setattr(self, key, value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str) or key not in {
            item.name for item in fields(self)
        }:
            return False
        value = getattr(self, key)
        return value is not None and value is not DEFAULT_RUNTIME_SERVICE

    def __iter__(self) -> Iterator[str]:
        return (item.name for item in fields(self))

    def get(self, key: str, default: object | None = None) -> object | None:
        if key not in self:
            return default
        value = getattr(self, key)
        return default if value is DEFAULT_RUNTIME_SERVICE else value


RuntimeServicesInput = RuntimeServices | Mapping[str, object] | None


__all__ = [
    "DEFAULT_RUNTIME_SERVICE",
    "RuntimeServices",
    "RuntimeServicesInput",
]
