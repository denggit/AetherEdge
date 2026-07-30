from __future__ import annotations

from src.runtime.services import (
    DEFAULT_RUNTIME_SERVICE,
    RuntimeServiceBundle,
)


_FIELD_TARGETS = {
    "strategy_host": ("execution", "strategy_host"),
    "market_feature_pipeline": ("market", "market_feature_pipeline"),
    "project_env_config": ("account", "project_env_config"),
    "runtime_requirements": ("execution", "runtime_requirements"),
    "sync_lifecycle": ("lifecycle", "sync_lifecycle"),
    "order_journal": ("execution", "order_journal"),
    "position_plan_store": ("execution", "position_plan_store"),
    "order_coordinator": ("execution", "order_coordinator"),
    "account_sync_service": ("account", "account_sync_service"),
    "order_sync_service": ("account", "order_sync_service"),
    "sync_service_registry": ("account", "sync_service_registry"),
    "signal_execution_service": ("execution", "signal_execution_service"),
    "request_sync_throttle": ("account", "request_sync_throttle"),
    "recovery_service": ("recovery", "recovery_service"),
    "recovery_coordinator": ("recovery", "recovery_coordinator"),
    "reconciliation_service": ("recovery", "reconciliation_service"),
    "reconciliation_coordinator": (
        "recovery",
        "reconciliation_coordinator",
    ),
    "live_persistence_writer": ("persistence", "writer"),
    "runtime_persistence_service": ("persistence", "runtime"),
    "market_data_persistence": ("persistence", "market_data"),
    "trade_feature_config": ("market", "trade_feature_config"),
    "producer_monitor": ("market", "producer_monitor"),
    "producer_supervisor": ("market", "producer_supervisor"),
    "closed_bar_scheduler": ("market", "closed_bar_scheduler"),
    "intent_factory": ("execution", "intent_factory"),
    "snapshot": ("recovery", "snapshot"),
    "runtime_health_state": ("lifecycle", "runtime_health_state"),
    "heartbeat_service": ("lifecycle", "heartbeat_service"),
    "shutdown_coordinator": ("lifecycle", "shutdown_coordinator"),
    "startup_phase_coordinator": ("lifecycle", "startup_phase_coordinator"),
    "startup_feature_backfill_providers": (
        "lifecycle",
        "startup_feature_backfill_providers",
    ),
    "account_clients": ("account", "clients"),
    "execution_clients": ("execution", "clients"),
    "kline_store": ("market", "kline_store"),
    "warmup_services": ("market", "warmup_services"),
    "warmup_service": ("market", "warmup_service"),
    "historical_trade_feed": ("market", "historical_trade_feed"),
    "trade_store": ("market", "trade_store"),
    "trade_data_integrity_tracker": (
        "market",
        "trade_data_integrity_tracker",
    ),
    "order_book_data_integrity_tracker": (
        "market",
        "order_book_data_integrity_tracker",
    ),
    "market_event_processor": ("market", "market_event_processor"),
    "range_bar_module": ("range", "module"),
    "range_bar_store": ("range", "bar_store"),
    "range_bar_builder": ("range", "bar_builder"),
    "range_bar_aggregator": ("range", "bar_aggregator"),
    "range_checkpoint_store": ("range", "checkpoint_store"),
    "range_checkpoint_writer": ("range", "checkpoint_writer"),
    "range_repair_journal_store": ("range", "repair_journal_store"),
    "range_repair_journal_writer": ("range", "repair_journal_writer"),
    "range_repair_bootstrap_service": (
        "range",
        "repair_bootstrap_service",
    ),
    "range_backfill_supervisor": ("range", "backfill_supervisor"),
    "range_micro_repair_supervisor": ("range", "micro_repair_supervisor"),
    "range_speed_history_refresher": ("range", "speed_history_refresher"),
}


class LegacyRuntimeServiceView:
    """Compatibility mapping/attribute view isolated to ``runtime.compat``."""

    def __init__(self, bundle: RuntimeServiceBundle) -> None:
        object.__setattr__(self, "_bundle", bundle)

    def _target(self, key: str):
        try:
            group_name, field_name = _FIELD_TARGETS[key]
        except KeyError:
            raise KeyError(key) from None
        return getattr(self._bundle, group_name), field_name

    def __getitem__(self, key: str) -> object:
        group, field_name = self._target(key)
        value = getattr(group, field_name)
        if value is DEFAULT_RUNTIME_SERVICE:
            raise KeyError(key)
        return value

    def __setitem__(self, key: str, value: object) -> None:
        group, field_name = self._target(key)
        setattr(group, field_name, value)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str) or key not in _FIELD_TARGETS:
            return False
        group, field_name = self._target(key)
        value = getattr(group, field_name)
        return value is not None and value is not DEFAULT_RUNTIME_SERVICE

    def __getattr__(self, key: str) -> object:
        try:
            return self[key]
        except KeyError:
            if key in _FIELD_TARGETS:
                group, field_name = self._target(key)
                return getattr(group, field_name)
            raise AttributeError(key) from None

    def __setattr__(self, key: str, value: object) -> None:
        if key == "_bundle":
            object.__setattr__(self, key, value)
            return
        try:
            self[key] = value
        except KeyError:
            object.__setattr__(self, key, value)


__all__ = ["LegacyRuntimeServiceView"]
