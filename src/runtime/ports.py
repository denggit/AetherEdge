from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from src.runtime.models import RuntimeHealth


class RuntimeServicePort(Protocol):
    async def start(self) -> RuntimeHealth:
        ...

    async def stop(self) -> RuntimeHealth:
        ...

    async def health(self) -> RuntimeHealth:
        ...


class BackgroundTaskQueue(Protocol):
    async def put(self, item: object) -> None:
        ...

    async def drain(self) -> None:
        ...


RuntimeCall = Callable[..., Any]


@dataclass(frozen=True, slots=True)
class WiringPorts:
    process_market_feature: RuntimeCall
    get_market_data_persistence: RuntimeCall
    get_range_repair_bootstrap_service: RuntimeCall
    strategy_range_speed_history_provider: RuntimeCall
    on_range_bar_persist_error: RuntimeCall
    on_completed_range_aggregate_persist_error: RuntimeCall
    on_live_persistence_write_rejected: RuntimeCall


@dataclass(frozen=True, slots=True)
class AccountPorts:
    execute_signals: RuntimeCall
    get_execution_clients: RuntimeCall
    get_order_journal: RuntimeCall
    get_position_plan_store: RuntimeCall
    strategy_pending_work_provider: RuntimeCall


@dataclass(frozen=True, slots=True)
class CatchupPorts:
    execute_signals: RuntimeCall
    get_market_feature_pipeline: RuntimeCall
    get_order_journal: RuntimeCall
    get_position_plan_store: RuntimeCall
    has_unresolved_follower_close: RuntimeCall
    require_range_module: RuntimeCall
    strategy_pending_work_provider: RuntimeCall
    strategy_position_index: RuntimeCall
    strategy_startup_preview_provider: RuntimeCall


@dataclass(frozen=True, slots=True)
class ClosedBarPorts:
    emit_alert_threadsafe: RuntimeCall
    get_market_data_persistence: RuntimeCall
    get_min_range_bars: RuntimeCall
    on_live_persistence_write_rejected: RuntimeCall
    process_market_feature: RuntimeCall
    raise_on_unhealthy_market_data: RuntimeCall
    raise_on_unhealthy_producer: RuntimeCall
    require_range_module: RuntimeCall
    trade_integrity_tracker: RuntimeCall


@dataclass(frozen=True, slots=True)
class LifecyclePorts:
    bootstrap_account_config_if_enabled: RuntimeCall
    call_on_start: RuntimeCall
    check_strategy_position_mode_requirements: RuntimeCall
    enqueue_market_event: RuntimeCall
    evaluate_startup_catchup_once: RuntimeCall
    finish_range_speed_warmup_after_catchup: RuntimeCall
    get_account_sync_service: RuntimeCall
    get_order_sync_service: RuntimeCall
    initialize_rangebar_trust_window: RuntimeCall
    mark_range_context_degraded_bucket: RuntimeCall
    periodic_follower_close_check: RuntimeCall
    process_market_feature: RuntimeCall
    recheck_account_config_after_recovery: RuntimeCall
    run_reconciliation: RuntimeCall
    run_recovery: RuntimeCall
    run_warmup: RuntimeCall
    start_range_speed_background_services: RuntimeCall
    strategy_capabilities: RuntimeCall
    warmup_range_speed_history: RuntimeCall


@dataclass(frozen=True, slots=True)
class MarketEventPorts:
    all_producers_done: RuntimeCall
    execute_signals: RuntimeCall
    get_market_feature_pipeline: RuntimeCall
    maybe_log_live_data_path_stats: RuntimeCall
    mark_range_context_degraded_bucket: RuntimeCall
    poll_closed_bar_once: RuntimeCall
    process_market_event: RuntimeCall
    raise_on_unhealthy_market_data: RuntimeCall
    raise_on_unhealthy_producer: RuntimeCall
    set_health: RuntimeCall


@dataclass(frozen=True, slots=True)
class OrderResultPorts:
    get_account_clients: RuntimeCall
    get_position_plan_store: RuntimeCall
    strategy_position_index: RuntimeCall
    validate_order_results_before_journal: RuntimeCall


@dataclass(frozen=True, slots=True)
class RecoveryPorts:
    execute_signals: RuntimeCall
    get_account_clients: RuntimeCall
    get_execution_clients: RuntimeCall
    get_order_journal: RuntimeCall
    get_position_plan_store: RuntimeCall
    resolved_account_config_env: RuntimeCall
    strategy_position_index: RuntimeCall


@dataclass(frozen=True, slots=True)
class SignalExecutionPorts:
    get_account_sync_service: RuntimeCall
    get_order_coordinator: RuntimeCall
    get_order_sync_service: RuntimeCall
    has_account_config_entry_block: RuntimeCall
    has_unresolved_follower_close: RuntimeCall
    process_order_result_feedback: RuntimeCall
    save_order_results: RuntimeCall
    set_health: RuntimeCall
    verify_stop_order_results: RuntimeCall


@dataclass(frozen=True, slots=True)
class StartupPorts:
    get_account_clients: RuntimeCall
    get_execution_clients: RuntimeCall
    get_market_feature_pipeline: RuntimeCall
    process_market_feature: RuntimeCall
    require_range_module: RuntimeCall
    set_health: RuntimeCall
    strategy_capabilities: RuntimeCall


@dataclass(frozen=True, slots=True)
class PersistencePorts:
    get_live_kline_store: RuntimeCall
    require_range_module: RuntimeCall


__all__ = [
    "AccountPorts",
    "BackgroundTaskQueue",
    "CatchupPorts",
    "ClosedBarPorts",
    "LifecyclePorts",
    "MarketEventPorts",
    "OrderResultPorts",
    "PersistencePorts",
    "RecoveryPorts",
    "RuntimeServicePort",
    "SignalExecutionPorts",
    "StartupPorts",
    "WiringPorts",
]
