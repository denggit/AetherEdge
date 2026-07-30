from __future__ import annotations

from src.runtime.compat.services import LegacyRuntimeServiceView
from src.runtime.ports import (
    AccountPorts,
    CatchupPorts,
    ClosedBarPorts,
    LifecyclePorts,
    MarketEventPorts,
    OrderResultPorts,
    PersistencePorts,
    RecoveryPorts,
    SignalExecutionPorts,
    StartupPorts,
)


def _copy(component, source, names: tuple[str, ...]) -> None:
    values = vars(source)
    component.bind_dependencies(
        **{name: values[name] for name in names if name in values}
    )


def configure_runtime_composition(runner, wiring, context) -> None:
    """Complete one explicit composition after the boundary DTO is copied."""

    _initialize_context(context, wiring)
    _inject_dependencies(runner, wiring)
    _inject_ports(runner)
    _inject_runner_compatibility(runner, wiring)


def _initialize_context(context, wiring) -> None:
    context.app_config = wiring.app_config
    context.app_context = wiring.context
    context.runtime_config = wiring.runtime_config
    context.range_config = wiring.range_config
    context.requirements = wiring.requirements
    context.resources.queues.market = wiring._market_queue
    context.resources.queues.market_event_available = (
        wiring._market_event_available
    )
    context.resources.queues.latest_state_mailbox = wiring._latest_state_mailbox
    context.resources.signals.stop_event = wiring._stop_event
    lifecycle = context.resources.lifecycle
    lifecycle.health_state = wiring._runtime_health_state
    lifecycle.heartbeat_service = wiring._heartbeat_service
    lifecycle.shutdown_coordinator = wiring._shutdown_coordinator
    lifecycle.startup_phase_coordinator = wiring._startup_phase_coordinator
    lifecycle.last_snapshot = wiring._last_snapshot
    lifecycle.last_snapshots = wiring._last_snapshots
    context.resources.statistics.live = wiring.stats


def _inject_dependencies(runner, source) -> None:
    common = ("app_config", "context", "requirements", "stats")
    selected = {
        runner.account_runtime: common + (
            "_account_clients", "_account_config_env",
            "_account_config_new_entries_blocked",
            "_account_snapshot_log_keepalive_seconds",
            "_last_account_snapshot_log_ms",
            "_last_account_snapshot_log_state", "_last_snapshots",
            "_position_plan_store", "_project_env",
            "_reconciliation_coordinator", "_reconciliation_service",
            "_request_sync_throttle", "_strategy_host",
            "_sync_service_registry",
        ),
        runner.catchup: common + (
            "_closed_bar_interval", "_closed_bar_interval_ms",
            "_closed_bar_scheduler", "_heartbeat_service",
            "_last_snapshots", "_order_journal", "_position_plan_store",
            "_startup_catchup_evaluated", "_strategy_host",
            "runtime_config",
        ),
        runner.closed_bar: common + (
            "_closed_bar_buffer_ms", "_closed_bar_interval",
            "_closed_bar_interval_ms", "_closed_bar_missing_alert_after_ms",
            "_closed_bar_scheduler", "_market_queue",
            "_range_aggregate_interval", "_range_pct",
        ),
        runner.lifecycle: common + (
            "_account_config_new_entries_blocked", "_closed_bar_interval_ms",
            "_feature_backfill_providers_resolved", "_health",
            "_heartbeat_service", "_producer_supervisor", "_producer_tasks",
            "_runtime_health_state", "_startup_feature_backfill_providers",
            "_startup_phase_coordinator", "_stop_event", "_sync_lifecycle",
        ),
        runner.market_events: common + (
            "_closed_bar_interval_ms", "_health",
            "_heartbeat_service",
            "_last_market_queue_backlog_log_ms",
            "_last_market_queue_full_alert_ms",
            "_last_market_queue_full_log_ms", "_last_trade_health_update_ms",
            "_latest_state_mailbox", "_market_event_available",
            "_market_queue", "_market_queue_backlog_warn_threshold",
            "_market_queue_drain_batch_size", "_prefer_latest_state_event",
            "_range_module", "_range_repair_journal", "_stop_event",
            "_strategy_host", "runtime_config",
        ),
        runner.order_results: (
            "app_config", "context", "runtime_config", "_execution_clients",
            "_order_coordinator", "_order_journal", "_project_env",
            "_strategy_host",
        ),
        runner.persistence: (
            "app_config", "context", "_position_plan_store", "_project_env",
            "_range_module", "_runtime_persistence_service",
            "_live_persistence_writer", "_market_data_persistence",
            "_market_feature_pipeline", "_persistence_alert_loop",
            "_last_live_data_path_log_ms", "_closed_bar_interval_ms",
            "_latest_fixed_time_trade_bar_open_time_ms", "stats",
        ),
        runner.market_data_lifecycle: (
            "app_config", "context", "range_config",
            "_closed_bar_interval_ms", "_range_background", "_range_module",
            "_range_pct", "_range_repair_bootstrap_service", "_stop_event",
            "_market_data_runtime", "_market_modules_managed",
        ),
        runner.recovery: common + (
            "runtime_config", "_last_snapshot", "_last_snapshots",
            "_recovery_coordinator", "_recovery_service",
            "_validated_strategy_capabilities",
        ),
        runner.signal_execution: common + (
            "_follower_close_alert_last_ms", "_health", "_intent_factory",
            "_signal_execution_service",
        ),
        runner.startup: common + (
            "runtime_config", "range_config", "_account_config_apply_writes",
            "_account_config_env", "_closed_bar_buffer_ms",
            "_closed_bar_interval", "_closed_bar_interval_ms", "_health",
            "_project_env", "_range_speed_warmup",
            "_startup_catchup_range_observed",
        ),
    }
    for component, names in selected.items():
        _copy(component, source, names)

    bundle = source.service_bundle
    runner.account_runtime.account_services = bundle.account
    runner.account_runtime.execution_services = bundle.execution
    runner.catchup.market_services = bundle.market
    runner.closed_bar.market_services = bundle.market
    runner.closed_bar.range_services = bundle.range
    runner.market_events.market_services = bundle.market
    runner.order_results.execution_services = bundle.execution
    runner.persistence.persistence_services = bundle.persistence
    runner.market_data_lifecycle.market_services = bundle.market
    runner.startup.market_services = bundle.market


def _inject_ports(runner) -> None:
    account = runner.account_runtime
    catchup = runner.catchup
    closed = runner.closed_bar
    lifecycle = runner.lifecycle
    market = runner.market_events
    orders = runner.order_results
    persistence = runner.persistence
    range_runtime = runner.market_data_lifecycle
    recovery = runner.recovery
    signals = runner.signal_execution
    startup = runner.startup

    account.bind_ports(AccountPorts(
        execute_signals=lambda *a, **k: signals._execute_signals(*a, **k),
        get_execution_clients=orders._get_execution_clients,
        get_order_journal=orders._get_order_journal,
        get_position_plan_store=persistence._get_position_plan_store,
        strategy_pending_work_provider=recovery._strategy_pending_work_provider,
    ))
    catchup.bind_ports(CatchupPorts(
        execute_signals=lambda *a, **k: signals._execute_signals(*a, **k),
        get_market_feature_pipeline=persistence._get_market_feature_pipeline,
        get_order_journal=orders._get_order_journal,
        get_position_plan_store=persistence._get_position_plan_store,
        has_unresolved_follower_close=account._has_unresolved_follower_close,
        require_range_module=range_runtime._require_range_module,
        strategy_pending_work_provider=recovery._strategy_pending_work_provider,
        strategy_position_index=account._strategy_position_index,
        strategy_startup_preview_provider=(
            recovery._strategy_startup_preview_provider
        ),
    ))
    closed.bind_ports(ClosedBarPorts(
        emit_alert_threadsafe=persistence._emit_alert_threadsafe,
        get_market_data_persistence=persistence._get_market_data_persistence,
        get_min_range_bars=catchup._get_min_range_bars,
        on_live_persistence_write_rejected=(
            persistence._on_live_persistence_write_rejected
        ),
        process_market_feature=lambda *a, **k: (
            runner.process_market_feature(*a, **k)
        ),
        raise_on_unhealthy_market_data=lambda *a, **k: (
            market._raise_on_unhealthy_market_data(*a, **k)
        ),
        raise_on_unhealthy_producer=lambda *a, **k: (
            lifecycle._raise_on_unhealthy_producer(*a, **k)
        ),
        require_range_module=range_runtime._require_range_module,
        trade_integrity_tracker=market._trade_integrity_tracker,
    ))
    lifecycle.bind_ports(LifecyclePorts(
        bootstrap_account_config_if_enabled=lambda *a, **k: (
            startup._bootstrap_account_config_if_enabled(*a, **k)
        ),
        call_on_start=lambda *a, **k: catchup._call_on_start(*a, **k),
        check_strategy_position_mode_requirements=lambda *a, **k: (
            startup._check_strategy_position_mode_requirements(*a, **k)
        ),
        enqueue_market_event=lambda *a, **k: (
            market._enqueue_market_event(*a, **k)
        ),
        evaluate_startup_catchup_once=lambda *a, **k: (
            catchup._evaluate_startup_catchup_once(*a, **k)
        ),
        finish_range_speed_warmup_after_catchup=lambda *a, **k: (
            startup._finish_range_speed_warmup_after_catchup(*a, **k)
        ),
        get_account_sync_service=lambda *a, **k: (
            account._get_account_sync_service(*a, **k)
        ),
        get_order_sync_service=lambda *a, **k: (
            account._get_order_sync_service(*a, **k)
        ),
        initialize_rangebar_trust_window=lambda *a, **k: (
            startup._initialize_rangebar_trust_window(*a, **k)
        ),
        mark_range_context_degraded_bucket=(
            market._mark_range_context_degraded_bucket
        ),
        periodic_follower_close_check=lambda *a, **k: (
            account._periodic_follower_close_check(*a, **k)
        ),
        process_market_feature=lambda *a, **k: (
            runner.process_market_feature(*a, **k)
        ),
        recheck_account_config_after_recovery=lambda *a, **k: (
            startup._recheck_account_config_after_recovery(*a, **k)
        ),
        run_reconciliation=lambda *a, **k: (
            account._run_reconciliation(*a, **k)
        ),
        run_recovery=lambda *a, **k: recovery._run_recovery(*a, **k),
        run_warmup=lambda *a, **k: startup._run_warmup(*a, **k),
        start_range_speed_background_services=lambda *a, **k: (
            range_runtime._start_range_speed_background_services(*a, **k)
        ),
        strategy_capabilities=lambda *a, **k: (
            recovery._strategy_capabilities(*a, **k)
        ),
        warmup_range_speed_history=lambda *a, **k: (
            startup._warmup_range_speed_history(*a, **k)
        ),
    ))
    market.bind_ports(MarketEventPorts(
        all_producers_done=lambda *a, **k: (
            lifecycle._all_producers_done(*a, **k)
        ),
        execute_signals=lambda *a, **k: signals._execute_signals(*a, **k),
        get_market_feature_pipeline=persistence._get_market_feature_pipeline,
        maybe_log_live_data_path_stats=(
            persistence._maybe_log_live_data_path_stats
        ),
        mark_range_context_degraded_bucket=(
            market._mark_range_context_degraded_bucket
        ),
        poll_closed_bar_once=lambda *a, **k: (
            closed.poll_closed_bar_once(*a, **k)
        ),
        process_market_event=lambda *a, **k: (
            runner.process_market_event(*a, **k)
        ),
        raise_on_unhealthy_market_data=lambda *a, **k: (
            market._raise_on_unhealthy_market_data(*a, **k)
        ),
        raise_on_unhealthy_producer=lambda *a, **k: (
            lifecycle._raise_on_unhealthy_producer(*a, **k)
        ),
        set_health=lambda *a, **k: lifecycle._set_health(*a, **k),
    ))
    orders.bind_ports(OrderResultPorts(
        get_account_clients=account._get_account_clients,
        get_position_plan_store=persistence._get_position_plan_store,
        strategy_position_index=account._strategy_position_index,
        validate_order_results_before_journal=(
            signals._validate_order_results_before_journal
        ),
    ))
    persistence.bind_ports(PersistencePorts(
        get_live_kline_store=range_runtime._get_live_kline_store,
        require_range_module=range_runtime._require_range_module,
    ))
    recovery.bind_ports(RecoveryPorts(
        execute_signals=lambda *a, **k: signals._execute_signals(*a, **k),
        get_account_clients=account._get_account_clients,
        get_execution_clients=orders._get_execution_clients,
        get_order_journal=orders._get_order_journal,
        get_position_plan_store=persistence._get_position_plan_store,
        resolved_account_config_env=account._resolved_account_config_env,
        strategy_position_index=account._strategy_position_index,
    ))
    signals.bind_ports(SignalExecutionPorts(
        get_account_sync_service=lambda: (
            account._get_account_sync_service()
        ),
        get_order_coordinator=lambda: orders._get_order_coordinator(),
        get_order_sync_service=lambda: account._get_order_sync_service(),
        has_account_config_entry_block=lambda: (
            account._has_account_config_entry_block()
        ),
        has_unresolved_follower_close=lambda: (
            account._has_unresolved_follower_close()
        ),
        process_order_result_feedback=lambda *a, **k: (
            orders._process_order_result_feedback(*a, **k)
        ),
        save_order_results=lambda *a, **k: orders._save_order_results(
            *a, **k
        ),
        set_health=lambda *a, **k: lifecycle._set_health(*a, **k),
        verify_stop_order_results=lambda *a, **k: (
            orders._verify_stop_order_results(*a, **k)
        ),
    ))
    startup.bind_ports(StartupPorts(
        get_account_clients=account._get_account_clients,
        get_execution_clients=orders._get_execution_clients,
        get_market_feature_pipeline=persistence._get_market_feature_pipeline,
        process_market_feature=lambda *a, **k: (
            runner.process_market_feature(*a, **k)
        ),
        require_range_module=range_runtime._require_range_module,
        set_health=lifecycle._set_health,
        strategy_capabilities=recovery._strategy_capabilities,
    ))


def _inject_runner_compatibility(runner, source) -> None:
    for name in (
        "app_config", "stats", "_health", "_market_queue",
        "_market_queue_backlog_warn_threshold",
        "_market_queue_drain_batch_size", "_shutdown_coordinator",
        "_stop_event", "_heartbeat_service", "requirements",
        "runtime_config", "range_config", "_latest_state_mailbox",
        "_market_event_available", "_producer_tasks", "_sync_tasks",
        "_closed_bar_scheduler", "_range_repair_journal",
        "_range_background", "_runtime_health_state",
        "_live_persistence_writer", "_runtime_persistence_service",
        "_market_data_persistence", "_market_feature_pipeline",
        "_producer_monitor", "_producer_supervisor",
        "_last_account_snapshot_log_ms",
        "_last_account_snapshot_log_state",
        "_account_snapshot_log_keepalive_seconds",
    ):
        if name in vars(source):
            vars(runner)[name] = vars(source)[name]
    runner.app_context = source.context
    runner.service_bundle = source.service_bundle
    runner.services = LegacyRuntimeServiceView(runner.service_bundle)
    runner.runtime_services = runner.services


__all__ = ["configure_runtime_composition"]
