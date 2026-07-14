from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass


StartupSnapshots = tuple[object, ...]

SyncStartupStep = Callable[[], None]
AsyncStartupStep = Callable[[], Awaitable[None]]
RangeSpeedWarmupStep = Callable[[], Awaitable[int]]
RangeSpeedResultStep = Callable[[int], None]
RecoveryStep = Callable[[], Awaitable[StartupSnapshots]]
SnapshotsAsyncStep = Callable[[StartupSnapshots], Awaitable[None]]
SnapshotAsyncStep = Callable[[object], Awaitable[None]]


@dataclass(frozen=True)
class RuntimeStartupPhasePlan:
    initialize_rangebar_trust_window: SyncStartupStep
    enter_warming_up: SyncStartupStep
    bootstrap_account_config: AsyncStartupStep
    check_position_mode: AsyncStartupStep
    run_warmup: AsyncStartupStep
    warmup_range_speed_history: RangeSpeedWarmupStep
    handle_range_speed_history_result: RangeSpeedResultStep
    check_feature_backfills: AsyncStartupStep
    enter_catching_up: SyncStartupStep
    run_recovery: RecoveryStep
    run_post_recovery_checks: SnapshotsAsyncStep
    run_reconciliation: SnapshotsAsyncStep
    call_strategy_on_start: SnapshotAsyncStep
    evaluate_startup_catchup: SnapshotAsyncStep
    finish_range_speed_warmup: AsyncStartupStep
    start_heartbeat: SyncStartupStep
    start_range_speed_background_services: SyncStartupStep
    enter_running: SyncStartupStep


class RuntimeStartupPhaseCoordinator:
    async def execute(
        self,
        plan: RuntimeStartupPhasePlan,
    ) -> StartupSnapshots:
        plan.initialize_rangebar_trust_window()
        plan.enter_warming_up()

        await plan.bootstrap_account_config()
        await plan.check_position_mode()
        await plan.run_warmup()

        loaded_range_speed_history = await plan.warmup_range_speed_history()
        plan.handle_range_speed_history_result(loaded_range_speed_history)

        await plan.check_feature_backfills()
        plan.enter_catching_up()

        snapshots = await plan.run_recovery()
        await plan.run_post_recovery_checks(snapshots)
        await plan.run_reconciliation(snapshots)

        first_snapshot = snapshots[0]
        await plan.call_strategy_on_start(first_snapshot)
        await plan.evaluate_startup_catchup(first_snapshot)

        await plan.finish_range_speed_warmup()
        plan.start_heartbeat()
        plan.start_range_speed_background_services()
        plan.enter_running()

        return snapshots


__all__ = [
    "RuntimeStartupPhaseCoordinator",
    "RuntimeStartupPhasePlan",
    "StartupSnapshots",
]
