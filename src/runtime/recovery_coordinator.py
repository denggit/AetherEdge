from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


RecoverySnapshots = tuple[object, ...]
RecoverySignalBatch = list[Any]

ResolveRecoveryService = Callable[[], object | None]
FallbackSnapshots = Callable[[], RecoverySnapshots]
InvokeRecoveryService = Callable[[object], Awaitable[Any]]
RecordRecoveryRun = Callable[[], None]
ValidateRecoveryReport = Callable[[Any], None]
PartitionRecoverySignals = Callable[
    [Any],
    tuple[RecoverySignalBatch, RecoverySignalBatch],
]
CaptureFailureCounts = Callable[[], tuple[int, int]]
ExecuteRecoverySignals = Callable[[RecoverySignalBatch], Awaitable[None]]
ValidateStopExecution = Callable[[tuple[int, int]], None]
AsyncRecoveryStep = Callable[[], Awaitable[None]]
FinalizeRecoveryReport = Callable[[Any], RecoverySnapshots]


@dataclass(frozen=True)
class RuntimeRecoveryPlan:
    resolve_service: ResolveRecoveryService
    fallback_snapshots: FallbackSnapshots
    invoke_service: InvokeRecoveryService
    record_run: RecordRecoveryRun
    validate_report: ValidateRecoveryReport
    partition_signals: PartitionRecoverySignals
    capture_failure_counts: CaptureFailureCounts
    execute_stop_signals: ExecuteRecoverySignals
    validate_stop_execution: ValidateStopExecution
    validate_post_execution_protection: AsyncRecoveryStep
    execute_other_signals: ExecuteRecoverySignals
    finalize_report: FinalizeRecoveryReport


class RuntimeRecoveryCoordinator:
    async def execute(
        self,
        plan: RuntimeRecoveryPlan,
    ) -> RecoverySnapshots:
        service = plan.resolve_service()
        if service is None:
            return plan.fallback_snapshots()

        report = await plan.invoke_service(service)
        plan.record_run()
        plan.validate_report(report)

        stop_signals, other_signals = plan.partition_signals(report)

        if stop_signals:
            failure_counts = plan.capture_failure_counts()
            await plan.execute_stop_signals(stop_signals)
            plan.validate_stop_execution(failure_counts)
            await plan.validate_post_execution_protection()

        if other_signals:
            await plan.execute_other_signals(other_signals)

        return plan.finalize_report(report)


__all__ = [
    "RuntimeRecoveryCoordinator",
    "RuntimeRecoveryPlan",
    "RecoverySnapshots",
]
