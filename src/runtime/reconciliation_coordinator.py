from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


ReconciliationSnapshots = tuple[object, ...]

ResolveReconciliationService = Callable[[], object | None]
ValidateReconciliationSnapshots = Callable[
    [ReconciliationSnapshots],
    None,
]
BeginReconciliation = Callable[[ReconciliationSnapshots], None]
ApplyLegacyAdoptions = Callable[[object], None]
InvokeReconciliationService = Callable[
    [object, ReconciliationSnapshots],
    Awaitable[Any],
]
HandleReconciliationReport = Callable[[Any], None]


@dataclass(frozen=True)
class RuntimeReconciliationPlan:
    resolve_service: ResolveReconciliationService
    validate_snapshots: ValidateReconciliationSnapshots
    begin_reconciliation: BeginReconciliation
    apply_legacy_adoptions: ApplyLegacyAdoptions
    invoke_service: InvokeReconciliationService
    handle_report: HandleReconciliationReport


class RuntimeReconciliationCoordinator:
    async def execute(
        self,
        snapshots: ReconciliationSnapshots,
        plan: RuntimeReconciliationPlan,
    ) -> None:
        service = plan.resolve_service()
        if service is None:
            return

        plan.validate_snapshots(snapshots)
        plan.begin_reconciliation(snapshots)
        plan.apply_legacy_adoptions(service)

        report = await plan.invoke_service(service, snapshots)
        plan.handle_report(report)


__all__ = [
    "ReconciliationSnapshots",
    "RuntimeReconciliationCoordinator",
    "RuntimeReconciliationPlan",
]
