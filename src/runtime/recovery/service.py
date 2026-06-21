from __future__ import annotations

from src.runtime.recovery.models import RecoveryReport


class RuntimeRecoveryService:
    """Generic recovery orchestrator placeholder.

    Later phases will connect platform snapshots, order journal and reconcile
    reports here. Concrete strategy recovery decisions must still live in the
    strategy plugin.
    """

    async def recover(self) -> RecoveryReport:
        return RecoveryReport(ok=True)
