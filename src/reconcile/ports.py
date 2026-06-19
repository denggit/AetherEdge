from __future__ import annotations

from typing import Protocol

from src.reconcile.models import ReconcileReport


class ReconcileNotifier(Protocol):
    async def notify(self, report: ReconcileReport) -> None:
        ...
