from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from src.order_management.models import OrderIntent
from src.platform.snapshot import PlatformSnapshot
from src.reconcile.models import ReconcileReport
from src.signals import TradeSignal


@dataclass(frozen=True)
class RecoveryReport:
    """Generic runtime recovery report.

    Strategy-specific recovery remains inside strategy plugins. This report is
    intentionally generic so runtime does not learn V8 internals.
    """

    ok: bool
    snapshots: tuple[PlatformSnapshot, ...] = ()
    reconcile_reports: tuple[ReconcileReport, ...] = ()
    order_intents: tuple[OrderIntent, ...] = ()
    strategy_signals: tuple[TradeSignal, ...] = ()
    issues: tuple[str, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=dict)
