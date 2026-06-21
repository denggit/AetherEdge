from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from src.order_management.models import OrderIntent
from src.platform.account.ports import AccountClient
from src.platform.execution.ports import ExecutionClient
from src.platform.snapshot import PlatformSnapshot, fetch_platform_snapshot
from src.platform.state.ports import StateStore
from src.reconcile.checker import Reconciler
from src.reconcile.models import ReconcileReport
from src.signals import TradeSignal
from src.strategy.ports import StrategyRecoveryContext
from src.runtime.recovery.models import RecoveryReport


@dataclass(frozen=True)
class RecoveryExchangeContext:
    account: AccountClient
    execution: ExecutionClient
    state_store: StateStore
    reconciler: Reconciler | None = None


class RuntimeRecoveryService:
    """Generic startup recovery orchestration.

    It collects read-only platform snapshots, persists snapshots, runs reconcile
    checks, optionally loads order intents from a journal, and then calls a
    strategy's optional ``recover`` hook. It does not make V8-specific decisions
    and it does not place/cancel orders.
    """

    def __init__(
        self,
        *,
        exchange_contexts: Sequence[RecoveryExchangeContext] = (),
        order_journal: Any | None = None,
        intent_ids: Sequence[str] = (),
    ) -> None:
        self.exchange_contexts = tuple(exchange_contexts)
        self.order_journal = order_journal
        self.intent_ids = tuple(intent_ids)

    async def recover(self, *, strategy: object | None = None) -> RecoveryReport:
        snapshots: list[PlatformSnapshot] = []
        reconcile_reports: list[ReconcileReport] = []
        issues: list[str] = []

        for context in self.exchange_contexts:
            snapshot = await fetch_platform_snapshot(account=context.account, execution=context.execution)
            snapshots.append(snapshot)
            context.state_store.save_snapshot(snapshot)
            reconciler = context.reconciler or Reconciler(account=context.account, execution=context.execution, state_store=context.state_store)
            report = await reconciler.check()
            reconcile_reports.append(report)
            issues.extend(issue.message for issue in report.issues)

        order_intents = self._load_order_intents()
        strategy_signals = await self._call_strategy_recover(strategy, snapshots=tuple(snapshots), reports=tuple(reconcile_reports), order_intents=order_intents)
        ok = not issues
        return RecoveryReport(
            ok=ok,
            snapshots=tuple(snapshots),
            reconcile_reports=tuple(reconcile_reports),
            order_intents=order_intents,
            strategy_signals=strategy_signals,
            issues=tuple(issues),
            metadata={"exchange_contexts": len(self.exchange_contexts), "intent_ids": len(self.intent_ids)},
        )

    def _load_order_intents(self) -> tuple[OrderIntent, ...]:
        if self.order_journal is None or not self.intent_ids:
            return ()
        out: list[OrderIntent] = []
        getter = getattr(self.order_journal, "get_intent", None)
        if getter is None:
            return ()
        for intent_id in self.intent_ids:
            item = getter(intent_id)
            if item is not None:
                out.append(item)
        return tuple(out)

    async def _call_strategy_recover(
        self,
        strategy: object | None,
        *,
        snapshots: tuple[PlatformSnapshot, ...],
        reports: tuple[ReconcileReport, ...],
        order_intents: tuple[OrderIntent, ...],
    ) -> tuple[TradeSignal, ...]:
        recover = getattr(strategy, "recover", None)
        if not callable(recover):
            return ()
        context = StrategyRecoveryContext(
            snapshots=snapshots,
            reconcile_reports=reports,
            order_intent_ids=tuple(intent.intent_id for intent in order_intents),
            metadata={"order_intent_count": len(order_intents)},
        )
        result = await recover(context)
        return tuple(result or ())
