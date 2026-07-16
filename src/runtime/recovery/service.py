from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from src.order_management.models import OrderIntent
from src.platform.exchanges.models import ExchangeName, Order, OrderStatus
from src.platform.exchanges.models import MarginMode
from src.platform.account.ports import AccountClient
from src.platform.execution.ports import ExecutionClient
from src.platform.snapshot import PlatformSnapshot, fetch_platform_snapshot
from src.platform.state.ports import StateStore
from src.reconcile.checker import Reconciler
from src.reconcile.models import ReconcileCategory, ReconcileIssue, ReconcileReport
from src.runtime.recovery.models import RecoveryReport
from src.runtime.strategy_capabilities import (
    DYNAMIC_STRATEGY_CAPABILITIES_VALIDATED,
    validate_dynamic_strategy_capabilities,
)
from src.runtime.strategy_positions import (
    StrategyPositionSnapshotIndex,
    resolve_strategy_position_snapshot_index,
)
from src.signals import TradeSignal
from src.strategy.ports import (
    StrategyPositionPlanRecoveryUpdateProvider,
    StrategyRecoveryContext,
)
from src.strategy.positions import StrategyPositionSnapshot

logger = logging.getLogger(__name__)

_AUTO_FIXABLE_MISSING_EXCHANGE_CATEGORIES = {
    ReconcileCategory.MISSING_EXCHANGE_ORDER,
    ReconcileCategory.MISSING_EXCHANGE_STOP_ORDER,
}


@dataclass(frozen=True)
class RecoveryExchangeContext:
    account: AccountClient
    execution: ExecutionClient
    state_store: StateStore
    reconciler: Reconciler | None = None
    leverage_margin_mode: MarginMode = MarginMode.CROSS


class RuntimeRecoveryService:
    """Generic startup recovery orchestration.

    It collects read-only platform snapshots, persists snapshots, runs reconcile
    checks, optionally loads order intents from a journal, and then calls a
    strategy's optional ``recover`` hook. It does not make plugin-specific decisions
    and it does not place/cancel orders.
    """

    def __init__(
        self,
        *,
        exchange_contexts: Sequence[RecoveryExchangeContext] = (),
        order_journal: Any | None = None,
        position_plan_store: Any | None = None,
        intent_ids: Sequence[str] = (),
    ) -> None:
        self.exchange_contexts = tuple(exchange_contexts)
        self.order_journal = order_journal
        self.position_plan_store = position_plan_store
        self.intent_ids = tuple(intent_ids)

    async def recover(self, *, strategy: object | None = None) -> RecoveryReport:
        snapshots: list[PlatformSnapshot] = []
        reconcile_reports: list[ReconcileReport] = []
        issues: list[str] = []

        for context in self.exchange_contexts:
            snapshot = await fetch_platform_snapshot(
                account=context.account,
                execution=context.execution,
                leverage_margin_mode=context.leverage_margin_mode,
            )
            snapshots.append(snapshot)
            context.state_store.save_snapshot(snapshot)
            self._auto_close_stale_local_orders_from_snapshot(context.state_store, snapshot)
            reconciler = context.reconciler or Reconciler(account=context.account, execution=context.execution, state_store=context.state_store)
            report = await reconciler.check()
            if self._has_auto_fixable_stale_local_order_issue(report):
                self._auto_close_stale_local_orders_from_snapshot(context.state_store, snapshot)
                report = await reconciler.check()
            reconcile_reports.append(report)
            issues.extend(issue.message for issue in self._fatal_reconcile_issues(report))

        order_intents = self._load_order_intents()
        active_position_plans = self._load_active_position_plans()
        strategy_position_index = resolve_strategy_position_snapshot_index(strategy)
        strategy_signals = await self._call_strategy_recover(
            strategy,
            snapshots=tuple(snapshots),
            reports=tuple(reconcile_reports),
            order_intents=order_intents,
            active_position_plans=active_position_plans,
            strategy_positions=strategy_position_index.snapshots,
            active_strategy_positions=strategy_position_index.active,
        )
        self._apply_strategy_position_plan_updates(strategy)
        dynamic_state = validate_dynamic_strategy_capabilities(strategy)
        recovered_strategy_position_index = StrategyPositionSnapshotIndex(
            dynamic_state.position_snapshots
        )
        ok = not issues
        return RecoveryReport(
            ok=ok,
            snapshots=tuple(snapshots),
            reconcile_reports=tuple(reconcile_reports),
            order_intents=order_intents,
            strategy_positions=recovered_strategy_position_index.snapshots,
            active_strategy_positions=recovered_strategy_position_index.active,
            strategy_signals=strategy_signals,
            issues=tuple(issues),
            metadata={
                "exchange_contexts": len(self.exchange_contexts),
                "intent_ids": len(self.intent_ids),
                "active_position_plans": active_position_plans,
                DYNAMIC_STRATEGY_CAPABILITIES_VALIDATED: True,
                "non_fatal_reconcile_issues": tuple(
                    issue.message
                    for report in reconcile_reports
                    for issue in report.issues
                    if self._is_auto_fixable_stale_local_order_issue(issue)
                ),
            },
        )

    def _auto_close_stale_local_orders_from_snapshot(self, state_store: StateStore, snapshot: PlatformSnapshot) -> None:
        marker = getattr(state_store, "mark_missing_open_orders_closed", None)
        if not callable(marker):
            return
        exchange = snapshot.balance.exchange
        symbol = snapshot.symbol
        for is_stop_order, orders, reason in (
            (False, snapshot.open_orders, "startup_recovery_missing_from_exchange_open_orders"),
            (True, snapshot.open_stop_orders, "startup_recovery_missing_from_exchange_open_stop_orders"),
        ):
            changed = marker(
                exchange=exchange,
                symbol=symbol,
                live_order_keys=_live_order_keys(orders),
                is_stop_order=is_stop_order,
                missing_status=OrderStatus.CANCELED,
                reason=reason,
            )
            logger.info(
                "Startup recovery auto-closed stale local orders | exchange=%s symbol=%s is_stop_order=%s changed=%s reason=%s",
                exchange.value if isinstance(exchange, ExchangeName) else exchange,
                symbol,
                str(is_stop_order).lower(),
                changed,
                reason,
            )

    def _fatal_reconcile_issues(self, report: ReconcileReport) -> tuple[ReconcileIssue, ...]:
        return tuple(issue for issue in report.issues if not self._is_auto_fixable_stale_local_order_issue(issue))

    def _has_auto_fixable_stale_local_order_issue(self, report: ReconcileReport) -> bool:
        return any(self._is_auto_fixable_stale_local_order_issue(issue) for issue in report.issues)

    def _is_auto_fixable_stale_local_order_issue(self, issue: ReconcileIssue) -> bool:
        return issue.category in _AUTO_FIXABLE_MISSING_EXCHANGE_CATEGORIES

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

    def _load_active_position_plans(self) -> tuple[dict[str, Any], ...]:
        if self.position_plan_store is None:
            return ()
        serializer = getattr(self.position_plan_store, "serialize_active_positions", None)
        if not callable(serializer):
            return ()
        return tuple(serializer())

    def _apply_strategy_position_plan_updates(self, strategy: object | None) -> None:
        """Apply explicit recovery decisions without duplicating strategy logic."""

        if self.position_plan_store is None or strategy is None:
            return
        apply_resolution = getattr(
            self.position_plan_store, "apply_recovery_leg_resolution", None
        )
        if not isinstance(
            strategy,
            StrategyPositionPlanRecoveryUpdateProvider,
        ) or not callable(apply_resolution):
            return
        for raw in strategy.position_plan_recovery_updates() or ():
            if not isinstance(raw, dict):
                continue
            position_id = str(raw.get("position_id") or "").strip()
            exchange = str(raw.get("exchange") or "").strip().lower()
            sync_status = str(raw.get("sync_status") or "").strip().lower()
            if not position_id or not exchange or not sync_status:
                continue
            metadata = raw.get("metadata")
            apply_resolution(
                position_id=position_id,
                exchange=exchange,
                sync_status=sync_status,
                metadata=dict(metadata) if isinstance(metadata, dict) else {},
            )
            logger.info(
                "Strategy recovery position-plan resolution applied | "
                "position_id=%s exchange=%s sync_status=%s reason=%s",
                position_id,
                exchange,
                sync_status,
                metadata.get("reason") if isinstance(metadata, dict) else None,
            )

    async def _call_strategy_recover(
        self,
        strategy: object | None,
        *,
        snapshots: tuple[PlatformSnapshot, ...],
        reports: tuple[ReconcileReport, ...],
        order_intents: tuple[OrderIntent, ...],
        active_position_plans: tuple[dict[str, Any], ...],
        strategy_positions: tuple[StrategyPositionSnapshot, ...],
        active_strategy_positions: tuple[StrategyPositionSnapshot, ...],
    ) -> tuple[TradeSignal, ...]:
        recover = getattr(strategy, "recover", None)
        if not callable(recover):
            return ()
        context = StrategyRecoveryContext(
            snapshots=snapshots,
            reconcile_reports=reports,
            order_intent_ids=tuple(intent.intent_id for intent in order_intents),
            metadata={
                "order_intent_count": len(order_intents),
                "active_position_plans": active_position_plans,
                "strategy_positions": strategy_positions,
                "active_strategy_positions": active_strategy_positions,
            },
        )
        result = await recover(context)
        return tuple(result or ())


def _live_order_keys(orders: Sequence[Order]) -> set[tuple[str | None, str | None]]:
    return {(order.order_id, order.client_order_id) for order in orders}
