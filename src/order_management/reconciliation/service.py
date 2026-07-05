"""Live startup state reconciliation service.

Compares exchange truth (positions, open orders, open stop orders) against
local state (PositionPlanStore, OrderJournal, StateStore) and repairs stale /
inconsistent local state before live trading begins.

This is called ONCE at startup, after recovery snapshots are collected and
before producers / sync tasks are started. It is the ONLY place that writes
corrective state during startup convergence.
"""

from __future__ import annotations

import time
from dataclasses import replace
from decimal import Decimal
from typing import Any

from src.app.alerts import AppAlert
from src.order_management.models import OrderJournalEvent, OrderIntentStatus
from src.order_management.position_plan.models import (
    LegPlan,
    LegSyncStatus,
    PositionPlanStatus,
)
from src.order_management.reconciliation.models import (
    FakeOrderRef,
    LiveStateReconciliationReport,
    ReconciliationAction,
    ReconciliationVerdict,
)
from src.order_management.reconciliation.validation import (
    is_fake_order_id,
    is_valid_client_order_id,
    is_valid_exchange_order_id,
)
from src.platform.exchanges.models import ExchangeName, Position
from src.platform.snapshot import PlatformSnapshot
from src.utils.log import get_logger

logger = get_logger(__name__)

# ── Reasons used in journal events ──
REASON_NO_EXCHANGE_POSITION_OR_OPEN_ORDERS = "no_exchange_position_or_open_orders"
SOURCE_LIVE_STARTUP_RECONCILIATION = "live_startup_reconciliation"


def _exchange_has_position(positions: list[Position], exchange: ExchangeName) -> bool:
    """Return True if any position for *exchange* has non-zero quantity."""
    return any(
        p.quantity != Decimal("0") and p.exchange == exchange for p in positions
    )


def _exchange_is_flat(
    positions: list[Position],
    open_orders: list[Any],
    open_stop_orders: list[Any],
    exchange: ExchangeName,
) -> bool:
    """Return True when *exchange* has no positions, no open orders, and no open stops."""
    has_pos = _exchange_has_position(positions, exchange)
    has_orders = any(
        getattr(o, "exchange", None) == exchange for o in open_orders
    )
    has_stops = any(
        getattr(o, "exchange", None) == exchange for o in open_stop_orders
    )
    return not has_pos and not has_orders and not has_stops


def _all_exchanges_flat(
    snapshots_by_exchange: dict[ExchangeName, PlatformSnapshot],
) -> bool:
    """Return True when every exchange snapshot is completely flat."""
    return all(
        _exchange_is_flat(
            s.positions, s.open_orders, s.open_stop_orders, exchange
        )
        for exchange, s in snapshots_by_exchange.items()
    )


def _detect_fake_order_refs(
    plan: Any,
    legs: tuple[LegPlan, ...],
) -> list[FakeOrderRef]:
    """Scan a position plan and its legs for fake/invalid order IDs."""
    refs: list[FakeOrderRef] = []
    for leg in legs:
        exchange_name = leg.exchange.value if hasattr(leg.exchange, "value") else str(leg.exchange)
        role = leg.role.value if hasattr(leg.role, "value") else str(leg.role)

        # Check entry order ID
        if leg.entry_order_id:
            if is_fake_order_id(leg.entry_order_id) or not is_valid_exchange_order_id(
                leg.exchange, leg.entry_order_id
            ):
                refs.append(
                    FakeOrderRef(
                        position_id=plan.position_id,
                        exchange=exchange_name,
                        leg_role=role,
                        field="entry_order_id",
                        value=leg.entry_order_id,
                        reason=(
                            "pattern_match"
                            if is_fake_order_id(leg.entry_order_id)
                            else "non_numeric_exchange_id"
                        ),
                    )
                )

        # Check stop order ID
        if leg.stop_order_id:
            if is_fake_order_id(leg.stop_order_id) or not is_valid_exchange_order_id(
                leg.exchange, leg.stop_order_id
            ):
                refs.append(
                    FakeOrderRef(
                        position_id=plan.position_id,
                        exchange=exchange_name,
                        leg_role=role,
                        field="stop_order_id",
                        value=leg.stop_order_id,
                        reason=(
                            "pattern_match"
                            if is_fake_order_id(leg.stop_order_id)
                            else "non_numeric_exchange_id"
                        ),
                    )
                )

    return refs


class LiveStateReconciliationService:
    """Startup state convergence: compare exchange truth against local state and repair.

    Usage in runner:
        service = LiveStateReconciliationService(...)
        report = await service.reconcile_and_apply(snapshots)
        if not report.ok:
            # handle failure
    """

    def __init__(
        self,
        *,
        position_plan_store: Any,  # SqlitePositionPlanStore
        order_journal: Any,  # SqliteOrderJournalStore
        state_store: Any,  # StateStore
        alert_sink: Any | None = None,
    ) -> None:
        self._position_plan_store = position_plan_store
        self._order_journal = order_journal
        self._state_store = state_store
        self._alert_sink = alert_sink

    # ── Public API ──────────────────────────────────────────────────────

    async def reconcile(
        self, snapshots: tuple[PlatformSnapshot, ...]
    ) -> LiveStateReconciliationReport:
        """Read-only comparison. Produces actions but does NOT apply them."""
        return self._build_report(snapshots)

    async def apply(self, report: LiveStateReconciliationReport) -> None:
        """Apply previously-computed reconciliation actions to local stores."""
        self._apply_actions(report.actions, report.symbol)

    async def reconcile_and_apply(
        self, snapshots: tuple[PlatformSnapshot, ...]
    ) -> LiveStateReconciliationReport:
        """Run reconcile, then apply. Returns the report after application."""
        report = self._build_report(snapshots)
        self._apply_actions(report.actions, report.symbol)
        # Re-evaluate active position state after apply
        report.active_position_after = bool(
            self._position_plan_store
            and callable(getattr(self._position_plan_store, "list_active_positions", None))
            and self._position_plan_store.list_active_positions()
        )
        report.ok = report.verdict in {
            ReconciliationVerdict.PASS,
            ReconciliationVerdict.PASS_WITH_CLEANUP,
        }
        return report

    # ── Report building ─────────────────────────────────────────────────

    def _build_report(
        self, snapshots: tuple[PlatformSnapshot, ...]
    ) -> LiveStateReconciliationReport:
        now_ms = int(time.time() * 1000)
        symbol = snapshots[0].symbol if snapshots else "unknown"
        exchanges = tuple(s.exchange.value if hasattr(s, "exchange") else s.leverage.exchange.value for s in snapshots)

        snapshots_by_exchange: dict[ExchangeName, PlatformSnapshot] = {}
        for s in snapshots:
            exchange = s.leverage.exchange
            snapshots_by_exchange[exchange] = s

        report = LiveStateReconciliationReport(
            checked_at_ms=now_ms,
            exchanges=exchanges,
            symbol=symbol,
            ok=True,
            exchange_positions={
                ex.value: sum(1 for p in s.positions if p.quantity != Decimal("0"))
                for ex, s in snapshots_by_exchange.items()
            },
            exchange_open_orders={
                ex.value: len(s.open_orders)
                for ex, s in snapshots_by_exchange.items()
            },
            exchange_open_stops={
                ex.value: len(s.open_stop_orders)
                for ex, s in snapshots_by_exchange.items()
            },
        )

        # Collect all active position plans
        active_plans = (
            self._position_plan_store.list_active_positions()
            if self._position_plan_store is not None
            else ()
        )

        if not active_plans:
            report.ok = True
            report.verdict = ReconciliationVerdict.PASS
            report.active_position_after = False
            return report

        all_flat = _all_exchanges_flat(snapshots_by_exchange)

        for plan in active_plans:
            legs = (
                self._position_plan_store.get_legs(plan.position_id)
                if self._position_plan_store is not None
                else ()
            )
            self._reconcile_plan(
                plan=plan,
                legs=legs,
                snapshots_by_exchange=snapshots_by_exchange,
                all_flat=all_flat,
                master_exchange=plan.master_exchange,
                report=report,
            )

        # Determine final verdict
        if report.actions and not report.issues:
            report.verdict = ReconciliationVerdict.PASS_WITH_CLEANUP
            report.ok = True
        elif report.issues:
            if any("unresolved_follower" in issue for issue in report.issues):
                report.verdict = ReconciliationVerdict.FAIL_UNRESOLVED_FOLLOWER_POSITION
                report.ok = False
            elif report.stale_plans_closed > 0 or report.fake_order_refs_found:
                report.verdict = ReconciliationVerdict.FAIL_NEEDS_RECONCILE
                report.ok = False
            else:
                report.verdict = ReconciliationVerdict.FAIL_CONFIG
                report.ok = False
        else:
            report.verdict = ReconciliationVerdict.PASS
            report.ok = True

        report.active_position_after = bool(
            self._position_plan_store
            and self._position_plan_store.list_active_positions()
        )

        return report

    def _reconcile_plan(
        self,
        *,
        plan: Any,
        legs: tuple[LegPlan, ...],
        snapshots_by_exchange: dict[ExchangeName, PlatformSnapshot],
        all_flat: bool,
        master_exchange: ExchangeName,
        report: LiveStateReconciliationReport,
    ) -> None:
        plan_ref = f"position_plan:{plan.position_id}"

        # ── Case 1: All exchanges flat + local active plan ──
        if all_flat and plan.status != PositionPlanStatus.CLOSED:
            report.actions.append(
                ReconciliationAction(
                    action_type="close_stale_plan",
                    target=plan_ref,
                    detail={
                        "reason": REASON_NO_EXCHANGE_POSITION_OR_OPEN_ORDERS,
                        "source": SOURCE_LIVE_STARTUP_RECONCILIATION,
                        "current_status": (
                            plan.status.value
                            if hasattr(plan.status, "value")
                            else str(plan.status)
                        ),
                    },
                )
            )
            for leg in legs:
                report.actions.append(
                    ReconciliationAction(
                        action_type="mark_leg_stale",
                        target=f"leg:{plan.position_id}:{leg.exchange.value}",
                        detail={
                            "reason": REASON_NO_EXCHANGE_POSITION_OR_OPEN_ORDERS,
                            "source": SOURCE_LIVE_STARTUP_RECONCILIATION,
                        },
                    )
                )
                # ── All exchanges flat → clear ALL order refs (exchange + client) ──
                if leg.entry_order_id:
                    report.actions.append(
                        ReconciliationAction(
                            action_type="clear_all_entry_order_refs",
                            target=f"leg:{plan.position_id}:{leg.exchange.value}",
                            detail={"value": leg.entry_order_id},
                        )
                    )
                if leg.stop_order_id:
                    report.actions.append(
                        ReconciliationAction(
                            action_type="clear_all_stop_order_refs",
                            target=f"leg:{plan.position_id}:{leg.exchange.value}",
                            detail={"value": leg.stop_order_id},
                        )
                    )
            report.stale_plans_closed += 1

        # ── Case 4: Detect fake order IDs (runs regardless of Case 1) ──
        fake_refs = _detect_fake_order_refs(plan, legs)
        for fake in fake_refs:
            report.fake_order_refs_found.append(fake)

        # If Case 1 didn't already close this plan, handle fake IDs individually
        if not all_flat:
            for fake in fake_refs:
                exchange_name = ExchangeName(fake.exchange)
                exchange_flat = all(
                    _exchange_is_flat(
                        s.positions, s.open_orders, s.open_stop_orders, exchange_name
                    )
                    for s in snapshots_by_exchange.values()
                    if s.leverage.exchange == exchange_name
                )
                if exchange_flat:
                    # Exchange flat, safe to close plan — clear ALL refs
                    if not any(
                        a.action_type == "close_stale_plan" and a.target == plan_ref
                        for a in report.actions
                    ):
                        report.actions.append(
                            ReconciliationAction(
                                action_type="close_stale_plan",
                                target=plan_ref,
                                detail={
                                    "reason": f"fake_order_id_and_exchange_flat:{fake.field}",
                                    "source": SOURCE_LIVE_STARTUP_RECONCILIATION,
                                    "fake_value": fake.value,
                                },
                            )
                        )
                        report.stale_plans_closed += 1
                else:
                    # Exchange has position — clear only the fake exchange ID,
                    # PRESERVE client_order_id for diagnostics and query fallback.
                    report.actions.append(
                        ReconciliationAction(
                            action_type=f"clear_fake_{fake.field}",
                            target=f"leg:{plan.position_id}:{fake.exchange}",
                            detail={
                                "fake_value": fake.value,
                                "reason": fake.reason,
                            },
                        )
                    )
                    report.issues.append(
                        f"fake_order_id_in_active_position:{fake.position_id}:{fake.exchange}:{fake.field}:{fake.value}"
                    )

        # ── Check master/follower specific cases ──
        if not all_flat:
            master_snapshot = snapshots_by_exchange.get(master_exchange)
            if master_snapshot is None:
                return

            master_flat = _exchange_is_flat(
                master_snapshot.positions,
                master_snapshot.open_orders,
                master_snapshot.open_stop_orders,
                master_exchange,
            )

            follower_exchanges = [
                leg.exchange for leg in legs if leg.exchange != master_exchange
            ]
            follower_flat = all(
                _exchange_is_flat(
                    snapshots_by_exchange[ex].positions,
                    snapshots_by_exchange[ex].open_orders,
                    snapshots_by_exchange[ex].open_stop_orders,
                    ex,
                )
                for ex in follower_exchanges
                if ex in snapshots_by_exchange
            )
            follower_open = any(
                not _exchange_is_flat(
                    snapshots_by_exchange[ex].positions,
                    snapshots_by_exchange[ex].open_orders,
                    snapshots_by_exchange[ex].open_stop_orders,
                    ex,
                )
                for ex in follower_exchanges
                if ex in snapshots_by_exchange
            )

            # ── Case 2: Master flat + follower still open ──
            if master_flat and follower_open:
                if plan.status != PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED:
                    report.actions.append(
                        ReconciliationAction(
                            action_type="set_master_closed_follower_close_required",
                            target=plan_ref,
                            detail={
                                "reason": "master_flat_follower_open",
                                "source": SOURCE_LIVE_STARTUP_RECONCILIATION,
                                "follower_exchanges": [
                                    ex.value for ex in follower_exchanges
                                    if ex in snapshots_by_exchange
                                    and not _exchange_is_flat(
                                        snapshots_by_exchange[ex].positions,
                                        snapshots_by_exchange[ex].open_orders,
                                        snapshots_by_exchange[ex].open_stop_orders,
                                        ex,
                                    )
                                ],
                            },
                        )
                    )
                report.unresolved_follower_positions += sum(
                    1
                    for ex in follower_exchanges
                    if ex in snapshots_by_exchange
                    and not _exchange_is_flat(
                        snapshots_by_exchange[ex].positions,
                        snapshots_by_exchange[ex].open_orders,
                        snapshots_by_exchange[ex].open_stop_orders,
                        ex,
                    )
                )
                report.issues.append(
                    f"master_flat_follower_open:{plan.position_id}"
                )
                report.alerts.append(
                    {
                        "subject": "AetherEdge master flat but follower still has position",
                        "severity": "error",
                        "content": (
                            f"position_id={plan.position_id}\n"
                            f"master_exchange={master_exchange.value}\n"
                            f"reason=master_flat_follower_open\n"
                            f"source={SOURCE_LIVE_STARTUP_RECONCILIATION}"
                        ),
                    }
                )

            # ── Case 3: Master open + follower flat ──
            if not master_flat and follower_flat and follower_exchanges:
                report.actions.append(
                    ReconciliationAction(
                        action_type="block_new_entries_alert",
                        target=plan_ref,
                        detail={
                            "reason": "master_open_follower_flat",
                            "source": SOURCE_LIVE_STARTUP_RECONCILIATION,
                            "follower_exchanges": [
                                ex.value for ex in follower_exchanges
                            ],
                        },
                    )
                )
                report.issues.append(
                    f"master_open_follower_flat:{plan.position_id}"
                )
                report.alerts.append(
                    {
                        "subject": "AetherEdge master open but follower flat",
                        "severity": "error",
                        "content": (
                            f"position_id={plan.position_id}\n"
                            f"master_exchange={master_exchange.value}\n"
                            f"reason=master_open_follower_flat\n"
                            f"source={SOURCE_LIVE_STARTUP_RECONCILIATION}"
                        ),
                    }
                )

    # ── Apply actions ───────────────────────────────────────────────────

    def _apply_actions(
        self, actions: list[ReconciliationAction], symbol: str
    ) -> None:
        """Write corrective state to local stores based on computed actions."""
        store = self._position_plan_store
        if store is None:
            logger.warning("Reconciliation apply skipped — no position plan store")
            return

        now_ms = int(time.time() * 1000)
        applied_close: set[str] = set()
        applied_stale: set[str] = set()

        for action in actions:
            if action.action_type == "close_stale_plan":
                position_id = action.target.split(":", 1)[1]
                if position_id in applied_close:
                    continue
                applied_close.add(position_id)

                plan = store.get_position(position_id)
                if plan is not None and plan.status != PositionPlanStatus.CLOSED:
                    store.upsert_position(
                        replace(
                            plan,
                            status=PositionPlanStatus.CLOSED,
                            updated_time_ms=now_ms,
                            metadata={
                                **dict(plan.metadata),
                                "reconciled_at_ms": now_ms,
                                "reconcile_reason": action.detail.get(
                                    "reason", "unknown"
                                ),
                                "reconcile_source": action.detail.get(
                                    "source", SOURCE_LIVE_STARTUP_RECONCILIATION
                                ),
                            },
                        )
                    )
                    # Write journal event for audit trail
                    self._write_journal_event(
                        position_id=position_id,
                        status="closed",
                        message=(
                            f"Stale plan closed by startup reconciliation: "
                            f"reason={action.detail.get('reason', 'unknown')}"
                        ),
                    )
                    logger.warning(
                        "Startup reconciliation: closed stale position plan | "
                        "position_id=%s reason=%s",
                        position_id,
                        action.detail.get("reason", "unknown"),
                    )

            elif action.action_type == "mark_leg_stale":
                parts = action.target.split(":")
                if len(parts) < 3:
                    continue
                _, position_id, exchange_str = parts
                stale_key = f"{position_id}:{exchange_str}"
                if stale_key in applied_stale:
                    continue
                applied_stale.add(stale_key)

                store.update_leg_sync_status(
                    position_id=position_id,
                    exchange=ExchangeName(exchange_str),
                    sync_status=LegSyncStatus.STALE_RECONCILED,
                )
                logger.info(
                    "Startup reconciliation: marked leg stale | "
                    "position_id=%s exchange=%s",
                    position_id,
                    exchange_str,
                )

            elif action.action_type in (
                "clear_fake_entry_order_id",
                "clear_fake_stop_order_id",
                "clear_fake_entry_exchange_order_id",
                "clear_fake_stop_exchange_order_id",
            ):
                # ── Granular: clear exchange order ID only, preserve client_order_id ──
                parts = action.target.split(":")
                if len(parts) < 3:
                    continue
                _, position_id, exchange_str = parts
                is_entry = action.action_type in (
                    "clear_fake_entry_order_id",
                    "clear_fake_entry_exchange_order_id",
                )
                is_stop = action.action_type in (
                    "clear_fake_stop_order_id",
                    "clear_fake_stop_exchange_order_id",
                )

                store.clear_leg_order_refs(
                    position_id=position_id,
                    exchange=ExchangeName(exchange_str),
                    clear_entry_exchange_order_id=is_entry,
                    clear_stop_exchange_order_id=is_stop,
                    # client_order_id is deliberately preserved
                )
                logger.warning(
                    "Startup reconciliation: cleared fake exchange order ID "
                    "(client_order_id preserved) | "
                    "position_id=%s exchange=%s field=%s value=%s",
                    position_id,
                    exchange_str,
                    "entry_order_id" if is_entry else "stop_order_id",
                    action.detail.get("fake_value", "unknown"),
                )

            elif action.action_type in (
                "clear_all_entry_order_refs",
                "clear_all_stop_order_refs",
            ):
                # ── Bulk: clear both exchange AND client order IDs (stale plans) ──
                parts = action.target.split(":")
                if len(parts) < 3:
                    continue
                _, position_id, exchange_str = parts
                clear_entry = action.action_type == "clear_all_entry_order_refs"
                clear_stop = action.action_type == "clear_all_stop_order_refs"

                store.clear_leg_order_ids(
                    position_id=position_id,
                    exchange=ExchangeName(exchange_str),
                    clear_entry_order_id=clear_entry,
                    clear_stop_order_id=clear_stop,
                )
                logger.warning(
                    "Startup reconciliation: cleared all order refs (bulk) | "
                    "position_id=%s exchange=%s field=%s",
                    position_id,
                    exchange_str,
                    "entry" if clear_entry else "stop",
                )

            elif action.action_type == "set_master_closed_follower_close_required":
                position_id = action.target.split(":", 1)[1]
                plan = store.get_position(position_id)
                if plan is not None and plan.status not in {
                    PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
                    PositionPlanStatus.CLOSED,
                }:
                    store.upsert_position(
                        replace(
                            plan,
                            status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
                            updated_time_ms=now_ms,
                            metadata={
                                **dict(plan.metadata),
                                "reconciled_at_ms": now_ms,
                                "reconcile_reason": action.detail.get(
                                    "reason", "unknown"
                                ),
                            },
                        )
                    )
                    self._write_journal_event(
                        position_id=position_id,
                        status="master_closed_follower_close_required",
                        message=(
                            "Master flat but follower open — "
                            "entering follower close required state"
                        ),
                    )
                    logger.warning(
                        "Startup reconciliation: master flat, follower open | "
                        "position_id=%s",
                        position_id,
                    )

            elif action.action_type == "block_new_entries_alert":
                if self._alert_sink is not None:
                    emit = getattr(self._alert_sink, "emit", None)
                    if callable(emit):
                        emit(
                            AppAlert(
                                subject="AetherEdge master open but follower flat",
                                content=(
                                    f"position_id={action.target}\n"
                                    f"reason={action.detail.get('reason', 'unknown')}\n"
                                    f"source={SOURCE_LIVE_STARTUP_RECONCILIATION}"
                                ),
                                severity="error",
                            )
                        )
                logger.error(
                    "Startup reconciliation: master open, follower flat | "
                    "position_id=%s reason=%s",
                    action.target,
                    action.detail.get("reason", "unknown"),
                )

    def _write_journal_event(
        self,
        *,
        position_id: str,
        status: str,
        message: str,
    ) -> None:
        """Write a reconciliation journal event for audit trail."""
        if self._order_journal is None:
            return
        add_event = getattr(self._order_journal, "add_event", None)
        if not callable(add_event):
            return
        try:
            event = OrderJournalEvent(
                intent_id=position_id,
                status=OrderIntentStatus(
                    status
                ) if status in {s.value for s in OrderIntentStatus} else OrderIntentStatus.FAILED,
                message=message,
                exchange=None,
                created_time_ms=int(time.time() * 1000),
                metadata={
                    "source": SOURCE_LIVE_STARTUP_RECONCILIATION,
                },
            )
            add_event(event)
        except Exception as exc:
            logger.debug(
                "Reconciliation journal event skipped | position_id=%s error=%s",
                position_id,
                exc,
            )
