from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from src.app.alerts import AppAlert
from src.order_management import LegSyncStatus, MasterFollowerExecutionPolicy, MultiExchangeOrderCoordinator, PositionPlanStatus, RepositoryDuplicateOrderGuard, SqliteOrderJournalStore, SqlitePositionPlanStore
from src.order_management.position_plan.models import LegRole
from src.order_management.reconciliation.service import LiveStateReconciliationService
from src.platform import create_account_client, create_execution_client
from src.platform.account.events import AccountEvent
from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, InstrumentRule, Order, OrderStatus, Position, PositionMode, PositionSide
from src.platform.snapshot import PlatformSnapshot
from src.runtime.account_config import (
    AccountConfigBootstrapResult,
    AccountConfigEnv,
    bootstrap_account_config,
    load_account_config_env,
    raise_on_failed_account_config,
)
from src.runtime.account_sync import AccountStateSyncService, OrderStateSyncService, RequestThrottle, SyncExchangeContext
from src.runtime.reconciliation_coordinator import (
    RuntimeReconciliationCoordinator,
    RuntimeReconciliationPlan,
)
from src.runtime.strategy_positions import (
    StrategyPositionSnapshotIndex,
    resolve_strategy_position_snapshot_index,
)
from src.signals import TradeSignal
from src.signals.models import SignalAction
from src.strategy.ports import (
    RangeSpeedHistoryProvider,
    StrategyDecisionAuditProvider,
    StrategyPendingWorkProvider,
    StrategyRecoveryStatus,
    StrategyRecoveryStatusProvider,
    StrategyStartupPreviewProvider,
    StrategyStopAdoptionProvider,
)

from src.runtime.live_helpers import _jittered_sleep
from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent
from src.runtime.services import DEFAULT_RUNTIME_SERVICE


class AccountComponent(RuntimeComponent):
    async def _periodic_follower_close_check(self, stop_event: asyncio.Event) -> None:
        await asyncio.sleep(30)
        while not stop_event.is_set():
            try:
                signals = self._build_unresolved_follower_close_signals()
                if signals:
                    logger.info(
                        "Auto-triggering follower close retry for %s unresolved follower(s)",
                        len(signals),
                    )
                    await self._execute_signals(
                        signals,
                        source="follower_close_periodic_check",
                        event_time_ms=None,
                        metadata={"trigger": "periodic_follower_close_check"},
                    )
            except Exception as exc:
                logger.error("Periodic follower close check error | error=%s", exc)
            await _jittered_sleep(stop_event, 60)

    def _build_unresolved_follower_close_signals(self) -> list[TradeSignal]:
        """Build standard TradeSignals for follower legs that still need closing.

        Scans PositionPlanStore for MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED plans
        and constructs follower-only close signals from the stored leg data.
        This is an order-lifecycle safety net — it does not depend on any
        strategy private method.
        """
        store = self._position_plan_store
        if store is None:
            return []
        signals: list[TradeSignal] = []
        for plan in store.list_active_positions():
            if plan.status != PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED:
                continue
            for leg in store.get_legs(plan.position_id):
                if leg.exchange == plan.master_exchange:
                    continue
                if leg.role not in {LegRole.FOLLOWER, "follower"}:
                    continue
                if leg.sync_status == LegSyncStatus.CLOSED:
                    continue
                # Determine quantity: prefer filled_qty_base, fall back to target_qty_base.
                qty = leg.filled_qty_base if leg.filled_qty_base > Decimal("0") else leg.target_qty_base
                if qty <= Decimal("0"):
                    logger.warning(
                        "Unresolved follower close skipped — zero quantity | position_id=%s exchange=%s",
                        plan.position_id,
                        leg.exchange.value,
                    )
                    continue
                action = SignalAction.CLOSE_LONG if plan.side == "long" else SignalAction.CLOSE_SHORT
                # Read persistent follower_close_generation from plan metadata.
                # This survives restarts and prevents replay of exhausted generations.
                plan_meta = dict(plan.metadata or {})
                current_gen = int(plan_meta.get("follower_close_generation", 0))
                signals.append(
                    TradeSignal(
                        symbol=self.app_config.symbol,
                        action=action,
                        quantity=qty,
                        reason="PERIODIC_MASTER_CLOSED_CLOSE_FOLLOWER",
                        metadata={
                            "target_exchanges": [leg.exchange.value],
                            "reduce_only": True,
                            "execution_purpose": "follower_close_after_master_close",
                            "position_id": plan.position_id,
                            "strategy_id": plan.strategy_id,
                            "master_already_closed": True,
                            "close_required_reason": "master_closed_follower_not_closed",
                            "trigger": "periodic_follower_close_check",
                            "follower_close_generation": current_gen,
                        },
                    )
                )
                logger.warning(
                    "Unresolved follower close detected | position_id=%s exchange=%s sync_status=%s qty=%s",
                    plan.position_id,
                    leg.exchange.value,
                    leg.sync_status.value if hasattr(leg.sync_status, "value") else str(leg.sync_status),
                    str(qty),
                )
        return signals

    def _has_unresolved_follower_close(self) -> bool:
        """Return True when at least one position plan has unresolved follower
        close after master close, blocking new entries."""
        store = self._position_plan_store
        if store is None:
            return False
        for plan in store.list_active_positions():
            if plan.status == PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED:
                return True
        return False

    def _has_account_config_entry_block(self) -> bool:
        """Return True when account config verification was blocked by existing
        exposure, preventing new position entries until resolved."""
        return self._account_config_new_entries_blocked

    async def _process_account_event(self, event: AccountEvent) -> None:
        self.stats.account_events_seen += 1
        save = getattr(self.context.state_store, "save_account_event", None)
        if callable(save):
            await asyncio.to_thread(save, event)
        signals = await self._strategy_host.on_account_event(event)
        if signals is None:
            return
        await self._execute_signals(signals or (), source=f"account:{event.exchange.value}", event_time_ms=event.event_time_ms)

    async def _on_account_snapshot_synced(self, snapshot: PlatformSnapshot, sync_type: str) -> None:
        snapshots = [
            existing
            for existing in self._last_snapshots
            if existing.balance.exchange != snapshot.balance.exchange
        ]
        snapshots.append(snapshot)
        self._last_snapshots = tuple(snapshots)
        if snapshot.balance.exchange == self.app_config.data_exchange:
            self._last_snapshot = snapshot

        callback_called = await self._strategy_host.on_account_snapshot(snapshot)
        if not callback_called:
            return

        exchange = snapshot.balance.exchange.value
        key = (exchange, sync_type)
        state = (snapshot.balance.available, snapshot.balance.total)
        previous_state = self._last_account_snapshot_log_state.get(key)
        now_ms = int(time.monotonic() * 1000)
        self._last_account_snapshot_log_state[key] = state

        if previous_state is None:
            self._last_account_snapshot_log_ms[key] = now_ms
            logger.info(
                "Strategy account snapshot refreshed | exchange=%s sync_type=%s available=%s total=%s reason=first_snapshot",
                exchange,
                sync_type,
                snapshot.balance.available,
                snapshot.balance.total,
            )
            return

        if state != previous_state:
            self._last_account_snapshot_log_ms[key] = now_ms
            logger.info(
                "Strategy account snapshot refreshed | exchange=%s sync_type=%s available=%s total=%s reason=balance_changed previous_available=%s previous_total=%s",
                exchange,
                sync_type,
                snapshot.balance.available,
                snapshot.balance.total,
                previous_state[0],
                previous_state[1],
            )
            return

        keepalive_seconds = self._account_snapshot_log_keepalive_seconds
        last_info_ms = self._last_account_snapshot_log_ms[key]
        if keepalive_seconds > 0 and now_ms - last_info_ms >= keepalive_seconds * 1000:
            self._last_account_snapshot_log_ms[key] = now_ms
            logger.info(
                "Strategy account snapshot refreshed | exchange=%s sync_type=%s available=%s total=%s reason=keepalive_unchanged keepalive_seconds=%g",
                exchange,
                sync_type,
                snapshot.balance.available,
                snapshot.balance.total,
                keepalive_seconds,
            )
            return

        logger.debug(
            "Account snapshot unchanged | exchange=%s sync_type=%s available=%s total=%s",
            exchange,
            sync_type,
            snapshot.balance.available,
            snapshot.balance.total,
        )

    def _get_account_clients(self) -> tuple[AccountClient, ...]:
        if self._account_clients is None:
            injected = self.service_dependencies().account_clients
            if injected is not None:
                self._account_clients = tuple(injected)
            else:
                self._account_clients = tuple(
                    create_account_client(exchange, symbol=self.app_config.symbol, config=ExchangeConfig.from_env(exchange))
                    for exchange in self.app_config.exchanges
                )
        return self._account_clients

    def _get_reconciliation_service(self):
        if self._reconciliation_service is DEFAULT_RUNTIME_SERVICE:
            self._reconciliation_service = LiveStateReconciliationService(
                position_plan_store=self._get_position_plan_store(),
                order_journal=self._get_order_journal(),
                state_store=self.context.state_store,
                alert_sink=self.context.alerts,
            )
        return self._reconciliation_service

    async def _run_reconciliation(self, snapshots: tuple[PlatformSnapshot, ...]) -> None:
        await self._reconciliation_coordinator.execute(
            snapshots,
            RuntimeReconciliationPlan(
                resolve_service=self._get_reconciliation_service,
                validate_snapshots=(
                    self._validate_startup_reconciliation_snapshots
                ),
                begin_reconciliation=self._log_startup_reconciliation_begin,
                apply_legacy_adoptions=(
                    self._apply_startup_legacy_stop_adoptions
                ),
                invoke_service=(
                    self._invoke_startup_reconciliation_service
                ),
                handle_report=self._handle_startup_reconciliation_report,
            ),
        )

    def _validate_startup_reconciliation_snapshots(
        self,
        snapshots: tuple[PlatformSnapshot, ...],
    ) -> None:
        expected = len(self.app_config.exchanges)
        if len(snapshots) != expected:
            snapshot_exchanges = sorted(
                snapshot.leverage.exchange.value
                if hasattr(snapshot, "leverage")
                else str(snapshot)
                for snapshot in snapshots
            )
            raise LiveRuntimeError(
                f"startup reconciliation missing exchange snapshots: "
                f"expected {expected} exchanges "
                f"({', '.join(ex.value for ex in self.app_config.exchanges)}), "
                f"got {len(snapshots)} ({', '.join(snapshot_exchanges) if snapshot_exchanges else 'none'})"
            )

    def _log_startup_reconciliation_begin(
        self,
        snapshots: tuple[PlatformSnapshot, ...],
    ) -> None:
        exchange_names = ", ".join(
            snapshot.leverage.exchange.value
            if hasattr(snapshot, "leverage")
            else "?"
            for snapshot in snapshots
        )
        logger.info(
            "Startup reconciliation starting | exchanges=%s count=%s",
            exchange_names,
            len(snapshots),
        )

    def _apply_startup_legacy_stop_adoptions(
        self,
        service: object,
    ) -> None:
        # ── Inject legacy stop adoptions from strategy recovery ───────
        strategy = self.context.strategy
        legacy_adoptions = (
            tuple(strategy.pending_stop_adoptions())
            if isinstance(strategy, StrategyStopAdoptionProvider)
            else ()
        )
        if legacy_adoptions:
            from src.order_management.reconciliation.models import (
                ReconciliationAction,
            )

            now_ms = int(time.time() * 1000)
            for adoption in legacy_adoptions:
                action = ReconciliationAction(
                    action_type="adopt_legacy_stop_reference",
                    target=(
                        f"leg:{adoption['position_id']}:"
                        f"{adoption['exchange']}"
                    ),
                    detail={
                        "position_id": adoption["position_id"],
                        "exchange": adoption["exchange"],
                        "stop_order_id": adoption["stop_order_id"],
                        "stop_client_order_id": adoption[
                            "stop_client_order_id"
                        ],
                        "effective_stop_price": adoption[
                            "effective_stop_price"
                        ],
                        "canonical_theoretical_stop_price": adoption[
                            "canonical_theoretical_stop_price"
                        ],
                        "resolution_status": adoption[
                            "resolution_status"
                        ],
                        "adopted_at_ms": now_ms,
                    },
                )
                # Apply legacy adoption directly to the store before
                # reconciliation runs (so reconciliation sees the
                # corrected state).
                service._apply_actions(  # type: ignore[attr-defined]
                    [action],
                    self.app_config.symbol,
                )
                logger.warning(
                    "Startup recovery: legacy stop adopted | "
                    "position_id=%s exchange=%s stop_order_id=%s "
                    "effective_stop_price=%s",
                    adoption["position_id"],
                    adoption["exchange"],
                    adoption["stop_order_id"],
                    adoption["effective_stop_price"],
                )
            # Clear the list so it is not re-applied on subsequent calls.
            strategy.clear_pending_stop_adoptions()

    async def _invoke_startup_reconciliation_service(
        self,
        service: object,
        snapshots: tuple[PlatformSnapshot, ...],
    ):
        return await service.reconcile_and_apply(  # type: ignore[attr-defined]
            snapshots
        )

    def _handle_startup_reconciliation_report(self, report) -> None:
        if report.stale_plans_closed > 0:
            logger.warning(
                "Startup reconciliation closed %s stale position plan(s) | "
                "fake_refs=%s verdict=%s",
                report.stale_plans_closed,
                len(report.fake_order_refs_found),
                report.verdict.value,
            )
        if report.fake_order_refs_found:
            for ref in report.fake_order_refs_found:
                logger.warning(
                    "Fake order ref cleaned | position_id=%s exchange=%s "
                    "field=%s value=%s reason=%s",
                    ref.position_id,
                    ref.exchange,
                    ref.field,
                    ref.value,
                    ref.reason,
                )
        if report.unresolved_follower_positions > 0:
            logger.warning(
                "Startup reconciliation: %s unresolved follower position(s) | "
                "position_id(s)=%s",
                report.unresolved_follower_positions,
                ", ".join(
                    a.target for a in report.actions
                    if a.action_type == "set_master_closed_follower_close_required"
                ),
            )
        for alert_dict in report.alerts:
            self.context.alerts.emit(
                AppAlert(
                    subject=alert_dict["subject"],
                    content=alert_dict["content"],
                    severity=alert_dict.get("severity", "error"),
                )
            )
        verdict = (
            report.verdict.value
            if hasattr(report.verdict, "value")
            else str(report.verdict)
        )
        if not report.ok:
            logger.error(
                "Startup reconciliation failed | verdict=%s issues=%s",
                verdict,
                report.issues,
            )
            raise LiveRuntimeError(
                "startup reconciliation failed: "
                f"verdict={verdict} issues={list(report.issues)}"
            )
        if (
            verdict == "pass_with_cleanup"
            or report.stale_plans_closed > 0
            or report.fake_order_refs_found
        ):
            logger.info(
                "Startup reconciliation passed with cleanup | "
                "verdict=%s stale_plans_closed=%s fake_refs=%s",
                verdict,
                report.stale_plans_closed,
                len(report.fake_order_refs_found),
            )
        else:
            logger.info(
                "Startup reconciliation passed | verdict=%s",
                verdict,
            )

    def _get_sync_contexts(self) -> tuple[SyncExchangeContext, ...]:
        if ("execution_clients" in self.services) != ("account_clients" in self.services):
            raise LiveRuntimeError("request sync requires account_clients and execution_clients to be injected together")
        clients = self._get_execution_clients()
        accounts = self._get_account_clients()
        execution_by_exchange = {client.exchange: client for client in clients}
        account_by_exchange = {client.exchange: client for client in accounts}
        expected = set(self.app_config.exchanges)
        if set(execution_by_exchange) != set(account_by_exchange):
            raise LiveRuntimeError(
                "request sync account/execution exchange mismatch: "
                f"accounts={sorted(exchange.value for exchange in account_by_exchange)} "
                f"executions={sorted(exchange.value for exchange in execution_by_exchange)}"
            )
        if set(execution_by_exchange) != expected:
            raise LiveRuntimeError(
                "request sync clients do not cover configured exchanges: "
                f"expected={sorted(exchange.value for exchange in expected)} "
                f"actual={sorted(exchange.value for exchange in execution_by_exchange)}"
            )
        config_env = self._resolved_account_config_env()
        contexts: list[SyncExchangeContext] = []
        for exchange in self.app_config.exchanges:
            target = config_env.target_for(exchange)
            contexts.append(
                SyncExchangeContext(
                    account=account_by_exchange[exchange],
                    execution=execution_by_exchange[exchange],
                    state_store=self.context.state_store,
                    leverage_margin_mode=(
                        config_env.margin_mode
                        if target is None
                        else target.margin_mode
                    ),
                    expected_leverage=(
                        None if target is None else target.leverage
                    ),
                )
            )
        return tuple(contexts)

    def _resolved_account_config_env(self) -> AccountConfigEnv:
        if self._account_config_env is None:
            self._account_config_env = load_account_config_env(
                exchanges=self.app_config.exchanges,
                symbol=self.app_config.symbol,
                environ=self._project_env.values,
                require_leverage=False,
            )
        return self._account_config_env

    def _build_account_sync_service(self):
        return AccountStateSyncService(
            contexts=self._get_sync_contexts(),
            config=self.requirements.account_state,
            alert_sink=self.context.alerts,
            throttle=self._request_sync_throttle,
            snapshot_callback=self._on_account_snapshot_synced,
        )

    def _get_account_sync_service(self):
        service = self._sync_service_registry.get_account(
            self._build_account_sync_service
        )
        self._account_sync_service = service
        return service

    def _build_order_sync_service(self):
        return OrderStateSyncService(
            contexts=self._get_sync_contexts(),
            config=self.requirements.order_state,
            alert_sink=self.context.alerts,
            throttle=self._request_sync_throttle,
            active_check=self._order_sync_active,
            position_plan_store=self._get_position_plan_store(),
        )

    def _get_order_sync_service(self):
        service = self._sync_service_registry.get_order(
            self._build_order_sync_service
        )
        self._order_sync_service = service
        return service

    def _order_sync_active(self) -> bool:
        if self._strategy_position_index().active:
            return True
        provider = self._strategy_pending_work_provider()
        if provider is not None and provider.has_pending_strategy_work():
            return True
        store = self._position_plan_store
        if store is not None and callable(getattr(store, "list_active_positions", None)) and store.list_active_positions():
            return True
        list_open = getattr(self.context.state_store, "list_open_orders", None)
        if callable(list_open):
            for exchange in self.app_config.exchanges:
                if list_open(exchange=exchange, symbol=self.app_config.symbol, include_stop_orders=True):
                    return True
        return False

    def _strategy_position_index(self) -> StrategyPositionSnapshotIndex:
        return resolve_strategy_position_snapshot_index(self.context.strategy)
