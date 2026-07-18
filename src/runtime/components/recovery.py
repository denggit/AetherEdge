from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety import (
    RecoveryExitOrderValidator,
    filter_orders_for_position_scope,
)
from src.order_management.safety.recovery_exit_validator import (
    RecoveryExitValidationResult,
)
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, InstrumentRule, Order, OrderStatus, Position, PositionMode, PositionSide
from src.platform.account.ports import AccountClient
from src.platform.execution.ports import ExecutionClient
from src.platform.markets import get_market_profile
from src.platform.markets.models import MarketProfile
from src.platform.snapshot import PlatformSnapshot
from src.runtime.strategy_capabilities import (
    StrategyCapabilityError,
    StrategyContractError,
    ValidatedStrategyCapabilities,
    validate_dynamic_strategy_capabilities,
    validate_strategy_capabilities,
)
from src.runtime.recovery_coordinator import (
    RuntimeRecoveryCoordinator,
    RuntimeRecoveryPlan,
)
from src.runtime.recovery.service import RecoveryExchangeContext, RuntimeRecoveryService
from src.runtime.recovery.models import RecoveryReport
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
from src.strategy.positions import StrategyPositionSnapshot

from src.runtime.live_helpers import _exchange_positions_matching_strategy_position, _fetch_execution_instrument_rule, _place_stop_scope_covers, _position_side_for_strategy_position, _raise_ambiguous_exchange_positions, _signal_position_id, _strategy_position_active_exchanges, _strategy_position_native_quantity, _strategy_position_requires_protective_stop, _strategy_position_stop_order_ids
from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent
from src.runtime.services import DEFAULT_RUNTIME_SERVICE


_PostExecutionExchangeState = tuple[
    Sequence[Position],
    Sequence[Order],
    PositionMode,
    InstrumentRule | None,
]


class RecoveryComponent(RuntimeComponent):
    async def _run_recovery(self) -> tuple[PlatformSnapshot, ...]:
        return await self._recovery_coordinator.execute(
            RuntimeRecoveryPlan(
                resolve_service=self._get_recovery_service,
                fallback_snapshots=self._recovery_fallback_snapshots,
                invoke_service=self._invoke_recovery_service,
                record_run=self._record_recovery_run,
                validate_report=self._validate_runtime_recovery_report,
                partition_signals=self._partition_recovery_signals,
                capture_failure_counts=(
                    self._capture_recovery_failure_counts
                ),
                execute_stop_signals=self._execute_recovery_stop_signals,
                validate_stop_execution=(
                    self._validate_recovery_stop_execution
                ),
                validate_post_execution_protection=(
                    self._validate_post_execution_stop_protection
                ),
                execute_other_signals=self._execute_recovery_other_signals,
                finalize_report=self._finalize_recovery_report,
            )
        )

    def _recovery_fallback_snapshots(
        self,
    ) -> tuple[PlatformSnapshot, ...]:
        if self._last_snapshot is None:
            raise LiveRuntimeError(
                "startup snapshot is required before live trading"
            )
        return (self._last_snapshot,)

    async def _invoke_recovery_service(
        self,
        service: object,
    ) -> RecoveryReport:
        return await service.recover(  # type: ignore[attr-defined]
            strategy=self.context.strategy
        )

    def _record_recovery_run(self) -> None:
        self.stats.recovery_runs += 1

    def _validate_runtime_recovery_report(
        self,
        report: RecoveryReport,
    ) -> None:
        capabilities = getattr(
            self,
            "_validated_strategy_capabilities",
            None,
        )
        if capabilities is None:
            raise StrategyContractError(
                "strategy dynamic contract validation requires established "
                "startup capabilities"
            )
        dynamic_state = validate_dynamic_strategy_capabilities(
            self.context.strategy,
            expected_strategy_id=capabilities.identity,
            expected_symbol=self.app_config.symbol,
            strategy_entry=self.app_config.strategy,
            runtime_mode=self.runtime_config.mode,
        )
        if not report.ok:
            raise LiveRuntimeError(f"runtime recovery failed: {tuple(report.issues)}")
        # ── Check strategy recovery blocking state ────────────────────────
        recovery_status = dynamic_state.recovery_status
        if recovery_status.blocking_manual_required:
            raise LiveRuntimeError(
                f"runtime recovery blocking manual required: "
                f"alerts={list(recovery_status.alerts)}"
            )
        # ── Pre-execution postcondition: must have coverage plan ───────────
        self._validate_recovery_protection_postcondition(report)

    def _strategy_recovery_status(self) -> StrategyRecoveryStatus:
        provider = self._strategy_recovery_status_provider()
        if provider is None:
            return StrategyRecoveryStatus()
        return provider.recovery_status()

    def _strategy_recovery_status_provider(
        self,
    ) -> StrategyRecoveryStatusProvider | None:
        capabilities = getattr(
            self,
            "_validated_strategy_capabilities",
            None,
        )
        if capabilities is not None:
            return capabilities.recovery_status
        strategy = self.context.strategy
        return (
            strategy
            if isinstance(strategy, StrategyRecoveryStatusProvider)
            else None
        )

    def _strategy_pending_work_provider(
        self,
    ) -> StrategyPendingWorkProvider | None:
        capabilities = getattr(
            self,
            "_validated_strategy_capabilities",
            None,
        )
        if capabilities is not None:
            return capabilities.pending_work
        strategy = self.context.strategy
        return (
            strategy
            if isinstance(strategy, StrategyPendingWorkProvider)
            else None
        )

    def _strategy_startup_preview_provider(
        self,
    ) -> StrategyStartupPreviewProvider | None:
        capabilities = getattr(
            self,
            "_validated_strategy_capabilities",
            None,
        )
        if capabilities is not None:
            return capabilities.startup_preview
        strategy = self.context.strategy
        return (
            strategy
            if isinstance(strategy, StrategyStartupPreviewProvider)
            else None
        )

    def _strategy_range_speed_history_provider(
        self,
    ) -> RangeSpeedHistoryProvider | None:
        capabilities = getattr(
            self,
            "_validated_strategy_capabilities",
            None,
        )
        if capabilities is not None:
            return capabilities.range_speed_history
        strategy = self.context.strategy
        return (
            strategy
            if isinstance(strategy, RangeSpeedHistoryProvider)
            else None
        )

    def _strategy_capabilities(self) -> ValidatedStrategyCapabilities:
        capabilities = getattr(
            self,
            "_validated_strategy_capabilities",
            None,
        )
        if capabilities is None:
            capabilities = validate_strategy_capabilities(
                self.context.strategy,
                self.requirements,
                strategy_entry=self.app_config.strategy,
                runtime_mode=self.runtime_config.mode,
            )
            self._validated_strategy_capabilities = capabilities
        return capabilities

    def _partition_recovery_signals(
        self,
        report: RecoveryReport,
    ) -> tuple[list[TradeSignal], list[TradeSignal]]:
        stop_actions = {
            SignalAction.PLACE_STOP_LOSS_LONG,
            SignalAction.PLACE_STOP_LOSS_SHORT,
        }
        stop_signals = [
            signal
            for signal in report.strategy_signals
            if signal.action in stop_actions
        ]
        other_signals = [
            signal
            for signal in report.strategy_signals
            if signal.action not in stop_actions
        ]
        return stop_signals, other_signals

    def _capture_recovery_failure_counts(self) -> tuple[int, int]:
        return (
            self.stats.failed_intents,
            self.stats.partial_failures,
        )

    async def _execute_recovery_stop_signals(
        self,
        signals: list[TradeSignal],
    ) -> None:
        await self._execute_signals(
            signals,
            source="recovery",
            event_time_ms=None,
            metadata={"feature_type": "recovery"},
        )

    def _validate_recovery_stop_execution(
        self,
        failure_counts: tuple[int, int],
    ) -> None:
        failed_before, partial_before = failure_counts
        if self.stats.failed_intents > failed_before:
            raise LiveRuntimeError(
                "recovery stop placement failed: "
                "all target exchanges rejected the stop order"
            )
        if self.stats.partial_failures > partial_before:
            raise LiveRuntimeError(
                "recovery stop placement partially failed: "
                "some target exchanges rejected the stop order"
            )

    async def _execute_recovery_other_signals(
        self,
        signals: list[TradeSignal],
    ) -> None:
        await self._execute_signals(
            signals,
            source="recovery",
            event_time_ms=None,
            metadata={"feature_type": "recovery"},
        )

    def _finalize_recovery_report(
        self,
        report: RecoveryReport,
    ) -> tuple[PlatformSnapshot, ...]:
        # ── All checks passed — safe to log completion ────────────────────
        logger.info(
            "Runtime recovery completed | snapshots=%s strategy_signals=%s issues=%s",
            len(report.snapshots),
            len(report.strategy_signals),
            len(report.issues),
        )
        if report.snapshots:
            self._last_snapshots = tuple(report.snapshots)
            self._last_snapshot = report.snapshots[0]  # backward-compat for on_start / legacy consumers
        if not self._last_snapshots:
            raise LiveRuntimeError("recovery completed without a startup snapshot")
        return self._last_snapshots

    def _validate_recovery_protection_postcondition(
        self,
        report: RecoveryReport,
    ) -> None:
        """Verify every active exchange position has protective stop coverage."""

        if self._strategy_recovery_status().blocking_manual_required:
            return
        active_positions = self._strategy_position_index().active
        if not active_positions:
            return
        converter = NativeQuantityConverter()
        validator = RecoveryExitOrderValidator(quantity_converter=converter)
        place_stop_scopes = self._recovery_place_stop_scopes(report)
        master_exchange = self.app_config.data_exchange.value
        for strategy_position in active_positions:
            if not _strategy_position_requires_protective_stop(
                strategy_position
            ):
                continue
            self._validate_strategy_recovery_protection(
                strategy_position=strategy_position,
                snapshots=report.snapshots,
                place_stop_scopes=place_stop_scopes,
                master_exchange=master_exchange,
                logical_position_count=len(active_positions),
                converter=converter,
                validator=validator,
            )

    def _recovery_place_stop_scopes(
        self,
        report: RecoveryReport,
    ) -> dict[str, set[str | None]]:
        scopes: dict[str, set[str | None]] = {}
        stop_actions = {
            SignalAction.PLACE_STOP_LOSS_LONG,
            SignalAction.PLACE_STOP_LOSS_SHORT,
        }
        for signal in report.strategy_signals:
            if signal.action not in stop_actions:
                continue
            position_id = _signal_position_id(signal)
            targets = (
                signal.metadata.get("target_exchanges", [])
                if signal.metadata
                else ()
            )
            if not isinstance(targets, (list, tuple)):
                continue
            for target in targets:
                exchange_scope = str(target).strip().lower()
                scopes.setdefault(exchange_scope, set()).add(position_id)
        return scopes

    def _validate_strategy_recovery_protection(
        self,
        *,
        strategy_position: StrategyPositionSnapshot,
        snapshots: Sequence[PlatformSnapshot],
        place_stop_scopes: Mapping[str, set[str | None]],
        master_exchange: str,
        logical_position_count: int,
        converter: NativeQuantityConverter,
        validator: RecoveryExitOrderValidator,
    ) -> None:
        relevant_exchanges = {
            master_exchange,
            *_strategy_position_active_exchanges(strategy_position),
        }
        market_profile = get_market_profile(strategy_position.symbol)
        for snapshot in snapshots:
            exchange_name = snapshot.balance.exchange
            exchange_str = (
                exchange_name.value
                if hasattr(exchange_name, "value")
                else str(exchange_name)
            )
            if exchange_str not in relevant_exchanges:
                continue
            self._validate_snapshot_recovery_protection(
                strategy_position=strategy_position,
                snapshot=snapshot,
                exchange_name=exchange_name,
                exchange_str=exchange_str,
                place_stop_scopes=place_stop_scopes,
                logical_position_count=logical_position_count,
                market_profile=market_profile,
                converter=converter,
                validator=validator,
            )

    def _validate_snapshot_recovery_protection(
        self,
        *,
        strategy_position: StrategyPositionSnapshot,
        snapshot: PlatformSnapshot,
        exchange_name: ExchangeName,
        exchange_str: str,
        place_stop_scopes: Mapping[str, set[str | None]],
        logical_position_count: int,
        market_profile: MarketProfile,
        converter: NativeQuantityConverter,
        validator: RecoveryExitOrderValidator,
    ) -> None:
        matching_positions = _exchange_positions_matching_strategy_position(
            getattr(snapshot, "positions", ()) or (),
            strategy_position,
        )
        if len(matching_positions) > 1:
            _raise_ambiguous_exchange_positions(
                context="recovery protection postcondition failed",
                strategy_position=strategy_position,
                exchange=exchange_str,
                ambiguous_count=len(matching_positions),
            )
        if not matching_positions:
            return
        active_pos = matching_positions[0]
        canonical_stop_price = strategy_position.stop_price
        expected_native_quantity = _strategy_position_native_quantity(
            strategy_position=strategy_position,
            active_pos=active_pos,
            exchange=exchange_name,
            market_profile=market_profile,
            converter=converter,
            logical_position_count=logical_position_count,
        )
        position_side = _position_side_for_strategy_position(
            strategy_position,
            active_pos,
        )
        if self._snapshot_has_valid_recovery_stop(
            strategy_position=strategy_position,
            snapshot=snapshot,
            exchange_name=exchange_name,
            canonical_stop_price=canonical_stop_price,
            expected_native_quantity=expected_native_quantity,
            position_side=position_side,
            logical_position_count=logical_position_count,
            market_profile=market_profile,
            validator=validator,
        ):
            return
        if _place_stop_scope_covers(
            place_stop_scopes,
            exchange=exchange_str,
            position_id=strategy_position.position_id,
            logical_position_count=logical_position_count,
        ):
            return
        self._raise_missing_recovery_protection(
            strategy_position=strategy_position,
            snapshot=snapshot,
            exchange_str=exchange_str,
            active_pos=active_pos,
        )

    def _snapshot_has_valid_recovery_stop(
        self,
        *,
        strategy_position: StrategyPositionSnapshot,
        snapshot: PlatformSnapshot,
        exchange_name: ExchangeName,
        canonical_stop_price: Decimal | None,
        expected_native_quantity: Decimal,
        position_side: PositionSide | None,
        logical_position_count: int,
        market_profile: MarketProfile,
        validator: RecoveryExitOrderValidator,
    ) -> bool:
        if canonical_stop_price is None or position_side is None:
            return False
        try:
            open_stop_orders = getattr(snapshot, "open_stop_orders", ()) or ()
            if logical_position_count > 1:
                open_stop_orders = filter_orders_for_position_scope(
                    open_stop_orders,
                    position_id=strategy_position.position_id,
                    known_order_ids=_strategy_position_stop_order_ids(
                        strategy_position
                    ),
                )
            validation = validator.validate_stop_orders(
                exchange=exchange_name,
                symbol=strategy_position.symbol,
                strategy_id=strategy_position.strategy_id,
                position_id=strategy_position.position_id,
                position_side=position_side,
                position_mode=snapshot.position_mode,
                current_position_native_quantity=expected_native_quantity,
                canonical_stop_price=canonical_stop_price,
                open_stop_orders=open_stop_orders,
                open_orders=getattr(snapshot, "open_orders", ()) or (),
                market_profile=market_profile,
                instrument_rule=snapshot.instrument_rule,
            )
            return validation.should_keep_existing_stop
        except Exception:
            return False

    def _raise_missing_recovery_protection(
        self,
        *,
        strategy_position: StrategyPositionSnapshot,
        snapshot: PlatformSnapshot,
        exchange_str: str,
        active_pos: Position,
    ) -> None:
        open_stop_orders = getattr(snapshot, "open_stop_orders", ()) or ()
        raise LiveRuntimeError(
            "recovery protection postcondition failed: "
            "active position without bot-owned valid stop or recovery stop signal | "
            f"strategy_position_id={strategy_position.position_id} "
            f"exchange={exchange_str} "
            f"symbol={strategy_position.symbol} "
            f"position_side={strategy_position.side.value} "
            f"position_qty={active_pos.quantity} "
            f"open_stop_orders={len(open_stop_orders)} "
            f"bot_owned_valid_stop=false "
            f"place_stop_signal=false "
            f"canonical_stop_price={strategy_position.stop_price} "
            f"strategy_recovery_blocking_manual_required=false"
        )

    async def _validate_post_execution_stop_protection(self) -> None:
        """Verify freshly placed recovery stops on every active exchange leg."""

        active_positions = self._strategy_position_index().active
        if not active_positions:
            return
        converter = NativeQuantityConverter()
        validator = RecoveryExitOrderValidator(quantity_converter=converter)
        exec_by_exchange = {
            client.exchange: client
            for client in self._get_execution_clients()
        }
        acct_by_exchange = {
            client.exchange: client
            for client in self._get_account_clients()
        }
        exchange_state: dict[
            ExchangeName,
            _PostExecutionExchangeState,
        ] = {}
        master_exchange = self.app_config.data_exchange.value

        for strategy_position in active_positions:
            if not _strategy_position_requires_protective_stop(
                strategy_position
            ):
                continue
            canonical_stop_price = strategy_position.stop_price
            if canonical_stop_price is None:
                raise LiveRuntimeError(
                    "post-execution stop validation failed: no canonical stop price available | "
                    f"strategy_position_id={strategy_position.position_id}"
                )
            relevant_exchanges = {
                master_exchange,
                *_strategy_position_active_exchanges(strategy_position),
            }
            market_profile = get_market_profile(strategy_position.symbol)
            for exchange in self.app_config.exchanges:
                if exchange.value not in relevant_exchanges:
                    continue
                exec_client = exec_by_exchange.get(exchange)
                acct_client = acct_by_exchange.get(exchange)
                if exec_client is None or acct_client is None:
                    continue
                state = await self._post_execution_exchange_state(
                    exchange=exchange,
                    exec_client=exec_client,
                    acct_client=acct_client,
                    cache=exchange_state,
                )
                self._validate_exchange_post_execution_protection(
                    strategy_position=strategy_position,
                    exchange=exchange,
                    state=state,
                    canonical_stop_price=canonical_stop_price,
                    logical_position_count=len(active_positions),
                    market_profile=market_profile,
                    converter=converter,
                    validator=validator,
                )

    async def _post_execution_exchange_state(
        self,
        *,
        exchange: ExchangeName,
        exec_client: ExecutionClient,
        acct_client: AccountClient,
        cache: dict[ExchangeName, _PostExecutionExchangeState],
    ) -> _PostExecutionExchangeState:
        cached = cache.get(exchange)
        if cached is not None:
            return cached
        try:
            positions = tuple(await acct_client.fetch_positions() or ())
            open_stop_orders = tuple(
                await exec_client.fetch_open_stop_orders() or ()
            )
            instrument_rule = await _fetch_execution_instrument_rule(
                exec_client
            )
        except Exception as exc:
            raise LiveRuntimeError(
                "post-execution stop validation failed: cannot fetch exchange state | "
                f"exchange={exchange.value} error={exc}"
            ) from exc
        try:
            mode = await acct_client.fetch_position_mode()
        except Exception:
            mode = PositionMode.ONE_WAY
        state = (
            positions,
            open_stop_orders,
            mode,
            instrument_rule,
        )
        cache[exchange] = state
        return state

    def _validate_exchange_post_execution_protection(
        self,
        *,
        strategy_position: StrategyPositionSnapshot,
        exchange: ExchangeName,
        state: _PostExecutionExchangeState,
        canonical_stop_price: Decimal,
        logical_position_count: int,
        market_profile: MarketProfile,
        converter: NativeQuantityConverter,
        validator: RecoveryExitOrderValidator,
    ) -> None:
        positions, open_stop_orders, mode, instrument_rule = state
        matching_positions = _exchange_positions_matching_strategy_position(
            positions,
            strategy_position,
        )
        if len(matching_positions) > 1:
            _raise_ambiguous_exchange_positions(
                context="post-execution stop validation failed",
                strategy_position=strategy_position,
                exchange=exchange.value,
                ambiguous_count=len(matching_positions),
            )
        if not matching_positions:
            return
        active_pos = matching_positions[0]
        position_side = _position_side_for_strategy_position(
            strategy_position,
            active_pos,
        )
        if position_side is None:
            raise LiveRuntimeError(
                "post-execution stop validation failed: unresolved position side | "
                f"strategy_position_id={strategy_position.position_id} "
                f"symbol={strategy_position.symbol} exchange={exchange.value}"
            )
        expected_native_quantity = _strategy_position_native_quantity(
            strategy_position=strategy_position,
            active_pos=active_pos,
            exchange=exchange,
            market_profile=market_profile,
            converter=converter,
            logical_position_count=logical_position_count,
        )
        scoped_stop_orders = (
            filter_orders_for_position_scope(
                open_stop_orders,
                position_id=strategy_position.position_id,
                known_order_ids=_strategy_position_stop_order_ids(
                    strategy_position
                ),
            )
            if logical_position_count > 1
            else open_stop_orders
        )
        validation = validator.validate_stop_orders(
            exchange=exchange,
            symbol=strategy_position.symbol,
            strategy_id=strategy_position.strategy_id,
            position_id=strategy_position.position_id,
            position_side=position_side,
            position_mode=mode,
            current_position_native_quantity=expected_native_quantity,
            canonical_stop_price=canonical_stop_price,
            open_stop_orders=scoped_stop_orders,
            open_orders=(),
            market_profile=market_profile,
            instrument_rule=instrument_rule,
        )
        if not validation.should_keep_existing_stop:
            self._raise_failed_post_execution_protection(
                strategy_position=strategy_position,
                exchange=exchange,
                active_pos=active_pos,
                canonical_stop_price=canonical_stop_price,
                validation=validation,
            )
        logger.info(
            "Post-execution stop protection validated | "
            "position_id=%s exchange=%s valid_bot_stops=%s",
            strategy_position.position_id,
            exchange.value,
            len(validation.valid_bot_owned_orders),
        )

    def _raise_failed_post_execution_protection(
        self,
        *,
        strategy_position: StrategyPositionSnapshot,
        exchange: ExchangeName,
        active_pos: Position,
        canonical_stop_price: Decimal,
        validation: RecoveryExitValidationResult,
    ) -> None:
        raise LiveRuntimeError(
            "post-execution stop validation failed: "
            "active position still without bot-owned valid stop "
            "after recovery stop placement | "
            f"strategy_position_id={strategy_position.position_id} "
            f"exchange={exchange.value} "
            f"symbol={strategy_position.symbol} "
            f"position_side={strategy_position.side.value} "
            f"position_qty={active_pos.quantity} "
            f"canonical_stop_price={canonical_stop_price} "
            f"valid_bot_stops={len(validation.valid_bot_owned_orders)} "
            f"invalid_bot_stops={len(validation.invalid_bot_owned_orders)} "
            f"unknown_stops={len(validation.unknown_exit_orders)} "
            f"primary_reason={validation.primary_invalid_reason} "
            f"detail_reason={validation.primary_invalid_detail_reason} "
            f"effective_expected_stop_price={validation.effective_expected_stop_price} "
            f"price_tick={validation.price_tick}"
        )

    def _get_recovery_service(self):
        if self._recovery_service is DEFAULT_RUNTIME_SERVICE:
            clients = self._get_execution_clients()
            accounts = self._get_account_clients()
            config_env = self._resolved_account_config_env()
            contexts = []
            for account, execution in zip(accounts, clients, strict=False):
                target = config_env.target_for(account.exchange)
                contexts.append(
                    RecoveryExchangeContext(
                        account=account,
                        execution=execution,
                        state_store=self.context.state_store,
                        leverage_margin_mode=(
                            config_env.margin_mode
                            if target is None
                            else target.margin_mode
                        ),
                    )
                )
            self._recovery_service = RuntimeRecoveryService(exchange_contexts=contexts, order_journal=self._get_order_journal(), position_plan_store=self._get_position_plan_store())
        return self._recovery_service
