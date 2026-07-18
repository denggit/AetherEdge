from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from src.app.alerts import AppAlert
from src.order_management import LegSyncStatus, MasterFollowerExecutionPolicy, MultiExchangeOrderCoordinator, PositionPlanStatus, RepositoryDuplicateOrderGuard, SqliteOrderJournalStore, SqlitePositionPlanStore
from src.order_management.models import ExchangeOrderResult
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety import (
    RecoveryExitOrderValidator,
    filter_orders_for_position_scope,
)
from src.order_management.safety.recovery_exit_validator import (
    RecoveryExitValidationResult,
)
from src.platform import create_account_client, create_execution_client
from src.platform.account.ports import AccountClient
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, InstrumentRule, Order, OrderStatus, Position, PositionMode, PositionSide
from src.platform.execution.ports import ExecutionClient
from src.platform.markets import get_market_profile
from src.platform.markets.models import MarketProfile
from src.signals import TradeSignal
from src.signals.models import SignalAction

from src.runtime.live_helpers import _exchange_position_metadata, _exchange_positions_matching_strategy_position, _fetch_execution_instrument_rule, _position_side_for_strategy_position, _stop_post_check_attempts_from_env, _stop_post_check_delay_from_env, _strategy_position_for_stop_signal, _strategy_position_native_quantity
from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.strategy_positions import StrategyPositionSnapshotIndex
from src.strategy.positions import StrategyPositionSnapshot
from src.runtime.components.base import RuntimeComponent


@dataclass(frozen=True)
class _StopPostCheckContext:
    position_index: StrategyPositionSnapshotIndex
    strategy_position: StrategyPositionSnapshot
    canonical_stop_price: Decimal
    execution_by_exchange: Mapping[ExchangeName, ExecutionClient]
    account_by_exchange: Mapping[ExchangeName, AccountClient]
    market_profile: MarketProfile
    converter: NativeQuantityConverter
    validator: RecoveryExitOrderValidator


@dataclass(frozen=True)
class _PositionCheckOutcome:
    position: Position | None = None
    terminal_result: ExchangeOrderResult | None = None
    retry: bool = False


class OrderResultsComponent(RuntimeComponent):
    async def _verify_stop_order_results(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
    ) -> Sequence[ExchangeOrderResult]:
        if not any(result.ok for result in results):
            return results

        position_index = self._strategy_position_index()
        strategy_position = _strategy_position_for_stop_signal(
            position_index,
            signal,
        )
        if strategy_position is None:
            if not position_index.active:
                return results
            return self._fail_successful_stop_results(
                results,
                reason="ambiguous_strategy_position_scope",
                metadata={
                    "post_check": "stop_order_exchange_verification",
                    "active_strategy_positions": len(position_index.active),
                },
            )
        canonical_stop_price = signal.trigger_price or strategy_position.stop_price
        if canonical_stop_price is None:
            return self._fail_successful_stop_results(
                results,
                reason="missing_canonical_stop_price",
                metadata={"post_check": "stop_order_exchange_verification"},
            )

        context = self._build_stop_post_check_context(
            position_index=position_index,
            strategy_position=strategy_position,
            canonical_stop_price=canonical_stop_price,
        )
        verified: list[ExchangeOrderResult] = []
        for result in results:
            if not result.ok:
                verified.append(result)
                continue
            verified.append(
                await self._verify_successful_stop_order_result(
                    signal=signal,
                    result=result,
                    context=context,
                )
            )
        return tuple(verified)

    def _fail_successful_stop_results(
        self,
        results: Sequence[ExchangeOrderResult],
        *,
        reason: str,
        metadata: Mapping[str, Any],
    ) -> list[ExchangeOrderResult]:
        return [
            self._stop_post_check_failed_result(
                result,
                reason=reason,
                metadata=metadata,
            )
            if result.ok
            else result
            for result in results
        ]

    def _build_stop_post_check_context(
        self,
        *,
        position_index: StrategyPositionSnapshotIndex,
        strategy_position: StrategyPositionSnapshot,
        canonical_stop_price: Decimal,
    ) -> _StopPostCheckContext:
        converter = NativeQuantityConverter()
        return _StopPostCheckContext(
            position_index=position_index,
            strategy_position=strategy_position,
            canonical_stop_price=canonical_stop_price,
            execution_by_exchange={
                client.exchange: client
                for client in self._get_execution_clients()
            },
            account_by_exchange={
                client.exchange: client
                for client in self._get_account_clients()
            },
            market_profile=get_market_profile(strategy_position.symbol),
            converter=converter,
            validator=RecoveryExitOrderValidator(
                quantity_converter=converter
            ),
        )

    async def _verify_successful_stop_order_result(
        self,
        *,
        signal: TradeSignal,
        result: ExchangeOrderResult,
        context: _StopPostCheckContext,
    ) -> ExchangeOrderResult:
        if not result.order_id and not result.client_order_id:
            return self._stop_post_check_failed_result(
                result,
                reason="missing_exchange_stop_order_id",
                metadata={"post_check": "stop_order_exchange_verification"},
            )
        if result.status not in {
            OrderStatus.NEW,
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
        }:
            return self._stop_post_check_failed_result(
                result,
                reason="stop_order_status_not_live",
                metadata={
                    "post_check": "stop_order_exchange_verification",
                    "status": (
                        None if result.status is None else result.status.value
                    ),
                },
            )
        exchange = result.exchange
        exec_client = context.execution_by_exchange.get(exchange)
        acct_client = context.account_by_exchange.get(exchange)
        if exec_client is None or acct_client is None:
            return self._stop_post_check_failed_result(
                result,
                reason="missing_exchange_client_for_stop_post_check",
                metadata={"post_check": "stop_order_exchange_verification"},
            )
        try:
            instrument_rule = await _fetch_execution_instrument_rule(
                exec_client
            )
        except Exception as exc:
            return self._stop_post_check_failed_result(
                result,
                reason="stop_post_check_instrument_rule_fetch_failed",
                metadata={
                    "post_check": "stop_order_exchange_verification",
                    "exchange": exchange.value,
                    "fetch_error": str(exc),
                },
            )
        return await self._retry_stop_order_post_check(
            signal=signal,
            result=result,
            context=context,
            exec_client=exec_client,
            acct_client=acct_client,
            instrument_rule=instrument_rule,
        )

    async def _retry_stop_order_post_check(
        self,
        *,
        signal: TradeSignal,
        result: ExchangeOrderResult,
        context: _StopPostCheckContext,
        exec_client: ExecutionClient,
        acct_client: AccountClient,
        instrument_rule: InstrumentRule,
    ) -> ExchangeOrderResult:
        attempts = _stop_post_check_attempts_from_env(self._project_env)
        delay = _stop_post_check_delay_from_env(self._project_env)
        exchange = result.exchange
        for attempt in range(1, attempts + 1):
            try:
                positions = await acct_client.fetch_positions()
                open_stop_orders = await exec_client.fetch_open_stop_orders()
            except Exception as exc:
                if attempt < attempts:
                    logger.warning(
                        "Stop post-check fetch failed; retrying | exchange=%s attempt=%s attempts=%s error=%s",
                        exchange.value,
                        attempt,
                        attempts,
                        exc,
                    )
                    await asyncio.sleep(delay)
                    continue
                return self._stop_post_check_failed_result(
                    result,
                    reason="stop_post_check_fetch_failed",
                    metadata={
                        "post_check": "stop_order_exchange_verification",
                        "fetch_error": str(exc),
                        "stop_post_check_attempts": attempt,
                    },
                )
            outcome = self._resolve_stop_check_position(
                positions=positions or (),
                result=result,
                context=context,
                attempt=attempt,
                attempts=attempts,
            )
            if outcome.retry:
                await asyncio.sleep(delay)
                continue
            if outcome.terminal_result is not None:
                return outcome.terminal_result
            checked = await self._evaluate_stop_order_validation(
                signal=signal,
                result=result,
                context=context,
                active_pos=outcome.position,
                open_stop_orders=open_stop_orders or (),
                instrument_rule=instrument_rule,
                acct_client=acct_client,
                attempt=attempt,
                attempts=attempts,
            )
            if checked is not None:
                return checked
            await asyncio.sleep(delay)
        raise RuntimeError("stop post-check retry loop exhausted unexpectedly")

    def _resolve_stop_check_position(
        self,
        *,
        positions: Sequence[Position],
        result: ExchangeOrderResult,
        context: _StopPostCheckContext,
        attempt: int,
        attempts: int,
    ) -> _PositionCheckOutcome:
        strategy_position = context.strategy_position
        matching_positions = _exchange_positions_matching_strategy_position(
            positions,
            strategy_position,
        )
        if len(matching_positions) > 1:
            return _PositionCheckOutcome(
                terminal_result=self._stop_post_check_failed_result(
                    result,
                    reason="ambiguous_exchange_position_scope",
                    metadata={
                        "post_check": "stop_order_exchange_verification",
                        "strategy_position_id": strategy_position.position_id,
                        "symbol": strategy_position.symbol,
                        "side": strategy_position.side.value,
                        "exchange": result.exchange.value,
                        "ambiguous_count": len(matching_positions),
                    },
                )
            )
        if matching_positions:
            return _PositionCheckOutcome(position=matching_positions[0])
        if attempt < attempts:
            logger.warning(
                "Stop post-check missing exchange position; retrying | "
                "exchange=%s attempt=%s attempts=%s",
                result.exchange.value,
                attempt,
                attempts,
            )
            return _PositionCheckOutcome(retry=True)
        return _PositionCheckOutcome(
            terminal_result=self._stop_post_check_failed_result(
                result,
                reason="stop_post_check_failed:missing_exchange_position",
                metadata={
                    "post_check": "stop_order_exchange_verification",
                    "stop_post_check_attempts": attempt,
                    "invalid_reason": "missing_exchange_position",
                },
            )
        )

    async def _evaluate_stop_order_validation(
        self,
        *,
        signal: TradeSignal,
        result: ExchangeOrderResult,
        context: _StopPostCheckContext,
        active_pos: Position | None,
        open_stop_orders: Sequence[Order],
        instrument_rule: InstrumentRule,
        acct_client: AccountClient,
        attempt: int,
        attempts: int,
    ) -> ExchangeOrderResult | None:
        if active_pos is None:
            raise RuntimeError("stop post-check position outcome is incomplete")
        strategy_position = context.strategy_position
        exchange = result.exchange
        position_side = _position_side_for_strategy_position(
            strategy_position,
            active_pos,
        )
        native_qty = _strategy_position_native_quantity(
            strategy_position=strategy_position,
            active_pos=active_pos,
            exchange=exchange,
            market_profile=context.market_profile,
            converter=context.converter,
            logical_position_count=len(context.position_index.active),
            scoped_base_quantity=signal.quantity,
        )
        if position_side is None or native_qty <= 0:
            return result
        try:
            position_mode = await acct_client.fetch_position_mode()
        except Exception:
            position_mode = PositionMode.ONE_WAY
        validation = context.validator.validate_stop_orders(
            exchange=exchange,
            symbol=strategy_position.symbol,
            strategy_id=strategy_position.strategy_id,
            position_id=strategy_position.position_id,
            position_side=position_side,
            position_mode=position_mode,
            current_position_native_quantity=native_qty,
            canonical_stop_price=context.canonical_stop_price,
            open_stop_orders=open_stop_orders,
            open_orders=(),
            market_profile=context.market_profile,
            instrument_rule=instrument_rule,
        )
        if validation.should_keep_existing_stop:
            return self._confirmed_stop_post_check_result(
                result=result,
                context=context,
                active_pos=active_pos,
                open_stop_orders=open_stop_orders,
                native_qty=native_qty,
                validation=validation,
                attempt=attempt,
            )
        if attempt < attempts:
            reason_hint = (
                validation.primary_invalid_reason or "missing_bot_owned_stop"
            )
            logger.warning(
                "Stop post-check not verified yet; retrying | exchange=%s attempt=%s attempts=%s invalid_category=%s invalid_detail_reason=%s",
                exchange.value,
                attempt,
                attempts,
                reason_hint,
                validation.primary_invalid_detail_reason,
            )
            return None
        return self._failed_stop_validation_result(
            result=result,
            context=context,
            open_stop_orders=open_stop_orders,
            native_qty=native_qty,
            validation=validation,
            attempt=attempt,
        )

    def _confirmed_stop_post_check_result(
        self,
        *,
        result: ExchangeOrderResult,
        context: _StopPostCheckContext,
        active_pos: Position,
        open_stop_orders: Sequence[Order],
        native_qty: Decimal,
        validation: RecoveryExitValidationResult,
        attempt: int,
    ) -> ExchangeOrderResult:
        fields = validation.diagnostic_fields(action="keep_existing_stop")
        confirmed_stop_price = validation.confirmed_stop_price
        position_metadata = _exchange_position_metadata(
            active_pos=active_pos,
            exchange=result.exchange,
            symbol=context.strategy_position.symbol,
            market_profile=context.market_profile,
            converter=context.converter,
        )
        verified = ExchangeOrderResult(
            exchange=result.exchange,
            ok=result.ok,
            order_id=result.order_id,
            client_order_id=result.client_order_id,
            status=result.status,
            side=result.side,
            quantity=result.quantity,
            filled_quantity=result.filled_quantity,
            avg_fill_price=result.avg_fill_price,
            fee=result.fee,
            fee_asset=result.fee_asset,
            raw={
                **dict(result.raw),
                "stop_post_check_attempts": attempt,
                **fields,
                "confirmed_stop_price": (
                    None
                    if confirmed_stop_price is None
                    else str(confirmed_stop_price)
                ),
                **position_metadata,
            },
        )
        logger.info(
            "Stop order post-check verified | exchange=%s position_qty=%s canonical_stop_price=%s effective_expected_stop_price=%s actual_exchange_stop_price=%s price_tick=%s open_stop_orders=%s attempts=%s",
            result.exchange.value,
            native_qty,
            context.canonical_stop_price,
            validation.effective_expected_stop_price,
            confirmed_stop_price,
            validation.price_tick,
            len(open_stop_orders),
            attempt,
        )
        return verified

    def _failed_stop_validation_result(
        self,
        *,
        result: ExchangeOrderResult,
        context: _StopPostCheckContext,
        open_stop_orders: Sequence[Order],
        native_qty: Decimal,
        validation: RecoveryExitValidationResult,
        attempt: int,
    ) -> ExchangeOrderResult:
        reason = validation.primary_invalid_reason or "missing_bot_owned_stop"
        detail_reason = validation.primary_invalid_detail_reason or reason
        fields = validation.diagnostic_fields(action="fail_post_check")
        logger.critical(
            "Stop order post-check failed after %s attempts | exchange=%s position_qty=%s canonical_stop_price=%s effective_expected_stop_price=%s actual_exchange_stop_price=%s price_tick=%s price_difference=%s open_stop_orders=%s invalid_category=%s invalid_detail_reason=%s",
            attempt,
            result.exchange.value,
            native_qty,
            context.canonical_stop_price,
            validation.effective_expected_stop_price,
            fields.get("actual_exchange_stop_price"),
            validation.price_tick,
            fields.get("price_difference"),
            len(open_stop_orders),
            reason,
            detail_reason,
        )
        self.context.alerts.emit(
            AppAlert(
                subject="AetherEdge stop order post-check failed",
                severity="critical",
                content=(
                    f"exchange={result.exchange.value}\n"
                    f"symbol={self.app_config.symbol}\n"
                    f"position_qty={native_qty}\n"
                    f"canonical_stop_price={context.canonical_stop_price}\n"
                    f"effective_expected_stop_price={validation.effective_expected_stop_price}\n"
                    f"actual_exchange_stop_price={fields.get('actual_exchange_stop_price')}\n"
                    f"price_tick={validation.price_tick}\n"
                    f"price_difference={fields.get('price_difference')}\n"
                    f"open_stop_orders={len(open_stop_orders)}\n"
                    f"invalid_category={reason}\n"
                    f"invalid_detail_reason={detail_reason}\n"
                    f"order_id={fields.get('existing_order_id')}\n"
                    f"client_order_id={fields.get('existing_client_order_id')}\n"
                    f"stop_post_check_attempts={attempt}\n"
                ),
            )
        )
        return self._stop_post_check_failed_result(
            result,
            reason=f"stop_post_check_failed:{reason}",
            metadata={
                "post_check": "stop_order_exchange_verification",
                "stop_post_check_attempts": attempt,
                "position_qty": str(native_qty),
                "desired_stop": str(context.canonical_stop_price),
                "open_stop_orders": len(open_stop_orders),
                "invalid_reason": reason,
                **fields,
            },
        )

    def _stop_post_check_failed_result(
        self,
        result: ExchangeOrderResult,
        *,
        reason: str,
        metadata: Mapping[str, Any],
    ) -> ExchangeOrderResult:
        return ExchangeOrderResult(
            exchange=result.exchange,
            ok=False,
            order_id=result.order_id,
            client_order_id=result.client_order_id,
            status=result.status,
            side=result.side,
            quantity=result.quantity,
            filled_quantity=result.filled_quantity,
            avg_fill_price=result.avg_fill_price,
            fee=result.fee,
            fee_asset=result.fee_asset,
            error=reason,
            raw={**dict(result.raw), **dict(metadata)},
        )

    def _save_order_results(self, signal: TradeSignal, results: Sequence[ExchangeOrderResult]) -> None:
        save_order = getattr(self.context.state_store, "save_order", None)
        if not callable(save_order):
            return
        is_stop = signal.action in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}
        for result in results:
            if not result.ok:
                continue
            if result.raw.get("execution_outcome") == "skipped_non_executable_quantity":
                continue
            save_order(
                Order(
                    exchange=result.exchange,
                    symbol=signal.symbol,
                    raw_symbol=signal.symbol,
                    order_id=result.order_id,
                    client_order_id=result.client_order_id,
                    status=result.status or OrderStatus.UNKNOWN,
                    side=result.side,
                    quantity=result.quantity,
                    filled_quantity=result.filled_quantity,
                    raw=result.raw,
                ),
                is_stop_order=is_stop,
            )

    async def _process_order_result_feedback(
        self,
        *,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
        source: str,
        event_time_ms: int | None,
    ) -> Sequence[TradeSignal]:
        follow_up = await self._strategy_host.on_order_results(
            signal=signal,
            results=results,
            source=source,
            event_time_ms=event_time_ms,
        )
        if follow_up is None:
            return ()
        follow_up_count = len(follow_up or ())
        if follow_up_count > 0:
            logger.info("Strategy order results processed | action=%s results=%s follow_up_signals=%s", signal.action.value, len(results), follow_up_count)
        else:
            logger.debug("Strategy order results processed | action=%s results=%s follow_up_signals=0", signal.action.value, len(results))
        return follow_up or ()

    def _get_execution_clients(self) -> tuple[ExecutionClient, ...]:
        if self._execution_clients is None:
            injected = self.service_dependencies().execution_clients
            if injected is not None:
                self._execution_clients = tuple(injected)
            else:
                self._execution_clients = tuple(
                    create_execution_client(exchange, symbol=self.app_config.symbol, config=ExchangeConfig.from_env(exchange))
                    for exchange in self.app_config.exchanges
                )
        return self._execution_clients

    def _get_order_journal(self):
        if self._order_journal is None:
            path = self._project_env.get("AETHER_ORDER_JOURNAL_DB", "data/state/aether_order_journal.sqlite3")
            self._order_journal = SqliteOrderJournalStore(path)
        return self._order_journal

    def _get_order_coordinator(self):
        if self._order_coordinator is None:
            journal = self._get_order_journal()
            self._order_coordinator = MultiExchangeOrderCoordinator(
                clients=self._get_execution_clients(),
                repository=journal,
                planner=self.context.planner,
                duplicate_guard=RepositoryDuplicateOrderGuard(journal),
                master_follower_policy=(
                    None
                    if self.runtime_config.master_follower_policy is None
                    else MasterFollowerExecutionPolicy.from_config(self.runtime_config.master_follower_policy)
                ),
                position_plan_store=self._get_position_plan_store(),
                post_result_validator=self._validate_order_results_before_journal,
            )
        return self._order_coordinator
