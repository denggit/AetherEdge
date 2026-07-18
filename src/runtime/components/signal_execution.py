from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from src.app.alerts import AppAlert
from src.order_management.models import ExchangeOrderResult
from src.platform.exchanges.models import ExchangeConfig, ExchangeName, InstrumentRule, Order, OrderStatus, Position, PositionMode, PositionSide
from src.runtime.models import RuntimeHealth, RuntimeMode, RuntimePhase
from src.runtime.signal_execution_service import (
    RuntimeSignalExecutionPlan,
    RuntimeSignalExecutionRequest,
    RuntimeSignalExecutionService,
)
from src.signals import TradeSignal
from src.signals.models import SignalAction

from src.runtime.live_types import (
    LiveRuntimeError, LiveRuntimeStats, MarketQueueDrainResult,
    StartupPreviewState, logger,
)
from src.runtime.components.base import RuntimeComponent


class SignalExecutionComponent(RuntimeComponent):
    async def _execute_signals(
        self,
        signals: Sequence[TradeSignal],
        *,
        source: str,
        event_time_ms: int | None,
        metadata: Mapping[str, Any] | None = None,
        feedback_depth: int = 0,
    ) -> None:
        await self._signal_execution_service.execute(
            RuntimeSignalExecutionRequest(
                signals=signals,
                source=source,
                event_time_ms=event_time_ms,
                metadata=metadata,
                feedback_depth=feedback_depth,
            ),
            RuntimeSignalExecutionPlan(
                prepare_signal=self._prepare_signal_execution,
                create_intent=self._create_signal_execution_intent,
                execute_intent=self._execute_signal_execution_intent,
                post_submit_sync=self._run_post_submit_order_sync,
                handle_results=self._handle_signal_execution_results,
                post_order_sync=self._run_post_order_account_sync,
                process_feedback=self._process_signal_execution_feedback,
                build_feedback_request=self._build_signal_feedback_request,
            ),
        )

    def _prepare_signal_execution(
        self,
        signal: TradeSignal,
        request: RuntimeSignalExecutionRequest,
    ) -> bool:
        self.stats.signals_seen += 1
        if self.app_config.dry_run:
            self.stats.dry_run_actions += 1
            logger.info(
                "Dry-run signal skipped | action=%s source=%s event_time_ms=%s",
                signal.action.value,
                request.source,
                request.event_time_ms,
            )
            return False
        # ── Entry guard: block new OPEN signals while account config
        #     is not verified due to existing exposure. ──
        exposure_increasing_actions = {
            SignalAction.OPEN_LONG,
            SignalAction.OPEN_SHORT,
        }
        if signal.action in exposure_increasing_actions:
            if self._has_account_config_entry_block():
                logger.warning(
                    "Blocking new entry — account config not verified due to existing exposure | action=%s source=%s",
                    signal.action.value,
                    request.source,
                )
                self.context.alerts.emit(
                    AppAlert(
                        subject="AetherEdge entry blocked: account config unverified",
                        severity="warning",
                        content=(
                            f"action={signal.action.value}\n"
                            f"source={request.source}\n"
                            f"reason=account_config_existing_exposure\n"
                        ),
                    )
                )
                return False
        # ── Entry guard: block new OPEN signals while any follower close
        #     is still unresolved after master close. ──
        if signal.action in {
            SignalAction.OPEN_LONG,
            SignalAction.OPEN_SHORT,
        }:
            purpose = str(
                signal.metadata.get("execution_purpose", "")
                if signal.metadata
                else ""
            ).strip().lower()
            if (
                purpose not in {"follower_recovery_topup"}
                and self._has_unresolved_follower_close()
            ):
                logger.warning(
                    "Blocking new entry — unresolved follower close after master close detected | action=%s source=%s",
                    signal.action.value,
                    request.source,
                )
                self.context.alerts.emit(
                    AppAlert(
                        subject="AetherEdge entry blocked due to unresolved follower close",
                        severity="warning",
                        content=(
                            f"action={signal.action.value}\n"
                            f"source={request.source}\n"
                            f"reason=unresolved_follower_close_after_master_close\n"
                        ),
                    )
                )
                return False
        logger.info(
            "Executing signal | action=%s source=%s event_time_ms=%s",
            signal.action.value,
            request.source,
            request.event_time_ms,
        )
        return True

    def _create_signal_execution_intent(
        self,
        signal: TradeSignal,
        request: RuntimeSignalExecutionRequest,
    ):
        return self._intent_factory.create(
            signal,
            source=request.source,
            event_time_ms=request.event_time_ms,
            metadata=request.metadata,
        )

    async def _execute_signal_execution_intent(self, intent):
        return await self._get_order_coordinator().execute(intent)

    async def _run_post_submit_order_sync(
        self,
        signal: TradeSignal,
        request: RuntimeSignalExecutionRequest,
    ) -> None:
        if self.requirements.order_state.post_submit_sync_enabled:
            logger.info(
                "Post-submit order sync started | action=%s source=%s",
                signal.action.value,
                request.source,
            )
            await self._get_order_sync_service().sync_once(
                sync_type="post_submit",
                priority=True,
            )

    def _handle_signal_execution_results(
        self,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
    ) -> None:
        self._record_order_results(results)
        self._save_order_results(signal, results)
        self._check_follower_close_failure(signal, results)

    async def _run_post_order_account_sync(
        self,
        signal: TradeSignal,
        request: RuntimeSignalExecutionRequest,
    ) -> None:
        if (
            self.requirements.account_state.post_order_sync_enabled
            and signal.action
            in {
                SignalAction.OPEN_LONG,
                SignalAction.OPEN_SHORT,
                SignalAction.CLOSE_LONG,
                SignalAction.CLOSE_SHORT,
            }
        ):
            await self._get_account_sync_service().sync_once(
                sync_type="post_order_account",
                priority=True,
            )

    async def _process_signal_execution_feedback(
        self,
        signal: TradeSignal,
        results: Sequence[ExchangeOrderResult],
        request: RuntimeSignalExecutionRequest,
    ):
        return await self._process_order_result_feedback(
            signal=signal,
            results=results,
            source=request.source,
            event_time_ms=request.event_time_ms,
        )

    def _build_signal_feedback_request(
        self,
        signal: TradeSignal,
        follow_up: Sequence[TradeSignal],
        request: RuntimeSignalExecutionRequest,
    ) -> RuntimeSignalExecutionRequest | None:
        if request.feedback_depth >= 5:
            logger.error(
                "Order result feedback depth exceeded | action=%s source=%s",
                signal.action.value,
                request.source,
            )
            self.context.alerts.emit(
                AppAlert(
                    subject="AetherEdge order feedback recursion blocked",
                    content=(
                        f"action={signal.action.value} source={request.source}"
                    ),
                    severity="error",
                )
            )
            return None
        return RuntimeSignalExecutionRequest(
            signals=follow_up,
            source="order_result_feedback",
            event_time_ms=request.event_time_ms,
            metadata={"parent_source": request.source},
            feedback_depth=request.feedback_depth + 1,
        )

    def _record_order_results(self, results: Sequence[ExchangeOrderResult]) -> None:
        self.stats.order_intents_created += 1
        self.stats.order_results_seen += len(results)
        ok_count = sum(1 for result in results if result.ok)
        if ok_count == len(results) and results:
            self.stats.submitted_intents += 1
            logger.info("Order intent submitted | exchanges=%s results=%s", ",".join(result.exchange.value for result in results), len(results))
            return
        if ok_count > 0:
            self.stats.partial_failures += 1
            logger.warning(
                "Order intent partially failed | ok=%s total=%s errors=%s",
                ok_count,
                len(results),
                [result.error for result in results if not result.ok],
            )
            self._set_health(
                RuntimePhase.RUNNING,
                healthy=False,
                error="partial exchange execution failure",
                metadata={**dict(self._health.metadata), "partial_failures": self.stats.partial_failures},
            )
        else:
            self.stats.failed_intents += 1
            logger.error("Order intent failed | total=%s errors=%s", len(results), [result.error for result in results])
            self._set_health(RuntimePhase.RUNNING, healthy=False, error="exchange execution failed")

    def _check_follower_close_failure(self, signal: TradeSignal, results: Sequence[ExchangeOrderResult]) -> None:
        purpose = str(signal.metadata.get("execution_purpose", "") if signal.metadata else "").strip().lower()
        if purpose != "follower_close_after_master_close":
            return
        now_ms = int(time.time() * 1000)
        position_id = str(signal.metadata.get("position_id", "unknown")) if signal.metadata else "unknown"
        target_exchanges = signal.metadata.get("target_exchanges", []) if signal.metadata else []
        # Check every targeted follower exchange independently. A single
        # filled result does not excuse another follower that is still open.
        for exchange_name in target_exchanges:
            exchange_str = str(exchange_name.value if hasattr(exchange_name, "value") else exchange_name).strip().lower()
            matched = [r for r in results if r.exchange.value == exchange_str]
            result = matched[0] if matched else None
            is_failure = (
                result is None
                or not result.ok
                or result.status is not OrderStatus.FILLED
                or result.filled_quantity is None
                or result.filled_quantity <= Decimal("0")
            )
            if not is_failure:
                continue
            throttle_key = f"{position_id}:{exchange_str}"
            last_ms = self._follower_close_alert_last_ms.get(throttle_key, 0)
            if now_ms - last_ms < 60_000:
                continue
            self._follower_close_alert_last_ms[throttle_key] = now_ms
            attempts = result.raw.get("attempts", 0) if result is not None and isinstance(result.raw, dict) else 0
            error_str = result.error if result is not None and result.error else ("missing result" if result is None else "not filled")
            self.context.alerts.emit(
                AppAlert(
                    subject="AetherEdge follower close failed after master close",
                    severity="error",
                    content=(
                        f"strategy_id={signal.metadata.get('strategy_id', 'unknown') if signal.metadata else 'unknown'}\n"
                        f"position_id={position_id}\n"
                        f"master_exchange={self.app_config.data_exchange.value}\n"
                        f"follower_exchange={exchange_str}\n"
                        f"symbol={signal.symbol}\n"
                        f"side={signal.action.value}\n"
                        f"quantity={str(signal.quantity)}\n"
                        f"status={result.status.value if result is not None and result.status else 'N/A'}\n"
                        f"filled_quantity={str(result.filled_quantity) if result is not None and result.filled_quantity is not None else 'N/A'}\n"
                        f"order_id={result.order_id if result is not None else 'N/A'}\n"
                        f"client_order_id={result.client_order_id if result is not None else 'N/A'}\n"
                        f"attempts={attempts}\n"
                        f"error={error_str}\n"
                        f"timestamp={now_ms}\n"
                    ),
                )
            )
            logger.error(
                "Follower close failed after master close | position_id=%s exchange=%s error=%s attempts=%s",
                position_id,
                exchange_str,
                error_str,
                attempts,
            )

    async def _validate_order_results_before_journal(
        self,
        *,
        intent,
        results: Sequence[ExchangeOrderResult],
    ) -> Sequence[ExchangeOrderResult]:
        if intent.signal.action not in {SignalAction.PLACE_STOP_LOSS_LONG, SignalAction.PLACE_STOP_LOSS_SHORT}:
            return results
        return await self._verify_stop_order_results(signal=intent.signal, results=results)
