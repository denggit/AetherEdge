from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Mapping, Sequence

from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus, OrderJournalEvent
from src.order_management.position_plan import LegSyncStatus
from src.order_management.ports import (
    ExecutionPreviewPort,
    OrderIntentRepository,
    PositionPlanStorePort,
    PositionPlanUpdaterPort,
)
from src.order_management.quantity import ExecutableQuantityResolution
from src.order_management.safety import ExitSafetyError
from src.planner import PlannedExecution
from src.platform.execution import ExecutionClient
from src.platform.exchanges.models import ExchangeName, OrderStatus
from src.utils.log import get_logger

logger = get_logger(__name__)


class ExecutionResultRecorder:
    def __init__(
        self,
        *,
        repository: OrderIntentRepository,
        position_plan_store: PositionPlanStorePort | None,
        position_plan_updater: PositionPlanUpdaterPort,
    ) -> None:
        self.repository = repository
        self.position_plan_store = position_plan_store
        self._position_plan_updater = position_plan_updater

    def record_skipped_recovery_topup(
        self,
        *,
        intent: OrderIntent,
        resolutions: Mapping[ExchangeName, ExecutableQuantityResolution],
    ) -> list[ExchangeOrderResult]:
        outcome = "skipped_non_executable_quantity"
        resolution_metadata = {
            exchange.value: resolution.metadata()
            for exchange, resolution in resolutions.items()
        }
        recovered_intent = replace(
            intent,
            status=OrderIntentStatus.RECOVERED,
            metadata={
                **dict(intent.metadata),
                "execution_outcome": outcome,
                "quantity_resolutions": resolution_metadata,
            },
        )
        self.repository.update_claimed_intent(recovered_intent)
        self.repository.update_status(
            intent_id=intent.intent_id, status=OrderIntentStatus.RECOVERED
        )
        results = [
            ExchangeOrderResult(
                exchange=exchange,
                ok=True,
                status=OrderStatus.CANCELED,
                quantity=Decimal("0"),
                filled_quantity=Decimal("0"),
                raw={
                    "execution_outcome": outcome,
                    **resolution.metadata(),
                },
            )
            for exchange, resolution in resolutions.items()
        ]
        save_result = getattr(self.repository, "save_result", None)
        if callable(save_result):
            for result in results:
                save_result(intent_id=intent.intent_id, result=result)
    
        if self.position_plan_store is not None and intent.signal.metadata:
            position_id = str(intent.signal.metadata.get("position_id") or "")
            apply_resolution = getattr(
                self.position_plan_store, "apply_recovery_leg_resolution", None
            )
            if position_id and callable(apply_resolution):
                for exchange, resolution in resolutions.items():
                    apply_resolution(
                        position_id=position_id,
                        exchange=exchange,
                        sync_status=LegSyncStatus.SYNCED,
                        metadata={
                            **dict(intent.signal.metadata),
                            "normalized_delta": str(
                                resolution.normalized_base_quantity
                            ),
                            "reason": "non_executable_rounding_dust",
                            "execution_outcome": outcome,
                        },
                    )
            self._position_plan_updater.advance_topup_generation(intent)
        logger.info(
            "Follower recovery top-up skipped before planning | "
            "intent_id=%s targets=%s outcome=%s resolutions=%s",
            intent.intent_id,
            [exchange.value for exchange in intent.target_exchanges],
            outcome,
            resolution_metadata,
        )
        return results

    def with_execution_metadata(
        self,
        intent: OrderIntent,
        *,
        clients: Sequence[ExecutionClient],
        items: Sequence[PlannedExecution],
        preview_conversion: ExecutionPreviewPort,
    ) -> OrderIntent:
        conversions: list[dict[str, object]] = []
        for client in clients:
            for sequence, item in enumerate(items):
                conversion = preview_conversion(client, item, intent=intent)
                if conversion:
                    conversion["sequence"] = sequence
                    conversion["action"] = item.action.value
                    conversions.append(conversion)
        metadata = {
            **dict(intent.metadata),
            "action": intent.signal.action.value,
            "signal_created_time_ms": intent.signal.created_time_ms,
            "original_canonical_quantity": None if intent.signal.quantity is None else str(intent.signal.quantity),
            "target_exchanges": [exchange.value for exchange in intent.target_exchanges],
            "quantity_semantics": "base_asset",
            "per_exchange_quantity": conversions,
        }
        return replace(intent, metadata=metadata)

    def record_exit_safety_event(self, *, intent: OrderIntent, exchange: ExchangeName, error: ExitSafetyError) -> None:
        add_event = getattr(self.repository, "add_event", None)
        if callable(add_event):
            add_event(
                OrderJournalEvent(
                    intent_id=intent.intent_id,
                    status=OrderIntentStatus.FAILED,
                    message="critical_exit_safety_rejected",
                    exchange=exchange,
                    metadata={"severity": "CRITICAL", "reason": error.reason, **dict(error.metadata)},
                )
            )


__all__ = ["ExecutionResultRecorder"]
