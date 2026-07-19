from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Mapping, Protocol, Sequence

from src.order_management.models import ExchangeOrderResult, OrderIntent, OrderIntentStatus
from src.platform.exchanges.models import ExchangeName
from src.signals.models import SignalAction

if TYPE_CHECKING:
    from src.order_management.position_plan import (
        LegPlan,
        LegSyncStatus,
        PositionPlan,
    )
    from src.order_management.quantity import ExecutableQuantityResolution
    from src.order_management.safety import ExitSafetyError
    from src.planner import PlannedExecution
    from src.platform.execution import ExecutionClient
    from src.platform.exchanges.models import (
        OrderRequest,
        StopMarketOrderRequest,
    )


class OrderIntentRepository(Protocol):
    def claim_intent(self, intent: OrderIntent) -> bool:
        ...

    def update_claimed_intent(self, intent: OrderIntent) -> None:
        ...

    def update_status(self, *, intent_id: str, status: OrderIntentStatus) -> None:
        ...

    def get_intent(self, intent_id: str) -> OrderIntent | None:
        ...


class ClientOrderIdFactory(Protocol):
    def create(
        self,
        *,
        intent_id: str,
        action: SignalAction,
        exchange: ExchangeName,
        sequence: int = 0,
    ) -> str:
        ...


class DuplicateOrderGuard(Protocol):
    def claim_or_raise(self, intent: OrderIntent) -> None:
        ...


class OrderCoordinatorPort(Protocol):
    async def execute(self, intent: OrderIntent) -> Sequence[ExchangeOrderResult]:
        ...


class PositionPlanStorePort(Protocol):
    def upsert_position(self, plan: PositionPlan) -> None: ...

    def upsert_leg(self, leg: LegPlan) -> None: ...

    def get_position(self, position_id: str) -> PositionPlan | None: ...

    def get_legs(self, position_id: str) -> tuple[LegPlan, ...]: ...

    def update_leg_sync_status(
        self,
        *,
        position_id: str,
        exchange: ExchangeName | str,
        sync_status: LegSyncStatus | str,
    ) -> None: ...

    def add_to_leg_target(
        self,
        *,
        position_id: str,
        exchange: ExchangeName,
        delta_target_qty_base: Decimal,
        delta_filled_qty_base: Decimal = Decimal("0"),
    ) -> None: ...

    def update_stop(
        self,
        *,
        position_id: str,
        stop_price: Decimal,
        exchange: ExchangeName | None = None,
        stop_order_id: str | None = None,
        stop_client_order_id: str | None = None,
        update_canonical: bool = True,
    ) -> None: ...


class ExecutionPreviewPort(Protocol):
    def __call__(
        self,
        client: ExecutionClient,
        item: PlannedExecution,
        *,
        intent: OrderIntent,
    ) -> dict[str, object] | None: ...


class OrderSafetyValidatorPort(Protocol):
    async def normalize_order(
        self,
        client: ExecutionClient,
        action: SignalAction,
        request: OrderRequest,
    ) -> OrderRequest: ...

    async def normalize_stop(
        self,
        client: ExecutionClient,
        action: SignalAction,
        request: StopMarketOrderRequest,
    ) -> StopMarketOrderRequest: ...


class ExecutionResultRecorderPort(Protocol):
    def record_skipped_recovery_topup(
        self,
        *,
        intent: OrderIntent,
        resolutions: Mapping[ExchangeName, ExecutableQuantityResolution],
    ) -> list[ExchangeOrderResult]: ...

    def record_exit_safety_event(
        self,
        *,
        intent: OrderIntent,
        exchange: ExchangeName,
        error: ExitSafetyError,
    ) -> None: ...

    def with_execution_metadata(
        self,
        intent: OrderIntent,
        *,
        clients: Sequence[ExecutionClient],
        items: Sequence[PlannedExecution],
        preview_conversion: ExecutionPreviewPort,
    ) -> OrderIntent: ...


class PositionPlanUpdaterPort(Protocol):
    def record_position_plan(
        self,
        intent: OrderIntent,
        results: Sequence[ExchangeOrderResult],
    ) -> None: ...

    def advance_topup_generation(self, intent: OrderIntent) -> None: ...


class MultiExchangeExecutorPort(Protocol):
    async def execute_for_client(
        self,
        client: ExecutionClient,
        intent: OrderIntent,
        items: Sequence[PlannedExecution],
        *,
        max_attempts: int = 1,
        retry_delay_seconds: float = 0.0,
    ) -> list[ExchangeOrderResult]: ...


class PostResultValidatorPort(Protocol):
    async def __call__(
        self,
        *,
        intent: OrderIntent,
        results: Sequence[ExchangeOrderResult],
    ) -> Sequence[ExchangeOrderResult]: ...
