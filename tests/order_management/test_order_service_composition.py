from __future__ import annotations

from src.order_management.coordinator import (
    ExecutionResultRecorder,
    MasterFollowerExecutor,
    MultiExchangeExecutor,
    OrderIntentPlanner,
    OrderSafetyValidator,
    PositionPlanUpdater,
)
from src.order_management.idempotency.client_order_id import (
    DeterministicClientOrderIdFactory,
)
from src.order_management.master_follower import MasterFollowerExecutionPolicy
from src.order_management.quantity import NativeQuantityConverter
from src.order_management.safety import ExitSafetyGuard
from src.order_management.sync import OrderStatusSynchronizer
from src.platform.exchanges.models import ExchangeName


def test_order_services_construct_independently_with_explicit_dependencies() -> None:
    repository = object()
    converter = NativeQuantityConverter()
    updater = PositionPlanUpdater(
        repository=repository,
        position_plan_store=None,
        master_follower_policy=None,
    )
    recorder = ExecutionResultRecorder(
        repository=repository,
        position_plan_store=None,
        position_plan_updater=updater,
    )
    safety = OrderSafetyValidator(
        exit_safety_guard=ExitSafetyGuard(quantity_converter=converter),
    )
    executor = MultiExchangeExecutor(
        client_order_id_factory=DeterministicClientOrderIdFactory(),
        quantity_converter=converter,
        order_status_synchronizer=OrderStatusSynchronizer(),
        safety_validator=safety,
        result_recorder=recorder,
    )
    intent_planner = OrderIntentPlanner(
        clients=(),
        quantity_converter=converter,
        result_recorder=recorder,
    )
    master_follower = MasterFollowerExecutor(
        policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
        executor=executor,
    )

    assert intent_planner._result_recorder is recorder
    assert executor._safety_validator is safety
    assert executor._result_recorder is recorder
    assert recorder._position_plan_updater is updater
    assert master_follower._executor is executor
