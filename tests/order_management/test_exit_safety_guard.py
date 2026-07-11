from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import (
    LegSyncStatus,
    MasterFollowerExecutionPolicy,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    OrderIntentStatus,
    PositionPlanStatus,
    RetryPolicy,
    SqlitePositionPlanStore,
)
from src.order_management.models import ExchangeOrderResult, OrderJournalEvent
from src.order_management.safety import ExitSafetyError, ExitSafetyGuard
from src.platform import ExchangeName, Order, OrderSide, OrderStatus, OrderType, Position, PositionMode, PositionSide, get_market_profile
from src.platform.exchanges.models import OrderRequest, StopMarketOrderRequest
from src.signals import SignalAction, TradeSignal


class MemoryOrderJournal:
    def __init__(self) -> None:
        self.intents: dict[str, OrderIntent] = {}
        self.statuses: dict[str, OrderIntentStatus] = {}
        self.results: list[tuple[str, ExchangeOrderResult]] = []
        self.events: list[OrderJournalEvent] = []

    def claim_intent(self, intent: OrderIntent) -> bool:
        if intent.intent_id in self.intents:
            return False
        self.intents[intent.intent_id] = intent
        self.statuses[intent.intent_id] = intent.status
        return True

    def update_claimed_intent(self, intent: OrderIntent) -> None:
        if intent.intent_id not in self.intents:
            raise ValueError(f"intent not claimed: {intent.intent_id}")
        self.intents[intent.intent_id] = intent
        self.statuses[intent.intent_id] = intent.status

    def update_status(self, *, intent_id: str, status: OrderIntentStatus) -> None:
        self.statuses[intent_id] = status

    def save_result(self, *, intent_id: str, result: ExchangeOrderResult) -> None:
        self.results.append((intent_id, result))

    def add_event(self, event: OrderJournalEvent) -> None:
        self.events.append(event)

    def get_intent(self, intent_id: str) -> OrderIntent | None:
        intent = self.intents.get(intent_id)
        if intent is None:
            return None
        return intent


class SafetyFakeClient:
    def __init__(
        self,
        exchange: ExchangeName,
        *,
        position_mode: PositionMode = PositionMode.ONE_WAY,
        positions: tuple[Position, ...] = (),
        fail_order_times: int = 0,
    ) -> None:
        self.exchange = exchange
        self.symbol = "ETH-USDT-PERP"
        self.market_profile = get_market_profile("ETH-USDT-PERP")
        self.position_mode = position_mode
        self.positions = positions
        self.fail_order_times = fail_order_times
        self.order_attempts = 0
        self.orders: list[OrderRequest] = []
        self.stop_orders: list[StopMarketOrderRequest] = []

    async def fetch_position_mode(self) -> PositionMode:
        return self.position_mode

    async def fetch_positions(self) -> list[Position]:
        return list(self.positions)

    async def place_order(self, request: OrderRequest) -> Order:
        self.order_attempts += 1
        if self.order_attempts <= self.fail_order_times:
            raise RuntimeError("HTTP 400 code=-4061 Order's position side does not match user's setting.")
        self.orders.append(request)
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=f"{self.exchange.value}-order",
            client_order_id=request.client_order_id,
            status=OrderStatus.FILLED,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            filled_quantity=request.quantity,
            raw={"avgPx": "2000"},
        )

    async def place_stop_market_order(self, request: StopMarketOrderRequest) -> Order:
        self.stop_orders.append(request)
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=f"{self.exchange.value}-stop",
            client_order_id=request.client_order_id,
            status=OrderStatus.NEW,
            side=request.side,
            order_type=OrderType.MARKET,
            price=request.trigger_price,
            quantity=request.quantity,
            raw={},
        )

    async def cancel_all_orders(self):
        return []

    async def cancel_all_stop_orders(self):
        return []


def _position(exchange: ExchangeName, side: PositionSide, quantity: Decimal) -> Position:
    return Position(exchange=exchange, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-PERP", side=side, quantity=quantity)


async def _execute_one(client: SafetyFakeClient, signal: TradeSignal):
    repo = MemoryOrderJournal()
    coordinator = MultiExchangeOrderCoordinator(clients=[client], repository=repo)
    intent = OrderIntent(
        intent_id=f"intent-{signal.action.value}-{client.exchange.value}",
        strategy_id="v8",
        signal=signal,
        target_exchanges=(client.exchange,),
    )
    results = await coordinator.execute(intent)
    return results, repo


@pytest.mark.asyncio
async def test_binance_hedge_open_short_sets_position_side_short() -> None:
    client = SafetyFakeClient(ExchangeName.BINANCE, position_mode=PositionMode.HEDGE)

    await _execute_one(client, TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_SHORT, quantity=Decimal("0.1")))

    assert client.orders[0].position_side is PositionSide.SHORT
    assert client.orders[0].reduce_only is False


@pytest.mark.asyncio
async def test_binance_hedge_open_long_sets_position_side_long() -> None:
    client = SafetyFakeClient(ExchangeName.BINANCE, position_mode=PositionMode.HEDGE)

    await _execute_one(client, TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1")))

    assert client.orders[0].position_side is PositionSide.LONG
    assert client.orders[0].reduce_only is False


@pytest.mark.asyncio
async def test_binance_hedge_close_short_is_position_side_short_and_exit_safe(caplog) -> None:
    client = SafetyFakeClient(
        ExchangeName.BINANCE,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-0.233")),),
    )

    with caplog.at_level("INFO"):
        results, _ = await _execute_one(client, TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CLOSE_SHORT, quantity=Decimal("0.233")))

    assert results[0].ok is True
    assert client.orders[0].side is OrderSide.BUY
    assert client.orders[0].position_side is PositionSide.SHORT
    assert client.orders[0].quantity == Decimal("0.233")
    assert client.orders[0].reduce_only is False
    assert "Binance hedge exit request normalized" in caplog.text
    assert "exit_safety_equivalent_reduce_only=True" in caplog.text
    assert "reduce_only_omitted_reason=binance_hedge_mode_api_constraint" in caplog.text


@pytest.mark.asyncio
async def test_binance_hedge_close_long_is_position_side_long_and_exit_safe() -> None:
    client = SafetyFakeClient(
        ExchangeName.BINANCE,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("0.233")),),
    )

    results, _ = await _execute_one(client, TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CLOSE_LONG, quantity=Decimal("0.233")))

    assert results[0].ok is True
    assert client.orders[0].side is OrderSide.SELL
    assert client.orders[0].position_side is PositionSide.LONG
    assert client.orders[0].quantity == Decimal("0.233")
    assert client.orders[0].reduce_only is False


@pytest.mark.asyncio
async def test_binance_hedge_short_stop_is_position_side_short_and_exit_safe() -> None:
    client = SafetyFakeClient(
        ExchangeName.BINANCE,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-0.233")),),
    )

    results, _ = await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_SHORT, quantity=Decimal("0.233"), trigger_price=Decimal("2100")),
    )

    assert results[0].ok is True
    assert client.stop_orders[0].side is OrderSide.BUY
    assert client.stop_orders[0].position_side is PositionSide.SHORT
    assert client.stop_orders[0].close_position is True
    assert client.stop_orders[0].quantity is None
    assert client.stop_orders[0].reduce_only is False


@pytest.mark.asyncio
async def test_binance_hedge_long_stop_is_position_side_long_and_exit_safe() -> None:
    client = SafetyFakeClient(
        ExchangeName.BINANCE,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("0.233")),),
    )

    results, _ = await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_LONG, quantity=Decimal("0.233"), trigger_price=Decimal("1900")),
    )

    assert results[0].ok is True
    assert client.stop_orders[0].side is OrderSide.SELL
    assert client.stop_orders[0].position_side is PositionSide.LONG
    assert client.stop_orders[0].close_position is True
    assert client.stop_orders[0].quantity is None
    assert client.stop_orders[0].reduce_only is False


@pytest.mark.asyncio
async def test_okx_one_way_short_stop_is_reduce_only() -> None:
    client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.ONE_WAY,
        positions=(_position(ExchangeName.OKX, PositionSide.BOTH, Decimal("-2")),),
    )

    await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_SHORT, quantity=Decimal("0.2"), trigger_price=Decimal("2100")),
    )

    assert client.stop_orders[0].position_side is None
    assert client.stop_orders[0].reduce_only is True
    assert client.stop_orders[0].quantity == Decimal("2")


@pytest.mark.asyncio
async def test_okx_one_way_long_stop_is_reduce_only() -> None:
    client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.ONE_WAY,
        positions=(_position(ExchangeName.OKX, PositionSide.BOTH, Decimal("2")),),
    )

    await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_LONG, quantity=Decimal("0.2"), trigger_price=Decimal("1900")),
    )

    assert client.stop_orders[0].position_side is None
    assert client.stop_orders[0].reduce_only is True
    assert client.stop_orders[0].quantity == Decimal("2")


@pytest.mark.asyncio
async def test_okx_hedge_short_stop_sets_pos_side_short_reduce_only() -> None:
    client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.OKX, PositionSide.SHORT, Decimal("2")),),
    )

    await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_SHORT, quantity=Decimal("0.2"), trigger_price=Decimal("2100")),
    )

    assert client.stop_orders[0].position_side is PositionSide.SHORT
    assert client.stop_orders[0].reduce_only is True
    assert client.stop_orders[0].quantity == Decimal("2")


@pytest.mark.asyncio
async def test_okx_hedge_long_stop_sets_pos_side_long_reduce_only() -> None:
    client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.OKX, PositionSide.LONG, Decimal("2")),),
    )

    await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_LONG, quantity=Decimal("0.2"), trigger_price=Decimal("1900")),
    )

    assert client.stop_orders[0].position_side is PositionSide.LONG
    assert client.stop_orders[0].reduce_only is True
    assert client.stop_orders[0].quantity == Decimal("2")


@pytest.mark.asyncio
async def test_okx_close_short_is_reduce_only() -> None:
    client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.OKX, PositionSide.SHORT, Decimal("2")),),
    )

    await _execute_one(client, TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CLOSE_SHORT, quantity=Decimal("0.2")))

    assert client.orders[0].position_side is PositionSide.SHORT
    assert client.orders[0].reduce_only is True
    assert client.orders[0].quantity == Decimal("2")


@pytest.mark.asyncio
async def test_okx_close_long_is_reduce_only() -> None:
    client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.OKX, PositionSide.LONG, Decimal("2")),),
    )

    await _execute_one(client, TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CLOSE_LONG, quantity=Decimal("0.2")))

    assert client.orders[0].position_side is PositionSide.LONG
    assert client.orders[0].reduce_only is True
    assert client.orders[0].quantity == Decimal("2")


@pytest.mark.asyncio
async def test_stop_quantity_exceeding_position_is_shrunk_instead_of_blocked() -> None:
    """stop-loss protective exit above position is now shrunk to position instead of rejected."""
    client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.ONE_WAY,
        positions=(_position(ExchangeName.OKX, PositionSide.BOTH, Decimal("-2.82")),),
    )

    results, repo = await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_SHORT, quantity=Decimal("2.82"), trigger_price=Decimal("1719.40")),
    )

    # Protective exits no longer rejected — they shrink to actual position.
    assert results[0].ok is True
    assert client.stop_orders
    # The shrunk quantity matches the actual position (2.82 native contracts)
    assert client.stop_orders[0].quantity == Decimal("2.82")
    # No critical rejection event — order was shrunk, not rejected
    assert not any(
        event.message == "critical_exit_safety_rejected"
        for event in repo.events
    )


@pytest.mark.asyncio
async def test_binance_hedge_close_short_quantity_exceeding_position_is_rejected() -> None:
    client = SafetyFakeClient(
        ExchangeName.BINANCE,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-0.233")),),
    )

    results, _ = await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CLOSE_SHORT, quantity=Decimal("2.33")),
    )

    assert results[0].ok is False
    assert "exit_order_quantity_exceeding_position" in (results[0].error or "")
    assert client.orders == []


@pytest.mark.asyncio
async def test_binance_hedge_short_stop_without_short_position_is_rejected() -> None:
    client = SafetyFakeClient(
        ExchangeName.BINANCE,
        position_mode=PositionMode.HEDGE,
        positions=(),
    )

    results, _ = await _execute_one(
        client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_SHORT, quantity=Decimal("0.233"), trigger_price=Decimal("2100")),
    )

    assert results[0].ok is False
    assert "stop_order_without_existing_position" in (results[0].error or "")
    assert client.stop_orders == []


@pytest.mark.asyncio
async def test_okx_exit_still_sends_reduce_only_when_supported() -> None:
    close_client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.OKX, PositionSide.LONG, Decimal("2")),),
    )
    stop_client = SafetyFakeClient(
        ExchangeName.OKX,
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.OKX, PositionSide.LONG, Decimal("2")),),
    )

    close_results, _ = await _execute_one(close_client, TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CLOSE_LONG, quantity=Decimal("0.2")))
    stop_results, _ = await _execute_one(
        stop_client,
        TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.PLACE_STOP_LOSS_LONG, quantity=Decimal("0.2"), trigger_price=Decimal("1900")),
    )

    assert close_results[0].ok is True
    assert close_client.orders[0].position_side is PositionSide.LONG
    assert close_client.orders[0].reduce_only is True
    assert close_client.orders[0].quantity == Decimal("2")
    assert stop_results[0].ok is True
    assert stop_client.stop_orders[0].position_side is PositionSide.LONG
    assert stop_client.stop_orders[0].reduce_only is True
    assert stop_client.stop_orders[0].quantity == Decimal("2")


def test_exit_order_without_reduce_only_or_close_position_is_rejected() -> None:
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
            exchange=ExchangeName.BINANCE,
            action=SignalAction.CLOSE_SHORT,
            request=OrderRequest(symbol="ETH-USDT-PERP", side=OrderSide.BUY, order_type=OrderType.MARKET, quantity=Decimal("0.1"), reduce_only=False),
            position_mode=PositionMode.HEDGE,
            positions=(_position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-0.1")),),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_without_reduce_only_or_close_position"


def test_exit_order_quantity_exceeding_position_is_rejected() -> None:
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
            exchange=ExchangeName.BINANCE,
            action=SignalAction.CLOSE_LONG,
            request=OrderRequest(symbol="ETH-USDT-PERP", side=OrderSide.SELL, order_type=OrderType.MARKET, quantity=Decimal("0.2"), reduce_only=True),
            position_mode=PositionMode.HEDGE,
            positions=(_position(ExchangeName.BINANCE, PositionSide.LONG, Decimal("0.1")),),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_quantity_exceeding_position"


def test_exit_order_wrong_position_side_is_rejected() -> None:
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
            exchange=ExchangeName.BINANCE,
            action=SignalAction.CLOSE_SHORT,
            request=OrderRequest(
                symbol="ETH-USDT-PERP",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.1"),
                reduce_only=True,
                position_side=PositionSide.LONG,
            ),
            position_mode=PositionMode.HEDGE,
            positions=(_position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-0.1")),),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_wrong_position_side"


def test_stop_order_without_existing_position_is_rejected() -> None:
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_stop_market(
            exchange=ExchangeName.BINANCE,
            action=SignalAction.PLACE_STOP_LOSS_SHORT,
            request=StopMarketOrderRequest(symbol="ETH-USDT-PERP", side=OrderSide.BUY, quantity=Decimal("0.1"), trigger_price=Decimal("2100"), reduce_only=True),
            position_mode=PositionMode.HEDGE,
            positions=(),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "stop_order_without_existing_position"


def test_take_profit_order_without_reduce_only_is_rejected_if_take_profit_supported() -> None:
    guard = ExitSafetyGuard()

    with pytest.raises(ExitSafetyError) as exc:
        guard.normalize_order(
            exchange=ExchangeName.BINANCE,
            action="take_profit_short",
            request=OrderRequest(symbol="ETH-USDT-PERP", side=OrderSide.BUY, order_type=OrderType.LIMIT, quantity=Decimal("0.1"), price=Decimal("1900"), reduce_only=False),
            position_mode=PositionMode.HEDGE,
            positions=(_position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-0.1")),),
            market_profile=get_market_profile("ETH-USDT-PERP"),
        )

    assert exc.value.reason == "exit_order_without_reduce_only_or_close_position"


def test_binance_hedge_short_take_profit_sets_position_side_short_reduce_only() -> None:
    guard = ExitSafetyGuard()

    request, report = guard.normalize_order(
        exchange=ExchangeName.BINANCE,
        action="take_profit_short",
        request=OrderRequest(symbol="ETH-USDT-PERP", side=OrderSide.BUY, order_type=OrderType.LIMIT, quantity=Decimal("0.1"), price=Decimal("1900"), reduce_only=True),
        position_mode=PositionMode.HEDGE,
        positions=(_position(ExchangeName.BINANCE, PositionSide.SHORT, Decimal("-0.1")),),
        market_profile=get_market_profile("ETH-USDT-PERP"),
    )

    assert request.position_side is PositionSide.SHORT
    assert request.reduce_only is True
    assert report is not None


@pytest.mark.asyncio
async def test_follower_entry_failure_still_never_closes_master_after_binance_hedge_reduce_only_patch(tmp_path) -> None:
    repo = MemoryOrderJournal()
    plan_store = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    okx = SafetyFakeClient(ExchangeName.OKX, position_mode=PositionMode.ONE_WAY)
    binance = SafetyFakeClient(ExchangeName.BINANCE, position_mode=PositionMode.HEDGE, fail_order_times=3)
    policy = MasterFollowerExecutionPolicy(
        master_exchange=ExchangeName.OKX,
        follower_exchanges=(ExchangeName.BINANCE,),
        follower_entry_retry=RetryPolicy(max_attempts=3, retry_delay_seconds=0),
    )
    coordinator = MultiExchangeOrderCoordinator(
        clients=[okx, binance],
        repository=repo,
        master_follower_policy=policy,
        position_plan_store=plan_store,
    )
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_SHORT,
        quantity=Decimal("0.282"),
        metadata={"position_id": "pos-follower-fail", "target_exchanges": ["okx", "binance"], "execution_purpose": "normal_entry"},
    )
    intent = OrderIntent(intent_id="intent-follower-fail", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))

    results = await coordinator.execute(intent)

    assert [result.ok for result in results] == [True, False]
    assert repo.statuses["intent-follower-fail"] is OrderIntentStatus.PARTIALLY_SUBMITTED
    assert len(okx.orders) == 1
    assert okx.orders[0].side is OrderSide.SELL
    assert all(order.side is not OrderSide.BUY for order in okx.orders)
    plan = plan_store.get_position("pos-follower-fail")
    assert plan is not None
    assert plan.status is PositionPlanStatus.ACTIVE
    legs = {leg.exchange: leg for leg in plan_store.get_legs("pos-follower-fail")}
    assert legs[ExchangeName.OKX].sync_status is LegSyncStatus.OPEN
    assert legs[ExchangeName.BINANCE].sync_status is LegSyncStatus.FOLLOWER_ENTRY_FAILED
    assert any(event.message == "critical_follower_entry_failed" and event.metadata["severity"] == "CRITICAL" for event in repo.events)
