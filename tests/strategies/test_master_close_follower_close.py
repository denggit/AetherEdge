from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.platform import ExchangeName
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import OrderSide, OrderStatus
from src.platform.exchanges.models import Order, Position, PositionSide
from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    MasterFollowerExecutionPolicy,
    MultiExchangeOrderCoordinator,
    PositionPlan,
    PositionPlanStatus,
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
)
from src.order_management.idempotency import DuplicateIntentError
from src.runtime.orders import LiveOrderIntentFactory
from src.runtime.runner import LiveRuntimeRunner
from src.signals import SignalAction
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy
from strategies.eth_portfolio_v1.domain.models import Side as PortfolioSide
from strategies.eth_portfolio_v1.strategy import Strategy as PortfolioStrategy


@pytest.mark.asyncio
async def test_master_close_fill_emits_follower_reduce_only_close_for_open_follower_legs() -> None:
    strategy = Strategy()
    strategy.position.open_master(side=Side.LONG, entry_time_ms=1, avg_entry=Decimal("2000"), qty=Decimal("0.2"), stop_price=Decimal("1900"), entry_engine="MOMENTUM_V3", position_id="p1")
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.2"))
    strategy.position.mark_leg_open(exchange="binance", avg_fill_price=Decimal("2001"), base_qty=Decimal("0.11"))

    signals = await strategy.on_account_event(
        AccountEvent(exchange=ExchangeName.OKX, event_type=AccountEventType.ORDER, symbol="ETH-USDT-PERP", order_status=OrderStatus.FILLED, side=OrderSide.SELL, price=Decimal("1990"), filled_quantity=Decimal("0.2"), event_time_ms=2)
    )

    assert len(signals) == 1
    assert signals[0].action is SignalAction.CLOSE_LONG
    assert signals[0].quantity == Decimal("0.11")
    assert signals[0].metadata["target_exchanges"] == ["binance"]
    assert signals[0].metadata["execution_purpose"] == "follower_close_after_master_close"
    assert strategy.position.in_pos is False


@pytest.mark.asyncio
async def test_follower_close_fill_does_not_reset_master_canonical_position() -> None:
    strategy = Strategy()
    strategy.position.open_master(side=Side.LONG, entry_time_ms=1, avg_entry=Decimal("2000"), qty=Decimal("0.2"), stop_price=Decimal("1900"), entry_engine="MOMENTUM_V3", position_id="p1")
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.2"))
    strategy.position.mark_leg_open(exchange="binance", avg_fill_price=Decimal("2001"), base_qty=Decimal("0.11"))

    signals = await strategy.on_account_event(
        AccountEvent(exchange=ExchangeName.BINANCE, event_type=AccountEventType.ORDER, symbol="ETH-USDT-PERP", order_status=OrderStatus.FILLED, side=OrderSide.SELL, price=Decimal("1990"), filled_quantity=Decimal("0.11"), event_time_ms=2)
    )

    assert signals == []
    assert strategy.position.in_pos is True
    assert strategy.position.legs["binance"].is_open is False
    assert strategy.position.legs["okx"].is_open is True


class _SuccessfulFollowerCloseClient:
    exchange = ExchangeName.BINANCE
    symbol = "ETH-USDT-PERP"

    def __init__(self) -> None:
        self.place_order_calls = 0

    async def fetch_position_mode(self):
        from src.platform.exchanges.models import PositionMode

        return PositionMode.ONE_WAY

    async def place_order(self, request):
        self.place_order_calls += 1
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol="ETHUSDT",
            order_id="follower-close-order",
            client_order_id=request.client_order_id,
            status=OrderStatus.FILLED,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            filled_quantity=request.quantity,
            raw={"avgPx": "1990"},
        )


@pytest.mark.asyncio
async def test_production_follower_close_builders_share_generation_identity(
    tmp_path,
) -> None:
    position_id = "portfolio-v1-follower-close-generation"
    plan_path = tmp_path / "follower-close-plans.sqlite3"
    journal_path = tmp_path / "follower-close-journal.sqlite3"
    plans = SqlitePositionPlanStore(plan_path)
    plans.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
            canonical_stop_price=Decimal("1900"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.2"),
            master_filled_qty_base=Decimal("0.2"),
            metadata={"follower_close_generation": 0},
        )
    )
    plans.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.2"),
            filled_qty_base=Decimal("0.2"),
            sync_status=LegSyncStatus.CLOSED,
        )
    )
    plans.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.11"),
            filled_qty_base=Decimal("0.11"),
            sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
        )
    )

    initial_strategy = PortfolioStrategy()
    initial_strategy.position.open_master(
        side=PortfolioSide.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2000"),
        qty=Decimal("0.2"),
        stop_price=Decimal("1900"),
        entry_engine="BULL_RECLAIM_V2",
        position_id=position_id,
    )
    initial_strategy.position.mark_leg_open(
        exchange="okx", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.2")
    )
    initial_strategy.position.mark_leg_open(
        exchange="binance", avg_fill_price=Decimal("2001"), base_qty=Decimal("0.11")
    )
    initial = initial_strategy._follower_close_signals_after_master_close(
        event_time_ms=2
    )[0]
    assert initial.metadata["follower_close_generation"] == 0
    assert initial.metadata["master_close_event_time_ms"] == 2

    payload = plans.serialize_active_positions()[0]
    recovery_strategy = PortfolioStrategy()
    recovery = recovery_strategy._recover_master_closed_with_active_plan(
        snapshots={
            "binance": SimpleNamespace(
                positions=[
                    Position(
                        exchange=ExchangeName.BINANCE,
                        symbol="ETH-USDT-PERP",
                        raw_symbol="ETHUSDT",
                        side=PositionSide.LONG,
                        quantity=Decimal("0.11"),
                        entry_price=Decimal("2001"),
                    )
                ]
            )
        },
        plan_payload=payload,
    )[0]

    runner = LiveRuntimeRunner.__new__(LiveRuntimeRunner)
    runner._position_plan_store = plans
    runner.app_config = SimpleNamespace(symbol="ETH-USDT-PERP")
    periodic = runner._build_unresolved_follower_close_signals()[0]

    factory = LiveOrderIntentFactory(
        strategy_id=initial_strategy.config.strategy_id,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )
    initial_intent = factory.create(
        initial, source="account_event", event_time_ms=2
    )
    recovery_intent = factory.create(recovery, source="startup_recovery")
    periodic_intent = factory.create(
        periodic, source="periodic_follower_close_check"
    )
    assert initial_intent.intent_id == recovery_intent.intent_id
    assert initial_intent.intent_id == periodic_intent.intent_id

    client = _SuccessfulFollowerCloseClient()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(journal_path),
        position_plan_store=plans,
        master_follower_policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
    )
    await coordinator.execute(initial_intent)
    persisted_plan = SqlitePositionPlanStore(plan_path).get_position(position_id)
    assert persisted_plan.metadata["follower_close_generation"] == 1

    with pytest.raises(DuplicateIntentError):
        await coordinator.execute(recovery_intent)
    assert client.place_order_calls == 1

    next_payload = {
        **payload,
        "position": {
            **dict(payload["position"]),
            "metadata": dict(persisted_plan.metadata),
        },
    }
    next_recovery = PortfolioStrategy()._recover_master_closed_with_active_plan(
        snapshots={
            "binance": SimpleNamespace(
                positions=[
                    Position(
                        exchange=ExchangeName.BINANCE,
                        symbol="ETH-USDT-PERP",
                        raw_symbol="ETHUSDT",
                        side=PositionSide.LONG,
                        quantity=Decimal("0.11"),
                        entry_price=Decimal("2001"),
                    )
                ]
            )
        },
        plan_payload=next_payload,
    )[0]
    next_intent = factory.create(next_recovery, source="startup_recovery")
    assert next_recovery.metadata["follower_close_generation"] == 1
    assert next_intent.intent_id != initial_intent.intent_id


@pytest.mark.asyncio
async def test_follower_close_crash_before_generation_write_is_claim_protected(
    tmp_path,
    monkeypatch,
) -> None:
    position_id = "portfolio-v1-follower-close-crash"
    plan_path = tmp_path / "follower-close-crash-plans.sqlite3"
    journal_path = tmp_path / "follower-close-crash-journal.sqlite3"
    plans = SqlitePositionPlanStore(plan_path)
    plans.upsert_position(
        PositionPlan(
            position_id=position_id,
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.MASTER_CLOSED_FOLLOWER_CLOSE_REQUIRED,
            canonical_stop_price=Decimal("1900"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.2"),
            master_filled_qty_base=Decimal("0.2"),
            metadata={"follower_close_generation": 0},
        )
    )
    plans.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.OKX,
            role=LegRole.MASTER,
            target_qty_base=Decimal("0.2"),
            filled_qty_base=Decimal("0.2"),
            sync_status=LegSyncStatus.CLOSED,
        )
    )
    plans.upsert_leg(
        LegPlan(
            position_id=position_id,
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.11"),
            filled_qty_base=Decimal("0.11"),
            sync_status=LegSyncStatus.FOLLOWER_CLOSE_FAILED,
        )
    )
    payload = plans.serialize_active_positions()[0]
    strategy = PortfolioStrategy()
    strategy.position.open_master(
        side=PortfolioSide.LONG,
        entry_time_ms=1,
        avg_entry=Decimal("2000"),
        qty=Decimal("0.2"),
        stop_price=Decimal("1900"),
        entry_engine="BULL_RECLAIM_V2",
        position_id=position_id,
    )
    strategy.position.mark_leg_open(
        exchange="okx", avg_fill_price=Decimal("2000"), base_qty=Decimal("0.2")
    )
    strategy.position.mark_leg_open(
        exchange="binance", avg_fill_price=Decimal("2001"), base_qty=Decimal("0.11")
    )
    signal = strategy._follower_close_signals_after_master_close(
        event_time_ms=2
    )[0]
    factory = LiveOrderIntentFactory(
        strategy_id=strategy.config.strategy_id,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )
    intent = factory.create(signal, source="account_event", event_time_ms=2)
    real_upsert = plans.upsert_position

    def crash_on_generation_write(candidate):
        if candidate.metadata.get("follower_close_generation") == 1:
            raise RuntimeError("simulated crash before follower close generation")
        real_upsert(candidate)

    monkeypatch.setattr(plans, "upsert_position", crash_on_generation_write)
    first_client = _SuccessfulFollowerCloseClient()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[first_client],
        repository=SqliteOrderJournalStore(journal_path),
        position_plan_store=plans,
        master_follower_policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
    )

    with pytest.raises(RuntimeError, match="simulated crash"):
        await coordinator.execute(intent)
    assert first_client.place_order_calls == 1

    restarted_plans = SqlitePositionPlanStore(plan_path)
    persisted = restarted_plans.get_position(position_id)
    assert persisted.metadata["follower_close_generation"] == 0
    replay_payload = {
        **payload,
        "position": {
            **dict(payload["position"]),
            "metadata": dict(persisted.metadata),
        },
    }
    replay_signal = PortfolioStrategy()._recover_master_closed_with_active_plan(
        snapshots={
            "binance": SimpleNamespace(
                positions=[
                    Position(
                        exchange=ExchangeName.BINANCE,
                        symbol="ETH-USDT-PERP",
                        raw_symbol="ETHUSDT",
                        side=PositionSide.LONG,
                        quantity=Decimal("0.11"),
                        entry_price=Decimal("2001"),
                    )
                ]
            )
        },
        plan_payload=replay_payload,
    )[0]
    replay = factory.create(replay_signal, source="startup_recovery")
    assert replay.intent_id == intent.intent_id
    second_client = _SuccessfulFollowerCloseClient()
    restarted = MultiExchangeOrderCoordinator(
        clients=[second_client],
        repository=SqliteOrderJournalStore(journal_path),
        position_plan_store=restarted_plans,
        master_follower_policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
    )
    with pytest.raises(DuplicateIntentError):
        await restarted.execute(replay)
    assert second_client.place_order_calls == 0
