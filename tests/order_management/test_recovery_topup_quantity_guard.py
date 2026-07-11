from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from src.order_management import (
    LegPlan,
    LegRole,
    LegSyncStatus,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    OrderIntentStatus,
    PositionPlan,
    PositionPlanStatus,
    SqliteOrderJournalStore,
    SqlitePositionPlanStore,
    MasterFollowerExecutionPolicy,
)
from src.order_management.idempotency import DuplicateIntentError
from src.platform import ExchangeName, InstrumentRule, get_market_profile
from src.runtime.orders import LiveOrderIntentFactory
from src.signals import SignalAction, TradeSignal
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.strategy import Strategy as PortfolioStrategy


class _NoWriteBinanceClient:
    exchange = ExchangeName.BINANCE
    symbol = "ETH-USDT-PERP"

    def __init__(self) -> None:
        self.place_order_calls = 0

    @property
    def market_profile(self):
        return get_market_profile(self.symbol)

    async def fetch_instrument_rule(self):
        return InstrumentRule(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol="ETHUSDT",
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
        )

    async def place_order(self, request):
        self.place_order_calls += 1
        raise AssertionError("non-executable recovery dust reached exchange write")


@pytest.mark.asyncio
async def test_coordinator_skips_non_executable_recovery_topup_without_failure(
    tmp_path,
) -> None:
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plans = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _seed_plan(plans)
    client = _NoWriteBinanceClient()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=journal,
        position_plan_store=plans,
    )
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.00095841427016089856221634149"),
        metadata={
            "target_exchanges": ["binance"],
            "execution_purpose": "follower_recovery_topup",
            "position_id": "p-dust-guard",
            "reference_price": "1800",
            "raw_target_qty": "0.06195841427016089856221634149",
            "confirmed_filled_qty": "0.061",
            "actual_exchange_qty": "0.061",
            "raw_delta": "0.00095841427016089856221634149",
        },
    )
    intent = OrderIntent(
        intent_id="i-dust-guard",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.BINANCE,),
    )

    results = await coordinator.execute(intent)

    assert client.place_order_calls == 0
    assert len(results) == 1
    assert results[0].ok is True
    assert results[0].raw["execution_outcome"] == (
        "skipped_non_executable_quantity"
    )
    assert results[0].raw["normalized_base_quantity"] == "0"
    assert journal.get_intent(intent.intent_id).status is OrderIntentStatus.RECOVERED
    follower = {
        leg.exchange: leg for leg in plans.get_legs("p-dust-guard")
    }[ExchangeName.BINANCE]
    assert follower.sync_status is LegSyncStatus.SYNCED
    assert follower.metadata["execution_outcome"] == (
        "skipped_non_executable_quantity"
    )
    assert follower.metadata["reason"] == "non_executable_rounding_dust"


class _SuccessfulBinanceClient:
    exchange = ExchangeName.BINANCE
    symbol = "ETH-USDT-PERP"

    def __init__(self) -> None:
        self.place_order_calls = 0
        self.last_request = None

    @property
    def market_profile(self):
        return get_market_profile(self.symbol)

    async def fetch_instrument_rule(self):
        return InstrumentRule(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol="ETHUSDT",
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            min_notional=Decimal("5"),
        )

    async def place_order(self, request):
        self.place_order_calls += 1
        self.last_request = request
        from src.platform import Order, OrderStatus, OrderType
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id="order-normalized",
            client_order_id=request.client_order_id,
            status=OrderStatus.FILLED,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            filled_quantity=request.quantity,
            raw={"avgPx": "2000"},
        )


class _ExhaustedBinanceClient(_SuccessfulBinanceClient):
    async def place_order(self, request):
        self.place_order_calls += 1
        raise RuntimeError("simulated exhausted topup failure")


class _NonExecutableOkxClient:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"

    def __init__(self) -> None:
        self.place_order_calls = 0

    @property
    def market_profile(self):
        return get_market_profile(self.symbol)

    async def fetch_instrument_rule(self):
        return InstrumentRule(
            exchange=self.exchange,
            symbol=self.symbol,
            raw_symbol="ETH-USDT-SWAP",
            quantity_step=Decimal("0.01"),
            min_quantity=Decimal("0.01"),
            min_notional=Decimal("100"),
        )

    async def place_order(self, request):
        self.place_order_calls += 1
        raise AssertionError("non-executable OKX recovery topup reached exchange write")


@pytest.mark.asyncio
async def test_executable_recovery_topup_journal_persists_normalized_payload(
    tmp_path,
) -> None:
    """Journal must persist normalized signal and targets after recovery top-up."""
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plans = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _seed_plan_for_normalized_test(plans)
    client = _SuccessfulBinanceClient()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=journal,
        position_plan_store=plans,
    )
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.0034"),
        metadata={
            "target_exchanges": ["binance"],
            "execution_purpose": "follower_recovery_topup",
            "position_id": "p-normalized-test",
            "reference_price": "2000",
        },
    )
    intent = OrderIntent(
        intent_id="i-normalized-test",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.BINANCE,),
    )

    results = await coordinator.execute(intent)

    assert client.place_order_calls == 1
    assert client.last_request.quantity == Decimal("0.003")
    assert len(results) == 1
    assert results[0].ok is True
    assert results[0].quantity == Decimal("0.003")

    saved = journal.get_intent(intent.intent_id)
    assert saved is not None
    assert saved.signal.quantity == Decimal("0.003")
    assert saved.signal.metadata.get("coordinator_quantity_normalized") is True
    assert saved.signal.metadata.get("exchange_quantities_base") == {
        "binance": "0.003",
    }
    assert saved.target_exchanges == (ExchangeName.BINANCE,)
    assert saved.metadata.get("target_exchanges") == ["binance"]


@pytest.mark.asyncio
async def test_multi_exchange_recovery_topup_journal_persists_filtered_targets(
    tmp_path,
) -> None:
    """When OKX is non-executable, journal must only contain Binance targets."""
    journal = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    plans = SqlitePositionPlanStore(tmp_path / "plans.sqlite3")
    _seed_plan_for_multi_exchange_test(plans)
    okx_client = _NonExecutableOkxClient()
    binance_client = _SuccessfulBinanceClient()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[okx_client, binance_client],
        repository=journal,
        position_plan_store=plans,
    )
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("0.0034"),
        metadata={
            "target_exchanges": ["okx", "binance"],
            "execution_purpose": "follower_recovery_topup",
            "position_id": "p-multi-exchange-test",
            "reference_price": "2000",
        },
    )
    intent = OrderIntent(
        intent_id="i-multi-exchange-test",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )

    results = await coordinator.execute(intent)

    assert okx_client.place_order_calls == 0
    assert binance_client.place_order_calls == 1
    assert len(results) == 1
    assert results[0].ok is True
    assert results[0].exchange is ExchangeName.BINANCE

    saved = journal.get_intent(intent.intent_id)
    assert saved is not None
    assert saved.target_exchanges == (ExchangeName.BINANCE,)
    assert saved.metadata.get("target_exchanges") == ["binance"]


@pytest.mark.asyncio
async def test_production_topup_builder_persists_retry_generation_across_restart(
    tmp_path,
) -> None:
    journal_path = tmp_path / "journal-durable-topup.sqlite3"
    plan_path = tmp_path / "plans-durable-topup.sqlite3"
    plans = SqlitePositionPlanStore(plan_path)
    _seed_plan_for_normalized_test(plans)
    leg = {
        item.exchange: item
        for item in plans.get_legs("p-normalized-test")
    }[ExchangeName.BINANCE]
    plans.upsert_leg(replace(leg, metadata={"topup_generation": 0}))

    strategy = PortfolioStrategy()
    plan_payload = plans.serialize_active_positions()[0]["position"]
    signal_0 = strategy._follower_topup_signal(
        exchange="binance",
        side=Side.LONG,
        quantity=Decimal("0.0034"),
        plan=plan_payload,
        quantity_metadata={"reference_price": "2000"},
        topup_generation=leg.metadata.get("topup_generation", 0),
    )
    factory = LiveOrderIntentFactory(
        strategy_id=strategy.config.strategy_id,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )
    intent_0 = factory.create(signal_0, source="recovery")
    client_0 = _SuccessfulBinanceClient()
    coordinator_0 = MultiExchangeOrderCoordinator(
        clients=[client_0],
        repository=SqliteOrderJournalStore(journal_path),
        position_plan_store=plans,
        master_follower_policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
    )

    await coordinator_0.execute(intent_0)

    restarted_plans = SqlitePositionPlanStore(plan_path)
    restarted_leg = {
        item.exchange: item
        for item in restarted_plans.get_legs("p-normalized-test")
    }[ExchangeName.BINANCE]
    assert restarted_leg.metadata["topup_generation"] == 1

    restarted_strategy = PortfolioStrategy()
    signal_1 = restarted_strategy._follower_topup_signal(
        exchange="binance",
        side=Side.LONG,
        quantity=Decimal("0.0034"),
        plan=restarted_plans.serialize_active_positions()[0]["position"],
        quantity_metadata={"reference_price": "2000"},
        topup_generation=restarted_leg.metadata["topup_generation"],
    )
    intent_1 = factory.create(signal_1, source="recovery")
    assert intent_1.intent_id != intent_0.intent_id

    client_1 = _SuccessfulBinanceClient()
    restarted_coordinator = MultiExchangeOrderCoordinator(
        clients=[client_1],
        repository=SqliteOrderJournalStore(journal_path),
        position_plan_store=restarted_plans,
        master_follower_policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
    )
    await restarted_coordinator.execute(intent_1)
    replay = factory.create(signal_1, source="startup_recovery")
    assert replay.intent_id == intent_1.intent_id
    with pytest.raises(DuplicateIntentError):
        await restarted_coordinator.execute(replay)
    assert client_1.place_order_calls == 1


@pytest.mark.asyncio
async def test_crash_before_topup_generation_write_replays_same_claimed_intent(
    tmp_path,
    monkeypatch,
) -> None:
    journal_path = tmp_path / "journal-crash-topup.sqlite3"
    plan_path = tmp_path / "plans-crash-topup.sqlite3"
    plans = SqlitePositionPlanStore(plan_path)
    _seed_plan_for_normalized_test(plans)
    leg = {
        item.exchange: item
        for item in plans.get_legs("p-normalized-test")
    }[ExchangeName.BINANCE]
    plans.upsert_leg(replace(leg, metadata={"topup_generation": 0}))
    strategy = PortfolioStrategy()
    signal = strategy._follower_topup_signal(
        exchange="binance",
        side=Side.LONG,
        quantity=Decimal("0.0034"),
        plan=plans.serialize_active_positions()[0]["position"],
        quantity_metadata={"reference_price": "2000"},
        topup_generation=0,
    )
    factory = LiveOrderIntentFactory(
        strategy_id=strategy.config.strategy_id,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )
    intent = factory.create(signal, source="recovery")
    real_upsert_leg = plans.upsert_leg

    def crash_on_generation_write(candidate):
        if candidate.metadata.get("topup_generation") == 1:
            raise RuntimeError("simulated crash before generation update")
        real_upsert_leg(candidate)

    monkeypatch.setattr(plans, "upsert_leg", crash_on_generation_write)
    first_client = _SuccessfulBinanceClient()
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
    restarted_leg = {
        item.exchange: item
        for item in restarted_plans.get_legs("p-normalized-test")
    }[ExchangeName.BINANCE]
    assert restarted_leg.metadata["topup_generation"] == 0
    replay_signal = PortfolioStrategy()._follower_topup_signal(
        exchange="binance",
        side=Side.LONG,
        quantity=Decimal("0.0034"),
        plan=restarted_plans.serialize_active_positions()[0]["position"],
        quantity_metadata={"reference_price": "2000"},
        topup_generation=restarted_leg.metadata["topup_generation"],
    )
    replay = factory.create(replay_signal, source="startup_recovery")
    assert replay.intent_id == intent.intent_id
    second_client = _SuccessfulBinanceClient()
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


@pytest.mark.asyncio
async def test_exhausted_production_topup_advances_durable_generation(
    tmp_path,
) -> None:
    plans = SqlitePositionPlanStore(tmp_path / "plans-failed-topup.sqlite3")
    _seed_plan_for_normalized_test(plans)
    leg = {
        item.exchange: item
        for item in plans.get_legs("p-normalized-test")
    }[ExchangeName.BINANCE]
    plans.upsert_leg(replace(leg, metadata={"topup_generation": 0}))
    strategy = PortfolioStrategy()
    signal = strategy._follower_topup_signal(
        exchange="binance",
        side=Side.LONG,
        quantity=Decimal("0.0034"),
        plan=plans.serialize_active_positions()[0]["position"],
        quantity_metadata={"reference_price": "2000"},
        topup_generation=0,
    )
    intent = LiveOrderIntentFactory(
        strategy_id=strategy.config.strategy_id,
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    ).create(signal, source="recovery")
    client = _ExhaustedBinanceClient()
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=SqliteOrderJournalStore(
            tmp_path / "journal-failed-topup.sqlite3"
        ),
        position_plan_store=plans,
        master_follower_policy=MasterFollowerExecutionPolicy(
            master_exchange=ExchangeName.OKX,
            follower_exchanges=(ExchangeName.BINANCE,),
        ),
    )

    results = await coordinator.execute(intent)

    assert results and results[0].ok is False
    assert client.place_order_calls == 1
    persisted = SqlitePositionPlanStore(plans.path).get_legs(
        "p-normalized-test"
    )[0]
    assert persisted.metadata["topup_generation"] == 1


def _seed_plan(store: SqlitePositionPlanStore) -> None:
    store.upsert_position(
        PositionPlan(
            position_id="p-dust-guard",
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1738.25"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.07"),
            master_filled_qty_base=Decimal("0.07"),
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id="p-dust-guard",
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.06195841427016089856221634149"),
            filled_qty_base=Decimal("0.061"),
            sync_status=LegSyncStatus.TOPUP_FAILED,
        )
    )


def _seed_plan_for_normalized_test(store: SqlitePositionPlanStore) -> None:
    store.upsert_position(
        PositionPlan(
            position_id="p-normalized-test",
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1738.25"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.07"),
            master_filled_qty_base=Decimal("0.07"),
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id="p-normalized-test",
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.0734"),
            filled_qty_base=Decimal("0.07"),
            sync_status=LegSyncStatus.TOPUP_FAILED,
        )
    )


def _seed_plan_for_multi_exchange_test(store: SqlitePositionPlanStore) -> None:
    store.upsert_position(
        PositionPlan(
            position_id="p-multi-exchange-test",
            strategy_id="eth_portfolio_v1",
            entry_engine="BULL_RECLAIM_V2",
            side="long",
            status=PositionPlanStatus.ACTIVE,
            canonical_stop_price=Decimal("1738.25"),
            master_exchange=ExchangeName.OKX,
            master_target_qty_base=Decimal("0.07"),
            master_filled_qty_base=Decimal("0.07"),
        )
    )
    store.upsert_leg(
        LegPlan(
            position_id="p-multi-exchange-test",
            exchange=ExchangeName.BINANCE,
            role=LegRole.FOLLOWER,
            target_qty_base=Decimal("0.0734"),
            filled_qty_base=Decimal("0.07"),
            sync_status=LegSyncStatus.TOPUP_FAILED,
        )
    )
