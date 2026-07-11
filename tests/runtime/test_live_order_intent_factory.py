from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from src.order_management import (
    DeterministicClientOrderIdFactory,
    DuplicateIntentError,
    MultiExchangeOrderCoordinator,
    OrderIntent,
    SqliteOrderJournalStore,
)
from src.order_management.ports import DuplicateOrderGuard
from src.platform import ExchangeName, InstrumentRule, Order, OrderStatus
from src.platform.markets import get_market_profile
from src.runtime.orders import LiveOrderIntentFactory
from src.signals import SignalAction, TradeSignal


# ── helpers ──────────────────────────────────────────────────────────────────


def _signal(
    action: SignalAction = SignalAction.OPEN_LONG,
    *,
    created_time_ms: int = 100,
    metadata: dict | None = None,
    quantity: Decimal = Decimal("0.1"),
    trigger_price: Decimal | None = None,
    client_order_id: str | None = None,
) -> TradeSignal:
    kwargs: dict = dict(
        symbol="ETH-USDT-PERP",
        action=action,
        quantity=quantity,
        created_time_ms=created_time_ms,
    )
    if metadata is not None:
        kwargs["metadata"] = metadata
    if trigger_price is not None:
        kwargs["trigger_price"] = trigger_price
    if client_order_id is not None:
        kwargs["client_order_id"] = client_order_id
    return TradeSignal(**kwargs)


def _factory() -> LiveOrderIntentFactory:
    return LiveOrderIntentFactory(
        strategy_id="test-strategy",
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )


# ── existing behaviour tests (preserved) ────────────────────────────────────


def test_live_order_intent_factory_uses_runtime_targets_by_default() -> None:
    factory = LiveOrderIntentFactory(strategy_id="s", target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1"))

    intent = factory.create(signal, source="test", event_time_ms=1)

    assert intent.target_exchanges == (ExchangeName.OKX, ExchangeName.BINANCE)
    assert intent.metadata["target_exchanges"] == ["okx", "binance"]


def test_live_order_intent_factory_respects_signal_target_exchanges() -> None:
    factory = LiveOrderIntentFactory(strategy_id="s", target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.PLACE_STOP_LOSS_LONG,
        quantity=Decimal("0.1"),
        trigger_price=Decimal("1600"),
        metadata={"target_exchanges": ["binance"]},
    )

    intent = factory.create(signal, source="request_sync:binance", event_time_ms=2)

    assert intent.target_exchanges == (ExchangeName.BINANCE,)
    assert intent.metadata["target_exchanges"] == ["binance"]


def test_live_order_intent_factory_rejects_unconfigured_signal_target() -> None:
    factory = LiveOrderIntentFactory(strategy_id="s", target_exchanges=(ExchangeName.OKX,))
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1"), metadata={"target_exchanges": ["binance"]})

    with pytest.raises(ValueError, match="not configured"):
        factory.create(signal, source="test", event_time_ms=1)


# ── stable intent identity ──────────────────────────────────────────────────


def test_same_event_different_created_time_produces_same_intent_id() -> None:
    """Two signals with the same logical event but different created_time_ms
    must produce the same intent_id."""
    factory = _factory()
    metadata = {"bar_close_time_ms": 1000}
    signal_a = _signal(created_time_ms=100, metadata=metadata)
    signal_b = _signal(created_time_ms=999, metadata=metadata)

    intent_a = factory.create(signal_a, source="bar_close", event_time_ms=1000)
    intent_b = factory.create(signal_b, source="bar_close", event_time_ms=1000)

    assert intent_a.intent_id == intent_b.intent_id


def test_factory_metadata_operation_sequence_takes_effect() -> None:
    """operation_sequence passed via create(metadata=...) must change identity."""
    factory = _factory()
    signal = _signal(metadata={"position_id": "p1"})

    intent_1 = factory.create(signal, source="recovery", metadata={"operation_sequence": 1})
    intent_2 = factory.create(signal, source="recovery", metadata={"operation_sequence": 2})

    assert intent_1.intent_id != intent_2.intent_id


def test_factory_metadata_retry_generation_takes_effect() -> None:
    """retry_generation passed via create(metadata=...) must change identity."""
    factory = _factory()
    signal = _signal(metadata={"position_id": "p1"})

    intent_0 = factory.create(signal, source="recovery", metadata={"retry_generation": 0})
    intent_1 = factory.create(signal, source="recovery", metadata={"retry_generation": 1})

    assert intent_0.intent_id != intent_1.intent_id


def test_event_identity_not_swallowed_by_position_purpose() -> None:
    """Same position_id + execution_purpose, different event_time_ms → different intent."""
    factory = _factory()
    metadata = {"position_id": "p1", "execution_purpose": "normal_entry"}

    intent_a = factory.create(_signal(metadata=metadata), source="bar_close", event_time_ms=1000)
    intent_b = factory.create(_signal(metadata=metadata), source="bar_close", event_time_ms=2000)

    assert intent_a.intent_id != intent_b.intent_id


def test_metadata_created_time_excluded_from_identity() -> None:
    """metadata['created_time_ms'] must NOT participate in identity."""
    factory = _factory()
    metadata_a = {"position_id": "p1", "retry_generation": 0, "created_time_ms": 100}
    metadata_b = {"position_id": "p1", "retry_generation": 0, "created_time_ms": 999}

    intent_a = factory.create(_signal(metadata=metadata_a), source="recovery")
    intent_b = factory.create(_signal(metadata=metadata_b), source="recovery")

    assert intent_a.intent_id == intent_b.intent_id


def test_no_stable_identity_raises_error() -> None:
    """When there is no event_time_ms and no operation identity → ValueError."""
    factory = _factory()
    signal = _signal(metadata={})

    with pytest.raises(ValueError, match="no stable identity"):
        factory.create(signal, source="bare", event_time_ms=None)


def test_metadata_insertion_order_does_not_change_identity() -> None:
    """Dict insertion order must not affect identity."""
    factory = _factory()
    metadata_a = {"position_id": "p1", "execution_purpose": "stop_sync", "stop_generation": 0}
    metadata_b = {"execution_purpose": "stop_sync", "position_id": "p1", "stop_generation": 0}

    intent_a = factory.create(_signal(metadata=metadata_a), source="recovery")
    intent_b = factory.create(_signal(metadata=metadata_b), source="recovery")

    assert intent_a.intent_id == intent_b.intent_id


def test_non_identity_audit_fields_do_not_change_id() -> None:
    """Audit fields like 'current_price' must not affect identity."""
    factory = _factory()
    base_metadata = {"position_id": "p1", "retry_generation": 0}
    intent_a = factory.create(
        _signal(metadata={**base_metadata, "current_price": "2000"}),
        source="recovery",
    )
    intent_b = factory.create(
        _signal(metadata={**base_metadata, "current_price": "3000"}),
        source="recovery",
    )

    assert intent_a.intent_id == intent_b.intent_id


def test_factory_create_does_not_mutate_signal_metadata() -> None:
    factory = _factory()
    original_metadata = {"position_id": "p1"}
    signal = _signal(metadata=original_metadata)

    factory.create(signal, source="recovery", metadata={"operation_sequence": 1})

    assert signal.metadata == original_metadata


# ── identity collision tests ────────────────────────────────────────────────


def test_different_event_times_produce_different_ids() -> None:
    factory = _factory()
    intent_a = factory.create(_signal(), source="bar_close", event_time_ms=1000)
    intent_b = factory.create(_signal(), source="bar_close", event_time_ms=2000)
    assert intent_a.intent_id != intent_b.intent_id


def test_different_actions_produce_different_ids() -> None:
    factory = _factory()
    intent_open = factory.create(_signal(SignalAction.OPEN_LONG), source="bar", event_time_ms=1000)
    intent_close = factory.create(_signal(SignalAction.CLOSE_LONG), source="bar", event_time_ms=1000)
    assert intent_open.intent_id != intent_close.intent_id


def test_different_positions_produce_different_ids() -> None:
    factory = _factory()
    intent_a = factory.create(_signal(metadata={"position_id": "p1", "retry_generation": 0}), source="recovery")
    intent_b = factory.create(_signal(metadata={"position_id": "p2", "retry_generation": 0}), source="recovery")
    assert intent_a.intent_id != intent_b.intent_id


def test_same_position_different_operation_sequence_different_ids() -> None:
    factory = _factory()
    intent_a = factory.create(
        _signal(metadata={"position_id": "p1", "operation_sequence": 0}),
        source="recovery",
    )
    intent_b = factory.create(
        _signal(metadata={"position_id": "p1", "operation_sequence": 1}),
        source="recovery",
    )
    assert intent_a.intent_id != intent_b.intent_id


def test_retry_generation_zero_vs_one_different_ids() -> None:
    factory = _factory()
    intent_0 = factory.create(
        _signal(metadata={"position_id": "p1", "retry_generation": 0}),
        source="recovery",
    )
    intent_1 = factory.create(
        _signal(metadata={"position_id": "p1", "retry_generation": 1}),
        source="recovery",
    )
    assert intent_0.intent_id != intent_1.intent_id


def test_target_exchange_set_different_produces_different_ids() -> None:
    factory = _factory()
    signal_okx = _signal(metadata={"target_exchanges": ["okx"]})
    signal_both = _signal()

    intent_okx = factory.create(signal_okx, source="bar", event_time_ms=1000)
    intent_both = factory.create(signal_both, source="bar", event_time_ms=1000)

    assert intent_okx.intent_id != intent_both.intent_id


def test_target_exchange_input_order_same_set_same_id() -> None:
    """Same exchange set in different order → same intent_id (set semantics)."""
    factory = LiveOrderIntentFactory(
        strategy_id="s",
        target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE),
    )
    signal_ab = _signal(metadata={"target_exchanges": ["okx", "binance"]})
    signal_ba = _signal(metadata={"target_exchanges": ["binance", "okx"]})

    intent_ab = factory.create(signal_ab, source="bar", event_time_ms=1000)
    intent_ba = factory.create(signal_ba, source="bar", event_time_ms=1000)

    assert intent_ab.intent_id == intent_ba.intent_id


# ── special action identity tests ───────────────────────────────────────────


def test_normal_entry_identity_stable() -> None:
    factory = _factory()
    metadata = {"execution_purpose": "normal_entry", "decision_type": "open", "bar_close_time_ms": 5000}
    intent_a = factory.create(_signal(metadata=metadata, created_time_ms=100), source="bar_close", event_time_ms=5000)
    intent_b = factory.create(_signal(metadata=metadata, created_time_ms=200), source="bar_close", event_time_ms=5000)
    assert intent_a.intent_id == intent_b.intent_id


def test_close_identity_stable() -> None:
    factory = _factory()
    metadata = {"execution_purpose": "normal_close", "position_id": "p1", "bar_close_time_ms": 5000}
    intent_a = factory.create(_signal(SignalAction.CLOSE_LONG, metadata=metadata, created_time_ms=100), source="bar_close", event_time_ms=5000)
    intent_b = factory.create(_signal(SignalAction.CLOSE_LONG, metadata=metadata, created_time_ms=200), source="bar_close", event_time_ms=5000)
    assert intent_a.intent_id == intent_b.intent_id


def test_reduce_identity_stable() -> None:
    factory = _factory()
    metadata = {"position_id": "p1", "decision_type": "reduce", "bar_close_time_ms": 3000}
    intent_a = factory.create(_signal(SignalAction.REDUCE_LONG, metadata=metadata, created_time_ms=100), source="bar_close", event_time_ms=3000)
    intent_b = factory.create(_signal(SignalAction.REDUCE_LONG, metadata=metadata, created_time_ms=200), source="bar_close", event_time_ms=3000)
    assert intent_a.intent_id == intent_b.intent_id


def test_initial_stop_identity_stable() -> None:
    factory = _factory()
    metadata = {"position_id": "p1", "execution_purpose": "stop_sync", "bar_close_time_ms": 4000}
    intent_a = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_LONG, metadata=metadata, created_time_ms=100, quantity=Decimal("0.1"), trigger_price=Decimal("1500")),
        source="bar_close", event_time_ms=4000,
    )
    intent_b = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_LONG, metadata=metadata, created_time_ms=200, quantity=Decimal("0.1"), trigger_price=Decimal("1500")),
        source="bar_close", event_time_ms=4000,
    )
    assert intent_a.intent_id == intent_b.intent_id


def test_stop_replacement_identity_stable() -> None:
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "execution_purpose": "stop_sync",
        "stop_replace_stage": "place_new",
        "bar_close_time_ms": 6000,
    }
    intent_a = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_SHORT, metadata=metadata, created_time_ms=100, quantity=Decimal("1"), trigger_price=Decimal("2000")),
        source="recovery", event_time_ms=6000,
    )
    intent_b = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_SHORT, metadata=metadata, created_time_ms=200, quantity=Decimal("1"), trigger_price=Decimal("2000")),
        source="recovery", event_time_ms=6000,
    )
    assert intent_a.intent_id == intent_b.intent_id


def test_scoped_stop_cancel_identity_stable() -> None:
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "stop_order_id": "stop-old-1",
        "stop_client_order_id": "client-old-1",
    }
    signal_a = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        metadata=metadata,
        created_time_ms=100,
        client_order_id="client-old-1",
    )
    signal_b = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        metadata=metadata,
        created_time_ms=200,
        client_order_id="client-old-1",
    )
    intent_a = factory.create(signal_a, source="stop_replace")
    intent_b = factory.create(signal_b, source="stop_replace")
    assert intent_a.intent_id == intent_b.intent_id


def test_cancel_all_stops_identity_stable() -> None:
    factory = _factory()
    metadata = {"position_id": "p1", "execution_purpose": "stop_sync", "stop_generation": 0}
    signal_a = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_STOP_ORDERS, metadata=metadata, created_time_ms=100)
    signal_b = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_STOP_ORDERS, metadata=metadata, created_time_ms=200)
    intent_a = factory.create(signal_a, source="recovery")
    intent_b = factory.create(signal_b, source="recovery")
    assert intent_a.intent_id == intent_b.intent_id


def test_follower_recovery_topup_identity_stable() -> None:
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "execution_purpose": "follower_recovery_topup",
        "operation_sequence": 0,
    }
    intent_a = factory.create(
        _signal(metadata=metadata, created_time_ms=100, quantity=Decimal("0.01")),
        source="recovery",
    )
    intent_b = factory.create(
        _signal(metadata=metadata, created_time_ms=200, quantity=Decimal("0.02")),
        source="recovery",
    )
    assert intent_a.intent_id == intent_b.intent_id


def test_stop_sync_identity_stable() -> None:
    factory = _factory()
    metadata = {"position_id": "p1", "execution_purpose": "stop_sync", "stop_generation": 0}
    intent_a = factory.create(
        _signal(SignalAction.CANCEL_ALL_STOP_ORDERS, metadata=metadata, created_time_ms=100),
        source="recovery",
    )
    intent_b = factory.create(
        _signal(SignalAction.CANCEL_ALL_STOP_ORDERS, metadata=metadata, created_time_ms=200),
        source="recovery",
    )
    assert intent_a.intent_id == intent_b.intent_id


def test_follower_close_after_master_close_identity_stable() -> None:
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "execution_purpose": "follower_close_after_master_close",
        "follower_close_generation": 0,
        "target_exchanges": ["binance"],
    }
    intent_a = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata, created_time_ms=100),
        source="follower_close_periodic_check",
    )
    intent_b = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata, created_time_ms=200),
        source="follower_close_periodic_check",
    )
    assert intent_a.intent_id == intent_b.intent_id


def test_startup_catchup_identity_stable() -> None:
    """Same candidate bar → same intent_id."""
    factory = _factory()
    metadata = {"startup_catchup": True, "candidate_open_ms": 8000}
    intent_a = factory.create(_signal(metadata=metadata, created_time_ms=100), source="startup_catchup", event_time_ms=8000)
    intent_b = factory.create(_signal(metadata=metadata, created_time_ms=200), source="startup_catchup", event_time_ms=8000)
    assert intent_a.intent_id == intent_b.intent_id


def test_startup_catchup_different_bar_different_id() -> None:
    factory = _factory()
    metadata_a = {"startup_catchup": True, "candidate_open_ms": 8000}
    metadata_b = {"startup_catchup": True, "candidate_open_ms": 9000}
    intent_a = factory.create(_signal(metadata=metadata_a), source="startup_catchup", event_time_ms=8000)
    intent_b = factory.create(_signal(metadata=metadata_b), source="startup_catchup", event_time_ms=9000)
    assert intent_a.intent_id != intent_b.intent_id


# ── client order ID stability ───────────────────────────────────────────────


def test_client_order_id_same_intent_different_created_time() -> None:
    """Same intent + exchange + sequence → same client_order_id regardless of created_time_ms."""
    factory = _factory()
    cid_factory = DeterministicClientOrderIdFactory()
    metadata = {"bar_close_time_ms": 5000}

    signal_a = _signal(metadata=metadata, created_time_ms=100)
    signal_b = _signal(metadata=metadata, created_time_ms=200)

    intent_a = factory.create(signal_a, source="bar_close", event_time_ms=5000)
    intent_b = factory.create(signal_b, source="bar_close", event_time_ms=5000)

    assert intent_a.intent_id == intent_b.intent_id

    cid_a = cid_factory.create(intent_id=intent_a.intent_id, action=SignalAction.OPEN_LONG, exchange=ExchangeName.OKX, sequence=0)
    cid_b = cid_factory.create(intent_id=intent_b.intent_id, action=SignalAction.OPEN_LONG, exchange=ExchangeName.OKX, sequence=0)

    assert cid_a == cid_b


def test_client_order_id_different_exchange() -> None:
    cid = DeterministicClientOrderIdFactory()
    okx = cid.create(intent_id="i1", action=SignalAction.OPEN_LONG, exchange=ExchangeName.OKX, sequence=0)
    bnc = cid.create(intent_id="i1", action=SignalAction.OPEN_LONG, exchange=ExchangeName.BINANCE, sequence=0)
    assert okx != bnc


def test_client_order_id_different_sequence() -> None:
    cid = DeterministicClientOrderIdFactory()
    s0 = cid.create(intent_id="i1", action=SignalAction.OPEN_LONG, exchange=ExchangeName.OKX, sequence=0)
    s1 = cid.create(intent_id="i1", action=SignalAction.OPEN_LONG, exchange=ExchangeName.OKX, sequence=1)
    assert s0 != s1


def test_client_order_id_different_action() -> None:
    cid = DeterministicClientOrderIdFactory()
    ol = cid.create(intent_id="i1", action=SignalAction.OPEN_LONG, exchange=ExchangeName.OKX, sequence=0)
    cl = cid.create(intent_id="i1", action=SignalAction.CLOSE_LONG, exchange=ExchangeName.OKX, sequence=0)
    assert ol != cl


def test_client_order_id_length_and_alnum() -> None:
    cid = DeterministicClientOrderIdFactory()
    result = cid.create(intent_id="test-intent-id", action=SignalAction.OPEN_LONG, exchange=ExchangeName.OKX, sequence=0)
    assert len(result) <= 32
    assert result.isalnum()


# ── real restart replay ────────────────────────────────────────────────────


class _CountingClient:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = None

    def __init__(self) -> None:
        self.place_order_calls = 0
        self.fetch_rule_calls = 0

    async def place_order(self, request):
        self.place_order_calls += 1
        await asyncio.sleep(0)
        return Order(
            exchange=self.exchange, symbol=request.symbol, raw_symbol=request.symbol,
            order_id="order-1", client_order_id=request.client_order_id,
            status=OrderStatus.FILLED, side=request.side, order_type=request.order_type,
            quantity=request.quantity, filled_quantity=request.quantity, raw={"avgPx": "2000"},
        )

    async def place_stop_market_order(self, request):
        raise AssertionError("unexpected stop")

    async def cancel_all_orders(self):
        raise AssertionError("unexpected cancel")

    async def cancel_all_stop_orders(self):
        raise AssertionError("unexpected cancel")

    async def fetch_instrument_rule(self):
        self.fetch_rule_calls += 1
        return InstrumentRule(exchange=self.exchange, symbol=self.symbol, raw_symbol=self.symbol)


class _CountingPlanner:
    def __init__(self) -> None:
        self.calls = 0
        from src.planner import ExecutionPlanner
        self.delegate = ExecutionPlanner()

    def plan(self, signal):
        self.calls += 1
        return self.delegate.plan(signal)


@pytest.mark.asyncio
async def test_restart_replay_through_real_factory_and_coordinator(tmp_path) -> None:
    """Signal A and Signal B (same logical event, different created_time_ms)
    must produce the same intent_id.  The second must be blocked by atomic claim."""
    db_path = tmp_path / "journal.sqlite3"

    # ── First run ──
    factory_a = _factory()
    client_a = _CountingClient()
    planner_a = _CountingPlanner()
    repo_a = SqliteOrderJournalStore(db_path)
    coord_a = MultiExchangeOrderCoordinator(
        clients=[client_a], repository=repo_a, planner=planner_a,
    )
    metadata = {"bar_close_time_ms": 5000}
    signal_a = _signal(metadata=metadata, created_time_ms=100)
    intent_a = factory_a.create(signal_a, source="bar_close", event_time_ms=5000)
    await coord_a.execute(intent_a)

    # ── Restart: new factory, coordinator, repo, client ──
    factory_b = _factory()
    client_b = _CountingClient()
    planner_b = _CountingPlanner()
    repo_b = SqliteOrderJournalStore(db_path)
    coord_b = MultiExchangeOrderCoordinator(
        clients=[client_b], repository=repo_b, planner=planner_b,
    )
    signal_b = _signal(metadata=metadata, created_time_ms=999)
    intent_b = factory_b.create(signal_b, source="bar_close", event_time_ms=5000)

    # Same intent_id produced
    assert intent_a.intent_id == intent_b.intent_id

    # Replay blocked
    with pytest.raises(DuplicateIntentError):
        await coord_b.execute(intent_b)

    # Zero downstream work on second run
    assert planner_b.calls == 0
    assert client_b.place_order_calls == 0
    assert client_b.fetch_rule_calls == 0


# ── cross-source replay ─────────────────────────────────────────────────────


def test_cross_source_closed_kline_vs_startup_catchup_same_id() -> None:
    """Same bar event via closed_kline and startup_catchup must produce same intent_id."""
    factory = _factory()
    metadata = {"bar_close_time_ms": 5000}
    signal_normal = _signal(metadata=metadata, created_time_ms=100)
    signal_catchup = _signal(metadata=metadata, created_time_ms=999)

    intent_normal = factory.create(signal_normal, source="closed_kline", event_time_ms=5000)
    intent_catchup = factory.create(signal_catchup, source="startup_catchup", event_time_ms=5000)

    assert intent_normal.intent_id == intent_catchup.intent_id


@pytest.mark.asyncio
async def test_cross_source_replay_blocked_through_coordinator(tmp_path) -> None:
    """Second execution with different source but same bar must be blocked."""
    db_path = tmp_path / "journal.sqlite3"

    factory = _factory()
    client = _CountingClient()
    planner = _CountingPlanner()
    repo = SqliteOrderJournalStore(db_path)
    coord = MultiExchangeOrderCoordinator(clients=[client], repository=repo, planner=planner)

    metadata = {"bar_close_time_ms": 7000}
    signal_a = _signal(metadata=metadata, created_time_ms=100)
    intent_a = factory.create(signal_a, source="closed_kline", event_time_ms=7000)
    await coord.execute(intent_a)

    # Replay with startup_catchup source
    signal_b = _signal(metadata=metadata, created_time_ms=999)
    intent_b = factory.create(signal_b, source="startup_catchup", event_time_ms=7000)
    assert intent_a.intent_id == intent_b.intent_id

    with pytest.raises(DuplicateIntentError):
        await coord.execute(intent_b)
    assert planner.calls == 1  # Only first execution planned


# ── follower close retry lifecycle ────────────────────────────────────────


def test_follower_close_generation_0_vs_1_different_ids() -> None:
    factory = _factory()
    metadata_0 = {
        "position_id": "p1",
        "execution_purpose": "follower_close_after_master_close",
        "follower_close_generation": 0,
        "target_exchanges": ["binance"],
    }
    metadata_1 = {**metadata_0, "follower_close_generation": 1}
    intent_0 = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata_0),
        source="follower_close_periodic_check",
    )
    intent_1 = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata_1),
        source="follower_close_periodic_check",
    )
    assert intent_0.intent_id != intent_1.intent_id


def test_follower_close_same_generation_replay_same_id() -> None:
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "execution_purpose": "follower_close_after_master_close",
        "follower_close_generation": 0,
        "target_exchanges": ["binance"],
    }
    intent_a = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata, created_time_ms=100),
        source="follower_close_periodic_check",
    )
    intent_b = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata, created_time_ms=999),
        source="recovery",
    )
    assert intent_a.intent_id == intent_b.intent_id


def test_recovery_and_periodic_same_generation_same_id() -> None:
    """source=recovery and source=follower_close_periodic_check with same gen must match."""
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "execution_purpose": "follower_close_after_master_close",
        "follower_close_generation": 2,
        "target_exchanges": ["binance"],
    }
    intent_recovery = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata),
        source="recovery",
    )
    intent_periodic = factory.create(
        _signal(SignalAction.CLOSE_LONG, metadata=metadata),
        source="follower_close_periodic_check",
    )
    assert intent_recovery.intent_id == intent_periodic.intent_id


# ── production top-up builder ──────────────────────────────────────────────


def test_production_topup_generation_takes_effect() -> None:
    factory = _factory()
    base = {
        "position_id": "p1",
        "execution_purpose": "follower_recovery_topup",
    }
    intent_0 = factory.create(
        _signal(metadata={**base, "topup_generation": 0}),
        source="recovery",
    )
    intent_1 = factory.create(
        _signal(metadata={**base, "topup_generation": 1}),
        source="recovery",
    )
    assert intent_0.intent_id != intent_1.intent_id


def test_production_topup_same_generation_same_id() -> None:
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "execution_purpose": "follower_recovery_topup",
        "topup_generation": 0,
    }
    intent_a = factory.create(
        _signal(metadata=metadata, created_time_ms=100, quantity=Decimal("0.01")),
        source="recovery",
    )
    intent_b = factory.create(
        _signal(metadata=metadata, created_time_ms=200, quantity=Decimal("0.02")),
        source="recovery",
    )
    assert intent_a.intent_id == intent_b.intent_id


# ── production stop repair builder ───────────────────────────────────────


def test_production_stop_generation_takes_effect() -> None:
    factory = _factory()
    base = {
        "position_id": "p1",
        "execution_purpose": "follower_stop_repair",
    }
    intent_0 = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_LONG, metadata={**base, "stop_generation": 0}, trigger_price=Decimal("1500")),
        source="recovery",
    )
    intent_1 = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_LONG, metadata={**base, "stop_generation": 1}, trigger_price=Decimal("1500")),
        source="recovery",
    )
    assert intent_0.intent_id != intent_1.intent_id


def test_production_stop_same_generation_same_id() -> None:
    factory = _factory()
    metadata = {
        "position_id": "p1",
        "execution_purpose": "follower_stop_repair",
        "stop_generation": 0,
    }
    intent_a = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_LONG, metadata=metadata, created_time_ms=100, trigger_price=Decimal("1500")),
        source="recovery",
    )
    intent_b = factory.create(
        _signal(SignalAction.PLACE_STOP_LOSS_LONG, metadata=metadata, created_time_ms=200, trigger_price=Decimal("1500")),
        source="recovery",
    )
    assert intent_a.intent_id == intent_b.intent_id


# ── fail-closed ────────────────────────────────────────────────────────────


def test_fail_closed_empty_execution_purpose() -> None:
    factory = _factory()
    with pytest.raises(ValueError):
        factory.create(
            _signal(metadata={"execution_purpose": "", "position_id": "p1", "retry_generation": 0}),
            source="recovery",
        )


def test_fail_closed_whitespace_execution_purpose() -> None:
    factory = _factory()
    with pytest.raises(ValueError):
        factory.create(
            _signal(metadata={"execution_purpose": "   ", "position_id": "p1", "retry_generation": 0}),
            source="recovery",
        )


def test_fail_closed_empty_position_id() -> None:
    factory = _factory()
    with pytest.raises(ValueError, match="empty position_id"):
        factory.create(
            _signal(metadata={"position_id": "", "execution_purpose": "stop_sync", "stop_generation": 0}),
            source="recovery",
        )


def test_fail_closed_purpose_only_no_position_or_generation() -> None:
    """execution_purpose alone without position_id/generation must fail."""
    factory = _factory()
    with pytest.raises(ValueError):
        factory.create(
            _signal(metadata={"execution_purpose": "stop_sync"}),
            source="recovery",
        )


def test_fail_closed_unserializable_object() -> None:
    """Non-serializable metadata values must fail closed."""
    from src.runtime.orders import _canonical_value

    class Unserializable:
        pass

    with pytest.raises(TypeError, match="unsupported identity value type"):
        _canonical_value(Unserializable())


# ── canonical serializer ─────────────────────────────────────────────────


def test_canonical_serializer_mapping_order_independent() -> None:
    from src.runtime.orders import _canonical_value

    a = {"b": 1, "a": 2}
    b = {"a": 2, "b": 1}
    assert _canonical_value(a) == _canonical_value(b)


def test_canonical_serializer_set_order_independent() -> None:
    from src.runtime.orders import _canonical_value

    assert _canonical_value({"c", "a", "b"}) == _canonical_value({"a", "b", "c"})


def test_canonical_serializer_enum() -> None:
    from src.runtime.orders import _canonical_value

    assert _canonical_value(SignalAction.OPEN_LONG) == "open_long"


def test_canonical_serializer_decimal() -> None:
    from src.runtime.orders import _canonical_value

    assert _canonical_value(Decimal("1.10")) == _canonical_value(Decimal("1.1"))


def test_canonical_serializer_list_and_tuple() -> None:
    from src.runtime.orders import _canonical_value

    assert _canonical_value([1, 2, 3]) == _canonical_value((1, 2, 3))


def test_canonical_serializer_bool() -> None:
    from src.runtime.orders import _canonical_value

    assert _canonical_value(True) == "1"
    assert _canonical_value(False) == "0"


def test_canonical_serializer_none() -> None:
    from src.runtime.orders import _canonical_value

    assert _canonical_value(None) is None


def test_canonical_serializer_string_strip() -> None:
    from src.runtime.orders import _canonical_value

    assert _canonical_value("  hello  ") == "hello"
