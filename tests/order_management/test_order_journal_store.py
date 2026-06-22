from __future__ import annotations

from decimal import Decimal

from src.order_management import ExchangeOrderResult, OrderIntent, OrderIntentStatus, SqliteOrderJournalStore
from src.platform import ExchangeName, OrderSide, OrderStatus
from src.signals import SignalAction, TradeSignal


def test_sqlite_order_journal_store_roundtrips_intent_status_and_results(tmp_path):
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_SHORT, quantity=Decimal("0.2"), reason="test", created_time_ms=100)
    intent = OrderIntent(intent_id="intent-1", strategy_id="v8", signal=signal, target_exchanges=(ExchangeName.OKX, ExchangeName.BINANCE))

    repo.save_intent(intent)
    repo.update_status(intent_id="intent-1", status=OrderIntentStatus.SUBMITTED)
    repo.save_result(
        intent_id="intent-1",
        result=ExchangeOrderResult(
            exchange=ExchangeName.OKX,
            ok=True,
            order_id="ord-1",
            client_order_id="cid-1",
            status=OrderStatus.NEW,
            side=OrderSide.SELL,
            quantity=Decimal("0.2"),
        ),
    )

    loaded = repo.get_intent("intent-1")
    results = repo.list_results(intent_id="intent-1")

    assert loaded is not None
    assert loaded.status is OrderIntentStatus.SUBMITTED
    assert loaded.signal.action is SignalAction.OPEN_SHORT
    assert loaded.target_exchanges == (ExchangeName.OKX, ExchangeName.BINANCE)
    assert len(results) == 1
    assert results[0].order_id == "ord-1"
    assert results[0].side is OrderSide.SELL


def test_order_journal_has_intent_with_position_id_from_signal_metadata(tmp_path):
    """has_intent_with_position_id returns True when signal metadata contains
    the position_id and False for a missing position."""
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("1"),
        metadata={"position_id": "v9c-test-position"},
        created_time_ms=100,
    )
    intent = OrderIntent(
        intent_id="intent-sig-1",
        strategy_id="v8",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
        status=OrderIntentStatus.SUBMITTED,
    )
    repo.save_intent(intent)

    assert repo.has_intent_with_position_id("v9c-test-position") is True
    assert repo.has_intent_with_position_id("missing-position") is False


def test_order_journal_has_intent_with_position_id_ignores_inactive_status(tmp_path):
    """CANCELED intents must NOT match has_intent_with_position_id."""
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("1"),
        metadata={"position_id": "v9c-canceled-position"},
        created_time_ms=100,
    )
    intent = OrderIntent(
        intent_id="intent-canceled-1",
        strategy_id="v8",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
        status=OrderIntentStatus.CANCELED,
    )
    repo.save_intent(intent)

    assert repo.has_intent_with_position_id("v9c-canceled-position") is False


def test_order_journal_has_intent_with_position_id_from_intent_metadata(tmp_path):
    """has_intent_with_position_id reads position_id from intent.metadata when
    signal metadata does not contain it."""
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.OPEN_LONG,
        quantity=Decimal("1"),
        metadata={},  # no position_id here
        created_time_ms=100,
    )
    intent = OrderIntent(
        intent_id="intent-meta-1",
        strategy_id="v8",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
        status=OrderIntentStatus.SUBMITTED,
        metadata={"position_id": "v9c-intent-meta-position"},
    )
    repo.save_intent(intent)

    assert repo.has_intent_with_position_id("v9c-intent-meta-position") is True
