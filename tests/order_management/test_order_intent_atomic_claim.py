from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

from src.order_management import OrderIntent, SqliteOrderJournalStore
from src.platform import ExchangeName
from src.signals import SignalAction, TradeSignal


def _intent(label: str, *, intent_id: str = "shared-intent") -> OrderIntent:
    is_first = label == "first"
    return OrderIntent(
        intent_id=intent_id,
        strategy_id=f"strategy-{label}",
        signal=TradeSignal(
            symbol="ETH-USDT-PERP",
            action=(
                SignalAction.OPEN_LONG
                if is_first
                else SignalAction.CLOSE_SHORT
            ),
            quantity=Decimal("0.1") if is_first else Decimal("9.9"),
            metadata={"owner": label},
            created_time_ms=100 if is_first else 200,
        ),
        target_exchanges=(
            (ExchangeName.OKX,) if is_first else (ExchangeName.BINANCE,)
        ),
        metadata={"claim_owner": label},
        created_time_ms=100 if is_first else 200,
    )


def test_duplicate_claim_does_not_overwrite_original_payload_or_add_records(
    tmp_path,
) -> None:
    db_path = tmp_path / "journal.sqlite3"
    repository = SqliteOrderJournalStore(db_path)
    original = _intent("first")
    conflicting = _intent("second")

    assert repository.claim_intent(original) is True
    assert repository.claim_intent(conflicting) is False

    loaded = repository.get_intent(original.intent_id)
    assert loaded is not None
    assert loaded.strategy_id == original.strategy_id
    assert loaded.signal.action is original.signal.action
    assert loaded.signal.quantity == original.signal.quantity
    assert loaded.signal.metadata == original.signal.metadata
    assert loaded.target_exchanges == original.target_exchanges
    assert loaded.metadata == original.metadata
    assert loaded.status is original.status
    assert repository.list_results(intent_id=original.intent_id) == []
    assert _count_rows(db_path, "order_intents") == 1
    assert _count_events(db_path, "intent_claimed") == 1


def test_two_repository_instances_atomically_compete_for_same_intent(
    tmp_path,
) -> None:
    db_path = tmp_path / "journal.sqlite3"
    repositories = (
        SqliteOrderJournalStore(db_path),
        SqliteOrderJournalStore(db_path),
    )
    intents = (_intent("first"), _intent("second"))
    barrier = threading.Barrier(2)

    def claim(index: int) -> tuple[int, bool]:
        barrier.wait(timeout=5)
        return index, repositories[index].claim_intent(intents[index])

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(claim, (0, 1)))

    winners = [index for index, claimed in outcomes if claimed]
    losers = [index for index, claimed in outcomes if not claimed]
    assert len(winners) == 1
    assert len(losers) == 1
    winner = intents[winners[0]]
    loaded = SqliteOrderJournalStore(db_path).get_intent(winner.intent_id)
    assert loaded is not None
    assert loaded.strategy_id == winner.strategy_id
    assert loaded.signal.action is winner.signal.action
    assert loaded.signal.quantity == winner.signal.quantity
    assert loaded.signal.metadata == winner.signal.metadata
    assert loaded.target_exchanges == winner.target_exchanges
    assert loaded.metadata == winner.metadata
    assert _count_rows(db_path, "order_intents") == 1
    assert _count_events(db_path, "intent_claimed") == 1


def _count_rows(path, table: str) -> int:
    with sqlite3.connect(path) as connection:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _count_events(path, message: str) -> int:
    with sqlite3.connect(path) as connection:
        return int(
            connection.execute(
                "SELECT COUNT(*) FROM order_journal_events WHERE message = ?",
                (message,),
            ).fetchone()[0]
        )
