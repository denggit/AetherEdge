from __future__ import annotations

from decimal import Decimal

import pytest

from src.order_management import DeterministicClientOrderIdFactory, DuplicateIntentError, OrderIntent, RepositoryDuplicateOrderGuard, SqliteOrderJournalStore
from src.platform import ExchangeName
from src.signals import SignalAction, TradeSignal


def _signal() -> TradeSignal:
    return TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.OPEN_LONG, quantity=Decimal("0.1"), created_time_ms=1234567890)


def test_client_order_id_is_deterministic_compact_and_exchange_specific():
    factory = DeterministicClientOrderIdFactory(prefix="AE")
    okx_id = factory.create(strategy_id="v8", signal=_signal(), exchange=ExchangeName.OKX, sequence=0)
    okx_id_again = factory.create(strategy_id="v8", signal=_signal(), exchange=ExchangeName.OKX, sequence=0)
    binance_id = factory.create(strategy_id="v8", signal=_signal(), exchange=ExchangeName.BINANCE, sequence=0)

    assert okx_id == okx_id_again
    assert okx_id != binance_id
    assert len(okx_id) <= 32
    assert okx_id.isalnum()


def test_repository_duplicate_guard_rejects_existing_intent(tmp_path):
    repo = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    intent = OrderIntent(intent_id="intent-1", strategy_id="v8", signal=_signal(), target_exchanges=(ExchangeName.OKX,))
    repo.save_intent(intent)

    with pytest.raises(DuplicateIntentError):
        RepositoryDuplicateOrderGuard(repo).assert_not_duplicate(intent)
