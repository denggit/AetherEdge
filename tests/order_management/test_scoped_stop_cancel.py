from __future__ import annotations

import pytest

from src.order_management import MultiExchangeOrderCoordinator, OrderIntent, SqliteOrderJournalStore
from src.platform import ExchangeName, Order, OrderStatus
from src.signals import SignalAction, TradeSignal


class _ScopedCancelClient:
    exchange = ExchangeName.OKX
    symbol = "ETH-USDT-PERP"
    market_profile = None

    def __init__(self) -> None:
        self.cancel_stop_requests = []
        self.cancel_all_stop_called = 0

    async def cancel_stop_order(self, request):
        self.cancel_stop_requests.append(request)
        return Order(
            exchange=self.exchange,
            symbol=request.symbol,
            raw_symbol=request.symbol,
            order_id=request.stop_order_id,
            client_order_id=request.client_order_id,
            status=OrderStatus.CANCELED,
        )

    async def cancel_all_stop_orders(self):
        self.cancel_all_stop_called += 1
        return []


class _NoGeneratedClientOrderId:
    def create(self, **kwargs):  # pragma: no cover - failure message is the assertion
        raise AssertionError("scoped stop cancel must not generate a client_order_id")


def test_scoped_cancel_signal_requires_an_explicit_stop_identifier() -> None:
    with pytest.raises(ValueError, match="scoped stop cancel"):
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.CANCEL_STOP_ORDER,
            metadata={
                "strategy_id": "eth_portfolio_v1",
                "sleeve_id": "lf",
                "position_id": "position-1",
            },
        )


def test_scoped_cancel_signal_accepts_client_order_id_without_quantity_or_trigger() -> None:
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        client_order_id="stop-client-1",
    )

    assert signal.quantity is None
    assert signal.trigger_price is None


def test_scoped_cancel_signal_accepts_metadata_stop_order_id() -> None:
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        metadata={"stop_order_id": "stop-order-1"},
    )

    assert signal.metadata["stop_order_id"] == "stop-order-1"


@pytest.mark.asyncio
async def test_coordinator_uses_only_scoped_stop_cancel_and_preserves_audit_metadata(tmp_path) -> None:
    metadata = {
        "strategy_id": "eth_portfolio_v1",
        "sleeve_id": "lf",
        "position_id": "position-1",
        "stop_order_id": "stop-order-1",
        "reason": "replace_verified_stop",
    }
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_STOP_ORDER,
        metadata=metadata,
        created_time_ms=100,
    )
    intent = OrderIntent(
        intent_id="scoped-cancel-1",
        strategy_id="eth_portfolio_v1",
        signal=signal,
        target_exchanges=(ExchangeName.OKX,),
    )
    client = _ScopedCancelClient()
    repository = SqliteOrderJournalStore(tmp_path / "journal.sqlite3")
    coordinator = MultiExchangeOrderCoordinator(
        clients=[client],
        repository=repository,
        client_order_id_factory=_NoGeneratedClientOrderId(),
    )

    results = await coordinator.execute(intent)

    assert len(client.cancel_stop_requests) == 1
    request = client.cancel_stop_requests[0]
    assert request.stop_order_id == "stop-order-1"
    assert request.client_order_id is None
    assert request.metadata is metadata
    assert client.cancel_all_stop_called == 0
    assert results[0].ok is True
    assert results[0].raw["execution_action"] == "cancel_stop_order"
    assert results[0].raw["cancel_stop_metadata"]["sleeve_id"] == "lf"

    saved_intent = repository.get_intent("scoped-cancel-1")
    assert saved_intent is not None
    assert saved_intent.metadata["action"] == "cancel_stop_order"
    assert saved_intent.signal.metadata["strategy_id"] == "eth_portfolio_v1"
    assert saved_intent.signal.metadata["sleeve_id"] == "lf"
    assert saved_intent.signal.metadata["position_id"] == "position-1"


def test_legacy_cancel_all_stop_signal_remains_valid() -> None:
    signal = TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.CANCEL_ALL_STOP_ORDERS,
    )

    assert signal.action is SignalAction.CANCEL_ALL_STOP_ORDERS
