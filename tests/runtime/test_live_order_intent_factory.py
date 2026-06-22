from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform import ExchangeName
from src.runtime.orders import LiveOrderIntentFactory
from src.signals import SignalAction, TradeSignal


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
