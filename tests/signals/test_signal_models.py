from decimal import Decimal

import pytest

from src.signals import SignalAction, SignalOrderType, TradeSignal


def test_trade_signal_validates_limit_price():
    with pytest.raises(ValueError, match="price"):
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.OPEN_LONG,
            quantity=Decimal("0.1"),
            order_type=SignalOrderType.LIMIT,
        )


def test_trade_signal_allows_cancel_without_quantity():
    signal = TradeSignal(symbol="ETH-USDT-PERP", action=SignalAction.CANCEL_ALL_ORDERS)
    assert signal.quantity is None


def test_trade_signal_requires_stop_trigger():
    with pytest.raises(ValueError, match="trigger_price"):
        TradeSignal(
            symbol="ETH-USDT-PERP",
            action=SignalAction.PLACE_STOP_LOSS_LONG,
            quantity=Decimal("0.1"),
        )
