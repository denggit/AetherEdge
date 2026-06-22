from __future__ import annotations

from decimal import Decimal

import pytest

from src.platform import ExchangeName
from src.platform.account.events import AccountEvent, AccountEventType
from src.platform.exchanges.models import OrderSide, OrderStatus
from src.signals import SignalAction
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy


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
