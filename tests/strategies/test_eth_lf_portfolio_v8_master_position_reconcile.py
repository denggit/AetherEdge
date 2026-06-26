from __future__ import annotations

import asyncio
from decimal import Decimal

from src.order_management.models import ExchangeOrderResult
from src.platform import ExchangeName
from src.platform.exchanges.models import OrderStatus
from src.signals import SignalAction, TradeSignal
from strategies.eth_lf_portfolio_v8.domain.models import Side
from strategies.eth_lf_portfolio_v8.strategy import Strategy


INITIAL_STOP = Decimal("1686.42")


def test_master_exchange_position_avgpx_reconciles_strategy_position_after_stop_confirmed() -> None:
    strategy = _short_strategy()

    asyncio.run(
        strategy.on_order_results(
            signal=_stop_signal("okx"),
            results=[
                _stop_result(
                    ExchangeName.OKX,
                    entry_price=Decimal("1620.50"),
                    base_quantity=Decimal("0.255"),
                    native_quantity=Decimal("2.55"),
                    side="short",
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.position.avg_entry == Decimal("1620.50")
    assert strategy.position.qty == Decimal("0.255")
    assert strategy.position.risk_per_coin == abs(Decimal("1620.50") - INITIAL_STOP)
    assert strategy.position.confirmed_stop_price == INITIAL_STOP
    assert strategy.position.pending_stop_replace is False


def test_follower_exchange_position_does_not_override_master_canonical_position() -> None:
    strategy = _short_strategy()

    asyncio.run(
        strategy.on_order_results(
            signal=_stop_signal("binance"),
            results=[
                _stop_result(
                    ExchangeName.BINANCE,
                    entry_price=Decimal("9999"),
                    base_quantity=Decimal("9"),
                    native_quantity=Decimal("9"),
                    side="short",
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.position.avg_entry == Decimal("1620.30")
    assert strategy.position.qty == Decimal("0.255")


def test_master_position_side_mismatch_enters_manual_required() -> None:
    strategy = _short_strategy()

    asyncio.run(
        strategy.on_order_results(
            signal=_stop_signal("okx"),
            results=[
                _stop_result(
                    ExchangeName.OKX,
                    entry_price=Decimal("1620.50"),
                    base_quantity=Decimal("0.255"),
                    native_quantity=Decimal("2.55"),
                    side="long",
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.recovery_manual_required is True
    assert strategy.recovery_blocking_manual_required is True
    assert "master_position_side_mismatch_manual_required" in strategy.recovery_alerts


def test_master_avg_entry_large_diff_alerts_but_uses_exchange_truth() -> None:
    strategy = _short_strategy()

    asyncio.run(
        strategy.on_order_results(
            signal=_stop_signal("okx"),
            results=[
                _stop_result(
                    ExchangeName.OKX,
                    entry_price=Decimal("1640.00"),
                    base_quantity=Decimal("0.255"),
                    native_quantity=Decimal("2.55"),
                    side="short",
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.position.avg_entry == Decimal("1640.00")
    assert "master_avg_entry_large_diff_manual_required" in strategy.recovery_alerts


def test_master_qty_large_diff_alerts_but_uses_exchange_truth() -> None:
    strategy = _short_strategy()

    asyncio.run(
        strategy.on_order_results(
            signal=_stop_signal("okx"),
            results=[
                _stop_result(
                    ExchangeName.OKX,
                    entry_price=Decimal("1620.50"),
                    base_quantity=Decimal("0.300"),
                    native_quantity=Decimal("3"),
                    side="short",
                )
            ],
            source="test",
            event_time_ms=5,
        )
    )

    assert strategy.position.qty == Decimal("0.300")
    assert "master_qty_large_diff_manual_required" in strategy.recovery_alerts


def _short_strategy() -> Strategy:
    strategy = Strategy()
    strategy.started = True
    strategy.equity = Decimal("1000")
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=0,
        avg_entry=Decimal("1620.30"),
        qty=Decimal("0.255"),
        stop_price=INITIAL_STOP,
        entry_engine="MOMENTUM_V3",
        position_id="master-position-reconcile",
        stop_confirmed=False,
    )
    strategy.position.first_entry = Decimal("1620.30")
    strategy.position.mark_leg_open(exchange="okx", avg_fill_price=Decimal("1620.30"), base_qty=Decimal("0.255"))
    return strategy


def _stop_signal(exchange: str) -> TradeSignal:
    return TradeSignal(
        symbol="ETH-USDT-PERP",
        action=SignalAction.PLACE_STOP_LOSS_SHORT,
        quantity=Decimal("0.255"),
        trigger_price=INITIAL_STOP,
        metadata={"target_exchanges": [exchange]},
    )


def _stop_result(
    exchange: ExchangeName,
    *,
    entry_price: Decimal,
    base_quantity: Decimal,
    native_quantity: Decimal,
    side: str,
) -> ExchangeOrderResult:
    return ExchangeOrderResult(
        exchange=exchange,
        ok=True,
        order_id=f"{exchange.value}-stop-1",
        status=OrderStatus.NEW,
        raw={
            "exchange_position_entry_price": entry_price,
            "exchange_position_base_quantity": base_quantity,
            "exchange_position_native_quantity": native_quantity,
            "exchange_position_side": side,
            "exchange_position_source": "stop_post_check",
        },
    )
