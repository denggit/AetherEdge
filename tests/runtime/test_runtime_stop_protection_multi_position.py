from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from src.app import AppConfig
from src.platform import (
    Balance,
    ExchangeName,
    LeverageInfo,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    PositionMode,
    PositionSide,
)
from src.platform.snapshot import PlatformSnapshot
from src.runtime.recovery.models import RecoveryReport
from src.runtime.runner import (
    LiveRuntimeError,
    LiveRuntimeRunner,
    _exchange_positions_matching_strategy_position,
)
from src.signals import SignalAction, TradeSignal
from src.strategy.positions import (
    StrategyPositionSide,
    StrategyPositionSnapshot,
    StrategyPositionStatus,
)


SYMBOL = "ETH-USDT-PERP"


class MultiPositionStrategy:
    recovery_blocking_manual_required = False

    def __init__(
        self,
        snapshots: tuple[StrategyPositionSnapshot, ...],
    ) -> None:
        self._snapshots = snapshots

    def position_snapshots(self) -> tuple[StrategyPositionSnapshot, ...]:
        return self._snapshots


class FakeAccount:
    exchange = ExchangeName.OKX

    def __init__(self, positions: tuple[Position, ...]) -> None:
        self.positions = positions

    async def fetch_positions(self):
        return self.positions

    async def fetch_position_mode(self):
        return PositionMode.ONE_WAY


class FakeExecution:
    exchange = ExchangeName.OKX

    def __init__(self, stops: tuple[Order, ...]) -> None:
        self.stops = stops

    async def fetch_open_stop_orders(self):
        return self.stops


def _strategy_position(
    position_id: str,
    side: StrategyPositionSide,
    *,
    stop_price: Decimal | None = Decimal("1719.40"),
) -> StrategyPositionSnapshot:
    return StrategyPositionSnapshot(
        strategy_id="test-strategy",
        position_id=position_id,
        symbol=SYMBOL,
        side=side,
        status=StrategyPositionStatus.ACTIVE,
        base_quantity=Decimal("0.282"),
        stop_price=stop_price,
    )


def _exchange_position(
    side: PositionSide,
    quantity: str,
) -> Position:
    return Position(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        side=side,
        quantity=Decimal(quantity),
        entry_price=Decimal("2000"),
    )


def _stop(
    position_id: str,
    *,
    side: OrderSide,
) -> Order:
    return Order(
        exchange=ExchangeName.OKX,
        symbol=SYMBOL,
        raw_symbol="ETH-USDT-SWAP",
        order_id=f"{position_id}-order",
        client_order_id=f"{position_id}-stop",
        status=OrderStatus.NEW,
        side=side,
        order_type=OrderType.MARKET,
        price=Decimal("1719.40"),
        quantity=Decimal("2.82"),
        raw={"reduceOnly": "true"},
    )


def _signal(
    position_id: str | None,
    side: StrategyPositionSide,
) -> TradeSignal:
    metadata: dict[str, object] = {"target_exchanges": ["okx"]}
    if position_id is not None:
        metadata["position_id"] = position_id
    return TradeSignal(
        symbol=SYMBOL,
        action=(
            SignalAction.PLACE_STOP_LOSS_LONG
            if side is StrategyPositionSide.LONG
            else SignalAction.PLACE_STOP_LOSS_SHORT
        ),
        quantity=Decimal("0.282"),
        trigger_price=Decimal("1719.40"),
        metadata=metadata,
    )


def _platform_snapshot(positions: tuple[Position, ...]) -> PlatformSnapshot:
    return PlatformSnapshot(
        symbol=SYMBOL,
        balance=Balance(
            exchange=ExchangeName.OKX,
            asset="USDT",
            total=Decimal("10000"),
            available=Decimal("10000"),
        ),
        positions=positions,
        open_orders=(),
        open_stop_orders=(),
        leverage=LeverageInfo(
            exchange=ExchangeName.OKX,
            symbol=SYMBOL,
            raw_symbol="ETH-USDT-SWAP",
            leverage=Decimal("3"),
        ),
        position_mode=PositionMode.ONE_WAY,
    )


def _runner(
    strategy_positions: tuple[StrategyPositionSnapshot, ...],
    *,
    exchange_positions: tuple[Position, ...] = (),
    stops: tuple[Order, ...] = (),
) -> LiveRuntimeRunner:
    runner = LiveRuntimeRunner.__new__(LiveRuntimeRunner)
    runner.app_config = AppConfig(
        symbol=SYMBOL,
        exchanges=(ExchangeName.OKX,),
        data_exchange=ExchangeName.OKX,
        strategy="test:Strategy",
        data_streams=(),
        state_db_path=":memory:",
        market_queue_maxsize=10,
        signal_queue_maxsize=10,
        alert_queue_maxsize=10,
        dry_run=True,
        enable_email_alerts=False,
    )
    runner.context = SimpleNamespace(
        strategy=MultiPositionStrategy(strategy_positions),
    )
    runner._account_clients = (FakeAccount(exchange_positions),)
    runner._execution_clients = (FakeExecution(stops),)
    return runner


def test_recovery_postcondition_validates_multiple_scoped_signals() -> None:
    long_position = _strategy_position("long-sleeve", StrategyPositionSide.LONG)
    short_position = _strategy_position("short-sleeve", StrategyPositionSide.SHORT)
    exchange_positions = (
        _exchange_position(PositionSide.LONG, "2.82"),
        _exchange_position(PositionSide.SHORT, "-2.82"),
    )
    report = RecoveryReport(
        ok=True,
        snapshots=(_platform_snapshot(exchange_positions),),
        strategy_signals=(
            _signal("long-sleeve", StrategyPositionSide.LONG),
            _signal("short-sleeve", StrategyPositionSide.SHORT),
        ),
    )

    _runner((long_position, short_position))._validate_recovery_protection_postcondition(
        report
    )


def test_unscoped_recovery_signal_does_not_cover_multiple_positions() -> None:
    long_position = _strategy_position("long-sleeve", StrategyPositionSide.LONG)
    short_position = _strategy_position("short-sleeve", StrategyPositionSide.SHORT)
    report = RecoveryReport(
        ok=True,
        snapshots=(
            _platform_snapshot(
                (
                    _exchange_position(PositionSide.LONG, "2.82"),
                    _exchange_position(PositionSide.SHORT, "-2.82"),
                )
            ),
        ),
        strategy_signals=(_signal(None, StrategyPositionSide.LONG),),
    )

    with pytest.raises(LiveRuntimeError, match="recovery protection postcondition failed"):
        _runner(
            (long_position, short_position)
        )._validate_recovery_protection_postcondition(report)


@pytest.mark.asyncio
async def test_post_execution_validation_handles_multiple_strategy_positions() -> None:
    long_position = _strategy_position("long-sleeve", StrategyPositionSide.LONG)
    short_position = _strategy_position("short-sleeve", StrategyPositionSide.SHORT)
    runner = _runner(
        (long_position, short_position),
        exchange_positions=(
            _exchange_position(PositionSide.LONG, "2.82"),
            _exchange_position(PositionSide.SHORT, "-2.82"),
        ),
        stops=(
            _stop("long-sleeve", side=OrderSide.SELL),
            _stop("short-sleeve", side=OrderSide.BUY),
        ),
    )

    await runner._validate_post_execution_stop_protection()


def test_exchange_long_and_short_positions_match_requested_side() -> None:
    long_position = _strategy_position("long-sleeve", StrategyPositionSide.LONG)
    exchange_positions = (
        _exchange_position(PositionSide.LONG, "2.82"),
        _exchange_position(PositionSide.SHORT, "-2.82"),
    )

    assert _exchange_positions_matching_strategy_position(
        exchange_positions,
        long_position,
    ) == (exchange_positions[0],)


def test_okx_style_positive_short_quantity_does_not_match_long_scope() -> None:
    long_position = _strategy_position("long-sleeve", StrategyPositionSide.LONG)
    exchange_positions = (
        _exchange_position(PositionSide.LONG, "2.82"),
        _exchange_position(PositionSide.SHORT, "2.82"),
    )

    assert _exchange_positions_matching_strategy_position(
        exchange_positions,
        long_position,
    ) == (exchange_positions[0],)


def test_okx_style_positive_short_quantity_matches_short_scope() -> None:
    short_position = _strategy_position(
        "short-sleeve",
        StrategyPositionSide.SHORT,
    )
    exchange_positions = (
        _exchange_position(PositionSide.LONG, "2.82"),
        _exchange_position(PositionSide.SHORT, "2.82"),
    )

    assert _exchange_positions_matching_strategy_position(
        exchange_positions,
        short_position,
    ) == (exchange_positions[1],)


def test_explicit_short_side_takes_priority_over_positive_quantity() -> None:
    long_position = _strategy_position("long-sleeve", StrategyPositionSide.LONG)
    explicit_short = _exchange_position(PositionSide.SHORT, "1.0")

    assert _exchange_positions_matching_strategy_position(
        (explicit_short,),
        long_position,
    ) == ()


def test_both_exchange_side_falls_back_to_quantity_sign() -> None:
    long_position = _strategy_position("long-sleeve", StrategyPositionSide.LONG)
    short_position = _strategy_position(
        "short-sleeve",
        StrategyPositionSide.SHORT,
    )
    positive_both = _exchange_position(PositionSide.BOTH, "1.0")
    negative_both = _exchange_position(PositionSide.BOTH, "-1.0")
    exchange_positions = (positive_both, negative_both)

    assert _exchange_positions_matching_strategy_position(
        exchange_positions,
        long_position,
    ) == (positive_both,)
    assert _exchange_positions_matching_strategy_position(
        exchange_positions,
        short_position,
    ) == (negative_both,)


def test_ambiguous_same_side_exchange_positions_fail_closed() -> None:
    strategy_position = _strategy_position(
        "long-sleeve",
        StrategyPositionSide.LONG,
    )
    report = RecoveryReport(
        ok=True,
        snapshots=(
            _platform_snapshot(
                (
                    _exchange_position(PositionSide.LONG, "1.00"),
                    _exchange_position(PositionSide.LONG, "1.82"),
                )
            ),
        ),
        strategy_signals=(_signal("long-sleeve", StrategyPositionSide.LONG),),
    )

    with pytest.raises(LiveRuntimeError, match="ambiguous_count=2") as exc_info:
        _runner((strategy_position,))._validate_recovery_protection_postcondition(
            report
        )

    message = str(exc_info.value)
    assert "strategy_position_id=long-sleeve" in message
    assert "side=long" in message
    assert "exchange=okx" in message
