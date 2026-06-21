from __future__ import annotations

from decimal import Decimal

from src.market_data.derived import RangeBarBuilder
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


def _trade(price: str, time_ms: int, *, side: TradeSide = TradeSide.BUY, quantity: str = "10") -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal(quantity),
        side=side,
        trade_id=str(time_ms),
        event_time_ms=time_ms,
        trade_time_ms=time_ms,
    )


def test_range_bar_builder_closes_after_price_moves_from_open_and_includes_closing_trade():
    builder = RangeBarBuilder(range_pct=Decimal("0.002"), contract_value=Decimal("0.01"))

    assert builder.on_trade(_trade("1000", 1_700_000_000_000)) == ()
    assert builder.on_trade(_trade("1001", 1_700_000_001_000, side=TradeSide.SELL)) == ()
    closed = builder.on_trade(_trade("1002", 1_700_000_002_000))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.open == Decimal("1000")
    assert bar.high == Decimal("1002")
    assert bar.low == Decimal("1000")
    assert bar.close == Decimal("1002")
    assert bar.volume == Decimal("30")
    assert bar.buy_notional == Decimal("200.2")
    assert bar.sell_notional == Decimal("100.10")
    assert bar.trade_count == 3
    assert str(bar.bar_id).startswith("20231114")


def test_range_bar_builder_snapshot_open_bar_does_not_reset_active_state():
    builder = RangeBarBuilder(range_pct="0.002", contract_value="0.01")
    builder.on_trade(_trade("1000", 1_700_000_000_000))

    snapshot = builder.snapshot_open_bar()

    assert snapshot is not None
    assert snapshot.open == Decimal("1000")
    assert builder.snapshot_open_bar() is not None
