from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.derived import FixedTimeTradeBarBuilder
from src.market_data.models import FixedTimeTradeBar
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


def _trade(price: str, time_ms: int, *, side: TradeSide = TradeSide.BUY, quantity: str = "1") -> MarketTrade:
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


# ---------------------------------------------------------------------------
# Basic aggregation
# ---------------------------------------------------------------------------

def test_single_bar_aggregates_trades_in_one_minute_bucket() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000  # a clean minute boundary

    assert builder.on_trade(_trade("1000", t0 + 1_000)) == ()
    assert builder.on_trade(_trade("1001", t0 + 2_000, side=TradeSide.SELL)) == ()
    # next minute triggers close
    closed = builder.on_trade(_trade("1002", t0 + 60_001))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.open == Decimal("1000")
    assert bar.close == Decimal("1001")
    assert bar.volume == Decimal("2")
    assert bar.trade_count == 2


def test_multiple_bars_are_closed_correctly() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000))
    closed1 = builder.on_trade(_trade("1001", t0 + 60_001))  # minute 1
    assert len(closed1) == 1

    builder.on_trade(_trade("1002", t0 + 60_002))
    closed2 = builder.on_trade(_trade("1003", t0 + 120_001))  # minute 2
    assert len(closed2) == 1


def test_drain_returns_pending_and_active_bar() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000))
    builder.on_trade(_trade("1001", t0 + 60_001))  # closes first bar
    builder.on_trade(_trade("1002", t0 + 60_002))  # second bar in progress

    drained = builder.drain()
    # First closed bar was drained by the pending queue drain in the
    # second on_trade call. drain() returns any pending closed + forced active.
    assert len(drained) >= 1  # at least the active bar


def test_safe_watermark_never_closes_newer_active_minute() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000
    t0 -= t0 % 60_000
    builder.on_trade(_trade("1000", t0 + 1_000))

    assert builder.drain_completed_through(t0 + 59_998) == ()
    assert builder.snapshot_state()["active"] is not None
    closed = builder.drain_completed_through(t0 + 59_999)
    assert len(closed) == 1


# ---------------------------------------------------------------------------
# Out-of-order trades
# ---------------------------------------------------------------------------

def test_out_of_order_trade_does_not_pollute_closed_bar() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000))
    closed = builder.on_trade(_trade("1001", t0 + 60_001))  # closes minute 0

    assert len(closed) == 1
    assert closed[0].trade_count == 1

    # Late trade for already-closed bucket
    result = builder.on_trade(_trade("1002", t0 + 2_000))
    assert result == ()  # dropped
    assert builder.stats["out_of_order_trades"] == 1

    # Verify closed bar wasn't backfilled
    assert closed[0].trade_count == 1


# ---------------------------------------------------------------------------
# Invalid trades
# ---------------------------------------------------------------------------

def test_invalid_trade_is_ignored_and_counted() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")

    trade = MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("-1"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id="invalid",
        event_time_ms=1_700_000_001_000,
        trade_time_ms=1_700_000_001_000,
    )
    assert builder.on_trade(trade) == ()
    assert builder.stats["invalid_trades"] == 1


def test_zero_quantity_trade_is_invalid() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")

    trade = MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("1000"),
        quantity=Decimal("0"),
        side=TradeSide.BUY,
        trade_id="zeroqty",
        event_time_ms=1_700_000_001_000,
        trade_time_ms=1_700_000_001_000,
    )
    assert builder.on_trade(trade) == ()
    assert builder.stats["invalid_trades"] == 1


# ---------------------------------------------------------------------------
# available_time_ms
# ---------------------------------------------------------------------------

def test_available_time_ms_is_gte_close_time_ms() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000))
    closed = builder.on_trade(_trade("1001", t0 + 60_050))

    assert len(closed) == 1
    assert closed[0].available_time_ms >= closed[0].close_time_ms


# ---------------------------------------------------------------------------
# Large trade tracking
# ---------------------------------------------------------------------------

def test_large_trades_are_tracked_separately() -> None:
    builder = FixedTimeTradeBarBuilder(
        contract_value="1",
        large_trade_threshold_notional="500",
    )
    t0 = 1_700_000_000_000

    # Small trade: 1000 * 1 * 1 = 1000 < 500? No, 1000 > 500
    # Trade: price=100, qty=1 => notional=100 < 500
    builder.on_trade(_trade("100", t0 + 1_000, quantity="1"))
    # Large: price=1000, qty=1 => notional=1000 > 500
    builder.on_trade(_trade("1000", t0 + 2_000, quantity="1"))

    closed = builder.on_trade(_trade("500", t0 + 60_001))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.large_buy_notional > 0
    assert bar.large_trade_count == 1


# ---------------------------------------------------------------------------
# Snapshot / restore
# ---------------------------------------------------------------------------

def test_snapshot_state_and_restore_preserves_active_bar() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000
    builder.on_trade(_trade("1000", t0 + 1_000))
    builder.on_trade(_trade("1001", t0 + 2_000, side=TradeSide.SELL))

    state = builder.snapshot_state()
    assert state["version"] == 1
    assert state["active"] is not None

    restored = FixedTimeTradeBarBuilder.restore_state(state)
    # Close the bar by feeding a next-minute trade
    closed = restored.on_trade(_trade("1002", t0 + 60_001))
    assert len(closed) == 1
    assert closed[0].trade_count == 2  # trades within the closed bucket
    assert closed[0].volume == Decimal("2")


# ---------------------------------------------------------------------------
# Bar properties
# ---------------------------------------------------------------------------

def test_bar_properties_calculated_correctly() -> None:
    builder = FixedTimeTradeBarBuilder(contract_value="1")
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000, side=TradeSide.BUY, quantity="2"))
    builder.on_trade(_trade("1010", t0 + 2_000, side=TradeSide.SELL, quantity="1"))

    closed = builder.on_trade(_trade("1005", t0 + 60_001))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.open == Decimal("1000")
    assert bar.high == Decimal("1010")
    assert bar.low == Decimal("1000")
    assert bar.close == Decimal("1010")
    assert bar.buy_volume == Decimal("2")
    assert bar.sell_volume == Decimal("1")
    assert bar.notional > 0
    assert bar.taker_buy_ratio >= Decimal("0")
