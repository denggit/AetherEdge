from __future__ import annotations

from decimal import Decimal

from src.market_data.derived import FixedTimeTradeBarBuilder, TradeFootprintBuilder
from src.market_data.models import TradeFootprintFeature, TradeFeatureQuality
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

def test_footprint_builder_produces_features_for_closed_buckets() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000, side=TradeSide.BUY))
    builder.on_trade(_trade("1001", t0 + 2_000, side=TradeSide.SELL))

    closed = builder.on_trade(_trade("1002", t0 + 60_001))

    assert len(closed) == 1
    f = closed[0]
    assert f.symbol == "ETH-USDT-PERP"
    assert f.timeframe == "1m"
    assert f.delta_notional > Decimal("0") or f.abs_delta_notional >= Decimal("0")


def test_footprint_delta_notional_is_correct() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    # Each trade notional = price * qty = 1000 * 2 = 2000, etc.
    builder.on_trade(_trade("1000", t0 + 1_000, side=TradeSide.BUY, quantity="2"))
    builder.on_trade(_trade("1000", t0 + 2_000, side=TradeSide.BUY, quantity="1"))
    builder.on_trade(_trade("1000", t0 + 3_000, side=TradeSide.SELL, quantity="1"))

    closed = builder.on_trade(_trade("1000", t0 + 60_001))

    assert len(closed) == 1
    f = closed[0]
    # buy_notional = 1000*2 + 1000*1 = 3000
    # sell_notional = 1000*1 = 1000
    # delta = 2000
    assert f.delta_notional == Decimal("2000")
    assert f.abs_delta_notional == Decimal("2000")


def test_taker_buy_ratio_is_correct() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("100", t0 + 1_000, side=TradeSide.BUY, quantity="3"))
    builder.on_trade(_trade("100", t0 + 2_000, side=TradeSide.SELL, quantity="1"))

    closed = builder.on_trade(_trade("100", t0 + 60_001))

    assert len(closed) == 1
    f = closed[0]
    # buy=300, sell=100, taker_buy_ratio = 300/400 = 0.75
    assert f.taker_buy_ratio == Decimal("0.75")


# ---------------------------------------------------------------------------
# Out-of-order
# ---------------------------------------------------------------------------

def test_out_of_order_trade_is_ignored_by_footprint_builder() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000))
    closed = builder.on_trade(_trade("1001", t0 + 60_001))

    assert len(closed) == 1

    result = builder.on_trade(_trade("1002", t0 + 2_000))
    assert result == ()
    assert builder.stats["out_of_order_trades"] == 1


# ---------------------------------------------------------------------------
# Invalid trades
# ---------------------------------------------------------------------------

def test_invalid_trade_increments_footprint_stats() -> None:
    builder = TradeFootprintBuilder()
    trade = MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("-1"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id="bad",
        event_time_ms=1_700_000_001_000,
        trade_time_ms=1_700_000_001_000,
    )
    builder.on_trade(trade)
    assert builder.stats["invalid_trades"] == 1


# ---------------------------------------------------------------------------
# Snapshot / restore
# ---------------------------------------------------------------------------

def test_footprint_builder_snapshot_restore() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000
    builder.on_trade(_trade("1000", t0 + 1_000))

    state = builder.snapshot_state()
    restored = TradeFootprintBuilder.restore_state(state)

    closed = restored.on_trade(_trade("1001", t0 + 60_001))
    assert len(closed) == 1


def test_safe_watermark_never_closes_newer_footprint_minute() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000
    t0 -= t0 % 60_000
    builder.on_trade(_trade("1000", t0 + 1_000))

    assert builder.drain_completed_through(t0 + 59_998) == ()
    closed = builder.drain_completed_through(t0 + 59_999)
    assert len(closed) == 1
    assert closed[0].quality == TradeFeatureQuality.COMPLETE.value


# ---------------------------------------------------------------------------
# fp_max_bucket_abs_delta_pressure
# ---------------------------------------------------------------------------

def test_price_bucket_notional_delta_and_max_pressure_are_exact() -> None:
    builder = TradeFootprintBuilder(price_bucket_size="1")
    t0 = 1_700_000_000_000

    trades = (
        _trade("100.2", t0 + 1_000, side=TradeSide.BUY, quantity="2"),
        _trade("100.8", t0 + 2_000, side=TradeSide.SELL, quantity="1"),
        _trade("101.1", t0 + 3_000, side=TradeSide.BUY, quantity="1"),
        _trade("101.9", t0 + 4_000, side=TradeSide.SELL, quantity="3"),
    )
    for trade in trades:
        builder.on_trade(trade)

    snapshot = builder.snapshot_state()
    buckets = snapshot["active"]["price_buckets"]
    assert Decimal(buckets["100"]["buy_notional"]) == Decimal("200.4")
    assert Decimal(buckets["100"]["sell_notional"]) == Decimal("100.8")
    assert Decimal(buckets["101"]["buy_notional"]) == Decimal("101.1")
    assert Decimal(buckets["101"]["sell_notional"]) == Decimal("305.7")

    closed = builder.on_trade(_trade("102", t0 + 60_001))

    assert len(closed) == 1
    pressure_100 = abs(Decimal("200.4") - Decimal("100.8")) / Decimal("301.2")
    pressure_101 = abs(Decimal("101.1") - Decimal("305.7")) / Decimal("406.8")
    assert closed[0].fp_max_bucket_abs_delta_pressure == max(
        pressure_100, pressure_101
    )
    assert closed[0].context_available is True
    assert closed[0].quality == TradeFeatureQuality.COMPLETE.value


def test_contract_value_matches_tradebar_notional_delta() -> None:
    footprint = TradeFootprintBuilder(
        contract_value="0.01", price_bucket_size="1"
    )
    tradebar = FixedTimeTradeBarBuilder(contract_value="0.01")
    t0 = 1_700_000_000_000
    trades = (
        _trade("1000", t0 + 1_000, side=TradeSide.BUY, quantity="4"),
        _trade("1001", t0 + 2_000, side=TradeSide.SELL, quantity="2"),
    )
    for trade in trades:
        footprint.on_trade(trade)
        tradebar.on_trade(trade)
    next_trade = _trade("1002", t0 + 60_001)
    closed_fp = footprint.on_trade(next_trade)[0]
    closed_bar = tradebar.on_trade(next_trade)[0]

    assert closed_fp.delta_notional == closed_bar.delta_notional
    assert closed_fp.abs_delta_notional == closed_bar.abs_delta_notional


# ---------------------------------------------------------------------------
# close_pos and range_pct
# ---------------------------------------------------------------------------

def test_footprint_close_pos_is_valid() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    # The footprint builder now tracks OHLCV internally from trades
    builder.on_trade(_trade("1000", t0 + 1_000))
    builder.on_trade(_trade("1020", t0 + 2_000))
    builder.on_trade(_trade("990", t0 + 3_000))
    builder.on_trade(_trade("1015", t0 + 4_000))

    closed = builder.on_trade(_trade("1010", t0 + 60_001))

    assert len(closed) == 1
    f = closed[0]
    assert Decimal("0") <= f.close_pos <= Decimal("1")
    assert f.range_pct >= Decimal("0")


def test_footprint_context_unavailable_when_pressure_cannot_be_computed() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    builder.on_trade(
        _trade("1000", t0 + 1_000, side=TradeSide.UNKNOWN)
    )
    closed = builder.on_trade(_trade("1001", t0 + 60_001))

    assert len(closed) == 1
    assert closed[0].context_available is False
    assert closed[0].quality == TradeFeatureQuality.MISSING_FOOTPRINT_CONTEXT.value
    assert closed[0].fp_max_bucket_abs_delta_pressure == Decimal("0")
