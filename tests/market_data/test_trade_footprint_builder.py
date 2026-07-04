from __future__ import annotations

from decimal import Decimal

from src.market_data.derived import TradeFootprintBuilder
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


# ---------------------------------------------------------------------------
# fp_max_bucket_abs_delta_pressure placeholder
# ---------------------------------------------------------------------------

def test_fp_max_bucket_abs_delta_pressure_is_zero_in_r007() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000, side=TradeSide.BUY))
    closed = builder.on_trade(_trade("1001", t0 + 60_001))

    assert len(closed) == 1
    assert closed[0].fp_max_bucket_abs_delta_pressure == Decimal("0")


# ---------------------------------------------------------------------------
# close_pos and range_pct
# ---------------------------------------------------------------------------

def test_footprint_close_pos_is_valid() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    # With bar context
    builder.on_trade(_trade("1000", t0 + 1_000), trade_bar_open=Decimal("1000"),
                     trade_bar_high=Decimal("1020"), trade_bar_low=Decimal("990"),
                     trade_bar_close=Decimal("1015"))
    closed = builder.on_trade(_trade("1010", t0 + 60_001), trade_bar_open=Decimal("1000"),
                              trade_bar_high=Decimal("1020"), trade_bar_low=Decimal("990"),
                              trade_bar_close=Decimal("1015"))

    assert len(closed) == 1
    f = closed[0]
    assert Decimal("0") <= f.close_pos <= Decimal("1")
    assert f.range_pct >= Decimal("0")


def test_footprint_context_available_is_true_for_normal_data() -> None:
    builder = TradeFootprintBuilder()
    t0 = 1_700_000_000_000

    builder.on_trade(_trade("1000", t0 + 1_000))
    closed = builder.on_trade(_trade("1001", t0 + 60_001))

    assert len(closed) == 1
    assert closed[0].context_available is True
    assert closed[0].quality == TradeFeatureQuality.COMPLETE.value
