from __future__ import annotations

from decimal import Decimal

import pytest

from src.market_data.models import MarketDataSet, TimeRange
from src.market_data.storage import SqliteKlineStore
from src.market_data.warmup import KlineGapDetector, interval_to_ms
from src.platform.data.models import MarketKline
from src.platform.exchanges.models import ExchangeName

STEP = 4 * 60 * 60_000


def _kline(open_time_ms: int, *, closed: bool = True) -> MarketKline:
    return MarketKline(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        interval="4H",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + STEP - 1,
        open=Decimal("1"),
        high=Decimal("2"),
        low=Decimal("1"),
        close=Decimal("1.5"),
        volume=Decimal("1"),
        is_closed=closed,
    )


def test_interval_to_ms_accepts_common_exchange_style_intervals():
    assert interval_to_ms("4H") == STEP
    assert interval_to_ms("1d") == 24 * 60 * 60_000
    with pytest.raises(ValueError):
        interval_to_ms("7H")


def test_kline_gap_detector_detects_empty_tail_and_middle_gaps(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    detector = KlineGapDetector(store)
    target_range = TimeRange(0, STEP * 3)

    empty = detector.find_gaps(symbol="ETH-USDT-PERP", dataset=MarketDataSet.KLINES, time_range=target_range, interval="4H")
    assert len(empty) == 1
    assert empty[0].time_range == target_range
    assert empty[0].reason == "empty"

    store.save([_kline(0), _kline(STEP * 2)])
    gaps = detector.find_gaps(symbol="ETH-USDT-PERP", dataset=MarketDataSet.KLINES, time_range=target_range, interval="4H")

    assert [(gap.time_range.start_time_ms, gap.time_range.end_time_ms, gap.reason) for gap in gaps] == [
        (STEP, STEP, "missing"),
        (STEP * 3, STEP * 3, "missing_tail"),
    ]


def test_kline_gap_detector_ignores_unclosed_rows(tmp_path):
    store = SqliteKlineStore(tmp_path / "market.sqlite3")
    store.save([_kline(0), _kline(STEP, closed=False)])
    detector = KlineGapDetector(store)

    gaps = detector.find_gaps(symbol="ETH-USDT-PERP", dataset=MarketDataSet.KLINES, time_range=TimeRange(0, STEP), interval="4H")

    assert [(gap.time_range.start_time_ms, gap.time_range.end_time_ms) for gap in gaps] == [(STEP, STEP)]
