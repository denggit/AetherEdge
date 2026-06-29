from __future__ import annotations

import time
import sqlite3
from decimal import Decimal

from src.market_data.models import TimeRange
from src.market_data.realtime_trade_recorder import RealtimeTradeRecorder, RealtimeTradeRecorderConfig
from src.market_data.storage import SqliteTradeStore
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


def _trade(ts: int) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal("100"),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        trade_id=str(ts),
        trade_time_ms=ts,
        event_time_ms=ts,
    )


def test_writer_batches_into_sqlite_store(tmp_path):
    recorder = RealtimeTradeRecorder(
        RealtimeTradeRecorderConfig(db_path=tmp_path / "market.sqlite3", batch_size=2, flush_interval_ms=50)
    )
    recorder.start()
    assert recorder.submit(_trade(1))
    assert recorder.submit(_trade(2))
    recorder.stop(flush=True)

    rows = SqliteTradeStore(tmp_path / "market.sqlite3").load(
        symbol="ETH-USDT-PERP",
        time_range=__import__("src.market_data.models", fromlist=["TimeRange"]).TimeRange(1, 2),
    )
    assert len(rows) == 2


def test_submit_does_not_wait_for_slow_db_write():
    class SlowStore:
        def save(self, rows):
            time.sleep(0.2)
            return len(rows)

        def mark_coverage(self, **_kwargs):
            return None

    recorder = RealtimeTradeRecorder(
        RealtimeTradeRecorderConfig(batch_size=1, flush_interval_ms=1, queue_maxsize=10),
        store=SlowStore(),  # type: ignore[arg-type]
    )
    recorder.start()
    start = time.monotonic()
    assert recorder.submit(_trade(1))
    elapsed = time.monotonic() - start
    recorder.stop(flush=False)
    assert elapsed < 0.05


def test_queue_full_returns_false_without_blocking():
    recorder = RealtimeTradeRecorder(RealtimeTradeRecorderConfig(queue_maxsize=1), store=object())  # type: ignore[arg-type]
    assert recorder.submit(_trade(1)) is True
    assert recorder.submit(_trade(2)) is False


def test_sqlite_locked_does_not_crash_recorder():
    class LockedStore:
        def save(self, _rows):
            raise sqlite3.OperationalError("database is locked")

    errors: list[BaseException] = []
    recorder = RealtimeTradeRecorder(
        RealtimeTradeRecorderConfig(batch_size=1, flush_interval_ms=1, queue_maxsize=10),
        store=LockedStore(),  # type: ignore[arg-type]
        on_error=errors.append,
    )
    recorder.start()
    assert recorder.submit(_trade(1))
    recorder.stop(flush=True)

    assert recorder.failures == 1
    assert isinstance(errors[0], sqlite3.OperationalError)


def test_flush_marks_realtime_segment_not_complete_bucket(tmp_path):
    recorder = RealtimeTradeRecorder(
        RealtimeTradeRecorderConfig(db_path=tmp_path / "market.sqlite3", batch_size=2, flush_interval_ms=50)
    )
    recorder.start()
    assert recorder.submit(_trade(1_000))
    assert recorder.submit(_trade(2_000))
    recorder.stop(flush=True)

    store = SqliteTradeStore(tmp_path / "market.sqlite3")
    rows = store.load(symbol="ETH-USDT-PERP", time_range=TimeRange(1_000, 2_000))
    coverage = store.coverage_ranges(
        symbol="ETH-USDT-PERP",
        time_range=TimeRange(1_000, 2_000),
        source="realtime_segment",
        coverage_status="SEGMENT",
    )

    assert len(rows) == 2
    assert coverage == [TimeRange(1_000, 2_000)]
