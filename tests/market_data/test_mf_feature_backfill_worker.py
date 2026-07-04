from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.market_data.backfill.coordinator import (
    LF_RANGE_BACKFILL_PRIORITY,
    RawTradeBackfillCoordinator,
)
from src.market_data.models import TimeRange, TradeFeatureBackfillTarget
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import safe_okx_archive_end_ms
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from tools import mf_feature_backfill_worker as worker

_MINUTE = 60_000


def _trade(price: str, time_ms: int, side: TradeSide) -> MarketTrade:
    return MarketTrade(
        exchange=ExchangeName.OKX,
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=side,
        trade_id=str(time_ms),
        event_time_ms=time_ms,
        trade_time_ms=time_ms,
    )


def _kwargs(tmp_path: Path) -> dict:
    return {
        "symbol": "ETH-USDT-PERP",
        "exchange": "okx",
        "market_db": str(tmp_path / "market.sqlite3"),
        "raw_root": str(tmp_path / "raw"),
        "status_path": str(tmp_path / "mf_status.json"),
        "lock_path": str(tmp_path / "mf.lock"),
        "global_lock_path": str(tmp_path / "global.lock"),
        "global_status_path": str(tmp_path / "global_status.json"),
        "mode": "live",
        "direction": "recent-to-oldest",
        "max_minutes_per_cycle": 2,
        "max_days_per_cycle": 1,
        "max_trades_per_cycle": 100,
        "max_seconds_per_cycle": 30.0,
        "chunk_sleep_seconds": 0.0,
        "no_download": True,
        "save_raw_trades": False,
        "contract_value": Decimal("0.01"),
        "large_trade_threshold": Decimal("10000"),
        "price_bucket_size": Decimal("1"),
    }


class _Archive:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def ensure_daily_file(self, **kwargs):
        return SimpleNamespace(path=Path("unused.zip"), downloaded=False)


def test_worker_empty_store_writes_tradebar_and_range_footprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    safe_end = safe_okx_archive_end_ms()
    start = safe_end - 2 * _MINUTE + 1
    trades = [
        _trade("990", start - _MINUTE, TradeSide.BUY),
        _trade("992", start - 30_000, TradeSide.SELL),
        _trade("1000.2", start + 1_000, TradeSide.BUY),
        _trade("1000.8", start + 2_000, TradeSide.SELL),
        _trade("1001.2", start + _MINUTE + 1_000, TradeSide.BUY),
        _trade("1002.3", start + _MINUTE + 2_000, TradeSide.SELL),
    ]
    monkeypatch.setattr(worker, "OkxHistoricalTradeArchive", _Archive)
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((date(2026, 7, 3),)),
    )
    monkeypatch.setattr(worker, "iter_trade_csv_chunks", lambda *_args, **_kwargs: iter((object(),)))
    monkeypatch.setattr(worker, "normalize_okx_trade_chunk", lambda *_args, **_kwargs: trades)

    result = worker.run_cycle(**_kwargs(tmp_path))

    assert result["status"] == "ok"
    assert result["target_end_ms"] <= result["safe_archive_end_ms"]
    assert result["total_bars_written"] == 2
    assert result["total_footprints_written"] == 2
    assert result["range_footprints_written"] == 2
    assert result["mf_signal_feature_ready"] is True
    store = SqliteTradeFeatureStore(path=tmp_path / "market.sqlite3")
    bars = store.load_range_tradebars(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        time_range=TimeRange(start, safe_end),
    )
    footprints = store.load_range_footprints(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        time_range=TimeRange(start, safe_end),
    )
    assert len(bars) == len(footprints) == 2
    assert all(item.context_available for item in footprints)
    assert all(item.quality == "COMPLETE" for item in footprints)
    range_footprints = store.load_range_footprint_features(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        time_range=TimeRange(start, safe_end),
    )
    assert len(range_footprints) == 1
    assert range_footprints[0].available_time_ms <= safe_end
    assert range_footprints[0].context_available is True
    assert (
        result["range_footprint_coverage_after"][
            "range_footprint_context_seed_available_time_ms"
        ]
        < start
    )
    assert result["coverage_after"]["available"] is True


def test_worker_clamps_requested_current_day_target_and_reports_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    safe_end = safe_okx_archive_end_ms()
    start = safe_end - _MINUTE + 1
    monkeypatch.setattr(
        worker,
        "compute_backfill_target",
        lambda **_: TradeFeatureBackfillTarget(
            start_ms=start,
            end_ms=safe_end + _MINUTE,
            reason="test_current_day",
        ),
    )
    monkeypatch.setattr(worker, "OkxHistoricalTradeArchive", _Archive)
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((date(2026, 7, 3),)),
    )
    monkeypatch.setattr(worker, "iter_trade_csv_chunks", lambda *_args, **_kwargs: iter(()))

    result = worker.run_cycle(**_kwargs(tmp_path))

    assert result["status"] == "partial"
    assert result["reason"] == "current_day_archive_not_ready"
    assert result["target_end_ms"] == safe_end
    assert result["current_day_gap_unrecoverable_until_archive"] is True


def test_worker_does_not_write_active_range_footprint_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    safe_end = safe_okx_archive_end_ms()
    start = safe_end - 2 * _MINUTE + 1
    trades = [
        _trade("1000.2", start + 1_000, TradeSide.BUY),
        _trade("1000.8", start + 2_000, TradeSide.SELL),
        _trade("1001.2", start + _MINUTE + 1_000, TradeSide.BUY),
        _trade("1001.8", start + _MINUTE + 2_000, TradeSide.SELL),
    ]
    monkeypatch.setattr(worker, "OkxHistoricalTradeArchive", _Archive)
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((date(2026, 7, 3),)),
    )
    monkeypatch.setattr(
        worker,
        "iter_trade_csv_chunks",
        lambda *_args, **_kwargs: iter((object(),)),
    )
    monkeypatch.setattr(
        worker, "normalize_okx_trade_chunk", lambda *_args, **_kwargs: trades
    )

    result = worker.run_cycle(**_kwargs(tmp_path))

    assert result["status"] == "partial"
    assert result["reason"] == "feature_coverage_incomplete"
    assert result["range_footprints_written"] == 0
    assert result["mf_signal_feature_ready"] is False
    store = SqliteTradeFeatureStore(path=tmp_path / "market.sqlite3")
    assert (
        store.load_range_footprint_features(
            symbol="ETH-USDT-PERP",
            exchange="okx",
            time_range=TimeRange(start, safe_end),
        )
        == []
    )


def test_live_worker_rejects_save_raw_trades(tmp_path: Path) -> None:
    kwargs = _kwargs(tmp_path)
    kwargs["save_raw_trades"] = True
    with pytest.raises(ValueError, match="forbidden in live mode"):
        worker.run_cycle(**kwargs)


def test_fresh_lf_lock_makes_mf_worker_wait_without_raw_read(
    tmp_path: Path,
) -> None:
    kwargs = _kwargs(tmp_path)
    lf = RawTradeBackfillCoordinator(
        lock_path=kwargs["global_lock_path"],
        status_path=kwargs["global_status_path"],
    )
    assert lf.try_acquire(
        owner="range_backfill",
        priority=LF_RANGE_BACKFILL_PRIORITY,
        symbol="ETH-USDT-PERP",
        raw_days=1,
    )
    try:
        result = worker.run_cycle(**kwargs)
    finally:
        lf.release()

    assert result["status"] == "skipped"
    assert result["reason"] == "waiting_for_lower_priority_worker"


def test_mf_worker_releases_global_lock_on_no_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kwargs = _kwargs(tmp_path)
    monkeypatch.setattr(worker, "compute_backfill_target", lambda **_: None)

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "up_to_date"
    assert not Path(kwargs["global_lock_path"]).exists()
