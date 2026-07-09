from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.market_data.backfill.coordinator import (
    BACKGROUND_BACKFILL_PRIORITY,
    EXPEDITED_BACKFILL_PRIORITY,
    RawTradeBackfillCoordinator,
)
from src.market_data.models import (
    FixedTimeTradeBar,
    RangeFootprintFeature,
    TimeRange,
    TradeFeatureBackfillTarget,
)
from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
from src.market_data.trade_features.coverage import safe_okx_archive_end_ms
from src.platform.data.models import MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName
from tools import mf_feature_backfill_worker as worker

_MINUTE = 60_000
_OKX_TIMEZONE = timezone(timedelta(hours=8))


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


def test_parser_required_minutes_defaults_to_4320() -> None:
    assert worker.parse_args([]).required_minutes == 4320
    assert worker.parse_args([]).archive_publish_lag_hours == 8.0


def test_parser_accepts_required_minutes_override() -> None:
    args = worker.parse_args(
        ["--required-minutes", "172800"]
    )

    assert args.required_minutes == 172800


def test_main_passes_required_minutes_to_run_cycle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = {}

    def capture_cycle(**kwargs):
        captured.update(kwargs)
        return {"status": "up_to_date", "reason": "no_gap_found"}

    monkeypatch.setattr(worker, "run_cycle", capture_cycle)
    monkeypatch.setattr(
        worker,
        "_update_status",
        lambda *args, **kwargs: None,
    )

    result = worker.main(
        [
            "--once",
            "--required-minutes",
            "172800",
            "--status-path",
            str(tmp_path / "status.json"),
        ]
    )

    assert result == 0
    assert captured["required_minutes"] == 172800
    assert captured["archive_publish_lag_hours"] == 8.0


class _Archive:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    def ensure_daily_file(self, **kwargs):
        return SimpleNamespace(path=Path("unused.zip"), downloaded=False)


def test_worker_empty_store_writes_tradebar_and_range_footprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    safe_end = safe_okx_archive_end_ms()
    archive_day = datetime.fromtimestamp(
        safe_end / 1_000,
        tz=timezone.utc,
    ).astimezone(_OKX_TIMEZONE).date()
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
        lambda *_: iter((archive_day,)),
    )
    monkeypatch.setattr(worker, "iter_trade_csv_chunks", lambda *_args, **_kwargs: iter((object(),)))
    monkeypatch.setattr(worker, "normalize_okx_trade_chunk", lambda *_args, **_kwargs: trades)

    result = worker.run_cycle(**_kwargs(tmp_path))

    # Chunk completes (2 minutes written) but full required window (4320 min)
    # is not satisfied → partial, not ok.
    assert result["status"] == "partial"
    assert result["reason"] == "mf_signal_feature_incomplete"
    assert result["target_end_ms"] <= result["safe_archive_end_ms"]
    assert result["total_bars_written"] == 2
    assert result["total_footprints_written"] == 2
    assert result["range_footprints_written"] == 2
    assert result["mf_signal_feature_ready"] is False
    assert result["full_window_required_minutes"] == 4320
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


def test_large_required_window_not_satisfied_by_single_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """required_minutes=5 > max_minutes_per_cycle=2: first chunk fills
    only 2 of 5 required minutes. Full-window mf_signal_feature_ready
    must be False; status must be partial, not ok."""
    safe_end = safe_okx_archive_end_ms()
    archive_day = datetime.fromtimestamp(
        safe_end / 1_000, tz=timezone.utc,
    ).astimezone(_OKX_TIMEZONE).date()
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
        lambda *_: iter((archive_day,)),
    )
    monkeypatch.setattr(
        worker, "iter_trade_csv_chunks", lambda *_args, **_kwargs: iter((object(),))
    )
    monkeypatch.setattr(
        worker, "normalize_okx_trade_chunk", lambda *_args, **_kwargs: trades
    )

    kwargs = _kwargs(tmp_path)
    kwargs["required_minutes"] = 5

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "partial"
    assert result["reason"] == "mf_signal_feature_incomplete"
    assert result["mf_signal_feature_ready"] is False
    assert result["full_window_required_minutes"] == 5
    # Chunk bars were written
    assert result["total_bars_written"] == 2


def test_full_window_ready_when_required_minutes_matches_chunk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """required_minutes == max_minutes_per_cycle == 2: after filling the
    chunk the full window must be ready (mf_signal_feature_ready=True)."""
    safe_end = safe_okx_archive_end_ms()
    archive_day = datetime.fromtimestamp(
        safe_end / 1_000, tz=timezone.utc,
    ).astimezone(_OKX_TIMEZONE).date()
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
        lambda *_: iter((archive_day,)),
    )
    monkeypatch.setattr(
        worker, "iter_trade_csv_chunks", lambda *_args, **_kwargs: iter((object(),))
    )
    monkeypatch.setattr(
        worker, "normalize_okx_trade_chunk", lambda *_args, **_kwargs: trades
    )

    kwargs = _kwargs(tmp_path)
    kwargs["required_minutes"] = 2

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "ok"
    assert result["reason"] == "cycle_complete"
    assert result["mf_signal_feature_ready"] is True
    assert result["full_window_required_minutes"] == 2


def test_main_download_failure_exits_with_cooldown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When run_cycle returns download_failures for old archives,
    main() must exit with retry cooldown, not sleep + retry."""
    cycle_results = [
        {
            "status": "partial",
            "reason": "download_failures",
            "failed_downloads": ["2026-07-01"],
            "archive_not_published_days": [],
            "mf_signal_feature_ready": False,
        },
    ]
    call_count = [0]
    status_writes = []

    def fake_cycle(**kwargs):
        idx = min(call_count[0], len(cycle_results) - 1)
        call_count[0] += 1
        return dict(cycle_results[idx])

    monkeypatch.setattr(worker, "run_cycle", fake_cycle)
    monkeypatch.setattr(
        worker,
        "_update_status",
        lambda status_path, **kw: status_writes.append(kw),
    )

    exit_code = worker.main(
        [
            "--no-once",
            "--mode", "live",
            "--status-path", str(tmp_path / "status.json"),
            "--failure-cooldown-seconds", "7200",
            "--no-download",
        ]
    )

    assert exit_code == 0
    assert call_count[0] == 1  # only one cycle, then exit
    # Final status write should be running=False with next_retry_after_ms
    final = [w for w in status_writes if w.get("running") is False]
    assert len(final) == 1
    assert final[0].get("next_retry_after_ms") is not None


def test_worker_clamps_requested_current_day_target_and_reports_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    safe_end = safe_okx_archive_end_ms()
    start = safe_end - _MINUTE + 1
    monkeypatch.setattr(
        worker,
        "compute_mf_signal_backfill_target",
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

    assert result["status"] == "deferred"
    assert result["reason"] == "archive_not_published_yet"
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
    assert result["reason"] == "mf_signal_feature_incomplete"
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


def test_live_worker_allows_archive_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live mode with no_download=False must allow archive download."""
    kwargs = _kwargs(tmp_path)
    kwargs["no_download"] = False
    calls = []

    class _DownloadArchive(_Archive):
        def ensure_daily_file(self, **values):
            calls.append(values)
            return super().ensure_daily_file(**values)

    monkeypatch.setattr(worker, "OkxHistoricalTradeArchive", _DownloadArchive)
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((date(2026, 7, 3),)),
    )
    monkeypatch.setattr(
        worker,
        "iter_trade_csv_chunks",
        lambda *_args, **_kwargs: iter(()),
    )

    worker.run_cycle(**kwargs)

    assert calls
    assert calls[0]["allow_download"] is True


def test_prebuild_worker_allows_archive_download(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kwargs = _kwargs(tmp_path)
    kwargs["mode"] = "prebuild"
    kwargs["no_download"] = False
    calls = []

    class _DownloadArchive(_Archive):
        def ensure_daily_file(self, **values):
            calls.append(values)
            return super().ensure_daily_file(**values)

    monkeypatch.setattr(worker, "OkxHistoricalTradeArchive", _DownloadArchive)
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((date(2026, 7, 3),)),
    )
    monkeypatch.setattr(
        worker,
        "iter_trade_csv_chunks",
        lambda *_args, **_kwargs: iter(()),
    )

    worker.run_cycle(**kwargs)

    assert calls
    assert calls[0]["allow_download"] is True


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
        priority=BACKGROUND_BACKFILL_PRIORITY,
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
    monkeypatch.setattr(
        worker, "compute_mf_signal_backfill_target", lambda **_: None
    )

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "up_to_date"
    assert not Path(kwargs["global_lock_path"]).exists()


def test_worker_does_not_backfill_historical_footprint_coverage_when_mf_signal_ready(
    tmp_path: Path,
) -> None:
    kwargs = _kwargs(tmp_path)
    kwargs["required_minutes"] = 3
    safe_end = safe_okx_archive_end_ms()
    start = safe_end - 3 * _MINUTE + 1
    store = SqliteTradeFeatureStore(path=kwargs["market_db"])
    bars = []
    for index in range(3):
        open_ms = start + index * _MINUTE
        close_ms = open_ms + _MINUTE - 1
        bars.append(
            FixedTimeTradeBar(
                exchange="okx",
                symbol="ETH-USDT-PERP",
                timeframe="1m",
                open_time_ms=open_ms,
                close_time_ms=close_ms,
                available_time_ms=close_ms,
                open=Decimal("3000"),
                high=Decimal("3005"),
                low=Decimal("2995"),
                close=Decimal("3002"),
                volume=Decimal("10"),
                buy_volume=Decimal("6"),
                sell_volume=Decimal("4"),
                buy_notional=Decimal("18000"),
                sell_notional=Decimal("12000"),
                delta_volume=Decimal("2"),
                delta_notional=Decimal("6000"),
                abs_delta_notional=Decimal("6000"),
                trade_count=5,
                large_trade_share=Decimal("0.05"),
                quality="COMPLETE",
            )
        )
    store.upsert_tradebars_many(bars)
    store.upsert_range_footprints_many(
        [
            RangeFootprintFeature(
                exchange="okx",
                symbol="ETH-USDT-PERP",
                range_pct=Decimal("0.002"),
                price_step=Decimal("1"),
                range_bar_id=1,
                range_start_ms=bars[-1].open_time_ms - 30_000,
                range_end_ms=bars[-1].open_time_ms - 1,
                available_time_ms=bars[-1].open_time_ms - 1,
                fp_max_bucket_abs_delta_pressure=Decimal("0.8"),
                fp_low_bucket_delta_pressure=Decimal("-0.2"),
                fp_high_bucket_delta_pressure=Decimal("0.4"),
                fp_delta_pressure=Decimal("0.1"),
                bucket_count=5,
                trade_count=20,
                context_available=True,
                quality="COMPLETE",
            )
        ]
    )

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "up_to_date"
    assert result["reason"] == "no_gap_found"
    assert not Path(kwargs["global_lock_path"]).exists()


def test_prebuild_does_not_attempt_archive_day_inside_publish_lag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lag_safe_end = _okx_day_end_ms(date(2026, 7, 5))
    calendar_safe_end = _okx_day_end_ms(date(2026, 7, 6))
    attempted_days = []

    def safe_end(*args, archive_publish_lag_hours=8.0, **kwargs):
        return (
            calendar_safe_end
            if archive_publish_lag_hours == 0
            else lag_safe_end
        )

    def archive_dates(start_ms, end_ms):
        if start_ms == lag_safe_end + 1:
            return iter((date(2026, 7, 6),))
        return iter((date(2026, 7, 5),))

    class RecordingArchive(_Archive):
        def ensure_daily_file(self, **values):
            attempted_days.append(values["day"])
            return super().ensure_daily_file(**values)

    kwargs = _kwargs(tmp_path)
    kwargs.update(mode="prebuild", no_download=False)
    monkeypatch.setattr(worker, "safe_okx_archive_end_ms", safe_end)
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        archive_dates,
    )
    monkeypatch.setattr(
        worker,
        "OkxHistoricalTradeArchive",
        RecordingArchive,
    )
    monkeypatch.setattr(
        worker,
        "iter_trade_csv_chunks",
        lambda *_args, **_kwargs: iter(()),
    )

    worker.run_cycle(**kwargs)

    assert attempted_days == [date(2026, 7, 5)]
    assert date(2026, 7, 6) not in attempted_days


def test_latest_unpublished_archive_failure_is_deferred(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lag_safe_end = _okx_day_end_ms(date(2026, 7, 5))
    calendar_safe_end = _okx_day_end_ms(date(2026, 7, 6))

    def safe_end(*args, archive_publish_lag_hours=8.0, **kwargs):
        return (
            calendar_safe_end
            if archive_publish_lag_hours == 0
            else lag_safe_end
        )

    class MissingArchive(_Archive):
        def ensure_daily_file(self, **kwargs):
            raise FileNotFoundError(kwargs["day"].isoformat())

    kwargs = _kwargs(tmp_path)
    kwargs.update(mode="prebuild", no_download=False)
    monkeypatch.setattr(worker, "safe_okx_archive_end_ms", safe_end)
    monkeypatch.setattr(
        worker,
        "compute_mf_signal_backfill_target",
        lambda **_: TradeFeatureBackfillTarget(
            start_ms=lag_safe_end - _MINUTE + 1,
            end_ms=lag_safe_end,
            reason="test_unpublished",
        ),
    )
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((date(2026, 7, 6),)),
    )
    monkeypatch.setattr(
        worker,
        "OkxHistoricalTradeArchive",
        MissingArchive,
    )

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "deferred"
    assert result["reason"] == "archive_not_published_yet"
    assert result["archive_not_published_days"] == ["2026-07-06"]
    assert result["failed_downloads"] == []
    assert result["processed_through_ms"] is None
    assert result["can_finalize_safe_history"] is False


def test_older_archive_failure_remains_download_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    safe_end_ms = _okx_day_end_ms(date(2026, 7, 6))

    class MissingArchive(_Archive):
        def ensure_daily_file(self, **kwargs):
            raise FileNotFoundError(kwargs["day"].isoformat())

    kwargs = _kwargs(tmp_path)
    kwargs.update(mode="prebuild", no_download=False)
    monkeypatch.setattr(
        worker,
        "safe_okx_archive_end_ms",
        lambda *args, **kwargs: safe_end_ms,
    )
    monkeypatch.setattr(
        worker,
        "compute_mf_signal_backfill_target",
        lambda **_: TradeFeatureBackfillTarget(
            start_ms=_okx_day_start_ms(date(2026, 7, 5)),
            end_ms=safe_end_ms,
            reason="test_older_failure",
        ),
    )
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((date(2026, 7, 5),)),
    )
    monkeypatch.setattr(
        worker,
        "OkxHistoricalTradeArchive",
        MissingArchive,
    )

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "partial"
    assert result["reason"] == "download_failures"
    assert result["failed_downloads"] == ["2026-07-05"]
    assert result["archive_not_published_days"] == []
    assert result["can_finalize_safe_history"] is False


def test_processed_through_stops_before_failed_later_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_day = date(2026, 7, 5)
    failed_day = date(2026, 7, 6)
    target_end = _okx_day_end_ms(failed_day)

    class SecondDayMissingArchive(_Archive):
        def ensure_daily_file(self, **kwargs):
            if kwargs["day"] == failed_day:
                raise FileNotFoundError(kwargs["day"].isoformat())
            return super().ensure_daily_file(**kwargs)

    kwargs = _kwargs(tmp_path)
    kwargs.update(mode="prebuild", no_download=False)
    monkeypatch.setattr(
        worker,
        "safe_okx_archive_end_ms",
        lambda *args, **kwargs: target_end,
    )
    monkeypatch.setattr(
        worker,
        "compute_mf_signal_backfill_target",
        lambda **_: TradeFeatureBackfillTarget(
            start_ms=_okx_day_start_ms(first_day),
            end_ms=target_end,
            reason="test_later_failure",
        ),
    )
    monkeypatch.setattr(
        worker,
        "iter_okx_archive_dates_for_utc_range",
        lambda *_: iter((first_day, failed_day)),
    )
    monkeypatch.setattr(
        worker,
        "OkxHistoricalTradeArchive",
        SecondDayMissingArchive,
    )
    monkeypatch.setattr(
        worker,
        "iter_trade_csv_chunks",
        lambda *_args, **_kwargs: iter(()),
    )

    result = worker.run_cycle(**kwargs)

    assert result["reason"] == "download_failures"
    assert result["processed_through_ms"] == _okx_day_end_ms(first_day)
    assert result["processed_through_ms"] < result["target_end_ms"]
    assert result["can_finalize_safe_history"] is False


# ---------------------------------------------------------------------------
# --once / --no-once parser and resolve_once tests
# ---------------------------------------------------------------------------


def test_parser_once_defaults_to_none() -> None:
    args = worker.parse_args([])
    assert args.once is None


def test_parser_once_flag_sets_true() -> None:
    args = worker.parse_args(["--once"])
    assert args.once is True


def test_parser_no_once_flag_sets_false() -> None:
    args = worker.parse_args(["--no-once"])
    assert args.once is False


def test_resolve_once_explicit_overrides_mode() -> None:
    args = worker.parse_args(["--once", "--mode", "live"])
    assert worker.resolve_once(args) is True

    args = worker.parse_args(["--no-once", "--mode", "prebuild"])
    assert worker.resolve_once(args) is False


def test_resolve_once_live_defaults_to_false() -> None:
    args = worker.parse_args(["--mode", "live"])
    assert worker.resolve_once(args) is False


def test_resolve_once_prebuild_defaults_to_true() -> None:
    args = worker.parse_args(["--mode", "prebuild"])
    assert worker.resolve_once(args) is True


def test_parser_sleep_seconds_default() -> None:
    args = worker.parse_args([])
    assert args.sleep_seconds == 30.0


def test_parser_global_lock_priority_default() -> None:
    # Default is now BACKGROUND to avoid starving live range-speed backfill
    # when the worker is invoked directly without an explicit priority.
    args = worker.parse_args([])
    assert args.global_lock_priority == BACKGROUND_BACKFILL_PRIORITY


# ---------------------------------------------------------------------------
# global_lock_priority tests
# ---------------------------------------------------------------------------


def test_run_cycle_uses_global_lock_priority_for_acquire(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_cycle must pass global_lock_priority to try_acquire."""
    kwargs = _kwargs(tmp_path)
    kwargs["global_lock_priority"] = 50
    acquire_calls = []

    class _TrackingCoordinator:
        def __init__(self, **kw):
            pass

        def try_acquire(self, **kw):
            acquire_calls.append(kw)
            return False

        def current_owner(self):
            return {}

        def release(self):
            pass

        def heartbeat(self):
            pass

    monkeypatch.setattr(
        worker, "RawTradeBackfillCoordinator", _TrackingCoordinator
    )

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "skipped"
    assert acquire_calls[0]["priority"] == 50


def test_run_cycle_global_lock_reason_lower_priority_holder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When holder priority < requested priority, reason is
    waiting_for_lower_priority_worker."""
    kwargs = _kwargs(tmp_path)
    kwargs["global_lock_priority"] = 50
    acquire_calls = []

    class _TrackingCoordinator:
        def __init__(self, **kw):
            pass

        def try_acquire(self, **kw):
            acquire_calls.append(kw)
            return False

        def current_owner(self):
            return {"priority": 10}

        def release(self):
            pass

        def heartbeat(self):
            pass

    monkeypatch.setattr(
        worker, "RawTradeBackfillCoordinator", _TrackingCoordinator
    )

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "skipped"
    assert result["reason"] == "waiting_for_lower_priority_worker"
    assert acquire_calls[0]["priority"] == 50


def test_run_cycle_global_lock_reason_higher_priority_holder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When holder priority >= requested priority, reason is
    global_lock_not_acquired."""
    kwargs = _kwargs(tmp_path)
    kwargs["global_lock_priority"] = 50
    acquire_calls = []

    class _TrackingCoordinator:
        def __init__(self, **kw):
            pass

        def try_acquire(self, **kw):
            acquire_calls.append(kw)
            return False

        def current_owner(self):
            return {"priority": 100}

        def release(self):
            pass

        def heartbeat(self):
            pass

    monkeypatch.setattr(
        worker, "RawTradeBackfillCoordinator", _TrackingCoordinator
    )

    result = worker.run_cycle(**kwargs)

    assert result["status"] == "skipped"
    assert result["reason"] == "global_lock_not_acquired"
    assert acquire_calls[0]["priority"] == 50


def _okx_day_start_ms(day: date) -> int:
    return int(
        datetime(
            day.year,
            day.month,
            day.day,
            tzinfo=_OKX_TIMEZONE,
        ).timestamp()
        * 1_000
    )


def _okx_day_end_ms(day: date) -> int:
    return _okx_day_start_ms(day + timedelta(days=1)) - 1
