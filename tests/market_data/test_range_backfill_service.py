from __future__ import annotations

import zipfile

from src.market_data.backfill.coverage import current_closed_bucket_end_ms
from src.market_data.backfill.models import RangeBackfillRequest
from src.market_data.backfill.service import RangeBackfillService
from src.market_data.historical_trades.okx_archive import okx_raw_symbol_from_canonical


def _write_zip(root, raw_symbol: str, day: str, rows: str) -> None:
    path = root / raw_symbol / f"{raw_symbol}-trades-{day}.zip"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{raw_symbol}-trades-{day}.csv", "ts,px,sz,side,trade_id\n" + rows)


def test_service_builds_forward_and_marks_only_closed_complete(tmp_path) -> None:
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    target_start = closed_end - 4 * 60 * 60_000 + 1
    _write_zip(raw_root, raw, "2026-06-29", "")
    _write_zip(
        raw_root,
        raw,
        "2026-06-30",
        f"{target_start + 1},100,1,buy,a\n"
        f"{target_start + 2},101.5,1,buy,b\n",
    )
    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=1,
        max_buckets_per_cycle=1,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
    )

    summary = RangeBackfillService(request).run_once(now_ms_value=now_ms)

    assert summary.status == "ok"
    assert summary.aggregates_written == 1
    assert summary.complete_after == 1
    assert summary.raw_rows == 2
    assert summary.filtered_rows == 2
    assert summary.dropped_rows == 0


def test_service_filters_raw_rows_before_normalize_and_reports_progress(tmp_path) -> None:
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    target_start = closed_end - 4 * 60 * 60_000 + 1
    _write_zip(raw_root, raw, "2026-06-29", "")
    _write_zip(
        raw_root,
        raw,
        "2026-06-30",
        "1577836799000,99,1,buy,old\n"
        f"{target_start + 1},100,1,buy,a\n"
        f"{target_start + 2},101.5,1,buy,b\n"
        f"{closed_end + 1},102,1,buy,future\n",
    )
    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=1,
        max_buckets_per_cycle=1,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        chunksize=10,
        progress_seconds=5,
    )
    events: list[tuple[str, dict]] = []

    summary = RangeBackfillService(
        request,
        progress_callback=lambda event, payload: events.append((event, dict(payload))),
    ).run_once(now_ms_value=now_ms)

    assert summary.status == "ok"
    assert summary.raw_rows == 4
    assert summary.filtered_rows == 2
    assert summary.dropped_rows == 2
    assert summary.trades_loaded == 2
    progress = [payload for event, payload in events if event == "chunk_progress"]
    assert progress
    assert progress[-1]["raw_rows"] == 4
    assert progress[-1]["filtered_rows"] == 2


def test_live_mode_can_skip_saving_raw_trades(tmp_path) -> None:
    request = RangeBackfillRequest(
        symbol="ETH-USDT-PERP",
        mode="live",
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        save_raw_trades=False,
    )

    service = RangeBackfillService(request)

    assert service.trade_store is None


def test_resource_limits_stop_cycle_before_marking_complete(tmp_path, monkeypatch) -> None:
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    target_start = closed_end - 4 * 60 * 60_000 + 1
    _write_zip(raw_root, raw, "2026-06-29", "")
    _write_zip(
        raw_root,
        raw,
        "2026-06-30",
        f"{target_start + 1},100,1,buy,a\n"
        f"{target_start + 2},101.5,1,buy,b\n",
    )
    sleeps: list[float] = []
    monkeypatch.setattr("src.market_data.backfill.service.time.sleep", lambda value: sleeps.append(value))
    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=1,
        max_buckets_per_cycle=1,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        save_raw_trades=False,
        chunksize=1,
        chunk_sleep_seconds=0.05,
        max_trades_per_cycle=1,
    )

    calls: list[tuple[int, int, int]] = []
    service = RangeBackfillService(request)

    def fake_replace_range(*, symbol, range_pct, time_range, rows):
        calls.append((time_range.start_time_ms, time_range.end_time_ms, len(rows)))
        return len(rows)

    service.range_bar_store.replace_range = fake_replace_range

    summary = service.run_once(now_ms_value=now_ms)

    assert summary.status == "partial"
    assert summary.aggregates_written == 0
    assert summary.complete_after == 0
    assert sleeps == [0.05]
    assert calls == [(target_start, target_start + 1, 0)]


def test_resource_limited_before_target_start_does_not_replace_range(tmp_path) -> None:
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    target_start = closed_end - 4 * 60 * 60_000 + 1
    _write_zip(raw_root, raw, "2026-06-29", "")
    _write_zip(raw_root, raw, "2026-06-30", f"{target_start - 100},100,1,buy,a\n")
    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=1,
        max_buckets_per_cycle=1,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        save_raw_trades=False,
        chunksize=1,
        max_trades_per_cycle=1,
    )
    service = RangeBackfillService(request)
    called = False

    def fake_replace_range(*, symbol, range_pct, time_range, rows):
        nonlocal called
        called = True
        return 0

    service.range_bar_store.replace_range = fake_replace_range

    summary = service.run_once(now_ms_value=now_ms)

    assert summary.status == "partial"
    assert summary.aggregates_written == 0
    assert called is False


def test_missing_anchor_day_skips_old_window_but_later_window_can_succeed(tmp_path) -> None:
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    bucket_ms = 4 * 60 * 60_000
    latest_start = closed_end - bucket_ms + 1
    _write_zip(raw_root, raw, "2026-06-29", "")
    _write_zip(
        raw_root,
        raw,
        "2026-06-30",
        f"{latest_start + 1},100,1,buy,a\n"
        f"{latest_start + 2},101.5,1,buy,b\n",
    )
    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=8,
        max_buckets_per_cycle=8,
        max_days_per_cycle=10_000,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        mode="prebuild",
    )

    summary = RangeBackfillService(request).run_once(now_ms_value=now_ms)

    assert summary.status == "partial"
    assert summary.aggregates_written > 0
    assert "2026-06-28" in summary.missing_raw_days
    assert summary.skipped_buckets_due_missing_raw > 0


def test_no_raw_available_is_no_progress_not_error(tmp_path) -> None:
    request = RangeBackfillRequest(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        raw_symbol="ETH-USDT-SWAP",
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=2,
        max_buckets_per_cycle=2,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        mode="prebuild",
    )

    summary = RangeBackfillService(request).run_once(now_ms_value=1782835200000)

    assert summary.status == "no_progress"
    assert summary.aggregates_written == 0
    assert summary.failed_downloads
    assert summary.hint == "raw OKX trades zip missing; run downloader or remove --no-download"


def test_live_current_day_archive_missing_exits_before_download_or_csv_read(
    tmp_path,
    monkeypatch,
) -> None:
    request = RangeBackfillRequest(
        symbol="ETH-USDT-PERP",
        exchange="okx",
        raw_symbol="ETH-USDT-SWAP",
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=1,
        max_buckets_per_cycle=1,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=tmp_path / "raw",
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=True,
        mode="live",
    )
    service = RangeBackfillService(request)
    monkeypatch.setattr(
        "src.market_data.backfill.service.OkxHistoricalTradeArchive.ensure_daily_file",
        lambda self, **kwargs: (_ for _ in ()).throw(
            AssertionError("current-day archive must not be downloaded")
        ),
    )
    monkeypatch.setattr(
        "src.market_data.backfill.service.iter_trade_csv_chunks",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("CSV reader must not run before every raw day is ready")
        ),
    )

    summary = service.run_once(now_ms_value=1782835200000)

    assert summary.status == "no_progress"
    assert summary.raw_rows == 0
    assert summary.trades_loaded == 0
    assert summary.aggregates_written == 0
    assert summary.missing_raw_days == ("2026-06-30",)
    assert summary.failed_downloads[0].endswith(
        "/20260630/ETH-USDT-SWAP-trades-2026-06-30.zip"
    )
