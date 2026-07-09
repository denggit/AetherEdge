from __future__ import annotations

import sqlite3
import zipfile
from decimal import Decimal

from src.market_data.backfill.coverage import current_closed_bucket_end_ms
from src.market_data.backfill.models import RangeBackfillRequest
from src.market_data.backfill.service import RangeBackfillService
from src.market_data.historical_trades.okx_archive import (
    OkxHistoricalTradeArchive,
    OkxHistoricalTradeDownloadError,
    okx_raw_symbol_from_canonical,
)
from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.storage import SqliteTradeStore


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
    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=1,
        max_buckets_per_cycle=1,
        market_db_path=market_db,
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
    )

    service = RangeBackfillService(request)
    summary = service.run_once(now_ms_value=now_ms)

    assert request.save_raw_trades is False
    assert service.trade_store is None
    assert summary.status == "ok"
    assert summary.aggregates_written == 1
    assert summary.complete_after == 1
    assert summary.raw_rows == 2
    assert summary.filtered_rows == 2
    assert summary.dropped_rows == 0
    assert summary.selected_archive_dates == ("2026-06-29", "2026-06-30")
    assert summary.target_trade_count == 2
    assert summary.candidate_range_bars == 1
    assert summary.candidate_aggregates == 1
    assert summary.filtered_reason_if_zero is None
    status = service.status_store.read()
    assert status["selected_archive_dates"] == ["2026-06-29", "2026-06-30"]
    assert status["aggregates_written"] == 1
    with sqlite3.connect(market_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM trade_coverage").fetchone()[0] == 0


def test_service_explicitly_enabled_raw_trade_persistence_still_works(
    tmp_path,
    caplog,
) -> None:
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    market_db = tmp_path / "market.sqlite3"
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
        market_db_path=market_db,
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        save_raw_trades=True,
    )

    service = RangeBackfillService(request)
    summary = service.run_once(now_ms_value=now_ms)

    assert service.trade_store is not None
    assert summary.aggregates_written == 1
    with sqlite3.connect(market_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM trade_coverage").fetchone()[0] == 0
    assert (
        "Raw trades persistence enabled; market DB may grow quickly"
        in caplog.text
    )


def test_service_repairs_existing_degraded_bucket_to_complete(tmp_path) -> None:
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
    service = RangeBackfillService(request)
    service.checkpoint_store.save_completed_aggregate(
        exchange="okx",
        aggregate=RangeBarAggregate(
            symbol=symbol,
            range_pct=Decimal("0.01"),
            bucket_start_ms=target_start,
            bucket_end_ms=closed_end,
            bar_count=25,
            first_open=Decimal("100"),
            last_close=Decimal("101"),
            high=Decimal("101"),
            low=Decimal("100"),
            buy_notional_sum=Decimal("1"),
            sell_notional_sum=Decimal("1"),
            delta_notional_sum=Decimal("0"),
            notional_sum=Decimal("2"),
        ),
        coverage_status=RangeCoverageStatus.RECOVERED_DEGRADED_MINOR.value,
        missing_gap_ms=87_580,
        completed_at_ms=closed_end,
    )

    before = service.check_coverage(now_ms_value=now_ms)
    assert before.required_window_missing_buckets[0].reason == "degraded_bucket"

    summary = service.run_once(now_ms_value=now_ms)

    assert summary.aggregates_written == 1
    history = service.checkpoint_store.load_complete_history(
        exchange="okx",
        symbol=symbol,
        range_pct="0.01",
        before_bucket_end_ms=closed_end + 1,
        limit=1,
    )
    assert len(history) == 1
    assert history[0].coverage_status == "COMPLETE"
    assert history[0].missing_gap_ms == 0


def test_zero_aggregate_reports_filtered_reason_and_no_progress(tmp_path) -> None:
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
        f"{target_start + 2},100,1,buy,b\n",
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

    service = RangeBackfillService(request)
    summary = service.run_once(now_ms_value=now_ms)

    assert summary.status == "no_progress"
    assert summary.aggregates_written == 0
    assert summary.filtered_reason_if_zero == "no_range_bar_closed_in_target_window"
    assert service.status_store.read()["filtered_reason_if_zero"] == (
        "no_range_bar_closed_in_target_window"
    )


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

    # 2026-06-30 15:00 UTC is still the 2026-06-30 UTC+8 archive day.
    summary = service.run_once(now_ms_value=1782831600000)

    assert summary.status == "archive_not_ready"
    assert summary.raw_rows == 0
    assert summary.trades_loaded == 0
    assert summary.aggregates_written == 0
    assert summary.missing_raw_days == ("2026-06-30",)
    assert summary.failed_downloads[0].endswith(
        "/20260630/ETH-USDT-SWAP-trades-2026-06-30.zip"
    )


def test_live_just_closed_okx_archive_404_is_archive_not_ready(
    tmp_path, monkeypatch
) -> None:
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    _write_zip(raw_root, raw, "2026-06-30", "")
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
        allow_download=True,
        mode="live",
    )
    original = OkxHistoricalTradeArchive.ensure_daily_file

    def fake_ensure(self, *, day, **kwargs):
        if day.isoformat() == "2026-07-01":
            raise OkxHistoricalTradeDownloadError(
                url="https://example.test/2026-07-01.zip",
                day=day,
                status=404,
            )
        return original(self, day=day, **kwargs)

    monkeypatch.setattr(OkxHistoricalTradeArchive, "ensure_daily_file", fake_ensure)

    # 2026-07-01 16:00 UTC has just closed the 2026-07-01 UTC+8 archive.
    summary = RangeBackfillService(request).run_once(
        now_ms_value=1782921600000
    )

    assert summary.status == "archive_not_ready"
    assert summary.missing_raw_days == ("2026-07-01",)
    assert summary.filtered_reason_if_zero == "archive_not_ready"
    assert summary.raw_rows == 0
    assert summary.trades_loaded == 0
    assert summary.aggregates_written == 0


def test_service_repairs_historical_isolated_gap_when_current_day_archive_not_ready(
    tmp_path, monkeypatch,
) -> None:
    """When two gaps exist — one historical with an available archive and one
    current-day whose archive date equals the current OKX archive day — the
    combined window fails with _live_archive_is_not_ready.  The service must
    fall back to individual gaps and successfully repair the historical gap
    instead of skipping all work.

    This reproduces the live scenario where one isolated gap sits in the
    rolling window and the latest-closed bucket needs an archive that has
    not been published yet."""
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    bucket_ms = 4 * 60 * 60_000
    now_ms = 1782950400000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    gap_historical_end = closed_end - 2 * bucket_ms
    gap_historical_start = gap_historical_end - bucket_ms + 1
    gap_current_end = closed_end

    # Provide archive files.  Compute which OKX days are needed by
    # looking at what _run_build_window chooses for each gap.
    from src.market_data.backfill.coverage import previous_utc_day_start_ms
    from src.market_data.backfill.service import _BuildWindowResult
    from src.market_data.historical_trades.okx_archive import (
        iter_okx_archive_dates_for_utc_range,
    )
    # Historical gap archive days.
    hist_anchor = previous_utc_day_start_ms(gap_historical_start)
    hist_okx_days = tuple(
        iter_okx_archive_dates_for_utc_range(hist_anchor, gap_historical_end)
    )
    # Write trades into the earliest historical archive day so the
    # individual fallback can find them.
    _write_zip(
        raw_root, raw, hist_okx_days[0].isoformat(), "",
    )
    _write_zip(
        raw_root, raw, hist_okx_days[-1].isoformat(),
        f"{gap_historical_start + 1},100,1,buy,a\n"
        f"{gap_historical_start + 2},101.5,1,buy,b\n",
    )

    market_db = tmp_path / "market.sqlite3"
    SqliteTradeStore(market_db)
    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=3,
        lookback_buckets=5,
        max_buckets_per_cycle=3,
        market_db_path=market_db,
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        mode="live",
    )

    service = RangeBackfillService(request)

    # ------------------------------------------------------------------
    # Stub _ensure_raw_days so the combined window fails with a
    # live-archive-not-ready error, but individual historical gaps
    # succeed.
    # ------------------------------------------------------------------
    original_ensure = service._ensure_raw_days
    ensure_call_gaps: list[tuple[int, int]] = []  # (earliest_start, latest_end)

    def staged_ensure(*, raw_symbol, days, skipped_buckets):
        if days:
            latest_day = days[-1]
        else:
            latest_day = None
        gap_end = None
        # Capture the window for assertions.
        # The combined window spans both gaps, individual windows span one.
        ensure_call_gaps.append(
            (days[0].toordinal() if days else 0,
             days[-1].toordinal() if days else 0)
        )
        result = original_ensure(
            raw_symbol=raw_symbol, days=days,
            skipped_buckets=skipped_buckets,
        )
        # If the window includes the current-day archive date
        # (the last day in the range), inject a live-archive-not-ready
        # failure.  This simulates the combined window failing because
        # the current-day archive is not published.
        if latest_day is not None:
            current_archive_day = service._current_archive_date()
            if latest_day >= current_archive_day:
                day_iso = latest_day.isoformat()
                service._archive_not_ready_days.add(day_iso)
                return _BuildWindowResult(
                    missing_raw_days=(day_iso,),
                    failed_downloads=(f"https://example.test/{day_iso}.zip",),
                    skipped_buckets_due_missing_raw=skipped_buckets,
                )
        return result

    monkeypatch.setattr(service, "_ensure_raw_days", staged_ensure)

    # Pre-populate the middle bucket as COMPLETE so only 2 gaps remain:
    # the historical isolated gap and the current-day bucket.
    middle_end = closed_end - bucket_ms
    middle_start = middle_end - bucket_ms + 1
    service.checkpoint_store.save_completed_aggregate(
        exchange="okx",
        aggregate=RangeBarAggregate(
            symbol=symbol, range_pct=Decimal("0.01"),
            bucket_start_ms=middle_start, bucket_end_ms=middle_end,
            bar_count=10, first_open=Decimal("100"), last_close=Decimal("101"),
            high=Decimal("101"), low=Decimal("100"),
            buy_notional_sum=Decimal("10"), sell_notional_sum=Decimal("5"),
            delta_notional_sum=Decimal("5"), notional_sum=Decimal("15"),
        ),
        coverage_status=RangeCoverageStatus.COMPLETE.value,
        completed_at_ms=middle_end,
    )

    summary = service.run_once(now_ms_value=now_ms)

    # ------------------------------------------------------------------
    # Assertion 2: summary reports the historical gap was written.
    # ------------------------------------------------------------------
    assert summary.aggregates_written > 0, (
        f"Expected >0 aggregates; got aggregates_written={summary.aggregates_written} "
        f"status={summary.status}"
    )

    # ------------------------------------------------------------------
    # Assertion 3: only the current-day archive-not-ready bucket remains.
    # ------------------------------------------------------------------
    assert summary.missing_after == 1, (
        f"Expected missing_after=1 (current-day gap only); "
        f"got missing_after={summary.missing_after} "
        f"complete_after={summary.complete_after}"
    )

    # ------------------------------------------------------------------
    # Assertion 4: historical gap is no longer in the required-window
    #              missing list after the repair.
    # ------------------------------------------------------------------
    after_coverage = service.check_coverage(now_ms_value=now_ms)
    after_missing_ends = {
        gap.bucket_end_ms
        for gap in after_coverage.required_window_missing_buckets
    }
    assert gap_historical_end not in after_missing_ends, (
        f"Historical gap {gap_historical_end} should NOT appear in "
        f"post-repair missing buckets; found: {after_missing_ends}"
    )
    assert gap_current_end in after_missing_ends, (
        f"Current-day gap {gap_current_end} SHOULD still be missing; "
        f"found: {after_missing_ends}"
    )

    # ------------------------------------------------------------------
    # Assertion 5: checkpoint store has the historical bucket as COMPLETE.
    # ------------------------------------------------------------------
    completed = service.checkpoint_store.load_completed_aggregate(
        exchange="okx",
        symbol=symbol,
        range_pct="0.01",
        bucket_end_ms=gap_historical_end,
    )
    assert completed is not None, (
        f"Historical bucket {gap_historical_end} not found in "
        f"completed_range_aggregates after repair"
    )
    assert completed.coverage_status == RangeCoverageStatus.COMPLETE.value, (
        f"Historical bucket coverage_status={completed.coverage_status}, "
        f"expected COMPLETE"
    )
    assert completed.rf_bar_count > 0, (
        f"Historical bucket rf_bar_count={completed.rf_bar_count}, expected >0"
    )

    # ------------------------------------------------------------------
    # Assertion 6: last_repaired_bucket_end_ms reflects the historical gap.
    # ------------------------------------------------------------------
    status = service.status_store.read()
    assert status is not None
    repaired = status.get("last_repaired_bucket_end_ms")
    assert repaired == gap_historical_end, (
        f"last_repaired_bucket_end_ms={repaired} != historical gap "
        f"end={gap_historical_end}"
    )

    # ------------------------------------------------------------------
    # Assertion 2: summary reports the historical gap was written.
    # ------------------------------------------------------------------
    assert summary.aggregates_written > 0, (
        f"Expected >0 aggregates; got aggregates_written={summary.aggregates_written} "
        f"status={summary.status}"
    )

    # ------------------------------------------------------------------
    # Assertion 3: only the current-day archive-not-ready bucket remains.
    # ------------------------------------------------------------------
    assert summary.missing_after == 1, (
        f"Expected missing_after=1 (current-day gap only); "
        f"got missing_after={summary.missing_after} "
        f"complete_after={summary.complete_after}"
    )

    # ------------------------------------------------------------------
    # Assertion 4: historical gap is no longer in the required-window
    #              missing list after the repair.
    # ------------------------------------------------------------------
    after_coverage = service.check_coverage(now_ms_value=now_ms)
    after_missing_ends = {
        gap.bucket_end_ms
        for gap in after_coverage.required_window_missing_buckets
    }
    assert gap_historical_end not in after_missing_ends, (
        f"Historical gap {gap_historical_end} should NOT appear in "
        f"post-repair missing buckets; found: {after_missing_ends}"
    )
    assert gap_current_end in after_missing_ends, (
        f"Current-day gap {gap_current_end} SHOULD still be missing; "
        f"found: {after_missing_ends}"
    )

    # ------------------------------------------------------------------
    # Assertion 5: checkpoint store has the historical bucket as COMPLETE.
    # ------------------------------------------------------------------
    completed = service.checkpoint_store.load_completed_aggregate(
        exchange="okx",
        symbol=symbol,
        range_pct="0.01",
        bucket_end_ms=gap_historical_end,
    )
    assert completed is not None, (
        f"Historical bucket {gap_historical_end} not found in "
        f"completed_range_aggregates after repair"
    )
    assert completed.coverage_status == RangeCoverageStatus.COMPLETE.value, (
        f"Historical bucket coverage_status={completed.coverage_status}, "
        f"expected COMPLETE"
    )
    assert completed.rf_bar_count > 0, (
        f"Historical bucket rf_bar_count={completed.rf_bar_count}, expected >0"
    )

    # ------------------------------------------------------------------
    # Assertion 6: last_repaired_bucket_end_ms reflects the historical gap.
    # ------------------------------------------------------------------
    status = service.status_store.read()
    assert status is not None
    repaired = status.get("last_repaired_bucket_end_ms")
    assert repaired == gap_historical_end, (
        f"last_repaired_bucket_end_ms={repaired} != historical gap "
        f"end={gap_historical_end}"
    )
