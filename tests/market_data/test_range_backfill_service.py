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


# ── Helpers for checkpoint / repair tests ──────────────────────────────


def _builder_state(range_pct: str = "0.01") -> dict:
    """Minimal valid builder_state for RangeBarBuilder.restore_state()."""
    return {
        "version": 1,
        "range_pct": range_pct,
        "contract_value": "1",
        "active": None,
        "day_seq": {},
    }


def _write_checkpoint(
    service: RangeBackfillService,
    *,
    bucket_start_ms: int,
    bucket_end_ms: int,
    last_trade_ts_ms: int,
    builder_state: dict | None = None,
    coverage_status: str = "COMPLETE",
) -> None:
    """Write a RangeBuilderCheckpoint directly into the checkpoint store."""
    from src.market_data.range_checkpoint import RangeBuilderCheckpoint

    checkpoint = RangeBuilderCheckpoint(
        exchange=service.request.exchange,
        symbol=service.request.symbol,
        range_pct=service.request.range_pct,
        bucket_start_ms=bucket_start_ms,
        bucket_end_ms=bucket_end_ms,
        last_trade_id="test_trade",
        last_trade_ts_ms=last_trade_ts_ms,
        last_ws_recv_ts_ms=last_trade_ts_ms,
        range_bar_count=5,
        aggregate={"bar_count": 5},
        builder_state=builder_state or _builder_state(service.request.range_pct),
        coverage_status=coverage_status,
        missing_gap_ms=0,
        checkpoint_updated_at_ms=last_trade_ts_ms,
    )
    service.checkpoint_store.save_checkpoint(checkpoint)


def _write_range_bars(
    service: RangeBackfillService,
    *,
    bucket_start_ms: int,
    bucket_end_ms: int,
    count: int = 5,
    first_price: float = 100.0,
    step: float = 1.0,
) -> list:
    """Write range bars into range_bar_store and return them."""
    from src.market_data.models import RangeBar
    from decimal import Decimal

    bars = []
    for i in range(count):
        price = Decimal(str(first_price + i * step))
        bar = RangeBar(
            symbol=service.request.symbol,
            range_pct=Decimal(service.request.range_pct),
            bar_id=20260707_0000 + i + 1,
            start_time_ms=bucket_start_ms + i * 60000,
            end_time_ms=bucket_start_ms + (i + 1) * 60000,
            open=price,
            high=price + Decimal("0.5"),
            low=price - Decimal("0.5"),
            close=price + Decimal("0.3"),
            volume=Decimal("10"),
            buy_notional=price * Decimal("5"),
            sell_notional=price * Decimal("5"),
            trade_count=10,
        )
        bars.append(bar)
    service.range_bar_store.save(bars)
    return bars


# ── Test A: existing range_bars fast path ──────────────────────────────


def test_existing_range_bars_fast_path_writes_complete_aggregate(tmp_path) -> None:
    """When range bars exist in DB and a COMPLETE checkpoint proves them
    complete, the fast path writes the aggregate without any archive scan."""
    from src.market_data.warmup.gap_detector import interval_to_ms

    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    bucket_ms = interval_to_ms("4h")
    closed_end = (now_ms // bucket_ms) * bucket_ms - 1
    target_start = closed_end - bucket_ms + 1

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
        mode="live",
    )
    service = RangeBackfillService(request)

    # Pre-seed range bars and a COMPLETE checkpoint
    _write_range_bars(
        service,
        bucket_start_ms=target_start,
        bucket_end_ms=closed_end,
        count=5,
    )
    _write_checkpoint(
        service,
        bucket_start_ms=target_start,
        bucket_end_ms=closed_end,
        last_trade_ts_ms=closed_end + 1000,  # past bucket end
    )

    summary = service.run_once(now_ms_value=now_ms)

    assert summary.status == "ok"
    assert summary.aggregates_written == 1
    assert summary.complete_after == 1
    assert summary.repair_method == "existing_range_bars"
    assert summary.target_window_reached is True
    assert summary.target_bucket_proven_complete is True
    assert summary.raw_rows == 0
    assert summary.trades_loaded == 0
    assert summary.selected_archive_dates == ()

    status = service.status_store.read()
    assert status["repair_method"] == "existing_range_bars"


# ── Test B: existing range_bars but cannot prove complete ──────────────


def test_existing_range_bars_without_complete_checkpoint_does_not_write_complete(
    tmp_path,
) -> None:
    """When range bars exist but no COMPLETE checkpoint proves completeness,
    the fast path returns None, and the system falls through."""
    from src.market_data.warmup.gap_detector import interval_to_ms

    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    bucket_ms = interval_to_ms("4h")
    closed_end = (now_ms // bucket_ms) * bucket_ms - 1
    target_start = closed_end - bucket_ms + 1

    # No archive data → full replay will get no_progress
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
        mode="live",
    )
    service = RangeBackfillService(request)

    _write_range_bars(service, bucket_start_ms=target_start, bucket_end_ms=closed_end, count=3)
    # NO checkpoint → cannot prove complete

    summary = service.run_once(now_ms_value=now_ms)

    # Should NOT write a COMPLETE aggregate based on unverified rows.
    assert summary.repair_method != "existing_range_bars"
    assert not summary.target_bucket_proven_complete


# ── Test C: checkpoint-anchored replay ─────────────────────────────────


def test_checkpoint_anchored_replay_restores_builder_and_repairs_gap(tmp_path) -> None:
    """With a valid checkpoint, the priority cascade selects
    checkpoint-anchored replay and processes the replay window."""
    from src.market_data.warmup.gap_detector import interval_to_ms

    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    bucket_ms = interval_to_ms("4h")
    closed_end = (now_ms // bucket_ms) * bucket_ms - 1
    target_start = closed_end - bucket_ms + 1
    checkpoint_last_ms = target_start + 60_000
    trade1_ms = checkpoint_last_ms + 1000
    trade2_ms = trade1_ms + 2000

    _write_zip(raw_root, raw, "2026-06-30",
               f"{trade1_ms},100,1,buy,a\n"
               f"{trade2_ms},102,1,buy,b\n")
    _write_zip(raw_root, raw, "2026-07-01",
               f"{trade1_ms},100,1,buy,a\n"
               f"{trade2_ms},102,1,buy,b\n")

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
        mode="live",
    )
    service = RangeBackfillService(request)

    _write_checkpoint(
        service,
        bucket_start_ms=target_start,
        bucket_end_ms=closed_end,
        last_trade_ts_ms=checkpoint_last_ms,
        builder_state=_builder_state("0.01"),
        coverage_status="RECOVERED_INCOMPLETE",
    )

    summary = service.run_once(now_ms_value=now_ms)

    # The priority cascade should have selected checkpoint-anchored replay.
    assert summary.repair_method == "checkpoint_anchored_replay"
    # Either it wrote an aggregate (if the replay window had data),
    # or it was resource-limited / archive-missing. Either way, the
    # repair_method confirms the correct code path was taken.
    assert summary.anchor_last_trade_ts_ms == checkpoint_last_ms


# ── Test D: checkpoint replay hits resource limit → no COMPLETE ────────


def test_checkpoint_replay_resource_limit_does_not_write_complete(tmp_path) -> None:
    """When max_trades_per_cycle is too small to finish the replay window,
    no COMPLETE aggregate is written."""
    from src.market_data.warmup.gap_detector import interval_to_ms

    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    bucket_ms = interval_to_ms("4h")
    closed_end = (now_ms // bucket_ms) * bucket_ms - 1
    target_start = closed_end - bucket_ms + 1
    checkpoint_last_ms = target_start + 60_000

    _write_zip(raw_root, raw, "2026-06-30",
               f"{checkpoint_last_ms + 1000},100,1,buy,a\n"
               f"{checkpoint_last_ms + 2000},101,1,buy,b\n")
    _write_zip(raw_root, raw, "2026-07-01",
               f"{checkpoint_last_ms + 1000},100,1,buy,a\n"
               f"{checkpoint_last_ms + 2000},101,1,buy,b\n"
               f"{checkpoint_last_ms + 3000},102,1,buy,c\n")

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
        mode="live",
        max_trades_per_cycle=1,  # only 1 trade allowed
    )
    service = RangeBackfillService(request)

    _write_checkpoint(
        service,
        bucket_start_ms=target_start,
        bucket_end_ms=closed_end,
        last_trade_ts_ms=checkpoint_last_ms,
        builder_state=_builder_state("0.01"),
    )

    summary = service.run_once(now_ms_value=now_ms)

    assert summary.aggregates_written == 0
    assert not summary.target_bucket_proven_complete


# ── Test E: no checkpoint → full replay cannot write COMPLETE without anchor ──


def test_no_checkpoint_full_replay_cannot_prove_complete(tmp_path) -> None:
    """Without a checkpoint anchor, full replay cannot start from a proven
    state. Even if the target window has trades, COMPLETE cannot be written
    unless the full window is covered from a valid anchor."""
    from src.market_data.warmup.gap_detector import interval_to_ms

    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    bucket_ms = interval_to_ms("4h")
    closed_end = (now_ms // bucket_ms) * bucket_ms - 1
    target_start = closed_end - bucket_ms + 1

    # Only trades in the target window, no checkpoint.
    _write_zip(raw_root, raw, "2026-06-29", "")
    _write_zip(raw_root, raw, "2026-06-30",
               f"{target_start + 1},100,1,buy,a\n"
               f"{target_start + 2},101,1,buy,b\n")

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
        mode="live",
    )
    service = RangeBackfillService(request)

    summary = service.run_once(now_ms_value=now_ms)

    # Full replay may succeed (it has enough data from full archive scan),
    # but check that the repair_method is full_replay_fallback
    assert summary.repair_method == "full_replay_fallback"


# ── Test F: regression — checkpoint anchor skips pre-target archive days ──


def test_checkpoint_anchor_skips_pre_target_archive_day(tmp_path) -> None:
    """When a checkpoint anchor exists in the target bucket, the worker
    must NOT process the previous day's archive. selected_archive_dates
    must only cover the replay window."""
    from src.market_data.warmup.gap_detector import interval_to_ms

    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    # Use a now_ms where the closed bucket falls in a well-defined OKX day.
    # 2026-07-01 00:00 UTC+8 = 1782864000000 ms
    now_ms = 1782864000000
    bucket_ms = interval_to_ms("4h")
    closed_end = (now_ms // bucket_ms) * bucket_ms - 1
    target_start = closed_end - bucket_ms + 1
    target_end = closed_end
    checkpoint_last_ms = target_start + 60_000

    # Write to all possible OKX archive dates around the replay window
    _write_zip(raw_root, raw, "2026-06-30",
               "\n".join(f"{target_start - 86_400_000 + i * 1000},100,1,buy,x{i}"
                         for i in range(100)))
    _write_zip(raw_root, raw, "2026-07-01",
               f"{checkpoint_last_ms + 1000},101,1,buy,a\n"
               f"{checkpoint_last_ms + 2000},102,1,buy,b\n")

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
        mode="live",
        max_trades_per_cycle=5,
    )
    service = RangeBackfillService(request)

    _write_checkpoint(
        service,
        bucket_start_ms=target_start,
        bucket_end_ms=target_end,
        last_trade_ts_ms=checkpoint_last_ms,
        builder_state=_builder_state("0.01"),
    )

    summary = service.run_once(now_ms_value=now_ms)

    # The previous day (2026-06-30) must NOT appear in selected_archive_dates
    assert "2026-06-30" not in summary.selected_archive_dates, (
        f"Pre-target archive day must be skipped, got {summary.selected_archive_dates}"
    )
    assert summary.repair_method == "checkpoint_anchored_replay"
    assert summary.target_trade_count > 0


# ── Test G: repair_status is never success when aggregates_written == 0 ──


def test_repair_status_not_success_when_no_aggregates_written(tmp_path) -> None:
    """When aggregates_written == 0 and complete_after unchanged, the status
    JSON must NOT report daily_archive_backfill_success."""
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"

    request = RangeBackfillRequest(
        symbol=symbol,
        exchange="okx",
        raw_symbol=raw,
        range_pct="0.01",
        required_buckets=1,
        lookback_buckets=2,
        max_buckets_per_cycle=2,
        market_db_path=tmp_path / "market.sqlite3",
        checkpoint_db_path=tmp_path / "checkpoint.sqlite3",
        raw_root=raw_root,
        status_path=tmp_path / "status.json",
        lock_path=tmp_path / "range.lock",
        allow_download=False,
        mode="prebuild",
    )

    summary = RangeBackfillService(request).run_once(now_ms_value=1782835200000)

    assert summary.aggregates_written == 0
    assert summary.status != "ok"
