from __future__ import annotations

from decimal import Decimal

from src.market_data.backfill.coverage import current_closed_bucket_end_ms
from src.market_data.backfill.models import RangeBackfillRequest
from src.market_data.backfill.service import RangeBackfillService, _BuildWindowResult
from src.market_data.historical_trades.okx_archive import (
    OkxHistoricalTradeArchive,
    OkxHistoricalTradeDownloadError,
    okx_raw_symbol_from_canonical,
)
from src.market_data.models import RangeBarAggregate, RangeCoverageStatus
from src.market_data.storage import SqliteTradeStore


def _write_zip(root, raw_symbol: str, day: str, rows: str) -> None:
    import zipfile
    path = root / raw_symbol / f"{raw_symbol}-trades-{day}.zip"
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{raw_symbol}-trades-{day}.csv", "ts,px,sz,side,trade_id\n" + rows)


# ── seek-phase resource-limit guard tests ────────────────────────────


def test_live_mode_resource_limit_does_not_block_historical_gap_fill(
    tmp_path,
) -> None:
    """In live mode with a tight resource limit, a historical archive-ready
    gap must still be filled because the seek-phase guard skips normal
    per-cycle caps until the target bucket end is reached.

    Without this guard the worker would stop at max_trades_per_cycle,
    return resource_limit_before_target_complete, and restart from the
    beginning every cycle — the gap would never be filled."""
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    now_ms = 1782835200000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    target_start = closed_end - 4 * 60 * 60_000 + 1

    # Write enough pre-target trades to exhaust a very low limit.
    # Each chunk holds 1 trade so that the limit fires quickly.
    pre_trades = "\n".join(
        f"{target_start - 10_000 - i * 1000},100,1,buy,pre_{i}"
        for i in range(5)
    )
    # Include a trade near the bucket end so processed_through_ms reaches
    # target_bucket_end_ms — this is what happens in real archives.
    target_trades = (
        f"{target_start + 1000},100,1,buy,a\n"
        f"{target_start + 2000},101.5,1,buy,b\n"
        f"{closed_end},101.5,1,buy,near_end\n"
    )
    _write_zip(raw_root, raw, "2026-06-29", pre_trades)
    _write_zip(raw_root, raw, "2026-06-30", target_trades)

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
        chunksize=1,
        max_trades_per_cycle=2,
    )

    service = RangeBackfillService(request)
    summary = service.run_once(now_ms_value=now_ms)

    # With the seek-phase guard the target bucket MUST be filled.
    assert summary.aggregates_written == 1, (
        f"Expected 1 aggregate; got aggregates_written={summary.aggregates_written} "
        f"status={summary.status} filtered_reason_if_zero={summary.filtered_reason_if_zero}"
    )
    assert summary.target_trade_count > 0
    assert summary.candidate_aggregates >= 1
    # "partial" is acceptable: the resource limit fires *after* the target
    # bucket is reached (seek phase completed), but the aggregate is
    # correctly written.  The key invariant is aggregates_written == 1.
    assert summary.status in ("partial", "ok")
    assert summary.complete_after == 1
    assert summary.filtered_reason_if_zero is None
    assert summary.reached_target_end is True, (
        f"Expected reached_target_end=True; got {summary.reached_target_end} "
        f"processed_through_ms={summary.processed_through_ms}"
    )

    # Verify the checkpoint is COMPLETE.
    completed = service.checkpoint_store.load_completed_aggregate(
        exchange="okx",
        symbol=symbol,
        range_pct="0.01",
        bucket_end_ms=closed_end,
    )
    assert completed is not None
    assert completed.coverage_status == RangeCoverageStatus.COMPLETE.value


def test_live_mode_seek_phase_does_not_force_current_day_archive_not_ready(
    tmp_path,
    monkeypatch,
) -> None:
    """The seek-phase guard must NOT try to force-process a gap whose
    archive day is not yet published.  The archive-not-ready check in
    _ensure_raw_days still takes precedence over resource-limit logic."""
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

    summary = service.run_once(now_ms_value=1782831600000)

    # Must still be archive_not_ready — the seek-phase guard is
    # never reached because _ensure_raw_days returns early.
    assert summary.status == "archive_not_ready"
    assert summary.raw_rows == 0
    assert summary.trades_loaded == 0
    assert summary.aggregates_written == 0
    assert summary.missing_raw_days == ("2026-06-30",)
    assert summary.processed_through_ms is None
    assert summary.reached_target_start is False
    assert summary.reached_target_end is False


def test_live_mode_seek_guard_preserves_reached_target_diagnostics(
    tmp_path,
) -> None:
    """When live mode successfully fills a historical gap, the diagnostic
    fields (processed_through_ms, reached_target_start,
    reached_target_end) must be populated."""
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
        f"{target_start + 2},101.5,1,buy,b\n"
        f"{closed_end},101.5,1,buy,near_end\n",
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
        mode="live",
        max_trades_per_cycle=300_000,
    )

    service = RangeBackfillService(request)
    summary = service.run_once(now_ms_value=now_ms)

    assert summary.aggregates_written == 1
    assert summary.processed_through_ms is not None
    assert summary.processed_through_ms >= closed_end, (
        f"processed_through_ms={summary.processed_through_ms} < "
        f"target_bucket_end_ms={closed_end}"
    )
    assert summary.reached_target_start is True
    assert summary.reached_target_end is True
    assert summary.resource_limit_phase is None  # not limited


def test_historical_isolated_gap_reports_last_repaired_bucket_end_ms(
    tmp_path,
    monkeypatch,
) -> None:
    """Strengthened assertion: the historical isolated-gap fallback must
    write COMPLETE, set last_repaired_bucket_end_ms, and populate the new
    diagnostic fields."""
    symbol = "ETH-USDT-PERP"
    raw = okx_raw_symbol_from_canonical(symbol)
    raw_root = tmp_path / "raw"
    bucket_ms = 4 * 60 * 60_000
    now_ms = 1782950400000
    closed_end = current_closed_bucket_end_ms(now_ms, "4h")
    gap_historical_end = closed_end - 2 * bucket_ms
    gap_historical_start = gap_historical_end - bucket_ms + 1
    gap_current_end = closed_end

    from src.market_data.backfill.coverage import previous_utc_day_start_ms
    from src.market_data.historical_trades.okx_archive import (
        iter_okx_archive_dates_for_utc_range,
    )
    hist_anchor = previous_utc_day_start_ms(gap_historical_start)
    hist_okx_days = tuple(
        iter_okx_archive_dates_for_utc_range(hist_anchor, gap_historical_end)
    )
    _write_zip(raw_root, raw, hist_okx_days[0].isoformat(), "")
    _write_zip(
        raw_root,
        raw,
        hist_okx_days[-1].isoformat(),
        f"{gap_historical_start + 1},100,1,buy,a\n"
        f"{gap_historical_start + 2},101.5,1,buy,b\n"
        f"{gap_historical_end},101.5,1,buy,near_end\n",
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
    original_ensure = service._ensure_raw_days

    def staged_ensure(*, raw_symbol, days, skipped_buckets):
        result = original_ensure(
            raw_symbol=raw_symbol, days=days,
            skipped_buckets=skipped_buckets,
        )
        if days:
            latest_day = days[-1]
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

    # Strengthened assertions
    assert summary.aggregates_written > 0
    assert summary.missing_after == 1
    assert summary.status in ("partial", "ok")

    # Historical gap is COMPLETE
    completed = service.checkpoint_store.load_completed_aggregate(
        exchange="okx", symbol=symbol, range_pct="0.01",
        bucket_end_ms=gap_historical_end,
    )
    assert completed is not None
    assert completed.coverage_status == RangeCoverageStatus.COMPLETE.value
    assert completed.rf_bar_count > 0

    # Diagnostic fields are populated for the successful gap
    assert summary.processed_through_ms is not None
    assert summary.reached_target_end is True

    # last_repaired_bucket_end_ms reflects the historical gap
    status = service.status_store.read()
    assert status is not None
    assert status.get("last_repaired_bucket_end_ms") == gap_historical_end

    # Current-day gap still missing
    after_coverage = service.check_coverage(now_ms_value=now_ms)
    after_missing_ends = {
        gap.bucket_end_ms
        for gap in after_coverage.required_window_missing_buckets
    }
    assert gap_historical_end not in after_missing_ends
    assert gap_current_end in after_missing_ends
