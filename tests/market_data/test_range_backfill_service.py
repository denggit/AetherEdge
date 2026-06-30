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

    summary = RangeBackfillService(request).run_once(now_ms_value=now_ms)

    assert summary.status == "partial"
    assert summary.aggregates_written == 0
    assert summary.complete_after == 0
    assert sleeps == [0.05]
