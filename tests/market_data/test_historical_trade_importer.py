from __future__ import annotations

import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

from src.market_data.historical_trades import HistoricalTradeImportService
from src.market_data.historical_trades import importer as importer_module
from src.market_data.models import TimeRange
from src.market_data.storage import SqliteTradeStore
from src.platform.exchanges.okx.historical_data import DownloadedArchive, OkxHistoricalTradesArchiveClient


H4 = 4 * 60 * 60_000
SYMBOL = "ETH-USDT-PERP"
RAW_SYMBOL = "ETH-USDT-SWAP"


def _ms(year: int, month: int, day: int, hour: int = 0) -> int:
    return int(datetime(year, month, day, hour, tzinfo=UTC).timestamp() * 1000)


def _raw_zip_path(raw_root: Path, date_text: str) -> Path:
    return raw_root / "trades" / RAW_SYMBOL / f"{RAW_SYMBOL}-trades-{date_text}.zip"


def _rows_for_bucket(bucket_start: int, *, sparse: bool = False) -> list[dict[str, object]]:
    if sparse:
        offsets = [1_000, H4 - 1_000]
    else:
        offsets = [
            1_000,
            20 * 60_000,
            40 * 60_000,
            60 * 60_000,
            80 * 60_000,
            100 * 60_000,
            120 * 60_000,
            140 * 60_000,
            160 * 60_000,
            180 * 60_000,
            200 * 60_000,
            220 * 60_000,
            H4 - 1_000,
        ]
    return [
        {
            "tradeId": f"raw_{i}",
            "px": "100",
            "sz": "1",
            "side": "buy",
            "ts": bucket_start + offset,
        }
        for i, offset in enumerate(offsets)
    ]


def _write_raw_zip(raw_root: Path, date_text: str, rows: list[dict[str, object]]) -> Path:
    path = _raw_zip_path(raw_root, date_text)
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0].keys())
    lines = [",".join(columns)]
    lines.extend(",".join(str(row[col]) for col in columns) for row in rows)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("trades.csv", "\n".join(lines) + "\n")
    return path


def _coverage_rows(path: Path) -> list[tuple]:
    import sqlite3

    with sqlite3.connect(path) as conn:
        return conn.execute(
            "SELECT symbol, start_time_ms, end_time_ms, source FROM trade_coverage ORDER BY start_time_ms"
        ).fetchall()


def test_importer_source_does_not_contain_okx_cdn_url() -> None:
    source = Path(importer_module.__file__).read_text(encoding="utf-8")

    assert "cdn/okex" not in source


def test_local_raw_zip_exists_does_not_download_and_marks_coverage(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    market_db = tmp_path / "market.sqlite3"
    base = _ms(2024, 1, 1)
    _write_raw_zip(raw_root, "2024-01-01", _rows_for_bucket(base))

    class NoDownloadClient(OkxHistoricalTradesArchiveClient):
        def download_daily_trades_zip(self, *args, **kwargs):
            raise AssertionError("local raw should be used first")

    service = HistoricalTradeImportService(
        trade_store=SqliteTradeStore(market_db),
        archive_client=NoDownloadClient(),
    )
    summary = service.import_missing_buckets(
        symbol=SYMBOL,
        raw_symbol=RAW_SYMBOL,
        exchange="okx",
        bucket_starts=[base],
        bucket_ms=H4,
        time_range=TimeRange(base, base + H4 - 1),
        raw_root=raw_root,
        trade_source="okx_cdn_daily",
        dry_run=False,
        dry_run_download_network=False,
        current_bucket_start_ms=base + 2 * H4,
    )

    assert summary.imported_buckets == 1
    assert summary.raw_files_found == [str(_raw_zip_path(raw_root, "2024-01-01"))]
    assert _coverage_rows(market_db) == [(SYMBOL, base, base + H4 - 1, "historical")]


def test_missing_raw_zip_downloads_through_adapter(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    base = _ms(2024, 1, 1)

    class FakeDownloadClient(OkxHistoricalTradesArchiveClient):
        def download_daily_trades_zip(self, raw_symbol, raw_date: date, destination: Path, *, overwrite: bool = False):
            _write_raw_zip(raw_root, raw_date.isoformat(), _rows_for_bucket(base))
            return DownloadedArchive(
                date=raw_date.isoformat(),
                url="https://example.invalid/raw.zip",
                path=str(destination),
                sha256="sha",
                size=destination.stat().st_size,
                status="downloaded",
            )

    service = HistoricalTradeImportService(
        trade_store=SqliteTradeStore(tmp_path / "market.sqlite3"),
        archive_client=FakeDownloadClient(),
    )
    summary = service.import_missing_buckets(
        symbol=SYMBOL,
        raw_symbol=RAW_SYMBOL,
        exchange="okx",
        bucket_starts=[base],
        bucket_ms=H4,
        time_range=TimeRange(base, base + H4 - 1),
        raw_root=raw_root,
        trade_source="okx_cdn_daily",
        dry_run=False,
        dry_run_download_network=False,
        current_bucket_start_ms=base + 2 * H4,
    )

    assert summary.raw_files_downloaded == [str(_raw_zip_path(raw_root, "2024-01-01"))]
    assert summary.imported_buckets == 1


def test_dry_run_default_no_download_no_write(tmp_path: Path) -> None:
    class NoDownloadClient(OkxHistoricalTradesArchiveClient):
        def download_daily_trades_zip(self, *args, **kwargs):
            raise AssertionError("dry-run without network must not download")

    market_db = tmp_path / "market.sqlite3"
    base = _ms(2024, 1, 1)
    service = HistoricalTradeImportService(
        trade_store=SqliteTradeStore(market_db),
        archive_client=NoDownloadClient(),
    )
    summary = service.import_missing_buckets(
        symbol=SYMBOL,
        raw_symbol=RAW_SYMBOL,
        exchange="okx",
        bucket_starts=[base],
        bucket_ms=H4,
        time_range=TimeRange(base, base + H4 - 1),
        raw_root=tmp_path / "raw",
        trade_source="okx_cdn_daily",
        dry_run=True,
        dry_run_download_network=False,
        current_bucket_start_ms=base + 2 * H4,
    )

    assert summary.would_download_buckets == 1
    assert _coverage_rows(market_db) == []


def test_dry_run_download_network_downloads_but_does_not_write_db(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    market_db = tmp_path / "market.sqlite3"
    base = _ms(2024, 1, 1)

    class FakeDownloadClient(OkxHistoricalTradesArchiveClient):
        def download_daily_trades_zip(self, raw_symbol, raw_date: date, destination: Path, *, overwrite: bool = False):
            _write_raw_zip(raw_root, raw_date.isoformat(), _rows_for_bucket(base))
            return DownloadedArchive(raw_date.isoformat(), "https://example.invalid/raw.zip", str(destination), "sha", destination.stat().st_size, "downloaded")

    service = HistoricalTradeImportService(
        trade_store=SqliteTradeStore(market_db),
        archive_client=FakeDownloadClient(),
    )
    summary = service.import_missing_buckets(
        symbol=SYMBOL,
        raw_symbol=RAW_SYMBOL,
        exchange="okx",
        bucket_starts=[base],
        bucket_ms=H4,
        time_range=TimeRange(base, base + H4 - 1),
        raw_root=raw_root,
        trade_source="okx_cdn_daily",
        dry_run=True,
        dry_run_download_network=True,
        current_bucket_start_ms=base + 2 * H4,
    )

    assert _raw_zip_path(raw_root, "2024-01-01").exists()
    assert summary.rows_read == 13
    assert summary.trades_saved == 0
    assert _coverage_rows(market_db) == []


def test_coverage_insufficient_does_not_mark(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    market_db = tmp_path / "market.sqlite3"
    base = _ms(2024, 1, 1)
    _write_raw_zip(raw_root, "2024-01-01", _rows_for_bucket(base, sparse=True))
    service = HistoricalTradeImportService(
        trade_store=SqliteTradeStore(market_db),
        archive_client=OkxHistoricalTradesArchiveClient(),
    )

    summary = service.import_missing_buckets(
        symbol=SYMBOL,
        raw_symbol=RAW_SYMBOL,
        exchange="okx",
        bucket_starts=[base],
        bucket_ms=H4,
        time_range=TimeRange(base, base + H4 - 1),
        raw_root=raw_root,
        trade_source="local_raw",
        dry_run=False,
        dry_run_download_network=False,
        current_bucket_start_ms=base + 2 * H4,
    )

    assert summary.imported_buckets == 0
    assert summary.coverage_validation_failed_buckets == 1
    assert _coverage_rows(market_db) == []
