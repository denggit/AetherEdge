from __future__ import annotations

import ast
import io
import urllib.error
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from src.platform.exchanges.okx.historical_archive import OkxArchiveUnavailableError, OkxHistoricalArchive, build_daily_trades_url


def test_build_daily_trades_url() -> None:
    assert build_daily_trades_url("ETH-USDT-SWAP", date(2026, 6, 28)) == (
        "https://www.okx.com/cdn/okex/traderecords/trades/daily/"
        "20260628/ETH-USDT-SWAP-trades-2026-06-28.zip"
    )


def test_iter_daily_trades_zip_parses_real_okx_header(tmp_path: Path) -> None:
    path = tmp_path / "trades.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(
            "ETH-USDT-SWAP.csv",
            "instrument_name,trade_id,side,price,size,created_time\n"
            "ETH-USDT-SWAP,abc,buy,100.1,2,1719532800000\n",
        )

    rows = list(OkxHistoricalArchive().iter_daily_trades_zip(path, raw_symbol="ETH-USDT-SWAP", symbol="ETH-USDT-PERP", chunksize=1))

    assert len(rows) == 1
    assert rows[0][0].symbol == "ETH-USDT-PERP"
    assert rows[0][0].raw_symbol == "ETH-USDT-SWAP"
    assert rows[0][0].trade_time_ms == 1719532800000


def test_current_utc_day_is_not_downloaded(tmp_path: Path) -> None:
    called = False

    def fake_urlopen(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("should not download current day")

    archive = OkxHistoricalArchive(urlopen=fake_urlopen)
    meta = archive.ensure_daily_trades_zip(
        raw_root=tmp_path,
        raw_symbol="ETH-USDT-SWAP",
        day=date(2026, 6, 30),
        now=datetime(2026, 6, 30, 12, tzinfo=UTC),
    )

    assert meta is None
    assert called is False


def test_missing_completed_day_downloads_with_part_atomic_replace(tmp_path: Path) -> None:
    class Response:
        def __init__(self) -> None:
            self._body = io.BytesIO(b"zip-bytes")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _size: int) -> bytes:
            return self._body.read(_size)

    archive = OkxHistoricalArchive(urlopen=lambda *_args, **_kwargs: Response(), sleep_seconds=0)
    meta = archive.ensure_daily_trades_zip(
        raw_root=tmp_path,
        raw_symbol="ETH-USDT-SWAP",
        day=date(2026, 6, 29),
        now=datetime(2026, 6, 30, tzinfo=UTC),
    )

    assert meta is not None
    assert Path(meta.path).read_bytes() == b"zip-bytes"
    assert not Path(str(meta.path) + ".part").exists()


def test_completed_day_404_is_typed_unavailable(tmp_path: Path) -> None:
    def fake_urlopen(*_args, **_kwargs):
        raise urllib.error.HTTPError("url", 404, "not found", {}, None)

    archive = OkxHistoricalArchive(urlopen=fake_urlopen, sleep_seconds=0)

    with pytest.raises(OkxArchiveUnavailableError) as caught:
        archive.ensure_daily_trades_zip(
            raw_root=tmp_path,
            raw_symbol="ETH-USDT-SWAP",
            day=date(2026, 6, 29),
            now=datetime(2026, 6, 30, tzinfo=UTC),
        )

    assert caught.value.status == "not_yet_published"


def test_platform_archive_does_not_import_business_domains() -> None:
    path = Path("src/platform/exchanges/okx/historical_archive.py")
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert not any(module.startswith(("src.market_data", "src.runtime", "strategies")) for module in imports)
