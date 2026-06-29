from __future__ import annotations

import io
import sqlite3
import zipfile
from datetime import date
from decimal import Decimal
from pathlib import Path

from src.platform.data.models import TradeSide
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.okx import historical_data
from src.platform.exchanges.okx.historical_data import OkxHistoricalTradesArchiveClient


RAW_SYMBOL = "ETH-USDT-SWAP"
SYMBOL = "ETH-USDT-PERP"


def _zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "trades.csv",
            "\n".join([
                "tradeId,px,sz,side,ts",
                "1,100,2,buy,1704067200000",
                "2,101,3,sell,1704067201",
                "3,102,4,buy,2024-01-01T00:00:02Z",
            ]) + "\n",
        )
    return buf.getvalue()


def _real_okx_header_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "ETH-USDT-SWAP-trades-2026-06-28.csv",
            "\n".join([
                "instrument_name,trade_id,side,price,size,created_time",
                "ETH-USDT-SWAP,4033860455,sell,1604.96,2.08,1782576000012",
                "ETH-USDT-SWAP,4033860456,buy,1604.97,1.5,1782576000032",
            ]) + "\n",
        )
    return buf.getvalue()


def test_build_daily_trades_url() -> None:
    client = OkxHistoricalTradesArchiveClient()

    assert client.build_daily_trades_url(RAW_SYMBOL, date(2024, 1, 2)) == (
        "https://www.okx.com/cdn/okex/traderecords/trades/daily/"
        "20240102/ETH-USDT-SWAP-trades-2024-01-02.zip"
    )


def test_iter_daily_trades_zip_supports_common_columns_and_timestamps(tmp_path: Path) -> None:
    path = tmp_path / "raw.zip"
    path.write_bytes(_zip_bytes())
    client = OkxHistoricalTradesArchiveClient()

    batches = list(client.iter_daily_trades_zip(path, raw_symbol=RAW_SYMBOL, symbol=SYMBOL, chunksize=2))
    trades = [trade for batch in batches for trade in batch]

    assert len(trades) == 3
    assert trades[0].symbol == SYMBOL
    assert trades[0].raw_symbol == RAW_SYMBOL
    assert trades[0].trade_id == "1"
    assert trades[1].trade_time_ms == 1704067201000
    assert trades[2].trade_time_ms == 1704067202000


def test_iter_daily_trades_zip_supports_okx_cdn_created_time_header(tmp_path: Path) -> None:
    path = tmp_path / "real-header.zip"
    path.write_bytes(_real_okx_header_zip_bytes())
    client = OkxHistoricalTradesArchiveClient()

    batches = list(client.iter_daily_trades_zip(path, raw_symbol=RAW_SYMBOL, symbol=SYMBOL, chunksize=1))
    trades = [trade for batch in batches for trade in batch]

    assert len(trades) == 2
    assert trades[0].exchange == ExchangeName.OKX
    assert trades[0].symbol == SYMBOL
    assert trades[0].raw_symbol == RAW_SYMBOL
    assert trades[0].trade_id == "4033860455"
    assert trades[0].side == TradeSide.SELL
    assert trades[0].price == Decimal("1604.96")
    assert trades[0].quantity == Decimal("2.08")
    assert trades[0].event_time_ms == 1782576000012
    assert trades[0].trade_time_ms == 1782576000012
    assert trades[1].side == TradeSide.BUY


def test_iter_daily_trades_zip_error_includes_member_columns_aliases_and_rows(tmp_path: Path) -> None:
    path = tmp_path / "bad.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("bad.csv", "foo,price\n1,2\n3,4\n")
    client = OkxHistoricalTradesArchiveClient()

    try:
        list(client.iter_daily_trades_zip(path, raw_symbol=RAW_SYMBOL, symbol=SYMBOL))
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected missing column error")

    assert "member='bad.csv'" in message
    assert "detected_columns=['foo', 'price']" in message
    assert "required_aliases=" in message
    assert "created_time" in message
    assert "first_3_rows=" in message


def test_download_daily_trades_zip_uses_part_and_atomic_replace(tmp_path: Path, monkeypatch) -> None:
    class FakeResponse:
        def __init__(self, payload: bytes) -> None:
            self._payload = io.BytesIO(payload)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self, size: int = -1) -> bytes:
            return self._payload.read(size)

    calls: list[str] = []

    def fake_urlopen(req, timeout):
        calls.append(req.full_url)
        return FakeResponse(_zip_bytes())

    monkeypatch.setattr(historical_data.urllib.request, "urlopen", fake_urlopen)
    client = OkxHistoricalTradesArchiveClient(timeout_seconds=5, max_retries=1)
    destination = tmp_path / "ETH-USDT-SWAP-trades-2024-01-01.zip"

    archive = client.download_daily_trades_zip(RAW_SYMBOL, date(2024, 1, 1), destination)

    assert destination.exists()
    assert not destination.with_name(destination.name + ".part").exists()
    assert archive.status == "downloaded"
    assert archive.size == destination.stat().st_size
    assert archive.sha256
    assert calls and calls[0].endswith("/20240101/ETH-USDT-SWAP-trades-2024-01-01.zip")


def test_platform_adapter_does_not_write_db(tmp_path: Path) -> None:
    path = tmp_path / "raw.zip"
    path.write_bytes(_zip_bytes())
    client = OkxHistoricalTradesArchiveClient()

    list(client.iter_daily_trades_zip(path, raw_symbol=RAW_SYMBOL, symbol=SYMBOL))

    sqlite_files = [item for item in tmp_path.iterdir() if item.suffix in {".sqlite", ".sqlite3", ".db"}]
    assert sqlite_files == []
