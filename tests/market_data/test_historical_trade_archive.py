from __future__ import annotations

from datetime import date
import zipfile

from src.market_data.historical_trades.importer import iter_trade_csv_chunks, normalize_okx_trade_chunk
from src.market_data.historical_trades.okx_archive import OkxHistoricalTradeArchive, okx_raw_symbol_from_canonical


def test_okx_raw_symbol_default_mapping() -> None:
    assert okx_raw_symbol_from_canonical("ETH-USDT-PERP") == "ETH-USDT-SWAP"


def test_existing_zip_is_reused_without_download(tmp_path) -> None:
    archive = OkxHistoricalTradeArchive(tmp_path)
    path = archive.local_path(raw_symbol="ETH-USDT-SWAP", day=date(2026, 6, 1))
    path.parent.mkdir(parents=True)
    path.write_bytes(b"zip")

    result = archive.ensure_daily_file(
        symbol="ETH-USDT-PERP",
        raw_symbol="ETH-USDT-SWAP",
        day=date(2026, 6, 1),
        allow_download=False,
    )

    assert result.path == path
    assert result.downloaded is False


def test_importer_reads_zip_and_normalizes_trades(tmp_path) -> None:
    path = tmp_path / "ETH-USDT-SWAP-trades-2026-06-01.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "trades.csv",
            "ts,px,sz,side,trade_id\n"
            "1780272000000,100,2,buy,a\n"
            "1780272000100,0,2,buy,b\n"
            "1780272000200,101,3,sell,c\n",
        )

    chunks = list(iter_trade_csv_chunks(path, chunksize=2))
    trades = [trade for chunk in chunks for trade in normalize_okx_trade_chunk(chunk, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP")]

    assert len(chunks) == 2
    assert [trade.trade_id for trade in trades] == ["a", "c"]
    assert trades[0].trade_time_ms == 1780272000000
