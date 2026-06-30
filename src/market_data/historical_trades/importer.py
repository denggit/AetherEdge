from __future__ import annotations

import csv
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping, Sequence
import zipfile

from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


def iter_trade_csv_chunks(path: str | Path, chunksize: int = 50_000) -> Iterable[list[dict[str, str]]]:
    """Yield CSV rows from a plain CSV or a ZIP containing CSV files."""

    p = Path(path)
    size = max(1, int(chunksize))
    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p) as archive:
            names = sorted(name for name in archive.namelist() if name.lower().endswith(".csv"))
            if not names:
                raise ValueError(f"zip archive contains no csv files: {p}")
            for name in names:
                with archive.open(name) as raw:
                    text = (line.decode("utf-8-sig", errors="replace") for line in raw)
                    yield from _iter_csv_reader_chunks(text, size)
        return
    with p.open("r", encoding="utf-8-sig", newline="") as handle:
        yield from _iter_csv_reader_chunks(handle, size)


def normalize_okx_trade_chunk(
    raw_df: object,
    *,
    symbol: str,
    raw_symbol: str,
    exchange: str = "okx",
) -> list[MarketTrade]:
    rows = _rows_from_chunk(raw_df)
    trades: list[MarketTrade] = []
    for row in rows:
        lowered = {str(key).strip().lower(): value for key, value in row.items()}
        ts = _parse_time_ms(_first(lowered, "ts", "timestamp", "time"))
        price = _parse_decimal(_first(lowered, "px", "price"))
        qty = _parse_decimal(_first(lowered, "sz", "size", "qty", "quantity"))
        if ts is None or price is None or qty is None or price <= 0 or qty <= 0:
            continue
        side = _parse_side(_first(lowered, "side"))
        trade_id = _first(lowered, "trade_id", "tradeid", "id")
        trades.append(
            MarketTrade(
                exchange=ExchangeName(str(exchange).strip().lower()),
                symbol=symbol,
                raw_symbol=raw_symbol,
                price=price,
                quantity=qty,
                side=side,
                trade_id=None if trade_id is None else str(trade_id),
                event_time_ms=ts,
                trade_time_ms=ts,
                source=MarketDataSource.REST,
                raw=dict(row),
            )
        )
    trades.sort(key=lambda item: ((item.trade_time_ms or item.event_time_ms or 0), item.trade_id or ""))
    return trades


def _iter_csv_reader_chunks(lines: Iterable[str], chunksize: int) -> Iterable[list[dict[str, str]]]:
    reader = csv.DictReader(lines)
    chunk: list[dict[str, str]] = []
    for row in reader:
        chunk.append(dict(row))
        if len(chunk) >= chunksize:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _rows_from_chunk(raw_df: object) -> list[Mapping[str, object]]:
    if hasattr(raw_df, "to_dict"):
        records = raw_df.to_dict("records")  # type: ignore[call-arg]
        return [dict(row) for row in records]
    if isinstance(raw_df, Sequence) and not isinstance(raw_df, (str, bytes, bytearray)):
        return [dict(row) for row in raw_df]  # type: ignore[arg-type]
    raise TypeError("raw trade chunk must be a sequence of mappings or a dataframe-like object")


def _first(row: Mapping[str, object], *names: str) -> object | None:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _parse_decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _parse_time_ms(value: object | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        raw = Decimal(text)
    except InvalidOperation:
        return None
    if raw <= 0:
        return None
    if raw < Decimal("10000000000"):
        raw *= Decimal("1000")
    return int(raw)


def _parse_side(value: object | None) -> TradeSide:
    text = str(value or "").strip().lower()
    if text == "buy":
        return TradeSide.BUY
    if text == "sell":
        return TradeSide.SELL
    return TradeSide.UNKNOWN
