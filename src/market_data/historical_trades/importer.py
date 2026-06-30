from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping, Sequence
import zipfile

from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName

MIN_VALID_TRADE_TIME_MS = 1_577_836_800_000
TIME_COLUMN_ALIASES = (
    "ts",
    "timestamp",
    "time",
    "datetime",
    "created_time",
    "createdtime",
    "create_time",
    "created_at",
    "createdat",
)


@dataclass(frozen=True)
class TradeChunkFilterResult:
    rows: list[Mapping[str, object]]
    raw_rows: int
    filtered_rows: int
    dropped_rows: int
    first_trade_time_ms: int | None = None
    last_trade_time_ms: int | None = None


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
    min_valid_trade_time_ms: int = MIN_VALID_TRADE_TIME_MS,
    max_valid_trade_time_ms: int | None = None,
) -> list[MarketTrade]:
    rows = _rows_from_chunk(raw_df)
    trades: list[MarketTrade] = []
    for row in rows:
        lowered = {str(key).strip().lower(): value for key, value in row.items()}
        ts = _parse_time_ms(_first(lowered, *TIME_COLUMN_ALIASES))
        price = _parse_decimal(_first(lowered, "px", "price"))
        qty = _parse_decimal(_first(lowered, "sz", "size", "qty", "quantity"))
        if ts is None or price is None or qty is None or price <= 0 or qty <= 0:
            continue
        if ts < int(min_valid_trade_time_ms):
            continue
        if max_valid_trade_time_ms is not None and ts > int(max_valid_trade_time_ms):
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


def filter_okx_trade_chunk_by_time(
    raw_df: object,
    *,
    start_time_ms: int,
    end_time_ms: int,
    min_valid_trade_time_ms: int = MIN_VALID_TRADE_TIME_MS,
    max_valid_trade_time_ms: int | None = None,
) -> TradeChunkFilterResult:
    rows = _rows_from_chunk(raw_df)
    filtered: list[Mapping[str, object]] = []
    first_ts: int | None = None
    last_ts: int | None = None
    for row in rows:
        lowered = {str(key).strip().lower(): value for key, value in row.items()}
        ts = _parse_time_ms(_first(lowered, *TIME_COLUMN_ALIASES))
        if ts is None:
            continue
        if first_ts is None:
            first_ts = ts
        last_ts = ts
        if ts < int(min_valid_trade_time_ms):
            continue
        if max_valid_trade_time_ms is not None and ts > int(max_valid_trade_time_ms):
            continue
        if int(start_time_ms) <= ts <= int(end_time_ms):
            filtered.append(row)
    return TradeChunkFilterResult(
        rows=filtered,
        raw_rows=len(rows),
        filtered_rows=len(filtered),
        dropped_rows=len(rows) - len(filtered),
        first_trade_time_ms=first_ts,
        last_trade_time_ms=last_ts,
    )


def _iter_csv_reader_chunks(lines: Iterable[str], chunksize: int) -> Iterable[list[dict[str, str]]]:
    reader = csv.reader(lines)
    header: list[str] | None = None
    chunk: list[dict[str, str]] = []
    for values in reader:
        if not values:
            continue
        if header is None:
            if _looks_like_header(values):
                header = [str(value).strip() for value in values]
                continue
            header = _infer_headerless_columns(values)
        chunk.append(_row_from_values(header, values))
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


def _looks_like_header(values: Sequence[object]) -> bool:
    lowered = {str(value).strip().lower() for value in values}
    has_time = any(name in lowered for name in TIME_COLUMN_ALIASES)
    has_price = any(name in lowered for name in ("px", "price"))
    has_size = any(name in lowered for name in ("sz", "size", "qty", "quantity"))
    return has_time and has_price and has_size


def _infer_headerless_columns(values: Sequence[object]) -> list[str]:
    count = len(values)
    if count >= 6 and _parse_time_ms(values[5]) is not None:
        header = ["inst_id", "trade_id", "px", "sz", "side", "ts"]
    elif count >= 5 and _parse_time_ms(values[4]) is not None:
        header = ["trade_id", "px", "sz", "side", "ts"]
    elif count >= 1 and _parse_time_ms(values[0]) is not None:
        header = ["ts", "px", "sz", "side", "trade_id"]
    else:
        header = ["ts", "px", "sz", "side", "trade_id"]
    if count > len(header):
        header = [*header, *(f"extra_{idx}" for idx in range(len(header), count))]
    return header[:count]


def _row_from_values(header: Sequence[str], values: Sequence[str]) -> dict[str, str]:
    row: dict[str, str] = {}
    for idx, value in enumerate(values):
        key = header[idx] if idx < len(header) else f"extra_{idx}"
        row[str(key)] = value
    return row


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
    digits = text
    if digits.startswith("+"):
        digits = digits[1:]
    if digits.isdigit():
        raw_int = int(digits)
        if raw_int <= 0:
            return None
        if raw_int < 10_000_000_000:
            raw_int *= 1000
        return raw_int
    try:
        raw = Decimal(text)
    except InvalidOperation:
        return _parse_datetime_ms(text)
    if raw <= 0:
        return None
    if raw < Decimal("10000000000"):
        raw *= Decimal("1000")
    return int(raw)


def _parse_datetime_ms(text: str) -> int | None:
    value = text.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def _parse_side(value: object | None) -> TradeSide:
    text = str(value or "").strip().lower()
    if text == "buy":
        return TradeSide.BUY
    if text == "sell":
        return TradeSide.SELL
    return TradeSide.UNKNOWN
