from __future__ import annotations

import csv
import hashlib
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterator, Sequence

from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


OKX_DAILY_TRADES_URL_TEMPLATE = (
    "https://www.okx.com/cdn/okex/traderecords/trades/daily/"
    "{yyyymmdd}/{symbol}-trades-{date}.zip"
)

RAW_TRADE_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "timestamp": ("created_time", "createdTime", "ts", "timestamp", "time", "datetime"),
    "raw_symbol": ("instrument_name", "instId", "inst_id", "symbol", "raw_symbol"),
    "trade_id": ("trade_id", "tradeId", "id"),
    "price": ("price", "px"),
    "size": ("size", "sz", "qty", "quantity", "amount"),
    "side": ("side",),
}


@dataclass(frozen=True)
class ArchiveMetadata:
    date: str
    url: str
    path: str
    sha256: str
    size: int
    status: str


def build_daily_trades_url(raw_symbol: str, day: date) -> str:
    day_text = day.isoformat()
    return OKX_DAILY_TRADES_URL_TEMPLATE.format(
        yyyymmdd=day_text.replace("-", ""),
        symbol=str(raw_symbol),
        date=day_text,
    )


def daily_trades_zip_path(raw_root: str | Path, raw_symbol: str, day: date) -> Path:
    return Path(raw_root) / "trades" / raw_symbol / f"{raw_symbol}-trades-{day.isoformat()}.zip"


def is_completed_utc_day(day: date, *, now: datetime | None = None) -> bool:
    current = (now or datetime.now(UTC)).astimezone(UTC).date()
    return day < current


class OkxHistoricalArchive:
    """OKX public daily trade archives.

    This adapter only knows OKX archive URLs, downloading and CSV parsing. It
    intentionally does not write AetherEdge stores or import market_data/runtime.
    """

    def __init__(
        self,
        *,
        timeout_seconds: float = 60.0,
        max_retries: int = 3,
        sleep_seconds: float = 2.0,
        urlopen: Callable[..., object] | None = None,
    ) -> None:
        self.timeout_seconds = float(timeout_seconds)
        self.max_retries = max(1, int(max_retries))
        self.sleep_seconds = max(0.0, float(sleep_seconds))
        self._urlopen = urlopen or urllib.request.urlopen

    def build_daily_trades_url(self, raw_symbol: str, day: date) -> str:
        return build_daily_trades_url(raw_symbol, day)

    def ensure_daily_trades_zip(
        self,
        *,
        raw_root: str | Path,
        raw_symbol: str,
        day: date,
        now: datetime | None = None,
    ) -> ArchiveMetadata | None:
        path = daily_trades_zip_path(raw_root, raw_symbol, day)
        if path.exists() and path.stat().st_size > 0:
            return _metadata(path=path, url=self.build_daily_trades_url(raw_symbol, day), day=day, status="found")
        if not is_completed_utc_day(day, now=now):
            return None
        return self.download_daily_trades_zip(raw_symbol=raw_symbol, day=day, destination=path)

    def download_daily_trades_zip(self, *, raw_symbol: str, day: date, destination: str | Path) -> ArchiveMetadata:
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        part_path = destination.with_name(destination.name + ".part")
        url = self.build_daily_trades_url(raw_symbol, day)
        last_error = ""
        for attempt in range(1, self.max_retries + 1):
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "AetherEdge/range-backfill",
                        "Accept": "application/zip,application/octet-stream,*/*",
                    },
                )
                with self._urlopen(req, timeout=self.timeout_seconds) as response:  # type: ignore[attr-defined]
                    with part_path.open("wb") as fh:
                        while True:
                            chunk = response.read(1024 * 1024)
                            if not chunk:
                                break
                            fh.write(chunk)
                if part_path.stat().st_size <= 0:
                    raise RuntimeError("downloaded daily trades zip is empty")
                part_path.replace(destination)
                return _metadata(path=destination, url=url, day=day, status="downloaded")
            except Exception as exc:  # noqa: BLE001 - download retries need broad capture
                last_error = repr(exc)
                try:
                    part_path.unlink(missing_ok=True)
                except OSError:
                    pass
                if attempt < self.max_retries and self.sleep_seconds > 0:
                    time.sleep(min(self.sleep_seconds * (2 ** (attempt - 1)), 30.0))
        raise RuntimeError(f"OKX daily trades download failed after {self.max_retries} attempts: {last_error}")

    def iter_daily_trades_zip(
        self,
        path: str | Path,
        *,
        raw_symbol: str,
        symbol: str,
        chunksize: int = 300_000,
    ) -> Iterator[list[MarketTrade]]:
        yield from iter_daily_trades_zip(path, raw_symbol=raw_symbol, symbol=symbol, chunksize=chunksize)


def iter_daily_trades_zip(
    path: str | Path,
    *,
    raw_symbol: str,
    symbol: str,
    chunksize: int = 300_000,
) -> Iterator[list[MarketTrade]]:
    import pandas as pd

    with zipfile.ZipFile(path) as zf:
        member = _first_zip_member(zf)
        with zf.open(member) as fh:
            for chunk in pd.read_csv(fh, chunksize=max(1, int(chunksize))):
                yield _chunk_to_trades(chunk, raw_symbol=raw_symbol, symbol=symbol)


def _chunk_to_trades(chunk, *, raw_symbol: str, symbol: str) -> list[MarketTrade]:
    columns = _raw_column_map(chunk.columns)
    ts_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["timestamp"])
    raw_symbol_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["raw_symbol"])
    trade_id_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["trade_id"])
    price_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["price"])
    size_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["size"])
    side_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["side"])
    if ts_col is None or price_col is None or size_col is None:
        raise ValueError(
            "OKX raw trades CSV missing timestamp/price/size columns "
            f"columns={list(map(str, chunk.columns))!r}"
        )

    trades: list[MarketTrade] = []
    for index, row in chunk.iterrows():
        ts_ms = _parse_timestamp_ms(row[ts_col])
        price = _decimal_or_none(row[price_col])
        size = _decimal_or_none(row[size_col])
        if ts_ms is None or price is None or size is None or price <= 0 or size <= 0:
            continue
        side = TradeSide.UNKNOWN
        if side_col is not None:
            side_raw = str(row[side_col]).strip().lower()
            if side_raw == "buy":
                side = TradeSide.BUY
            elif side_raw == "sell":
                side = TradeSide.SELL
        row_raw_symbol = raw_symbol
        if raw_symbol_col is not None and _present(row[raw_symbol_col]):
            row_raw_symbol = str(row[raw_symbol_col]).strip()
        trade_id = f"{row_raw_symbol}:{ts_ms}:{index}"
        if trade_id_col is not None and _present(row[trade_id_col]):
            trade_id = str(row[trade_id_col]).strip()
        trades.append(
            MarketTrade(
                exchange=ExchangeName.OKX,
                symbol=symbol,
                raw_symbol=row_raw_symbol,
                price=price,
                quantity=size,
                side=side,
                trade_id=trade_id,
                event_time_ms=ts_ms,
                trade_time_ms=ts_ms,
                source=MarketDataSource.REST,
                raw={str(col): _json_scalar(row[col]) for col in chunk.columns},
            )
        )
    return trades


def _first_zip_member(zf: zipfile.ZipFile) -> str:
    for info in zf.infolist():
        if not info.is_dir():
            return info.filename
    raise ValueError("OKX daily trades zip has no file members")


def _raw_column_map(columns) -> dict[str, str]:
    return {_normalize_col(str(col)): str(col) for col in columns}


def _normalize_col(value: str) -> str:
    return "".join(ch for ch in value.strip().lower() if ch.isalnum())


def _find_raw_col(columns: dict[str, str], aliases: Sequence[str]) -> str | None:
    for alias in aliases:
        found = columns.get(_normalize_col(alias))
        if found is not None:
            return found
    return None


def _parse_timestamp_ms(value: object) -> int | None:
    if not _present(value):
        return None
    raw = str(value).strip()
    try:
        number = Decimal(raw)
        if number >= Decimal("100000000000"):
            return int(number)
        return int(number * Decimal("1000"))
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def _decimal_or_none(value: object) -> Decimal | None:
    if not _present(value):
        return None
    try:
        dec = Decimal(str(value).strip())
    except Exception:
        return None
    return dec if dec.is_finite() else None


def _present(value: object) -> bool:
    return value is not None and str(value).strip() != "" and str(value).strip().lower() != "nan"


def _json_scalar(value: object) -> object:
    return None if not _present(value) else str(value)


def _metadata(*, path: Path, url: str, day: date, status: str) -> ArchiveMetadata:
    return ArchiveMetadata(
        date=day.isoformat(),
        url=url,
        path=str(path),
        sha256=_sha256(path),
        size=path.stat().st_size,
        status=status,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
