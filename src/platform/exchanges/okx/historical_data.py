from __future__ import annotations

import hashlib
import csv
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Sequence

from src.platform.data.models import MarketDataSource, MarketTrade, TradeSide
from src.platform.exchanges.models import ExchangeName


OKX_DAILY_TRADES_URL_TEMPLATE = (
    "https://www.okx.com/cdn/okex/traderecords/trades/daily/"
    "{yyyymmdd}/{symbol}-trades-{date}.zip"
)
OKX_HISTORY_TRADES_PATH = "/api/v5/market/history-trades"
OKX_RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
OKX_TOO_MANY_REQUESTS_CODE = "50011"
RAW_TRADE_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "timestamp": ("created_time", "createdTime", "ts", "timestamp", "time", "datetime"),
    "raw_symbol": ("instrument_name", "instId", "inst_id", "symbol", "raw_symbol"),
    "trade_id": ("trade_id", "tradeId", "id"),
    "price": ("price", "px"),
    "size": ("size", "sz", "qty", "quantity"),
    "side": ("side",),
}
HEADERLESS_RAW_TRADE_COLUMNS_BY_COUNT: dict[int, list[str]] = {
    5: ["trade_id", "price", "size", "side", "timestamp"],
    6: ["raw_symbol", "trade_id", "side", "price", "size", "timestamp"],
}


@dataclass(frozen=True)
class DownloadedArchive:
    date: str
    url: str
    path: str
    sha256: str | None
    size: int | None
    status: str
    error: str | None = None

    def to_manifest_record(self) -> dict[str, object]:
        record: dict[str, object] = {
            "date": self.date,
            "url": self.url,
            "path": self.path,
            "sha256": self.sha256,
            "size": self.size,
            "status": self.status,
        }
        if self.error:
            record["error"] = self.error
        return record

    def to_manifest_json(self) -> str:
        return json.dumps(self.to_manifest_record(), ensure_ascii=False, sort_keys=True)


@dataclass(frozen=True)
class _OkxRawTrade:
    trade_id: str
    price: str
    size: str
    side: str
    ts: str


class OkxHistoricalTradesArchiveClient:
    """OKX public historical trade archives and REST-history access.

    This adapter knows about OKX URLs, retry behavior, archive parsing, and
    OKX trade payload shapes. It does not write AetherEdge databases or mark
    internal coverage.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://www.okx.com",
        daily_trades_url_template: str = OKX_DAILY_TRADES_URL_TEMPLATE,
        timeout_seconds: int = 60,
        max_retries: int = 3,
        sleep_seconds: float = 2.0,
    ) -> None:
        self._base_url = str(base_url).rstrip("/")
        self._daily_trades_url_template = str(daily_trades_url_template)
        self._timeout = int(timeout_seconds)
        self._max_retries = int(max_retries)
        self._sleep_seconds = float(sleep_seconds)

    def build_daily_trades_url(self, raw_symbol: str, date: date) -> str:
        date_text = date.isoformat()
        return self._daily_trades_url_template.format(
            yyyymmdd=date_text.replace("-", ""),
            symbol=str(raw_symbol),
            date=date_text,
        )

    def download_daily_trades_zip(
        self,
        raw_symbol: str,
        date: date,
        destination: Path,
        *,
        overwrite: bool = False,
    ) -> DownloadedArchive:
        url = self.build_daily_trades_url(raw_symbol, date)
        destination = Path(destination)
        if destination.exists() and destination.stat().st_size > 0 and not overwrite:
            return DownloadedArchive(
                date=date.isoformat(),
                url=url,
                path=str(destination),
                sha256=_sha256_file(destination),
                size=destination.stat().st_size,
                status="found",
            )

        destination.parent.mkdir(parents=True, exist_ok=True)
        part_path = destination.with_name(destination.name + ".part")
        last_error = ""
        for attempt in range(1, self._max_retries + 1):
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "AetherEdge/repair-tool",
                        "Accept": "application/zip,application/octet-stream,*/*",
                    },
                )
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    with part_path.open("wb") as fh:
                        while True:
                            chunk = resp.read(1024 * 1024)
                            if not chunk:
                                break
                            fh.write(chunk)
                if part_path.stat().st_size <= 0:
                    raise RuntimeError("downloaded raw zip is empty")
                part_path.replace(destination)
                return DownloadedArchive(
                    date=date.isoformat(),
                    url=url,
                    path=str(destination),
                    sha256=_sha256_file(destination),
                    size=destination.stat().st_size,
                    status="downloaded",
                )
            except Exception as exc:
                last_error = repr(exc)
                try:
                    if part_path.exists():
                        part_path.unlink()
                except OSError:
                    pass
                if attempt < self._max_retries and self._sleep_seconds > 0:
                    time.sleep(self._retry_sleep(attempt))

        raise RuntimeError(f"raw zip download failed after {self._max_retries} attempts: {last_error}")

    def iter_daily_trades_zip(
        self,
        path: Path,
        *,
        raw_symbol: str,
        symbol: str,
        chunksize: int = 300_000,
    ) -> Iterator[list[MarketTrade]]:
        import pandas as pd

        with zipfile.ZipFile(path) as zf:
            member = _first_zip_member(zf)
            has_header = _zip_member_has_header(zf, member)
            with zf.open(member) as fh:
                read_kwargs: dict[str, object] = {}
                if not has_header:
                    column_count = _zip_member_column_count(zf, member)
                    read_kwargs = {
                        "header": None,
                        "names": HEADERLESS_RAW_TRADE_COLUMNS_BY_COUNT.get(column_count),
                    }
                for chunk in pd.read_csv(
                    fh,
                    chunksize=max(1, int(chunksize)),
                    **read_kwargs,
                ):
                    yield _raw_chunk_to_trades(
                        chunk,
                        symbol=symbol,
                        raw_symbol=raw_symbol,
                        member=member,
                    )

    def fetch_history_trades_page(
        self,
        raw_symbol: str,
        *,
        after: str | None = None,
        limit: int = 100,
    ) -> tuple[list[_OkxRawTrade], str | None]:
        params = f"instId={urllib.parse.quote(str(raw_symbol))}&limit={min(max(1, int(limit)), 100)}"
        if after:
            params += f"&after={urllib.parse.quote(str(after))}"
        url = f"{self._base_url}{OKX_HISTORY_TRADES_PATH}?{params}"

        last_error = ""
        for attempt in range(1, self._max_retries + 1):
            try:
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "AetherEdge/repair-tool",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(body)
                code = str(payload.get("code", ""))
                if code != "0":
                    if code == OKX_TOO_MANY_REQUESTS_CODE and attempt < self._max_retries:
                        time.sleep(self._retry_sleep(attempt))
                        continue
                    raise RuntimeError(f"OKX API error code={code} msg={payload.get('msg', '')}")
                raw_data = payload.get("data") or []
                trades = [
                    _OkxRawTrade(
                        trade_id=str(item.get("tradeId", "")),
                        price=str(item.get("px", "0")),
                        size=str(item.get("sz", "0")),
                        side=str(item.get("side", "")),
                        ts=str(item.get("ts", "0")),
                    )
                    for item in raw_data
                    if item.get("tradeId")
                ]
                next_after = trades[-1].trade_id if trades else None
                return trades, next_after
            except urllib.error.HTTPError as exc:
                detail = ""
                try:
                    detail = exc.read().decode("utf-8", errors="replace")[:300]
                except Exception:
                    pass
                last_error = f"HTTP {exc.code}: {detail}"
                if exc.code in OKX_RETRYABLE_HTTP_CODES and attempt < self._max_retries:
                    time.sleep(self._retry_sleep(attempt))
                    continue
                raise RuntimeError(f"OKX request failed: {last_error}") from exc
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                last_error = repr(exc)
                if attempt < self._max_retries:
                    time.sleep(self._retry_sleep(attempt))
                    continue
                raise RuntimeError(f"OKX request failed: {last_error}") from exc

        raise RuntimeError(f"OKX request failed after {self._max_retries} attempts: {last_error}")

    def download_history_bucket_trades(
        self,
        raw_symbol: str,
        bucket_start_ms: int,
        bucket_end_ms: int,
        *,
        limit: int = 100,
        max_pages: int | None = None,
        symbol: str = "",
    ) -> tuple[list[MarketTrade], int, bool]:
        all_raw: list[_OkxRawTrade] = []
        after: str | None = None
        pages = 0
        complete = False

        while True:
            if max_pages is not None and pages >= max_pages:
                break
            page, next_after = self.fetch_history_trades_page(raw_symbol, after=after, limit=limit)
            pages += 1
            if not page:
                break

            in_range = [
                trade
                for trade in page
                if bucket_start_ms <= int(trade.ts) <= bucket_end_ms
            ]
            all_raw.extend(in_range)

            oldest_ts = min(int(trade.ts) for trade in page)
            if oldest_ts < bucket_start_ms:
                complete = True
                break
            if next_after is None:
                break
            after = next_after
            if self._sleep_seconds > 0:
                time.sleep(self._sleep_seconds)

        trades = [
            MarketTrade(
                exchange=ExchangeName.OKX,
                symbol=symbol,
                raw_symbol=raw_symbol,
                price=Decimal(raw.price),
                quantity=Decimal(raw.size),
                side=TradeSide.BUY if raw.side.lower() == "buy" else TradeSide.SELL,
                trade_id=raw.trade_id,
                event_time_ms=int(raw.ts),
                trade_time_ms=int(raw.ts),
                source=MarketDataSource.REST,
                raw={
                    "tradeId": raw.trade_id,
                    "px": raw.price,
                    "sz": raw.size,
                    "side": raw.side,
                    "ts": raw.ts,
                },
            )
            for raw in all_raw
        ]
        return trades, pages, complete

    def _retry_sleep(self, attempt: int) -> float:
        return min(self._sleep_seconds * (2 ** max(0, attempt - 1)), 30.0)


def _raw_chunk_to_trades(chunk, *, symbol: str, raw_symbol: str, member: str = "<unknown>") -> list[MarketTrade]:
    columns = _raw_column_map(chunk.columns)
    raw_symbol_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["raw_symbol"])
    trade_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["trade_id"])
    price_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["price"])
    size_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["size"])
    side_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["side"])
    ts_col = _find_raw_col(columns, RAW_TRADE_COLUMN_ALIASES["timestamp"])
    if price_col is None or size_col is None or ts_col is None:
        raise ValueError(_raw_columns_error_message(chunk, member=member))

    trades: list[MarketTrade] = []
    for index, row in chunk.iterrows():
        ts_ms = _parse_raw_timestamp(row[ts_col])
        if ts_ms is None:
            continue
        price = _decimal_or_none(row[price_col])
        quantity = _decimal_or_none(row[size_col])
        if price is None or quantity is None or price <= 0 or quantity <= 0:
            continue
        side = TradeSide.UNKNOWN
        if side_col is not None:
            side_raw = str(row[side_col]).strip().lower()
            if side_raw == "buy":
                side = TradeSide.BUY
            elif side_raw == "sell":
                side = TradeSide.SELL
        trade_id = None
        if trade_col is not None:
            raw_trade_id = row[trade_col]
            if raw_trade_id is not None and str(raw_trade_id).strip().lower() != "nan":
                trade_id = str(raw_trade_id).strip()
        if trade_id is None:
            trade_id = f"{raw_symbol}:{ts_ms}:{index}"
        trade_raw_symbol = raw_symbol
        if raw_symbol_col is not None:
            row_raw_symbol = row[raw_symbol_col]
            if row_raw_symbol is not None and str(row_raw_symbol).strip().lower() != "nan":
                trade_raw_symbol = str(row_raw_symbol).strip()
        trades.append(
            MarketTrade(
                exchange=ExchangeName.OKX,
                symbol=symbol,
                raw_symbol=trade_raw_symbol,
                source=MarketDataSource.REST,
                price=price,
                quantity=quantity,
                side=side,
                trade_id=trade_id,
                event_time_ms=ts_ms,
                trade_time_ms=ts_ms,
                raw={str(col): _json_scalar(row[col]) for col in chunk.columns},
            )
        )
    return trades


def _first_zip_member(zf: zipfile.ZipFile) -> str:
    for info in zf.infolist():
        if not info.is_dir():
            return info.filename
    raise ValueError("raw zip has no file members")


def _zip_member_has_header(zf: zipfile.ZipFile, member: str) -> bool:
    first_row = _zip_member_first_row(zf, member)
    normalized = {_normalize_raw_col(value) for value in first_row}
    known_columns = {
        _normalize_raw_col(alias)
        for aliases in RAW_TRADE_COLUMN_ALIASES.values()
        for alias in aliases
    }
    return bool(normalized & known_columns)


def _zip_member_column_count(zf: zipfile.ZipFile, member: str) -> int:
    return len(_zip_member_first_row(zf, member))


def _zip_member_first_row(zf: zipfile.ZipFile, member: str) -> list[str]:
    with zf.open(member) as fh:
        first_line = fh.readline().decode("utf-8-sig", errors="replace")
    if not first_line:
        return []
    return next(csv.reader([first_line]))


def _raw_column_map(columns) -> dict[str, str]:
    return {_normalize_raw_col(str(col)): str(col) for col in columns}


def _normalize_raw_col(value: str) -> str:
    return "".join(ch for ch in value.strip().lower() if ch.isalnum())


def _find_raw_col(columns: dict[str, str], candidates: Sequence[str]) -> str | None:
    for candidate in candidates:
        found = columns.get(_normalize_raw_col(candidate))
        if found is not None:
            return found
    return None


def _raw_columns_error_message(chunk, *, member: str) -> str:
    required_aliases = {
        "price": list(RAW_TRADE_COLUMN_ALIASES["price"]),
        "size": list(RAW_TRADE_COLUMN_ALIASES["size"]),
        "timestamp": list(RAW_TRADE_COLUMN_ALIASES["timestamp"]),
    }
    return (
        "raw trades CSV missing required price/size/timestamp columns "
        f"member={member!r} "
        f"detected_columns={list(map(str, chunk.columns))!r} "
        f"required_aliases={required_aliases!r} "
        f"all_aliases={ {key: list(value) for key, value in RAW_TRADE_COLUMN_ALIASES.items()}!r} "
        f"first_3_rows={chunk.head(3).to_dict(orient='records')!r}"
    )


def _parse_raw_timestamp(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.lower() == "nan":
        return None
    try:
        dec = Decimal(raw)
        if dec >= Decimal("100000000000"):
            return int(dec)
        return int(dec * Decimal("1000"))
    except Exception:
        pass
    try:
        normalized = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def _decimal_or_none(value: object) -> Decimal | None:
    try:
        dec = Decimal(str(value).strip())
    except Exception:
        return None
    if not dec.is_finite():
        return None
    return dec


def _json_scalar(value: object) -> object:
    if value is None:
        return None
    raw = str(value)
    return None if raw.lower() == "nan" else raw


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
