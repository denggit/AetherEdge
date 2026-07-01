from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import time
from typing import Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import zipfile

from src.market_data.historical_trades.models import HistoricalTradeFile
from src.utils.log import get_logger

logger = get_logger(__name__)

OKX_DAILY_TRADES_URL_TEMPLATE = (
    "https://www.okx.com/cdn/okex/traderecords/trades/daily/"
    "{yyyymmdd}/{symbol}-trades-{date}.zip"
)

OKX_HISTORICAL_TRADES_HEADERS = {
    "User-Agent": "AetherEdge/okx-historical-trades",
    "Accept": "application/zip,application/octet-stream,*/*",
}


class OkxHistoricalTradeDownloadError(IOError):
    def __init__(
        self,
        *,
        url: str,
        day: date,
        status: int | None = None,
        reason: str | None = None,
    ) -> None:
        self.url = url
        self.day = day
        self.status = status
        self.reason = reason
        details = []
        if status is not None:
            details.append(f"status={status}")
        if reason:
            details.append(f"reason={reason}")
        suffix = " " + " ".join(details) if details else ""
        super().__init__(f"failed to download OKX historical trades: {url}{suffix}")


def okx_raw_symbol_from_canonical(symbol: str) -> str:
    value = str(symbol).strip().upper()
    if value.endswith("-PERP"):
        return value[: -len("-PERP")] + "-SWAP"
    return value


def okx_daily_trade_url(*, raw_symbol: str, day: date) -> str:
    text = day.isoformat()
    return OKX_DAILY_TRADES_URL_TEMPLATE.format(
        yyyymmdd=text.replace("-", ""),
        symbol=raw_symbol,
        date=text,
    )


def okx_archive_date_from_utc_ms(ts_ms: int) -> date:
    """Map a UTC instant to OKX's UTC+8 daily archive filename date."""

    utc = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC)
    return (utc + timedelta(hours=8)).date()


def iter_okx_archive_dates_for_utc_range(
    start_ms: int, end_ms: int
) -> Iterator[date]:
    """Yield every UTC+8 archive date intersecting an inclusive UTC range."""

    if int(end_ms) < int(start_ms):
        raise ValueError("end_ms must be greater than or equal to start_ms")
    day = okx_archive_date_from_utc_ms(start_ms)
    end_day = okx_archive_date_from_utc_ms(end_ms)
    while day <= end_day:
        yield day
        day += timedelta(days=1)


@dataclass(frozen=True)
class OkxHistoricalTradeArchive:
    root: Path
    timeout_seconds: float = 20.0
    retries: int = 3
    retry_sleep_seconds: float = 1.0

    def local_path(self, *, raw_symbol: str, day: date) -> Path:
        return self.root / raw_symbol / f"{raw_symbol}-trades-{day.isoformat()}.zip"

    def ensure_daily_file(
        self,
        *,
        symbol: str,
        raw_symbol: str,
        day: date,
        allow_download: bool = True,
    ) -> HistoricalTradeFile:
        path = self.local_path(raw_symbol=raw_symbol, day=day)
        if path.exists() and path.stat().st_size > 0:
            return HistoricalTradeFile(
                exchange="okx",
                symbol=symbol,
                raw_symbol=raw_symbol,
                date=day.isoformat(),
                path=path,
                downloaded=False,
            )
        if not allow_download:
            raise FileNotFoundError(path)

        path.parent.mkdir(parents=True, exist_ok=True)
        url = okx_daily_trade_url(raw_symbol=raw_symbol, day=day)
        part = path.with_suffix(path.suffix + ".part")
        last_error: BaseException | None = None
        for attempt in range(max(1, int(self.retries))):
            try:
                request = Request(url, headers=OKX_HISTORICAL_TRADES_HEADERS)
                with urlopen(request, timeout=max(0.1, float(self.timeout_seconds))) as response:
                    payload = response.read()
                if not payload:
                    raise OkxHistoricalTradeDownloadError(
                        url=url,
                        day=day,
                        reason="empty response",
                    )
                part.write_bytes(payload)
                if part.stat().st_size <= 0:
                    raise OkxHistoricalTradeDownloadError(
                        url=url,
                        day=day,
                        reason="empty file",
                    )
                _validate_zip(part, url=url, day=day)
                part.replace(path)
                return HistoricalTradeFile(
                    exchange="okx",
                    symbol=symbol,
                    raw_symbol=raw_symbol,
                    date=day.isoformat(),
                    path=path,
                    downloaded=True,
                )
            except HTTPError as exc:
                _delete_if_exists(part)
                last_error = OkxHistoricalTradeDownloadError(
                    url=url,
                    day=day,
                    status=exc.code,
                    reason=str(exc.reason),
                )
            except (OSError, URLError, zipfile.BadZipFile, OkxHistoricalTradeDownloadError) as exc:
                _delete_if_exists(part)
                last_error = exc
            if attempt + 1 < max(1, int(self.retries)):
                logger.warning(
                    "OKX historical trades download retry | url=%s attempt=%s retries=%s error=%s",
                    url,
                    attempt + 1,
                    max(1, int(self.retries)),
                    last_error,
                )
                time.sleep(max(0.0, float(self.retry_sleep_seconds)))
        if isinstance(last_error, OkxHistoricalTradeDownloadError):
            raise last_error
        raise OkxHistoricalTradeDownloadError(
            url=url,
            day=day,
            reason=str(last_error) if last_error is not None else "unknown",
        ) from last_error


def _validate_zip(path: Path, *, url: str, day: date) -> None:
    try:
        with zipfile.ZipFile(path) as archive:
            bad_member = archive.testzip()
    except zipfile.BadZipFile as exc:
        raise OkxHistoricalTradeDownloadError(
            url=url,
            day=day,
            reason=f"bad zip: {exc}",
        ) from exc
    if bad_member is not None:
        raise OkxHistoricalTradeDownloadError(
            url=url,
            day=day,
            reason=f"bad zip member: {bad_member}",
        )


def _delete_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
