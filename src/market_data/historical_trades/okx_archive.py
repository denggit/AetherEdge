from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import time
from urllib.error import URLError
from urllib.request import urlopen

from src.market_data.historical_trades.models import HistoricalTradeFile

OKX_DAILY_TRADES_URL_TEMPLATE = (
    "https://www.okx.com/cdn/okex/traderecords/trades/daily/"
    "{yyyymmdd}/{symbol}-trades-{date}.zip"
)


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
        last_error: BaseException | None = None
        for attempt in range(max(1, int(self.retries))):
            try:
                with urlopen(url, timeout=max(0.1, float(self.timeout_seconds))) as response:
                    payload = response.read()
                if not payload:
                    raise IOError(f"empty OKX historical trade archive: {url}")
                tmp = path.with_suffix(path.suffix + ".tmp")
                tmp.write_bytes(payload)
                tmp.replace(path)
                return HistoricalTradeFile(
                    exchange="okx",
                    symbol=symbol,
                    raw_symbol=raw_symbol,
                    date=day.isoformat(),
                    path=path,
                    downloaded=True,
                )
            except (OSError, URLError) as exc:
                last_error = exc
                if attempt + 1 < max(1, int(self.retries)):
                    time.sleep(max(0.0, float(self.retry_sleep_seconds)))
        raise IOError(f"failed to download OKX historical trades: {url}") from last_error
