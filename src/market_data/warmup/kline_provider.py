from __future__ import annotations

from datetime import datetime, timezone

from src.market_data.models import TimeRange
from src.market_data.ports import KlineRepository
from src.market_data.warmup.gap_detector import interval_to_ms
from src.market_data.warmup.historical_klines import BackfillDiagnostics, HistoricalKlineProvider
from src.platform.data.models import MarketKline
from src.platform.data.ports import MarketDataFeed
from src.platform.exchanges.models import ExchangeName
from src.platform.exchanges.symbols import to_exchange_symbol
from src.platform.markets import get_market_profile
from src.utils.log import get_logger

logger = get_logger(__name__)

# OKX history-candles returns at most 100 candles per request.
_DEFAULT_PAGE_LIMIT = 100
# Safety cap to prevent runaway pagination (365 days × 6 bars/day ≈ 2190).
_MAX_PAGES = 30


class MarketDataKlineProvider:
    """Fetch historical closed klines via the platform MarketDataFeed with
    correct backward pagination for the OKX history-candles endpoint.

    This provider translates canonical symbols (e.g. ``ETH-USDT-PERP``) to
    exchange-specific raw symbols internally and returns only closed candles
    identified by the canonical symbol.  It is designed as a fallback when
    the local KlineStore does not contain enough records for a safe live
    startup.
    """

    def __init__(
        self,
        *,
        data_feed: MarketDataFeed,
        repository: KlineRepository,
        page_limit: int = _DEFAULT_PAGE_LIMIT,
        max_pages: int = _MAX_PAGES,
    ) -> None:
        self._data_feed = data_feed
        self._repository = repository
        self._page_limit = min(max(int(page_limit), 1), 100)
        self._max_pages = int(max_pages)

    @property
    def exchange(self) -> ExchangeName:
        return self._data_feed.exchange

    async def fetch_klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_open_ms: int,
        end_open_ms: int,
    ) -> list[MarketKline]:
        """Fetch all closed klines in [start_open_ms, end_open_ms].

        Paginates backwards from *end_open_ms* using the exchange history
        endpoint so that long time ranges (e.g. 365 days of 4H candles) are
        covered correctly.
        """
        step_ms = interval_to_ms(interval)
        collected: dict[int, MarketKline] = {}
        before_ms = end_open_ms

        for page in range(self._max_pages):
            rows = await self._data_feed.fetch_klines(
                interval=interval,
                limit=self._page_limit,
                start_time_ms=start_open_ms,
                end_time_ms=before_ms,
                use_cache=False,
                oldest_first=False,
            )
            # Keep only closed canonical-symbol candles within the window.
            page_closed = [
                row
                for row in rows
                if row.is_closed
                and row.symbol == symbol
                and start_open_ms <= row.open_time_ms <= end_open_ms
            ]
            if not page_closed:
                break

            before_count = len(collected)
            for row in page_closed:
                collected[row.open_time_ms] = row  # dedup by open_time
            after_count = len(collected)

            logger.debug(
                "Kline provider page %s | before_ms=%s rows=%s new=%s total=%s",
                page + 1,
                before_ms,
                len(page_closed),
                after_count - before_count,
                after_count,
            )

            # Advance cursor backward: the next page should end before the
            # earliest open_time we just received.
            earliest_in_page = min(row.open_time_ms for row in page_closed)
            if earliest_in_page <= start_open_ms:
                break
            new_before = earliest_in_page - 1
            if new_before >= before_ms:
                break
            before_ms = new_before
        else:
            logger.warning(
                "Kline provider reached max_pages=%s | collected=%s start_open_ms=%s end_open_ms=%s",
                self._max_pages,
                len(collected),
                start_open_ms,
                end_open_ms,
            )

        return sorted(collected.values(), key=lambda row: row.open_time_ms)

    async def backfill_and_reload(
        self,
        *,
        symbol: str,
        interval: str,
        time_range: TimeRange,
        min_records: int,
        store_class: str = "",
        store_path: str = "",
    ) -> BackfillDiagnostics:
        """Backfill missing klines, persist to the repository, reload, and
        return structured diagnostics."""
        profile = get_market_profile(symbol)
        raw_aliases = tuple(
            f"{exchange.value}:{profile.raw_symbol(exchange)}"
            for exchange in profile.exchange_symbols
        )

        start_utc = datetime.fromtimestamp(time_range.start_time_ms / 1000, tz=timezone.utc).isoformat()
        end_utc = datetime.fromtimestamp(time_range.end_time_ms / 1000, tz=timezone.utc).isoformat()

        records_before = len(
            self._repository.load(symbol=symbol, interval=interval, time_range=time_range)
        )

        diag = BackfillDiagnostics(
            symbol=symbol,
            raw_aliases=raw_aliases,
            interval=interval,
            start_open_ms=time_range.start_time_ms,
            end_open_ms=time_range.end_time_ms,
            start_open_utc=start_utc,
            end_open_utc=end_utc,
            records_loaded_before=records_before,
            records_loaded_after=records_before,
            min_records=min_records,
            kline_store_class=store_class,
            kline_store_path=store_path,
            provider_used=type(self).__name__,
            fetched_records=0,
            saved_records=0,
            success=False,
        )

        fetched = await self.fetch_klines(
            symbol=symbol,
            interval=interval,
            start_open_ms=time_range.start_time_ms,
            end_open_ms=time_range.end_time_ms,
        )
        diag.fetched_records = len(fetched)

        if fetched:
            saved = self._repository.save(fetched)
            diag.saved_records = saved

        records_after = len(
            self._repository.load(symbol=symbol, interval=interval, time_range=time_range)
        )
        diag.records_loaded_after = records_after
        diag.success = records_after >= min_records

        logger.info(
            "Kline backfill diagnostics | symbol=%s interval=%s "
            "range=[%s, %s] utc=[%s, %s] "
            "before=%s fetched=%s saved=%s after=%s min=%s success=%s",
            symbol,
            interval,
            time_range.start_time_ms,
            time_range.end_time_ms,
            start_utc,
            end_utc,
            records_before,
            diag.fetched_records,
            diag.saved_records,
            records_after,
            min_records,
            diag.success,
        )

        return diag
