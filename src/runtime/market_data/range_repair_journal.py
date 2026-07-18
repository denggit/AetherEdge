from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from src.app.alerts import AppAlert
from src.market_data.range_repair import (
    JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
    RangeRepairJournalWriter,
    RangeRepairTrade,
    SqliteRangeRepairJournalStore,
)
from src.platform.data.models import MarketTrade
from src.platform.exchanges.models import ExchangeName
from src.runtime.range_repair_bootstrap import RangeRepairBootstrapResult
from src.utils.log import get_logger


logger = get_logger(__name__)
AlertEmitter = Callable[[AppAlert], None]


@dataclass(frozen=True)
class RangeRepairJournalConfig:
    symbol: str
    exchange: ExchangeName
    range_pct: Decimal
    bucket_interval_ms: int


class RangeRepairJournalSession:
    """Own the live-trade repair journal state for one Range module."""

    def __init__(
        self,
        *,
        config: RangeRepairJournalConfig,
        emit_alert: AlertEmitter,
        store: SqliteRangeRepairJournalStore | None = None,
        writer: RangeRepairJournalWriter | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self.config = config
        self.emit_alert = emit_alert
        self.store = store
        self.writer = writer
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.bucket_start_ms: int | None = None
        self.checkpoint_last_trade_ts_ms: int | None = None
        self.first_live_submitted = False
        self.finalize_submitted = False
        self.append_failure_warned = False

    def activate(self, result: RangeRepairBootstrapResult) -> None:
        self.store = result.journal_store
        self.writer = result.journal_writer
        if result.journal_bucket_start_ms is None:
            return
        self.bucket_start_ms = result.journal_bucket_start_ms
        self.checkpoint_last_trade_ts_ms = (
            result.checkpoint_last_trade_ts_ms
        )
        self.first_live_submitted = False
        self.finalize_submitted = False
        self.append_failure_warned = False

    def set_resources(
        self,
        *,
        store: SqliteRangeRepairJournalStore | None,
        writer: RangeRepairJournalWriter | None,
    ) -> None:
        self.store = store
        self.writer = writer

    def append(self, trade: MarketTrade) -> None:
        trade_time_ms = trade.trade_time_ms or trade.event_time_ms
        bucket = self.bucket_start_ms
        checkpoint_ts = self.checkpoint_last_trade_ts_ms
        writer = self.writer
        if (
            writer is None
            or bucket is None
            or checkpoint_ts is None
            or trade_time_ms is None
            or trade.exchange != self.config.exchange
            or getattr(trade.source, "value", str(trade.source))
            != "websocket"
            or trade_time_ms <= checkpoint_ts
            or trade_time_ms < bucket
            or trade_time_ms >= bucket + self.config.bucket_interval_ms
        ):
            return
        if self.finalize_submitted:
            self.invalidate(
                bucket_start_ms=bucket,
                status=JOURNAL_INVALID_MARKET_QUEUE_DRAIN_INCOMPLETE,
                reason="live trade arrived after journal finalize",
            )
        now_ms = self._clock_ms()
        if not self.first_live_submitted:
            accepted = writer.submit_first_live(
                exchange=trade.exchange.value,
                symbol=trade.symbol,
                range_pct=str(self.config.range_pct),
                bucket_start_ms=bucket,
                trade_time_ms=trade_time_ms,
                trade_id=trade.trade_id,
                recorded_at_ms=now_ms,
            )
            if accepted:
                self.first_live_submitted = True
        accepted = writer.submit_trade(
            RangeRepairTrade(
                exchange=trade.exchange.value,
                symbol=trade.symbol,
                range_pct=str(self.config.range_pct),
                bucket_start_ms=bucket,
                trade_time_ms=trade_time_ms,
                event_time_ms=trade.event_time_ms,
                trade_id=trade.trade_id,
                raw_symbol=trade.raw_symbol,
                side=getattr(trade.side, "value", str(trade.side)),
                price=str(trade.price),
                quantity=str(trade.quantity),
                source=getattr(trade.source, "value", str(trade.source)),
                created_at_ms=now_ms,
            )
        )
        if not accepted and not self.append_failure_warned:
            self.append_failure_warned = True
            logger.warning(
                "Range repair journal trade dropped | symbol=%s "
                "exchange=%s bucket_start_ms=%s trade_time_ms=%s",
                trade.symbol,
                trade.exchange.value,
                bucket,
                trade_time_ms,
            )
            self.emit_alert(
                AppAlert(
                    subject="AetherEdge range repair journal trade dropped",
                    content=(
                        f"symbol={trade.symbol}\n"
                        f"bucket_start_ms={bucket}\n"
                        f"trade_time_ms={trade_time_ms}"
                    ),
                    severity="warning",
                )
            )

    def invalidate(
        self,
        *,
        bucket_start_ms: int,
        status: str,
        reason: str,
        dropped_trades: int = 0,
    ) -> None:
        writer = self.writer
        if writer is None or self.bucket_start_ms != bucket_start_ms:
            return
        writer.submit_invalidation(
            exchange=self.config.exchange.value,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            bucket_start_ms=bucket_start_ms,
            status=status,
            last_error=reason,
            dropped_trades=dropped_trades,
        )

    def finalize(self, *, bucket_start_ms: int, finalized_at_ms: int) -> None:
        writer = self.writer
        if writer is None or self.bucket_start_ms != bucket_start_ms:
            return
        writer.submit_finalize(
            exchange=self.config.exchange.value,
            symbol=self.config.symbol,
            range_pct=str(self.config.range_pct),
            bucket_start_ms=bucket_start_ms,
            finalized_at_ms=finalized_at_ms,
        )
        self.finalize_submitted = True

    async def stop(self) -> None:
        writer = self.writer
        if writer is None:
            return
        stop = getattr(writer, "stop", None)
        if callable(stop):
            await asyncio.to_thread(stop, flush=True)


__all__ = ["RangeRepairJournalConfig", "RangeRepairJournalSession"]
