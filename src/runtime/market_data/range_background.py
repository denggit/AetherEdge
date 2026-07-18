from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from src.market_data.range_checkpoint import SqliteRangeCheckpointStore
from src.runtime.range_backfill_supervisor import (
    RangeBackfillSupervisor,
    RangeBackfillSupervisorConfig,
)
from src.runtime.range_micro_repair_supervisor import RangeMicroRepairSupervisor
from src.runtime.range_speed_history import RangeSpeedHistoryRefresher
from src.runtime.market_data.range_config import RangeRuntimeConfig
from src.runtime.market_data.range_speed_runtime import ProviderFactory
from src.utils.log import get_logger


logger = get_logger(__name__)
MicroRepairFactory = Callable[[], RangeMicroRepairSupervisor]


@dataclass(frozen=True)
class RangeBackgroundConfig:
    symbol: str
    exchange: str
    range_pct: Decimal
    bucket_interval: str
    micro_repair_enabled: bool
    speed_refresh_enabled: bool
    speed_refresh_seconds: float
    speed_warning_seconds: float
    backfill_status_path: str
    backfill: RangeBackfillSupervisorConfig


class RangeBackgroundServices:
    """Own all optional Range monitoring and refresh background tasks."""

    def __init__(
        self,
        *,
        config: RangeBackgroundConfig,
        checkpoint_store: Callable[[], SqliteRangeCheckpointStore],
        provider: ProviderFactory,
        micro_repair_factory: MicroRepairFactory,
        backfill_supervisor: RangeBackfillSupervisor | None = None,
        micro_repair_supervisor: RangeMicroRepairSupervisor | None = None,
        speed_refresher: RangeSpeedHistoryRefresher | None = None,
    ) -> None:
        self.config = config
        self._checkpoint_store = checkpoint_store
        self.provider = provider
        self.micro_repair_factory = micro_repair_factory
        self.backfill_supervisor = backfill_supervisor
        self.micro_repair_supervisor = micro_repair_supervisor
        self.speed_refresher = speed_refresher

    def start(self, stop_event: asyncio.Event) -> None:
        if self.config.micro_repair_enabled:
            try:
                self.get_micro_repair_supervisor().start_monitor(
                    stop_event=stop_event
                )
            except Exception as exc:
                logger.warning(
                    "Range micro repair supervisor initialization failed | error=%s",
                    exc,
                )
        if self.config.backfill.enabled:
            try:
                self.get_backfill_supervisor().start_monitor(
                    stop_event=stop_event,
                    symbol=self.config.symbol,
                    exchange=self.config.exchange,
                    range_pct=str(self.config.range_pct),
                    bucket_interval=self.config.bucket_interval,
                )
            except Exception as exc:
                logger.warning(
                    "Range backfill supervisor initialization failed | error=%s",
                    exc,
                )
        if self.config.speed_refresh_enabled:
            try:
                self.get_speed_refresher().start(stop_event)
            except Exception as exc:
                logger.warning(
                    "Range speed history refresher initialization failed | error=%s",
                    exc,
                )

    async def stop(self) -> None:
        if self.speed_refresher is not None:
            await self.speed_refresher.stop()
        supervisor = self.backfill_supervisor
        if supervisor is not None:
            stop_async = getattr(supervisor, "stop_async", None)
            if callable(stop_async):
                await stop_async()
            else:
                stop = getattr(supervisor, "stop", None)
                if callable(stop):
                    await asyncio.to_thread(stop)
        micro = self.micro_repair_supervisor
        if micro is not None:
            stop_async = getattr(micro, "stop_async", None)
            if callable(stop_async):
                await stop_async()

    def get_backfill_supervisor(self) -> RangeBackfillSupervisor:
        if self.backfill_supervisor is None:
            self.backfill_supervisor = RangeBackfillSupervisor(
                self.config.backfill
            )
        return self.backfill_supervisor

    def get_micro_repair_supervisor(self) -> RangeMicroRepairSupervisor:
        if self.micro_repair_supervisor is None:
            self.micro_repair_supervisor = self.micro_repair_factory()
        return self.micro_repair_supervisor

    def get_speed_refresher(self) -> RangeSpeedHistoryRefresher:
        if self.speed_refresher is None:
            self.speed_refresher = RangeSpeedHistoryRefresher(
                provider=self.provider,
                store=self._checkpoint_store(),
                symbol=self.config.symbol,
                exchange=self.config.exchange,
                range_pct=str(self.config.range_pct),
                bucket_interval=self.config.bucket_interval,
                refresh_seconds=self.config.speed_refresh_seconds,
                warning_seconds=self.config.speed_warning_seconds,
                backfill_enabled=self.config.backfill.enabled,
                status_path=self.config.backfill_status_path,
            )
        return self.speed_refresher


def range_background_config(
    runtime: RangeRuntimeConfig,
    *,
    symbol: str,
    exchange: str,
    range_pct: Decimal,
    bucket_interval: str,
    repo_root: Path,
) -> RangeBackgroundConfig:
    return RangeBackgroundConfig(
        symbol=symbol,
        exchange=exchange,
        range_pct=range_pct,
        bucket_interval=bucket_interval,
        micro_repair_enabled=runtime.micro_repair_enabled,
        speed_refresh_enabled=runtime.speed_refresh_enabled,
        speed_refresh_seconds=runtime.speed_refresh_seconds,
        speed_warning_seconds=runtime.speed_status_warning_seconds,
        backfill_status_path=runtime.backfill_status_path,
        backfill=RangeBackfillSupervisorConfig(
            enabled=runtime.backfill_enabled,
            required_buckets=runtime.backfill_required_buckets,
            lookback_buckets=runtime.backfill_lookback_buckets,
            max_buckets_per_cycle=runtime.backfill_max_buckets_per_cycle,
            max_days_per_cycle=runtime.backfill_max_days_per_cycle,
            sleep_seconds=runtime.backfill_sleep_seconds,
            heartbeat_stale_seconds=runtime.backfill_heartbeat_stale_seconds,
            restart_cooldown_seconds=runtime.backfill_restart_cooldown_seconds,
            archive_publish_lag_hours=runtime.backfill_archive_publish_lag_hours,
            failure_cooldown_seconds=runtime.repair_failure_cooldown_seconds,
            archive_not_ready_cooldown_seconds=(
                runtime.repair_archive_not_ready_cooldown_seconds
            ),
            daily_retry_after_utc_hour=runtime.repair_daily_retry_after_utc_hour,
            monitor_seconds=runtime.backfill_monitor_seconds,
            status_path=Path(runtime.backfill_status_path),
            lock_path=Path(runtime.backfill_lock_path),
            low_priority=runtime.backfill_low_priority,
            chunksize=runtime.backfill_chunksize,
            raw_root=Path(runtime.backfill_raw_root),
            market_db_path=Path(runtime.market_data_db_path),
            checkpoint_db_path=Path(runtime.checkpoint_db_path),
            save_raw_trades=runtime.backfill_save_raw_trades,
            chunk_sleep_seconds=runtime.backfill_chunk_sleep_seconds,
            max_seconds_per_cycle=runtime.backfill_max_seconds_per_cycle,
            max_trades_per_cycle=runtime.backfill_max_trades_per_cycle,
            repo_root=repo_root,
        ),
    )


__all__ = [
    "RangeBackgroundConfig",
    "RangeBackgroundServices",
    "range_background_config",
]
