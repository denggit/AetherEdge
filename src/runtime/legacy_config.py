from __future__ import annotations

from dataclasses import dataclass

from src.runtime.config import LiveRuntimeConfig
from src.runtime.market_data.range_config import RangeRuntimeConfig


@dataclass(frozen=True)
class LegacyLiveRuntimeConfig(LiveRuntimeConfig):
    """Thin read-only adapter for pre-refactor strategy tooling.

    Runtime orchestration and production composition use the typed Range
    module config directly.  These two aliases remain only so existing
    strategy-owned preflight providers do not need to change during the
    architecture migration.
    """

    _compat_range_config: RangeRuntimeConfig = RangeRuntimeConfig()

    @classmethod
    def wrap(
        cls,
        runtime: LiveRuntimeConfig,
        *,
        range_config: RangeRuntimeConfig,
    ) -> "LegacyLiveRuntimeConfig":
        return cls(
            app=runtime.app,
            mode=runtime.mode,
            warmup_enabled=runtime.warmup_enabled,
            background_queue_maxsize=runtime.background_queue_maxsize,
            scheduler_poll_seconds=runtime.scheduler_poll_seconds,
            closed_bar_interval=runtime.closed_bar_interval,
            closed_bar_buffer_ms=runtime.closed_bar_buffer_ms,
            closed_bar_retry_interval_ms=runtime.closed_bar_retry_interval_ms,
            closed_bar_missing_alert_after_ms=(
                runtime.closed_bar_missing_alert_after_ms
            ),
            producer_stale_timeout_ms=runtime.producer_stale_timeout_ms,
            master_follower_policy=runtime.master_follower_policy,
            startup_catchup=runtime.startup_catchup,
            _compat_range_config=range_config,
        )

    @property
    def range_checkpoint_db_path(self) -> str:
        return self._compat_range_config.checkpoint_db_path

    @property
    def market_data_db_path(self) -> str:
        return self._compat_range_config.market_data_db_path


__all__ = ["LegacyLiveRuntimeConfig"]
