from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from src.market_data.events import MarketFeatureEvent
from src.market_data.trade_features.backfill_supervisor import (
    TradeFeatureBackfillConfig,
    TradeFeatureBackfillSupervisor,
)
from src.platform.config import (
    ProjectEnvConfig,
    get_project_env_config,
)
from src.platform.exchanges.models import ExchangeName
from src.runtime.features import trade_feature_readiness_feature
from strategies.eth_portfolio_v1.domain.mf_data import (
    MfDataReadiness,
)


class PortfolioV1MfFeatureBackfillProvider:
    """Strategy-owned bridge from trade coverage to MF readiness."""

    name = "portfolio_v1_trade_feature_backfill"

    def __init__(
        self,
        *,
        strategy: object,
        project_env: ProjectEnvConfig | None = None,
        supervisor: object | None = None,
        readiness_reader: Callable[
            [], Mapping[str, Any]
        ] | None = None,
    ) -> None:
        self.strategy = strategy
        self.project_env = (
            project_env
            if project_env is not None
            else get_project_env_config()
        )
        self.enabled = resolve_mf_feature_backfill_enabled(
            self.project_env.values
        )
        self.required_minutes = effective_mf_required_minutes(
            self.strategy.config
        )
        self.poll_interval_seconds = max(
            10.0,
            self.project_env.get_float(
                "AETHER_MF_FEATURE_READINESS_POLL_SECONDS",
                60.0,
            ),
        )
        self._readiness_reader = (
            readiness_reader
            if readiness_reader is not None
            else self._build_readiness_reader()
        )
        self.supervisor = (
            supervisor
            if supervisor is not None
            else self._build_supervisor()
        )

    def check_and_launch(self) -> Mapping[str, Any]:
        if not self.enabled:
            return {
                "enabled": False,
                "action": "none",
                "reason": "mf_supervisor_disabled",
                "coverage": {},
            }
        try:
            return {
                "enabled": True,
                **dict(self.supervisor.check_and_launch()),
            }
        except Exception as exc:
            return self.failure_result(exc)

    def poll_readiness(self) -> Mapping[str, Any]:
        try:
            if self.enabled:
                scan = getattr(
                    self.supervisor,
                    "scan_coverage",
                    None,
                )
                if callable(scan):
                    coverage = dict(scan())
                else:
                    coverage = dict(self._readiness_reader())
            else:
                coverage = dict(self._readiness_reader())
            return {
                "enabled": self.enabled,
                "action": "none",
                "reason": "periodic_coverage_scan",
                "coverage": coverage,
            }
        except Exception as exc:
            return self.failure_result(exc)

    def market_feature_events(
        self,
        result: Mapping[str, Any],
    ) -> Sequence[MarketFeatureEvent]:
        coverage_value = result.get("coverage", {})
        coverage = (
            dict(coverage_value)
            if isinstance(coverage_value, Mapping)
            else {}
        )
        readiness = {
            "mf_signal_feature_ready": bool(
                coverage.get(
                    "mf_signal_feature_ready",
                    False,
                )
            ),
            "range_footprint_ready": bool(
                coverage.get("range_footprint_ready", False)
            ),
            "tradebar_ready": bool(
                coverage.get("tradebar_ready", False)
            ),
            "fixed_time_footprint_ready": bool(
                coverage.get(
                    "fixed_time_footprint_ready",
                    False,
                )
            ),
            "coverage_ready": bool(
                coverage.get("coverage_ready", False)
            ),
            "large_share_samples_ready": bool(
                coverage.get("large_share_samples_ready", False)
            ),
            "large_share_sample_count": int(
                coverage.get("large_share_sample_count", 0) or 0
            ),
            "live_freshness_required": True,
            "live_freshness_max_age_ms": 300_000,
            "mf_freshness_mode": "live_freshness_at_event",
            "reason": result.get("reason"),
            "action": result.get("action"),
        }
        readiness["mf_signal_ready"] = all(
            readiness[field]
            for field in (
                "mf_signal_feature_ready",
                "range_footprint_ready",
                "tradebar_ready",
                "fixed_time_footprint_ready",
                "coverage_ready",
                "large_share_samples_ready",
            )
        )
        return (
            trade_feature_readiness_feature(
                symbol=self.strategy.config.symbol,
                exchange=ExchangeName(
                    self.strategy.config.data_exchange
                ),
                event_time_ms=int(time.time() * 1000),
                readiness=readiness,
                source=(
                    "portfolio_v1_mf_feature_backfill_provider"
                ),
            ),
        )

    def failure_result(
        self,
        exc: BaseException,
    ) -> Mapping[str, Any]:
        return {
            "enabled": self.enabled,
            "action": "none",
            "reason": "mf_supervisor_failed",
            "error": f"{type(exc).__name__}: {exc}",
            "coverage": {},
        }

    def _build_supervisor(self) -> TradeFeatureBackfillSupervisor:
        root = Path(__file__).resolve().parents[3]
        config = self.strategy.config
        trade_config = TradeFeatureBackfillConfig(
            symbol=config.symbol,
            exchange=config.data_exchange,
            worker_script=(
                root / "tools" / "mf_feature_backfill_worker.py"
            ),
            repository_root=root,
            market_db=self.project_env.get(
                "AETHER_MARKET_DATA_DB",
                "data/market_data/aether_market_data.sqlite3",
            ),
            status_path=Path(
                self.project_env.get(
                    "AETHER_MF_FEATURE_BACKFILL_STATUS_PATH",
                    "data/state/mf_feature_backfill_status.json",
                )
            ),
            global_lock_path=Path(
                self.project_env.get(
                    "AETHER_RAW_TRADE_BACKFILL_GLOBAL_LOCK_PATH",
                    "data/state/raw_trade_backfill_global.lock",
                )
            ),
            global_status_path=Path(
                self.project_env.get(
                    "AETHER_RAW_TRADE_BACKFILL_GLOBAL_STATUS_PATH",
                    "data/state/raw_trade_backfill_global_status.json",
                )
            ),
            worker_log_path=Path(
                self.project_env.get(
                    "AETHER_MF_FEATURE_BACKFILL_LOG_PATH",
                    "logs/mf_feature_backfill_worker.out",
                )
            ),
            required_minutes=max(
                self.required_minutes,
                self.project_env.get_int(
                    "AETHER_MF_FEATURE_BACKFILL_REQUIRED_MINUTES",
                    self.required_minutes,
                ),
            ),
            max_seconds_per_cycle=self.project_env.get_float(
                "AETHER_MF_FEATURE_BACKFILL_MAX_SECONDS_PER_CYCLE",
                60.0,
            ),
            raw_root=self.project_env.get(
                "AETHER_RANGE_BACKFILL_RAW_ROOT",
                "data/okx/raw/trades",
            ),
            contract_value=self.project_env.get(
                "AETHER_MF_FEATURE_CONTRACT_VALUE",
                "0.01",
            ),
            price_bucket_size=self.project_env.get(
                "AETHER_MF_FEATURE_PRICE_BUCKET_SIZE",
                "1",
            ),
            range_footprint_range_pct=self.project_env.get(
                "AETHER_MF_RANGE_FOOTPRINT_RANGE_PCT",
                str(config.mf.range_pct),
            ),
            range_footprint_price_step=self.project_env.get(
                "AETHER_MF_RANGE_FOOTPRINT_PRICE_STEP",
                str(config.mf.range_price_step),
            ),
            range_footprint_warmup_days=self.project_env.get_int(
                "AETHER_MF_RANGE_FOOTPRINT_WARMUP_DAYS",
                1,
            ),
            large_trade_threshold=self.project_env.get(
                "AETHER_MF_FEATURE_LARGE_TRADE_THRESHOLD",
                "10000",
            ),
        )
        return TradeFeatureBackfillSupervisor(
            config=trade_config,
            coverage_reader=self._readiness_reader,
        )

    def _build_readiness_reader(
        self,
    ) -> Callable[[], Mapping[str, Any]]:
        config = self.strategy.config
        readiness = MfDataReadiness(
            symbol=config.symbol,
            exchange=config.data_exchange,
            store_path=self.project_env.get(
                "AETHER_MARKET_DATA_DB",
                "data/market_data/aether_market_data.sqlite3",
            ),
            required_minutes=max(
                self.required_minutes,
                self.project_env.get_int(
                    "AETHER_MF_FEATURE_BACKFILL_REQUIRED_MINUTES",
                    self.required_minutes,
                ),
            ),
            worker_status_path=self.project_env.get(
                "AETHER_MF_FEATURE_BACKFILL_STATUS_PATH",
                "data/state/mf_feature_backfill_status.json",
            ),
            global_lock_path=self.project_env.get(
                "AETHER_RAW_TRADE_BACKFILL_GLOBAL_LOCK_PATH",
                "data/state/raw_trade_backfill_global.lock",
            ),
            range_pct=str(config.mf.range_pct),
            price_step=str(config.mf.range_price_step),
            large_share_min_samples=config.mf.large_share_min_samples,
            large_share_window_days=config.mf.large_share_window_days,
        )
        return readiness.readiness


def effective_mf_required_minutes(config: object) -> int:
    mf = getattr(config, "mf")
    return max(
        int(mf.decision_buffer_minutes),
        int(mf.large_share_min_samples),
        int(mf.large_share_window_days) * 1_440,
    )


def resolve_mf_feature_backfill_enabled(
    values: Mapping[str, str],
) -> bool:
    raw = values.get("AETHER_MF_FEATURE_BACKFILL_ENABLED")
    if raw not in (None, ""):
        return str(raw).strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }
    return (
        str(values.get("AETHER_RUNTIME_MODE", "")).strip().lower()
        == "live_runtime"
        and str(values.get("AETHER_LIVE_TRADING", "")).strip().lower()
        in {"1", "true", "yes", "y", "on"}
        and str(values.get("AETHER_DRY_RUN", "")).strip().lower()
        not in {"1", "true", "yes", "y", "on"}
    )


__all__ = [
    "PortfolioV1MfFeatureBackfillProvider",
    "effective_mf_required_minutes",
    "resolve_mf_feature_backfill_enabled",
]
