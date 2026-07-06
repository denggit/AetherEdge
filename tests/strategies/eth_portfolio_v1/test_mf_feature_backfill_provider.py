from __future__ import annotations

from pathlib import Path
from types import MappingProxyType

from src.market_data.trade_features.backfill_supervisor import (
    TradeFeatureBackfillSupervisor,
)
from src.platform.config import ProjectEnvConfig
from strategies.eth_portfolio_v1.preflight.mf_feature_backfill import (
    PortfolioV1MfFeatureBackfillProvider,
    resolve_mf_feature_backfill_enabled,
)
from strategies.eth_portfolio_v1.strategy import Strategy


class _Supervisor:
    def __init__(self, result) -> None:
        self.result = result
        self.calls = 0

    def check_and_launch(self):
        self.calls += 1
        return self.result

    def scan_coverage(self):
        return dict(self.result.get("coverage", {}))


def _env(tmp_path: Path, **overrides: str) -> ProjectEnvConfig:
    values = {
        "AETHER_MF_FEATURE_BACKFILL_ENABLED": "true",
        "AETHER_MARKET_DATA_DB": str(tmp_path / "market.sqlite3"),
        "AETHER_MF_FEATURE_BACKFILL_STATUS_PATH": str(
            tmp_path / "status.json"
        ),
        "AETHER_MF_FEATURE_BACKFILL_LOCK_PATH": str(
            tmp_path / "worker.lock"
        ),
        "AETHER_RAW_TRADE_BACKFILL_GLOBAL_LOCK_PATH": str(
            tmp_path / "global.lock"
        ),
        "AETHER_RAW_TRADE_BACKFILL_GLOBAL_STATUS_PATH": str(
            tmp_path / "global-status.json"
        ),
        "AETHER_MF_FEATURE_BACKFILL_LOG_PATH": str(
            tmp_path / "worker.out"
        ),
        **overrides,
    }
    return ProjectEnvConfig(
        values=MappingProxyType(values),
        source_files=(),
        env_file=tmp_path / ".env",
        example_file=None,
    )


def _coverage(ready: bool) -> dict[str, bool]:
    return {
        "mf_signal_feature_ready": ready,
        "range_footprint_ready": ready,
        "tradebar_ready": ready,
        "fixed_time_footprint_ready": ready,
        "coverage_ready": ready,
        "large_share_samples_ready": ready,
        "large_share_sample_count": 129_600 if ready else 0,
    }


def _provider(
    tmp_path: Path,
    *,
    result,
    strategy: Strategy | None = None,
) -> PortfolioV1MfFeatureBackfillProvider:
    return PortfolioV1MfFeatureBackfillProvider(
        strategy=strategy or Strategy(),
        project_env=_env(tmp_path),
        supervisor=_Supervisor(result),
        readiness_reader=lambda: _coverage(False),
    )


def test_strategy_exposes_startup_feature_backfill_provider(
    tmp_path,
) -> None:
    strategy = Strategy()
    provider = _provider(tmp_path, result={}, strategy=strategy)
    strategy._mf_feature_backfill_provider = provider

    assert strategy.startup_feature_backfill_providers() == (
        provider,
    )


def test_direct_live_defaults_feature_backfill_enabled() -> None:
    assert resolve_mf_feature_backfill_enabled(
        {
            "AETHER_RUNTIME_MODE": "live_runtime",
            "AETHER_LIVE_TRADING": "true",
            "AETHER_DRY_RUN": "false",
        }
    )


def test_non_live_defaults_feature_backfill_disabled() -> None:
    assert not resolve_mf_feature_backfill_enabled(
        {
            "AETHER_RUNTIME_MODE": "legacy_app",
            "AETHER_LIVE_TRADING": "false",
        }
    )


def test_explicit_false_disables_feature_backfill() -> None:
    assert not resolve_mf_feature_backfill_enabled(
        {
            "AETHER_RUNTIME_MODE": "live_runtime",
            "AETHER_LIVE_TRADING": "true",
            "AETHER_DRY_RUN": "false",
            "AETHER_MF_FEATURE_BACKFILL_ENABLED": "false",
        }
    )


def test_provider_builds_generic_supervisor_with_worker_config(
    tmp_path,
) -> None:
    provider = PortfolioV1MfFeatureBackfillProvider(
        strategy=Strategy(),
        project_env=_env(tmp_path),
        readiness_reader=lambda: _coverage(False),
    )

    assert isinstance(
        provider.supervisor,
        TradeFeatureBackfillSupervisor,
    )
    config = provider.supervisor.config
    assert config.worker_script.name == (
        "mf_feature_backfill_worker.py"
    )
    assert config.market_db == str(tmp_path / "market.sqlite3")
    assert config.required_minutes == 129_600


def test_coverage_ready_emits_true_readiness(tmp_path) -> None:
    provider = _provider(
        tmp_path,
        result={
            "action": "none",
            "reason": "coverage_complete",
            "coverage": _coverage(True),
        },
    )

    result = provider.check_and_launch()
    event = provider.market_feature_events(result)[0]

    assert event.data["mf_signal_feature_ready"] is True
    assert event.data["mf_signal_ready"] is True


def test_coverage_gap_launch_result_is_preserved(tmp_path) -> None:
    provider = _provider(
        tmp_path,
        result={
            "action": "launched",
            "reason": "coverage_gap",
            "coverage": _coverage(False),
        },
    )

    result = provider.check_and_launch()

    assert result["action"] == "launched"
    assert provider.supervisor.calls == 1


def test_worker_already_running_does_not_duplicate_launch(
    tmp_path,
) -> None:
    provider = _provider(
        tmp_path,
        result={
            "action": "none",
            "reason": "worker_already_running",
            "coverage": _coverage(False),
        },
    )

    first = provider.check_and_launch()
    second = provider.check_and_launch()

    assert first["reason"] == "worker_already_running"
    assert second["action"] == "none"
    assert provider.supervisor.calls == 2


def test_provider_maps_coverage_to_strategy_readiness_event(
    tmp_path,
) -> None:
    provider = _provider(
        tmp_path,
        result={
            "action": "none",
            "reason": "coverage_gap",
            "coverage": {
                **_coverage(False),
                "tradebar_ready": True,
            },
        },
    )

    event = provider.market_feature_events(
        provider.check_and_launch()
    )[0]

    assert event.data["tradebar_ready"] is True
    assert event.data["range_footprint_ready"] is False
    assert event.data["mf_signal_ready"] is False


def test_not_ready_event_keeps_mf_signal_blocked(tmp_path) -> None:
    strategy = Strategy()
    provider = _provider(
        tmp_path,
        strategy=strategy,
        result={
            "action": "none",
            "reason": "coverage_gap",
            "coverage": _coverage(False),
        },
    )
    event = provider.market_feature_events(
        provider.check_and_launch()
    )[0]

    signals = strategy.mf_feature_observer.on_market_feature(event)

    assert signals == ()
    assert strategy.last_mf_signal_audit["data_ready"] is False
