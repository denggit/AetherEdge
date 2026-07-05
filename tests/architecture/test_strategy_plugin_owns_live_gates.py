from __future__ import annotations

import inspect
from pathlib import Path

from src.runtime.position_mode_gate import (
    PositionModeRequirement,
    resolve_position_mode_requirements,
)
from src.runtime.runner import LiveRuntimeRunner
from strategies.eth_portfolio_v1.strategy import Strategy


ROOT = Path(__file__).resolve().parents[2]
PLUGIN = ROOT / "strategies" / "eth_portfolio_v1"


def test_strategy_plugin_owns_live_gate_and_readiness() -> None:
    assert (PLUGIN / "preflight" / "live_gate.py").is_file()
    assert (PLUGIN / "preflight" / "readiness.py").is_file()
    assert not (
        ROOT / "src" / "runtime" / "portfolio_v1_live_gate.py"
    ).exists()
    assert not (
        ROOT / "src" / "runtime" / "portfolio_v1_readiness.py"
    ).exists()


def test_strategy_exposes_generic_live_hooks() -> None:
    strategy = Strategy()
    requirements = resolve_position_mode_requirements(strategy)
    assert requirements
    assert all(
        isinstance(item, PositionModeRequirement)
        for item in requirements
    )
    assert callable(strategy.live_smoke_provider)
    assert callable(strategy.live_preflight_provider)
    assert callable(strategy.startup_feature_backfill_providers)


def test_runner_consumes_only_generic_position_mode_requirements() -> None:
    source = inspect.getsource(
        LiveRuntimeRunner._check_strategy_position_mode_requirements
    ).lower()
    assert "resolve_position_mode_requirements" in source
    assert "eth_portfolio_v1" not in source
    assert "portfolio_v1" not in source
    assert "portfolio v1" not in source


def test_runner_consumes_only_generic_feature_backfill_hook() -> None:
    source = inspect.getsource(
        LiveRuntimeRunner._check_startup_feature_backfills
    ).lower()
    assert "startup_feature_backfill" in source
    assert ("mf_" + "feature") not in source
    assert "low_sweep" not in source


def test_strategy_plugin_owns_feature_backfill_provider() -> None:
    path = PLUGIN / "preflight" / "mf_feature_backfill.py"
    assert path.is_file()
    source = path.read_text(encoding="utf-8")
    assert "TradeFeatureBackfillSupervisor" in source


def test_live_tools_use_strategy_hooks_without_direct_plugin_imports() -> None:
    preflight = (
        ROOT / "tools" / "live_preflight_check.py"
    ).read_text(encoding="utf-8").lower()
    smoke = (
        ROOT / "tools" / "live_server_smoke.py"
    ).read_text(encoding="utf-8").lower()
    assert "strategies.eth_portfolio_v1" not in preflight
    assert "strategies.eth_portfolio_v1" not in smoke
    assert "src.runtime.portfolio_v1" not in smoke
    assert "live_preflight_provider" in preflight
    assert "live_smoke_provider" in smoke
