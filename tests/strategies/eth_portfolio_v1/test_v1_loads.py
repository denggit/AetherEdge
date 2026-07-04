from __future__ import annotations

import importlib
import importlib.abc
import sys

from src.strategy.loader import load_strategy


V1_PREFIX = "strategies.eth_portfolio_v1"
BLOCKED_PREFIXES = (
    "strategies.eth_lf_portfolio_v8",
    "strategies.eth_lf_portfolio_v10b",
)
EXPECTED_RUNTIME_REQUIREMENTS = {
    "closed_kline",
    "trades",
    "range_bars",
    "account_state",
    "order_state",
}


class _BlockLegacyPortfolioImports(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path=None, target=None):
        if any(fullname == prefix or fullname.startswith(f"{prefix}.") for prefix in BLOCKED_PREFIXES):
            raise ModuleNotFoundError(f"blocked legacy strategy import: {fullname}")
        return None


def test_v1_loads_when_v8_and_v10b_imports_are_blocked(monkeypatch) -> None:
    prefixes_to_clear = (V1_PREFIX, *BLOCKED_PREFIXES)
    for module_name in tuple(sys.modules):
        if any(module_name == prefix or module_name.startswith(f"{prefix}.") for prefix in prefixes_to_clear):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    monkeypatch.setattr(sys, "meta_path", [_BlockLegacyPortfolioImports(), *sys.meta_path])

    package = importlib.import_module(V1_PREFIX)
    strategy = load_strategy(f"{V1_PREFIX}:Strategy")

    assert strategy.__class__ is package.Strategy
    assert strategy.__class__.__module__ == f"{V1_PREFIX}.strategy"
    assert strategy.config.strategy_id == "eth_portfolio_v1"


def test_v1_runtime_requirements_include_live_inputs() -> None:
    strategy = load_strategy(f"{V1_PREFIX}:Strategy")

    assert EXPECTED_RUNTIME_REQUIREMENTS <= strategy.runtime_requirements().keys()


def test_v1_signal_mapper_default_identity() -> None:
    signal_mapper = importlib.import_module(f"{V1_PREFIX}.execution.signal_mapper")

    assert signal_mapper.SignalMapperConfig().strategy_id == "eth_portfolio_v1"
