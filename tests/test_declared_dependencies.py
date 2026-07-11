from __future__ import annotations

from importlib import import_module

import pytest


@pytest.mark.parametrize(
    "module_name",
    ("numpy", "pandas", "pytest", "pytest_asyncio", "requests", "websockets"),
)
def test_declared_third_party_dependency_imports(module_name: str) -> None:
    import_module(module_name)


@pytest.mark.parametrize(
    "module_name",
    (
        "scripts.run_live",
        "tools.run_live",
        "tools.live_preflight_check",
        "tools.live_server_smoke",
        "src.platform",
        "src.runtime",
        "src.order_management",
        "src.market_data",
    ),
)
def test_maintained_entrypoint_and_platform_imports(module_name: str) -> None:
    import_module(module_name)


def test_portfolio_v1_strategy_imports() -> None:
    strategy_module = import_module("strategies.eth_portfolio_v1")
    assert strategy_module.Strategy is not None
