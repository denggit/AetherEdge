from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
STRATEGY_MARKERS = (
    "eth_portfolio_v1",
    "portfolio_v1",
    "portfolio v1",
)


def _assert_generic_tree(relative_root: str) -> None:
    root = ROOT / relative_root
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8").lower()
        for marker in STRATEGY_MARKERS:
            assert marker not in source, (
                f"{path.relative_to(ROOT)} contains {marker!r}"
            )
        assert "strategies.eth_portfolio_v1" not in source


def test_runtime_contains_no_strategy_specific_logic() -> None:
    _assert_generic_tree("src/runtime")


def test_framework_layers_contain_no_strategy_specific_logic() -> None:
    for relative_root in (
        "src/platform",
        "src/order_management",
        "src/market_data",
    ):
        _assert_generic_tree(relative_root)
