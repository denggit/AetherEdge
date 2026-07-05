from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_platform_has_no_portfolio_v1_business_dependency() -> None:
    platform_root = ROOT / "src" / "platform"
    for path in platform_root.rglob("*.py"):
        source = path.read_text(encoding="utf-8").lower()
        assert "strategies.eth_portfolio_v1" not in source
        assert "eth_portfolio_v1" not in source
        assert "strategies." not in source


def test_runtime_position_mode_gate_depends_on_platform_not_strategy() -> None:
    source = (
        ROOT / "src" / "runtime" / "position_mode_gate.py"
    ).read_text(encoding="utf-8").lower()
    assert "from src.platform" in source
    assert "from strategies" not in source
    assert "import strategies" not in source
