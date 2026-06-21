from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_new_reusable_domains_exist_with_models_and_ports():
    for relative in ["src/market_data", "src/order_management", "src/runtime"]:
        folder = ROOT / relative
        assert folder.exists(), f"missing domain folder: {relative}"
        assert (folder / "__init__.py").exists(), f"missing __init__.py in {relative}"
        assert (folder / "models.py").exists(), f"missing models.py in {relative}"
        assert (folder / "ports.py").exists(), f"missing ports.py in {relative}"


def test_market_data_domain_is_not_empty():
    text = (ROOT / "src" / "market_data" / "models.py").read_text(encoding="utf-8")
    assert "RangeBar" in text
    assert "Warmup" in text


def test_order_management_domain_is_not_empty():
    text = (ROOT / "src" / "order_management" / "models.py").read_text(encoding="utf-8")
    assert "OrderIntent" in text


def test_runtime_domain_is_not_empty():
    text = (ROOT / "src" / "runtime" / "models.py").read_text(encoding="utf-8")
    assert "RuntimePhase" in text
