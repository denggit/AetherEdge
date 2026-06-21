from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _scan(folder: Path, forbidden_tokens: tuple[str, ...]) -> list[tuple[str, str]]:
    leaks: list[tuple[str, str]] = []
    for path in folder.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        for token in forbidden_tokens:
            if token in text:
                leaks.append((str(path.relative_to(ROOT)), token))
    return leaks


def test_platform_data_does_not_contain_internal_pipeline_terms():
    leaks = _scan(
        ROOT / "src" / "platform" / "data",
        (
            "RangeBarBuilder",
            "WarmupService",
            "OrderIntent",
            "eth_lf_portfolio_v8",
        ),
    )
    assert leaks == []


def test_platform_execution_does_not_contain_order_management_terms():
    leaks = _scan(
        ROOT / "src" / "platform" / "execution",
        (
            "OrderIntent",
            "OrderJournal",
            "StrategyPort",
            "eth_lf_portfolio_v8",
        ),
    )
    assert leaks == []


def test_utils_does_not_become_business_dumping_ground():
    leaks = _scan(
        ROOT / "src" / "utils",
        (
            "RangeBar",
            "OrderIntent",
            "WarmupService",
            "StrategyPort",
        ),
    )
    assert leaks == []
