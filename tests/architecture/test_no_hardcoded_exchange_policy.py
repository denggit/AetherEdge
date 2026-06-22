from __future__ import annotations

from pathlib import Path


BUSINESS_DIRS = (
    Path("src/order_management"),
    Path("src/runtime"),
    Path("src/app"),
    Path("src/strategy"),
    Path("src/signals"),
    Path("src/planner"),
    Path("src/reconcile"),
    Path("src/market_data"),
)

FORBIDDEN_POLICY_DEFAULTS = (
    "ExchangeName.OKX",
    "ExchangeName.BINANCE",
)


def test_business_modules_do_not_hardcode_okx_binance_exchange_policy() -> None:
    offenders: list[str] = []
    for directory in BUSINESS_DIRS:
        for path in directory.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            for token in FORBIDDEN_POLICY_DEFAULTS:
                if token in text:
                    offenders.append(f"{path}:{token}")

    assert offenders == []
