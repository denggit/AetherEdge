from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

from src.signals import SignalAction
from strategies.eth_portfolio_v1.domain.models import Side
from strategies.eth_portfolio_v1.strategy import Strategy


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PLUGIN_ROOT = PROJECT_ROOT / "strategies" / "eth_portfolio_v1"


def test_v1_execution_sources_do_not_construct_global_stop_cancel() -> None:
    violations: list[str] = []
    for path in sorted(PLUGIN_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "SignalAction"
            and node.attr == "CANCEL_ALL_STOP_ORDERS"
            for node in ast.walk(tree)
        ):
            violations.append(str(path.relative_to(PROJECT_ROOT)))

    assert violations == []


def test_v1_replace_stop_signals_never_returns_global_stop_cancel() -> None:
    strategy = Strategy()
    strategy.position.open_master(
        side=Side.SHORT,
        entry_time_ms=1,
        avg_entry=Decimal("2500"),
        qty=Decimal("0.30"),
        stop_price=Decimal("2600"),
        entry_engine="MOMENTUM_V3",
        position_id="v1-lf-short-position",
    )
    leg = strategy.position.mark_leg_open(
        exchange="okx",
        avg_fill_price=Decimal("2500"),
        base_qty=Decimal("0.30"),
    )
    leg.stop_order_id = "okx-old-short-stop"

    signals = strategy._replace_stop_signals(
        target_exchanges=["okx"],
        quantity=Decimal("0.30"),
        stop_price=Decimal("2550"),
        reason="V1_LF_TRAILING_STOP_UPDATE",
        bar_close_time_ms=2,
    )

    assert signals[0].action is SignalAction.PLACE_STOP_LOSS_SHORT
    assert len(signals) == 1
    assert signals[0].metadata["scoped_cancel_pending"] is True
    assert all(signal.action is not SignalAction.CANCEL_ALL_STOP_ORDERS for signal in signals)
