from __future__ import annotations

from src.strategy import RecoverableStrategyPort, StrategyRecoveryContext


def test_recoverable_strategy_port_and_context_are_exported():
    assert RecoverableStrategyPort is not None
    ctx = StrategyRecoveryContext(snapshots=(), reconcile_reports=(), order_intent_ids=("intent-1",))
    assert ctx.order_intent_ids == ("intent-1",)
