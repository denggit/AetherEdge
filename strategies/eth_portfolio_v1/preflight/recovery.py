from __future__ import annotations

from decimal import Decimal
from typing import Any, Mapping, Sequence

from strategies.eth_portfolio_v1.domain.recovery import (
    audit_portfolio_v1_plans,
)


def audit_preflight_recovery(
    *,
    plan_store: object,
    snapshots: Sequence[object] = (),
) -> dict[str, Any]:
    """Build the strategy-owned recovery audit used by preflight."""

    audit = audit_portfolio_v1_plans(
        tuple(plan_store.serialize_active_positions())
    )
    active_plan_count = int(audit["plans"]["active_count"])
    active_exchange_positions = sum(
        1
        for snapshot in snapshots
        for position in getattr(snapshot, "positions", ())
        if Decimal(str(position.quantity)) != 0
    )
    issues = audit["issues"]
    if snapshots and active_plan_count and not active_exchange_positions:
        issues.append("local_active_plans_exchange_flat")
    elif snapshots and not active_plan_count and active_exchange_positions:
        issues.append("exchange_position_without_local_plan")
    if issues:
        audit["recovery_ok"] = False
        audit["manual_required"] = True
        audit["startup_blocked"] = True
        audit["hard_fail"] = any(
            str(issue).startswith(
                (
                    "exchange_",
                    "local_active_plans_exchange_flat",
                )
            )
            for issue in issues
        )
    audit["exchange_snapshot_summary"] = {
        "snapshot_count": len(snapshots),
        "active_position_count": active_exchange_positions,
    }
    return audit


__all__ = ["audit_preflight_recovery"]
