"""Dataclasses and enums for the live startup reconciliation report."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ReconciliationVerdict(str, Enum):
    """Top-level verdict after startup reconciliation."""

    PASS = "pass"
    PASS_WITH_CLEANUP = "pass_with_cleanup"
    FAIL_NEEDS_RECONCILE = "fail_needs_reconcile"
    FAIL_UNRESOLVED_FOLLOWER_POSITION = "fail_unresolved_follower_position"
    FAIL_CONFIG = "fail_config"


@dataclass
class ReconciliationAction:
    """A concrete action produced by reconciliation (to be applied or reported)."""

    action_type: str
    # Values:
    #   close_stale_plan
    #   mark_leg_stale
    #   clear_fake_entry_order_id              → clears exchange order_id only (preserves client)
    #   clear_fake_stop_order_id               → clears exchange order_id only (preserves client)
    #   clear_fake_entry_exchange_order_id     → explicit exchange-order-id-only clear
    #   clear_fake_stop_exchange_order_id      → explicit exchange-order-id-only clear
    #   clear_all_entry_order_refs             → clears both entry order_id + entry client_order_id
    #   clear_all_stop_order_refs              → clears both stop order_id + stop client_order_id
    #   set_master_closed_follower_close_required
    #   block_new_entries_alert
    #   journal_event
    target: str  # e.g. "position_plan:{position_id}" or "leg:{position_id}:{exchange}"
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class FakeOrderRef:
    """A detected fake / invalid order reference in local state."""

    position_id: str
    exchange: str  # exchange value string e.g. "okx", "binance"
    leg_role: str  # "master" or "follower"
    field: str  # "entry_order_id", "stop_order_id", etc.
    value: str  # the actual fake value found
    reason: str  # "pattern_match" or "non_numeric_exchange_id"


@dataclass
class LiveStateReconciliationReport:
    """Result of startup state convergence check and repair."""

    checked_at_ms: int
    exchanges: tuple[str, ...]
    symbol: str
    ok: bool = False
    verdict: ReconciliationVerdict = ReconciliationVerdict.PASS
    issues: list[str] = field(default_factory=list)
    actions: list[ReconciliationAction] = field(default_factory=list)
    active_position_after: bool = False
    stale_plans_closed: int = 0
    fake_order_refs_found: list[FakeOrderRef] = field(default_factory=list)
    unresolved_follower_positions: int = 0
    alerts: list[dict[str, Any]] = field(default_factory=list)

    # Per-exchange snapshot summaries (for diagnostics)
    exchange_positions: dict[str, int] = field(default_factory=dict)
    exchange_open_orders: dict[str, int] = field(default_factory=dict)
    exchange_open_stops: dict[str, int] = field(default_factory=dict)
