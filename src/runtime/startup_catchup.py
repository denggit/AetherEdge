from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

from src.runtime.heartbeat import RuntimeHeartbeat
from src.utils.log import get_logger

logger = get_logger(__name__)


# ── Configuration ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class StartupCatchupConfig:
    """Per-strategy configuration for the startup catch-up window.

    Defaults use a 300 s fresh-window, tight price
    guards, and all safety requirements enabled.
    """

    enabled: bool = True
    fresh_open_window_seconds: int = 300
    max_adverse_price_pct: Decimal = Decimal("0.0015")
    max_favorable_price_pct: Decimal = Decimal("0.0030")
    require_clean_reconciliation: bool = True
    require_no_active_position: bool = True
    require_no_pending_orders: bool = True
    require_range_aggregate: bool = True

    @classmethod
    def from_mapping(cls, data: dict | None) -> "StartupCatchupConfig":
        raw = dict(data or {})
        return cls(
            enabled=_bool(raw.get("enabled"), True),
            fresh_open_window_seconds=int(raw.get("fresh_open_window_seconds", 300)),
            max_adverse_price_pct=_dec(raw.get("max_adverse_price_pct"), Decimal("0.0015")),
            max_favorable_price_pct=_dec(raw.get("max_favorable_price_pct"), Decimal("0.0030")),
            require_clean_reconciliation=_bool(raw.get("require_clean_reconciliation"), True),
            require_no_active_position=_bool(raw.get("require_no_active_position"), True),
            require_no_pending_orders=_bool(raw.get("require_no_pending_orders"), True),
            require_range_aggregate=_bool(raw.get("require_range_aggregate"), True),
        )


# ── Decision model ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class StartupCatchupDecision:
    eligible: bool
    reason: str
    metadata: dict[str, object]


# ── Eligibility evaluation ──────────────────────────────────────────────────


def evaluate_startup_catchup_eligibility(
    *,
    now_ms: int,
    current_4h_open_time_ms: int,
    candidate_closed_bar_open_time_ms: int,
    candidate_closed_bar_close_time_ms: int,
    previous_heartbeat: RuntimeHeartbeat | None,
    current_price: Decimal,
    theoretical_open_price: Decimal,
    side: str,
    has_active_position: bool,
    has_pending_orders: bool,
    has_unresolved_follower_close: bool,
    already_executed: bool,
    range_aggregate_available: bool,
    config: StartupCatchupConfig,
) -> StartupCatchupDecision:
    """Evaluate whether a startup catch-up entry is safe and permitted.

    Returns a ``StartupCatchupDecision`` with ``eligible=True`` only when
    *every* guard passes.  The caller is responsible for reading the decision
    and acting on it — this function never calls downstream services.
    """

    meta: dict[str, object] = {}

    # 1. Config enabled?
    if not config.enabled:
        return StartupCatchupDecision(eligible=False, reason="startup_catchup_disabled", metadata=meta)

    # 2. Within the fresh 4H open window?
    fresh_window_age_ms = now_ms - current_4h_open_time_ms
    fresh_window_ms = config.fresh_open_window_seconds * 1000
    meta["fresh_window_age_seconds"] = fresh_window_age_ms // 1000
    meta["fresh_open_window_seconds"] = config.fresh_open_window_seconds
    meta["current_4h_open_time_ms"] = current_4h_open_time_ms

    if fresh_window_age_ms > fresh_window_ms:
        meta["outside_window_by_seconds"] = (fresh_window_age_ms - fresh_window_ms) // 1000
        return StartupCatchupDecision(
            eligible=False,
            reason="outside_fresh_4h_open_window",
            metadata=meta,
        )

    # 3. Heartbeat metadata (informational only — not a hard requirement).
    if previous_heartbeat is not None:
        meta["heartbeat_available"] = True
        meta["previous_runtime_id"] = previous_heartbeat.runtime_id
        meta["previous_pid"] = previous_heartbeat.pid
        downtime_ms = now_ms - previous_heartbeat.last_alive_ms
        meta["downtime_seconds"] = downtime_ms // 1000
        meta["downtime_ms"] = downtime_ms
    else:
        meta["heartbeat_available"] = False
        meta["downtime_seconds"] = -1  # unknown

    # 4. Candidate bar must correspond to the bar that just closed at
    #    current_4h_open_time_ms (the previous 4H bar).
    expected_close = current_4h_open_time_ms - 1
    expected_open = current_4h_open_time_ms - (4 * 60 * 60_000)
    if candidate_closed_bar_close_time_ms != expected_close:
        return StartupCatchupDecision(
            eligible=False,
            reason="candidate_bar_not_previous_4h",
            metadata={
                **meta,
                "expected_close_ms": expected_close,
                "actual_close_ms": candidate_closed_bar_close_time_ms,
            },
        )
    meta["candidate_bar_open_time_ms"] = candidate_closed_bar_open_time_ms
    meta["candidate_bar_close_time_ms"] = candidate_closed_bar_close_time_ms

    # 5. No active position.
    if config.require_no_active_position and has_active_position:
        return StartupCatchupDecision(
            eligible=False,
            reason="active_position_exists",
            metadata=meta,
        )

    # 6. No pending orders.
    if config.require_no_pending_orders and has_pending_orders:
        return StartupCatchupDecision(
            eligible=False,
            reason="pending_orders_exist",
            metadata=meta,
        )

    # 7. No unresolved follower close.
    if has_unresolved_follower_close:
        return StartupCatchupDecision(
            eligible=False,
            reason="unresolved_follower_close_exists",
            metadata=meta,
        )

    # 8. Bar not already executed.
    if already_executed:
        return StartupCatchupDecision(
            eligible=False,
            reason="already_executed",
            metadata=meta,
        )

    # 9. Range aggregate available.
    if config.require_range_aggregate and not range_aggregate_available:
        return StartupCatchupDecision(
            eligible=False,
            reason="range_aggregate_unavailable",
            metadata=meta,
        )

    # 10. Price guard.
    price_ok = _check_price_guard(
        current_price=current_price,
        theoretical_open_price=theoretical_open_price,
        side=side,
        max_adverse_pct=config.max_adverse_price_pct,
        max_favorable_pct=config.max_favorable_price_pct,
    )
    meta["current_price"] = str(current_price)
    meta["theoretical_open_price"] = str(theoretical_open_price)
    meta["side"] = side

    if not price_ok:
        meta["max_adverse_price_pct"] = str(config.max_adverse_price_pct)
        meta["max_favorable_price_pct"] = str(config.max_favorable_price_pct)
        deviation_pct = _deviation_pct(current_price, theoretical_open_price)
        meta["price_deviation_pct"] = str(deviation_pct)
        return StartupCatchupDecision(
            eligible=False,
            reason="price_guard_failed",
            metadata=meta,
        )

    meta["price_deviation_pct"] = str(_deviation_pct(current_price, theoretical_open_price))

    return StartupCatchupDecision(eligible=True, reason="all_guards_passed", metadata=meta)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _check_price_guard(
    *,
    current_price: Decimal,
    theoretical_open_price: Decimal,
    side: str,
    max_adverse_pct: Decimal,
    max_favorable_pct: Decimal,
) -> bool:
    side_normalized = str(side).strip().lower()
    if side_normalized == "long":
        upper = theoretical_open_price * (Decimal("1") + max_adverse_pct)
        lower = theoretical_open_price * (Decimal("1") - max_favorable_pct)
        return lower <= current_price <= upper
    elif side_normalized == "short":
        lower = theoretical_open_price * (Decimal("1") - max_adverse_pct)
        upper = theoretical_open_price * (Decimal("1") + max_favorable_pct)
        return lower <= current_price <= upper
    # Unknown side — refuse.
    return False


def _deviation_pct(current: Decimal, reference: Decimal) -> Decimal:
    if reference == Decimal("0"):
        return Decimal("0")
    return ((current - reference) / reference).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)


def _bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _dec(value: object, default: Decimal) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except Exception:
        return default
