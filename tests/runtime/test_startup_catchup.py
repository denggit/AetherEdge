from __future__ import annotations

from decimal import Decimal

import pytest

from src.runtime.heartbeat import RuntimeHeartbeat
from src.runtime.startup_catchup import (
    StartupCatchupConfig,
    StartupCatchupDecision,
    evaluate_startup_catchup_eligibility,
)

H4_MS = 4 * 60 * 60_000  # 4 hours in ms
FIVE_MIN_MS = 5 * 60_000

# ── Helpers ──────────────────────────────────────────────────────────────────


def _base_config(**overrides) -> StartupCatchupConfig:
    kwargs = dict(
        enabled=True,
        fresh_open_window_seconds=300,
        max_adverse_price_pct=Decimal("0.0015"),
        max_favorable_price_pct=Decimal("0.0030"),
        require_clean_reconciliation=True,
        require_no_active_position=True,
        require_no_pending_orders=True,
        require_range_aggregate=True,
    )
    kwargs.update(overrides)
    return StartupCatchupConfig(**kwargs)


def _base_kwargs(**overrides):
    now_ms = 12 * 60 * 60_000 + 120_000  # 12:02:00 UTC
    current_4h_open = 12 * 60 * 60_000  # 12:00
    candidate_open = 8 * 60 * 60_000  # 08:00
    candidate_close = current_4h_open - 1  # 11:59:59.999
    kwargs = dict(
        now_ms=now_ms,
        current_4h_open_time_ms=current_4h_open,
        candidate_closed_bar_open_time_ms=candidate_open,
        candidate_closed_bar_close_time_ms=candidate_close,
        previous_heartbeat=None,
        current_price=Decimal("105"),
        theoretical_open_price=Decimal("105"),
        side="long",
        has_active_position=False,
        has_pending_orders=False,
        has_unresolved_follower_close=False,
        already_executed=False,
        range_aggregate_available=True,
        config=_base_config(),
    )
    kwargs.update(overrides)
    return kwargs


# ── Tests: fresh open window ─────────────────────────────────────────────────


def test_startup_catchup_allows_within_fresh_open_window():
    """4H open + 120 s → eligible when all guards pass."""
    decision = evaluate_startup_catchup_eligibility(**_base_kwargs())
    assert decision.eligible is True
    assert decision.reason == "all_guards_passed"


def test_startup_catchup_skips_after_fresh_open_window():
    """4H open + 301 s → not eligible."""
    now_ms = 12 * 60 * 60_000 + 301_000  # 12:05:01
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(now_ms=now_ms),
    )
    assert decision.eligible is False
    assert decision.reason == "outside_fresh_4h_open_window"
    assert decision.metadata["fresh_window_age_seconds"] == 301


def test_startup_catchup_allows_at_exact_window_boundary():
    """4H open + 300 s (exact boundary) → eligible."""
    now_ms = 12 * 60 * 60_000 + 300_000  # 12:05:00
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(now_ms=now_ms),
    )
    assert decision.eligible is True


# ── Tests: heartbeat ─────────────────────────────────────────────────────────


def test_startup_catchup_does_not_require_heartbeat_inside_fresh_window():
    """Heartbeat is optional — eligibility works without previous heartbeat."""
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(previous_heartbeat=None),
    )
    assert decision.eligible is True
    assert decision.metadata["heartbeat_available"] is False


def test_startup_catchup_metadata_includes_heartbeat_downtime():
    """When a previous heartbeat exists, downtime metadata is populated."""
    hb = RuntimeHeartbeat(
        runtime_id="test::ETH-USDT-PERP",
        pid=1234,
        started_at_ms=12 * 60 * 60_000 - 120_000,
        last_alive_ms=12 * 60 * 60_000 - 30_000,
        last_market_event_ms=None,
        last_closed_bar_open_time_ms=None,
    )
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(previous_heartbeat=hb),
    )
    assert decision.eligible is True
    assert decision.metadata["heartbeat_available"] is True
    assert decision.metadata["previous_runtime_id"] == "test::ETH-USDT-PERP"
    assert isinstance(decision.metadata["downtime_seconds"], int)
    assert decision.metadata["downtime_seconds"] > 0


# ── Tests: disabled config ───────────────────────────────────────────────────


def test_startup_catchup_skips_when_disabled():
    """Config enabled=False → not eligible."""
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(config=_base_config(enabled=False)),
    )
    assert decision.eligible is False
    assert decision.reason == "startup_catchup_disabled"


# ── Tests: candidate bar validation ──────────────────────────────────────────


def test_startup_catchup_skips_when_bar_not_previous_4h():
    """Candidate bar must be the one that just closed (previous 4H)."""
    # Candidate close doesn't match current_4h_open - 1
    wrong_close = 12 * 60 * 60_000 - 2 * 60_000  # two minutes before 12:00
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            candidate_closed_bar_close_time_ms=wrong_close,
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "candidate_bar_not_previous_4h"


# ── Tests: price guard ───────────────────────────────────────────────────────


def test_startup_catchup_skips_long_when_price_adverse_too_large():
    """Long: current price > open * (1 + max_adverse) → skip."""
    open_price = Decimal("100")
    # max_adverse = 0.0015 → upper = 100.15
    # current = 100.20 → exceeds upper
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100.20"),
            theoretical_open_price=open_price,
            side="long",
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "price_guard_failed"


def test_startup_catchup_skips_long_when_price_too_favorable():
    """Long: current price < open * (1 - max_favorable) → skip."""
    open_price = Decimal("100")
    # max_favorable = 0.0030 → lower = 99.70
    # current = 99.50 → below lower
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("99.50"),
            theoretical_open_price=open_price,
            side="long",
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "price_guard_failed"


def test_startup_catchup_skips_short_when_price_adverse_too_large():
    """Short: current price < open * (1 - max_adverse) → skip."""
    open_price = Decimal("100")
    # max_adverse = 0.0015 → lower = 99.85
    # current = 99.70 → below lower
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("99.70"),
            theoretical_open_price=open_price,
            side="short",
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "price_guard_failed"


def test_startup_catchup_skips_short_when_price_too_favorable():
    """Short: current price > open * (1 + max_favorable) → skip."""
    open_price = Decimal("100")
    # max_favorable = 0.0030 → upper = 100.30
    # current = 100.50 → exceeds upper
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100.50"),
            theoretical_open_price=open_price,
            side="short",
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "price_guard_failed"


def test_startup_catchup_price_guard_passes_long_within_bounds():
    """Long: price slightly above open but within adverse bound → eligible."""
    open_price = Decimal("100")
    # upper = 100.15, slightly above open → should pass
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100.10"),
            theoretical_open_price=open_price,
            side="long",
        ),
    )
    assert decision.eligible is True


def test_startup_catchup_price_guard_passes_short_within_bounds():
    """Short: price slightly below open but within adverse bound → eligible."""
    open_price = Decimal("100")
    # lower = 99.85, slightly below → should pass
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("99.90"),
            theoretical_open_price=open_price,
            side="short",
        ),
    )
    assert decision.eligible is True


# ── Tests: range aggregate ───────────────────────────────────────────────────


def test_startup_catchup_skips_when_range_aggregate_unavailable():
    """Range aggregate unavailable + require_range_aggregate=True → skip."""
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(range_aggregate_available=False),
    )
    assert decision.eligible is False
    assert decision.reason == "range_aggregate_unavailable"


def test_startup_catchup_allows_when_range_aggregate_not_required():
    """If config.require_range_aggregate=False, unavailable aggregate is OK."""
    config = _base_config(require_range_aggregate=False)
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(range_aggregate_available=False, config=config),
    )
    assert decision.eligible is True


# ── Tests: position / order / follower guards ─────────────────────────────────


def test_startup_catchup_skips_when_active_position_exists():
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(has_active_position=True),
    )
    assert decision.eligible is False
    assert decision.reason == "active_position_exists"


def test_startup_catchup_skips_when_pending_order_exists():
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(has_pending_orders=True),
    )
    assert decision.eligible is False
    assert decision.reason == "pending_orders_exist"


def test_startup_catchup_skips_when_unresolved_follower_close_exists():
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(has_unresolved_follower_close=True),
    )
    assert decision.eligible is False
    assert decision.reason == "unresolved_follower_close_exists"


# ── Tests: dedup ─────────────────────────────────────────────────────────────


def test_startup_catchup_skips_when_already_executed():
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(already_executed=True),
    )
    assert decision.eligible is False
    assert decision.reason == "already_executed"


# ── Test: window edge cases ──────────────────────────────────────────────────


def test_startup_catchup_at_window_boundary_plus_one_second():
    """4H open + 301 s precisely → outside window."""
    now_ms = 12 * 60 * 60_000 + 301_000
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(now_ms=now_ms),
    )
    assert decision.eligible is False
    assert decision.reason == "outside_fresh_4h_open_window"


def test_startup_catchup_custom_window_config():
    """Custom fresh_open_window_seconds is respected."""
    config = _base_config(fresh_open_window_seconds=600)  # 10 minutes
    now_ms = 12 * 60 * 60_000 + 400_000  # 6m40s → within 10 min
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(now_ms=now_ms, config=config),
    )
    assert decision.eligible is True


# ── Test: V9C min_records ────────────────────────────────────────────────────


def test_v9c_closed_kline_min_records_is_configured():
    """V9C config.json must define closed_kline.min_records >= 2000."""
    import json
    from pathlib import Path

    config_path = Path("strategies/eth_lf_portfolio_v8/config.json")
    assert config_path.exists(), f"Config not found at {config_path}"
    data = json.loads(config_path.read_text(encoding="utf-8"))
    ck = data["runtime_requirements"]["closed_kline"]
    assert "min_records" in ck, "min_records key missing in closed_kline config"
    assert ck["min_records"] >= 2000, f"min_records={ck['min_records']} < 2000"


# ── Tests: price guard uses current market price (P0-2) ───────────────────────


def test_startup_catchup_price_guard_differentiates_current_vs_theoretical():
    """current_price != theoretical_open must be detectable by price guard.

    With kline.close=100 for both, the guard always passes.
    With current_price=101 and theoretical_open=100 for a LONG,
    the 1% deviation should fail the adverse bound (0.15%).
    """
    open_price = Decimal("100")
    # LONG: max_adverse=0.0015, upper=100.15, max_favorable=0.0030, lower=99.70
    # current=101 → above 100.15 → fails adverse
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("101"),
            theoretical_open_price=open_price,
            side="long",
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "price_guard_failed"
    assert decision.metadata["current_price"] == "101"
    assert decision.metadata["theoretical_open_price"] == "100"


def test_startup_catchup_price_guard_passes_when_current_near_open():
    """Long: current_price slightly above open but within adverse bound."""
    open_price = Decimal("100")
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100.10"),
            theoretical_open_price=Decimal("100.05"),
            side="long",
        ),
    )
    # max_adverse=0.0015 → upper=100.05*1.0015=100.200075
    # current=100.10 → within bound
    assert decision.eligible is True


# ── Tests: side from signal action, NOT kline colour (P0-3) ───────────────────


def test_startup_catchup_price_guard_uses_signal_side_not_kline_direction():
    """Short-side price guard is applied when signal side is short,
    regardless of whether the kline was green (close > open).
    """
    open_price = Decimal("100")
    # SHORT: max_adverse=0.0015 → lower=99.85, max_favorable=0.0030 → upper=100.30
    # current=99.70 → below 99.85 → fails adverse for SHORT
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("99.70"),
            theoretical_open_price=open_price,
            side="short",
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "price_guard_failed"


def test_startup_catchup_short_guard_differs_from_long_guard():
    """Same prices, different sides → different outcomes."""
    open_price = Decimal("100")
    # current=100.20: for LONG, upper=100.15 → fails; for SHORT, upper=100.30 → passes
    long_decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100.20"),
            theoretical_open_price=open_price,
            side="long",
        ),
    )
    assert long_decision.eligible is False  # LONG fails

    short_decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100.20"),
            theoretical_open_price=open_price,
            side="short",
        ),
    )
    assert short_decision.eligible is True  # SHORT passes — same price, opposite rule


# ── Tests: side normalization ─────────────────────────────────────────────────


def test_startup_catchup_price_guard_accepts_upper_case_side():
    """Side 'LONG' should be normalized to 'long'."""
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100.05"),
            theoretical_open_price=Decimal("100"),
            side="LONG",
        ),
    )
    assert decision.eligible is True


def test_startup_catchup_price_guard_refuses_unknown_side():
    """Unknown side → refuse (never guess)."""
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(
            current_price=Decimal("100"),
            theoretical_open_price=Decimal("100"),
            side="unknown",
        ),
    )
    assert decision.eligible is False
    assert decision.reason == "price_guard_failed"


# ── Tests: metadata completeness ──────────────────────────────────────────────


def test_startup_catchup_metadata_includes_bar_timestamps():
    """Decision metadata must include candidate bar open/close times."""
    decision = evaluate_startup_catchup_eligibility(**_base_kwargs())
    assert decision.metadata["candidate_bar_open_time_ms"] == 8 * 60 * 60_000
    assert decision.metadata["candidate_bar_close_time_ms"] == 12 * 60 * 60_000 - 1


def test_startup_catchup_metadata_includes_window_info():
    """Decision metadata must include fresh window age and config."""
    decision = evaluate_startup_catchup_eligibility(**_base_kwargs())
    assert "fresh_window_age_seconds" in decision.metadata
    assert decision.metadata["fresh_open_window_seconds"] == 300


# ── Tests: range aggregate unavailable coverage ───────────────────────────────


def test_startup_catchup_range_unavailable_metadata_contains_bucket_info():
    """When range aggregate is unavailable, metadata should tell operators why."""
    decision = evaluate_startup_catchup_eligibility(
        **_base_kwargs(range_aggregate_available=False),
    )
    assert decision.eligible is False
    assert decision.reason == "range_aggregate_unavailable"


# ── Tests: config defaults immutable ──────────────────────────────────────────


def test_startup_catchup_config_defaults_are_safe():
    """Default config must enable all safety requirements."""
    config = StartupCatchupConfig()
    assert config.enabled is True
    assert config.fresh_open_window_seconds == 300
    assert config.max_adverse_price_pct == Decimal("0.0015")
    assert config.max_favorable_price_pct == Decimal("0.0030")
    assert config.require_clean_reconciliation is True
    assert config.require_no_active_position is True
    assert config.require_no_pending_orders is True
    assert config.require_range_aggregate is True


def test_startup_catchup_config_from_mapping_handles_empty():
    """from_mapping with None or empty dict → defaults."""
    c1 = StartupCatchupConfig.from_mapping(None)
    assert c1.enabled is True
    c2 = StartupCatchupConfig.from_mapping({})
    assert c2.enabled is True


def test_startup_catchup_config_from_mapping_overrides():
    """from_mapping with partial dict overrides only specified keys."""
    c = StartupCatchupConfig.from_mapping({
        "enabled": "false",
        "fresh_open_window_seconds": "600",
    })
    assert c.enabled is False
    assert c.fresh_open_window_seconds == 600
    # Unspecified keys keep defaults
    assert c.max_adverse_price_pct == Decimal("0.0015")
