from __future__ import annotations

import ast
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Files that must NOT contain strategy-specific vocabulary
PUBLIC_MF_SOURCES = (
    PROJECT_ROOT / "src" / "market_data" / "derived" / "fixed_time_trade_bar_builder.py",
    PROJECT_ROOT / "src" / "market_data" / "derived" / "trade_footprint_builder.py",
    PROJECT_ROOT / "src" / "market_data" / "derived" / "range_footprint_builder.py",
    PROJECT_ROOT / "src" / "market_data" / "storage" / "trade_feature_store.py",
    PROJECT_ROOT / "src" / "market_data" / "trade_features" / "coverage.py",
    PROJECT_ROOT / "src" / "market_data" / "backfill" / "coordinator.py",
    PROJECT_ROOT / "src" / "runtime" / "mf_feature_backfill_supervisor.py",
    PROJECT_ROOT / "tools" / "mf_feature_backfill_worker.py",
)

V1_ONLY_SOURCES = (
    PROJECT_ROOT / "strategies" / "eth_portfolio_v1" / "domain" / "mf_data.py",
)

FORBIDDEN_FILES = (
    PROJECT_ROOT / "strategies" / "eth_lf_portfolio_v8",
    PROJECT_ROOT / "strategies" / "eth_lf_portfolio_v10b",
)


def _imports(path: Path) -> tuple[str, ...]:
    if not path.exists():
        return ()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules = [
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    ]
    modules.extend(
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    return tuple(modules)


# ---------------------------------------------------------------------------
# 1. Public sources: no strategy-specific vocabulary
# ---------------------------------------------------------------------------

def test_public_mf_sources_have_no_strategy_specific_vocabulary() -> None:
    forbidden = re.compile(
        r"eth_portfolio_v1|eth_lf_portfolio_v8|eth_lf_portfolio_v10b|"
        r"low_sweep|\biceberg",
        re.IGNORECASE,
    )
    # Allow "mf" in file names / comments that are about MF specifically.
    # Strategy-specific LF/HF references are still forbidden.
    allowed_in_mf_context = re.compile(
        r"mf_feature|mf_data|mf_signal|MfFeature|MfData|MfSignal|mf_backfill|MF_FEATURE",
        re.IGNORECASE,
    )

    for path in PUBLIC_MF_SOURCES:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        # Replace allowed MF terms before checking
        cleaned = allowed_in_mf_context.sub("", text)
        match = forbidden.search(cleaned)
        if match:
            raise AssertionError(
                f"File {path.name} contains forbidden strategy-specific term: {match.group()}"
            )


# ---------------------------------------------------------------------------
# 2. Builder: no runtime/strategy/platform adapter imports
# ---------------------------------------------------------------------------

def test_bar_builder_has_no_runtime_or_strategy_imports() -> None:
    for builder_path in (
        PROJECT_ROOT / "src" / "market_data" / "derived" / "fixed_time_trade_bar_builder.py",
        PROJECT_ROOT / "src" / "market_data" / "derived" / "trade_footprint_builder.py",
        PROJECT_ROOT / "src" / "market_data" / "derived" / "range_footprint_builder.py",
    ):
        imports = _imports(builder_path)
        assert not any(m.startswith("src.runtime") for m in imports), \
            f"{builder_path.name} imports runtime"
        assert not any(m.startswith("strategies") for m in imports), \
            f"{builder_path.name} imports strategies"
        assert not any(m.startswith("src.platform.exchanges.okx") for m in imports), \
            f"{builder_path.name} imports platform exchanges"


# ---------------------------------------------------------------------------
# 3. Store: no raw trade persistence
# ---------------------------------------------------------------------------

def test_trade_feature_store_has_no_save_raw_trades_param() -> None:
    from src.market_data.storage.trade_feature_store import SqliteTradeFeatureStore
    import inspect

    sig = inspect.signature(SqliteTradeFeatureStore.__init__)
    params = list(sig.parameters.keys())
    assert "save_raw_trades" not in params


# ---------------------------------------------------------------------------
# 4. Worker: live mode forbids save_raw_trades
# ---------------------------------------------------------------------------

def test_worker_live_mode_rejects_save_raw_trades() -> None:
    worker_path = PROJECT_ROOT / "tools" / "mf_feature_backfill_worker.py"
    if not worker_path.exists():
        return
    text = worker_path.read_text(encoding="utf-8")
    # Must check save_raw_trades in live mode
    assert "--save-raw-trades" in text
    assert "save_raw_trades" in text
    assert "live" in text.lower()


# ---------------------------------------------------------------------------
# 5. No modification to V8/V10B
# ---------------------------------------------------------------------------

def test_v8_and_v10b_sources_unchanged() -> None:
    """Verify no MF/feature logic added to V8/V10B strategies."""
    for strategy_dir in FORBIDDEN_FILES:
        if not strategy_dir.exists():
            continue
        for py_file in strategy_dir.rglob("*.py"):
            text = py_file.read_text(encoding="utf-8")
            # Must not contain new MF feature logic
            forbidden_new = [
                "FixedTimeTradeBar",
                "TradeFootprintFeature",
                "SqliteTradeFeatureStore",
                "MfFeatureBackfillSupervisor",
            ]
            for term in forbidden_new:
                assert term not in text, \
                    f"{py_file.relative_to(PROJECT_ROOT)} contains forbidden import: {term}"


# ---------------------------------------------------------------------------
# 6. MF signal_ready is always False in R007
# ---------------------------------------------------------------------------

def test_mf_signal_ready_always_false_in_readiness_gate() -> None:
    from strategies.eth_portfolio_v1.domain.mf_data import MfDataReadiness
    import inspect

    # Check the property returns False literally
    src = inspect.getsource(MfDataReadiness.mf_signal_ready.fget)
    assert "False" in src or "return False" in src


def test_market_data_readiness_has_no_strategy_signal_gate() -> None:
    from src.market_data.trade_features.coverage import (
        resolve_trade_feature_readiness,
    )
    import inspect

    src = inspect.getsource(resolve_trade_feature_readiness)
    assert ("mf_" + "signal") not in src


# ---------------------------------------------------------------------------
# 7. FixedTimeTradeBar model field completeness
# ---------------------------------------------------------------------------

def test_fixed_time_trade_bar_has_required_fields() -> None:
    from src.market_data.models import FixedTimeTradeBar
    import dataclasses

    fields = {f.name for f in dataclasses.fields(FixedTimeTradeBar)}
    required = {
        "exchange", "symbol", "timeframe", "open_time_ms", "close_time_ms",
        "available_time_ms", "open", "high", "low", "close",
        "volume", "buy_volume", "sell_volume",
        "buy_notional", "sell_notional", "delta_volume", "delta_notional",
        "abs_delta_notional", "trade_count",
        "large_buy_notional", "large_sell_notional",
        "large_trade_count", "large_trade_share",
        "quality", "source",
    }
    missing = required - fields
    assert not missing, f"FixedTimeTradeBar missing fields: {missing}"


def test_trade_footprint_feature_has_required_fields() -> None:
    from src.market_data.models import TradeFootprintFeature
    import dataclasses

    fields = {f.name for f in dataclasses.fields(TradeFootprintFeature)}
    required = {
        "exchange", "symbol", "timeframe", "open_time_ms", "close_time_ms",
        "available_time_ms", "delta_notional", "abs_delta_notional",
        "taker_buy_ratio", "close_pos", "range_pct", "return_pct",
        "fp_max_bucket_abs_delta_pressure",
        "context_available", "quality", "source",
    }
    missing = required - fields
    assert not missing, f"TradeFootprintFeature missing fields: {missing}"


def test_range_footprint_feature_has_required_fields() -> None:
    from src.market_data.models import RangeFootprintFeature
    import dataclasses

    fields = {f.name for f in dataclasses.fields(RangeFootprintFeature)}
    required = {
        "exchange", "symbol", "range_pct", "price_step", "range_bar_id",
        "range_start_ms", "range_end_ms", "available_time_ms",
        "fp_max_bucket_abs_delta_pressure",
        "fp_low_bucket_delta_pressure", "fp_high_bucket_delta_pressure",
        "fp_delta_pressure", "context_available", "quality", "source",
    }
    missing = required - fields
    assert not missing, f"RangeFootprintFeature missing fields: {missing}"
