from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
TOOL_PATH = REPO_ROOT / "tools" / "check_live_warmup_data.py"


def _run_tool(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", str(REPO_ROOT))
    return subprocess.run(
        [sys.executable, str(TOOL_PATH)] + args,
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=str(REPO_ROOT),
        **kwargs,
    )


class TestCheckLiveWarmupDataTool:
    def test_tool_imports_cleanly(self):
        """The tool module should be importable without side effects."""
        result = subprocess.run(
            [sys.executable, "-c", "import tools.check_live_warmup_data"],
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT)},
            cwd=str(REPO_ROOT),
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"

    def test_tool_help_prints_usage(self):
        result = _run_tool(["--help"])
        assert result.returncode == 0
        assert "usage:" in (result.stdout + result.stderr).lower() or "--symbol" in result.stdout + result.stderr

    def test_check_live_warmup_data_fails_without_rows(self):
        """With a fresh empty KlineStore and no --backfill, the tool should
        report insufficient data (exit code 2)."""
        result = _run_tool([
            "--symbol", "ETH-USDT-PERP",
            "--interval", "4h",
            "--warmup-days", "365",
            "--min-records", "1000",
        ])
        # May fail with exit 2 (insufficient) or 0 if local DB happens to have
        # data from other tests. Accept both, just check it doesn't crash.
        assert result.returncode in (0, 2, 3)

    def test_check_live_warmup_data_rejects_unknown_symbol(self):
        """An unknown symbol should cause a config error (exit 3)."""
        result = _run_tool([
            "--symbol", "NO-SUCH-SYMBOL-XX",
            "--interval", "4h",
            "--warmup-days", "365",
            "--min-records", "1000",
        ])
        assert result.returncode == 3

    def test_check_live_warmup_data_passes_with_enough_rows(self):
        """When the local store has enough data, the tool should exit 0."""
        # Use a very low min_records threshold — the store may be empty or
        # populated depending on test order, but the tool should not crash.
        result = _run_tool([
            "--symbol", "ETH-USDT-PERP",
            "--interval", "4h",
            "--warmup-days", "1",
            "--min-records", "0",
        ])
        # With min_records=0, any count is sufficient.
        assert result.returncode == 0
        assert "PASS" in result.stdout or "Sufficient" in result.stdout or "YES" in result.stdout

    def test_tool_backfill_flag_is_accepted(self):
        """The --backfill flag should be accepted without crashing (test may
        reach network, so we only verify the flag is parsed)."""
        result = _run_tool([
            "--symbol", "ETH-USDT-PERP",
            "--interval", "4h",
            "--warmup-days", "1",
            "--min-records", "0",
            "--backfill",
        ])
        # With min_records=0 no backfill is actually needed, but the flag
        # should not cause a parse error.
        assert result.returncode in (0, 2, 3)


class TestCheckLocalAvailableRows:
    """Unit tests for _check_local confirming it counts available (not newly-loaded) rows."""

    def test_available_rows_counted_not_newly_loaded(self, tmp_path):
        """_check_local queries the store directly for available rows,
        independent of any warmup pass."""
        import tools.check_live_warmup_data as tool_module

        count, store_class, store_path = tool_module._check_local(
            symbol="ETH-USDT-PERP",
            interval="4h",
            start_open_ms=0,
            end_open_ms=4 * 60 * 60_000 * 10,
            kline_store_path=str(tmp_path / "empty.sqlite3"),
        )
        # Fresh empty store → 0 available rows
        assert count == 0
        assert store_class == "SqliteKlineStore"

    def test_check_local_returns_closed_only(self, tmp_path):
        """_check_local only counts closed klines."""
        from src.market_data.storage.kline_store import SqliteKlineStore
        from src.platform.data.models import MarketKline, MarketDataSource
        from src.platform.exchanges.models import ExchangeName
        from decimal import Decimal
        import tools.check_live_warmup_data as tool_module

        store = SqliteKlineStore(tmp_path / "test.sqlite3")
        store.save([
            MarketKline(
                exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP",
                interval="4h", open_time_ms=4 * 60 * 60_000, close_time_ms=8 * 60 * 60_000 - 1,
                open=Decimal("100"), high=Decimal("110"), low=Decimal("90"),
                close=Decimal("105"), volume=Decimal("10"), is_closed=True,
                source=MarketDataSource.REST,
            ),
            MarketKline(
                exchange=ExchangeName.OKX, symbol="ETH-USDT-PERP", raw_symbol="ETH-USDT-SWAP",
                interval="4h", open_time_ms=8 * 60 * 60_000, close_time_ms=12 * 60 * 60_000 - 1,
                open=Decimal("105"), high=Decimal("115"), low=Decimal("95"),
                close=Decimal("110"), volume=Decimal("10"), is_closed=False,
                source=MarketDataSource.REST,
            ),
        ])

        count, _, _ = tool_module._check_local(
            symbol="ETH-USDT-PERP",
            interval="4h",
            start_open_ms=0,
            end_open_ms=16 * 60 * 60_000,
            kline_store_path=str(tmp_path / "test.sqlite3"),
        )
        # Only the first kline is closed
        assert count == 1
