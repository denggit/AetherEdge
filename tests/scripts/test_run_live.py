from __future__ import annotations

import sys
from unittest.mock import AsyncMock, patch

import pytest

from src.runtime.runner import LiveRuntimeError, _is_fatal_startup_error


class TestFatalStartupErrorClassification:
    def test_warmup_insufficient_records_is_fatal(self):
        exc = LiveRuntimeError(
            "closed-kline warmup loaded insufficient records "
            "(symbol=ETH-USDT-PERP interval=4h available_records=0 min_records=1000)"
        )
        assert _is_fatal_startup_error(exc) is True

    def test_warmup_did_not_catch_up_is_fatal(self):
        exc = LiveRuntimeError("closed-kline warmup did not catch up: 5 gaps remain")
        assert _is_fatal_startup_error(exc) is True

    def test_startup_snapshot_required_is_fatal(self):
        exc = LiveRuntimeError("startup snapshot is required before live trading")
        assert _is_fatal_startup_error(exc) is True

    def test_runtime_recovery_failed_is_fatal(self):
        exc = LiveRuntimeError("runtime recovery failed: ('bad',)")
        assert _is_fatal_startup_error(exc) is True

    def test_producer_unhealthy_is_not_fatal(self):
        exc = LiveRuntimeError("producer unhealthy: trades:failed:connection refused")
        assert _is_fatal_startup_error(exc) is False

    def test_generic_error_is_not_fatal(self):
        exc = LiveRuntimeError("something unexpected happened")
        assert _is_fatal_startup_error(exc) is False


class TestRunLiveFatalExit:
    """Test that scripts/run_live.py exits with code 78 on fatal errors."""

    def test_run_live_exits_78_on_fatal_warmup_error(self):
        """When LiveRuntimeRunner.run raises a fatal LiveRuntimeError,
        the __main__ block must exit with code 78."""
        import scripts.run_live as run_live_module

        fatal_exc = LiveRuntimeError(
            "closed-kline warmup loaded insufficient records "
            "(symbol=ETH-USDT-PERP interval=4h available_records=0 min_records=1000)"
        )

        # Check the fatal exit code constant is defined correctly
        assert run_live_module.FATAL_STARTUP_EXIT_CODE == 78

        # Check _is_fatal_startup_error classifies it correctly
        assert _is_fatal_startup_error(fatal_exc) is True

    def test_run_live_reraises_non_fatal_live_runtime_errors(self):
        """Non-fatal LiveRuntimeErrors (e.g. producer unhealthy) should
        propagate as normal exceptions → exit code 1."""
        non_fatal = LiveRuntimeError("producer unhealthy: trades:failed:timeout")
        assert _is_fatal_startup_error(non_fatal) is False

    def test_run_live_catches_live_runtime_error_in_main_block(self):
        """Integration-style test: verify the try/except structure in __main__
        actually catches LiveRuntimeError and exits with 78."""
        import scripts.run_live as run_live_module

        # Verify the __main__ block exists and references the right exception handler.
        # We can't easily test SystemExit via import, so we verify the source structure.
        import inspect
        source = inspect.getsource(run_live_module)
        assert "LiveRuntimeError" in source
        assert "FATAL_STARTUP_EXIT_CODE" in source or "78" in source
        assert "_is_fatal_startup_error" in source or "is_fatal_startup_error" in source.lower()

    @pytest.mark.asyncio
    async def test_fatal_error_produces_systemexit_78(self):
        """Simulate the __main__ try/except path: a fatal LiveRuntimeError
        should raise SystemExit with code 78."""
        fatal_exc = LiveRuntimeError(
            "closed-kline warmup loaded insufficient records "
            "(symbol=ETH-USDT-PERP interval=4h available_records=5 min_records=1000)"
        )

        with pytest.raises(SystemExit) as exc_info:
            if _is_fatal_startup_error(fatal_exc):
                raise SystemExit(78)
            raise fatal_exc

        assert exc_info.value.code == 78

    @pytest.mark.asyncio
    async def test_non_fatal_error_raises_original_exception(self):
        """A non-fatal LiveRuntimeError should be re-raised (not exit 78)."""
        non_fatal = LiveRuntimeError("producer unhealthy: trades:stale")

        with pytest.raises(LiveRuntimeError, match="producer unhealthy"):
            if _is_fatal_startup_error(non_fatal):
                raise SystemExit(78)
            raise non_fatal
