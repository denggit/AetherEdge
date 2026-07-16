from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_LIVE = PROJECT_ROOT / "scripts" / "run_live.py"


def _manifest(*, position_snapshots: object = False) -> str:
    return repr(
        {
            "manifest_version": 1,
            "strategy_id": "contract-subprocess",
            "position_snapshots": position_snapshots,
            "recovery_status": False,
            "market_features": False,
            "range_speed_history": False,
            "startup_preview": False,
            "pending_work": False,
        }
    )


def _strategy_source(*, manifest: str | None, has_position_provider: bool) -> str:
    source = textwrap.dedent(
        """
        class Strategy:
            def strategy_identity(self):
                return "contract-subprocess"

            async def on_start(self, snapshot):
                return []

            async def on_kline(self, kline):
                return []

            async def on_ticker(self, ticker):
                return []

            async def on_trade(self, trade):
                return []

            async def on_order_book(self, order_book):
                return []

            async def on_account_event(self, event):
                return []
        """
    )
    if manifest is not None:
        source += textwrap.indent(
            textwrap.dedent(
                f"""
    def runtime_requirements(self):
        return {{
            \"capabilities\": {manifest},
            \"account_state\": {{
                \"startup_snapshot_enabled\": False,
                \"poll_enabled\": False,
                \"post_order_sync_enabled\": False,
            }},
            \"order_state\": {{
                \"post_submit_sync_enabled\": False,
                \"poll_when_position_enabled\": False,
                \"sync_open_orders\": False,
                \"sync_open_stop_orders\": False,
                \"sync_position\": False,
            }},
        }}
                """
            ),
            "    ",
        )
    if has_position_provider:
        source += textwrap.indent(
            textwrap.dedent(
                """
    def position_snapshots(self):
        return ()
                """
            ),
            "    ",
        )
    return source


def _run_live(
    tmp_path: Path,
    *,
    strategy_source: str,
    runtime_mode: str = "live_runtime",
    sitecustomize_source: str | None = None,
) -> subprocess.CompletedProcess[str]:
    module_path = tmp_path / "contract_subprocess_strategy.py"
    module_path.write_text(strategy_source, encoding="utf-8")
    heartbeat_isolation = textwrap.dedent(
        """
        import src.runtime.runner as _runner_module

        class _IsolatedHeartbeatService:
            pass

        _runner_module.RuntimeHeartbeatService = _IsolatedHeartbeatService
        """
    )
    (tmp_path / "sitecustomize.py").write_text(
        heartbeat_isolation + (sitecustomize_source or ""),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.update(
        {
            "AETHER_RUNTIME_MODE": runtime_mode,
            "AETHER_LIVE_TRADING": "false",
            "AETHER_DRY_RUN": "true",
            "AETHER_MARKET": "ETH-USDT-PERP",
            "AETHER_EXCHANGES": "okx",
            "AETHER_FOLLOWER_EXCHANGES": "",
            "AETHER_DATA_EXCHANGE": "okx",
            "AETHER_STRATEGY": (
                "contract_subprocess_strategy:Strategy"
            ),
            "AETHER_REQUIRED_LIVE_STRATEGY": "",
            "AETHER_REQUIRE_LIVE_GATE_REPORTS": "false",
            "AETHER_STATE_DB": str(tmp_path / "state.sqlite3"),
            "PYTHONPATH": os.pathsep.join(
                filter(
                    None,
                    (
                        str(tmp_path),
                        str(PROJECT_ROOT),
                        env.get("PYTHONPATH", ""),
                    ),
                )
            ),
        }
    )
    return subprocess.run(
        [sys.executable, str(RUN_LIVE)],
        cwd=PROJECT_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize(
    "strategy_source",
    (
        _strategy_source(
            manifest=None,
            has_position_provider=False,
        ),
        _strategy_source(
            manifest=_manifest(position_snapshots="true"),
            has_position_provider=True,
        ),
        _strategy_source(
            manifest=_manifest(position_snapshots=True),
            has_position_provider=False,
        ),
    ),
    ids=("manifest_missing", "manifest_invalid", "static_provider_missing"),
)
def test_real_run_live_subprocess_exits_78_for_static_contract_failures(
    tmp_path: Path,
    strategy_source: str,
) -> None:
    completed = _run_live(tmp_path, strategy_source=strategy_source)

    assert completed.returncode == 78, completed.stderr


def test_real_run_live_subprocess_exits_78_for_post_recovery_position_contract(
    tmp_path: Path,
) -> None:
    strategy_source = textwrap.dedent(
        f"""
        from decimal import Decimal
        from src.strategy import (
            StrategyPositionSide,
            StrategyPositionSnapshot,
            StrategyPositionStatus,
        )

        class Strategy:
            def __init__(self):
                self.recovered = False

            def strategy_identity(self):
                return "contract-subprocess"

            def runtime_requirements(self):
                return {{
                    "capabilities": {_manifest(position_snapshots=True)},
                    "account_state": {{
                        "startup_snapshot_enabled": False,
                        "poll_enabled": False,
                        "post_order_sync_enabled": False,
                    }},
                    "order_state": {{
                        "post_submit_sync_enabled": False,
                        "poll_when_position_enabled": False,
                        "sync_open_orders": False,
                        "sync_open_stop_orders": False,
                        "sync_position": False,
                    }},
                }}

            def position_snapshots(self):
                if not self.recovered:
                    return ()
                return (
                    StrategyPositionSnapshot(
                        strategy_id="contract-subprocess",
                        position_id="",
                        symbol="ETH-USDT-PERP",
                        side=StrategyPositionSide.LONG,
                        status=StrategyPositionStatus.ACTIVE,
                        base_quantity=Decimal("1"),
                    ),
                )

            async def on_start(self, snapshot):
                return []

            async def on_kline(self, kline):
                return []

            async def on_ticker(self, ticker):
                return []

            async def on_trade(self, trade):
                return []

            async def on_order_book(self, order_book):
                return []

            async def on_account_event(self, event):
                return []
        """
    )
    sitecustomize_source = textwrap.dedent(
        """
        from src.runtime.recovery.models import RecoveryReport
        from src.runtime.runner import LiveRuntimeRunner

        class _RecoveryService:
            async def recover(self, *, strategy):
                strategy.recovered = True
                return RecoveryReport(ok=True)

        async def _skip_account_config(self):
            return None

        LiveRuntimeRunner._bootstrap_account_config_if_enabled = (
            _skip_account_config
        )
        LiveRuntimeRunner._get_recovery_service = (
            lambda self: _RecoveryService()
        )
        """
    )

    completed = _run_live(
        tmp_path,
        strategy_source=strategy_source,
        sitecustomize_source=sitecustomize_source,
    )

    assert completed.returncode == 78, completed.stderr


def test_real_run_live_subprocess_exits_78_for_unsupported_runtime_mode(
    tmp_path: Path,
) -> None:
    completed = _run_live(
        tmp_path,
        strategy_source=_strategy_source(
            manifest=_manifest(),
            has_position_provider=False,
        ),
        runtime_mode="legacy_app",
    )

    assert completed.returncode == 78, completed.stderr
