import sys

import pytest

from src.app.alerts import AppAlert
from src.app.watchdog import ProcessWatchdog, WatchdogConfig, build_live_runner_command


class CollectingSink:
    def __init__(self):
        self.alerts: list[AppAlert] = []

    async def send(self, alert: AppAlert) -> None:
        self.alerts.append(alert)


@pytest.mark.asyncio
async def test_process_watchdog_exits_cleanly_without_restart():
    sink = CollectingSink()
    watchdog = ProcessWatchdog(
        WatchdogConfig(command=(sys.executable, "-c", "raise SystemExit(0)"), restart_delay_seconds=0, max_restarts=2),
        alert_sink=sink,
    )

    stats = await watchdog.run()

    assert stats.starts == 1
    assert stats.restarts == 0
    assert stats.last_return_code == 0
    assert sink.alerts == []


@pytest.mark.asyncio
async def test_process_watchdog_restarts_failed_child_until_limit():
    sink = CollectingSink()
    watchdog = ProcessWatchdog(
        WatchdogConfig(command=(sys.executable, "-c", "raise SystemExit(2)"), restart_delay_seconds=0, max_restarts=2),
        alert_sink=sink,
    )

    stats = await watchdog.run()

    assert stats.starts == 3
    assert stats.restarts == 2
    assert stats.last_return_code == 2
    assert stats.alerts_sent == 3
    assert len(sink.alerts) == 3


def test_build_live_runner_command_points_to_scripts_run_live(tmp_path):
    command = build_live_runner_command(project_root=tmp_path, child_args=("--max-events", "1"))

    assert command[0] == sys.executable
    assert str(tmp_path / "scripts" / "run_live.py") == command[1]
    assert command[-2:] == ("--max-events", "1")
