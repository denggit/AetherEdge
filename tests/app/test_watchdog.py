import asyncio
import sys
from pathlib import Path

import pytest

from src.app.alerts import AppAlert
from src.app.watchdog import (
    ProcessWatchdog,
    WatchdogConfig,
    build_live_runner_command,
    build_live_watchdog_from_env,
)


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


@pytest.mark.asyncio
async def test_process_watchdog_honors_live_script_failure_limit():
    sink = CollectingSink()
    watchdog = ProcessWatchdog(
        WatchdogConfig(
            command=(sys.executable, "-c", "raise SystemExit(2)"),
            restart_delay_seconds=0,
            max_failures=2,
        ),
        alert_sink=sink,
    )

    stats = await watchdog.run()

    assert stats.starts == 2
    assert stats.restarts == 1
    assert stats.last_return_code == 2
    assert stats.alerts_sent == 2


@pytest.mark.asyncio
async def test_process_watchdog_opens_quick_fail_circuit():
    sink = CollectingSink()
    watchdog = ProcessWatchdog(
        WatchdogConfig(
            command=(sys.executable, "-c", "raise SystemExit(2)"),
            restart_delay_seconds=0,
            quick_fail_seconds=60,
            max_quick_failures=3,
        ),
        alert_sink=sink,
    )

    stats = await watchdog.run()

    assert stats.starts == 3
    assert stats.restarts == 2
    assert stats.quick_failures == 3
    assert stats.last_return_code == 2
    assert [alert.subject for alert in sink.alerts].count(
        "AetherEdge watchdog quick-fail circuit opened"
    ) == 1


@pytest.mark.asyncio
async def test_process_watchdog_stops_immediately_on_fatal_exit():
    sink = CollectingSink()
    watchdog = ProcessWatchdog(
        WatchdogConfig(
            command=(sys.executable, "-c", "raise SystemExit(78)"),
            restart_delay_seconds=0,
            fatal_exit_codes=frozenset({78}),
        ),
        alert_sink=sink,
    )

    stats = await watchdog.run()

    assert stats.starts == 1
    assert stats.restarts == 0
    assert stats.last_return_code == 78
    assert [alert.subject for alert in sink.alerts] == [
        "AetherEdge watchdog received fatal exit code"
    ]


@pytest.mark.asyncio
async def test_process_watchdog_resets_quick_failures_after_clean_exit(tmp_path: Path):
    marker = tmp_path / "started"
    code = (
        "from pathlib import Path; import sys; "
        "p=Path(sys.argv[1]); seen=p.exists(); p.touch(); "
        "raise SystemExit(0 if seen else 1)"
    )
    watchdog = ProcessWatchdog(
        WatchdogConfig(
            command=(sys.executable, "-c", code, str(marker)),
            restart_delay_seconds=0,
            max_restarts=1,
            quick_fail_seconds=60,
        )
    )

    stats = await watchdog.run()

    assert stats.starts == 2
    assert stats.restarts == 1
    assert stats.quick_failures == 0
    assert stats.last_return_code == 0


@pytest.mark.asyncio
async def test_process_watchdog_resets_quick_failures_after_long_run(tmp_path: Path):
    counter = tmp_path / "counter"
    code = (
        "from pathlib import Path; import sys,time; p=Path(sys.argv[1]); "
        "n=int(p.read_text()) if p.exists() else 0; p.write_text(str(n+1)); "
        "time.sleep(1.1 if n == 1 else 0); raise SystemExit(1)"
    )
    watchdog = ProcessWatchdog(
        WatchdogConfig(
            command=(sys.executable, "-c", code, str(counter)),
            restart_delay_seconds=0,
            max_restarts=2,
            quick_fail_seconds=1,
        )
    )

    stats = await watchdog.run()

    assert stats.starts == 3
    assert stats.restarts == 2
    assert stats.quick_failures == 1


@pytest.mark.asyncio
async def test_process_watchdog_retains_output_and_cleans_pid_file(tmp_path: Path):
    log_path = tmp_path / "child.log"
    pid_path = tmp_path / "child.pid"
    watchdog = ProcessWatchdog(
        WatchdogConfig(
            command=(sys.executable, "-c", "print('child-ready')"),
            restart_delay_seconds=0,
            stdout_path=log_path,
            child_pid_file=pid_path,
        )
    )

    stats = await watchdog.run()

    assert stats.last_return_code == 0
    assert log_path.read_text(encoding="utf-8").strip() == "child-ready"
    assert not pid_path.exists()


@pytest.mark.asyncio
async def test_process_watchdog_stop_terminates_child_and_cleans_pid_file(tmp_path: Path):
    pid_path = tmp_path / "child.pid"
    watchdog = ProcessWatchdog(
        WatchdogConfig(
            command=(sys.executable, "-c", "import time; time.sleep(30)"),
            child_pid_file=pid_path,
            child_stop_timeout_seconds=0.1,
        )
    )

    task = asyncio.create_task(watchdog.run())
    while watchdog.stats.starts == 0:
        await asyncio.sleep(0)
    watchdog.stop()
    stats = await task

    assert watchdog.stop_requested
    assert stats.starts == 1
    assert not pid_path.exists()


def test_build_live_runner_command_points_to_scripts_run_live(tmp_path):
    command = build_live_runner_command(project_root=tmp_path, child_args=("--max-events", "1"))

    assert command[0] == sys.executable
    assert str(tmp_path / "scripts" / "run_live.py") == command[1]
    assert command[-2:] == ("--max-events", "1")


def test_production_watchdog_defaults_to_formal_child_and_fatal_78(tmp_path):
    watchdog = build_live_watchdog_from_env(
        project_root=tmp_path,
        environ={},
    )

    assert watchdog.config.command[0] == sys.executable
    assert watchdog.config.command[1] == "-u"
    assert watchdog.config.command[2] == str(
        tmp_path / "scripts" / "run_live.py"
    )
    assert watchdog.config.fatal_exit_codes == frozenset({78})
