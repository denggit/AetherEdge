import sys

from src.app.process_control import PidFileProcessController


def test_pid_file_controller_start_status_stop(tmp_path):
    pid_file = tmp_path / "run" / "watchdog.pid"
    log_file = tmp_path / "logs" / "watchdog.log"
    controller = PidFileProcessController(pid_file=pid_file, log_file=log_file, cwd=tmp_path)

    start = controller.start((sys.executable, "-c", "import time; time.sleep(30)"))
    assert start.ok
    assert start.pid is not None
    assert pid_file.exists()

    status = controller.status()
    assert status.ok
    assert status.status == "running"
    assert status.pid == start.pid

    stopped = controller.stop(timeout_seconds=2)
    assert stopped.ok
    assert stopped.status in {"stopped", "killed"}
    assert not pid_file.exists()


def test_pid_file_controller_removes_stale_pid_file(tmp_path):
    pid_file = tmp_path / "watchdog.pid"
    log_file = tmp_path / "watchdog.log"
    pid_file.write_text("99999999", encoding="utf-8")
    controller = PidFileProcessController(pid_file=pid_file, log_file=log_file)

    status = controller.status()

    assert not status.ok
    assert status.status == "stale"
    assert not pid_file.exists()
