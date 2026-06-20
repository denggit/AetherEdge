from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_all_python_startup_scripts_have_project_root_bootstrap():
    missing = []
    for path in sorted((ROOT / "scripts").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        has_bootstrap = (
            "PROJECT_ROOT = Path(__file__).resolve().parents[1]" in text
            and "sys.path.insert(0, str(PROJECT_ROOT))" in text
        )
        if not has_bootstrap:
            missing.append(str(path.relative_to(ROOT)))
    assert missing == []


def test_all_shell_startup_scripts_define_project_root():
    missing = []
    for path in sorted((ROOT / "scripts").glob("*.sh")):
        text = path.read_text(encoding="utf-8")
        if "PROJECT_ROOT=" not in text:
            missing.append(str(path.relative_to(ROOT)))
    assert missing == []


def test_watchdog_uses_simple_shell_pid_control():
    shell_text = (ROOT / "scripts" / "start_live_watchdog.sh").read_text(encoding="utf-8")
    py_text = (ROOT / "scripts" / "watchdog_live.py").read_text(encoding="utf-8")

    assert 'case "${1:-start}" in' in shell_text
    assert "nohup" in shell_text
    assert "WATCHDOG_PID" in shell_text
    assert "DEFAULT_CHILD_PID_FILE" in py_text
    assert "subprocess.Popen" in py_text
    assert "ProcessWatchdog" not in py_text
    assert "PidFileProcessController" not in py_text
