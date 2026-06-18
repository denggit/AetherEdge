from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_platform_keeps_only_infrastructure_modules_at_top_level():
    allowed = {
        "account",
        "data",
        "exchanges",
        "execution",
        "markets",
        "runtime",
        "state",
        "__init__.py",
        "config.py",
        "snapshot.py",
    }
    actual = {path.name for path in (ROOT / "src" / "platform").iterdir() if not path.name.startswith("__pycache__")}
    assert actual <= allowed


def test_state_store_is_storage_not_state_machine_or_recovery_engine():
    state_files = list((ROOT / "src" / "platform" / "state").rglob("*.py"))
    forbidden_tokens = ["place_order", "cancel_order", "amend_order", "recover", "reconcile", "strategy"]
    leaks = []
    for path in state_files:
        text = path.read_text(encoding="utf-8")
        for token in forbidden_tokens:
            if token in text:
                leaks.append((str(path.relative_to(ROOT)), token))
    assert leaks == []
