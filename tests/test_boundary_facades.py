from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_execution_and_account_have_no_exchange_rest_endpoints():
    leaks = []
    for folder in [ROOT / "src" / "platform" / "execution", ROOT / "src" / "platform" / "account"]:
        for path in folder.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if "/api/v5" in text or "/fapi/" in text:
                leaks.append(str(path.relative_to(ROOT)))
    assert leaks == []
