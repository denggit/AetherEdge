from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_app_lives_outside_platform_and_does_not_import_exchange_adapters():
    assert (ROOT / "src" / "app" / "factory.py").exists()
    assert not (ROOT / "src" / "platform" / "app").exists()
    forbidden = ["/api/v5", "/fapi/", "OkxExchangeClient", "BinanceExchangeClient"]
    leaks = []
    for path in (ROOT / "src" / "app").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                leaks.append((str(path.relative_to(ROOT)), token))
    assert leaks == []


def test_tools_run_live_has_repo_root_bootstrap():
    text = (ROOT / "tools" / "run_live.py").read_text(encoding="utf-8")
    assert "REPO_ROOT = Path(__file__).resolve().parents[1]" in text
    assert "sys.path.insert(0, str(REPO_ROOT))" in text
