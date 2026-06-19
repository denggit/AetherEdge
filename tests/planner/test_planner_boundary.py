from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_signals_and_planner_live_outside_platform():
    assert (ROOT / "src" / "signals" / "models.py").exists()
    assert (ROOT / "src" / "planner" / "service.py").exists()
    assert not (ROOT / "src" / "platform" / "signals").exists()
    assert not (ROOT / "src" / "platform" / "planner").exists()


def test_signals_and_planner_do_not_import_exchange_adapters_or_rest_endpoints():
    forbidden = ["/api/v5", "/fapi/", "OkxExchangeClient", "BinanceExchangeClient", "create_exchange_client"]
    leaks = []
    for folder in [ROOT / "src" / "signals", ROOT / "src" / "planner"]:
        for path in folder.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            for token in forbidden:
                if token in text:
                    leaks.append((str(path.relative_to(ROOT)), token))
    assert leaks == []


def test_planner_does_not_call_execution_client_methods():
    forbidden = [".place_order(", ".cancel_order(", ".amend_order(", ".replace_order("]
    leaks = []
    for path in (ROOT / "src" / "planner").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                leaks.append((str(path.relative_to(ROOT)), token))
    assert leaks == []
