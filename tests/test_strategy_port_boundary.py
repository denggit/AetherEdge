from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_strategy_port_lives_outside_platform():
    assert (ROOT / "src" / "strategy" / "ports.py").exists()
    assert not (ROOT / "src" / "platform" / "strategy").exists()


def test_strategy_and_runtime_do_not_import_exchange_adapters_or_rest_endpoints():
    leaks = []
    for folder in [ROOT / "src" / "strategy", ROOT / "src" / "platform" / "runtime"]:
        for path in folder.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            if any(token in text for token in ["/api/v5", "/fapi/", "OkxExchangeClient", "BinanceExchangeClient"]):
                leaks.append(str(path.relative_to(ROOT)))
    assert leaks == []


def test_platform_package_does_not_export_strategy_port():
    text = (ROOT / "src" / "platform" / "__init__.py").read_text(encoding="utf-8")
    assert "StrategyPort" not in text
