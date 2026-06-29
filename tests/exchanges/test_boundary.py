from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"


def _py_files():
    return sorted(SRC.rglob("*.py"))


def test_exchange_rest_endpoints_stay_inside_exchange_adapters():
    allowed_parts = {
        ("src", "platform", "exchanges", "okx", "client.py"),
        ("src", "platform", "exchanges", "okx", "historical_data.py"),
        ("src", "platform", "exchanges", "okx", "rest_tail_trades.py"),
        ("src", "platform", "exchanges", "binance", "client.py"),
    }
    forbidden_tokens = ["/api/v5", "/fapi/"]

    leaks = []
    for path in _py_files():
        rel_parts = path.relative_to(ROOT).parts
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in forbidden_tokens) and rel_parts not in allowed_parts:
            leaks.append(str(path.relative_to(ROOT)))

    assert leaks == []


def test_business_package_does_not_import_exchange_adapters_directly():
    adapter_import_tokens = [
        "src.platform.exchanges.okx.client",
        "src.platform.exchanges.binance.client",
        "OkxExchangeClient",
        "BinanceExchangeClient",
    ]
    allowed = {
        Path("src/platform/exchanges/factory.py"),
        Path("src/platform/exchanges/okx/__init__.py"),
        Path("src/platform/exchanges/binance/__init__.py"),
        Path("src/platform/exchanges/okx/client.py"),
        Path("src/platform/exchanges/binance/client.py"),
    }

    leaks = []
    for path in _py_files():
        rel = path.relative_to(ROOT)
        if rel in allowed:
            continue
        text = path.read_text(encoding="utf-8")
        if any(token in text for token in adapter_import_tokens):
            leaks.append(str(rel))

    assert leaks == []


def test_no_legacy_oversized_exchange_files_left():
    deleted_legacy_files = [
        SRC / "platform" / "data" / "okx_loader.py",
        SRC / "platform" / "exchanges" / "okx" / "semantic_executor.py",
        SRC / "platform" / "exchanges" / "binance" / "trading_client.py",
        SRC / "platform" / "exchanges" / "binance" / "live_preflight.py",
    ]
    assert [str(p.relative_to(ROOT)) for p in deleted_legacy_files if p.exists()] == []
