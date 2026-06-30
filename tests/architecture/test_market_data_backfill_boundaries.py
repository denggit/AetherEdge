from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _py_files(path: str):
    return (ROOT / path).rglob("*.py")


def test_market_data_backfill_does_not_import_strategies() -> None:
    offenders = []
    for path in _py_files("src/market_data"):
        text = path.read_text(encoding="utf-8")
        if "import strategies" in text or "from strategies" in text:
            offenders.append(path)
    assert offenders == []


def test_runtime_does_not_import_exchange_clients_directly() -> None:
    offenders = []
    for path in _py_files("src/runtime"):
        text = path.read_text(encoding="utf-8")
        if "src.platform.exchanges.okx.client" in text or "src.platform.exchanges.binance.client" in text:
            offenders.append(path)
    assert offenders == []


def test_okx_api_v5_not_in_runtime_strategy_or_backfill_service() -> None:
    paths = list(_py_files("src/runtime")) + list(_py_files("strategies")) + [ROOT / "src/market_data/backfill/service.py"]
    offenders = [path for path in paths if "/api/v5" in path.read_text(encoding="utf-8")]
    assert offenders == []


def test_platform_data_not_polluted_with_range_backfill_keywords() -> None:
    platform_data = ROOT / "src/platform/data"
    if not platform_data.exists():
        return
    offenders = []
    for path in platform_data.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if any(keyword in text for keyword in ("range_backfill", "prebuild_range_speed")):
            offenders.append(path)
    assert offenders == []
