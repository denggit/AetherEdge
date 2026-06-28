from __future__ import annotations

from pathlib import Path


PLUGIN_ROOT = Path(__file__).resolve().parents[3] / "strategies" / "eth_lf_portfolio_v10a"


def test_plugin_does_not_import_research_or_exchange_clients() -> None:
    forbidden = (
        "coinbacktest",
        "from backtest",
        "import backtest",
        "from research",
        "import research",
        "/api/v5",
        "fapi",
        "dapi",
        "api/v3",
        "trading_client._client",
    )
    python_text = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in PLUGIN_ROOT.rglob("*.py")
    )

    for marker in forbidden:
        assert marker not in python_text


def test_plugin_does_not_build_range_bars_or_submit_orders() -> None:
    forbidden_imports = (
        "range_bar_builder",
        "rangebarbuilder",
        "okxclient",
        "binanceclient",
    )
    python_text = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in PLUGIN_ROOT.rglob("*.py")
    )

    for marker in forbidden_imports:
        assert marker not in python_text
