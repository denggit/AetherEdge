from __future__ import annotations

from pathlib import Path


PLUGIN_ROOT = Path(__file__).resolve().parents[3] / "strategies" / "eth_lf_portfolio_v10b"


def test_plugin_does_not_import_backtest_or_exchange_clients() -> None:
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
        "okxclient",
        "binanceclient",
    )
    python_text = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in PLUGIN_ROOT.rglob("*.py")
    )

    for marker in forbidden:
        assert marker not in python_text


def test_v10b_package_exports_its_own_strategy() -> None:
    init_text = (PLUGIN_ROOT / "__init__.py").read_text(encoding="utf-8")

    assert "eth_lf_portfolio_v10b.strategy" in init_text
    assert "eth_lf_portfolio_v10a.strategy" not in init_text
