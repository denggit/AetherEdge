from __future__ import annotations

from tools import prebuild_range_speed_history as tool


def test_prebuild_defaults_do_not_require_db_args(monkeypatch) -> None:
    monkeypatch.delenv("AETHER_MARKET_DATA_DB", raising=False)

    args = tool.build_parser().parse_args(["--check-only"])
    request = tool.request_from_args(args)

    assert request.symbol == "ETH-USDT-PERP"
    assert str(request.market_db_path).endswith("aether_market_data.sqlite3")


def test_prebuild_buckets_100_takes_effect() -> None:
    args = tool.build_parser().parse_args(["--buckets", "100"])
    request = tool.request_from_args(args)

    assert request.required_buckets == 100
    assert request.max_buckets_per_cycle == 100
