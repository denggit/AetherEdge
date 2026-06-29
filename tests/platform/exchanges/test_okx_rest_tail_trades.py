from __future__ import annotations

import ast
import io
import json
import urllib.error
from pathlib import Path

import pytest

from src.platform.exchanges.okx.rest_tail_trades import OkxRestTailTradesError, fetch_okx_history_trades_tail


class Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self._body = io.BytesIO(json.dumps(payload).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._body.read()


def _payload(rows: list[dict[str, str]]) -> dict[str, object]:
    return {"code": "0", "data": rows}


def test_parses_filters_sorts_and_deduplicates_history_trades() -> None:
    pages = [
        _payload(
            [
                {"instId": "ETH-USDT-SWAP", "tradeId": "3", "side": "sell", "px": "101", "sz": "2", "ts": "3000"},
                {"instId": "ETH-USDT-SWAP", "tradeId": "1", "side": "buy", "px": "100", "sz": "1", "ts": "1000"},
                {"instId": "ETH-USDT-SWAP", "tradeId": "3", "side": "sell", "px": "101", "sz": "2", "ts": "3000"},
                {"instId": "ETH-USDT-SWAP", "tradeId": "9", "side": "buy", "px": "999", "sz": "1", "ts": "9000"},
            ]
        ),
        _payload([]),
    ]

    def urlopen(*_args, **_kwargs):
        return Response(pages.pop(0))

    rows = fetch_okx_history_trades_tail(
        raw_symbol="ETH-USDT-SWAP",
        symbol="ETH-USDT-PERP",
        start_time_ms=1_000,
        end_time_ms=3_000,
        urlopen=urlopen,
        sleep_seconds=0,
    )

    assert [row.trade_id for row in rows] == ["1", "3"]
    assert [row.trade_time_ms for row in rows] == [1_000, 3_000]
    assert rows[0].symbol == "ETH-USDT-PERP"


def test_stops_at_max_pages() -> None:
    calls = 0

    def urlopen(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return Response(_payload([{"instId": "ETH-USDT-SWAP", "tradeId": str(calls), "side": "buy", "px": "100", "sz": "1", "ts": "2000"}]))

    rows = fetch_okx_history_trades_tail(
        raw_symbol="ETH-USDT-SWAP",
        symbol="ETH-USDT-PERP",
        start_time_ms=1_000,
        end_time_ms=3_000,
        urlopen=urlopen,
        limit=1,
        max_pages=2,
        sleep_seconds=0,
    )

    assert calls == 2
    assert len(rows) == 2


def test_handles_empty_page() -> None:
    rows = fetch_okx_history_trades_tail(
        raw_symbol="ETH-USDT-SWAP",
        symbol="ETH-USDT-PERP",
        start_time_ms=1_000,
        end_time_ms=3_000,
        urlopen=lambda *_args, **_kwargs: Response(_payload([])),
        sleep_seconds=0,
    )

    assert rows == []


def test_retries_429_then_returns_page() -> None:
    calls = 0

    def urlopen(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise urllib.error.HTTPError("url", 429, "too many", {}, None)
        return Response(_payload([{"instId": "ETH-USDT-SWAP", "tradeId": "1", "side": "buy", "px": "100", "sz": "1", "ts": "2000"}]))

    rows = fetch_okx_history_trades_tail(
        raw_symbol="ETH-USDT-SWAP",
        symbol="ETH-USDT-PERP",
        start_time_ms=1_000,
        end_time_ms=3_000,
        urlopen=urlopen,
        sleep_seconds=0,
    )

    assert calls == 2
    assert [row.trade_id for row in rows] == ["1"]


def test_non_retryable_http_error_is_controlled() -> None:
    def urlopen(*_args, **_kwargs):
        raise urllib.error.HTTPError("url", 400, "bad", {}, None)

    with pytest.raises(OkxRestTailTradesError):
        fetch_okx_history_trades_tail(
            raw_symbol="ETH-USDT-SWAP",
            symbol="ETH-USDT-PERP",
            start_time_ms=1_000,
            end_time_ms=3_000,
            urlopen=urlopen,
            sleep_seconds=0,
        )


def test_rest_tail_adapter_does_not_import_business_domains() -> None:
    tree = ast.parse(Path("src/platform/exchanges/okx/rest_tail_trades.py").read_text(encoding="utf-8"))
    imports = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    assert not any(module.startswith(("src.market_data", "src.runtime", "strategies")) for module in imports)
