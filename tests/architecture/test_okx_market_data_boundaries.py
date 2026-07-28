from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _python_text(root: Path) -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in root.rglob("*.py")
    )


def test_runtime_and_strategies_do_not_parse_okx_book_protocol() -> None:
    runtime_text = _python_text(ROOT / "src" / "runtime")
    strategies_text = _python_text(ROOT / "strategies")
    for token in (
        "/api/v5/market/books-full",
        "prevSeqId",
        "seqId",
        "checksum",
    ):
        assert token not in runtime_text
        assert token not in strategies_text


def test_strategies_do_not_import_okx_market_data_adapters() -> None:
    strategies_text = _python_text(ROOT / "strategies")
    assert "platform.data.websocket.okx" not in strategies_text
    assert "platform.data.rest.okx" not in strategies_text
    assert "books-full" not in strategies_text
    assert "open-interest" not in strategies_text


def test_new_capabilities_and_callbacks_are_distinct() -> None:
    capabilities = (
        ROOT / "src" / "runtime" / "capabilities.py"
    ).read_text(encoding="utf-8")
    host = (
        ROOT / "src" / "runtime" / "strategy_host.py"
    ).read_text(encoding="utf-8")
    assert 'CapabilityId("market.order_book")' in capabilities
    assert 'CapabilityId("market.order_book_l2")' in capabilities
    assert 'CapabilityId("market.full_order_book")' in capabilities
    assert 'CapabilityId("market.open_interest")' in capabilities
    assert "on_order_book_l2" in host
    assert "on_full_order_book" in host
    assert "on_open_interest" in host


def test_no_oi_rest_fallback_or_checksum_implementation() -> None:
    source_text = _python_text(ROOT / "src")
    assert "/api/v5/public/open-interest" not in source_text
    assert "zlib.crc32" not in source_text
