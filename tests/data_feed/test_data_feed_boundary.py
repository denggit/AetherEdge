from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_FEED = ROOT / "src" / "platform" / "data"


def _data_feed_py_files():
    return sorted(DATA_FEED.rglob("*.py"))


def test_data_feed_does_not_contain_exchange_rest_endpoints():
    leaks = []
    for path in _data_feed_py_files():
        text = path.read_text(encoding="utf-8")
        if "/api/v5" in text or "/fapi/" in text:
            leaks.append(str(path.relative_to(ROOT)))
    assert leaks == []


def test_data_feed_does_not_call_private_trading_methods():
    forbidden_tokens = [
        ".place_order(",
        ".cancel_order(",
        ".fetch_balance(",
        ".fetch_positions(",
        "OrderRequest",
        "CancelOrderRequest",
    ]
    leaks = []
    for path in _data_feed_py_files():
        text = path.read_text(encoding="utf-8")
        hits = [token for token in forbidden_tokens if token in text]
        if hits:
            leaks.append((str(path.relative_to(ROOT)), hits))
    assert leaks == []
