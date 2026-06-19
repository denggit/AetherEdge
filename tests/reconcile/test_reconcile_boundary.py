from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_reconcile_lives_outside_platform():
    assert (ROOT / "src" / "reconcile" / "checker.py").exists()
    assert not (ROOT / "src" / "platform" / "reconcile").exists()


def test_reconcile_is_read_only_and_adapter_free():
    forbidden = [
        "/api/v5",
        "/fapi/",
        "OkxExchangeClient",
        "BinanceExchangeClient",
        ".place_order(",
        ".cancel_order(",
        ".amend_order(",
        "create_exchange_client",
    ]
    leaks = []
    for path in (ROOT / "src" / "reconcile").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for token in forbidden:
            if token in text:
                leaks.append((str(path.relative_to(ROOT)), token))
    assert leaks == []


def test_email_sender_stays_outside_reconcile_core():
    checker = (ROOT / "src" / "reconcile" / "checker.py").read_text(encoding="utf-8")
    assert "email_sender" not in checker
    assert "smtplib" not in checker
