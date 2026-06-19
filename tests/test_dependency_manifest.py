from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_requirements_file_declares_runtime_websocket_dependency():
    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert "websockets" in requirements
