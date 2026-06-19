from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_all_tools_scripts_have_repo_root_bootstrap():
    missing = []
    for path in sorted((ROOT / "tools").glob("*.py")):
        text = path.read_text(encoding="utf-8")
        has_bootstrap = (
            "REPO_ROOT = Path(__file__).resolve().parents[1]" in text
            and "sys.path.insert(0, str(REPO_ROOT))" in text
        )
        if not has_bootstrap:
            missing.append(str(path.relative_to(ROOT)))
    assert missing == []
