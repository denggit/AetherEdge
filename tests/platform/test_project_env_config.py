from __future__ import annotations

from pathlib import Path

from src.platform.config import load_project_env_config


def test_load_project_env_config_reads_all_keys_from_env_example(tmp_path):
    example = tmp_path / ".env.example"
    env = tmp_path / ".env"
    example.write_text(
        "\n".join(
            [
                "AETHER_LIVE_TRADING=false",
                "AETHER_EXCHANGES=okx",
                "BINANCE_LEVERAGE=15",
                "NEW_FUTURE_KEY=abc",
            ]
        ),
        encoding="utf-8",
    )
    env.write_text(
        "\n".join(
            [
                "AETHER_LIVE_TRADING=true",
                "AETHER_EXCHANGES=okx,binance",
            ]
        ),
        encoding="utf-8",
    )

    config = load_project_env_config(example_file=example, env_file=env, include_process_env=False)

    assert config.get("AETHER_LIVE_TRADING") == "true"
    assert config.get("AETHER_EXCHANGES") == "okx,binance"
    assert config.get("BINANCE_LEVERAGE") == "15"
    assert config.get("NEW_FUTURE_KEY") == "abc"


def test_project_env_config_does_not_use_process_env_by_default(tmp_path):
    example = tmp_path / ".env.example"
    env = tmp_path / ".env"
    example.write_text("AETHER_LIVE_TRADING=false\n", encoding="utf-8")
    env.write_text("AETHER_LIVE_TRADING=true\n", encoding="utf-8")

    config = load_project_env_config(
        example_file=example,
        env_file=env,
        include_process_env=False,
        process_env={"AETHER_LIVE_TRADING": "false"},
    )

    assert config.get("AETHER_LIVE_TRADING") == "true"


def test_project_env_config_can_include_process_env_when_explicit(tmp_path):
    example = tmp_path / ".env.example"
    env = tmp_path / ".env"
    example.write_text("AETHER_LIVE_TRADING=false\n", encoding="utf-8")
    env.write_text("AETHER_LIVE_TRADING=true\n", encoding="utf-8")

    config = load_project_env_config(
        example_file=example,
        env_file=env,
        include_process_env=True,
        process_env={"AETHER_LIVE_TRADING": "false"},
    )

    assert config.get("AETHER_LIVE_TRADING") == "false"


def test_live_safety_config_core_paths_do_not_use_bare_os_getenv():
    root = Path(__file__).resolve().parents[2]
    core_files = [
        root / "src/app/config.py",
        root / "src/runtime/config.py",
        root / "src/runtime/account_config.py",
        root / "src/runtime/runner.py",
    ]
    safety_keys = [
        "AETHER_LIVE_TRADING",
        "_SANDBOX",
        "SANDBOX",
        "OKX_LEVERAGE",
        "BINANCE_LEVERAGE",
        "MARGIN_MODE",
    ]

    offenders: list[str] = []
    for path in core_files:
        text = path.read_text(encoding="utf-8")
        for key in safety_keys:
            needle = f'os.getenv("{key}'
            if needle in text:
                offenders.append(f"{path.relative_to(root)}:{key}")

    assert offenders == []
