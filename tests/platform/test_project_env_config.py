from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.platform.config import load_project_env_config


def test_load_project_env_config_ignores_env_example(tmp_path):
    example = tmp_path / ".env.example"
    env = tmp_path / ".env"
    example.write_text(
        "\n".join(
            [
                "AETHER_LIVE_TRADING=false",
                "AETHER_EXAMPLE_ONLY=not-runtime-config",
                "OKX_API_KEY=placeholder-api-key",
            ]
        ),
        encoding="utf-8",
    )
    env.write_text(
        "\n".join(
            [
                "# project runtime values",
                "AETHER_LIVE_TRADING=true",
                "AETHER_EXCHANGES='okx,binance'",
                "",
            ]
        ),
        encoding="utf-8",
    )

    config = load_project_env_config(
        example_file=example,
        env_file=env,
        process_env={},
    )

    assert config.get("AETHER_LIVE_TRADING") == "true"
    assert config.get("AETHER_EXCHANGES") == "okx,binance"
    assert "AETHER_EXAMPLE_ONLY" not in config.values
    assert "OKX_API_KEY" not in config.values
    assert config.source_files == (str(env),)
    assert config.example_file is None


def test_project_env_config_process_env_overrides_dotenv_without_mutation(tmp_path):
    env = tmp_path / ".env"
    env.write_text(
        "AETHER_LIVE_TRADING=false\nAETHER_EXCHANGES=okx,binance\n",
        encoding="utf-8",
    )
    process_env = {"AETHER_LIVE_TRADING": "true"}
    original_process_env = dict(process_env)
    original_os_environ = dict(os.environ)

    config = load_project_env_config(
        env_file=env,
        process_env=process_env,
    )

    assert config.get("AETHER_LIVE_TRADING") == "true"
    assert config.get("AETHER_EXCHANGES") == "okx,binance"
    assert process_env == original_process_env
    assert dict(os.environ) == original_os_environ


def test_legacy_include_process_env_false_cannot_disable_process_env(tmp_path):
    env = tmp_path / ".env"
    env.write_text("AETHER_LIVE_TRADING=true\n", encoding="utf-8")

    config = load_project_env_config(
        env_file=env,
        include_process_env=False,
        process_env={"AETHER_LIVE_TRADING": "false"},
    )

    assert config.get("AETHER_LIVE_TRADING") == "false"


def test_project_env_config_values_are_read_only(tmp_path):
    env = tmp_path / ".env"
    env.write_text("AETHER_LIVE_TRADING=true\n", encoding="utf-8")
    config = load_project_env_config(env_file=env, process_env={})

    with pytest.raises(TypeError):
        config.values["AETHER_LIVE_TRADING"] = "false"


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
