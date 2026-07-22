from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.platform import config as config_module
from src.platform.config import load_env_config, load_project_env_config


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


def test_project_env_config_process_env_overrides_dotenv_without_mutation(
    tmp_path,
    monkeypatch,
):
    env = tmp_path / ".env"
    env.write_text(
        "AETHER_LIVE_TRADING=false\nAETHER_EXCHANGES=okx,binance\n",
        encoding="utf-8",
    )
    process_env = {"AETHER_LIVE_TRADING": "true"}
    original_process_env = dict(process_env)
    monkeypatch.setenv("AETHER_TEST_ENV_SENTINEL", "sentinel-before-load")
    monkeypatch.setenv("AETHER_LIVE_TRADING", "host-fake-value")

    config = load_project_env_config(
        env_file=env,
        process_env=process_env,
    )

    assert config.get("AETHER_LIVE_TRADING") == "true"
    assert config.get("AETHER_EXCHANGES") == "okx,binance"
    assert process_env == original_process_env
    assert os.environ["AETHER_TEST_ENV_SENTINEL"] == "sentinel-before-load"
    assert os.environ["AETHER_LIVE_TRADING"] == "host-fake-value"


@pytest.mark.parametrize(
    "loader",
    (load_project_env_config, load_env_config),
)
def test_runtime_env_loaders_reject_env_example_before_parsing(
    tmp_path,
    monkeypatch,
    loader,
):
    example = tmp_path / ".env.example"
    fake_secret = "FAKE_SECRET_MUST_NOT_APPEAR"
    example.write_text(
        f"OKX_API_KEY={fake_secret}\n"
        f"BINANCE_SECRET_KEY={fake_secret}\n",
        encoding="utf-8",
    )

    def fail_if_parsed(_path):
        pytest.fail(".env.example reached the runtime parser")

    monkeypatch.setattr(config_module, "_parse_env_file", fail_if_parsed)

    kwargs = (
        {"process_env": {}}
        if loader is load_project_env_config
        else {"environ": {}}
    )
    with pytest.raises(
        ValueError,
        match="documentation-only",
    ) as exc_info:
        loader(env_file=example, **kwargs)

    assert fake_secret not in str(exc_info.value)


@pytest.mark.parametrize(
    "loader",
    (load_project_env_config, load_env_config),
)
def test_runtime_env_loaders_accept_custom_env_filename(tmp_path, loader):
    env = tmp_path / "production.env"
    env.write_text("AETHER_MARKET=ETH-USDT-PERP\n", encoding="utf-8")

    kwargs = (
        {"process_env": {}}
        if loader is load_project_env_config
        else {"environ": {}}
    )
    config = loader(env_file=env, **kwargs)
    values = config if isinstance(config, dict) else config.values

    assert values["AETHER_MARKET"] == "ETH-USDT-PERP"


def test_project_env_loader_rejects_symlink_to_env_example(tmp_path, monkeypatch):
    example = tmp_path / ".env.example"
    link = tmp_path / ".env"
    example.write_text("OKX_API_KEY=fake-symlink-secret\n", encoding="utf-8")
    try:
        link.symlink_to(example)
    except OSError:
        original_resolve = Path.resolve
        monkeypatch.setattr(
            Path,
            "resolve",
            lambda self: example if self == link else original_resolve(self),
        )

    with pytest.raises(ValueError, match="documentation-only"):
        load_project_env_config(env_file=link, process_env={})


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
