from __future__ import annotations

import importlib
import os
import sys
from contextlib import contextmanager

from src.platform import config as platform_config
from src.platform.config import get_project_env_config


@contextmanager
def _isolated_run_live_import(tmp_path):
    module_name = "scripts.run_live"
    previous_module = sys.modules.pop(module_name, None)
    previous_project_env = platform_config._PROJECT_ENV_CONFIG
    real_loader = platform_config.load_project_env_config
    import_env = tmp_path / "isolated-import.env"
    import_env.write_text("", encoding="utf-8")
    import_config = real_loader(
        env_file=import_env,
        process_env={},
    )
    imported_module = None

    def isolated_import_loader(*_args, **_kwargs):
        return import_config

    platform_config.load_project_env_config = isolated_import_loader
    try:
        imported_module = importlib.import_module(module_name)
        platform_config.load_project_env_config = real_loader
        imported_module.load_project_env_config = real_loader
        yield imported_module, import_config
    finally:
        platform_config.load_project_env_config = real_loader
        if imported_module is not None:
            imported_module.load_project_env_config = real_loader
            imported_module.PROJECT_ENV_CONFIG = import_config
        if previous_project_env is None:
            platform_config.reset_project_env_config_for_tests()
        else:
            platform_config.set_project_env_config(previous_project_env)
        sys.modules.pop(module_name, None)
        if previous_module is not None:
            sys.modules[module_name] = previous_module


def test_bootstrap_live_process_config_uses_process_env_without_example(
    tmp_path,
    monkeypatch,
):
    previous_module = sys.modules.get("scripts.run_live")
    previous_module_config = (
        getattr(previous_module, "PROJECT_ENV_CONFIG", None)
        if previous_module is not None
        else None
    )
    previous_module_loader = (
        getattr(previous_module, "load_project_env_config", None)
        if previous_module is not None
        else None
    )
    previous_platform_loader = platform_config.load_project_env_config
    previous_project_env = platform_config._PROJECT_ENV_CONFIG
    project_root = tmp_path / "runtime-project"
    project_root.mkdir()
    (project_root / ".env.example").write_text(
        "AETHER_EXAMPLE_ONLY=not-runtime-config\n"
        "OKX_API_KEY=fake-placeholder-api-key\n",
        encoding="utf-8",
    )
    (project_root / ".env").write_text(
        "AETHER_LIVE_TRADING=false\nAETHER_EXCHANGES=okx,binance\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AETHER_TEST_ENV_SENTINEL", "sentinel-before-load")
    monkeypatch.setenv("AETHER_LIVE_TRADING", "true")

    with _isolated_run_live_import(tmp_path) as (run_live, import_config):
        assert run_live.PROJECT_ENV_CONFIG is import_config

        config = run_live.bootstrap_live_process_config(project_root)

        assert config.get("AETHER_LIVE_TRADING") == "true"
        assert config.get("AETHER_EXCHANGES") == "okx,binance"
        assert "AETHER_EXAMPLE_ONLY" not in config.values
        assert "OKX_API_KEY" not in config.values
        assert config.source_files == (str(project_root / ".env"),)
        assert config.example_file is None
        assert get_project_env_config() is config
        assert run_live.PROJECT_ENV_CONFIG is config
        assert os.environ["AETHER_TEST_ENV_SENTINEL"] == "sentinel-before-load"
        assert os.environ["AETHER_LIVE_TRADING"] == "true"

    assert platform_config._PROJECT_ENV_CONFIG is previous_project_env
    assert platform_config.load_project_env_config is previous_platform_loader
    assert sys.modules.get("scripts.run_live") is previous_module
    if previous_module is not None:
        assert previous_module.PROJECT_ENV_CONFIG is previous_module_config
        assert previous_module.load_project_env_config is previous_module_loader
