from __future__ import annotations

import os

from src.platform.config import get_project_env_config, reset_project_env_config_for_tests


def test_bootstrap_live_process_config_uses_process_env_without_example(
    tmp_path,
    monkeypatch,
):
    import scripts.run_live as run_live

    reset_project_env_config_for_tests()
    project_root = tmp_path
    (project_root / ".env.example").write_text(
        "AETHER_EXAMPLE_ONLY=not-runtime-config\n"
        "OKX_API_KEY=placeholder-api-key\n",
        encoding="utf-8",
    )
    (project_root / ".env").write_text(
        "AETHER_LIVE_TRADING=false\nAETHER_EXCHANGES=okx,binance\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AETHER_LIVE_TRADING", "true")
    process_env_before = dict(os.environ)

    try:
        config = run_live.bootstrap_live_process_config(project_root)

        assert config.get("AETHER_LIVE_TRADING") == "true"
        assert config.get("AETHER_EXCHANGES") == "okx,binance"
        assert "AETHER_EXAMPLE_ONLY" not in config.values
        assert "OKX_API_KEY" not in config.values
        assert config.source_files == (str(project_root / ".env"),)
        assert config.example_file is None
        assert get_project_env_config() is config
        assert run_live.PROJECT_ENV_CONFIG is config
        assert dict(os.environ) == process_env_before
    finally:
        reset_project_env_config_for_tests()
