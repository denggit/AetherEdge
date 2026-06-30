from __future__ import annotations

import os

from src.platform.config import get_project_env_config, reset_project_env_config_for_tests


def test_bootstrap_live_process_config_initializes_snapshot_without_os_environ(tmp_path):
    import scripts.run_live as run_live

    reset_project_env_config_for_tests()
    project_root = tmp_path
    (project_root / ".env.example").write_text(
        "AETHER_LIVE_TRADING=false\nAETHER_EXCHANGES=okx\n",
        encoding="utf-8",
    )
    (project_root / ".env").write_text(
        "AETHER_LIVE_TRADING=true\nAETHER_EXCHANGES=okx,binance\n",
        encoding="utf-8",
    )
    os.environ.pop("AETHER_LIVE_TRADING", None)

    config = run_live.bootstrap_live_process_config(project_root)

    assert config.get("AETHER_LIVE_TRADING") == "true"
    assert get_project_env_config().get("AETHER_LIVE_TRADING") == "true"
    assert "AETHER_LIVE_TRADING" not in os.environ
