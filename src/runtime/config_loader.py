from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from src.platform.config import (
    get_project_env_config,
    load_env_config,
)


def load_runtime_env(
    *,
    env_file: str | Path | None,
    environ: Mapping[str, str] | None,
) -> dict[str, str]:
    if environ is None and env_file is None:
        return dict(get_project_env_config().values)
    if environ is not None and env_file is None:
        return {str(key): str(value) for key, value in environ.items()}
    return dict(load_env_config(env_file, environ=environ))


def load_runtime_defaults(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {}
    return json.loads(target.read_text(encoding="utf-8"))


__all__ = ["load_runtime_defaults", "load_runtime_env"]
