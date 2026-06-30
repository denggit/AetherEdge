from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProjectEnvConfig:
    values: Mapping[str, str]
    source_files: tuple[str, ...]
    env_file: Path
    example_file: Path | None

    def get(self, key: str, default: str = "") -> str:
        return self.values.get(key, default)

    def get_bool(self, key: str, default: bool = False) -> bool:
        raw = self.values.get(key)
        if raw is None or raw == "":
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}

    def get_int(self, key: str, default: int = 0) -> int:
        raw = self.values.get(key)
        if raw is None or raw == "":
            return default
        return int(str(raw).strip())

    def get_float(self, key: str, default: float = 0.0) -> float:
        raw = self.values.get(key)
        if raw is None or raw == "":
            return default
        return float(str(raw).strip())

    def subset(self, prefix: str) -> dict[str, str]:
        return {key: value for key, value in self.values.items() if key.startswith(prefix)}


_PROJECT_ENV_CONFIG: ProjectEnvConfig | None = None


def load_project_env_config(
    env_file: str | Path | None = None,
    example_file: str | Path | None = None,
    *,
    include_process_env: bool = False,
    process_env: Mapping[str, str] | None = None,
) -> ProjectEnvConfig:
    """Load a project-wide config snapshot without mutating ``os.environ``.

    ``.env.example`` acts as the complete key/default registry and ``.env``
    overlays user-specific values. Process environment overrides are opt-in so
    live trading config is not silently changed by the shell that launches it.
    """

    root = Path(__file__).resolve().parents[2]
    env_path = Path(env_file) if env_file is not None else root / ".env"
    example_path = Path(example_file) if example_file is not None else root / ".env.example"
    config: dict[str, str] = {}
    source_files: list[str] = []

    if example_path.exists():
        config.update(_parse_env_file(example_path))
        source_files.append(str(example_path))

    if env_path.exists():
        config.update(_parse_env_file(env_path))
        source_files.append(str(env_path))

    if include_process_env:
        values = os.environ if process_env is None else process_env
        config.update({str(key): str(value) for key, value in values.items()})

    return ProjectEnvConfig(
        values=MappingProxyType(dict(config)),
        source_files=tuple(source_files),
        env_file=env_path,
        example_file=example_path if example_path.exists() else None,
    )


def set_project_env_config(config: ProjectEnvConfig) -> None:
    global _PROJECT_ENV_CONFIG
    _PROJECT_ENV_CONFIG = config


def get_project_env_config() -> ProjectEnvConfig:
    global _PROJECT_ENV_CONFIG
    if _PROJECT_ENV_CONFIG is None:
        logger.warning("Project env config accessed before initialization; lazy-loading from project files")
        _PROJECT_ENV_CONFIG = load_project_env_config()
    return _PROJECT_ENV_CONFIG


def has_project_env_config() -> bool:
    return _PROJECT_ENV_CONFIG is not None


def reset_project_env_config_for_tests() -> None:
    global _PROJECT_ENV_CONFIG
    _PROJECT_ENV_CONFIG = None


def load_env_config(env_file: str | Path | None = None, *, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Load project .env, then overlay process environment.

    Only exchange-specific API keys are resolved by the exchange credential
    helpers. This loader intentionally stays generic.
    """

    path = Path(env_file) if env_file is not None else Path(__file__).resolve().parents[2] / ".env"
    config = _parse_env_file(path) if path.exists() else {}

    values = os.environ if environ is None else environ
    config.update({str(key): str(value) for key, value in values.items()})
    return config


def _parse_env_file(path: Path) -> dict[str, str]:
    config: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        config[key] = value
    return config
