from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


def load_env_config(env_file: str | Path | None = None, *, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Load project .env, then overlay process environment.

    Only exchange-specific API keys are resolved by the exchange credential
    helpers. This loader intentionally stays generic.
    """

    path = Path(env_file) if env_file is not None else Path(__file__).resolve().parents[2] / ".env"
    config: dict[str, str] = {}

    if path.exists():
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

    values = os.environ if environ is None else environ
    config.update({str(key): str(value) for key, value in values.items()})
    return config
