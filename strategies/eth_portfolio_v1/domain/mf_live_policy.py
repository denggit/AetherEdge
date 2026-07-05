from __future__ import annotations

R007_MF_EXIT_VARIANT = "none"
MF_LIVE_EXIT_VARIANT = "time48"
_ALLOWED_MF_EXIT_VARIANTS = frozenset({MF_LIVE_EXIT_VARIANT})


def validate_mf_exit_variant(value: str) -> str:
    """Enforce the one live MF exit variant promoted by R008."""
    normalized = str(value).strip().lower()
    if normalized not in _ALLOWED_MF_EXIT_VARIANTS:
        raise ValueError(f"MF live exit variant is not allowed: {value!r}")
    return normalized
