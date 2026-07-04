from __future__ import annotations

R007_MF_EXIT_VARIANT = "none"
_ALLOWED_MF_EXIT_VARIANTS = frozenset({"none", "time48"})


def validate_mf_exit_variant(value: str) -> str:
    """Allow no live exit in R007 and the single approved future variant."""
    normalized = str(value).strip().lower()
    if normalized not in _ALLOWED_MF_EXIT_VARIANTS:
        raise ValueError(f"MF live exit variant is not allowed: {value!r}")
    return normalized
