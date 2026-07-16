"""Compatibility-entrypoint tests for the canonical watchdog core."""


def test_parse_fatal_exit_codes_default_empty():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    assert _parse_fatal_exit_codes(None) == frozenset()
    assert _parse_fatal_exit_codes("") == frozenset()


def test_parse_fatal_exit_codes_single():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    assert _parse_fatal_exit_codes("78") == frozenset({78})


def test_parse_fatal_exit_codes_multi():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    assert _parse_fatal_exit_codes("78, 13, 42") == frozenset({78, 13, 42})


def test_parse_fatal_exit_codes_ignores_junk():
    from scripts.watchdog_live import _parse_fatal_exit_codes

    assert _parse_fatal_exit_codes("78,abc,42") == frozenset({78, 42})


def test_script_exports_canonical_watchdog_helpers():
    import scripts.watchdog_live as entrypoint
    from src.app import watchdog as core

    assert entrypoint.build_command is core.build_command
    assert entrypoint.run_live_watchdog is core.run_live_watchdog


def test_main_delegates_to_canonical_watchdog(monkeypatch):
    import scripts.watchdog_live as entrypoint

    calls = []
    monkeypatch.setattr(
        entrypoint,
        "run_live_watchdog",
        lambda *, project_root: calls.append(project_root) or 78,
    )

    assert entrypoint.main() == 78
    assert calls == [entrypoint.PROJECT_ROOT]
