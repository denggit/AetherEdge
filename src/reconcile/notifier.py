from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from typing import Any

from src.reconcile.models import ReconcileReport, ReconcileSeverity


class NoopReconcileNotifier:
    async def notify(self, report: ReconcileReport) -> None:
        return None


class EmailReconcileNotifier:
    """Optional warning notifier for reconciliation reports.

    The checker stays read-only. This adapter only sends a warning when the report
    contains issues at or above ``min_severity``.

    It is compatible with the existing async ``src.utils.email_sender.send_email``
    signature: ``send_email(subject, content, content_type='plain')``.
    """

    def __init__(
        self,
        *,
        enabled: bool = True,
        min_severity: ReconcileSeverity = ReconcileSeverity.WARNING,
        subject_prefix: str = "[AetherEdge Reconcile Warning]",
        email_sender: Callable[..., Any] | None = None,
    ) -> None:
        self.enabled = enabled
        self.min_severity = min_severity
        self.subject_prefix = subject_prefix
        self._email_sender = email_sender

    async def notify(self, report: ReconcileReport) -> None:
        if not self.enabled:
            return
        issues = report.issues_at_or_above(self.min_severity)
        if not issues:
            return
        subject = f"{self.subject_prefix} {report.exchange.value} {report.symbol} {len(issues)} issue(s)"
        body = format_reconcile_report(report, min_severity=self.min_severity)
        sender = self._email_sender or _load_default_email_sender()
        await _call_email_sender(sender, subject, body)


def format_reconcile_report(report: ReconcileReport, *, min_severity: ReconcileSeverity = ReconcileSeverity.INFO) -> str:
    lines = [
        "AetherEdge reconciliation warning",
        f"exchange: {report.exchange.value}",
        f"symbol: {report.symbol}",
        f"checked_at_ms: {report.checked_at_ms}",
        "",
        "issues:",
    ]
    selected = report.issues_at_or_above(min_severity)
    if not selected:
        lines.append("- none")
        return "\n".join(lines)
    for issue in selected:
        lines.append(f"- [{issue.severity.name}] {issue.category.value}: {issue.message}")
        if issue.entity_id:
            lines.append(f"  entity_id: {issue.entity_id}")
        if issue.local:
            lines.append(f"  local: {dict(issue.local)}")
        if issue.remote:
            lines.append(f"  remote: {dict(issue.remote)}")
    return "\n".join(lines)


def _load_default_email_sender() -> Callable[..., Any]:
    module = importlib.import_module("src.utils.email_sender")
    sender = getattr(module, "send_reconcile_warning", None)
    sender = sender or getattr(module, "send_warning_email", None)
    sender = sender or getattr(module, "send_email", None)
    if sender is None:
        raise RuntimeError("src.utils.email_sender must expose send_email() or a warning-email helper")
    return sender


async def _call_email_sender(sender: Callable[..., Any], subject: str, body: str) -> None:
    result = _invoke_email_sender(sender, subject, body)
    if inspect.isawaitable(result):
        await result


def _invoke_email_sender(sender: Callable[..., Any], subject: str, body: str) -> Any:
    try:
        params = inspect.signature(sender).parameters
    except (TypeError, ValueError):
        params = {}

    if _accepts_kwargs(params) or "content" in params:
        return sender(subject=subject, content=body, content_type="plain")
    if "body" in params:
        return sender(subject=subject, body=body)
    return sender(subject, body)


def _accepts_kwargs(params: MappingLike) -> bool:
    return any(param.kind is inspect.Parameter.VAR_KEYWORD for param in params.values())


MappingLike = dict[str, inspect.Parameter]
