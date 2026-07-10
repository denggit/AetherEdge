from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from strategies.eth_portfolio_v1.domain.mf_signal import (
    MF_ENGINE_NAME,
    MF_POSITION_ID_PREFIX,
    MF_TIME_EXIT_BARS,
)
from strategies.eth_portfolio_v1.domain.sleeves import (
    LF_SLEEVE_ID,
    MF_RESERVED_SLEEVE_ID,
)


MF_RECOVERY_REQUIRED_METADATA = (
    "sleeve_id",
    "position_id",
    "engine",
    "entry_execution_time_ms",
    "entry_tradebar_open_time_ms",
    "signal_time_ms",
    "time48_holding_minutes",
    "exit_variant",
    "quantity_scope",
    "average_entry_price",
)

_MINUTE_MS = 60_000


def merged_plan_metadata(plan_payload: Mapping[str, Any]) -> dict[str, Any]:
    position = _position(plan_payload)
    direct = position.get("metadata")
    direct_mapping = dict(direct) if isinstance(direct, Mapping) else {}
    nested = direct_mapping.get("signal_metadata")
    merged = dict(nested) if isinstance(nested, Mapping) else {}
    merged.update(
        {
            key: value
            for key, value in direct_mapping.items()
            if key != "signal_metadata"
        }
    )
    merged.setdefault("position_id", position.get("position_id"))
    merged.setdefault("engine", position.get("entry_engine"))
    return merged


def plan_sleeve_id(plan_payload: Mapping[str, Any]) -> str | None:
    position = _position(plan_payload)
    metadata = merged_plan_metadata(plan_payload)
    explicit = str(metadata.get("sleeve_id") or "").strip().lower()
    if explicit:
        return explicit if explicit in {LF_SLEEVE_ID, MF_RESERVED_SLEEVE_ID} else None
    engine = str(
        metadata.get("engine") or position.get("entry_engine") or ""
    ).strip()
    position_id = str(position.get("position_id") or "")
    if engine == MF_ENGINE_NAME or position_id.startswith(MF_POSITION_ID_PREFIX):
        return MF_RESERVED_SLEEVE_ID
    return LF_SLEEVE_ID


def audit_portfolio_v1_plans(
    plans: Sequence[Mapping[str, Any]],
    *,
    now_ms: int | None = None,
) -> dict[str, Any]:
    checked_at_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    by_sleeve: dict[str, list[Mapping[str, Any]]] = {
        LF_SLEEVE_ID: [],
        MF_RESERVED_SLEEVE_ID: [],
    }
    issues: list[str] = []
    plan_audits: dict[str, dict[str, Any]] = {}

    for payload in plans:
        position = _position(payload)
        position_id = str(position.get("position_id") or "").strip()
        audit_key = position_id or f"missing-position-{len(plan_audits) + 1}"
        sleeve_id = plan_sleeve_id(payload)
        status = str(position.get("status") or "").strip().lower()
        plan_issues: list[str] = []
        if not position_id:
            plan_issues.append("missing_position_id")
        if sleeve_id is None:
            plan_issues.append("unparseable_sleeve_id")
        elif sleeve_id == LF_SLEEVE_ID and position_id.startswith(
            MF_POSITION_ID_PREFIX
        ):
            plan_issues.append("lf_position_id_has_mf_prefix")
        elif sleeve_id == MF_RESERVED_SLEEVE_ID and not position_id.startswith(
            MF_POSITION_ID_PREFIX
        ):
            plan_issues.append("mf_position_id_prefix_invalid")
        if status != "active":
            plan_issues.append(f"unsafe_plan_status:{status or 'unknown'}")

        if sleeve_id is not None:
            by_sleeve[sleeve_id].append(payload)
        if sleeve_id == MF_RESERVED_SLEEVE_ID:
            plan_issues.extend(_mf_plan_issues(payload))
        elif sleeve_id == LF_SLEEVE_ID:
            if _positive_decimal(
                position.get("master_filled_qty_base")
                or position.get("master_target_qty_base")
            ) is None:
                plan_issues.append("lf_quantity_missing")

        issues.extend(
            f"{audit_key}:{issue}" for issue in _dedupe(plan_issues)
        )
        plan_audits[audit_key] = {
            "position_id": position_id or None,
            "sleeve_id": sleeve_id,
            "side": str(position.get("side") or "").strip().lower() or None,
            "status": status or None,
            "issues": list(_dedupe(plan_issues)),
        }

    for sleeve_id, sleeve_plans in by_sleeve.items():
        if len(sleeve_plans) > 1:
            issues.append(f"duplicated_active_plan:{sleeve_id}")

    lf_payload = _only(by_sleeve[LF_SLEEVE_ID])
    mf_payload = _only(by_sleeve[MF_RESERVED_SLEEVE_ID])
    lf_audit = _sleeve_audit(lf_payload, sleeve_id=LF_SLEEVE_ID)
    mf_audit = _sleeve_audit(mf_payload, sleeve_id=MF_RESERVED_SLEEVE_ID)
    if lf_payload is not None:
        lf_position_id = str(_position(lf_payload).get("position_id") or "")
        lf_audit["issues"] = list(
            plan_audits.get(lf_position_id, {}).get("issues", ())
        )
    if mf_payload is not None:
        mf_position_id = str(_position(mf_payload).get("position_id") or "")
        mf_audit["issues"] = list(
            plan_audits.get(mf_position_id, {}).get("issues", ())
        )
    mf_metadata = (
        merged_plan_metadata(mf_payload) if mf_payload is not None else {}
    )
    protective_stop_required = mf_metadata.get(
        "protective_stop_required"
    )
    mf_stop_expected = bool(
        mf_payload is not None
        and protective_stop_required in (True, "true", "True", 1)
    )
    if mf_payload is not None:
        entry_open_ms = _integer_or_none(
            mf_metadata.get("entry_tradebar_open_time_ms")
        )
        holding_minutes = (
            _holding_minutes(checked_at_ms, entry_open_ms)
            if entry_open_ms is not None
            else None
        )
        mf_audit.update(
            {
                "entry_execution_time_ms": _integer_or_none(
                    mf_metadata.get("entry_execution_time_ms")
                ),
                "entry_tradebar_open_time_ms": entry_open_ms,
                "holding_minutes_at_recovery": holding_minutes,
                "time48_due_at_recovery": (
                    holding_minutes is not None
                    and holding_minutes >= MF_TIME_EXIT_BARS
                ),
                "stop_expected": mf_stop_expected,
                "stop_validated": None,
            }
        )
    lf_audit.update(
        {
            "stop_expected": bool(lf_payload is not None),
            "stop_validated": None,
        }
    )

    unique_issues = list(_dedupe(issues))
    position_ids = [
        str(_position(payload).get("position_id"))
        for payload in plans
        if _position(payload).get("position_id")
    ]
    return {
        "strategy": "eth_portfolio_v1",
        "checked_at_ms": checked_at_ms,
        "recovery_ok": not unique_issues,
        "manual_required": bool(unique_issues),
        "startup_blocked": bool(unique_issues),
        "hard_fail": False,
        "active_sleeves": [
            sleeve_id
            for sleeve_id, sleeve_plans in by_sleeve.items()
            if sleeve_plans
        ],
        "active_position_ids": position_ids,
        "lf": lf_audit,
        "mf": mf_audit,
        "plans": {
            "active_count": len(plans),
            "by_position_id": plan_audits,
        },
        "exchange": {},
        "issues": unique_issues,
    }


def _mf_plan_issues(plan_payload: Mapping[str, Any]) -> list[str]:
    position = _position(plan_payload)
    metadata = merged_plan_metadata(plan_payload)
    issues: list[str] = []
    missing = [
        field
        for field in MF_RECOVERY_REQUIRED_METADATA
        if metadata.get(field) in (None, "")
    ]
    issues.extend(f"mf_missing_metadata:{field}" for field in missing)
    if str(position.get("side") or "").strip().lower() != "long":
        issues.append("mf_side_must_be_long")
    if metadata.get("sleeve_id") not in (None, MF_RESERVED_SLEEVE_ID):
        issues.append("mf_sleeve_id_invalid")
    if str(metadata.get("engine") or "") != MF_ENGINE_NAME:
        issues.append("mf_engine_invalid")
    if str(metadata.get("exit_variant") or "") != "time48":
        issues.append("mf_exit_variant_invalid")
    if str(metadata.get("quantity_scope") or "") != "mf_sleeve_quantity":
        issues.append("mf_quantity_scope_invalid")
    if _integer_or_none(metadata.get("time48_holding_minutes")) != MF_TIME_EXIT_BARS:
        issues.append("mf_holding_minutes_invalid")
    if _positive_decimal(metadata.get("average_entry_price")) is None:
        issues.append("mf_average_entry_price_invalid")
    if _positive_decimal(
        position.get("master_filled_qty_base")
        or position.get("master_target_qty_base")
    ) is None:
        issues.append("mf_quantity_missing")
    metadata_quantities = _positive_decimal_mapping(
        metadata.get("exchange_quantities_base")
    )
    protective_stop_required = metadata.get("protective_stop_required")
    if protective_stop_required not in (
        False, "false", "False", 0,
        True, "true", "True", 1,
    ):
        issues.append("mf_no_stop_policy_missing")
    elif protective_stop_required in (True, "true", "True", 1):
        # When hard stop is enabled, protective_stop_required=True is
        # the expected live state. Validate stop fields if they are
        # present in the metadata (old plans without stop data are
        # tolerated to avoid blocking legacy recovery).
        stop_price = metadata.get("stop_price") or metadata.get(
            "hard_stop_price"
        )
        stop_ids = metadata.get("stop_order_ids_by_exchange")
        if stop_price is not None or stop_ids is not None:
            if stop_price is None or _positive_decimal(stop_price) is None:
                issues.append(
                    "mf_protective_stop_required_but_price_missing"
                )
    legs = tuple(
        dict(item)
        for item in plan_payload.get("legs", ())
        if isinstance(item, Mapping)
    )
    if not legs:
        issues.append("mf_plan_legs_missing")
    leg_quantities: dict[str, Decimal] = {}
    for leg in legs:
        exchange = str(leg.get("exchange") or "unknown").strip().lower()
        sync_status = str(leg.get("sync_status") or "").strip().lower()
        leg_quantity = _positive_decimal(
            leg.get("filled_qty_base") or leg.get("target_qty_base")
        )
        if exchange != "unknown" and leg_quantity is not None:
            leg_quantities[exchange] = leg_quantity
        if sync_status not in {"open", "synced"}:
            issues.append(
                f"mf_leg_not_recoverable:{exchange}:{sync_status or 'unknown'}"
            )
    for exchange, quantity in metadata_quantities.items():
        leg_quantity = leg_quantities.get(exchange)
        if leg_quantity is None:
            issues.append(f"mf_metadata_quantity_leg_missing:{exchange}")
            continue
        tolerance = quantity * Decimal("0.05")
        if abs(leg_quantity - quantity) > tolerance:
            issues.append(
                "mf_metadata_quantity_mismatch:"
                f"{exchange}:metadata={quantity}:leg={leg_quantity}"
            )
    return list(_dedupe(issues))


def _sleeve_audit(
    payload: Mapping[str, Any] | None,
    *,
    sleeve_id: str,
) -> dict[str, Any]:
    if payload is None:
        return {
            "active": False,
            "sleeve_id": sleeve_id,
            "position_id": None,
            "side": None,
            "quantity": "0",
            "entry_price": None,
            "issues": [],
        }
    position = _position(payload)
    metadata = merged_plan_metadata(payload)
    quantity = (
        position.get("master_filled_qty_base")
        or position.get("master_target_qty_base")
        or "0"
    )
    exchange_quantities = _plan_exchange_quantities(payload, metadata=metadata)
    return {
        "active": True,
        "sleeve_id": sleeve_id,
        "position_id": position.get("position_id"),
        "side": position.get("side"),
        "quantity": str(quantity),
        "exchange_quantities_base": {
            exchange: str(qty)
            for exchange, qty in sorted(exchange_quantities.items())
        },
        "target_exchanges": sorted(exchange_quantities),
        "entry_price": metadata.get("average_entry_price"),
        "issues": [],
    }


def _position(payload: Mapping[str, Any]) -> dict[str, Any]:
    value = payload.get("position", {})
    return dict(value) if isinstance(value, Mapping) else {}


def _only(values: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    return values[0] if len(values) == 1 else None


def _positive_decimal(value: object) -> Decimal | None:
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not decimal.is_finite() or decimal <= 0:
        return None
    return decimal


def _positive_decimal_mapping(value: object) -> dict[str, Decimal]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, Decimal] = {}
    for key, item in value.items():
        exchange = str(key).strip().lower()
        decimal = _positive_decimal(item)
        if exchange and decimal is not None:
            out[exchange] = decimal
    return out


def _plan_exchange_quantities(
    payload: Mapping[str, Any],
    *,
    metadata: Mapping[str, Any],
) -> dict[str, Decimal]:
    quantities: dict[str, Decimal] = {}
    for raw_leg in payload.get("legs", ()):
        if not isinstance(raw_leg, Mapping):
            continue
        exchange = str(raw_leg.get("exchange") or "").strip().lower()
        quantity = _positive_decimal(
            raw_leg.get("filled_qty_base")
            or raw_leg.get("target_qty_base")
        )
        if exchange and quantity is not None:
            quantities[exchange] = quantity
    if quantities:
        return quantities
    return _positive_decimal_mapping(metadata.get("exchange_quantities_base"))


def _integer_or_none(value: object) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _holding_minutes(now_ms: int, entry_open_ms: int) -> int:
    if now_ms <= entry_open_ms:
        return 0
    current_bar_open_ms = (now_ms // _MINUTE_MS) * _MINUTE_MS
    return max(0, (current_bar_open_ms - entry_open_ms) // _MINUTE_MS)


def _dedupe(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(values))


__all__ = [
    "MF_RECOVERY_REQUIRED_METADATA",
    "audit_portfolio_v1_plans",
    "merged_plan_metadata",
    "plan_sleeve_id",
]
