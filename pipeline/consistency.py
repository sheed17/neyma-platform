"""Canonical normalization helpers to keep diagnostic outputs internally aligned."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


SERVICE_STATUSES = {"dedicated", "mention_only", "missing", "unknown"}


def _canonical_service_status(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    if s in SERVICE_STATUSES:
        return s
    if s in {"dedicated_page", "strong"}:
        return "dedicated"
    if s in {"strong_umbrella", "weak_presence", "weak", "weak_stub_page", "umbrella_only", "moderate"}:
        return "mention_only"
    if s in {"not_evaluated"}:
        return "unknown"
    return "unknown"


def normalize_service_intelligence(service_intel: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(service_intel, dict):
        return {}
    out = dict(service_intel)
    rows_raw = out.get("high_value_services")
    if not isinstance(rows_raw, list) or not rows_raw:
        return out

    rows: List[Dict[str, Any]] = []
    for raw in rows_raw:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        status = _canonical_service_status(
            row.get("service_status")
            or row.get("qualification_status")
            or row.get("optimization_tier")
            or ("dedicated" if row.get("page_exists") else "missing")
        )
        row["service_status"] = status
        row["qualification_status"] = status
        row["optimization_tier"] = status
        if not row.get("display_name") and row.get("service"):
            row["display_name"] = str(row.get("service")).replace("_", " ").title()
        rows.append(row)

    if not rows:
        return out

    out["high_value_services"] = rows
    missing = [
        str(r.get("display_name") or r.get("service") or "")
        for r in rows
        if str(r.get("service_status") or "").lower() == "missing"
    ]
    missing = [m for m in missing if m]
    out["missing_high_value_pages"] = missing

    total = len(rows)
    dedicated_rows = [r for r in rows if str(r.get("service_status") or "").lower() == "dedicated"]
    mention_rows = [r for r in rows if str(r.get("service_status") or "").lower() == "mention_only"]
    missing_rows = [r for r in rows if str(r.get("service_status") or "").lower() == "missing"]
    wc_present = [
        int(r.get("word_count") or 0)
        for r in (dedicated_rows + mention_rows)
        if int(r.get("word_count") or 0) > 0
    ]
    summary = dict(out.get("high_value_summary") or {})
    coverage = round(len(dedicated_rows) / total, 3) if total else 0.0
    summary.update(
        {
            "total_high_value_services": int(total),
            "services_dedicated": int(len(dedicated_rows)),
            "services_mention_only": int(len(mention_rows)),
            "services_missing": int(len(missing_rows)),
            "services_present": int(len(dedicated_rows)),
            "services_strong": int(len(dedicated_rows)),
            "services_moderate": int(len(mention_rows)),
            "services_weak": int(len(missing_rows)),
            "coverage_score": coverage,
            "service_coverage_ratio": coverage,
            "optimized_ratio": coverage,
            "average_word_count": int(sum(wc_present) / len(wc_present)) if wc_present else 0,
        }
    )
    out["high_value_summary"] = summary
    out["coverage_score"] = coverage
    return out


def _capture_value(capture_verification: Any, key: str) -> Any:
    if not isinstance(capture_verification, dict):
        return None
    node = capture_verification.get(key)
    if isinstance(node, dict):
        return node.get("value")
    return None


def _capture_confidence(capture_verification: Any, key: str) -> Optional[str]:
    if not isinstance(capture_verification, dict):
        return None
    node = capture_verification.get(key)
    if isinstance(node, dict):
        val = str(node.get("confidence") or "").strip().lower()
        return val or None
    return None


def normalize_conversion_infrastructure(
    conversion: Optional[Dict[str, Any]],
    *,
    service_intel: Optional[Dict[str, Any]] = None,
    signals: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = dict(conversion or {})
    signals = signals or {}
    capture = out.get("capture_verification")
    if capture is None and isinstance(signals, dict):
        capture = signals.get("signal_capture_verification")
    if capture is not None:
        out["capture_verification"] = capture

    booking_flow = (
        _capture_value(capture, "booking_flow")
        or out.get("booking_flow_type")
        or (signals.get("signal_booking_flow_type") if isinstance(signals, dict) else None)
    )
    if booking_flow:
        out["booking_flow_type"] = booking_flow

    if booking_flow == "online_self_scheduling":
        out["online_booking"] = True
    elif booking_flow in {"appointment_request_form", "call_only"}:
        out["online_booking"] = False
    elif out.get("online_booking") not in {True, False}:
        out["online_booking"] = None

    contact_value = _capture_value(capture, "contact_form")
    if contact_value in {True, False, "unknown"}:
        if contact_value == "unknown":
            contact_value = None
        out["contact_form"] = contact_value

    if isinstance(service_intel, dict) and service_intel.get("contact_form_detected_sitewide"):
        out["contact_form"] = True

    if out.get("contact_form") not in {True, False}:
        fallback = out.get("contact_form")
        if fallback not in {True, False}:
            sig_val = signals.get("signal_has_contact_form") if isinstance(signals, dict) else None
            out["contact_form"] = sig_val if sig_val in {True, False} else None

    booking_conf = _capture_confidence(capture, "booking_flow")
    if booking_conf:
        out["booking_flow_confidence"] = booking_conf
    contact_conf = _capture_confidence(capture, "contact_form")
    if contact_conf:
        out["contact_form_confidence"] = contact_conf

    if out.get("scheduling_cta_detected") is None:
        if booking_flow in {"online_self_scheduling", "appointment_request_form", "call_only"}:
            out["scheduling_cta_detected"] = True
        elif isinstance(signals, dict):
            out["scheduling_cta_detected"] = signals.get("signal_scheduling_cta_detected")

    if out.get("contact_form_cta_detected") is None and isinstance(signals, dict):
        out["contact_form_cta_detected"] = signals.get("signal_contact_form_cta_detected")

    return out


def normalize_diagnostic_payload(resp: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(resp, dict):
        return {}
    out = dict(resp)
    signals = dict(out.get("signals") or {}) if isinstance(out.get("signals"), dict) else {}
    service_intel = normalize_service_intelligence(out.get("service_intelligence") or {})
    if service_intel:
        out["service_intelligence"] = service_intel

    conversion = normalize_conversion_infrastructure(
        out.get("conversion_infrastructure") or {},
        service_intel=service_intel,
        signals=signals,
    )
    if conversion:
        out["conversion_infrastructure"] = conversion

    if signals:
        if conversion.get("online_booking") in {True, False}:
            signals["signal_has_automated_scheduling"] = conversion.get("online_booking")
        if conversion.get("contact_form") in {True, False}:
            signals["signal_has_contact_form"] = conversion.get("contact_form")
        if conversion.get("booking_flow_type") is not None:
            signals["signal_booking_flow_type"] = conversion.get("booking_flow_type")
        out["signals"] = signals

    brief = dict(out.get("brief") or {}) if isinstance(out.get("brief"), dict) else {}
    if brief:
        if service_intel:
            ht = dict(brief.get("high_ticket_gaps") or {})
            detected = service_intel.get("high_ticket_procedures_detected")
            if isinstance(detected, list) and detected:
                ht["high_ticket_services_detected"] = list(detected)
            if not service_intel.get("suppress_service_gap"):
                ht["missing_landing_pages"] = list(service_intel.get("missing_high_value_pages") or [])
            else:
                ht.pop("missing_landing_pages", None)
            brief["high_ticket_gaps"] = ht

            spa = dict(brief.get("service_page_analysis") or {})
            spa["services"] = list(service_intel.get("high_value_services") or [])
            spa["summary"] = dict(service_intel.get("high_value_summary") or {})
            if service_intel.get("high_value_service_leverage") is not None:
                spa["leverage"] = service_intel.get("high_value_service_leverage")
            if service_intel.get("service_page_analysis_v2") is not None:
                spa["v2"] = dict(service_intel.get("service_page_analysis_v2") or {})
            brief["service_page_analysis"] = spa

        brief["conversion_infrastructure"] = normalize_conversion_infrastructure(
            brief.get("conversion_infrastructure") or {},
            service_intel=service_intel,
            signals=signals,
        )
        out["brief"] = brief

    return out
