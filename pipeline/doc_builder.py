"""
Typed document builders for hybrid RAG retrieval.

Builds compact, deterministic text chunks from existing lead facts.
No external calls, no side effects.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


DOC_TYPES = {
    "signal_profile",
    "service_coverage",
    "market_context",
    "conversion_path",
    "llm_brief_summary",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bucket_review_gap(value: Optional[float]) -> str:
    if value is None:
        return "unknown"
    if value < 50:
        return "0-50"
    if value < 150:
        return "50-150"
    if value < 300:
        return "150-300"
    return "300+"


def _first_non_null(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def _get_signal(lead: Dict[str, Any], signals: Dict[str, Any], key: str) -> Any:
    return _first_non_null(
        signals.get(key),
        lead.get(key),
        lead.get(f"signal_{key}"),
        (lead.get("signals") or {}).get(key) if isinstance(lead.get("signals"), dict) else None,
        (lead.get("signals") or {}).get(f"signal_{key}") if isinstance(lead.get("signals"), dict) else None,
    )


def _base_metadata(lead: Dict[str, Any], signals: Dict[str, Any], diagnostics: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = lead.get("competitive_snapshot") or diagnostics.get("competitive_snapshot") or {}
    review_count = _first_non_null(
        _get_signal(lead, signals, "review_count"),
        lead.get("user_ratings_total"),
    )
    avg_reviews = _first_non_null(snapshot.get("avg_review_count"), diagnostics.get("avg_review_count"))
    review_gap = None
    try:
        if avg_reviews is not None and review_count is not None:
            review_gap = float(avg_reviews) - float(review_count)
    except (TypeError, ValueError):
        review_gap = None

    market_density = _first_non_null(
        snapshot.get("market_density"),
        snapshot.get("market_density_score"),
        diagnostics.get("market_density"),
        ((lead.get("objective_intelligence") or {}).get("competitive_profile") or {}).get("market_density"),
    )

    meta = {
        "vertical": (diagnostics.get("vertical") or "dentist"),
        "city": lead.get("city") or diagnostics.get("city"),
        "state": lead.get("state") or diagnostics.get("state"),
        "niche": diagnostics.get("niche") or (lead.get("service_category") or "general_dentistry"),
        "review_gap": round(review_gap, 1) if isinstance(review_gap, float) else review_gap,
        "review_gap_bucket": _bucket_review_gap(review_gap),
        "market_density": market_density,
        "has_booking": _get_signal(lead, signals, "has_automated_scheduling"),
        "runs_paid_ads": _get_signal(lead, signals, "runs_paid_ads"),
        "revenue_band": ((lead.get("revenue_intelligence") or {}).get("revenue_band_estimate") or diagnostics.get("revenue_band")),
        "created_at": _now_iso(),
    }
    return meta


def _compact_text(text: str, min_len: int = 200, max_len: int = 800) -> str:
    out = " ".join((text or "").split()).strip()
    if not out:
        return ""
    if len(out) < min_len:
        out = (out + " " + out)[:min_len].strip()
    return out[:max_len]


def build_llm_brief_summary_doc(
    lead: Dict[str, Any],
    signals: Dict[str, Any],
    diagnostics: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build llm_brief_summary doc when LLM output exists."""
    llm = diagnostics.get("llm_reasoning_layer") or lead.get("llm_reasoning_layer") or {}
    if not isinstance(llm, dict):
        return None

    primary_constraint = llm.get("primary_constraint") or diagnostics.get("constraint")
    primary_leverage = llm.get("primary_leverage") or diagnostics.get("primary_leverage")
    angle = llm.get("outreach_angle") or llm.get("recommended_outreach_angle")
    risks = llm.get("risks") or llm.get("risk_objections") or []
    if not (primary_constraint or primary_leverage or angle):
        return None

    risks_line = "; ".join(str(r) for r in risks[:3]) if isinstance(risks, list) else ""
    text = (
        f"Brief summary: primary constraint is {primary_constraint or 'unknown'}. "
        f"Primary leverage is {primary_leverage or 'unknown'}. "
        f"Recommended outreach angle: {angle or 'n/a'}. "
        f"Confidence {llm.get('confidence') if llm.get('confidence') is not None else 'n/a'}. "
        f"Key risks: {risks_line or 'none specified'}."
    )
    content = _compact_text(text)
    if not content:
        return None

    return {
        "doc_type": "llm_brief_summary",
        "content_text": content,
        "metadata_json": _base_metadata(lead, signals, diagnostics),
    }


def build_typed_docs_for_lead(
    lead: Dict[str, Any],
    signals: Dict[str, Any],
    diagnostics: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Build deterministic typed docs for hybrid retrieval.

    Includes the four deterministic docs always; includes llm_brief_summary
    only when LLM summary/intel exists in diagnostics/lead.
    """
    meta = _base_metadata(lead, signals, diagnostics)

    review_count = _first_non_null(_get_signal(lead, signals, "review_count"), lead.get("user_ratings_total"))
    rating = _first_non_null(_get_signal(lead, signals, "rating"), lead.get("rating"))
    has_ssl = _get_signal(lead, signals, "has_ssl")
    load_ms = _get_signal(lead, signals, "page_load_time_ms")
    has_form = _get_signal(lead, signals, "has_contact_form")
    has_email = _get_signal(lead, signals, "has_email")
    booking = _get_signal(lead, signals, "has_automated_scheduling")
    paid = _get_signal(lead, signals, "runs_paid_ads")
    hiring = _get_signal(lead, signals, "hiring_active")
    extraction_method = _get_signal(lead, signals, "extraction_method")

    service_intel = lead.get("service_intelligence") or diagnostics.get("service_intelligence") or {}
    missing_services = service_intel.get("missing_high_value_pages") or service_intel.get("missing_services") or []
    detected_services = service_intel.get("high_ticket_services_detected") or service_intel.get("detected_services") or []

    snapshot = lead.get("competitive_snapshot") or diagnostics.get("competitive_snapshot") or {}
    market_density = meta.get("market_density")
    avg_reviews = snapshot.get("avg_review_count")
    review_position = snapshot.get("review_positioning") or snapshot.get("review_positioning_tier")
    competitor_count = snapshot.get("competitor_count") or snapshot.get("sample_size")

    booking_path = _get_signal(lead, signals, "booking_conversion_path")
    phone_clickable = _get_signal(lead, signals, "phone_clickable")
    cta_count = _get_signal(lead, signals, "cta_count")

    docs: List[Dict[str, Any]] = []

    signal_profile = _compact_text(
        (
            f"Signal profile for {lead.get('name') or 'business'} in {meta.get('city') or 'unknown city'}: "
            f"rating {rating}, reviews {review_count}, website SSL {has_ssl}, load time {load_ms}ms, "
            f"contact form {has_form}, email visible {has_email}, booking automation {booking}, "
            f"paid ads {paid}, hiring active {hiring}, extraction method {extraction_method}."
        )
    )
    docs.append({"doc_type": "signal_profile", "content_text": signal_profile, "metadata_json": dict(meta)})

    service_cov = _compact_text(
        (
            f"Service coverage snapshot: detected services {detected_services[:8] if isinstance(detected_services, list) else detected_services}; "
            f"missing high-value services {missing_services[:8] if isinstance(missing_services, list) else missing_services}. "
        )
    )
    docs.append({"doc_type": "service_coverage", "content_text": service_cov, "metadata_json": dict(meta)})

    market_context = _compact_text(
        (
            f"Market context: density {market_density}, competitor sample {competitor_count}, "
            f"market average reviews {avg_reviews}, review position {review_position}, "
            f"review gap bucket {meta.get('review_gap_bucket')} and raw gap {meta.get('review_gap')}."
        )
    )
    docs.append({"doc_type": "market_context", "content_text": market_context, "metadata_json": dict(meta)})

    conversion_path = _compact_text(
        (
            f"Conversion path: booking path {booking_path}, automated booking {booking}, "
            f"contact form {has_form}, phone clickable {phone_clickable}, CTA count {cta_count}, "
            f"paid ads {paid}. Friction is elevated when booking is absent and form coverage is weak."
        )
    )
    docs.append({"doc_type": "conversion_path", "content_text": conversion_path, "metadata_json": dict(meta)})

    llm_doc = build_llm_brief_summary_doc(lead, signals, diagnostics)
    if llm_doc:
        docs.append(llm_doc)

    return [d for d in docs if d.get("doc_type") in DOC_TYPES and d.get("content_text")]
