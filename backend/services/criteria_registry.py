"""Strict criteria registry + validation helpers for Ask intent parsing/execution."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

CRITERIA_REGISTRY = frozenset({
    "below_review_avg",
    "low_review_count",
    "high_review_gap",
    "runs_paid_ads",
    "no_paid_ads",
    "has_website",
    "no_website",
    "slow_website",
    "no_ssl",
    "mobile_unfriendly",
    "no_contact_form",
    "no_booking",
    "has_booking",
    "missing_service_page",
    "weak_service_depth",
    "no_trust_badges",
    "no_financing_option",
    "high_competition_density",
    "low_competition_density",
    "primary_constraint_visibility",
    "primary_constraint_conversion",
    "primary_constraint_authority",
    "primary_constraint_service_depth",
    "high_modeled_revenue_upside",
    "low_modeled_revenue_upside",
})

SERVICE_SLUGS = frozenset({
    "implants",
    "invisalign",
    "orthodontics",
    "veneers",
    "whitening",
    "botox",
    "fillers",
    "iv_therapy",
    "acupuncture",
    "emergency",
    "pediatric",
    "root_canal",
    "crowns",
})

ALLOWED_VERTICALS = frozenset({
    "dentist",
    "orthodontist",
    "med_spa",
    "chiropractor",
    "hvac",
    "plumber",
    "roofer",
    "law_firm",
    "restaurant",
    "gym",
    "general_local_business",
})

SEO_SEMANTIC_MAP = {
    "losing money": ["below_review_avg", "high_competition_density"],
    "poor visibility": ["below_review_avg"],
    "weak seo": ["below_review_avg", "weak_service_depth"],
    "technical seo issues": ["slow_website", "no_ssl", "mobile_unfriendly"],
    "conversion issues": ["no_booking", "no_contact_form"],
    "high ticket gap": ["missing_service_page"],
    "wasting ad spend": ["runs_paid_ads", "below_review_avg"],
    "authority gap": ["high_review_gap"],
    "vulnerable to competitors": ["high_competition_density", "below_review_avg"],
}


def _to_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_vertical(value: Any) -> str | None:
    raw = _to_text(value).lower().replace("-", "_").replace(" ", "_")
    if not raw:
        return None
    if raw in ALLOWED_VERTICALS:
        return raw
    alias = {
        "dental": "dentist",
        "dentists": "dentist",
        "orthodontists": "orthodontist",
        "medspa": "med_spa",
        "med_spas": "med_spa",
        "law": "law_firm",
        "attorney": "law_firm",
        "attorneys": "law_firm",
        "local_business": "general_local_business",
    }
    mapped = alias.get(raw)
    if mapped in ALLOWED_VERTICALS:
        return mapped
    return None


def normalize_accuracy_mode(value: Any) -> str:
    mode = _to_text(value).lower()
    return "verified" if mode == "verified" else "fast"


def normalize_limit(value: Any, default: int = 10) -> int:
    try:
        out = int(value)
    except (TypeError, ValueError):
        out = default
    return max(1, min(out, 20))


def sanitize_criteria(
    criteria: Iterable[Dict[str, Any]] | None,
    unsupported_parts: List[str] | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    unsupported = list(unsupported_parts or [])
    cleaned: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for item in list(criteria or []):
        if not isinstance(item, dict):
            unsupported.append(f"invalid criterion payload: {item}")
            continue
        ctype = _to_text(item.get("type"))
        service = _to_text(item.get("service")).lower() or ""

        if ctype not in CRITERIA_REGISTRY:
            if ctype:
                unsupported.append(f"unsupported criterion: {ctype}")
            continue

        if ctype == "missing_service_page":
            if not service:
                unsupported.append("missing_service_page requires service slug")
                continue
            if service not in SERVICE_SLUGS:
                unsupported.append(f"unsupported service slug: {service}")
                continue

        key = (ctype, service)
        if key in seen:
            continue
        seen.add(key)
        out = {"type": ctype, "service": service or None}
        cleaned.append(out)

    return cleaned, unsupported


def sanitize_must_not(
    must_not: Iterable[Dict[str, Any]] | None,
    unsupported_parts: List[str] | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    return sanitize_criteria(must_not, unsupported_parts)


def semantic_criteria_for_query(normalized_query: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for phrase, criteria in SEO_SEMANTIC_MAP.items():
        if phrase in normalized_query:
            for c in criteria:
                if c in CRITERIA_REGISTRY:
                    out.append({"type": c, "service": None})
    return out


def intent_confidence(city: Any, state: Any, vertical: Any, criteria: List[Dict[str, Any]]) -> str:
    has_city = bool(_to_text(city))
    has_state = bool(_to_text(state))
    has_vertical = bool(_to_text(vertical))
    has_criteria = len(criteria) > 0
    if has_city and has_state and has_vertical and has_criteria:
        return "high"
    if has_city and has_state and (has_vertical or has_criteria):
        return "medium"
    return "low"
