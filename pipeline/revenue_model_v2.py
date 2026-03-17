"""
Revenue model v2: tier-based annual revenue bands, capped organic gap, confidence score.

Deterministic, versioned, calibratable. No LLM. No linear review_count * multiplier.
"""

import re
from typing import Dict, Any, List, Optional, Tuple

REVENUE_MODEL_VERSION = "v2"

# Floor for operating dentist (annual USD)
REVENUE_FLOOR_LOWER = 300_000
REVENUE_FLOOR_UPPER = 400_000


def _get_review_count(context: Dict) -> int:
    return int(context.get("signal_review_count") or context.get("user_ratings_total") or 0)


def _base_revenue_band(review_count: int) -> Tuple[int, int]:
    """Tier-based annual revenue band (lower, upper) in USD.

    More granular tiers based on dental industry benchmarks:
    Reviews correlate with years in practice, patient volume, and provider count.
    """
    if review_count <= 10:
        return (300_000, 600_000)
    if review_count <= 30:
        return (500_000, 1_000_000)
    if review_count <= 75:
        return (800_000, 1_500_000)
    if review_count <= 150:
        return (1_200_000, 2_100_000)
    if review_count <= 300:
        return (1_800_000, 3_000_000)
    if review_count <= 500:
        return (2_500_000, 4_000_000)
    return (3_000_000, 5_000_000)


def _normalize_label(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _in_scope(label: Any, expected_services: List[str]) -> bool:
    target = _normalize_label(label)
    if not target:
        return False
    for svc in expected_services or []:
        s = _normalize_label(svc)
        if not s:
            continue
        if target == s or target in s or s in target:
            return True
    return False


def _high_ticket_emphasized(high_ticket_procedures: List[Any], practice_type: str = "general_dentist", expected_services: Optional[List[str]] = None) -> bool:
    """True if implants, orthodontics, or veneers explicitly present (high-ticket emphasis)."""
    if not high_ticket_procedures:
        return False
    expected_services = expected_services or []
    non_general = str(practice_type or "").strip().lower() not in {"", "general_dentist"}
    for p in high_ticket_procedures:
        s = (p.get("procedure") if isinstance(p, dict) else p) or ""
        if non_general and expected_services and not _in_scope(s, expected_services):
            continue
        s = str(s).lower()
        if any(kw in s for kw in ("implant", "invisalign", "orthodontic", "veneer", "cosmetic")):
            return True
    return False


def _apply_revenue_adjustments(
    lower: int,
    upper: int,
    high_ticket_emphasized: bool,
    multiple_locations: bool,
    staff_count: Optional[int],
    high_income_metro: bool = False,
    ads_active: bool = False,
    rating: Optional[float] = None,
    review_count: int = 0,
) -> Tuple[int, int]:
    """Apply proportional adjustments. Each modifier adds a percentage to the band."""
    mult = 1.0
    if high_ticket_emphasized:
        mult += 0.15
    if multiple_locations:
        mult += 0.20
    if staff_count is not None and staff_count >= 5:
        mult += 0.15
    elif staff_count is not None and staff_count >= 3:
        mult += 0.10
    if high_income_metro:
        mult += 0.10
    if ads_active:
        mult += 0.10
    if rating is not None and rating >= 4.5 and review_count >= 50:
        mult += 0.05
    return (int(round(lower * mult, 0)), int(round(upper * mult, 0)))


def _organic_gap_percentage(
    missing_high_value_pages: bool,
    ads_active: bool,
) -> Tuple[float, float]:
    """Return (gap_pct_low, gap_pct_high) for organic revenue gap."""
    if missing_high_value_pages and ads_active:
        return (0.15, 0.20)
    if missing_high_value_pages:
        return (0.08, 0.12)
    return (0.03, 0.07)


def _cap_organic_gap(
    gap_lower: float,
    gap_upper: float,
    revenue_band_upper: int,
    max_gap_pct: float = 0.30,
) -> Tuple[int, int]:
    """Hard cap: organic_gap_upper must not exceed max_gap_pct of revenue_band_upper."""
    cap = int(revenue_band_upper * max_gap_pct)
    gap_upper_capped = min(int(round(gap_upper, 0)), cap)
    gap_lower_capped = min(int(round(gap_lower, 0)), cap)
    if gap_lower_capped > gap_upper_capped:
        gap_lower_capped = gap_upper_capped
    return (gap_lower_capped, gap_upper_capped)


def _revenue_confidence_score(
    context: Dict,
    dentist_profile: Dict,
    objective_layer: Dict,
    high_ticket_emphasized: bool,
    multiple_locations: bool,
    staff_count: Optional[int],
    pricing_page_detected: bool,
) -> int:
    """0-100. Higher if staff_count, multi_location, pricing page, high-ticket. Lower if no website, very low reviews, no service clarity."""
    score = 50
    if staff_count is not None:
        score += 10
    if multiple_locations:
        score += 10
    if pricing_page_detected:
        score += 10
    if high_ticket_emphasized:
        score += 10
    if not context.get("signal_has_website"):
        score -= 25
    review_count = _get_review_count(context)
    if review_count < 15:
        score -= 15
    elif review_count < 30:
        score -= 5
    svc = (objective_layer or {}).get("service_intelligence") or {}
    high_ticket = svc.get("high_ticket_procedures_detected") or []
    general = svc.get("general_services_detected") or []
    if not high_ticket and not general:
        score -= 15
    return max(0, min(100, score))


def compute_revenue_v2(
    context: Dict[str, Any],
    dentist_profile: Dict[str, Any],
    objective_layer: Dict[str, Any],
    high_income_metro: bool = False,
    pricing_page_detected: bool = False,
) -> Dict[str, Any]:
    """
    Tier-based revenue model v2.

    Returns:
        revenue_band_estimate: { lower, upper, currency, period }
        organic_revenue_gap_estimate: { lower, upper, ... } or None
        revenue_confidence_score: 0-100
        model_version: "v2"
    """
    obj = objective_layer or {}
    svc = obj.get("service_intelligence") or {}
    if str(svc.get("crawl_confidence") or "").strip().lower() == "low" or bool(svc.get("suppress_revenue_modeling") or svc.get("suppress_service_gap")):
        return {
            "revenue_band_estimate": None,
            "organic_revenue_gap_estimate": None,
            "revenue_confidence_score": 0,
            "indicative_only": True,
            "revenue_reliability_grade": "C",
            "model_version": REVENUE_MODEL_VERSION,
            "suppressed_due_to_low_crawl_confidence": True,
        }

    high_ticket = svc.get("high_ticket_procedures_detected") or []
    practice_type = str(svc.get("practice_type") or "general_dentist")
    expected_services = [str(x) for x in (svc.get("expected_services") or []) if str(x).strip()]
    missing_pages = svc.get("missing_high_value_pages") or []
    missing_high_value = bool(missing_pages)
    ads_active = context.get("signal_runs_paid_ads") is True

    review_count = _get_review_count(context)
    low, upp = _base_revenue_band(review_count)

    staff_count = context.get("staff_count")
    if staff_count is None and dentist_profile:
        ops = (dentist_profile.get("operations") or {}).get("staff_count_estimate") or {}
        if isinstance(ops.get("value"), (int, float)):
            staff_count = int(ops["value"])
    if staff_count is not None:
        try:
            staff_count = int(staff_count)
        except (TypeError, ValueError):
            staff_count = None

    multiple_locations = context.get("multiple_location_flag") is True
    if not multiple_locations and dentist_profile:
        multiple_locations = (dentist_profile.get("operations") or {}).get("multiple_locations") is True

    high_ticket_emph = _high_ticket_emphasized(
        high_ticket,
        practice_type=practice_type,
        expected_services=expected_services,
    )
    rating = context.get("signal_rating")
    if rating is not None:
        try:
            rating = float(rating)
        except (TypeError, ValueError):
            rating = None
    low, upp = _apply_revenue_adjustments(
        low, upp, high_ticket_emph, multiple_locations, staff_count, high_income_metro,
        ads_active=ads_active, rating=rating, review_count=review_count,
    )
    # Floor: no unrealistic revenue under floor for operating dentist
    low = max(low, REVENUE_FLOOR_LOWER)
    upp = max(upp, REVENUE_FLOOR_UPPER)
    if low > upp:
        upp = low

    revenue_band_estimate = {
        "lower": low,
        "upper": upp,
        "currency": "USD",
        "period": "annual",
    }

    organic_revenue_gap_estimate = None
    gap_pct_lo, gap_pct_hi = _organic_gap_percentage(missing_high_value, ads_active)
    gap_lower = low * gap_pct_lo
    gap_upper = upp * gap_pct_hi
    gap_lower, gap_upper = _cap_organic_gap(gap_lower, gap_upper, upp, max_gap_pct=0.30)
    if gap_lower > 0 or gap_upper > 0:
        organic_revenue_gap_estimate = {
            "lower": gap_lower,
            "upper": gap_upper,
            "currency": "USD",
            "period": "annual",
            "driver": "missing_high_value_pages" if missing_high_value else "baseline",
        }

    revenue_confidence_score = _revenue_confidence_score(
        context,
        dentist_profile,
        objective_layer,
        high_ticket_emph,
        multiple_locations,
        staff_count,
        pricing_page_detected,
    )

    has_website = context.get("signal_has_website") is True
    has_services = bool(high_ticket or svc.get("general_services_detected"))
    indicative_only = (
        not has_website
        or review_count < 15
        or not has_services
        or revenue_confidence_score < 40
    )

    # revenue_reliability_grade: A = GA4/direct (we have none), B = strong proxy + competitor context, C = weak proxy only
    competitive_snapshot = (objective_layer or {}).get("competitive_snapshot") or {}
    has_competitor_context = (competitive_snapshot.get("dentists_sampled") or 0) >= 3
    if has_website and review_count >= 30 and has_services and has_competitor_context:
        revenue_reliability_grade = "B"
    elif has_website and (review_count >= 15 or has_services):
        revenue_reliability_grade = "B"
    else:
        revenue_reliability_grade = "C"

    return {
        "revenue_band_estimate": revenue_band_estimate,
        "organic_revenue_gap_estimate": organic_revenue_gap_estimate,
        "revenue_confidence_score": revenue_confidence_score,
        "indicative_only": indicative_only,
        "revenue_reliability_grade": revenue_reliability_grade,
        "model_version": REVENUE_MODEL_VERSION,
    }
