"""
Traffic model v2: composite traffic index (authority + content depth + technical + paid modifier).

Deterministic, versioned. No review-count-only tier. Includes efficiency score and label.
"""

from typing import Dict, Any, List, Optional

TRAFFIC_MODEL_VERSION = "v2"


def _get_review_count(context: Dict) -> int:
    return int(context.get("signal_review_count") or context.get("user_ratings_total") or 0)


def _authority_score(context: Dict) -> int:
    """0-100: review_count tier, review_velocity, rating strength."""
    review_count = _get_review_count(context)
    velocity = context.get("signal_review_velocity_30d")
    if velocity is not None:
        try:
            velocity = int(velocity)
        except (TypeError, ValueError):
            velocity = 0
    else:
        velocity = 0
    rating = context.get("signal_rating") or context.get("rating")
    rating_val = float(rating) if rating is not None else 0.0

    # Review count tier: 0-30 -> 20, 30-80 -> 45, 80-150 -> 65, 150-400 -> 85, 400+ -> 95
    if review_count >= 400:
        count_score = 95
    elif review_count >= 150:
        count_score = 85
    elif review_count >= 80:
        count_score = 65
    elif review_count >= 30:
        count_score = 45
    else:
        count_score = max(10, min(25, review_count))
    # Velocity: 0 -> 0, 1-2 -> 10, 3+ -> 15
    vel_bonus = 15 if velocity >= 3 else (10 if velocity >= 1 else 0)
    # Rating: 4.5+ -> 10, 4.0+ -> 5, else 0
    rating_bonus = 10 if rating_val >= 4.5 else (5 if rating_val >= 4.0 else 0)
    return max(0, min(100, count_score + vel_bonus + rating_bonus))


def _content_depth_score(context: Dict, objective_layer: Dict) -> int:
    """0-100: service pages, high-ticket pages present, missing_high_value_pages (penalty), blog (optional)."""
    obj = objective_layer or {}
    svc = obj.get("service_intelligence") or {}
    high_ticket = svc.get("high_ticket_procedures_detected") or []
    missing = svc.get("missing_high_value_pages") or []
    general = svc.get("general_services_detected") or []
    procedure_confidence = float(svc.get("procedure_confidence") or 0)

    # Base from pages we infer: high_ticket + general as proxy for "content depth"
    n_services = len(high_ticket) + len(general)
    if n_services >= 8:
        base = 70
    elif n_services >= 5:
        base = 55
    elif n_services >= 2:
        base = 40
    else:
        base = 20
    # Procedure confidence (from service_depth) adds up to 20
    base += int(procedure_confidence * 20)
    # Missing pages penalty: each missing -5, min 0
    base -= min(30, len(missing) * 5)
    # Blog: not in signals; optional placeholder. If we had signal_blog_present we'd add 10.
    blog_bonus = 10 if context.get("signal_blog_present") else 0
    return max(0, min(100, base + blog_bonus))


def _technical_score(context: Dict) -> int:
    """0-100: schema, mobile, SSL, page speed, contact form."""
    score = 0
    if context.get("signal_has_schema_microdata") or (context.get("signal_schema_types") or []):
        score += 25
    if context.get("signal_mobile_friendly"):
        score += 20
    if context.get("signal_has_ssl"):
        score += 20
    page_load = context.get("signal_page_load_time_ms")
    if page_load is not None:
        try:
            pl = int(page_load)
            if pl <= 1000:
                score += 20
            elif pl <= 1500:
                score += 15
            elif pl <= 2500:
                score += 10
            else:
                score += 5
        except (TypeError, ValueError):
            score += 10
    else:
        score += 10
    if context.get("signal_has_contact_form"):
        score += 15
    return max(0, min(100, score))


def _paid_modifier(context: Dict) -> int:
    """+10 if ads active, else 0."""
    return 10 if context.get("signal_runs_paid_ads") is True else 0


def _traffic_index(
    authority: int,
    content_depth: int,
    technical: int,
    paid_mod: int,
) -> int:
    """Composite: 0.35 * authority + 0.35 * content + 0.2 * technical + paid_mod, clamped 0-100."""
    raw = authority * 0.35 + content_depth * 0.35 + technical * 0.20 + paid_mod
    return max(0, min(100, int(round(raw, 0))))


def _traffic_tier_from_index(index: int) -> str:
    """0-30 Low, 30-60 Moderate, 60-80 High, 80+ Very High."""
    if index >= 80:
        return "Very High"
    if index >= 60:
        return "High"
    if index >= 30:
        return "Moderate"
    return "Low"


def _efficiency_score_and_label(context: Dict, objective_layer: Dict) -> tuple:
    """Traffic efficiency: 0-100 score and label (Inefficient / Moderate / Optimized / Highly Optimized)."""
    score = 100
    if context.get("signal_runs_paid_ads") is True:
        score -= 20
    booking_path = context.get("signal_booking_conversion_path")
    if booking_path in ("Online booking (limited)", "Online booking (full)"):
        has_online_booking = True
    elif booking_path in ("Phone-only", "Request form"):
        has_online_booking = False
    else:
        booking_flag = context.get("signal_has_automated_scheduling")
        has_online_booking = True if booking_flag is True else (False if booking_flag is False else None)
    if has_online_booking is False:
        score -= 15
    svc = (objective_layer or {}).get("service_intelligence") or {}
    missing = svc.get("missing_high_value_pages") or []
    if missing:
        score -= 10
    if not (context.get("signal_has_schema_microdata") or (context.get("signal_schema_types") or [])):
        score -= 10
    page_load = context.get("signal_page_load_time_ms")
    if page_load is not None:
        try:
            if int(page_load) > 1500:
                score -= 10
        except (TypeError, ValueError):
            pass
    score = max(0, min(100, score))
    if score >= 85:
        label = "Highly Optimized"
    elif score >= 70:
        label = "Optimized"
    elif score >= 40:
        label = "Moderate"
    else:
        label = "Inefficient"
    return (score, label)


TRAFFIC_ASSUMPTIONS = (
    "Traffic is estimated from public proxy signals (reviews, market density, ads presence). Not GA4."
)
PAID_CLICKS_ASSUMPTIONS = (
    "Paid click range is indicative; based on ad presence and typical dental CPC. Not from platform data."
)


def _traffic_estimate_monthly(
    context: Dict,
    objective_layer: Dict,
    index: int,
) -> Dict[str, Any]:
    """Estimate monthly visit range anchored proportionally to traffic_index. Deterministic; no LLM."""
    review_count = _get_review_count(context)
    has_website = context.get("signal_has_website") is True
    comp = (objective_layer or {}).get("competitive_snapshot") or {}
    density = (comp.get("market_density_score") or "Low").lower()

    if not has_website:
        lower, upper = 20, 80
        conf = 25
    elif index >= 80:
        lower = 400 + (review_count * 0.5)
        upper = 900 + (review_count * 1.5)
        conf = 55
    elif index >= 60:
        lower = 150 + (review_count * 0.3)
        upper = 500 + review_count
        conf = 50
    elif index >= 30:
        lower = 50 + (review_count * 0.2)
        upper = 200 + (review_count * 0.5)
        conf = 45
    else:
        lower = 20 + (review_count * 0.1)
        upper = 80 + (review_count * 0.3)
        conf = 40

    lower, upper = int(round(lower)), int(round(upper))
    if density == "high":
        lower = int(lower * 0.8)
        upper = int(upper * 1.2)
    elif density == "low":
        upper = int(upper * 0.9)
    return {
        "lower": max(0, lower),
        "upper": max(0, upper),
        "unit": "visits/month",
        "confidence": min(100, max(0, conf)),
    }


def _paid_clicks_estimate_monthly(context: Dict) -> Optional[Dict[str, Any]]:
    """When ads active, rough paid clicks/month range. Unit: clicks/month."""
    if not context.get("signal_runs_paid_ads"):
        return None
    channels = context.get("signal_paid_ads_channels") or []
    # Indicative: dental CPC ~$5–15; assume $1k–4k/mo => ~100–400 clicks; $3k–10k => ~300–800
    if "meta" in channels and "google" in channels:
        lower, upper = 300, 900
    elif "google" in channels:
        lower, upper = 150, 500
    elif "meta" in channels:
        lower, upper = 100, 400
    else:
        lower, upper = 80, 350
    return {
        "lower": lower,
        "upper": upper,
        "unit": "clicks/month",
        "confidence": 35,
    }


def _traffic_confidence_score(
    context: Dict,
    objective_layer: Dict,
    index: int,
) -> int:
    """0-100 confidence in traffic estimates."""
    score = 40
    if context.get("signal_has_website"):
        score += 20
    review_count = _get_review_count(context)
    if review_count >= 50:
        score += 15
    elif review_count >= 20:
        score += 10
    if context.get("signal_review_velocity_30d") is not None:
        score += 10
    comp = (objective_layer or {}).get("competitive_snapshot") or {}
    if comp.get("dentists_sampled", 0) >= 3:
        score += 10
    return max(0, min(100, score))


def compute_traffic_v2(
    context: Dict[str, Any],
    objective_layer: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Composite traffic model v2.

    Returns:
        traffic_index: 0-100 (internal/drilldown)
        traffic_estimate_tier: "Low" | "Moderate" | "High" | "Very High"
        traffic_estimate_monthly: { lower, upper, unit: "visits/month", confidence }
        paid_clicks_estimate_monthly: { lower, upper, unit: "clicks/month", confidence } or None
        traffic_confidence_score: 0-100
        traffic_efficiency_score, traffic_efficiency_interpretation
        traffic_assumptions, paid_clicks_assumptions (disclaimer strings)
        model_version: "v2"
    """
    authority = _authority_score(context)
    content_depth = _content_depth_score(context, objective_layer or {})
    technical = _technical_score(context)
    paid_mod = _paid_modifier(context)
    index = _traffic_index(authority, content_depth, technical, paid_mod)
    tier = _traffic_tier_from_index(index)
    eff_score, eff_label = _efficiency_score_and_label(context, objective_layer or {})

    traffic_estimate_monthly = _traffic_estimate_monthly(context, objective_layer or {}, index)
    paid_clicks = _paid_clicks_estimate_monthly(context)
    traffic_confidence_score = _traffic_confidence_score(context, objective_layer or {}, index)

    return {
        "traffic_index": index,
        "traffic_estimate_tier": tier,
        "traffic_estimate_monthly": traffic_estimate_monthly,
        "paid_clicks_estimate_monthly": paid_clicks,
        "traffic_confidence_score": traffic_confidence_score,
        "traffic_efficiency_score": eff_score,
        "traffic_efficiency_interpretation": eff_label,
        "traffic_assumptions": TRAFFIC_ASSUMPTIONS,
        "paid_clicks_assumptions": PAID_CLICKS_ASSUMPTIONS if paid_clicks else None,
        "model_version": TRAFFIC_MODEL_VERSION,
    }
