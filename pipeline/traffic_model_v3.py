"""
Traffic model v3: additive upgrade to v2. Deterministic, no LLM, no external APIs.

Adds: review acceleration, keyword footprint, backlink proxy, paid duration stability,
market density multiplier, geo-income multiplier (optional), competitor gap adjustment.
Output shape compatible with compute_traffic_v2; includes traffic_debug_components.
"""

from typing import Dict, Any, List, Optional

from pipeline.traffic_model_v2 import (
    _get_review_count,
    _authority_score,
    _content_depth_score,
    _technical_score,
    _traffic_tier_from_index,
    _efficiency_score_and_label,
    _paid_clicks_estimate_monthly,
    _traffic_confidence_score,
    TRAFFIC_ASSUMPTIONS,
    PAID_CLICKS_ASSUMPTIONS,
)

TRAFFIC_MODEL_VERSION = "v3"


def _review_acceleration_score(context: Dict) -> int:
    """
    Velocity ratio: last_30d / (last_90d/3). Clamp -10 to +15.
    Influences authority moderately (blended in v3 composite).
    """
    vel_30 = context.get("signal_review_velocity_30d")
    vel_90 = context.get("signal_review_velocity_90d")
    try:
        v30 = int(vel_30) if vel_30 is not None else 0
    except (TypeError, ValueError):
        v30 = 0
    try:
        v90 = int(vel_90) if vel_90 is not None else None
    except (TypeError, ValueError):
        v90 = None

    if v90 is not None and v90 > 0:
        # velocity_ratio = last_30d / (last_90d / 3)
        ratio = v30 / (v90 / 3.0)
    elif v30 > 0:
        # No 90d: assume stable, ratio 1.0
        ratio = 1.0
    else:
        ratio = 0.0

    if ratio >= 1.5:
        bonus = 15
    elif ratio >= 1.0:
        bonus = 8
    elif ratio >= 0.5:
        bonus = 0
    else:
        bonus = -10
    return max(-10, min(15, bonus))


def _keyword_footprint_score(context: Dict, objective_layer: Dict) -> int:
    """
    0-100 from high_ticket_procedures_detected, missing_high_value_pages,
    procedure_confidence. Feeds into content-depth weighting.
    """
    obj = objective_layer or {}
    svc = obj.get("service_intelligence") or {}
    high_ticket = svc.get("high_ticket_procedures_detected") or []
    missing = svc.get("missing_high_value_pages") or []
    procedure_confidence = float(svc.get("procedure_confidence") or 0)

    n_high = len(high_ticket)
    if n_high >= 8:
        base = 70
    elif n_high >= 4:
        base = 50
    elif n_high >= 1:
        base = 30
    else:
        base = 10

    penalty = min(30, len(missing) * 5)
    base -= penalty
    if procedure_confidence >= 0.8:
        base += 10
    return max(0, min(100, base))


def _backlink_proxy_score(context: Dict) -> int:
    """
    0-100 from domain, social links, schema, SSL, contact info, optional domain_age_years.
    No paid APIs; proxy only.
    """
    score = 0
    if context.get("signal_domain") or context.get("domain"):
        score += 20
    social = context.get("signal_social_platforms") or context.get("social_platforms") or []
    if isinstance(social, list) and len(social) >= 2:
        score += 10
    elif isinstance(social, list) and len(social) >= 1:
        score += 5
    if context.get("signal_has_schema_microdata") or (context.get("signal_schema_types") or []):
        score += 15
    if context.get("signal_has_ssl"):
        score += 10
    if context.get("signal_has_contact_form") or context.get("signal_has_phone"):
        score += 10
    domain_age = context.get("signal_domain_age_years") or context.get("domain_age_years")
    if domain_age is not None:
        try:
            years = float(domain_age)
            score += min(20, int(years * 2))
        except (TypeError, ValueError):
            pass
    return max(0, min(100, score))


def _paid_stability_score(context: Dict) -> int:
    """
    Replaces flat +10 paid modifier. From signal_ad_duration_days.
    = 90 -> +15, = 30 -> +8, < 30 -> +3, None -> 0.
    """
    if not context.get("signal_runs_paid_ads"):
        return 0
    days = context.get("signal_ad_duration_days")
    if days is None:
        return 0
    try:
        d = int(days)
        if d >= 90:
            return 15
        if d >= 30:
            return 8
        if d > 0:
            return 3
    except (TypeError, ValueError):
        pass
    return 0


def _traffic_estimate_monthly_v3(
    context: Dict,
    objective_layer: Dict,
    index: int,
) -> Dict[str, Any]:
    """
    V3 monthly range: same index-anchored formula as v2, then apply
    market density multiplier, competitor gap (review_positioning), geo-income (optional).
    Does NOT affect traffic_index; only estimated monthly range.
    """
    review_count = _get_review_count(context)
    has_website = context.get("signal_has_website") is True
    comp = (objective_layer or {}).get("competitive_snapshot") or {}
    density = (comp.get("market_density_score") or "Low").lower()
    review_positioning = (comp.get("review_positioning") or "").strip()

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

    # Market density multiplier (traffic estimate only)
    if density == "high":
        upper = int(upper * 0.85)
        lower = int(lower * 0.9)
    # Competitor gap / review_positioning
    if "below" in review_positioning.lower() or review_positioning == "Below sample average":
        upper = int(upper * 0.8)
    elif "above" in review_positioning.lower() or review_positioning == "Above sample average":
        upper = int(upper * 1.1)

    # Geo-income multiplier (optional); normalized 0.7–1.3, clamp no > 1.5x
    zip_income = context.get("signal_zip_income_index")
    if zip_income is not None:
        try:
            factor = float(zip_income)
            factor = max(0.7, min(1.5, factor))
            upper = int(upper * factor)
        except (TypeError, ValueError):
            pass

    return {
        "lower": max(0, lower),
        "upper": max(0, upper),
        "unit": "visits/month",
        "confidence": min(100, max(0, conf)),
    }


def compute_traffic_v3(
    context: Dict[str, Any],
    objective_layer: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Traffic model v3. Same output structure as v2 plus traffic_debug_components.

    Weights: Authority 30%, Content depth + keyword footprint 30%, Technical 15%,
    Backlink proxy 15%, Paid stability 10%. All deterministic; no LLM.
    """
    # Base scores from v2 (reused)
    authority_base = _authority_score(context)
    acceleration_bonus = _review_acceleration_score(context)
    authority = max(0, min(100, authority_base + acceleration_bonus))

    content_depth = _content_depth_score(context, objective_layer or {})
    keyword_footprint = _keyword_footprint_score(context, objective_layer or {})
    content_keyword = (content_depth + keyword_footprint) // 2  # 0-100

    technical = _technical_score(context)
    backlink_proxy = _backlink_proxy_score(context)
    paid_stability = _paid_stability_score(context)

    # Composite: Authority 30%, Content+keyword 30%, Technical 15%, Backlink 15%, Paid stability 10%
    # paid_stability is 0-15; 10% of 100 = 10 points max
    index = (
        authority * 0.30
        + content_keyword * 0.30
        + technical * 0.15
        + backlink_proxy * 0.15
        + min(15, paid_stability) * (10 / 15.0)
    )
    index = max(0, min(100, int(round(index))))

    tier = _traffic_tier_from_index(index)
    eff_score, eff_label = _efficiency_score_and_label(context, objective_layer or {})

    traffic_estimate_monthly = _traffic_estimate_monthly_v3(
        context, objective_layer or {}, index
    )
    paid_clicks = _paid_clicks_estimate_monthly(context)
    traffic_conf = _traffic_confidence_score(context, objective_layer or {}, index)

    # Override with real SEO traffic data when available
    real_organic = context.get("signal_real_organic_traffic")
    traffic_source = "proxy_model"
    if real_organic is not None and real_organic > 0:
        traffic_source = context.get("signal_traffic_source", "seo_api")
        traffic_estimate_monthly = {"lower": int(real_organic * 0.8), "upper": int(real_organic * 1.2)}
        traffic_conf = min(95, traffic_conf + 30)

    real_paid = context.get("signal_real_paid_traffic")
    if real_paid is not None and real_paid > 0:
        paid_clicks = {"lower": int(real_paid * 0.8), "upper": int(real_paid * 1.2)}

    return {
        "traffic_index": index,
        "traffic_estimate_tier": tier,
        "traffic_estimate_monthly": traffic_estimate_monthly,
        "paid_clicks_estimate_monthly": paid_clicks,
        "traffic_confidence_score": traffic_conf,
        "traffic_efficiency_score": eff_score,
        "traffic_efficiency_interpretation": eff_label,
        "traffic_source": traffic_source,
        "traffic_assumptions": TRAFFIC_ASSUMPTIONS,
        "paid_clicks_assumptions": PAID_CLICKS_ASSUMPTIONS if paid_clicks else None,
        "model_version": TRAFFIC_MODEL_VERSION,
        "traffic_debug_components": {
            "authority": authority,
            "acceleration_bonus": acceleration_bonus,
            "keyword_footprint": keyword_footprint,
            "technical": technical,
            "backlink_proxy": backlink_proxy,
            "paid_stability": paid_stability,
        },
    }
