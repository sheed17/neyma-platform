"""
Context-First Opportunity Intelligence — Deterministic Interpreter.

Consumes extracted signals and emits context dimensions (status + evidence + confidence)
plus reasoning summary and optional synthesis. No LLM in this module.
"""

from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

# Six dimensions (agreed architecture)
DIMENSIONS = [
    "Paid Growth",
    "Hiring & Timing",
    "Reviews & Reputation",
    "Website & Funnel",
    "Operational Maturity",
    "Reachability",
]

# Weights for overall confidence (known = true or false)
SIGNAL_WEIGHTS = {
    "has_website": 1.0,
    "website_accessible": 1.0,
    "has_phone": 1.0,
    "has_contact_form": 1.5,
    "has_email": 1.0,
    "has_automated_scheduling": 1.5,
    "review_count": 1.0,
    "last_review_days_ago": 1.0,
    "runs_paid_ads": 1.0,
    "hiring_active": 0.5,
}

# Optional: per-dimension weights for priority (override via env or config later)
# Default: all dimensions equal weight for composite score
DIMENSION_PRIORITY_WEIGHTS = {
    "Paid Growth": 1.2,
    "Hiring & Timing": 1.0,
    "Reviews & Reputation": 1.0,
    "Website & Funnel": 1.2,
    "Operational Maturity": 1.0,
    "Reachability": 0.8,
}

REVIEW_FRESH_DAYS = 30
REVIEW_WARM_DAYS = 90
REVIEW_STALE_DAYS = 180
LOW_REVIEW_COUNT = 30


def _normalize_signals(lead_or_signals: Dict) -> Dict:
    """Accept lead dict (signal_* keys) or plain signals; return plain key signals."""
    out = {}
    for key, value in lead_or_signals.items():
        if key.startswith("signal_"):
            out[key[7:]] = value
        else:
            out[key] = value
    return out


def _is_true(v: Any) -> bool:
    return v is True


def _is_false(v: Any) -> bool:
    return v is False


def _is_known(v: Any) -> bool:
    return v is not None


def _get_signal(signals: Dict, key: str) -> Any:
    return signals.get(key)


# -----------------------------------------------------------------------------
# Per-dimension builders (status: Strong | Moderate | Weak | Unknown)
# -----------------------------------------------------------------------------

def _dimension_paid_growth(signals: Dict) -> Dict:
    """Paid Growth: runs ads, conversion gaps, ROI opportunity."""
    runs_ads = _get_signal(signals, "runs_paid_ads")
    if not _is_true(runs_ads):
        if _is_known(runs_ads):
            return {"status": "Weak", "evidence": ["No paid ads detected"], "confidence": 0.7}
        return {"status": "Unknown", "evidence": [], "confidence": 0.0}

    evidence = []
    channels = _get_signal(signals, "paid_ads_channels") or []
    channel_str = ", ".join(channels) if channels else "detected"
    evidence.append(f"Running paid ads ({channel_str})")
    gap_count = 0

    if _is_false(_get_signal(signals, "has_contact_form")):
        evidence.append("No contact form to capture ad traffic")
        gap_count += 1
    if _is_false(_get_signal(signals, "has_automated_scheduling")):
        evidence.append("No automated scheduling to convert visitors")
        gap_count += 1
    if _is_false(_get_signal(signals, "mobile_friendly")):
        evidence.append("Website not mobile-friendly")
        gap_count += 1
    if not _is_true(_get_signal(signals, "website_accessible")):
        evidence.append("Website accessibility issues")
        gap_count += 2

    if gap_count == 0:
        evidence.append("Ad spend active — review ROI opportunity")
        return {"status": "Moderate", "evidence": evidence, "confidence": 0.7}
    status = "Strong" if gap_count >= 2 else "Moderate"
    return {"status": status, "evidence": evidence, "confidence": 0.8}


def _dimension_hiring_timing(signals: Dict) -> Dict:
    """Hiring & Timing: hiring activity, scaling pressure."""
    hiring = _get_signal(signals, "hiring_active")
    if not _is_true(hiring):
        if _is_known(hiring):
            return {"status": "Weak", "evidence": ["No hiring activity detected"], "confidence": 0.6}
        return {"status": "Unknown", "evidence": [], "confidence": 0.0}

    evidence = []
    roles = _get_signal(signals, "hiring_roles") or []
    if roles:
        evidence.append(f"Actively hiring: {', '.join(roles)}")
    else:
        evidence.append("Hiring activity detected on website")

    if _is_false(_get_signal(signals, "has_automated_scheduling")):
        evidence.append("Manual scheduling during growth phase")
    if _is_false(_get_signal(signals, "has_contact_form")):
        evidence.append("No online contact form for increased volume")
    rc = _get_signal(signals, "review_count")
    if rc is not None and rc > 50:
        evidence.append(f"Established business ({rc} reviews) in growth mode")

    return {"status": "Strong" if (roles or len(evidence) >= 3) else "Moderate", "evidence": evidence, "confidence": 0.75}


def _dimension_reviews_reputation(signals: Dict) -> Dict:
    """Reviews & Reputation: volume, freshness, rating trend, and review context (themes/summary)."""
    review_count = _get_signal(signals, "review_count")
    last_days = _get_signal(signals, "last_review_days_ago")
    rating = _get_signal(signals, "rating")
    rating_delta = _get_signal(signals, "rating_delta_60d")
    review_themes = _get_signal(signals, "review_themes")
    review_summary_text = _get_signal(signals, "review_summary_text")

    if not _is_known(review_count) and not _is_known(last_days) and not _is_known(rating):
        return {"status": "Unknown", "evidence": [], "confidence": 0.0}

    evidence = []
    if review_themes and isinstance(review_themes, list):
        evidence.append(f"Review themes: {', '.join(review_themes[:5])}")
    if review_summary_text and isinstance(review_summary_text, str) and len(review_summary_text) > 20:
        evidence.append(f"Summary: {review_summary_text[:150]}{'...' if len(review_summary_text) > 150 else ''}")
    if last_days is not None and last_days > REVIEW_STALE_DAYS:
        months = last_days // 30
        evidence.append(f"No recent reviews in {months}+ months")
    if review_count is not None and review_count < 10:
        evidence.append(f"Very low review volume ({review_count})")
    elif review_count is not None and review_count < LOW_REVIEW_COUNT:
        evidence.append(f"Low review volume ({review_count})")
    if rating_delta is not None and rating_delta < -0.3:
        evidence.append(f"Rating trending down ({rating_delta:+.1f} in 60 days)")
    if rating is not None and rating < 4.0:
        evidence.append(f"Below-average rating ({rating})")
    if not evidence and review_count is not None and review_count >= 50:
        evidence.append(f"Solid review base ({review_count} reviews)")

    if not evidence:
        if review_count is not None:
            evidence.append(f"Review count: {review_count}" + (f", rating {rating}" if rating is not None else ""))
        return {"status": "Moderate", "evidence": evidence or ["Limited review data"], "confidence": 0.6}
    n = len(evidence)
    status = "Strong" if n >= 3 else ("Moderate" if n >= 2 else "Weak")
    return {"status": status, "evidence": evidence, "confidence": 0.85}


def _dimension_website_funnel(signals: Dict) -> Dict:
    """Website & Funnel: presence, accessibility, conversion paths."""
    has_website = _get_signal(signals, "has_website")
    accessible = _get_signal(signals, "website_accessible")

    if _is_false(has_website):
        return {"status": "Weak", "evidence": ["No business website listed"], "confidence": 0.9}
    if not _is_true(has_website):
        return {"status": "Unknown", "evidence": [], "confidence": 0.0}

    evidence = ["Has website"]
    if _is_false(accessible):
        return {"status": "Strong", "evidence": ["Website exists but not accessible"], "confidence": 0.9}
    if _is_true(accessible):
        evidence.append("Website accessible")
    if _is_false(_get_signal(signals, "has_ssl")):
        evidence.append("No SSL (HTTPS)")
    if _is_false(_get_signal(signals, "mobile_friendly")):
        evidence.append("Not mobile-friendly")
    if _is_false(_get_signal(signals, "has_contact_form")):
        evidence.append("No contact form")
    if _is_false(_get_signal(signals, "has_email")):
        evidence.append("No email visible")
    if _is_false(_get_signal(signals, "has_automated_scheduling")):
        evidence.append("No automated scheduling")

    gaps = sum(1 for e in evidence if "No " in e or "not " in e.lower())
    if gaps >= 2:
        status = "Strong"
    elif gaps == 1:
        status = "Moderate"
    else:
        status = "Moderate" if len(evidence) <= 2 else "Weak"
    return {"status": status, "evidence": evidence, "confidence": 0.8}


def _dimension_operational_maturity(signals: Dict) -> Dict:
    """Operational Maturity: scheduling, forms, trust, automation."""
    evidence = []
    scheduling = _get_signal(signals, "has_automated_scheduling")
    if _is_true(scheduling):
        evidence.append("Automated scheduling detected")
    elif _is_false(scheduling):
        evidence.append("No automated scheduling (manual operations)")
    if _is_true(_get_signal(signals, "has_contact_form")):
        evidence.append("Contact form present")
    if _is_true(_get_signal(signals, "has_trust_badges")):
        evidence.append("Trust badges present")
    if _is_true(_get_signal(signals, "has_ssl")):
        evidence.append("SSL/HTTPS")

    if not evidence:
        return {"status": "Unknown", "evidence": [], "confidence": 0.0}
    strong = sum(1 for e in evidence if "automated" in e.lower() or "trust" in e.lower() or "SSL" in e)
    if _is_false(scheduling) and _get_signal(signals, "review_count") and _get_signal(signals, "review_count") > 20:
        evidence.append("Active business with manual operations")
    status = "Strong" if strong >= 2 else ("Moderate" if evidence else "Weak")
    return {"status": status, "evidence": evidence, "confidence": 0.75}


def _dimension_reachability(signals: Dict) -> Dict:
    """Reachability: phone, email, form — can we contact them. Phase 0.1: phone/address in HTML, social links."""
    evidence = []
    has_phone = _get_signal(signals, "has_phone")
    has_email = _get_signal(signals, "has_email")
    has_form = _get_signal(signals, "has_contact_form")
    if _is_true(has_phone):
        evidence.append("Phone available")
    elif _is_false(has_phone):
        evidence.append("No phone found")
    if _is_true(has_email):
        evidence.append("Email visible")
    elif _is_false(has_email):
        evidence.append("No email found")
    if _is_true(has_form):
        evidence.append("Contact form available")
    elif _is_false(has_form):
        evidence.append("No contact form")
    if _is_true(_get_signal(signals, "has_phone_in_html")):
        evidence.append("Phone visible on website")
    if _is_true(_get_signal(signals, "has_social_links")):
        platforms = _get_signal(signals, "social_platforms") or []
        evidence.append(f"Social links on website ({', '.join(platforms[:3])})" if platforms else "Social links on website")

    if not evidence:
        return {"status": "Unknown", "evidence": [], "confidence": 0.0}
    contact_points = sum(1 for v, k in [(has_phone, "phone"), (has_email, "email"), (has_form, "form")] if _is_true(v))
    if contact_points >= 2:
        status = "Strong"
    elif contact_points == 1:
        status = "Moderate"
    elif _is_false(has_phone) and _is_false(has_form) and _is_false(has_email):
        status = "Weak"
    else:
        status = "Moderate"
    return {"status": status, "evidence": evidence, "confidence": 0.85}


def _build_reasoning_summary(dimensions: List[Dict], signals: Dict) -> str:
    """One-paragraph deterministic summary of context."""
    parts = []
    for d in dimensions:
        name = d.get("dimension", "")
        status = d.get("status", "Unknown")
        ev = d.get("evidence", [])
        if status == "Unknown" or not ev:
            continue
        parts.append(f"{name}: {status}. {' '.join(ev[:2])}.")
    if not parts:
        return "Limited signal coverage; prioritize leads with more data for higher confidence."
    return " ".join(parts)[:500]


def _build_synthesis(
    dimensions: List[Dict],
    signals: Dict,
) -> Dict[str, Any]:
    """Priority suggestion, primary themes, suggested outreach angles (deterministic)."""
    priority_suggestion = "Low"
    themes = []
    angles = []

    status_scores = {"Strong": 3, "Moderate": 2, "Weak": 1, "Unknown": 0}
    weights = DIMENSION_PRIORITY_WEIGHTS
    by_dim = {d["dimension"]: d for d in dimensions}
    paid = by_dim.get("Paid Growth", {})
    hiring = by_dim.get("Hiring & Timing", {})
    reviews = by_dim.get("Reviews & Reputation", {})
    website = by_dim.get("Website & Funnel", {})
    ops = by_dim.get("Operational Maturity", {})
    reach = by_dim.get("Reachability", {})

    if paid.get("status") == "Strong":
        themes.append("Paid growth / conversion optimization")
        angles.append("Reduce ad waste with conversion-focused landing pages and forms")
    if hiring.get("status") in ("Strong", "Moderate"):
        themes.append("Scaling and hiring")
        angles.append("Support growth with scheduling and intake automation")
    if reviews.get("status") in ("Strong", "Moderate"):
        themes.append("Reputation and reviews")
        angles.append("Review generation and reputation management")
    if website.get("status") == "Strong" and "not accessible" in " ".join(website.get("evidence", [])):
        themes.append("Digital presence")
        angles.append("Website reliability and mobile experience")
    if ops.get("status") == "Weak" or (_is_false(signals.get("has_automated_scheduling")) and _get_signal(signals, "review_count") and _get_signal(signals, "review_count") > 10):
        themes.append("Operational efficiency")
        angles.append("Introduce scheduling and form capture to reduce manual work")
    if reach.get("status") == "Strong":
        angles.append("Multiple contact channels — good candidate for outreach")

    # Weighted score for priority (configurable via DIMENSION_PRIORITY_WEIGHTS)
    score = sum(
        status_scores.get(d.get("status"), 0) * weights.get(d.get("dimension"), 1.0)
        for d in dimensions
    )
    if score >= 12:
        priority_suggestion = "High"
    elif score >= 7:
        priority_suggestion = "Medium"

    # Short "why" for priority (debuggable)
    strong_dims = [d["dimension"] for d in dimensions if d.get("status") == "Strong"]
    moderate_dims = [d["dimension"] for d in dimensions if d.get("status") == "Moderate"]
    if priority_suggestion == "High":
        priority_derivation = "Priority High: " + ", ".join(strong_dims[:3]) + (" (+ moderate)" if moderate_dims else "")
    elif priority_suggestion == "Medium":
        priority_derivation = "Priority Medium: " + ", ".join((strong_dims + moderate_dims)[:3])
    else:
        priority_derivation = "Priority Low: limited strong/moderate dimensions"

    return {
        "priority_suggestion": priority_suggestion,
        "priority_derivation": priority_derivation,
        "primary_themes": themes[:5] if themes else ["General outreach"],
        "suggested_outreach_angles": angles[:5] if angles else ["Lead has sufficient signals for tailored outreach"],
    }


def calculate_confidence(signals: Dict) -> float:
    """Overall data coverage confidence (0–1)."""
    observed = 0.0
    total = 0.0
    for name, weight in SIGNAL_WEIGHTS.items():
        total += weight
        v = signals.get(name)
        if _is_known(v):
            observed += weight
    if total == 0:
        return 0.0
    return round(observed / total, 2)


def build_context(lead_or_signals: Dict) -> Dict[str, Any]:
    """
    Build context dimensions and reasoning from signals (deterministic).

    Args:
        lead_or_signals: Lead dict with signal_* keys, or plain signals dict.

    Returns:
        {
          "context_dimensions": [{"dimension", "status", "evidence", "confidence"}, ...],
          "reasoning_summary": str,
          "priority_suggestion": str,
          "primary_themes": list,
          "suggested_outreach_angles": list,
          "confidence": float,
        }
    """
    signals = _normalize_signals(lead_or_signals)
    dimension_builders = [
        ("Paid Growth", _dimension_paid_growth),
        ("Hiring & Timing", _dimension_hiring_timing),
        ("Reviews & Reputation", _dimension_reviews_reputation),
        ("Website & Funnel", _dimension_website_funnel),
        ("Operational Maturity", _dimension_operational_maturity),
        ("Reachability", _dimension_reachability),
    ]
    context_dimensions = []
    for name, fn in dimension_builders:
        out = fn(signals)
        context_dimensions.append({
            "dimension": name,
            "status": out["status"],
            "evidence": out["evidence"],
            "confidence": out["confidence"],
        })
    reasoning_summary = _build_reasoning_summary(context_dimensions, signals)
    synthesis = _build_synthesis(context_dimensions, signals)
    confidence = calculate_confidence(signals)

    # No-opportunity flag: all dimensions Weak/Unknown and enough signal coverage
    statuses = [d["status"] for d in context_dimensions]
    no_opportunity = (
        confidence >= 0.5
        and all(s in ("Weak", "Unknown") for s in statuses)
    )
    no_opportunity_reason = (
        "No clear gap or opportunity; dimensions are Weak or Unknown despite sufficient data."
        if no_opportunity else None
    )

    return {
        "context_dimensions": context_dimensions,
        "reasoning_summary": reasoning_summary,
        "priority_suggestion": synthesis["priority_suggestion"],
        "priority_derivation": synthesis.get("priority_derivation"),
        "primary_themes": synthesis["primary_themes"],
        "suggested_outreach_angles": synthesis["suggested_outreach_angles"],
        "confidence": confidence,
        "no_opportunity": no_opportunity,
        "no_opportunity_reason": no_opportunity_reason,
    }
