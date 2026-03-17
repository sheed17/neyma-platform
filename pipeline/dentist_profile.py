"""
Dentist vertical profile (dentist_profile_v1): deterministic inference for SEO agency opportunity.

Only populated when the business is identified as a dental practice.
Uses existing signals + optional website HTML for trust scan. No new scraping.
"""

import re
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# Detection: name or context suggests dental practice
DENTAL_NAME_PATTERNS = re.compile(
    r"\b(dentist|dental|orthodont|cosmetic\s*dentist|oral\s*surgery|periodont|endodont|dds|dmd|teeth|smile)\b",
    re.I,
)

# Procedure keywords for LTV and review intent
PROCEDURE_HIGH_LTV = ["implant", "invisalign", "cosmetic", "veneer", "orthodontic", "whitening", "bonding", "braces"]
PROCEDURE_MEDIUM = ["cleaning", "general dentistry", "checkup", "filling", "crown", "root canal", "extraction", "x-ray"]
URGENCY_KEYWORDS = ["emergency", "same day", "urgent", "walk-in", "pain", "toothache"]
INSURANCE_KEYWORDS = ["insurance", "accepts", "in-network", "ppo", "hmo", "coverage"]

# Trust signals: website content patterns
TRUST_INSURANCE_PATTERNS = re.compile(
    r"insurance|accepts\s+.*insurance|in-?network|ppo|hmo|dental\s+plans?|coverage",
    re.I,
)
TRUST_BEFORE_AFTER_PATTERNS = re.compile(
    r"before\s*and\s*after|before\s*&\s*after|before/after|gallery|results|transform",
    re.I,
)
TRUST_CREDENTIALS_PATTERNS = re.compile(
    r"doctor|dr\.|dds|dmd|credentials|education|residency|specialty|board\s+certified",
    re.I,
)


def _get_signal(lead: Dict, key: str, prefix: str = "signal_") -> Any:
    return lead.get(f"{prefix}{key}") if key != "name" else lead.get("name")


def is_dental_practice(lead: Dict) -> bool:
    """Return True if the business appears to be a dental practice (name or review context)."""
    name = (lead.get("name") or "").strip()
    if DENTAL_NAME_PATTERNS.search(name):
        return True
    summary = (lead.get("signal_review_summary_text") or "").lower()
    snippets = lead.get("signal_review_sample_snippets") or []
    combined = summary + " " + " ".join(str(s) for s in snippets).lower()
    if any(k in combined for k in ["dentist", "dental", "teeth", "implant", "cleaning", "orthodont"]):
        return True
    return False


def _procedure_focus_detected(lead: Dict) -> List[str]:
    """Detect procedure focus from review summary and snippets."""
    found = []
    summary = (lead.get("signal_review_summary_text") or "").lower()
    snippets = lead.get("signal_review_sample_snippets") or []
    combined = summary + " " + " ".join(str(s) for s in snippets).lower()
    for kw in PROCEDURE_HIGH_LTV + PROCEDURE_MEDIUM:
        if kw in combined and kw not in found:
            found.append(kw)
    return found[:10]


def _estimated_ltv_class(procedure_focus: List[str], has_high_intent: bool) -> str:
    if has_high_intent or any(p in PROCEDURE_HIGH_LTV for p in procedure_focus):
        return "High"
    if procedure_focus or any(p in PROCEDURE_MEDIUM for p in procedure_focus):
        return "Medium"
    return "Low"


def _practice_type_from_focus(procedure_focus: List[str]) -> str:
    if any(p in ["orthodontic", "braces", "invisalign"] for p in procedure_focus):
        return "orthodontic"
    if any(p in ["cosmetic", "veneer", "whitening", "bonding"] for p in procedure_focus):
        return "cosmetic"
    if len(procedure_focus) > 3 or ("implant" in procedure_focus and "cosmetic" in procedure_focus):
        return "multi_specialty"
    if any(p in PROCEDURE_MEDIUM for p in procedure_focus) or procedure_focus:
        return "general_dentistry"
    return "unknown"


def _confidence_from_signals(lead: Dict, has_website: bool, has_reviews: bool) -> float:
    n = 0
    if (lead.get("signal_review_summary_text") or "").strip():
        n += 1
    if lead.get("signal_review_count"):
        n += 1
    if has_website:
        n += 1
    if lead.get("signal_rating") is not None:
        n += 1
    return round(min(1.0, n * 0.25), 2)


def _build_dental_practice_profile(lead: Dict) -> Dict[str, Any]:
    procedure_focus = _procedure_focus_detected(lead)
    high_intent = any(p in PROCEDURE_HIGH_LTV for p in procedure_focus)
    ltv = _estimated_ltv_class(procedure_focus, high_intent)
    practice_type = _practice_type_from_focus(procedure_focus)
    has_reviews = bool((lead.get("signal_review_summary_text") or "").strip() or lead.get("signal_review_count"))
    has_website = lead.get("signal_has_website") is True
    conf = _confidence_from_signals(lead, has_website, has_reviews)
    return {
        "practice_type": practice_type,
        "procedure_focus_detected": procedure_focus,
        "estimated_ltv_class": ltv,
        "confidence": conf,
    }


def _build_patient_acquisition_readiness(lead: Dict) -> Dict[str, Any]:
    booking_path = lead.get("signal_booking_conversion_path")
    has_booking = (
        booking_path in ("Online booking (limited)", "Online booking (full)")
        or lead.get("signal_has_automated_scheduling") is True
    )
    no_booking = (
        booking_path in ("Phone-only", "Request form")
        or lead.get("signal_has_automated_scheduling") is False
    )
    has_phone = lead.get("signal_has_phone") is True
    has_form = lead.get("signal_has_contact_form") is True
    if no_booking and has_phone and not has_form:
        booking_friction = "High"
    elif no_booking:
        booking_friction = "Moderate"
    else:
        booking_friction = "Low"
    conversion_leaks = []
    if not has_form and lead.get("signal_has_website") is True:
        conversion_leaks.append("No contact form for web leads")
    if no_booking and has_phone:
        conversion_leaks.append("Phone-only intake; no online booking")
    rating = lead.get("signal_rating")
    review_count = lead.get("signal_review_count") or 0
    if rating and rating >= 4.0 and review_count >= 5 and no_booking:
        chair_fill_risk = "Moderate"
    elif no_booking:
        chair_fill_risk = "High"
    else:
        chair_fill_risk = "Low"
    conf = _confidence_from_signals(lead, lead.get("signal_has_website") is True, bool(review_count))
    return {
        "booking_friction": booking_friction,
        "conversion_leaks": conversion_leaks[:5],
        "chair_fill_risk": chair_fill_risk,
        "confidence": conf,
    }


def _build_local_search_positioning(lead: Dict) -> Dict[str, Any]:
    review_count = lead.get("signal_review_count") or 0
    rating = lead.get("signal_rating")
    last_days = lead.get("signal_last_review_days_ago")
    if review_count < 50:
        review_count_vs_market = "Below Average"
    elif review_count < 150:
        review_count_vs_market = "Average"
    else:
        review_count_vs_market = "Above Average"
    if rating is None:
        rating_strength = "Weak"
    elif rating >= 4.8 and review_count >= 20:
        rating_strength = "Strong"
    elif rating >= 4.0:
        rating_strength = "Moderate"
    else:
        rating_strength = "Weak"
    if review_count < 30 and last_days is not None and last_days > 180:
        map_pack_competitiveness = "Low"
    elif review_count >= 100:
        map_pack_competitiveness = "High"
    else:
        map_pack_competitiveness = "Moderate"
    if review_count_vs_market == "Below Average" and rating_strength in ("Moderate", "Strong"):
        visibility_gap = "Underutilized"
    elif map_pack_competitiveness == "High":
        visibility_gap = "Saturated"
    else:
        visibility_gap = "Competitive"
    conf = _confidence_from_signals(lead, lead.get("signal_has_website") is True, bool(review_count))
    return {
        "review_count_vs_market": review_count_vs_market,
        "rating_strength": rating_strength,
        "map_pack_competitiveness": map_pack_competitiveness,
        "visibility_gap": visibility_gap,
        "confidence": conf,
    }


def _scan_trust_signals(html: Optional[str]) -> Dict[str, Any]:
    """Scan website HTML for dentist trust/conversion signals. Returns booleans + confidence."""
    if not (html or "").strip():
        return {"insurance_accepted_visible": False, "before_after_gallery": False, "doctor_credentials_visible": False, "confidence": 0.0}
    text = html[:50000]  # cap size
    ins = bool(TRUST_INSURANCE_PATTERNS.search(text))
    before_after = bool(TRUST_BEFORE_AFTER_PATTERNS.search(text))
    creds = bool(TRUST_CREDENTIALS_PATTERNS.search(text))
    n = sum([ins, before_after, creds])
    conf = round(min(1.0, 0.3 + n * 0.2), 2)
    return {
        "insurance_accepted_visible": ins,
        "before_after_gallery": before_after,
        "doctor_credentials_visible": creds,
        "confidence": conf,
    }


def _build_trust_conversion_signals(lead: Dict, website_html: Optional[str]) -> Dict[str, Any]:
    if website_html:
        return _scan_trust_signals(website_html)
    return {"insurance_accepted_visible": False, "before_after_gallery": False, "doctor_credentials_visible": False, "confidence": 0.0}


def _build_review_intent_analysis(lead: Dict) -> Dict[str, Any]:
    procedure_mentions = _procedure_focus_detected(lead)
    summary = (lead.get("signal_review_summary_text") or "").lower()
    snippets = lead.get("signal_review_sample_snippets") or []
    combined = summary + " " + " ".join(str(s) for s in snippets).lower()
    urgency = any(u in combined for u in URGENCY_KEYWORDS)
    insurance = any(i in combined for i in INSURANCE_KEYWORDS)
    conf = _confidence_from_signals(lead, lead.get("signal_has_website") is True, bool(combined.strip()))
    return {
        "procedure_mentions": procedure_mentions[:8],
        "urgency_language_detected": urgency,
        "insurance_mentions": insurance,
        "confidence": conf,
    }


def _build_agency_fit_reasoning(lead: Dict, dental_profile: Dict, patient_readiness: Dict, local_pos: Dict, review_intent: Dict) -> Dict[str, Any]:
    why = []
    risk_flags = []
    ideal = False
    strong_rep = (lead.get("signal_rating") or 0) >= 4.0 and (lead.get("signal_review_count") or 0) >= 5
    low_volume = (lead.get("signal_review_count") or 0) < 80
    booking_path = lead.get("signal_booking_conversion_path")
    no_booking = (
        booking_path in ("Phone-only", "Request form")
        or lead.get("signal_has_automated_scheduling") is False
    )
    high_intent_procedures = bool(review_intent.get("procedure_mentions")) or dental_profile.get("estimated_ltv_class") == "High"
    if strong_rep and low_volume and no_booking:
        why.append("Strong reputation with room to grow review volume")
    if no_booking:
        why.append("No online booking funnel; SEO can capture demand")
    if high_intent_procedures:
        why.append("High-intent procedures detected; SEO ROI potential")
    if local_pos.get("visibility_gap") == "Underutilized":
        why.append("Underutilized local visibility; map pack opportunity")
    if patient_readiness.get("conversion_leaks"):
        why.append("Conversion leaks present; CRO + SEO angle")
    if strong_rep and (no_booking or low_volume) and (high_intent_procedures or local_pos.get("visibility_gap") == "Underutilized"):
        ideal = True
    if (lead.get("signal_review_count") or 0) < 10:
        risk_flags.append("Very low review volume")
    if lead.get("signal_last_review_days_ago") is not None and lead.get("signal_last_review_days_ago") > 365:
        risk_flags.append("Stale reviews")
    if not lead.get("signal_has_website"):
        risk_flags.append("No website")
    if lead.get("signal_runs_paid_ads") is True:
        risk_flags.append("Existing paid spend; may have agency")
    conf = _confidence_from_signals(lead, lead.get("signal_has_website") is True, bool(lead.get("signal_review_count")))
    return {
        "ideal_for_seo_outreach": ideal,
        "why": why[:5],
        "risk_flags": risk_flags[:5],
        "confidence": conf,
    }


def fetch_website_html_for_trust(url: str) -> Optional[str]:
    """Fetch website HTML for trust scan. Returns None on failure."""
    try:
        import requests
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        r = requests.get(url, timeout=15, allow_redirects=True)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        logger.debug("Dentist trust fetch failed for %s: %s", url[:50], e)
    return None


def build_dentist_profile_v1(lead: Dict, website_html: Optional[str] = None) -> Dict[str, Any]:
    """
    Build dentist_profile_v1 if lead is a dental practice. Otherwise return {}.

    Uses existing signals; optionally website_html for trust_conversion_signals.
    Deterministic only; no LLM.
    """
    if not is_dental_practice(lead):
        return {}
    dental_profile = _build_dental_practice_profile(lead)
    patient_readiness = _build_patient_acquisition_readiness(lead)
    local_pos = _build_local_search_positioning(lead)
    trust_signals = _build_trust_conversion_signals(lead, website_html)
    review_intent = _build_review_intent_analysis(lead)
    agency_fit = _build_agency_fit_reasoning(lead, dental_profile, patient_readiness, local_pos, review_intent)
    return {
        "dental_practice_profile": dental_profile,
        "patient_acquisition_readiness": patient_readiness,
        "local_search_positioning": local_pos,
        "trust_conversion_signals": trust_signals,
        "review_intent_analysis": review_intent,
        "agency_fit_reasoning": agency_fit,
    }
