"""
Revenue Intelligence Brief — UI rendering.

Primary sources: objective_intelligence, revenue_intelligence, competitive_snapshot, signals.
summary_60s / objective_decision_layer / canonical_summary_v1 used only as fallback.
No LLM. No new calculations (only normalization + formatting).
No model version labels. Wording focused on SEO for dental practices (local visibility, service pages, GBP, review velocity).
"""

from typing import Dict, Any, List, Optional, Tuple

from pipeline.consistency import normalize_conversion_infrastructure, normalize_service_intelligence


def _is_dental_for_brief(lead: Dict[str, Any]) -> bool:
    """Lightweight dental check for brief rendering (avoids heavy dentist_profile import)."""
    if lead.get("dentist_profile_v1"):
        return True
    if (lead.get("objective_intelligence") or {}).get("service_intel"):
        return True
    name = (lead.get("name") or "").lower()
    if "dental" in name or "dentist" in name:
        return True
    return False

# Canonical service buckets: canonical_label -> list of raw strings to match (lowercase)
CANONICAL_SERVICE_BUCKETS: Dict[str, List[str]] = {
    "implants": ["implant", "dental implant", "implants", "dental implants", "all-on-4", "all on 4"],
    "orthodontics": ["orthodontic", "orthodontics", "invisalign", "braces", "clear aligner", "clear aligners"],
    "veneers": ["veneer", "veneers", "porcelain veneer", "porcelain veneers"],
    "emergency": ["emergency", "emergency dental", "emergency dentist", "same day", "same-day", "urgent dental"],
    "cosmetic": ["cosmetic", "cosmetic dentistry", "smile makeover", "teeth whitening", "whitening"],
    "sedation": ["sedation", "sedation dentistry", "iv sedation", "nitrous", "nitrous oxide", "oral sedation", "sleep dentistry"],
    "crowns": ["crown", "crowns", "same day crown", "same-day crown", "dental crown", "dental crowns"],
    "sleep apnea": ["sleep apnea", "sleep-apnea", "snoring"],
}
CANONICAL_ORDER = ["Implants", "Orthodontics", "Veneers", "Emergency", "Cosmetic", "Sedation", "Crowns", "Sleep Apnea"]

# Bottleneck snake_case -> Title Case
BOTTLENECK_TO_LABEL: Dict[str, str] = {
    "saturation_limited": "Saturation Limited",
    "visibility_limited": "Visibility Limited",
    "conversion_constrained": "Conversion Constrained",
    "capture_constrained": "Capture Constrained",
    "trust_limited": "Trust Limited",
}

# Bottleneck -> short leverage label (for Primary Leverage)
BOTTLENECK_TO_LEVERAGE: Dict[str, str] = {
    "saturation_limited": "High-ticket capture",
    "visibility_limited": "Visibility",
    "conversion_constrained": "Conversion",
    "capture_constrained": "High-ticket capture",
    "trust_limited": "Trust",
}

# Primary service for revenue upside section: priority order (first match from missing_high_value_pages wins)
PRIMARY_SERVICE_PRIORITY: List[Tuple[str, str]] = [
    ("implants", "Implants"),
    ("orthodontics", "Orthodontics"),
    ("invisalign", "Invisalign"),
    ("orthodontic", "Orthodontic"),
    ("veneers", "Veneers"),
    ("cosmetic", "Cosmetic"),
    ("sedation", "Sedation"),
    ("crowns", "Crowns"),
    ("sleep apnea", "Sleep Apnea"),
    ("emergency", "Emergency"),
]
# Case value proxy (low, high) per canonical key for revenue breakdown — tight deterministic bands
PRIMARY_SERVICE_CASE_VALUE: Dict[str, Tuple[int, int]] = {
    "implants": (4000, 6000),
    "orthodontics": (3500, 5500),
    "invisalign": (3500, 5500),
    "orthodontic": (3500, 5500),
    "veneers": (3000, 5000),
    "cosmetic": (3000, 5000),
    "sedation": (2000, 4000),
    "crowns": (1500, 3000),
    "sleep apnea": (2000, 4000),
    "emergency": (1000, 2500),
}
# Consult range (low, high) per traffic_estimate_tier — max spread 2x
TRAFFIC_TIER_CONSULTS: Dict[str, Tuple[int, int]] = {
    "High": (3, 6),
    "Moderate": (2, 4),
}
DEFAULT_CONSULTS = (1, 3)
DEFAULT_CASE_VALUE = (2500, 4000)


def _review_volume_tier(review_count: Optional[float], avg_reviews: Optional[float]) -> Optional[str]:
    try:
        if review_count is None or avg_reviews is None or float(avg_reviews) <= 0:
            return None
        ratio = float(review_count) / float(avg_reviews)
    except (TypeError, ValueError, ZeroDivisionError):
        return None
    if ratio >= 1.75:
        return "Dominant"
    if ratio >= 1.2:
        return "Above Average"
    if ratio >= 0.8:
        return "Competitive"
    if ratio >= 0.5:
        return "Below Average"
    return "Weak"


def _review_volume_label_from_tier(tier: Optional[str]) -> str:
    if tier in ("Dominant", "Above Average"):
        return "Above average"
    if tier == "Competitive":
        return "At market average"
    return "Below average"


def _format_miles(val: Any) -> str:
    try:
        dist = float(val)
    except (TypeError, ValueError):
        return str(val) if val is not None else "—"
    if dist.is_integer():
        return f"{int(dist)} mi"
    return f"{dist:.1f} mi"


def _service_key_from_text(service_text: Any) -> Optional[Tuple[str, str]]:
    if not isinstance(service_text, str):
        return None
    raw = service_text.strip()
    if not raw:
        return None
    lower = raw.lower()
    for key, display in PRIMARY_SERVICE_PRIORITY:
        if key in lower or lower in key:
            return key, display
    for canonical_key, aliases in CANONICAL_SERVICE_BUCKETS.items():
        if any(alias in lower for alias in aliases):
            display = canonical_key.title() if canonical_key != "sleep apnea" else "Sleep Apnea"
            return canonical_key, display
    return None


def _service_key_equivalent(a: str, b: str) -> bool:
    """Treat close orthodontic variants as equivalent for selection checks."""
    if a == b:
        return True
    ortho = {"orthodontics", "orthodontic", "invisalign"}
    return a in ortho and b in ortho


def _yes_no_unknown(v: Any) -> str:
    if v is None:
        return "Not Evaluated (Low Crawl Confidence)"
    return "Yes" if bool(v) else "No"


def _refine_risk_flags(
    flags: List[str],
    detected_canonical: List[str],
    missing_canonical: List[str],
    paid_active: bool,
    suppress_service_gap: bool = False,
    suppress_conversion_absence_claims: bool = False,
) -> List[str]:
    if not flags:
        return []
    missing_txt = ", ".join(missing_canonical[:3]) if missing_canonical else ""
    all_missing = bool(detected_canonical and missing_canonical and len(missing_canonical) >= len(detected_canonical))
    out: List[str] = []
    for flag in flags:
        f = (flag or "").strip()
        if not f:
            continue
        lower = f.lower()
        if suppress_conversion_absence_claims and (
            "no contact form" in lower
            or "phone-only" in lower
            or "phone only" in lower
            or "no booking" in lower
            or "no online booking" in lower
        ):
            continue
        if suppress_service_gap and (
            "missing dedicated" in lower
            or "missing service" in lower
            or "missing page" in lower
            or "high-value service pages missing" in lower
        ):
            continue
        if "high-ticket services offered but no dedicated landing pages" in lower:
            if suppress_service_gap:
                continue
            if all_missing:
                out.append("High-ticket services offered but no dedicated landing pages found in scan")
            elif missing_canonical:
                out.append(
                    f"No dedicated pages found for some high-value services ({missing_txt})"
                    if missing_txt
                    else "No dedicated pages found for some high-value services in scan"
                )
            continue
        if "paid ads running but high-value service pages missing" in lower:
            if suppress_service_gap:
                continue
            if paid_active and missing_canonical:
                out.append(
                    f"Paid ads running but no dedicated pages found for some services ({missing_txt})"
                    if missing_txt
                    else "Paid ads running but no dedicated service pages found in scan"
                )
            elif missing_canonical:
                out.append(
                    f"No dedicated pages found for some high-value services ({missing_txt})"
                    if missing_txt
                    else "No dedicated pages found for some high-value services in scan"
                )
            continue
        out.append(f)
    return out


def compute_opportunity_profile(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deterministic Opportunity Profile label and short parenthetical why.
    No LLM. No new APIs. Uses objective_intelligence, competitive_snapshot, revenue_intelligence, signals.
    Returns {
      "label": "High-Leverage"|"Moderate"|"Low-Leverage",
      "why": "...",
      "leverage_drivers": {...}
    } or {} when omitted.
    """
    try:
        oi = lead.get("objective_intelligence") or {}
        cs = lead.get("competitive_snapshot") or oi.get("competitive_profile") or {}
        ri = lead.get("revenue_intelligence") or {}
        if lead.get("signals") and isinstance(lead["signals"], dict):
            signals = lead["signals"]
        else:
            signals = {k: v for k, v in lead.items() if k.startswith("signal_") or k in ("user_ratings_total", "rating")}
        service = oi.get("service_intel") or lead.get("service_intelligence") or {}
        suppress_service = bool(service.get("suppress_service_gap"))
        crawl_conf = str(service.get("crawl_confidence") or "").strip().lower()
        if not suppress_service and crawl_conf == "low":
            suppress_service = True

        missing_pages = service.get("missing_high_value_pages") if not suppress_service else []
        missing_high_value = bool(missing_pages and (isinstance(missing_pages, list) and len(missing_pages) > 0))

        market_density = (cs.get("market_density_score") or "").strip() or (oi.get("competitive_profile") or {}).get("market_density") or ""
        high_density = (market_density == "High")

        reviews = lead.get("signal_review_count") or signals.get("signal_review_count") or signals.get("user_ratings_total") or lead.get("user_ratings_total") or 0
        try:
            reviews = int(reviews)
        except (TypeError, ValueError):
            reviews = 0

        local_avg = cs.get("avg_review_count") if cs.get("avg_review_count") is not None else cs.get("avg_reviews")
        try:
            local_avg_reviews = float(local_avg) if local_avg is not None else 0
        except (TypeError, ValueError):
            local_avg_reviews = 0

        review_deficit = (local_avg_reviews > 0 and reviews < 0.5 * local_avg_reviews)
        rating = signals.get("signal_rating") or signals.get("rating")
        try:
            rating = float(rating) if rating is not None else None
        except (TypeError, ValueError):
            rating = None
        avg_rating = cs.get("avg_rating")
        try:
            avg_rating = float(avg_rating) if avg_rating is not None else None
        except (TypeError, ValueError):
            avg_rating = None
        strong_rating = bool(rating is not None and (avg_rating is None or rating >= avg_rating))

        paid_channels = signals.get("signal_paid_ads_channels")
        paid_active = (
            signals.get("signal_runs_paid_ads") is True
            or (isinstance(paid_channels, list) and len(paid_channels) > 0)
            or (paid_channels and not isinstance(paid_channels, list))
        )

        # Crawl-based trust structure (schema-independent)
        v2 = service.get("service_page_analysis_v2") if isinstance(service.get("service_page_analysis_v2"), dict) else {}
        svc_summary = service.get("high_value_summary") if isinstance(service.get("high_value_summary"), dict) else {}
        v2_cov = v2.get("service_coverage") if isinstance(v2.get("service_coverage"), dict) else {}
        v2_conv = v2.get("conversion_readiness") if isinstance(v2.get("conversion_readiness"), dict) else {}
        v2_trust = v2.get("structured_trust_signals") if isinstance(v2.get("structured_trust_signals"), dict) else {}

        coverage_ratio_val = v2_cov.get("ratio")
        if coverage_ratio_val is None:
            coverage_ratio_val = svc_summary.get("service_coverage_ratio")
        try:
            coverage_ratio = float(coverage_ratio_val) if coverage_ratio_val is not None else None
        except (TypeError, ValueError):
            coverage_ratio = None

        avg_internal_links_val = v2_conv.get("average_internal_links")
        try:
            avg_internal_links = float(avg_internal_links_val) if avg_internal_links_val is not None else None
        except (TypeError, ValueError):
            avg_internal_links = None
        faq_status = str(v2_trust.get("faq_status") or "").strip().lower()
        faq_missing = faq_status == "not_detected"
        structured_trust_weak = bool(
            (coverage_ratio is not None and coverage_ratio < 0.6)
            or faq_missing
            or (avg_internal_links is not None and avg_internal_links < 3.0)
        )

        # Leverage classification inputs:
        # - missing_high_value: at least one high-value service page is missing
        # - high_density: market_density_score == "High"
        # - paid_active: paid ads signals present
        # - review_deficit: review count below 50% of local average
        #
        # Classification rules (keep deterministic and stable):
        # - High-Leverage: missing_high_value AND high_density
        # - Moderate: missing_high_value OR (high_density AND (paid_active OR review_deficit))
        # - Low-Leverage: all other cases
        if missing_high_value and high_density:
            label = "High-Leverage"
        elif missing_high_value or (high_density and (paid_active or review_deficit)):
            label = "Moderate"
        else:
            label = "Low-Leverage"

        fragments = []
        if missing_high_value:
            fragments.append("no dedicated landing page found for a high-ticket service in our scan")
        if structured_trust_weak:
            fragments.append("structured trust signals are weak")
        if review_deficit:
            fragments.append("review authority below local market")
        if strong_rating:
            fragments.append("rating quality remains strong")
        if paid_active:
            fragments.append("paid demand present but capture layer is weak")
        if high_density:
            fragments.append("high-density competitive market")
        why = ", ".join(fragments[:3])
        if not why:
            why = "baseline capture and market conditions"

        return {
            "label": label,
            "why": why,
            "leverage_drivers": {
                "missing_high_value_pages": bool(missing_high_value),
                "market_density_high": bool(high_density),
                "structured_trust_weak": bool(structured_trust_weak),
                "paid_active": bool(paid_active),
                "review_deficit": bool(review_deficit),
            },
        }
    except Exception:
        return {}


def compute_paid_demand_status(lead: Dict[str, Any]) -> Dict[str, str]:
    """
    Deterministic Paid Demand Status from existing signals.
    No LLM. No new APIs.
    Returns {"status": "...", "interpretation": "..."} or {} when omitted.
    """
    try:
        signals = lead.get("signals")
        if not isinstance(signals, dict):
            signals = {k: v for k, v in lead.items() if k.startswith("signal_")}
        ri = lead.get("revenue_intelligence") or {}
        cs = lead.get("competitive_snapshot") or (lead.get("objective_intelligence") or {}).get("competitive_profile") or {}
        oi = lead.get("objective_intelligence") or {}
        service_intel = oi.get("service_intel") or oi.get("service_intelligence") or lead.get("service_intelligence") or {}

        channels = signals.get("signal_paid_ads_channels") or []
        if not isinstance(channels, list):
            channels = [channels] if channels else []
        channels_lower = [str(c).strip().lower() for c in channels if c]

        runs_google = signals.get("signal_runs_paid_ads") is True and "google" in channels_lower
        runs_meta = "meta" in channels_lower
        high_density = (
            (cs.get("market_density_score") or "").strip() == "High"
            or (cs.get("market_density") or "").strip() == "High"
        )
        high_ticket_raw = service_intel.get("high_ticket_detected") or service_intel.get("high_ticket_procedures_detected")
        high_ticket = bool(high_ticket_raw and (isinstance(high_ticket_raw, list) and len(high_ticket_raw) > 0))

        if not runs_google and not runs_meta:
            return {"status": "Inactive", "interpretation": "No detectable paid demand activity."}
        if runs_google and high_density and high_ticket:
            return {"status": "Aggressive Search Presence", "interpretation": "Practice appears to be competing actively for high-value service demand."}
        if runs_google and runs_meta:
            return {"status": "Active (Search + Meta)", "interpretation": "Practice is investing in both high-intent and awareness channels."}
        if runs_google:
            return {"status": "Active (Search)", "interpretation": "Practice is investing in high-intent search demand."}
        if runs_meta:
            return {"status": "Active (Meta)", "interpretation": "Practice has Meta ads presence."}
        return {}
    except Exception:
        return {}


def compute_organic_visibility(lead: Dict[str, Any]) -> Dict[str, str]:
    """
    Deterministic organic visibility tier relative to the local market.

    Replaces fabricated numeric traffic ranges with a qualitative assessment
    anchored to competitive context and observable signals.

    Returns {"tier": "High"|"Moderate"|"Low", "reason": "..."} or {} if data missing.
    """
    try:
        cs = lead.get("competitive_snapshot") or {}
        oi = lead.get("objective_intelligence") or {}
        svc = oi.get("service_intel") or lead.get("service_intelligence") or {}

        if lead.get("signals") and isinstance(lead["signals"], dict):
            signals = lead["signals"]
        else:
            signals = {k: v for k, v in lead.items() if k.startswith("signal_") or k in ("user_ratings_total", "rating")}

        review_count = (
            signals.get("signal_review_count")
            or signals.get("user_ratings_total")
            or cs.get("lead_review_count")
            or 0
        )
        try:
            review_count = int(review_count)
        except (TypeError, ValueError):
            review_count = 0

        avg_reviews = cs.get("avg_review_count") or 0
        try:
            avg_reviews = float(avg_reviews)
        except (TypeError, ValueError):
            avg_reviews = 0

        has_website = signals.get("signal_has_website") is True
        has_service_pages = bool(
            svc.get("high_ticket_procedures_detected")
            or svc.get("high_ticket_services_detected")
        )
        missing_pages = svc.get("missing_high_value_pages") or []
        has_all_service_pages = has_service_pages and len(missing_pages) == 0
        runs_ads = signals.get("signal_runs_paid_ads") is True
        pages_crawled = svc.get("pages_crawled") or 0

        if not has_website:
            return {"tier": "Low", "reason": "No website detected in Google listing"}

        # Count positive signals for tiering
        positive = 0
        reasons_high: List[str] = []
        reasons_mod: List[str] = []
        reasons_low: List[str] = []

        if avg_reviews > 0 and review_count >= avg_reviews:
            positive += 2
            reasons_high.append("review authority at or above local average")
        elif avg_reviews > 0 and review_count >= avg_reviews * 0.3:
            positive += 1
            reasons_mod.append("moderate review presence relative to market")
        else:
            reasons_low.append("review count below local market average")

        if has_all_service_pages:
            positive += 2
            reasons_high.append("dedicated service pages for high-ticket procedures")
        elif has_service_pages:
            positive += 1
            reasons_mod.append("some high-ticket services detected but no dedicated pages found in scan")

        if pages_crawled >= 50:
            positive += 1
            reasons_high.append(f"{pages_crawled} indexed pages")
        elif pages_crawled >= 15:
            reasons_mod.append(f"{pages_crawled} indexed pages")

        if runs_ads:
            positive += 1
            reasons_mod.append("active paid campaigns supplementing organic")

        # Classification
        if positive >= 5:
            tier = "High"
            reason_parts = reasons_high[:3] or reasons_mod[:2]
        elif positive >= 2:
            tier = "Moderate"
            reason_parts = reasons_mod[:3] or reasons_high[:1] or reasons_low[:2]
        else:
            tier = "Low"
            reason_parts = reasons_low[:3] or reasons_mod[:1]

        if not reason_parts:
            reason_parts = ["limited public signals available"]

        market_ctx = ""
        density = cs.get("market_density_score") or ""
        if density:
            market_ctx = f" in a {density} density market"

        reason = ". ".join(r.capitalize() for r in reason_parts) + market_ctx

        return {"tier": tier, "reason": reason}
    except Exception:
        return {}


def _primary_service_from_missing(missing: List[str]) -> Optional[Tuple[str, str]]:
    """
    Select ONE primary service from missing_high_value_pages.
    Priority: implants > invisalign > orthodontic > veneers > cosmetic.
    Returns (canonical_key, display_name) or None if no match.
    """
    if not missing:
        return None
    missing_lower = [str(m).strip().lower() for m in missing if m]
    for key, display in PRIMARY_SERVICE_PRIORITY:
        for m in missing_lower:
            if key in m or m in key:
                return (key, display)
        if key == "orthodontic":
            if any(any(x in m for x in ("orthodont", "brace")) for m in missing_lower):
                return ("orthodontic", "Orthodontic")
    return None


def _get_summary_60s(lead: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Resolve summary_60s: agency_decision_v1.summary_60s > canonical_summary_v1 > internal_debug."""
    adv1 = lead.get("agency_decision_v1")
    if adv1 and isinstance(adv1, dict):
        s = adv1.get("summary_60s")
        if s and isinstance(s, dict):
            return s
    s = lead.get("canonical_summary_v1")
    if s and isinstance(s, dict):
        return s
    debug = lead.get("internal_debug")
    if debug and isinstance(debug, dict):
        adv1 = debug.get("agency_decision_v1_deprecated")
        if adv1 and isinstance(adv1, dict):
            s = adv1.get("summary_60s")
            if s and isinstance(s, dict):
                return s
    return None


def _get_revenue_intelligence(lead: Dict[str, Any]) -> Dict[str, Any]:
    """revenue_intelligence from lead or signals."""
    rev = lead.get("revenue_intelligence")
    if rev and isinstance(rev, dict):
        return rev
    sig = lead.get("signals")
    if sig and isinstance(sig, dict):
        rev = sig.get("revenue_intelligence")
        if rev and isinstance(rev, dict):
            return rev
    return {}


def _get_signals(lead: Dict[str, Any]) -> Dict[str, Any]:
    """Signals: lead.signals or top-level signal_* / user_ratings_total / rating."""
    if lead.get("signals") and isinstance(lead["signals"], dict):
        return lead["signals"]
    return {k: v for k, v in lead.items() if k.startswith("signal_") or k in ("user_ratings_total", "rating")}


def _get_objective_intelligence(lead: Dict[str, Any]) -> Dict[str, Any]:
    """objective_intelligence from lead (primary source for root constraint, intervention plan, etc.)."""
    oi = lead.get("objective_intelligence")
    if oi and isinstance(oi, dict):
        return oi
    return {}


def _get_objective_layer(lead: Dict[str, Any]) -> Dict[str, Any]:
    """objective_decision_layer or models (fallback when objective_intelligence missing)."""
    obj = lead.get("objective_decision_layer")
    if obj and isinstance(obj, dict):
        return obj
    obj = lead.get("objective_layer")
    if obj and isinstance(obj, dict):
        return obj
    return lead.get("models") or {}


def _get_competitive_snapshot(lead: Dict[str, Any]) -> Dict[str, Any]:
    """competitive_snapshot from objective_layer, lead, or signals."""
    obj = _get_objective_layer(lead)
    comp = obj.get("competitive_snapshot") if obj else None
    if comp and isinstance(comp, dict):
        return comp
    comp = lead.get("competitive_snapshot")
    if comp and isinstance(comp, dict):
        return comp
    sig = lead.get("signals")
    if sig and isinstance(sig, dict):
        comp = sig.get("competitive_snapshot")
        if comp and isinstance(comp, dict):
            return comp
    return {}


def _get_dentist_profile(lead: Dict[str, Any]) -> Dict[str, Any]:
    """dentist_profile_v1 from lead or internal_debug."""
    profile = lead.get("dentist_profile_v1")
    if profile and isinstance(profile, dict):
        return profile
    debug = lead.get("internal_debug")
    if debug and isinstance(debug, dict):
        return debug.get("dentist_profile_v1") or {}
    return {}


def _snake_to_title(s: str) -> str:
    """snake_case -> Title Case."""
    if not s:
        return s
    return " ".join(w.capitalize() for w in s.split("_"))


def _fmt_currency(val: Any) -> str:
    """Format number as $X.XM or $Xk."""
    if val is None:
        return ""
    try:
        n = int(float(val))
        if n >= 1_000_000:
            return f"${n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"${n / 1_000:.0f}k"
        return f"${n:,}"
    except (TypeError, ValueError):
        return str(val)


def _revenue_confidence_label(score: Any) -> str:
    try:
        value = int(float(score))
    except (TypeError, ValueError):
        value = 0
    if value >= 70:
        return "High"
    if value >= 40:
        return "Medium"
    return "Low"


def _build_modeled_upside_display(
    rev: Dict[str, Any],
    *,
    annual_low: Optional[int],
    annual_high: Optional[int],
    service_label: Optional[str],
    source: str,
) -> Dict[str, Any]:
    confidence_score = int(rev.get("revenue_confidence_score") or 0)
    reliability_grade = str(rev.get("revenue_reliability_grade") or "C").strip().upper() or "C"
    indicative_only = bool(rev.get("revenue_indicative_only"))
    traffic_tier = str(rev.get("traffic_estimate_tier") or "").strip()
    confidence_label = _revenue_confidence_label(confidence_score)
    service_context = f"{service_label} capture gap" if service_label else "Modeled capture gap"

    if annual_low is None or annual_high is None:
        display_mode = "suppressed"
    elif not indicative_only and reliability_grade in {"A", "B"} and confidence_score >= 70:
        display_mode = "range"
    else:
        display_mode = "indicative"

    basis_parts: List[str] = []
    if service_label:
        basis_parts.append(f"{service_label.lower()} gap")
    if source == "ga4_calibrated":
        basis_parts.append("GA4-calibrated conversion proxy")
    else:
        if traffic_tier:
            basis_parts.append(f"{traffic_tier.lower()} traffic tier")
        else:
            basis_parts.append("traffic proxy")
        basis_parts.append("service-value assumptions")

    return {
        "display_mode": display_mode,
        "display_value": f"${annual_low:,}–${annual_high:,} annually" if display_mode == "range" and annual_low is not None and annual_high is not None else None,
        "confidence_score": confidence_score,
        "confidence_label": confidence_label,
        "reliability_grade": reliability_grade,
        "basis": f"Modeled from {', '.join(basis_parts)}." if basis_parts else "Modeled from public proxy signals.",
        "context": "Directional estimate only — not a booked revenue forecast.",
        "suppressed_reason": (
            "Numeric range withheld because the estimate is still proxy-based or confidence is limited."
            if display_mode != "range"
            else None
        ),
        "service_context": service_context,
    }


def _normalize_to_canonical_services(
    high_ticket: List[str], missing: List[str]
) -> Tuple[List[str], List[str]]:
    """
    Map raw procedure/page names to canonical buckets. Returns (detected_canonical, missing_canonical)
    in priority order: Implants, Orthodontics, Veneers, Emergency, Cosmetic.
    """
    def match_bucket(raw: str) -> Optional[str]:
        r = (raw or "").strip().lower()
        for canonical_key, aliases in CANONICAL_SERVICE_BUCKETS.items():
            if r in aliases or any(r == a for a in aliases):
                return canonical_key
        return None

    detected_set = set()
    for item in high_ticket or []:
        if not isinstance(item, str):
            continue
        b = match_bucket(item)
        if b:
            detected_set.add(b)

    missing_set = set()
    for item in missing or []:
        if not isinstance(item, str):
            continue
        b = match_bucket(item)
        if b:
            missing_set.add(b)

    order_keys = ["implants", "orthodontics", "veneers", "emergency", "cosmetic", "sedation", "crowns", "sleep apnea"]
    display_map = {
        "implants": "Implants", "orthodontics": "Orthodontics", "veneers": "Veneers",
        "emergency": "Emergency", "cosmetic": "Cosmetic", "sedation": "Sedation",
        "crowns": "Crowns", "sleep apnea": "Sleep Apnea",
    }
    detected_list = [display_map[k] for k in order_keys if k in detected_set]
    missing_list = [display_map[k] for k in order_keys if k in missing_set]
    return (detected_list, missing_list)


def _dedupe_risks_preserve_order(
    cost_leakage: List[str], primary_risks: List[str], risk_flags: List[str]
) -> List[str]:
    """Deduplicate risk statements; order: cost_leakage, then primary_risks, then risk_flags."""
    seen_lower = set()
    out = []
    for src in (cost_leakage or [], primary_risks or [], risk_flags or []):
        for s in (src if isinstance(src, list) else []):
            if not s or not isinstance(s, str):
                continue
            key = s.lower().strip()[:80]
            if key in seen_lower:
                continue
            seen_lower.add(key)
            out.append(s.strip())
    return out


def _flatten_supporting_evidence(s60: Optional[Dict[str, Any]], max_items: int = 5) -> List[str]:
    """Flatten summary_60s.supporting_evidence into a list of strings (max_items)."""
    if not s60:
        return []
    evidence = s60.get("supporting_evidence")
    if not evidence or not isinstance(evidence, dict):
        return []
    out = []
    for key in ("reputation_signals", "market_signals", "digital_signals", "traffic_signals", "revenue_signals"):
        arr = evidence.get(key)
        if isinstance(arr, list):
            for x in arr:
                if isinstance(x, str) and x.strip():
                    out.append(x.strip())
                    if len(out) >= max_items:
                        return out[:max_items]
    return out[:max_items]


def _suppressed_evidence_line(
    line: str,
    suppress_service_gap: bool,
    suppress_conversion_absence_claims: bool,
) -> bool:
    text = (line or "").strip().lower()
    if not text:
        return True
    if suppress_conversion_absence_claims and (
        "no contact form" in text
        or "phone-only" in text
        or "phone only" in text
        or "no booking" in text
        or "no online booking" in text
    ):
        return True
    if suppress_service_gap and (
        "missing dedicated" in text
        or "missing service" in text
        or "missing page" in text
    ):
        return True
    return False


def _dedupe_evidence(bullets: List[str], max_items: int = 10) -> List[str]:
    """Deduplicate evidence bullets by normalized text; preserve order."""
    seen = set()
    out = []
    for b in bullets:
        if not b or not isinstance(b, str):
            continue
        n = b.strip().lower()[:100]
        if n in seen:
            continue
        seen.add(n)
        out.append(b.strip())
        if len(out) >= max_items:
            break
    return out


def build_revenue_brief_view_model(lead: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build view model from lead. Source priority:
    1) objective_intelligence, revenue_intelligence, competitive_snapshot, signals
    2) objective_decision_layer, agency_decision_v1.summary_60s, canonical_summary_v1
    Missing data => omit subsection or field; no fabrication.
    """
    oi = _get_objective_intelligence(lead)
    s60 = _get_summary_60s(lead)
    rev = _get_revenue_intelligence(lead)
    signals = _get_signals(lead)
    obj = _get_objective_layer(lead)
    comp = _get_competitive_snapshot(lead)
    profile = _get_dentist_profile(lead)

    vm = {
        "executive_diagnosis": {},
        "market_position": {},
        "competitive_context": {},
        "competitive_service_gap": None,
        "strategic_gap": None,
        "revenue_upside_capture_gap": None,
        "demand_signals": {},
        "high_ticket_gaps": {},
        "service_page_analysis": None,
        "conversion_infrastructure": {},
        "conversion_structure": {},
        "competitive_delta": None,
        "market_saturation": None,
        "review_intelligence": None,
        "geo_coverage": None,
        "authority_proxy": None,
        "risk_flags": [],
        "intervention_plan": [],
        "intervention_fallback": None,
        "evidence_bullets": [],
        "executive_footnote": "",
    }

    # Resolve organic_revenue_gap (priority: rev > s60)
    gap = rev.get("organic_revenue_gap_estimate")
    if not gap or not isinstance(gap, dict):
        gap = (s60 or {}).get("organic_revenue_gap_estimate")
    gap_lo = gap.get("lower") if isinstance(gap, dict) else None
    gap_hi = gap.get("upper") if isinstance(gap, dict) else None

    # ---------- 1) Executive Diagnosis (prefer objective_intelligence) ----------
    rc_oi = (oi.get("root_constraint") or {}) if oi else {}
    constraint = (rc_oi.get("label") or "").strip() if isinstance(rc_oi, dict) else None
    if not constraint:
        rbc = (obj.get("root_bottleneck_classification") or {}) if obj else {}
        bottleneck_raw = rbc.get("bottleneck")
        if bottleneck_raw and isinstance(bottleneck_raw, str):
            constraint = BOTTLENECK_TO_LABEL.get(bottleneck_raw) or _snake_to_title(bottleneck_raw)
        else:
            constraint = (s60 or {}).get("root_constraint")
    if constraint:
        vm["executive_diagnosis"]["constraint"] = constraint

    primary_leverage = None
    pgv = (oi.get("primary_growth_vector") or {}) if oi else {}
    if isinstance(pgv, dict) and (pgv.get("label") or "").strip():
        primary_leverage = (pgv.get("label") or "").strip()
    if not primary_leverage:
        rbc = (obj.get("root_bottleneck_classification") or {}) if obj else {}
        bottleneck_raw = rbc.get("bottleneck")
        leverage_type = BOTTLENECK_TO_LEVERAGE.get(bottleneck_raw, "High-ticket capture") if bottleneck_raw else None
        rla = (obj.get("revenue_leverage_analysis") or {}) if obj else {}
        driver_detected = rla.get("primary_revenue_driver_detected")
        driver_label = _snake_to_title(driver_detected) if isinstance(driver_detected, str) else (s60 or {}).get("primary_revenue_driver")
        if leverage_type:
            primary_leverage = leverage_type
            if driver_label:
                primary_leverage = f"{leverage_type} ({driver_label})"
    if primary_leverage:
        vm["executive_diagnosis"]["primary_leverage"] = primary_leverage

    opp = compute_opportunity_profile(lead)
    if opp and opp.get("label") and opp.get("why"):
        vm["executive_diagnosis"]["opportunity_profile"] = opp

    if gap_lo is not None and gap_hi is not None:
        vm["executive_diagnosis"]["modeled_revenue_upside"] = f"{_fmt_currency(gap_lo)}–{_fmt_currency(gap_hi)} annually"

    # Clarify when rating strength and review-volume position diverge.
    try:
        lead_reviews_num = float(signals.get("signal_review_count") or signals.get("user_ratings_total") or 0)
        avg_reviews_num = float(comp.get("avg_review_count") or 0)
    except (TypeError, ValueError):
        lead_reviews_num = 0.0
        avg_reviews_num = 0.0
    try:
        lead_rating_num = float(signals.get("signal_rating") or signals.get("rating")) if (signals.get("signal_rating") or signals.get("rating")) is not None else None
        avg_rating_num = float(comp.get("avg_rating")) if comp.get("avg_rating") is not None else None
    except (TypeError, ValueError):
        lead_rating_num = None
        avg_rating_num = None
    if avg_reviews_num > 0 and lead_reviews_num < avg_reviews_num and lead_rating_num is not None:
        if avg_rating_num is None or lead_rating_num >= avg_rating_num:
            vm["executive_diagnosis"]["constraint"] = (
                "Strong rating but review volume below local market; growth depends on better capture and trust infrastructure."
            )

    vm["executive_footnote"] = "Modeled from public proxy signals; not GA4/Ads platform."

    # ---------- 2) Market Position ----------
    band = rev.get("revenue_band_estimate") or (s60 or {}).get("revenue_band")
    if isinstance(band, dict) and band.get("lower") is not None and band.get("upper") is not None:
        vm["market_position"]["revenue_band"] = f"{_fmt_currency(band['lower'])}–{_fmt_currency(band['upper'])}"
        vm["market_position"]["revenue_band_method"] = (
            "From: review volume, rating, service mix, and local market benchmarks (no GA4)."
        )
    rc = signals.get("signal_review_count") or signals.get("user_ratings_total")
    rt = signals.get("signal_rating") or signals.get("rating")
    if rc is not None:
        vm["market_position"]["reviews"] = f"{rc}" + (f" ({rt})" if rt is not None else "")
    avg = comp.get("avg_review_count")
    if avg is not None:
        try:
            vm["market_position"]["local_avg"] = f"{float(avg):.0f}"
        except (TypeError, ValueError):
            vm["market_position"]["local_avg"] = str(avg)
    density = comp.get("market_density_score")
    if density:
        vm["market_position"]["market_density"] = str(density)

    # ---------- 2b) Competitive Context (local visibility, competitors) ----------
    cp = (oi.get("competitive_profile") or {}) if oi else {}
    if cp.get("dentists_sampled") or comp.get("dentists_sampled"):
        n = int(cp.get("dentists_sampled") or comp.get("dentists_sampled") or 0)
        radius = int(cp.get("radius_used_miles") or comp.get("search_radius_used_miles") or 2)
        avg_reviews = cp.get("avg_reviews") if (cp.get("avg_reviews") is not None) else comp.get("avg_review_count")
        avg_rating_val = cp.get("avg_rating") if (cp.get("avg_rating") is not None and cp.get("avg_rating") != 0) else comp.get("avg_rating")
        market_density = (cp.get("market_density") or comp.get("market_density_score") or "—").strip()
        lead_reviews = int(comp.get("lead_review_count") or rc or 0)
        lead_rating = rt
        review_tier = (cp.get("review_tier") or comp.get("review_positioning_tier") or "").strip()
        computed_tier = _review_volume_tier(lead_reviews, float(avg_reviews) if avg_reviews is not None else None)
        if not review_tier:
            review_tier = computed_tier or "—"
        review_label = _review_volume_label_from_tier(review_tier)
        nearest = cp.get("nearest_competitors") or (comp.get("competitor_summary") or {}).get("nearest_competitors") or []
        if n or radius is not None:
            vm["competitive_context"]["line1"] = (
                f"{n} practices sampled within {radius} mi · "
                + (f"Avg reviews {float(avg_reviews):.0f}" if avg_reviews is not None else "Avg reviews —")
                + " · "
                + (f"Avg rating {float(avg_rating_val):.1f}" if avg_rating_val is not None and avg_rating_val != 0 else "Avg rating —")
                + f" · Density {market_density}."
            )
        vm["competitive_context"]["line2"] = (
            f"Lead: {lead_reviews} reviews" + (f" ({lead_rating})" if lead_rating is not None else "") + f" · Review volume: {review_label} · Tier: {review_tier}"
        )
        if nearest:
            parts = []
            for c in nearest[:3]:
                name = (c.get("name") or "").strip() or "—"
                revs = c.get("reviews") or c.get("user_ratings_total") or 0
                dist = c.get("distance_miles")
                if dist is not None:
                    parts.append(f"{name} — {revs} reviews — {_format_miles(dist)}")
                else:
                    parts.append(f"{name} — {revs} reviews")
            if parts:
                vm["competitive_context"]["line3"] = "Nearest competitors:"
                vm["competitive_context"]["line3_items"] = parts
        top5 = comp.get("top_5_avg_reviews")
        median = comp.get("competitor_median_reviews")
        gap_med = comp.get("target_gap_from_median")
        if top5 is not None or median is not None or gap_med is not None:
            gap_txt = None
            if gap_med is not None:
                try:
                    g = float(gap_med)
                    gap_txt = f"{abs(g):.0f} {'below' if g < 0 else 'above'} median"
                except (TypeError, ValueError):
                    gap_txt = str(gap_med)
            vm["market_saturation"] = {
                "top_5_avg_reviews": top5,
                "competitor_median_reviews": median,
                "target_gap_from_median": gap_txt,
                "competitors_with_website_checked": comp.get("competitors_with_website_checked"),
                "pct_competitors_with_blog": comp.get("pct_competitors_with_blog"),
                "pct_competitors_with_booking": comp.get("pct_competitors_with_booking"),
            }

    # ---------- 2c) Competitive Service Gap (objective_intelligence.competitive_service_gap) ----------
    gap_block = oi.get("competitive_service_gap") if oi else None
    if gap_block and isinstance(gap_block, dict) and gap_block.get("service"):
        vm["competitive_service_gap"] = gap_block

    # ---------- 2d) Strategic Gap (objective_intelligence.strategic_gap) ----------
    strat = oi.get("strategic_gap") if oi else None
    if strat and isinstance(strat, dict) and strat.get("service"):
        vm["strategic_gap"] = strat
    if isinstance(lead.get("competitive_delta"), dict):
        vm["competitive_delta"] = lead.get("competitive_delta")

    # ---------- 3) Demand Signals (minimal, factual, deterministic) ----------
    channels = signals.get("signal_paid_ads_channels") or []
    if not isinstance(channels, list):
        channels = [channels] if channels else []
    channels_lower = [str(c).strip().lower() for c in channels if c]
    runs_google = signals.get("signal_runs_paid_ads") is True and "google" in channels_lower
    runs_meta = "meta" in channels_lower

    google_ads_line = "Not detected"
    gads_api = lead.get("google_ads_api_data")
    if gads_api and gads_api.get("total_spend_usd", 0) > 0:
        spend = gads_api["total_spend_usd"]
        clicks = gads_api.get("total_clicks", 0)
        google_ads_line = f"Active (${spend:,.0f}/mo · {clicks:,} clicks)"
        runs_google = True
    elif runs_google:
        gads_check = signals.get("google_ads_check") or {}
        gads_conv_ids = gads_check.get("google_ads_conversion_ids") or []
        gads_confidence = gads_check.get("confidence", "")
        if gads_conv_ids:
            google_ads_line = "Active (Conversion tracking confirmed)"
        elif gads_confidence == "high":
            google_ads_line = "Active (Multiple tracking signals)"
        else:
            google_ads_line = "Active (Paid spend detected)"
    vm["demand_signals"]["google_ads_line"] = google_ads_line
    vm["demand_signals"]["meta_ads_line"] = "Active" if runs_meta else "Not detected"
    if runs_google:
        vm["demand_signals"]["google_ads_source"] = "Google Ads Transparency Center."
    if runs_meta:
        vm["demand_signals"]["meta_ads_source"] = "Meta Ad Library."
    if channels:
        vm["demand_signals"]["paid_channels_detected"] = [str(c) for c in channels if c]
    visibility = compute_organic_visibility(lead)
    if visibility:
        vm["demand_signals"]["organic_visibility_tier"] = visibility.get("tier", "")
        vm["demand_signals"]["organic_visibility_reason"] = visibility.get("reason", "")
    last_rev = lead.get("days_since_last_review")
    if last_rev is None:
        last_rev = signals.get("signal_last_review_days_ago")
    if last_rev is not None:
        vm["demand_signals"]["last_review_days_ago"] = last_rev
        vm["demand_signals"]["last_review_estimated"] = True
    real_vel = lead.get("signal_real_review_velocity_30d")
    if real_vel is not None:
        vm["demand_signals"]["review_velocity_30d"] = real_vel
        vm["demand_signals"]["review_velocity_estimated"] = False
    else:
        vel = lead.get("review_velocity_last_30_days")
        if vel is None:
            vel = signals.get("signal_review_velocity_30d")
        if vel is not None:
            vm["demand_signals"]["review_velocity_30d"] = vel
            vm["demand_signals"]["review_velocity_estimated"] = True

    # ---------- 4) High-Ticket Capture Gaps (canonical normalization) ----------
    svc = normalize_service_intelligence(obj.get("service_intelligence") or {})
    suppress_service_gap = bool(svc.get("suppress_service_gap"))
    suppress_conversion_absence_claims = bool(svc.get("suppress_conversion_absence_claims") or suppress_service_gap)
    high_ticket_raw = svc.get("high_ticket_procedures_detected")
    missing_raw = svc.get("missing_high_value_pages")
    detected_canonical, missing_canonical = _normalize_to_canonical_services(
        high_ticket_raw if isinstance(high_ticket_raw, list) else [],
        missing_raw if isinstance(missing_raw, list) else [],
    )
    if detected_canonical:
        vm["high_ticket_gaps"]["high_ticket_services_detected"] = detected_canonical
    if missing_canonical and not suppress_service_gap:
        vm["high_ticket_gaps"]["missing_landing_pages"] = missing_canonical
    if svc.get("service_level_upside") and isinstance(svc["service_level_upside"], list):
        vm["high_ticket_gaps"]["service_level_upside"] = svc["service_level_upside"]
    else:
        vm["high_ticket_gaps"]["service_level_upside_available"] = False
    if isinstance(svc.get("high_value_services"), list):
        vm["service_page_analysis"] = {
            "services": svc.get("high_value_services") or [],
            "summary": svc.get("high_value_summary") or {},
            "leverage": svc.get("high_value_service_leverage"),
            "v2": svc.get("service_page_analysis_v2") or {},
        }

    # ---------- 4b) Modeled Revenue Upside — {Primary Service} Capture Gap (deterministic, dental) ----------
    missing_from_oi = (oi.get("service_intel") or {}).get("missing_high_value_pages") if oi else []
    missing_list = list(missing_from_oi) if isinstance(missing_from_oi, list) and missing_from_oi else (list(missing_raw) if isinstance(missing_raw, list) else [])
    preferred_from_gap = _service_key_from_text((vm.get("competitive_service_gap") or {}).get("service"))
    if preferred_from_gap:
        gap_key, _ = preferred_from_gap
        missing_keys = {
            mk[0]
            for mk in (_service_key_from_text(x) for x in missing_list)
            if mk
        }
        detected_keys = {
            dk[0]
            for dk in (_service_key_from_text(x) for x in detected_canonical)
            if dk
        }
        if not any(_service_key_equivalent(gap_key, k) for k in (missing_keys | detected_keys)):
            preferred_from_gap = None
    if not suppress_service_gap and (missing_list or preferred_from_gap) and rev:
        # Primary modeled service should come from missing-page priority (Implants > ...),
        # not from the competitor gap label when multiple gaps exist.
        primary = _primary_service_from_missing(missing_list) or preferred_from_gap
        if primary:
            key, display_name = primary
            tier = (rev.get("traffic_estimate_tier") or "").strip()
            consult_low, consult_high = TRAFFIC_TIER_CONSULTS.get(tier, DEFAULT_CONSULTS)
            case_low, case_high = PRIMARY_SERVICE_CASE_VALUE.get(key, DEFAULT_CASE_VALUE)
            annual_low = round((consult_low * case_low * 12) / 1000) * 1000
            annual_high = round((consult_high * case_high * 12) / 1000) * 1000

            # Cap annual_high at 30% of revenue band upper; scale annual_low proportionally
            band = rev.get("revenue_band_estimate")
            if isinstance(band, dict) and band.get("upper") is not None:
                try:
                    revenue_upper = float(band["upper"])
                    max_allowed = revenue_upper * 0.30
                    if annual_high > max_allowed:
                        scale = max_allowed / annual_high if annual_high > 0 else 1.0
                        annual_low = round((annual_low * scale) / 1000) * 1000
                        annual_high = round(max_allowed / 1000) * 1000
                except (TypeError, ValueError):
                    pass

            annual_low = min(annual_low, annual_high)  # ensure annual_low <= annual_high

            upside_source = "proxy_model"
            ga4_data = lead.get("ga4_data")
            if ga4_data and ga4_data.get("total_sessions", 0) > 0:
                ga4_conversions = sum(ga4_data.get("conversions", {}).values())
                ga4_sessions = ga4_data["total_sessions"]
                if ga4_sessions > 100 and ga4_conversions > 0:
                    conv_rate = ga4_conversions / ga4_sessions
                    estimated_additional = int(ga4_sessions * 0.15 * conv_rate * 12)
                    if estimated_additional > 0:
                        annual_low = estimated_additional * case_low
                        annual_high = estimated_additional * case_high
                        consult_low = max(1, estimated_additional // 12)
                        consult_high = max(consult_low + 1, int(estimated_additional * 1.3) // 12)
                        upside_source = "ga4_calibrated"

            vm["revenue_upside_capture_gap"] = {
                "primary_service": display_name,
                "consult_low": consult_low,
                "consult_high": consult_high,
                "case_low": case_low,
                "case_high": case_high,
                "annual_low": annual_low,
                "annual_high": annual_high,
                "source": upside_source,
                "method_note": "From: service gaps, traffic proxy, and conversion assumptions (see disclaimer).",
            }
            vm["revenue_upside_capture_gap"].update(
                _build_modeled_upside_display(
                    rev,
                    annual_low=annual_low,
                    annual_high=annual_high,
                    service_label=display_name,
                    source=upside_source,
                )
            )
            gap_service_raw = (vm.get("competitive_service_gap") or {}).get("service")
            if gap_service_raw and str(gap_service_raw).strip().lower() != display_name.strip().lower():
                vm["revenue_upside_capture_gap"]["gap_service"] = str(gap_service_raw).strip()
            # Keep executive upside aligned to the same service-specific range shown below.
            vm["executive_diagnosis"]["modeled_revenue_upside"] = (
                f"${annual_low:,}–${annual_high:,} annually ({display_name} capture gap)"
            )

    # ---------- 5) Conversion Infrastructure ----------
    crawl_conf = str(svc.get("crawl_confidence") or "").strip().lower()
    low_crawl = crawl_conf == "low"
    vm["conversion_infrastructure"]["online_booking"] = signals.get("signal_has_automated_scheduling")
    vm["conversion_infrastructure"]["contact_form"] = True if svc.get("contact_form_detected_sitewide") else signals.get("signal_has_contact_form")
    vm["conversion_infrastructure"]["booking_flow_type"] = signals.get("signal_booking_flow_type")
    vm["conversion_infrastructure"]["booking_flow_confidence"] = signals.get("signal_booking_flow_confidence")
    vm["conversion_infrastructure"]["scheduling_cta_detected"] = signals.get("signal_scheduling_cta_detected")
    vm["conversion_infrastructure"]["contact_form_confidence"] = "high" if svc.get("contact_form_detected_sitewide") else signals.get("signal_contact_form_confidence")
    vm["conversion_infrastructure"]["contact_form_cta_detected"] = signals.get("signal_contact_form_cta_detected")
    vm["conversion_infrastructure"]["capture_verification"] = signals.get("signal_capture_verification")
    vm["conversion_infrastructure"]["phone_prominent"] = signals.get("signal_has_phone") is True
    vm["conversion_infrastructure"]["mobile_optimized"] = signals.get("signal_mobile_friendly") is True
    page_load = signals.get("signal_page_load_time_ms")
    if page_load is not None:
        vm["conversion_infrastructure"]["page_load_ms"] = page_load
    vm["conversion_infrastructure"] = normalize_conversion_infrastructure(
        vm["conversion_infrastructure"],
        service_intel=svc,
        signals=signals,
    )
    vm["conversion_structure"] = {
        "phone_clickable": signals.get("signal_phone_clickable"),
        "cta_count": signals.get("signal_cta_count"),
        "form_single_or_multi_step": signals.get("signal_form_single_or_multi_step"),
    }
    if low_crawl:
        vm["conversion_structure"]["evaluation_note"] = "Not Evaluated (Low Crawl Confidence)"

    review_intel = lead.get("review_intelligence") or signals.get("signal_review_intelligence")
    if isinstance(review_intel, dict):
        vm["review_intelligence"] = {
            "review_sample_size": review_intel.get("review_sample_size"),
            "summary": review_intel.get("review_summary"),
            "service_mentions": review_intel.get("service_mentions") or {},
            "complaint_themes": review_intel.get("complaint_themes") or {},
        }

    city_pages = svc.get("city_or_near_me_page_count")
    has_multi_loc = svc.get("has_multi_location_page")
    geo_examples = svc.get("geo_page_examples") or []
    if city_pages is not None or has_multi_loc is not None:
        vm["geo_coverage"] = {
            "city_or_near_me_page_count": city_pages,
            "has_multi_location_page": has_multi_loc,
            "geo_page_examples": geo_examples[:3],
        }

    if isinstance(lead.get("authority_proxy"), dict):
        vm["authority_proxy"] = lead.get("authority_proxy")

    # ---------- 6) Risk Flags (prefer objective_intelligence.cost_leakage_signals, then rev, s60) ----------
    cost_leakage = (oi.get("cost_leakage_signals") or []) if oi else []
    if not cost_leakage:
        cost_leakage = rev.get("cost_leakage_signals")
    if not cost_leakage and s60:
        cost_leakage = s60.get("cost_leakage_signals")
    primary_risks = lead.get("primary_risks") or []
    agency_fit = profile.get("agency_fit_reasoning") or {}
    risk_flags = agency_fit.get("risk_flags") or []
    merged_risks = _dedupe_risks_preserve_order(cost_leakage or [], primary_risks, risk_flags)
    vm["risk_flags"] = _refine_risk_flags(
        merged_risks,
        detected_canonical,
        missing_canonical,
        paid_active=bool(runs_google or runs_meta),
        suppress_service_gap=suppress_service_gap,
        suppress_conversion_absence_claims=suppress_conversion_absence_claims,
    )

    # ---------- 7) Intervention Plan (prefer objective_intelligence.intervention_plan; max 3 steps) ----------
    plan_oi = (oi.get("intervention_plan") or []) if oi else []
    steps = []
    if plan_oi and isinstance(plan_oi, list):
        for item in plan_oi[:3]:
            if not isinstance(item, dict):
                continue
            step_num = item.get("step")
            cat = item.get("category") or "Capture"
            action = (item.get("action") or "").strip()
            if action:
                steps.append(f"Step {step_num} — {cat}: {action}")
    if not steps:
        plan = obj.get("intervention_plan") if obj else None
        if plan and isinstance(plan, list) and len(plan) > 0:
            for item in plan:
                if not isinstance(item, dict):
                    continue
                prio = item.get("priority")
                cat = item.get("category")
                action = item.get("action")
                if action:
                    steps.append(f"Step {prio} — {cat}: {action}")
    if steps:
        vm["intervention_plan"] = steps
    else:
        # Fallback: Strategic Frame + Tactical Levers
        primary_lev = vm["executive_diagnosis"].get("primary_leverage") or "High-ticket capture"
        tactical_parts = []
        if missing_canonical:
            tactical_parts.append("missing pages: " + ", ".join(missing_canonical))
        if not tactical_parts:
            tactical_parts.append("Assess service page coverage and conversion capture.")
        vm["intervention_fallback"] = {
            "strategic_frame": f"Increase revenue via {primary_lev}.",
            "tactical_levers": " ".join(tactical_parts) if tactical_parts else "—",
        }

    # ---------- 8) Evidence (collapsible; prefer objective_intelligence.root_constraint.evidence) ----------
    evidence_list = []
    canonical_review_line = None
    if rc is not None and avg is not None:
        try:
            rc_num = float(rc)
            avg_num = float(avg)
        except (TypeError, ValueError):
            rc_num = None
            avg_num = None
        tier = _review_volume_tier(rc_num, avg_num) if rc_num is not None and avg_num is not None else None
        label = _review_volume_label_from_tier(tier) if tier else "Market comparison"
        canonical_review_line = f"Review volume vs market: {label} ({rc} vs local avg {avg})"
        evidence_list.append(canonical_review_line)
    if rt is not None and comp.get("avg_rating") is not None:
        try:
            rating_delta = float(rt) - float(comp.get("avg_rating"))
            rating_strength = "Strong" if rating_delta >= 0 else "Below market"
            evidence_list.append(f"Rating strength vs market: {rating_strength} ({rt} vs local avg {float(comp.get('avg_rating')):.1f})")
        except (TypeError, ValueError):
            pass
    pages_crawled = svc.get("pages_crawled")
    if pages_crawled is not None:
        evidence_list.append(f"Pages crawled: {pages_crawled} (sitemap + nav).")
    service_page_count = svc.get("service_page_count")
    if service_page_count is not None:
        evidence_list.append(f"Service pages: {service_page_count} (path/keyword match to service buckets).")
    faq_count = svc.get("service_pages_with_faq_or_schema")
    if faq_count is not None and service_page_count is not None and faq_count > 0:
        evidence_list.append(f"FAQ sections: {faq_count} of {service_page_count} service pages.")
    phone_clickable = signals.get("signal_phone_clickable")
    if phone_clickable is not None:
        evidence_list.append(f"Phone clickable on homepage (tel:): {'Yes' if phone_clickable else 'No'}.")
    cta_count = signals.get("signal_cta_count")
    if cta_count is not None:
        evidence_list.append(f"CTA elements detected: {cta_count} (Book/Schedule/Contact/Call patterns).")
    form_mode = signals.get("signal_form_single_or_multi_step")
    if form_mode:
        evidence_list.append(f"Form structure detected: {form_mode}.")
    geo_count = svc.get("city_or_near_me_page_count")
    if geo_count is not None:
        evidence_list.append(f"City/near-me pages: {geo_count} (URL/title contains city or 'near me').")
    dentists_sampled = comp.get("dentists_sampled")
    radius = comp.get("search_radius_used_miles")
    if dentists_sampled is not None:
        radius_txt = f" within {radius} mi" if radius is not None else ""
        evidence_list.append(f"Competitors sampled: {dentists_sampled}{radius_txt} (review counts from Places API).")
    evidence_from_oi = (rc_oi.get("evidence") or []) if isinstance(rc_oi.get("evidence"), list) else []
    for e in evidence_from_oi[:5]:
        if isinstance(e, str) and e.strip():
            e_clean = e.strip()
            e_lower = e_clean.lower()
            if canonical_review_line and e_lower.startswith("review count vs market:"):
                continue
            if _suppressed_evidence_line(e_clean, suppress_service_gap, suppress_conversion_absence_claims):
                continue
            evidence_list.append(e_clean)
    if not evidence_from_oi:
        rbc = (obj.get("root_bottleneck_classification") or {}) if obj else {}
        rbc_evidence = (rbc.get("evidence") or []) if isinstance(rbc.get("evidence"), list) else []
        for e in rbc_evidence[:5]:
            if isinstance(e, str) and e.strip():
                e_clean = e.strip()
                e_lower = e_clean.lower()
                if canonical_review_line and e_lower.startswith("review count vs market:"):
                    continue
                if _suppressed_evidence_line(e_clean, suppress_service_gap, suppress_conversion_absence_claims):
                    continue
                evidence_list.append(e_clean)
    for e in _flatten_supporting_evidence(s60, max_items=5):
        if _suppressed_evidence_line(e, suppress_service_gap, suppress_conversion_absence_claims):
            continue
        evidence_list.append(e)
    if suppress_conversion_absence_claims:
        evidence_list.append("Conversion infrastructure not fully evaluated (limited crawl depth).")
    if suppress_service_gap:
        evidence_list.append("Service visibility not fully evaluated (limited crawl depth).")
    vm["evidence_bullets"] = _dedupe_evidence(evidence_list, max_items=20)

    return vm


def render_revenue_brief_html(lead: Dict[str, Any], title: str = "Revenue Intelligence Brief") -> str:
    """Render the Revenue Intelligence Brief as HTML. Deterministic; no LLM."""
    vm = build_revenue_brief_view_model(lead)
    sections = []

    # 1) Executive Diagnosis
    ed = vm.get("executive_diagnosis") or {}
    footnote = vm.get("executive_footnote") or ""
    if ed or footnote:
        parts = []
        if ed.get("constraint"):
            parts.append(f"<p><strong>Constraint:</strong> {_h(ed['constraint'])}</p>")
        if ed.get("primary_leverage"):
            parts.append(f"<p><strong>Primary Leverage:</strong> {_h(ed['primary_leverage'])}</p>")
        opp = ed.get("opportunity_profile")
        if opp and isinstance(opp, dict) and opp.get("label") and opp.get("why"):
            parts.append(f"<p><strong>Opportunity Profile:</strong> {_h(opp['label'])} <em>({_h(opp['why'])})</em></p>")
            drivers = opp.get("leverage_drivers") or {}
            if isinstance(drivers, dict) and drivers:
                parts.append(
                    "<p><small>Based on: "
                    + f"missing high-value pages {'✓' if drivers.get('missing_high_value_pages') else '✗'}, "
                    + f"high-density market {'✓' if drivers.get('market_density_high') else '✗'}, "
                    + f"paid ads active {'✓' if drivers.get('paid_active') else '✗'}, "
                    + f"review deficit (&lt;50% of local avg) {'✓' if drivers.get('review_deficit') else '✗'}, "
                    + f"structured trust weak {'✓' if drivers.get('structured_trust_weak') else '✗'}."
                    + "</small></p>"
                )
        if ed.get("modeled_revenue_upside"):
            parts.append(f"<p><strong>Modeled Revenue Upside:</strong> <strong>{_h(ed['modeled_revenue_upside'])}</strong></p>")
        if footnote:
            parts.append(f"<p class=\"brief-footnote\"><small>{_h(footnote)}</small></p>")
        if parts:
            sections.append(
                "<section class=\"brief-section brief-executive\">\n  <h2>Executive Diagnosis</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    # 2) Market Position
    mp = vm.get("market_position") or {}
    if mp:
        parts = []
        if mp.get("revenue_band"):
            parts.append(f"<p><strong>Revenue Band:</strong> {_h(mp['revenue_band'])}</p>")
        if mp.get("revenue_band_method"):
            parts.append(f"<p><small>{_h(mp['revenue_band_method'])}</small></p>")
        if mp.get("reviews"):
            parts.append(f"<p><strong>Reviews:</strong> {_h(mp['reviews'])}</p>")
        if mp.get("local_avg"):
            parts.append(f"<p><strong>Local Avg:</strong> {_h(mp['local_avg'])}</p>")
        if mp.get("market_density"):
            parts.append(f"<p><strong>Market Density:</strong> {_h(mp['market_density'])}</p>")
        if parts:
            sections.append(
                "<section class=\"brief-section brief-market\">\n  <h2>Market Position</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    # 2b) Competitive Context (practices sampled, lead vs competitors, nearest 3)
    cc = vm.get("competitive_context") or {}
    if cc.get("line1") or cc.get("line2") or cc.get("line3"):
        parts = []
        if cc.get("line1"):
            parts.append(f"<p>{_h(cc['line1'])}</p>")
        if cc.get("line2"):
            parts.append(f"<p>{_h(cc['line2'])}</p>")
        if cc.get("line3"):
            parts.append(f"<p>{_h(cc['line3'])}</p>")
            line3_items = cc.get("line3_items")
            if isinstance(line3_items, list) and line3_items:
                parts.append("<ul>" + "".join(f"<li>{_h(item)}</li>" for item in line3_items[:5]) + "</ul>")
        if parts:
            sections.append(
                "<section class=\"brief-section brief-competitive\">\n  <h2>Competitive Context</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    # 2c) Competitive Service Gap (high-margin capture gap vs nearest competitor)
    gap_block = vm.get("competitive_service_gap")
    if gap_block and isinstance(gap_block, dict):
        g_type = gap_block.get("type") or "High-Margin Capture Gap"
        g_service = gap_block.get("service") or "—"
        g_competitor = gap_block.get("competitor_name") or "—"
        g_comp_reviews = gap_block.get("competitor_reviews")
        g_lead_reviews = gap_block.get("lead_reviews")
        g_dist = gap_block.get("distance_miles")
        parts = [f"<p><strong>Type:</strong> {_h(g_type)}</p>", f"<p><strong>Service:</strong> {_h(g_service)}</p>", f"<p><strong>Nearest competitor:</strong> {_h(g_competitor)}"]
        if g_comp_reviews is not None:
            parts.append(f"<p><strong>Competitor reviews:</strong> {_h(g_comp_reviews)}</p>")
        if g_lead_reviews is not None:
            parts.append(f"<p><strong>Lead reviews:</strong> {_h(g_lead_reviews)}</p>")
        if g_dist is not None:
            parts.append(f"<p><strong>Distance:</strong> {_h(g_dist)} mi</p>")
        sections.append(
            "<section class=\"brief-section brief-service-gap\">\n  <h2>Competitive Service Gap</h2>\n  "
            + "\n  ".join(parts)
            + "\n</section>"
        )

    # 2e) Competitive Delta (target vs competitor averages when available)
    cd = vm.get("competitive_delta")
    if cd and isinstance(cd, dict):
        parts = []
        t_pages = cd.get("target_service_page_count")
        c_pages = cd.get("competitor_avg_service_pages")
        competitor_note = cd.get("competitor_crawl_note")
        competitor_site_metrics_count = cd.get("competitor_site_metrics_count")
        if c_pages is not None:
            parts.append(
                f"<p><strong>Service pages:</strong> {_h(t_pages)} pages with service-like paths "
                f"(for example, /implants, /cosmetic) vs competitor avg {float(c_pages):.1f}</p>"
            )
        else:
            parts.append(
                f"<p><strong>Service pages:</strong> Target {_h(t_pages)} pages with service-like paths "
                f"(e.g. /implants, /cosmetic).</p>"
            )
        t_faq = cd.get("target_pages_with_faq_schema")
        c_faq = cd.get("competitor_avg_pages_with_schema")
        if c_faq is not None and t_faq:
            parts.append(
                f"<p><strong>FAQ coverage:</strong> {_h(t_faq)} of {_h(t_pages)} service pages "
                f"vs competitor avg {float(c_faq):.1f} pages</p>"
            )
        elif t_faq is not None and t_faq > 0:
            parts.append(f"<p><strong>FAQ coverage:</strong> {_h(t_faq)} of {_h(t_pages)} service pages</p>")
        t_words = cd.get("target_avg_word_count_service_pages")
        t_min_words = cd.get("target_min_word_count_service_pages")
        t_max_words = cd.get("target_max_word_count_service_pages")
        c_words = cd.get("competitor_avg_word_count")
        if t_words is not None and c_words is not None:
            parts.append(
                f"<p><strong>Service page depth:</strong> Average word count across {_h(t_pages)} service pages: "
                f"~{int(t_words)} (min {_h(t_min_words) if t_min_words is not None else 'N/A'}, "
                f"max {_h(t_max_words) if t_max_words is not None else 'N/A'}) vs competitor avg ~{int(c_words)}</p>"
            )
        elif t_words is not None:
            parts.append(
                f"<p><strong>Service page depth:</strong> Average word count across {_h(t_pages)} service pages: "
                f"~{int(t_words)} (min {_h(t_min_words) if t_min_words is not None else 'N/A'}, "
                f"max {_h(t_max_words) if t_max_words is not None else 'N/A'})</p>"
            )
        sample_n = cd.get("competitors_sampled")
        if sample_n:
            parts.append(f"<p class=\"brief-footnote\"><small>Based on {int(sample_n)} nearby competitors.</small></p>")
            if competitor_site_metrics_count:
                parts.append(
                    f"<p class=\"brief-footnote\"><small>Competitor averages from {int(competitor_site_metrics_count)} competitor sites crawled.</small></p>"
                )
        else:
            parts.append("<p class=\"brief-footnote\"><small>Target-only competitive delta for this run.</small></p>")
        if c_pages is None:
            parts.append(
                f"<p class=\"brief-footnote\"><small>{_h(competitor_note or 'Competitor site metrics were not run for this brief; only target metrics are shown.')}</small></p>"
            )
        sections.append(
            "<section class=\"brief-section brief-competitive-delta\">\n  <h2>Competitive Delta</h2>\n  "
            + "\n  ".join(parts)
            + "\n</section>"
        )

    # 3) Demand Signals (minimal, factual; omit if all data missing)
    ds = vm.get("demand_signals") or {}
    ds_parts = []
    if ds.get("google_ads_line"):
        ds_parts.append(f"<p><strong>Google Ads:</strong> {_h(ds['google_ads_line'])}</p>")
        if ds.get("google_ads_source"):
            ds_parts.append(f"<p><small>Source: {_h(ds['google_ads_source'])}</small></p>")
    if ds.get("meta_ads_line"):
        ds_parts.append(f"<p><strong>Meta Ads:</strong> {_h(ds['meta_ads_line'])}</p>")
        if ds.get("meta_ads_source"):
            ds_parts.append(f"<p><small>Source: {_h(ds['meta_ads_source'])}</small></p>")
    if ds.get("paid_channels_detected"):
        ds_parts.append(
            f"<p><strong>Paid channels detected:</strong> {_h(', '.join(ds['paid_channels_detected']))}</p>"
        )
    if ds.get("paid_spend_method"):
        ds_parts.append(f"<p><small>{_h(ds['paid_spend_method'])}</small></p>")
    if ds.get("organic_visibility_tier"):
        vis_line = f"{_h(ds['organic_visibility_tier'])}"
        if ds.get("organic_visibility_reason"):
            vis_line += f" — {_h(ds['organic_visibility_reason'])}"
        ds_parts.append(f"<p><strong>Organic Visibility:</strong> {vis_line}</p>")
    if ds.get("last_review_days_ago") is not None:
        ds_parts.append(f"<p><strong>Last Review:</strong> ~{_h(ds['last_review_days_ago'])} days ago</p>")
    if ds.get("review_velocity_30d") is not None:
        ds_parts.append(f"<p><strong>Review Velocity:</strong> ~{_h(ds['review_velocity_30d'])} in last 30 days</p>")
    if ds_parts:
        sections.append(
            "<section class=\"brief-section brief-demand\">\n  <h2>Demand Signals</h2>\n  "
            + "\n  ".join(ds_parts)
            + "\n</section>"
        )

    # 3c) Review Intelligence (sample-size framed)
    ri = vm.get("review_intelligence")
    if ri and isinstance(ri, dict):
        n = ri.get("review_sample_size")
        summary = ri.get("summary")
        service_mentions = ri.get("service_mentions") or {}
        complaint_themes = ri.get("complaint_themes") or {}
        parts = []
        if n is not None:
            parts.append(
                f"<p><strong>Directional signal from {int(n)} sampled Google reviews:</strong> "
                f"{_h(summary) if summary else 'No strong qualitative pattern extracted.'}</p>"
            )
        elif summary:
            parts.append(f"<p>{_h(summary)}</p>")
        if isinstance(service_mentions, dict) and service_mentions:
            sm = ", ".join(f"{k}: {v}/{n}" if n else f"{k}: {v}" for k, v in list(service_mentions.items())[:6])
            parts.append(f"<p><strong>Most-mentioned services in sample:</strong> {_h(sm)}</p>")
        if isinstance(complaint_themes, dict) and complaint_themes:
            ct = ", ".join(f"{k}: {v}/{n}" if n else f"{k}: {v}" for k, v in list(complaint_themes.items())[:4])
            parts.append(f"<p><strong>Negative/friction mentions in sample:</strong> {_h(ct)}</p>")
        if n is not None:
            parts.append(
                f"<p><small>Source: Google Place Details ({int(n)} reviews max). "
                "Use this as directional voice-of-customer input, not a full review corpus.</small></p>"
            )
        if parts:
            sections.append(
                "<section class=\"brief-section brief-reviews\">\n  <h2>Review Intelligence</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    # 4) Local SEO & High-Value Service Pages (GBP, service pages; revenue only in Executive Diagnosis)
    ht = vm.get("high_ticket_gaps") or {}
    if ht:
        parts = []
        if ht.get("high_ticket_services_detected"):
            items = " ".join(f"<li>{_h(x)}</li>" for x in ht["high_ticket_services_detected"])
            parts.append(f"<p><strong>High-value services detected:</strong></p><ul>{items}</ul>")
        if ht.get("missing_landing_pages"):
            items = " ".join(f"<li>{_h(x)}</li>" for x in ht["missing_landing_pages"])
            parts.append(f"<p><strong>Missing service/landing pages:</strong></p><ul>{items}</ul>")
        if ht.get("service_level_upside"):
            items = " ".join(
                f"<li>{_h(x.get('service', x) if isinstance(x, dict) else x)}: {_h(x.get('upside', ''))}</li>"
                for x in ht["service_level_upside"][:10]
            )
            parts.append("<details><summary>View Modeled Upside by Service</summary><ul>" + items + "</ul></details>")
        if parts:
            sections.append(
                "<section class=\"brief-section brief-highticket\">\n  <h2>Local SEO & High-Value Service Pages</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    # 4a) Modeled Revenue Upside — {Primary Service} Capture Gap (only when missing pages + revenue_intelligence)
    rucg = vm.get("revenue_upside_capture_gap")
    if rucg and isinstance(rucg, dict) and rucg.get("primary_service"):
        svc_label = _h(rucg.get("primary_service") or "Service")
        c_lo = rucg.get("consult_low", 1)
        c_hi = rucg.get("consult_high", 4)
        case_lo = rucg.get("case_low", 2500)
        case_hi = rucg.get("case_high", 4000)
        a_lo = rucg.get("annual_low", 0)
        a_hi = rucg.get("annual_high", 0)
        parts = [
            f"<p>{c_lo}–{c_hi} additional consults/month</p>",
            f"<p>${case_lo:,}–${case_hi:,} per case</p>",
            f"<p><strong>${a_lo:,}–${a_hi:,} annually</strong></p>",
        ]
        if rucg.get("method_note"):
            parts.append(f"<p><small>{_h(rucg.get('method_note'))}</small></p>")
        if rucg.get("gap_service"):
            parts.append(f"<p><strong>Competitive gap service:</strong> {_h(rucg.get('gap_service'))}</p>")
        sections.append(
            "<section class=\"brief-section brief-revenue-capture-gap\">\n  "
            f"<h2>Modeled Revenue Upside — {svc_label} Capture Gap</h2>\n  "
            + "\n  ".join(parts)
            + "\n</section>"
        )

    # 4b) Strategic Gap Identified (only when objective_intelligence.strategic_gap exists)
    sg = vm.get("strategic_gap")
    if sg and isinstance(sg, dict) and sg.get("competitor_name"):
        cname = _h(sg.get("competitor_name") or "—")
        crev = sg.get("competitor_reviews")
        lrev = sg.get("lead_reviews")
        dist = sg.get("distance_miles")
        md = _h(sg.get("market_density") or "High")
        comparison = "relative to that competitor."
        try:
            cnum = int(crev) if crev is not None else None
            lnum = int(lrev) if lrev is not None else None
            if cnum is not None and lnum is not None:
                if lnum < cnum:
                    comparison = "below that competitor."
                elif lnum > cnum:
                    comparison = "above that competitor."
                else:
                    comparison = "in line with that competitor."
        except (TypeError, ValueError):
            pass
        parts = [
            f"<p>Nearest competitor {cname} is {_h(_format_miles(dist)) if dist is not None else '—'} away and has {_h(crev) if crev is not None else '—'} reviews.</p>",
            f"<p>Market density: {md}. This practice&apos;s review position is {comparison}</p>",
        ]
        sections.append(
            "<section class=\"brief-section brief-strategic-gap\">\n  <h2>Strategic Gap Identified</h2>\n  "
            + "\n  ".join(parts)
            + "\n</section>"
        )

    # 5) Conversion Infrastructure
    ci = vm.get("conversion_infrastructure") or {}
    if ci:
        parts = [
            f"<p><strong>Online Booking:</strong> {_yes_no_unknown(ci.get('online_booking'))}</p>",
            f"<p><strong>Contact Form:</strong> {_yes_no_unknown(ci.get('contact_form'))}</p>",
            f"<p><strong>Phone Prominent:</strong> {_yes_no_unknown(ci.get('phone_prominent'))}</p>",
            f"<p><strong>Mobile Optimized:</strong> {_yes_no_unknown(ci.get('mobile_optimized'))}</p>",
        ]
        if ci.get("page_load_ms") is not None:
            parts.append(f"<p><strong>Page Load:</strong> {_h(ci['page_load_ms'])} ms</p>")
        sections.append(
            "<section class=\"brief-section brief-conversion\">\n  <h2>Conversion Infrastructure</h2>\n  "
            + "\n  ".join(parts)
            + "\n</section>"
        )

    cs = vm.get("conversion_structure") or {}
    if cs and any(cs.get(k) is not None for k in ("phone_clickable", "cta_count", "form_single_or_multi_step")):
        parts = []
        if cs.get("phone_clickable") is not None:
            parts.append(
                f"<p><strong>Phone on homepage:</strong> {'tap-to-call (clickable)' if cs.get('phone_clickable') else 'not clickable'}</p>"
            )
        if cs.get("cta_count") is not None:
            parts.append(f"<p><strong>CTAs:</strong> {_h(cs.get('cta_count'))} (e.g. Book, Schedule, Contact)</p>")
        if cs.get("form_single_or_multi_step"):
            form_mode = str(cs.get("form_single_or_multi_step"))
            if form_mode == "multi_step":
                form_value = "multi-step (more than one step to submit)"
            elif form_mode == "single_step":
                form_value = "single-step"
            else:
                form_value = form_mode
            parts.append(f"<p><strong>Form:</strong> {_h(form_value)}</p>")
        sections.append(
            "<section class=\"brief-section brief-conversion-structure\">\n  <h2>Conversion Structure</h2>\n  "
            + "\n  ".join(parts)
            + "\n</section>"
        )

    ms = vm.get("market_saturation")
    if ms and isinstance(ms, dict):
        parts = []
        if ms.get("top_5_avg_reviews") is not None:
            parts.append(f"<p><strong>Top 5 avg reviews:</strong> {_h(ms.get('top_5_avg_reviews'))}</p>")
        if ms.get("competitor_median_reviews") is not None and ms.get("target_gap_from_median") is not None:
            parts.append(
                f"<p><strong>Median comparison:</strong> median {_h(ms.get('competitor_median_reviews'))}; target is {_h(ms.get('target_gap_from_median'))}</p>"
            )
        if parts:
            sections.append(
                "<section class=\"brief-section brief-market-sat\">\n  <h2>Market Saturation</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    geo = vm.get("geo_coverage")
    if geo and isinstance(geo, dict):
        parts = []
        if geo.get("city_or_near_me_page_count") is not None:
            parts.append(f"<p><strong>City/near-me pages:</strong> {_h(geo.get('city_or_near_me_page_count'))} detected</p>")
            parts.append("<p><small>(URLs with city name or 'near me' in path/title).</small></p>")
        if geo.get("has_multi_location_page") is not None:
            parts.append(f"<p><strong>Multi-location page:</strong> {'Detected' if geo.get('has_multi_location_page') else 'Not detected'}</p>")
        examples = geo.get("geo_page_examples") or []
        if isinstance(examples, list) and examples:
            parts.append("<p><strong>Examples:</strong> " + _h(", ".join(str(x) for x in examples[:3])) + "</p>")
        if parts:
            sections.append(
                "<section class=\"brief-section brief-geo\">\n  <h2>Geographic Coverage</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    # Authority proxy kept internal for modeling; intentionally omitted from rendered brief UI.

    # 6) Risk Flags
    risks = vm.get("risk_flags") or []
    if risks:
        items = " ".join(f"<li>{_h(r)}</li>" for r in risks[:15])
        sections.append(
            "<section class=\"brief-section brief-risks\">\n  <h2>⚠ Risk Flags</h2>\n  <ul>" + items + "</ul>\n</section>"
        )

    # 7) Intervention Plan (3 steps) — no Recommended Call Positioning when plan present
    plan = vm.get("intervention_plan") or []
    fallback = vm.get("intervention_fallback")
    if plan:
        items = " ".join(f"<li>{_h(p)}</li>" for p in plan)
        sections.append(
            "<section class=\"brief-section brief-intervention\">\n  <h2>Intervention Plan (3 steps)</h2>\n  <ul>" + items + "</ul>\n</section>"
        )
    elif fallback and isinstance(fallback, dict):
        parts = []
        if fallback.get("strategic_frame"):
            parts.append(f"<p><strong>Strategic Frame:</strong> {_h(fallback['strategic_frame'])}</p>")
        if fallback.get("tactical_levers"):
            parts.append(f"<p><strong>Tactical Levers:</strong> {_h(fallback['tactical_levers'])}</p>")
        if parts:
            sections.append(
                "<section class=\"brief-section brief-intervention\">\n  <h2>Intervention Plan</h2>\n  "
                + "\n  ".join(parts)
                + "\n</section>"
            )

    # 8) Evidence
    evidence = vm.get("evidence_bullets") or []
    if evidence:
        items = " ".join(f"<li>{_h(e)}</li>" for e in evidence)
        sections.append(
            "<section class=\"brief-section brief-evidence\">\n  <h2>Evidence</h2>\n  <ul>"
            + items
            + "</ul>\n</section>"
        )

    body = "\n".join(sections)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{_h(title)}</title>
  <style>
    .revenue-brief {{ max-width: 720px; margin: 0 auto; font-family: system-ui, sans-serif; line-height: 1.5; color: #1a1a1a; }}
    .revenue-brief .brief-section {{ margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 1px solid #e5e5e5; }}
    .revenue-brief .brief-executive {{ border-bottom-width: 2px; padding-bottom: 1.25rem; }}
    .revenue-brief h2 {{ font-size: 1rem; text-transform: uppercase; letter-spacing: 0.05em; color: #555; margin-bottom: 0.5rem; }}
    .revenue-brief p {{ margin: 0.25rem 0; }}
    .revenue-brief ul {{ margin: 0.25rem 0; padding-left: 1.25rem; }}
    .revenue-brief .brief-footnote {{ color: #666; font-size: 0.875rem; margin-top: 0.5rem; }}
    .revenue-brief details {{ margin-top: 0.5rem; }}
    .revenue-brief details summary {{ cursor: pointer; font-weight: 600; }}
  </style>
</head>
<body class="revenue-brief">
  <h1>{_h(title)}</h1>
  {body}
</body>
</html>"""


def _h(s: Any) -> str:
    """Escape for HTML text content."""
    if s is None:
        return ""
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
