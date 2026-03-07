"""
Objective Decision Layer: demand / capture / conversion / trust framing.

Separates (1) DEMAND, (2) CAPTURE/VISIBILITY, (3) CONVERSION, (4) TRUST/REPUTATION,
picks exactly ONE root bottleneck per lead, and produces a prioritized plan that is not
one-dimensional. Prevents over-prescription (e.g. always "booking friction").

Pure function: takes lead JSON (with dentist_profile_v1), returns augmented block.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional, Tuple

logger = logging.getLogger(__name__)

ROOT_BOTTLENECKS = (
    "demand_limited",
    "visibility_limited",
    "conversion_limited",
    "trust_limited",
    "saturation_limited",
    "differentiation_limited",
)

DEFAULT_MODEL = "gpt-4o-mini"
REQUEST_TIMEOUT = 55


# --- Demand / Capture / Conversion / Trust model (deterministic) ---


_FILLER_PREFIXES = ("limited ", "no data",)

def _signal_block(status: str, evidence: List[str], confidence: float) -> Dict[str, Any]:
    meaningful = [e for e in evidence if not any(e.lower().startswith(p) for p in _FILLER_PREFIXES)]
    conf = 0.5 + 0.1 * len(meaningful)
    conf = round(min(0.85, max(0.0, min(confidence, conf))), 2)
    return {"status": status, "evidence": evidence[:5], "confidence": conf}


def _compute_demand_signals(lead: Dict, dentist_profile: Dict) -> Dict[str, Any]:
    """Demand: paid ads, procedure intent, review volume as proxy for local interest."""
    evidence = []
    runs_ads = lead.get("signal_runs_paid_ads") is True
    if runs_ads:
        evidence.append("Paid ads running (demand investment)")
    review_count = lead.get("signal_review_count") or 0
    if review_count >= 30:
        evidence.append("Solid review volume suggests local interest")
    elif review_count >= 10:
        evidence.append("Some review activity indicates demand")
    profile = dentist_profile.get("dental_practice_profile") or {}
    procedures = profile.get("procedure_focus_detected") or []
    if procedures:
        evidence.append("Service intent detected: " + ", ".join(procedures[:3]))
    intent = dentist_profile.get("review_intent_analysis") or {}
    if intent.get("urgency_language_detected"):
        evidence.append("Urgency language in reviews (high-intent demand)")
    score = 0.0
    if runs_ads:
        score += 0.4
    if review_count >= 30:
        score += 0.3
    elif review_count >= 10:
        score += 0.15
    if procedures:
        score += 0.2
    if intent.get("urgency_language_detected"):
        score += 0.15
    if score >= 0.6:
        status = "Strong"
    elif score >= 0.25:
        status = "Moderate"
    else:
        status = "Weak"
    conf = 0.5 + 0.1 * len(evidence)
    return _signal_block(status, evidence or ["Limited demand signals available"], min(1.0, conf))


def _compute_capture_signals(lead: Dict, dentist_profile: Dict) -> Dict[str, Any]:
    """Capture/visibility: reviews vs market, GBP, service page proxy, visibility gap."""
    evidence = []
    local = dentist_profile.get("local_search_positioning") or {}
    review_count = lead.get("signal_review_count") or 0
    last_days = lead.get("signal_last_review_days_ago")
    rv = local.get("review_count_vs_market", "")
    if rv:
        evidence.append(f"Review count vs market: {rv}")
    rt = local.get("rating_strength", "")
    if rt:
        evidence.append(f"Rating strength: {rt}")
    gap = local.get("visibility_gap", "")
    if gap:
        evidence.append(f"Visibility gap: {gap}")
    has_website = lead.get("signal_has_website") is True
    if has_website:
        evidence.append("Website present (visibility channel)")
    trust_web = dentist_profile.get("trust_conversion_signals") or {}
    if trust_web.get("doctor_credentials_visible") or trust_web.get("before_after_gallery"):
        evidence.append("Service/trust content on site (capture support)")
    if last_days is not None:
        if last_days <= 90:
            evidence.append("Recent review activity")
        elif last_days > 180:
            evidence.append("Stale review velocity")
    score = 0.0
    if rv == "Above Average":
        score += 0.35
    elif rv == "Average":
        score += 0.2
    if rt == "Strong":
        score += 0.25
    elif rt == "Moderate":
        score += 0.15
    if gap == "Underutilized":
        score += 0.2
    elif gap == "Competitive":
        score += 0.1
    if gap == "Saturated":
        score -= 0.2
    if has_website:
        score += 0.1
    if score >= 0.5:
        status = "Strong"
    elif score >= 0.2:
        status = "Moderate"
    else:
        status = "Weak"
    conf = 0.5 + 0.1 * len(evidence)
    return _signal_block(status, evidence or ["Limited capture signals"], min(1.0, conf))


def _compute_conversion_signals(lead: Dict, dentist_profile: Dict) -> Dict[str, Any]:
    """Conversion: booking friction, contact form, call-only intake, mobile UX proxy."""
    evidence = []
    readiness = dentist_profile.get("patient_acquisition_readiness") or {}
    svc = lead.get("service_intelligence") or {}
    low_crawl = str((svc.get("crawl_confidence") or "")).strip().lower() == "low"
    booking_friction = readiness.get("booking_friction", "")
    leaks = readiness.get("conversion_leaks") or []
    has_booking = lead.get("signal_has_automated_scheduling") is True
    has_form = lead.get("signal_has_contact_form") is True
    has_phone = lead.get("signal_has_phone") is True
    if low_crawl:
        evidence.append("Conversion structure not fully evaluated (low crawl confidence)")
    elif has_booking:
        evidence.append("Online booking present")
    else:
        evidence.append("No online booking")
    if low_crawl:
        pass
    elif has_form:
        evidence.append("Contact form present")
    else:
        evidence.append("No contact form detected")
    if has_phone:
        evidence.append("Phone available for intake")
    for leak in leaks[:3]:
        evidence.append(leak)
    score = 0.0
    if low_crawl:
        score += 0.15
    elif has_booking:
        score += 0.5
    if not low_crawl and has_form:
        score += 0.25
    if booking_friction == "Low":
        score += 0.25
    elif booking_friction == "Moderate":
        score += 0.1
    if score >= 0.6:
        status = "Strong"
    elif score >= 0.25:
        status = "Moderate"
    else:
        status = "Weak"
    conf = 0.5 + 0.1 * len(evidence)
    return _signal_block(status, evidence or ["Limited conversion signals"], min(1.0, conf))


def _compute_trust_signals(lead: Dict, dentist_profile: Dict) -> Dict[str, Any]:
    """Trust: rating strength, negative theme risk proxy, stale review velocity."""
    evidence = []
    rating = lead.get("signal_rating")
    review_count = lead.get("signal_review_count") or 0
    last_days = lead.get("signal_last_review_days_ago")
    local = dentist_profile.get("local_search_positioning") or {}
    rt = local.get("rating_strength", "")
    trust_web = dentist_profile.get("trust_conversion_signals") or {}
    if rating is not None:
        evidence.append(f"Rating: {rating}")
    if rt:
        evidence.append(f"Rating strength: {rt}")
    if review_count < 10 and review_count > 0:
        evidence.append("Low review count (trust signal weak)")
    elif review_count >= 20:
        evidence.append("Sufficient review volume for trust")
    if last_days is not None:
        if last_days > 365:
            evidence.append("Very stale reviews")
        elif last_days <= 90:
            evidence.append("Recent review activity")
    if trust_web.get("insurance_accepted_visible"):
        evidence.append("Insurance info visible on site")
    if trust_web.get("doctor_credentials_visible"):
        evidence.append("Doctor credentials visible")
    score = 0.0
    if rating is not None:
        if rating >= 4.5:
            score += 0.4
        elif rating >= 4.0:
            score += 0.25
        else:
            score -= 0.2
    if rt == "Strong":
        score += 0.3
    elif rt == "Moderate":
        score += 0.15
    if review_count >= 20:
        score += 0.15
    if review_count < 10 and review_count > 0:
        score -= 0.2
    if last_days is not None and last_days > 365:
        score -= 0.15
    if score >= 0.5:
        status = "Strong"
    elif score >= 0.2:
        status = "Moderate"
    else:
        status = "Weak"
    conf = 0.5 + 0.1 * len(evidence)
    return _signal_block(status, evidence or ["Limited trust signals"], min(1.0, conf))


def _compute_demand_capture_conversion_model(lead: Dict, dentist_profile: Dict) -> Dict[str, Any]:
    return {
        "demand_signals": _compute_demand_signals(lead, dentist_profile),
        "capture_signals": _compute_capture_signals(lead, dentist_profile),
        "conversion_signals": _compute_conversion_signals(lead, dentist_profile),
        "trust_signals": _compute_trust_signals(lead, dentist_profile),
    }


# --- Root bottleneck: exactly one ---


def _compute_root_bottleneck(
    lead: Dict,
    dentist_profile: Dict,
    dcm: Dict[str, Any],
    service_intelligence: Optional[Dict] = None,
    competitive_snapshot: Optional[Dict] = None,
    revenue_leverage: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Pick exactly one root bottleneck. Uses comparative + revenue context when available."""
    trust = dcm["trust_signals"]["status"]
    capture = dcm["capture_signals"]["status"]
    conversion = dcm["conversion_signals"]["status"]
    demand = dcm["demand_signals"]["status"]
    local = dentist_profile.get("local_search_positioning") or {}
    visibility_gap = local.get("visibility_gap", "")
    map_pack = local.get("map_pack_competitiveness", "")
    density = (competitive_snapshot or {}).get("market_density_score", "")
    asymmetry = (revenue_leverage or {}).get("estimated_revenue_asymmetry", "Low")
    high_ticket = (service_intelligence or {}).get("high_ticket_procedures_detected") or []
    has_niche_positioning = asymmetry in ("High", "Moderate") or len(high_ticket) >= 2
    runs_ads = lead.get("signal_runs_paid_ads") is True

    # Booking only primary if: high traffic proxies (ads or strong demand) AND conversion weak AND no established differentiation
    conversion_ok_for_booking = (
        (runs_ads or demand in ("Strong", "Moderate"))
        and conversion == "Weak"
        and not (capture == "Strong" and trust == "Strong" and has_niche_positioning)
    )

    has_website = lead.get("signal_has_website") is True

    if trust == "Weak" and not has_website:
        return {
            "bottleneck": "visibility_limited",
            "why_root_cause": "No website detected; the practice is essentially invisible online beyond its GBP listing.",
            "evidence": dcm["capture_signals"]["evidence"] + ["No website detected"],
            "what_would_change": "Establishing a web presence would shift this classification.",
            "confidence": dcm["capture_signals"]["confidence"],
        }
    if trust == "Weak":
        return {
            "bottleneck": "trust_limited",
            "why_root_cause": "Reputation or trust signals are weak; patients are less likely to choose this practice before visibility or conversion fixes matter.",
            "evidence": dcm["trust_signals"]["evidence"],
            "what_would_change": "Stronger rating, more recent positive reviews, or clearer trust signals on the website would shift this classification.",
            "confidence": dcm["trust_signals"]["confidence"],
        }
    # Differentiation_limited: strong reviews, strong capture, competitive market, no strong niche
    if (
        trust in ("Strong", "Moderate")
        and capture in ("Strong", "Moderate")
        and (visibility_gap == "Saturated" or density == "High" or map_pack == "High")
        and not has_niche_positioning
    ):
        return {
            "bottleneck": "differentiation_limited",
            "why_root_cause": "Reviews and visibility are solid but the market is competitive; the practice lacks clear service or niche positioning to stand out.",
            "evidence": dcm["capture_signals"]["evidence"],
            "what_would_change": "Strong high-ticket or niche service positioning (e.g. dedicated implant or cosmetic pages) would shift this.",
            "confidence": dcm["capture_signals"]["confidence"],
        }
    if visibility_gap == "Saturated" and map_pack == "High" and trust != "Weak":
        return {
            "bottleneck": "saturation_limited",
            "why_root_cause": "Local visibility is already competitive; the main constraint is market saturation rather than a single fix like booking or reviews.",
            "evidence": dcm["capture_signals"]["evidence"],
            "what_would_change": "A drop in competitor activity or a clear underutilized channel (e.g. new service pages, new location) would shift this.",
            "confidence": dcm["capture_signals"]["confidence"],
        }
    if capture == "Weak" and demand != "Weak":
        return {
            "bottleneck": "visibility_limited",
            "why_root_cause": "Demand appears present but the practice is not capturing it well (visibility, review volume, or local presence is the limit).",
            "evidence": dcm["capture_signals"]["evidence"],
            "what_would_change": "Higher review volume, stronger local visibility, or better service-page coverage would shift this classification.",
            "confidence": dcm["capture_signals"]["confidence"],
        }
    if conversion_ok_for_booking:
        return {
            "bottleneck": "conversion_limited",
            "why_root_cause": "Demand and visibility are adequate, but intake or booking friction is limiting how many leads become patients.",
            "evidence": dcm["conversion_signals"]["evidence"],
            "what_would_change": "Online booking, contact form, or smoother intake process would shift this classification.",
            "confidence": dcm["conversion_signals"]["confidence"],
        }
    if demand == "Weak":
        return {
            "bottleneck": "demand_limited",
            "why_root_cause": "Demand signals are weak; the priority is validating or building demand before heavy investment in capture or conversion.",
            "evidence": dcm["demand_signals"]["evidence"],
            "what_would_change": "Stronger local interest signals, paid demand, or procedure-specific demand would shift this.",
            "confidence": dcm["demand_signals"]["confidence"],
        }
    if capture in ("Weak", "Moderate") and conversion != "Weak":
        return {
            "bottleneck": "visibility_limited",
            "why_root_cause": "Capture/visibility is the primary constraint relative to conversion and trust.",
            "evidence": dcm["capture_signals"]["evidence"],
            "what_would_change": "Improved local visibility or review presence would shift this.",
            "confidence": dcm["capture_signals"]["confidence"],
        }
    if conversion == "Weak":
        return {
            "bottleneck": "conversion_limited",
            "why_root_cause": "Conversion is the primary constraint; demand and visibility are relatively stronger.",
            "evidence": dcm["conversion_signals"]["evidence"],
            "what_would_change": "Better booking or intake would shift this.",
            "confidence": dcm["conversion_signals"]["confidence"],
        }
    return {
        "bottleneck": "visibility_limited",
        "why_root_cause": "No single dominant bottleneck; visibility is the default lever to improve next.",
        "evidence": dcm["capture_signals"]["evidence"],
        "what_would_change": "Clear weakness in trust, demand, or conversion would shift this.",
        "confidence": 0.5,
    }


# --- SEO lever assessment (replaces boolean flag) ---

# When SEO is not primary, what is the alternative lever
_ALTERNATIVE_LEVER = {
    "trust_limited": "Reputation / trust",
    "saturation_limited": "Differentiation or conversion",
    "demand_limited": "Demand generation",
    "conversion_limited": "Conversion / booking",
    "visibility_limited": "—",
    "differentiation_limited": "—",
}


def _compute_seo_best_lever(bottleneck: str, dcm: Dict[str, Any]) -> Dict[str, Any]:
    """Return seo_lever_assessment: is_primary_growth_lever, confidence, reasoning, alternative_primary_lever."""
    trust = dcm["trust_signals"]["status"]
    alt = _ALTERNATIVE_LEVER.get(bottleneck, "—")
    if bottleneck == "trust_limited":
        return {
            "is_primary_growth_lever": False,
            "reasoning": "Reputation and trust are the root bottleneck; SEO is not the best first lever until trust is addressed.",
            "confidence": 0.85,
            "alternative_primary_lever": alt,
        }
    if bottleneck == "saturation_limited":
        return {
            "is_primary_growth_lever": False,
            "reasoning": "Market is saturated; SEO may not be the highest-impact lever compared to differentiation or conversion.",
            "confidence": 0.75,
            "alternative_primary_lever": alt,
        }
    if bottleneck == "demand_limited":
        return {
            "is_primary_growth_lever": False,
            "reasoning": "Demand is the constraint; SEO captures demand but does not create it; validate or build demand first.",
            "confidence": 0.75,
            "alternative_primary_lever": alt,
        }
    if bottleneck == "conversion_limited":
        return {
            "is_primary_growth_lever": False,
            "reasoning": "Conversion (booking, intake) is the root bottleneck; fix conversion before investing heavily in SEO traffic.",
            "confidence": 0.8,
            "alternative_primary_lever": alt,
        }
    if bottleneck == "differentiation_limited":
        return {
            "is_primary_growth_lever": True,
            "reasoning": "Differentiation is the constraint; SEO (service pages, local positioning) can help the practice stand out in a competitive market.",
            "confidence": 0.75,
            "alternative_primary_lever": "",
        }
    if bottleneck == "visibility_limited" and trust != "Weak":
        return {
            "is_primary_growth_lever": True,
            "reasoning": "Visibility is the root bottleneck and trust is adequate; SEO is a strong next lever to capture more demand.",
            "confidence": dcm["capture_signals"]["confidence"],
            "alternative_primary_lever": "",
        }
    return {
        "is_primary_growth_lever": False,
        "reasoning": "Insufficient signal to recommend SEO as the best next lever.",
        "confidence": 0.5,
        "alternative_primary_lever": alt,
    }


# --- Comparative context (one sentence, internal proxy) ---


def _compute_comparative_context(
    lead: Dict,
    competitive_snapshot: Optional[Dict[str, Any]] = None,
) -> str:
    """One sentence framing lead vs local pattern. Uses competitive_snapshot when available."""
    review_count = lead.get("signal_review_count") or 0
    last_days = lead.get("signal_last_review_days_ago")
    if competitive_snapshot and competitive_snapshot.get("dentists_sampled", 0) > 0:
        avg = competitive_snapshot.get("avg_review_count") or 0
        positioning = competitive_snapshot.get("review_positioning") or "—"
        density = competitive_snapshot.get("market_density_score", "")
        return (
            f"Among {competitive_snapshot['dentists_sampled']} nearby practices, this practice has "
            f"{review_count} reviews (sample avg {avg:.0f}); {positioning}. Market density: {density}."
        )
    typical_volume = 50
    typical_recency_days = 90
    below_volume = review_count < typical_volume
    stale = last_days is not None and last_days > typical_recency_days
    if below_volume and stale:
        return (
            f"In this area, practices with strong map visibility typically have higher review volume and recent activity; "
            f"this profile is below that pattern (review count {review_count}, last review {last_days} days ago)."
        )
    if below_volume and (last_days is None or last_days <= typical_recency_days):
        return (
            f"This profile has below-typical review volume ({review_count} reviews) but recent activity; "
            f"visibility could improve with more consistent engagement."
        )
    if not below_volume and stale:
        return (
            f"This profile has solid review volume ({review_count}) but review activity has slowed "
            f"(last review {last_days} days ago); refreshing engagement could help visibility."
        )
    return (
        f"This profile is consistent with typical local visibility patterns based on review count ({review_count}) "
        f"and recency (last review {last_days} days ago)."
    )


# --- LLM: primary_sales_anchor (mapped to bottleneck), intervention_plan, access_request_plan, de_risking_questions ---


def _get_client():
    try:
        from openai import OpenAI
        return OpenAI()
    except ImportError:
        return None


def _llm_objective_layer(
    lead: Dict,
    dentist_profile: Dict,
    dcm: Dict[str, Any],
    root_bottleneck: Dict[str, Any],
    seo_lever: Dict[str, Any],
    comparative_context: str,
    service_intelligence: Optional[Dict] = None,
    revenue_leverage: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Call LLM to fill primary_sales_anchor (aligned to bottleneck), intervention_plan (concrete only), access_request_plan, de_risking_questions."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return _fallback_objective_output(root_bottleneck, service_intelligence)
    client = _get_client()
    if not client:
        return _fallback_objective_output(root_bottleneck, service_intelligence)

    bottleneck = root_bottleneck.get("bottleneck", "visibility_limited")
    payload = {
        "name": lead.get("name"),
        "root_bottleneck": bottleneck,
        "root_bottleneck_why": root_bottleneck.get("why_root_cause"),
        "demand_capture_conversion_model": dcm,
        "seo_lever_assessment": seo_lever,
        "comparative_context": comparative_context,
        "service_intelligence": service_intelligence,
        "revenue_leverage": revenue_leverage,
    }

    system = """You are a senior SEO agency consultant. Output:
1) primary_sales_anchor: ONE issue mapping to the root bottleneck. Do NOT default to "booking friction" unless root_bottleneck is conversion_limited.
2) intervention_plan: EXACTLY 3 actions. Each must be specific and immediately usable by an SEO practitioner—deliverables they can implement (pages, schema, tracking, GBP, review flow). No vague phrases. Examples: "Create dedicated [procedure] landing page with H1/local keywords and LocalBusiness schema"; "Add MedicalBusiness/Service schema to site and submit to Search Console"; "Set up post-visit review request (email/SMS) and track review velocity"; "Add conversion goal and CTA above fold on high-value service pages"; "Optimize GBP primary category and service attributes for [procedure]". Category per item: Demand, Capture, Conversion, or Trust. First item only: "why_not_secondaries_yet" (one sentence).
3) access_request_plan: minimal access (Google Business Profile – Manager, Website Admin, Analytics/Search Console). When to ask.
4) de_risking_questions: exactly 3 questions. Tie to uncertainty.
Short, verbatim-ready for an SEO agency."""

    user = f"""Respond with a single JSON object only (no markdown). Keys:

- "primary_sales_anchor": {{ "issue": "...", "why_this_first": "...", "what_happens_if_ignored": "...", "confidence": 0.0 }}
  Issue MUST align with root_bottleneck={bottleneck}. No "booking friction" unless conversion_limited.

- "intervention_plan": [ EXACTLY 3 items. Each: "priority", "action" (specific SEO deliverable—page, schema, tracking, GBP, review flow—implementable in 60 days), "category" ("Demand"|"Capture"|"Conversion"|"Trust"), "expected_impact", "time_to_signal_days", "confidence". First item only: "why_not_secondaries_yet". ]

- "access_request_plan": [ 1-4 items. "intervention_ref", "access_type", "why_needed", "risk_level", "when_to_ask". ]

- "de_risking_questions": [ 3 items. "question", "ties_to_uncertainty". ]

Data:
{json.dumps(payload, indent=2, default=str)}"""

    try:
        model = os.getenv("OPENAI_MODEL", DEFAULT_MODEL)
        r = client.chat.completions.create(
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.3,
            timeout=REQUEST_TIMEOUT,
        )
        text = (r.choices[0].message.content or "").strip()
        if text.startswith("```"):
            lines = text.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)
        data = json.loads(text)
        return _normalize_llm_objective(data, root_bottleneck)
    except (json.JSONDecodeError, TypeError, KeyError, IndexError) as e:
        logger.warning("Objective layer LLM parse error: %s", e)
        return _fallback_objective_output(root_bottleneck, service_intelligence)
    except Exception as e:
        logger.warning("Objective layer LLM request failed: %s", e)
        return _fallback_objective_output(root_bottleneck, service_intelligence)


def _fallback_objective_output(
    root_bottleneck: Dict[str, Any],
    service_intelligence: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Deterministic fallback when LLM is unavailable. Exactly 3 SEO-agency actions."""
    b = root_bottleneck.get("bottleneck", "visibility_limited")
    issue_map = {
        "demand_limited": "Validate demand before scaling visibility",
        "visibility_limited": "Improve local visibility and review presence",
        "conversion_limited": "Reduce booking or intake friction",
        "trust_limited": "Strengthen reputation and trust signals first",
        "saturation_limited": "Differentiate or focus on conversion before more visibility",
        "differentiation_limited": "Build clear service or niche positioning (e.g. dedicated high-value procedure pages)",
    }
    missing = (service_intelligence or {}).get("missing_high_value_pages") or []
    first_missing = (missing[0] if isinstance(missing[0], str) else str(missing[0])) if missing else "high-value procedure"

    # Step 1: Primary lever (page or conversion)
    step1_actions = {
        "demand_limited": "Validate demand and channel mix with a simple tracking setup before investing in new pages.",
        "visibility_limited": f"Create dedicated {first_missing} landing page with H1/local keywords and LocalBusiness schema; submit URL in Search Console.",
        "conversion_limited": "Add conversion-tracked landing page (or online booking CTA above fold) and set a Search/GA goal for high-value procedures.",
        "trust_limited": "Set up post-visit review request (email or SMS) and track review velocity; add trust signals (ratings, credentials) above fold on key pages.",
        "saturation_limited": f"Create dedicated {first_missing} landing page with local intent and schema; avoid generic 'services' page—target one high-value procedure first.",
        "differentiation_limited": f"Create dedicated {first_missing} landing page with H1/local keywords and MedicalBusiness or Service schema for local pack.",
    }
    step1_cat = "Trust" if b == "trust_limited" else "Capture" if b in ("visibility_limited", "differentiation_limited", "saturation_limited") else "Conversion" if b == "conversion_limited" else "Demand"

    # Step 2: Schema or GBP or review flow
    step2_actions = {
        "demand_limited": "Add LocalBusiness (and MedicalBusiness if applicable) schema to site; verify in Search Console Rich Results.",
        "visibility_limited": "Add MedicalBusiness or Service schema to site; verify in Search Console and fix any errors.",
        "conversion_limited": "Add LocalBusiness schema and clear CTA (phone + booking) above fold on service pages.",
        "trust_limited": "Add LocalBusiness and AggregateRating schema where eligible; improve GBP Q&A and service attributes.",
        "saturation_limited": "Add schema to new and existing service pages; optimize GBP primary category and service attributes for the procedure.",
        "differentiation_limited": "Add MedicalBusiness/Service schema to the new page and key service URLs; submit sitemap in Search Console.",
    }
    step2_cat = "Capture" if b in ("visibility_limited", "differentiation_limited", "saturation_limited") else "Trust" if b == "trust_limited" else "Conversion" if b == "conversion_limited" else "Demand"

    # Step 3: Conversion tracking, GBP, or review velocity
    step3_actions = {
        "demand_limited": "Create or optimize one high-intent landing page and add a conversion goal (form submit or click) to measure demand capture.",
        "visibility_limited": "Optimize Google Business Profile: primary category, service attributes, and one post per month; consider review request link in post-visit flow.",
        "conversion_limited": "Optimize GBP for booking (add booking link, hours, services); optionally add review request in post-visit flow.",
        "trust_limited": "Improve on-page trust (credentials, insurance, before/after if applicable); add conversion goal for contact/booking to measure impact.",
        "saturation_limited": "Add conversion goal and CTA above fold on the new high-value page; optimize GBP service attributes to match.",
        "differentiation_limited": "Add conversion tracking and clear CTA on the new page; optimize GBP primary category and one monthly post for the procedure.",
    }
    step3_cat = "Conversion" if b == "conversion_limited" else "Trust" if b == "trust_limited" else "Capture"

    intervention_plan = [
        {
            "priority": 1,
            "action": step1_actions.get(b, step1_actions["visibility_limited"]),
            "category": step1_cat,
            "expected_impact": "Addresses root constraint; measurable in 60 days.",
            "time_to_signal_days": 30,
            "confidence": 0.5,
            "why_not_secondaries_yet": "Addressing the root bottleneck first avoids spreading effort.",
        },
        {
            "priority": 2,
            "action": step2_actions.get(b, step2_actions["visibility_limited"]),
            "category": step2_cat,
            "expected_impact": "Improves local pack and SERP visibility.",
            "time_to_signal_days": 45,
            "confidence": 0.5,
        },
        {
            "priority": 3,
            "action": step3_actions.get(b, step3_actions["visibility_limited"]),
            "category": step3_cat,
            "expected_impact": "Reinforces capture or conversion; measurable within 60 days.",
            "time_to_signal_days": 45,
            "confidence": 0.5,
        },
    ]
    return {
        "primary_sales_anchor": {
            "issue": issue_map.get(b, issue_map["visibility_limited"]),
            "why_this_first": root_bottleneck.get("why_root_cause", ""),
            "what_happens_if_ignored": "Revenue or patient flow remains constrained.",
            "confidence": root_bottleneck.get("confidence", 0.5),
        },
        "intervention_plan": intervention_plan,
        "access_request_plan": [
            {"intervention_ref": "Primary lever", "access_type": "Google Business Profile – Manager", "why_needed": "To implement visibility or reputation actions.", "risk_level": "Low", "when_to_ask": "After initial agreement"}
        ],
        "de_risking_questions": [
            {"question": "How are new patients finding you today?", "ties_to_uncertainty": "Demand and channel mix"},
            {"question": "Do you have a marketing agency or someone handling your online presence?", "ties_to_uncertainty": "Existing ownership of channels"},
            {"question": "What would need to be true for you to add or change something in the next 90 days?", "ties_to_uncertainty": "Readiness to act"},
        ],
    }


def _normalize_llm_objective(data: Dict[str, Any], root_bottleneck: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize LLM response to schema."""
    anchor = data.get("primary_sales_anchor") or {}
    out = {
        "primary_sales_anchor": {
            "issue": str(anchor.get("issue") or "").strip(),
            "why_this_first": str(anchor.get("why_this_first") or "").strip(),
            "what_happens_if_ignored": str(anchor.get("what_happens_if_ignored") or "").strip(),
            "confidence": _clamp(anchor.get("confidence"), 0, 1),
        },
        "intervention_plan": [],
        "access_request_plan": [],
        "de_risking_questions": [],
    }
    for i, item in enumerate((data.get("intervention_plan") or [])[:3]):
        if not isinstance(item, dict):
            continue
        cat = str(item.get("category") or "Capture").strip()
        if cat not in ("Demand", "Capture", "Conversion", "Trust"):
            cat = "Capture"
        plan_item = {
            "priority": i + 1,
            "action": str(item.get("action") or "").strip(),
            "category": cat,
            "expected_impact": str(item.get("expected_impact") or "").strip(),
            "time_to_signal_days": int(item.get("time_to_signal_days")) if isinstance(item.get("time_to_signal_days"), (int, float)) else 30,
            "confidence": _clamp(item.get("confidence"), 0, 1),
        }
        if i == 0:
            plan_item["why_not_secondaries_yet"] = str(item.get("why_not_secondaries_yet") or "").strip()
        out["intervention_plan"].append(plan_item)
    for item in (data.get("access_request_plan") or [])[:4]:
        if not isinstance(item, dict):
            continue
        out["access_request_plan"].append({
            "intervention_ref": str(item.get("intervention_ref") or "").strip(),
            "access_type": str(item.get("access_type") or "").strip(),
            "why_needed": str(item.get("why_needed") or "").strip(),
            "risk_level": str(item.get("risk_level") or "Low").strip(),
            "when_to_ask": str(item.get("when_to_ask") or "").strip(),
        })
    for item in (data.get("de_risking_questions") or [])[:3]:
        if not isinstance(item, dict):
            continue
        out["de_risking_questions"].append({
            "question": str(item.get("question") or "").strip(),
            "ties_to_uncertainty": str(item.get("ties_to_uncertainty") or "").strip(),
        })
    return out


def _clamp(val: Any, lo: float, hi: float) -> float:
    if isinstance(val, (int, float)):
        return round(max(lo, min(hi, float(val))), 2)
    return 0.5


# --- Public API ---


def compute_objective_decision_layer(
    lead: Dict,
    service_intelligence: Optional[Dict] = None,
    competitive_snapshot: Optional[Dict] = None,
    revenue_leverage: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Pure function: takes lead (with dentist_profile_v1) and optional service_intelligence,
    competitive_snapshot, revenue_leverage. Returns the full objective decision layer block.

    When revenue_leverage is not provided but service_intelligence is, revenue_leverage is
    built from service_intelligence + dentist_profile.
    """
    dentist_profile = lead.get("dentist_profile_v1") or {}
    if not dentist_profile:
        return {}

    if revenue_leverage is None and service_intelligence is not None:
        try:
            from pipeline.revenue_leverage import build_revenue_leverage_analysis
            revenue_leverage = build_revenue_leverage_analysis(
                lead, dentist_profile, service_intelligence, competitive_snapshot,
            )
        except Exception:
            revenue_leverage = {}

    dcm = _compute_demand_capture_conversion_model(lead, dentist_profile)
    root_bottleneck = _compute_root_bottleneck(
        lead, dentist_profile, dcm,
        service_intelligence=service_intelligence,
        competitive_snapshot=competitive_snapshot,
        revenue_leverage=revenue_leverage,
    )
    seo_lever = _compute_seo_best_lever(root_bottleneck["bottleneck"], dcm)
    comparative_context = _compute_comparative_context(lead, competitive_snapshot)

    use_llm = os.getenv("USE_LLM_OBJECTIVE_LAYER", "").strip().lower() in ("1", "true", "yes")
    if use_llm:
        llm_block = _llm_objective_layer(
            lead, dentist_profile, dcm, root_bottleneck, seo_lever, comparative_context,
            service_intelligence=service_intelligence,
            revenue_leverage=revenue_leverage,
        )
    else:
        llm_block = _fallback_objective_output(root_bottleneck, service_intelligence)

    seo_sales_value_score = 50
    if revenue_leverage and competitive_snapshot is not None:
        try:
            from pipeline.revenue_leverage import compute_seo_sales_value_score
            seo_sales_value_score = compute_seo_sales_value_score(
                lead, dentist_profile,
                service_intelligence or {},
                competitive_snapshot,
                revenue_leverage,
                root_bottleneck["bottleneck"],
                dcm,
            )
        except Exception:
            pass

    out = {
        "root_bottleneck_classification": {
            "bottleneck": root_bottleneck["bottleneck"],
            "why_root_cause": root_bottleneck["why_root_cause"],
            "evidence": root_bottleneck["evidence"],
            "what_would_change": root_bottleneck["what_would_change"],
            "confidence": root_bottleneck["confidence"],
        },
        "seo_lever_assessment": {
            "is_primary_growth_lever": seo_lever["is_primary_growth_lever"],
            "confidence": seo_lever["confidence"],
            "reasoning": seo_lever["reasoning"],
            "alternative_primary_lever": seo_lever.get("alternative_primary_lever", ""),
        },
        "demand_capture_conversion_model": dcm,
        "comparative_context": comparative_context,
        "primary_sales_anchor": llm_block["primary_sales_anchor"],
        "intervention_plan": llm_block["intervention_plan"],
        "access_request_plan": llm_block["access_request_plan"],
        "de_risking_questions": llm_block["de_risking_questions"],
        "seo_sales_value_score": seo_sales_value_score,
    }
    if service_intelligence is not None:
        out["service_intelligence"] = service_intelligence
    if competitive_snapshot is not None:
        out["competitive_snapshot"] = competitive_snapshot
    if revenue_leverage is not None:
        out["revenue_leverage_analysis"] = revenue_leverage
    return out
