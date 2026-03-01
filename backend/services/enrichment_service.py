"""
Enrichment service: resolves place_id, runs pipeline, returns diagnostic summary.
Reuses existing pipeline modules; no duplicated enrichment logic.
"""

import os
import sys
import logging
import time
from typing import Dict, Optional, List, Any

# Ensure project root on path when running from backend
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))
except ImportError:
    pass

logger = logging.getLogger(__name__)


def _env_enabled(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _extract_city_from_address(formatted_address: Optional[str]) -> str:
    """Extract city from formatted_address (e.g. '123 Main St, San Jose, CA 95110')."""
    if not formatted_address or not isinstance(formatted_address, str):
        return "—"
    parts = [p.strip() for p in formatted_address.split(",")]
    if len(parts) >= 2:
        # Second part is often city (US format)
        return parts[-2] if len(parts) >= 2 else parts[0]
    return parts[0] if parts else "—"


def _parse_root_constraint_label(raw: str) -> str:
    val = (raw or "").strip().lower()
    if any(k in val for k in ("visibility", "capture", "position")):
        return "visibility"
    if any(k in val for k in ("conversion", "cro")):
        return "conversion"
    if any(k in val for k in ("trust", "authority", "reputation")):
        return "trust"
    if val:
        return "mixed"
    return "unknown"


def _derive_demand_level(merged: Dict[str, Any]) -> str:
    dcm = (merged.get("objective_decision_layer") or {}).get("demand_capture_conversion_model") or {}
    demand = str((dcm.get("demand_signals") or {}).get("status") or "").strip().lower()
    if demand == "strong":
        return "high"
    if demand == "moderate":
        return "medium"
    if demand == "weak":
        return "low"
    return "unknown"


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _build_deterministic_intervention_plan(
    merged: Dict[str, Any],
    constraint_label: str,
) -> Dict[str, Any]:
    signals = merged
    svc = merged.get("service_intelligence") or {}
    comp = merged.get("competitive_snapshot") or {}
    lead_reviews = _to_float(signals.get("signal_review_count") or signals.get("user_ratings_total")) or 0.0
    avg_reviews = _to_float(comp.get("avg_review_count")) or 0.0
    review_gap_pct = ((avg_reviews - lead_reviews) / avg_reviews) if avg_reviews > 0 else 0.0

    has_website = bool(signals.get("signal_has_website") or signals.get("signal_website_url"))
    has_ssl = bool(signals.get("signal_has_ssl"))
    has_form = bool(signals.get("signal_has_contact_form"))
    has_phone = bool(signals.get("signal_has_phone"))
    has_schema = bool(signals.get("signal_has_schema_microdata"))
    missing_pages = svc.get("missing_high_value_pages") if isinstance(svc.get("missing_high_value_pages"), list) else []
    demand_level = _derive_demand_level(merged)
    root = _parse_root_constraint_label(constraint_label)

    actions: Dict[str, Dict[str, Any]] = {
        "A1": {
            "action_id": "A1",
            "title": "Fix conversion path on core pages",
            "category": "Conversion",
            "action": "Ensure phone + contact form are prominent above the fold on homepage and core service pages.",
            "why_this_now": "Demand is leaking due to weak conversion capture basics.",
            "expected_window_days": 14,
            "evidence_refs": ["signal_has_contact_form", "signal_has_phone"],
        },
        "A2": {
            "action_id": "A2",
            "title": "Establish technical trust baseline",
            "category": "Trust",
            "action": "Add/repair SSL and LocalBusiness + Service schema and align contact details site-wide.",
            "why_this_now": "Trust and crawlability gaps reduce local conversion and visibility.",
            "expected_window_days": 21,
            "evidence_refs": ["signal_has_ssl", "signal_has_schema_microdata"],
        },
        "A3": {
            "action_id": "A3",
            "title": "Run a review velocity system",
            "category": "Reputation",
            "action": "Implement a post-visit review request + response cadence with weekly tracking.",
            "why_this_now": "Review authority trails local market benchmarks.",
            "expected_window_days": 30,
            "evidence_refs": ["competitive_snapshot.avg_review_count", "signal_review_count"],
        },
        "A4": {
            "action_id": "A4",
            "title": "Strengthen local visibility foundations",
            "category": "SEO",
            "action": "Optimize GBP categories/services/photos/posts cadence and align core local landing pages.",
            "why_this_now": "Visibility constraints require stronger local discovery signals.",
            "expected_window_days": 30,
            "evidence_refs": ["objective_decision_layer.root_bottleneck_classification"],
        },
        "A5": {
            "action_id": "A5",
            "title": "Close high-intent service page gaps",
            "category": "SEO",
            "action": "Create dedicated landing pages for highest-value missing services with local intent copy.",
            "why_this_now": "Service-level capture is constrained by missing high-intent pages.",
            "expected_window_days": 45,
            "evidence_refs": ["service_intelligence.missing_high_value_pages"],
        },
        "A6": {
            "action_id": "A6",
            "title": "Launch focused paid capture test",
            "category": "Demand",
            "action": "Run a limited high-intent branded/service paid test after conversion baseline is fixed.",
            "why_this_now": "Demand can be accelerated once capture infrastructure is stable.",
            "expected_window_days": 21,
            "evidence_refs": ["objective_decision_layer.demand_capture_conversion_model"],
        },
    }

    selected: List[str] = []
    if not has_website:
        selected.extend(["A1", "A2", "A4"])
    if (not has_ssl) or (not has_schema):
        selected.append("A2")
    if (not has_form) or (not has_phone):
        selected.append("A1")
    if review_gap_pct >= 0.25:
        selected.append("A3")

    if root == "visibility":
        selected.extend(["A4", "A3", "A2"])
    elif root == "conversion":
        selected.extend(["A1", "A2", "A4"])
    elif root == "trust":
        selected.extend(["A2", "A3", "A1"])
    else:
        selected.extend(["A4", "A1", "A2"])

    if missing_pages:
        selected.append("A5")
    if demand_level == "high" and has_form and has_phone:
        selected.append("A6")

    ordered: List[str] = []
    for a in selected:
        if a not in ordered:
            ordered.append(a)
    ordered = ordered[:5] if len(ordered) >= 3 else (ordered + ["A4", "A1", "A2"])[:3]

    plan_structured = []
    for idx, aid in enumerate(ordered, start=1):
        item = dict(actions[aid])
        item["step"] = idx
        plan_structured.append(item)

    plan_text = [
        f"{item['title']}: {item['action']} Why now: {item['why_this_now']}."
        for item in plan_structured
    ]
    return {"structured": plan_structured, "text": plan_text}


def _build_diagnostic_response(
    lead_id: int,
    merged: Dict[str, Any],
    city: str,
    state: Optional[str] = None,
) -> Dict[str, Any]:
    """Build UI-ready diagnostic response from enriched lead."""
    oi = merged.get("objective_intelligence") or {}
    comp = merged.get("competitive_snapshot") or (oi.get("competitive_profile") or {})
    ed = {}
    if merged.get("agency_decision_v1") and isinstance(merged["agency_decision_v1"], dict):
        adv1 = merged["agency_decision_v1"]
        exec_diag = adv1.get("executive_diagnosis") or adv1.get("executive_diagnosis_vm")
        if isinstance(exec_diag, dict):
            ed = exec_diag

    # Use revenue_brief_renderer for canonical values when available
    try:
        from pipeline.revenue_brief_renderer import (
            build_revenue_brief_view_model,
            compute_opportunity_profile,
            compute_paid_demand_status,
        )
        vm = build_revenue_brief_view_model(merged)
        opp = compute_opportunity_profile(merged)
        paid = compute_paid_demand_status(merged)
    except Exception as e:
        logger.warning("Could not use revenue_brief_renderer: %s", e)
        vm = {}
        opp = {}
        paid = {}

    opportunity_profile = "—"
    if opp and opp.get("label") and opp.get("why"):
        opportunity_profile = f"{opp['label']} ({opp['why']})"
    elif ed.get("opportunity_profile") and isinstance(ed["opportunity_profile"], dict):
        o = ed["opportunity_profile"]
        if o.get("label"):
            opportunity_profile = f"{o.get('label', '')} ({o.get('why', '')})".strip().rstrip("()").strip() or "—"

    constraint = (
        ed.get("constraint")
        or (oi.get("root_constraint") or {}).get("label")
        or "—"
    )
    primary_leverage = ed.get("primary_leverage") or "—"
    market_density = (
        comp.get("market_density_score")
        or comp.get("market_density")
        or (vm.get("market_position") or {}).get("market_density")
        or "—"
    )
    review_position = (
        comp.get("review_positioning")
        or comp.get("review_positioning_tier")
        or (vm.get("competitive_context") or {}).get("line2", "—")
    )
    if isinstance(review_position, str) and len(review_position) > 100:
        review_position = "—"
    paid_status = (paid or {}).get("status") or "Inactive"

    # Deterministic intervention matrix (brief v2 consistency)
    det_plan = _build_deterministic_intervention_plan(merged, str(constraint))
    intervention_items: List[Dict[str, Any]] = []
    for item in det_plan.get("structured", []):
        intervention_items.append(
            {
                "step": int(item.get("step") or len(intervention_items) + 1),
                "category": str(item.get("category") or "SEO"),
                "action": str(item.get("action") or ""),
            }
        )

    business_name = merged.get("name") or "—"
    if city == "—" and merged.get("formatted_address"):
        city = _extract_city_from_address(merged.get("formatted_address"))

    out = {
        "lead_id": lead_id,
        "place_id": merged.get("place_id"),
        "business_name": business_name,
        "city": str(city),
        "state": state or None,
        "phone": (
            merged.get("signal_phone")
            or merged.get("international_phone_number")
            or merged.get("phone")
            or ((merged.get("signals") or {}).get("signal_phone") if isinstance(merged.get("signals"), dict) else None)
            or ((merged.get("signals") or {}).get("international_phone_number") if isinstance(merged.get("signals"), dict) else None)
        ),
        "website": (
            merged.get("signal_website_url")
            or merged.get("website")
            or merged.get("website_url")
            or ((merged.get("signals") or {}).get("signal_website_url") if isinstance(merged.get("signals"), dict) else None)
            or ((merged.get("signals") or {}).get("website") if isinstance(merged.get("signals"), dict) else None)
        ),
        "opportunity_profile": str(opportunity_profile),
        "constraint": str(constraint),
        "primary_leverage": str(primary_leverage),
        "market_density": str(market_density),
        "review_position": str(review_position),
        "paid_status": str(paid_status),
        "intervention_plan": intervention_items,
    }
    if vm:
        # Keep renderer/LLM intervention plan when present; use deterministic plan only as fallback.
        if not vm.get("intervention_plan"):
            vm["intervention_plan"] = det_plan.get("text", [])
        if not vm.get("intervention_plan_structured"):
            vm["intervention_plan_structured"] = det_plan.get("structured", [])
        out["brief"] = vm

    # Service intelligence, revenue breakdowns, conversion, risk flags, evidence
    service_intel = merged.get("service_intelligence") or {}
    rev = merged.get("revenue_intelligence") or {}
    signals = merged
    oi = merged.get("objective_intelligence") or {}
    snapshot = merged.get("competitive_snapshot") or {}

    detected_raw = service_intel.get("high_ticket_services_detected") or service_intel.get("high_ticket_procedures_detected") or []
    detected_services: List[str] = []
    for x in detected_raw:
        if isinstance(x, str):
            detected_services.append(x)
        elif isinstance(x, dict) and (x.get("procedure") or x.get("service")):
            detected_services.append(str(x.get("procedure") or x.get("service")))

    service_block = {
        "detected_services": detected_services,
        "missing_services": list(service_intel.get("missing_high_value_pages") or []),
        "schema_detected": signals.get("signal_has_schema_microdata"),
        "high_value_services": list(service_intel.get("high_value_services") or []),
        "high_value_summary": dict(service_intel.get("high_value_summary") or {}),
        "high_value_service_leverage": service_intel.get("high_value_service_leverage"),
        "service_page_analysis_v2": dict(service_intel.get("service_page_analysis_v2") or {}),
    }

    revenue_breakdowns: List[Dict[str, Any]] = []
    for svc in rev.get("service_opportunities", []):
        if not isinstance(svc, dict):
            continue
        revenue_breakdowns.append({
            "service": str(svc.get("service") or ""),
            "consults_per_month": str(svc.get("consults_range") or svc.get("consults_per_month") or ""),
            "revenue_per_case": str(svc.get("revenue_per_case") or ""),
            "annual_revenue_range": str(svc.get("annual_revenue_range") or ""),
        })

    conversion_block = {
        "online_booking": signals.get("signal_has_automated_scheduling"),
        "contact_form": signals.get("signal_has_contact_form"),
        "phone_prominent": signals.get("signal_has_phone"),
        "mobile_optimized": signals.get("signal_mobile_friendly"),
        "page_load_ms": signals.get("signal_page_load_time_ms"),
    }

    evidence: List[Dict[str, str]] = []
    lead_reviews = snapshot.get("lead_review_count")
    avg_reviews = snapshot.get("avg_review_count")
    if lead_reviews is not None and avg_reviews is not None:
        evidence.append({
            "label": "Reviews vs Market",
            "value": f"{lead_reviews} vs {avg_reviews}",
        })
    if signals.get("signal_has_schema_microdata") is False:
        evidence.append({
            "label": "Schema",
            "value": "Not detected",
        })

    risk_flags = oi.get("risk_flags") if isinstance(oi.get("risk_flags"), list) else []
    if not risk_flags and vm and isinstance(vm.get("risk_flags"), list):
        risk_flags = vm["risk_flags"]

    out["service_intelligence"] = service_block
    out["revenue_breakdowns"] = revenue_breakdowns
    out["conversion_infrastructure"] = conversion_block
    out["risk_flags"] = list(risk_flags)
    out["evidence"] = evidence
    out["competitive_delta"] = merged.get("competitive_delta")
    out["review_intelligence"] = merged.get("review_intelligence") or merged.get("signal_review_intelligence")
    out["geo_coverage"] = merged.get("geo_coverage")
    out["authority_proxy"] = merged.get("authority_proxy")

    # Final deterministic consistency pass to prevent contradictory brief claims.
    try:
        from pipeline.validation import enforce_diagnostic_consistency
        warnings = enforce_diagnostic_consistency(out, merged=merged)
        if warnings:
            out["consistency_warnings"] = warnings
    except Exception as e:
        logger.warning("Could not run diagnostic consistency enforcement: %s", e)

    return out


def run_diagnostic(
    business_name: str,
    city: str,
    state: Optional[str] = None,
    website: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolve place, run enrichment pipeline, store lead, return diagnostic summary.

    Required: business_name, city
    Optional: website (for reference; resolution uses name+city)

    Raises FileNotFoundError for not found, RuntimeError for pipeline/API failures.
    """
    from backend.services.place_resolver import resolve_from_name_city
    from pipeline.enrich import PlaceDetailsEnricher
    from pipeline.signals import extract_signals, merge_signals_into_lead
    from pipeline.meta_ads import get_meta_access_token, augment_lead_with_meta_ads
    from pipeline.google_ads_check import augment_lead_with_google_ads
    from pipeline.seo_traffic import augment_lead_with_seo_traffic
    from pipeline.ga4_integration import augment_lead_with_ga4
    from pipeline.google_ads_api import augment_lead_with_google_ads_api
    from pipeline.semantic_signals import build_semantic_signals
    from pipeline.decision_agent import DecisionAgent
    from pipeline.dentist_profile import is_dental_practice, build_dentist_profile_v1, fetch_website_html_for_trust
    from pipeline.service_depth import build_service_intelligence, get_page_texts_for_llm
    from pipeline.competitor_sampling import (
        fetch_competitors_nearby,
        build_competitive_snapshot,
        enrich_competitors_with_site_metrics,
    )
    from pipeline.competitive_delta import build_competitive_delta
    from pipeline.authority_proxy import build_authority_proxy
    from pipeline.objective_decision_layer import compute_objective_decision_layer
    from pipeline.objective_intelligence import (
        build_objective_intelligence,
        build_objective_intelligence_summary,
    )
    from pipeline.revenue_intelligence import build_revenue_intelligence
    from pipeline.dentist_llm_reasoning import dentist_llm_reasoning_layer
    from pipeline.sales_intervention import build_sales_intervention_intelligence
    from pipeline.agency_decision import build_agency_decision_v1
    from pipeline.llm_structured_extraction import extract_structured
    from pipeline.llm_executive_compression import build_executive_summary_and_outreach
    from pipeline.context import build_context
    from pipeline.db import (
        init_db,
        create_run,
        insert_lead,
        insert_lead_signals,
        insert_decision,
        update_lead_dentist_data,
        update_run_completed,
        update_run_failed,
        save_review_snapshot,
        get_review_velocity,
    )
    from pipeline.embedding_store import store_lead_embedding_if_eligible

    # 1) Resolve place via name + city + state (most reliable)
    resolved_state = state.strip() if state else None
    lead = resolve_from_name_city(business_name.strip(), city.strip(), state=resolved_state)
    resolved_city = city.strip()

    if not lead or not lead.get("place_id"):
        raise FileNotFoundError("Business not found")  # 404

    init_db()

    # 2) Enrich with Place Details
    try:
        enricher = PlaceDetailsEnricher()
    except ValueError as e:
        raise RuntimeError("Places API not configured") from e

    enriched_list = enricher.enrich_leads_batch([lead], progress_interval=999)
    if not enriched_list:
        raise RuntimeError("Place Details enrichment failed")

    enriched = enriched_list[0]

    # 3) Extract signals
    signals = extract_signals(enriched)
    merged = merge_signals_into_lead(enriched, signals)
    if isinstance(merged.get("signal_review_intelligence"), dict):
        merged["review_intelligence"] = merged.get("signal_review_intelligence")
    # Optional enrichments are latency-heavy. Keep them explicit opt-in for fast diagnostics/re-runs.
    optional_budget_sec_raw = os.getenv("NEYMA_DIAGNOSTIC_OPTIONAL_ENRICH_BUDGET_SEC", "6")
    try:
        optional_budget_sec = float(optional_budget_sec_raw)
    except ValueError:
        optional_budget_sec = 6.0
    optional_t0 = time.monotonic()

    def _optional_budget_ok() -> bool:
        return (time.monotonic() - optional_t0) < optional_budget_sec

    # Meta Ads enrichment
    if _env_enabled("NEYMA_DIAGNOSTIC_ENABLE_META_ADS", default=False) and _optional_budget_ok():
        try:
            use_meta = get_meta_access_token() is not None
            if use_meta:
                augment_lead_with_meta_ads(merged)
        except Exception as e:
            logger.warning("Optional meta ads enrichment failed: %s", e)

    # Google Ads Transparency check (can be slow due subdomain probes)
    if _env_enabled("NEYMA_DIAGNOSTIC_ENABLE_GOOGLE_ADS_CHECK", default=False) and _optional_budget_ok():
        try:
            augment_lead_with_google_ads(merged)
        except Exception as e:
            logger.warning("Optional google ads check failed: %s", e)

    # SEO traffic API enrichment
    if _env_enabled("NEYMA_DIAGNOSTIC_ENABLE_SEO_TRAFFIC", default=False) and _optional_budget_ok():
        try:
            augment_lead_with_seo_traffic(merged)
        except Exception as e:
            logger.warning("Optional SEO traffic enrichment failed: %s", e)

    # GA4 enrichment
    if _env_enabled("NEYMA_DIAGNOSTIC_ENABLE_GA4", default=False) and _optional_budget_ok():
        try:
            augment_lead_with_ga4(merged)
        except Exception as e:
            logger.warning("Optional GA4 enrichment failed: %s", e)

    # Google Ads API enrichment
    if _env_enabled("NEYMA_DIAGNOSTIC_ENABLE_GOOGLE_ADS_API", default=False) and _optional_budget_ok():
        try:
            augment_lead_with_google_ads_api(merged)
        except Exception as e:
            logger.warning("Optional Google Ads API enrichment failed: %s", e)

    # Review tracking: save snapshot and compute real velocity from historical data
    place_id = merged.get("place_id")
    review_count = merged.get("signal_review_count") or merged.get("user_ratings_total")
    rating_val = merged.get("signal_rating") or merged.get("rating")
    if place_id and review_count:
        try:
            review_count_int = int(review_count)
            rating_float = float(rating_val) if rating_val else None
            save_review_snapshot(place_id, review_count_int, rating_float)
            real_velocity = get_review_velocity(place_id)
            if real_velocity is not None:
                merged["real_review_velocity"] = real_velocity
                merged["signal_real_review_velocity_30d"] = real_velocity["velocity_per_30d"]
        except (ValueError, TypeError):
            pass

    agency_type = os.getenv("AGENCY_TYPE", "marketing").lower() or "marketing"
    run_id = create_run({
        "source": "diagnostic_api",
        "agency_type": agency_type,
    })
    agent = DecisionAgent(agency_type=agency_type)

    try:
        lead_id = insert_lead(run_id, merged)
        insert_lead_signals(lead_id, signals)

        if is_dental_practice(merged):
            url = merged.get("signal_website_url")
            website_html = fetch_website_html_for_trust(url) if url else None
            dentist_profile_v1 = build_dentist_profile_v1(merged, website_html=website_html)
            obj_layer = None
            llm_reasoning_layer = {}
            sales_intel = None
            if dentist_profile_v1:
                merged["dentist_profile_v1"] = dentist_profile_v1
                procedure_mentions = (dentist_profile_v1.get("review_intent_analysis") or {}).get("procedure_mentions") or []
                service_intel = build_service_intelligence(
                    url,
                    website_html,
                    procedure_mentions,
                    city=resolved_city,
                    state=resolved_state,
                    vertical="dentist",
                )
                competitors = []
                search_radius_used_miles = 2
                lat, lng = merged.get("latitude"), merged.get("longitude")
                if lat is not None and lng is not None:
                    competitors, search_radius_used_miles = fetch_competitors_nearby(lat, lng, merged.get("place_id"))
                    if _env_enabled("NEYMA_DIAGNOSTIC_ENABLE_COMPETITOR_SITE_METRICS", default=True):
                        competitors = enrich_competitors_with_site_metrics(
                            competitors,
                            vertical="dentist",
                        )
                competitive_snap = build_competitive_snapshot(merged, competitors, search_radius_used_miles) if competitors else {}
                competitive_delta = build_competitive_delta(merged, service_intel, competitors)
                authority_proxy = build_authority_proxy(
                    service_intelligence=service_intel,
                    domain_age_years=merged.get("domain_age_years"),
                )
                merged["competitive_snapshot"] = competitive_snap
                merged["service_intelligence"] = service_intel
                merged["competitive_delta"] = competitive_delta
                merged["authority_proxy"] = authority_proxy
                merged["geo_coverage"] = {
                    "city_or_near_me_page_count": service_intel.get("city_or_near_me_page_count"),
                    "has_multi_location_page": service_intel.get("has_multi_location_page"),
                    "geo_page_examples": service_intel.get("geo_page_examples") or [],
                }
                obj_layer = compute_objective_decision_layer(
                    merged,
                    service_intelligence=service_intel,
                    competitive_snapshot=competitive_snap if competitors else None,
                    revenue_leverage=None,
                )
                merged["objective_decision_layer"] = obj_layer if obj_layer else {}
                pricing_page_detected = False
                if os.getenv("USE_LLM_STRUCTURED_EXTRACTION", "").strip().lower() in ("1", "true", "yes"):
                    page_texts = get_page_texts_for_llm(merged.get("signal_website_url"), website_html)
                    pricing_page_detected = bool(page_texts and page_texts.get("pricing_page_text"))
                rev_intel = build_revenue_intelligence(
                    merged,
                    dentist_profile_v1,
                    obj_layer or {},
                    pricing_page_detected=pricing_page_detected,
                    paid_intelligence=merged.get("paid_intelligence"),
                )
                merged["revenue_intelligence"] = rev_intel

            oi = build_objective_intelligence(merged)
            merged["objective_intelligence"] = oi
            oi_summary = build_objective_intelligence_summary(oi)
            decision = agent.decide_from_objective_summary(oi_summary, lead_name=merged.get("name") or "")
            merged["verdict"] = decision.verdict
            merged["confidence"] = decision.confidence
            insert_decision(
                lead_id=lead_id,
                agency_type=agency_type,
                signals_snapshot={"objective_intelligence_summary": oi_summary},
                verdict=decision.verdict,
                confidence=decision.confidence,
                reasoning=decision.reasoning,
                primary_risks=decision.primary_risks,
                what_would_change=decision.what_would_change,
                prompt_version=agent.prompt_version,
            )

            if dentist_profile_v1:
                context = build_context(merged)
                lead_score = round((merged.get("confidence") or 0) * 100)
                llm_reasoning_layer = dentist_llm_reasoning_layer(
                    business_snapshot=merged,
                    dentist_profile_v1=dentist_profile_v1,
                    context_dimensions=context.get("context_dimensions", []),
                    lead_score=lead_score,
                    priority=merged.get("verdict"),
                    confidence=merged.get("confidence"),
                )
                sales_intel = build_sales_intervention_intelligence(
                    business_snapshot=merged,
                    dentist_profile_v1=dentist_profile_v1,
                    context_dimensions=context.get("context_dimensions", []),
                    verdict=merged.get("verdict"),
                    confidence=merged.get("confidence"),
                    llm_reasoning_layer=llm_reasoning_layer,
                )
                llm_extraction = None
                if os.getenv("USE_LLM_STRUCTURED_EXTRACTION", "").strip().lower() in ("1", "true", "yes"):
                    page_texts = get_page_texts_for_llm(merged.get("signal_website_url"), website_html)
                    llm_extraction = extract_structured(
                        (page_texts or {}).get("homepage_text") or "",
                        (page_texts or {}).get("services_page_text"),
                        (page_texts or {}).get("pricing_page_text"),
                    )
                    merged["llm_structured_extraction"] = llm_extraction
                executive_summary = None
                outreach_angle = None
                rev_intel = merged.get("revenue_intelligence") or {}
                if os.getenv("USE_LLM_EXECUTIVE_COMPRESSION", "").strip().lower() in ("1", "true", "yes"):
                    root = (obj_layer or {}).get("root_bottleneck_classification") or {}
                    comp_res = build_executive_summary_and_outreach(
                        primary_constraint=root.get("why_root_cause") or root.get("bottleneck") or "",
                        revenue_gap=rev_intel.get("organic_revenue_gap_estimate"),
                        cost_leakage_signals=rev_intel.get("cost_leakage_signals"),
                        service_focus=(merged.get("llm_structured_extraction") or {}).get("service_focus"),
                    )
                    executive_summary = comp_res.get("executive_summary")
                    outreach_angle = comp_res.get("outreach_angle")
                merged["agency_decision_v1"] = build_agency_decision_v1(
                    merged,
                    dentist_profile_v1,
                    obj_layer or {},
                    rev_intel,
                    llm_extraction=llm_extraction,
                    executive_summary=executive_summary,
                    outreach_angle=outreach_angle,
                )
            update_lead_dentist_data(
                lead_id,
                dentist_profile_v1=merged.get("dentist_profile_v1"),
                llm_reasoning_layer=llm_reasoning_layer if dentist_profile_v1 else None,
                sales_intervention_intelligence=sales_intel if sales_intel else None,
                objective_decision_layer=obj_layer if obj_layer else None,
            )
            if merged.get("objective_intelligence"):
                store_lead_embedding_if_eligible(lead_id, merged, force_embed=False)
        else:
            semantic = build_semantic_signals(merged)
            decision = agent.decide(semantic, lead_name=merged.get("name") or "")
            merged["verdict"] = decision.verdict
            merged["confidence"] = decision.confidence
            insert_decision(
                lead_id=lead_id,
                agency_type=agency_type,
                signals_snapshot=semantic,
                verdict=decision.verdict,
                confidence=decision.confidence,
                reasoning=decision.reasoning,
                primary_risks=decision.primary_risks,
                what_would_change=decision.what_would_change,
                prompt_version=agent.prompt_version,
            )

        update_run_completed(run_id, 1, run_stats={"total": 1})
        response = _build_diagnostic_response(lead_id, merged, resolved_city, state=resolved_state)

        # Store predictions for outcome tracking / calibration
        try:
            from pipeline.outcome_tracking import save_diagnostic_predictions, ensure_outcome_tables
            ensure_outcome_tables()
            rev_intel = merged.get("revenue_intelligence") or {}
            oi = merged.get("objective_intelligence") or {}
            svc_intel = merged.get("service_intelligence") or {}
            predictions = {
                "revenue_band": rev_intel.get("revenue_band_estimate"),
                "revenue_upside": rev_intel.get("organic_revenue_gap_estimate"),
                "constraint": (oi.get("root_bottleneck") or {}).get("bottleneck"),
                "missing_services": svc_intel.get("missing_high_value_pages", []),
                "detected_services": svc_intel.get("high_ticket_services_detected", []),
                "has_booking": merged.get("signal_has_automated_scheduling"),
                "has_schema": merged.get("signal_has_schema_microdata"),
                "runs_google_ads": merged.get("signal_runs_paid_ads"),
                "review_count": merged.get("signal_review_count") or merged.get("user_ratings_total"),
                "review_velocity_30d": merged.get("signal_review_velocity_30d"),
                "traffic_estimate": rev_intel.get("traffic_estimate_monthly"),
            }
            save_diagnostic_predictions(lead_id, merged.get("place_id", ""), predictions)
        except Exception as pred_exc:
            logger.warning("Failed to save diagnostic predictions: %s", pred_exc)

        return response

    except Exception:
        update_run_failed(run_id)
        raise
