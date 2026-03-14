"""
Canonical 60-second summary builder. Single source of truth for agency_decision_v1.summary_60s.

Pulls ONLY from already-computed objects: lead (signals), objective_layer, revenue_intelligence,
competitive_snapshot (inside objective_layer), service_intelligence (inside objective_layer),
optional paid_intelligence. No LLM; no new numeric derivation. Deterministic.
"""

from typing import Dict, Any, List, Optional

from pipeline.consistency import normalize_service_intelligence


def _compute_worth_pursuing(
    lead: Dict,
    bottleneck: str,
    why_root: str,
    is_seo_primary: bool,
    alternative_lever: str,
    seo_revenue_score: int,
    verdict: str,
    revenue_confidence: int,
) -> tuple:
    """Return (worth_pursuing: 'Yes'|'No'|'Maybe', reason: str)."""
    has_website = lead.get("signal_has_website") is True
    if not has_website:
        return ("Maybe", "Limited data; no website. Verify before outreach.")
    if bottleneck == "trust_limited" and revenue_confidence < 40:
        return ("Maybe", "Trust and reputation are the main constraint; verify readiness before pitching SEO.")
    if bottleneck == "saturation_limited" and not is_seo_primary:
        return ("Maybe", "Market saturated; differentiation or conversion may matter more than SEO.")
    if verdict == "HIGH" and is_seo_primary and seo_revenue_score >= 55:
        return ("Yes", "Strong fit: SEO is the primary lever with clear upside.")
    if verdict == "LOW" and seo_revenue_score < 35:
        return ("No", "Low fit for SEO-first outreach; consider pass or different angle.")
    if verdict == "LOW":
        return ("Maybe", (why_root[:200] if why_root else "Worth a closer look; constraint and lever above."))
    return ("Maybe", (why_root[:200] if why_root else "Worth a closer look; constraint and lever above."))


def _market_position_one_line(competitive_snapshot: Dict) -> str:
    if not competitive_snapshot or competitive_snapshot.get("dentists_sampled", 0) == 0:
        return "No competitive sample available."
    pos = competitive_snapshot.get("review_positioning") or "Unknown position"
    density = (competitive_snapshot.get("market_density_score") or "Unknown").lower()
    return f"{pos} in a {density} density market."


def _right_lever_summary(seo_lever: Dict) -> str:
    if not seo_lever:
        return "SEO role unclear; review constraint and intervention."
    if seo_lever.get("is_primary_growth_lever"):
        return "SEO is the primary growth lever."
    alt = (seo_lever.get("alternative_primary_lever") or "").strip()
    if alt:
        return f"SEO is secondary; focus on {alt} first."
    return "SEO may not be the highest-impact lever; see constraint above."


def _confidence_summary(revenue_confidence_score: int, lead: Dict) -> str:
    if revenue_confidence_score >= 70:
        level = "High"
    elif revenue_confidence_score >= 40:
        level = "Medium"
    else:
        level = "Low"
    limits = []
    if not lead.get("signal_has_website"):
        limits.append("no website")
    rev_count = lead.get("signal_review_count") or lead.get("user_ratings_total") or 0
    if isinstance(rev_count, (int, float)) and int(rev_count) < 15:
        limits.append("very low review count")
    if limits:
        return f"{level} (limited by: {', '.join(limits)})"
    return level


def _confidence_notes(
    lead: Dict,
    rev: Dict,
    service_intelligence: Dict,
) -> List[str]:
    """What's missing or limiting confidence."""
    notes = []
    if not lead.get("signal_has_website"):
        notes.append("No website; estimates are indicative.")
    rev_count = lead.get("signal_review_count") or lead.get("user_ratings_total") or 0
    try:
        rev_count = int(rev_count)
    except (TypeError, ValueError):
        rev_count = 0
    if rev_count < 15:
        notes.append("Very low review count; revenue band is indicative.")
    svc = normalize_service_intelligence(service_intelligence or {})
    crawl_conf = str(svc.get("crawl_confidence") or "").strip().lower()
    if crawl_conf == "low":
        notes.append("Low crawl confidence; service and conversion gaps were not fully evaluated.")
    high_ticket = svc.get("high_ticket_procedures_detected") or []
    general = svc.get("general_services_detected") or []
    if not high_ticket and not general:
        notes.append("No services detected; revenue and gap are indicative.")
    if rev.get("revenue_indicative_only"):
        notes.append("Revenue band is indicative only (limited data).")
    traffic_conf = rev.get("traffic_confidence_score")
    if traffic_conf is not None and traffic_conf < 50:
        notes.append("Traffic estimate has low confidence (proxy signals only).")
    return notes


def _build_supporting_evidence(
    lead: Dict,
    competitive_snapshot: Dict,
    service_intelligence: Dict,
    rev: Dict,
    dentist_profile: Dict,
    paid_intelligence: Optional[Dict] = None,
    suppress_service_gap: bool = False,
    suppress_conversion_absence_claims: bool = False,
) -> Dict[str, List[str]]:
    """Evidence buckets: reputation, market, digital, traffic, revenue. Max 4 per category (traffic 3)."""
    out = {
        "reputation_signals": [],
        "market_signals": [],
        "digital_signals": [],
        "traffic_signals": [],
        "revenue_signals": [],
    }
    rating = lead.get("signal_rating") or lead.get("rating")
    review_count = lead.get("signal_review_count") or lead.get("user_ratings_total") or 0
    try:
        review_count = int(review_count)
    except (TypeError, ValueError):
        review_count = 0
    last_days = lead.get("signal_last_review_days_ago")
    velocity = lead.get("signal_review_velocity_30d")
    if velocity is not None:
        try:
            velocity = int(velocity)
        except (TypeError, ValueError):
            velocity = None

    # Reputation (max 4)
    if rating is not None and review_count > 0:
        out["reputation_signals"].append(f"Rating {rating} from {review_count} reviews")
    elif review_count > 0:
        out["reputation_signals"].append(f"{review_count} reviews")
    if last_days is not None:
        try:
            d = int(last_days)
            if d <= 30:
                out["reputation_signals"].append("Last review within last 30 days")
            else:
                out["reputation_signals"].append(f"Last review {d} days ago")
        except (TypeError, ValueError):
            pass
    if velocity is not None and velocity > 0:
        out["reputation_signals"].append(f"{velocity} new reviews in last 30 days")
    if rating is not None:
        try:
            r = float(rating)
            if r < 4.0:
                out["reputation_signals"].append(f"Below-market rating ({rating})")
        except (TypeError, ValueError):
            pass
    out["reputation_signals"] = out["reputation_signals"][:4]

    # Market (max 4)
    if competitive_snapshot and competitive_snapshot.get("dentists_sampled", 0) > 0:
        pos = competitive_snapshot.get("review_positioning")
        lead_count = competitive_snapshot.get("lead_review_count")
        avg_count = competitive_snapshot.get("avg_review_count")
        if pos and lead_count is not None and avg_count is not None:
            try:
                ac = int(round(float(avg_count)))
                out["market_signals"].append(f"{pos} ({lead_count} vs {ac})")
            except (TypeError, ValueError):
                out["market_signals"].append(pos)
        elif pos:
            out["market_signals"].append(pos)
        density = competitive_snapshot.get("market_density_score")
        if density:
            out["market_signals"].append(f"{density} market density")
    local = (dentist_profile or {}).get("local_search_positioning") or {}
    if local.get("visibility_gap") == "Saturated":
        out["market_signals"].append("Market saturation detected")
    out["market_signals"] = out["market_signals"][:4]

    # Digital (max 4) — paid_evidence from paid_intelligence when present
    paid_intel = paid_intelligence or lead.get("paid_intelligence") or {}
    paid_evidence = paid_intel.get("paid_evidence") or []
    for pe in paid_evidence[:3]:
        if pe and isinstance(pe, str):
            out["digital_signals"].append(pe)
    if lead.get("signal_runs_paid_ads") is True and not any("Meta Ads" in s or "Google" in s for s in out["digital_signals"]):
        channels = lead.get("signal_paid_ads_channels") or []
        labels = []
        if "google" in [str(c).lower() for c in channels]:
            labels.append("Google Ads")
        if "meta" in [str(c).lower() for c in channels]:
            labels.append("Meta Ads")
        if labels:
            out["digital_signals"].append(" and ".join(labels) + " active")
        else:
            out["digital_signals"].append("Paid ads active")
    missing = (service_intelligence or {}).get("missing_high_value_pages") or []
    if not suppress_service_gap:
        for m in missing[:3]:
            label = m if isinstance(m, str) else str(m)
            if label:
                out["digital_signals"].append(f"Missing dedicated {label} page")
    booking_path = lead.get("signal_booking_conversion_path")
    if booking_path in ("Online booking (limited)", "Online booking (full)"):
        has_online_booking = True
    elif booking_path in ("Phone-only", "Request form"):
        has_online_booking = False
    else:
        booking_flag = lead.get("signal_has_automated_scheduling")
        has_online_booking = True if booking_flag is True else (False if booking_flag is False else None)
    if (not suppress_conversion_absence_claims) and (has_online_booking is False) and (missing or lead.get("signal_runs_paid_ads")):
        out["digital_signals"].append("No online booking detected")
    if suppress_conversion_absence_claims:
        out["digital_signals"].append("Conversion infrastructure not fully evaluated (limited crawl depth)")
    if suppress_service_gap:
        out["digital_signals"].append("Service visibility not fully evaluated (limited crawl depth)")
    schema = bool(lead.get("signal_has_schema_microdata")) or bool(lead.get("signal_schema_types") or [])
    if not schema:
        out["digital_signals"].append("No schema markup")
    if not lead.get("signal_has_website"):
        out["digital_signals"].append("No website")
    out["digital_signals"] = out["digital_signals"][:4]

    # Traffic (max 3) — include numeric range when available
    paid_intel = paid_intelligence or lead.get("paid_intelligence") or {}
    if paid_intel.get("active_ads", 0) > 0:
        out["traffic_signals"].append("Active Meta ads detected")
        primary_svc = paid_intel.get("primary_service_promoted")
        if primary_svc:
            out["traffic_signals"].append(f"Promoting {primary_svc} via paid ads")
    monthly = (rev or {}).get("traffic_estimate_monthly")
    if monthly and isinstance(monthly, dict):
        lo, hi = monthly.get("lower"), monthly.get("upper")
        if lo is not None and hi is not None:
            out["traffic_signals"].append(f"Est. traffic: {lo}–{hi} visits/month")
    tier = (rev or {}).get("traffic_estimate_tier")
    if tier:
        out["traffic_signals"].append(f"Traffic tier: {tier}")
    paid = (rev or {}).get("paid_spend_range_estimate")
    if paid and paid != "Not detected":
        out["traffic_signals"].append(f"Estimated paid spend: {paid}")
    eff = (rev or {}).get("traffic_efficiency_interpretation")
    if eff:
        out["traffic_signals"].append(f"Traffic efficiency: {eff}")
    out["traffic_signals"] = out["traffic_signals"][:3]

    # Revenue (max 4)
    band = (rev or {}).get("revenue_band_estimate") or {}
    if band.get("lower") is not None and band.get("upper") is not None:
        lo, hi = band["lower"], band["upper"]
        out["revenue_signals"].append(f"Revenue band: ${lo/1e6:.1f}M–${hi/1e6:.1f}M (annual)")
    gap = (rev or {}).get("organic_revenue_gap_estimate")
    if gap and isinstance(gap, dict) and (gap.get("lower") or gap.get("upper")):
        out["revenue_signals"].append("Organic revenue gap estimated (missing pages / baseline)")
    if rev.get("revenue_indicative_only"):
        out["revenue_signals"].append("Revenue indicative only (limited data)")
    out["revenue_signals"] = out["revenue_signals"][:4]

    return out


def _traffic_estimate_for_summary(rev: Dict) -> Dict[str, Any]:
    """traffic_estimate: numeric range + assumptions (not tier-only). Includes efficiency and confidence for narrator."""
    monthly = rev.get("traffic_estimate_monthly")
    paid_clicks = rev.get("paid_clicks_estimate_monthly")
    out = {
        "traffic_estimate_monthly": monthly or {"lower": None, "upper": None, "unit": "visits/month", "confidence": 0},
        "paid_clicks_estimate_monthly": paid_clicks,
        "traffic_assumptions": rev.get("traffic_assumptions") or "Traffic is estimated from public proxy signals. Not GA4.",
        "paid_clicks_assumptions": rev.get("paid_clicks_assumptions"),
        "traffic_efficiency_interpretation": rev.get("traffic_efficiency_interpretation"),
        "traffic_confidence_score": rev.get("traffic_confidence_score"),
    }
    return out


def _disclaimers(rev: Dict) -> List[str]:
    """Short disclaimer strings for summary."""
    disclaimers = []
    if rev.get("traffic_assumptions"):
        disclaimers.append(rev["traffic_assumptions"])
    if rev.get("revenue_indicative_only"):
        disclaimers.append("Revenue band is indicative where data is limited (no website, low reviews, or no services detected).")
    return disclaimers


def build_canonical_summary_60s(
    lead: Dict[str, Any],
    dentist_profile: Dict[str, Any],
    objective_layer: Dict[str, Any],
    revenue_intelligence: Dict[str, Any],
    paid_intelligence: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build the single canonical summary_60s from existing computed objects only.
    Order: worth_pursuing → financial → traffic → constraint → lever → action → market → leakage → evidence → confidence → versions/disclaimers.
    When worth_pursuing is "No", organic_revenue_gap_estimate is nulled (no big upside for Pass leads).
    """
    obj = objective_layer or {}
    rev = revenue_intelligence or {}
    competitive_snapshot = obj.get("competitive_snapshot") or {}
    service_intelligence = obj.get("service_intelligence") or {}
    root = obj.get("root_bottleneck_classification") or {}
    seo_lever = obj.get("seo_lever_assessment") or {}
    revenue_leverage = obj.get("revenue_leverage_analysis") or {}

    bottleneck = root.get("bottleneck") or "visibility_limited"
    why_root = root.get("why_root_cause") or ""
    revenue_band = rev.get("revenue_band_estimate") or {}
    organic_gap_raw = rev.get("organic_revenue_gap_estimate")
    paid_spend = rev.get("paid_spend_range_estimate") or "Not detected"
    traffic_index = rev.get("traffic_index")
    if traffic_index is None:
        traffic_index = 0
    traffic_efficiency = rev.get("traffic_efficiency_score")
    if traffic_efficiency is None:
        traffic_efficiency = 50
    revenue_confidence_score = rev.get("revenue_confidence_score")
    if revenue_confidence_score is None:
        revenue_confidence_score = 50
    model_versions = rev.get("model_versions") or {}
    cost_leakage = list(rev.get("cost_leakage_signals") or [])

    seo_sales_value = int(obj.get("seo_sales_value_score") or 50)
    seo_revenue_score = max(0, min(100, (seo_sales_value + traffic_efficiency) // 2))
    verdict = lead.get("verdict") or "LOW"
    is_seo_primary = seo_lever.get("is_primary_growth_lever") is True
    alternative_lever = (seo_lever.get("alternative_primary_lever") or "").strip()

    worth_pursuing, worth_pursuing_reason = _compute_worth_pursuing(
        lead, bottleneck, why_root, is_seo_primary, alternative_lever,
        seo_revenue_score, verdict, revenue_confidence_score,
    )

    # Cap or null organic_gap when worth_pursuing is "No"
    organic_revenue_gap_estimate = organic_gap_raw
    if worth_pursuing == "No" and organic_gap_raw and isinstance(organic_gap_raw, dict):
        organic_revenue_gap_estimate = None

    primary_revenue_driver = (revenue_leverage.get("primary_revenue_driver_detected") or "").strip()
    if not primary_revenue_driver:
        high_ticket = service_intelligence.get("high_ticket_procedures_detected") or []
        if high_ticket:
            first = high_ticket[0]
            primary_revenue_driver = (first.get("procedure") if isinstance(first, dict) else str(first)) or "General"
        else:
            primary_revenue_driver = "General"
    primary_revenue_driver = primary_revenue_driver.capitalize() if primary_revenue_driver else "General"

    market_position_one_line = _market_position_one_line(competitive_snapshot)
    right_lever_summary = _right_lever_summary(seo_lever)
    confidence_summary = _confidence_summary(revenue_confidence_score, lead)
    confidence_notes = _confidence_notes(lead, rev, service_intelligence)
    supporting_evidence = _build_supporting_evidence(
        lead,
        competitive_snapshot,
        service_intelligence,
        rev,
        dentist_profile or {},
        paid_intelligence,
        suppress_service_gap=bool((service_intelligence or {}).get("suppress_service_gap")),
        suppress_conversion_absence_claims=bool((service_intelligence or {}).get("suppress_conversion_absence_claims")),
    )
    traffic_estimate = _traffic_estimate_for_summary(rev)
    disclaimers = _disclaimers(rev)

    # Primary intervention
    intervention_plan = obj.get("intervention_plan") or []
    primary_anchor = obj.get("primary_sales_anchor") or {}
    first_intervention = intervention_plan[0] if intervention_plan else {}
    lever = first_intervention.get("action") or primary_anchor.get("issue") or "Improve local visibility and capture"
    rationale = first_intervention.get("expected_impact") or primary_anchor.get("why_this_first") or "Addresses root constraint."
    time_to_signal_days = first_intervention.get("time_to_signal_days")
    if time_to_signal_days is None or not isinstance(time_to_signal_days, (int, float)):
        time_to_signal_days = 30
    time_to_signal_days = int(time_to_signal_days)
    revenue_upside_estimate = None
    if organic_revenue_gap_estimate and isinstance(organic_revenue_gap_estimate, dict):
        revenue_upside_estimate = {
            "lower": organic_revenue_gap_estimate.get("lower"),
            "upper": organic_revenue_gap_estimate.get("upper"),
            "currency": organic_revenue_gap_estimate.get("currency", "USD"),
            "period": organic_revenue_gap_estimate.get("period", "annual"),
        }
    highest_leverage_move = {
        "lever": lever[:500] if isinstance(lever, str) else str(lever)[:500],
        "rationale": rationale[:500] if isinstance(rationale, str) else str(rationale)[:500],
        "time_to_signal_days": time_to_signal_days,
        "revenue_upside_estimate": revenue_upside_estimate,
    }

    # Ordered summary_60s (single source of truth for UI)
    summary_60s = {
        "worth_pursuing": worth_pursuing,
        "worth_pursuing_reason": worth_pursuing_reason,
        "revenue_band": revenue_band,
        "primary_revenue_driver": primary_revenue_driver,
        "organic_revenue_gap_estimate": organic_revenue_gap_estimate,
        "paid_spend_range_estimate": paid_spend,
        "traffic_estimate": traffic_estimate,
        "root_constraint": bottleneck.replace("_", " ").title(),
        "root_constraint_reason": (why_root[:400] if why_root else bottleneck.replace("_", " ").title()),
        "right_lever_summary": right_lever_summary,
        "highest_leverage_move": highest_leverage_move,
        "market_position_one_line": market_position_one_line,
        "cost_leakage_signals": cost_leakage,
        "supporting_evidence": supporting_evidence,
        "confidence_summary": confidence_summary,
        "confidence_notes": confidence_notes,
        "model_versions": model_versions,
        "disclaimers": disclaimers,
    }
    return summary_60s
