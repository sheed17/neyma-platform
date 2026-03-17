"""
Objective Intelligence Layer: single deterministic aggregation for the brief and DecisionAgent.

Reads from lead: objective_decision_layer, competitive_snapshot, service_intelligence,
revenue_intelligence, signal_*. For dental leads, intervention_plan may be generated via
one optional LLM call (generate_intervention_plan_from_intelligence). No new API calls for data.
"""

import json
import logging
import os
from typing import Dict, Any, List, Optional

from pipeline.dentist_profile import is_dental_practice

logger = logging.getLogger(__name__)

INTERVENTION_LLM_MODEL = "gpt-4o-mini"
INTERVENTION_LLM_TIMEOUT = 55

# Bottleneck -> plain-English label (SEO/dentist focus)
BOTTLENECK_LABEL: Dict[str, str] = {
    "saturation_limited": "Already strong locally; growth requires differentiation or capture",
    "visibility_limited": "Local visibility is the main constraint",
    "conversion_limited": "Conversion or booking friction is the main constraint",
    "trust_limited": "Trust and reputation signals are the main constraint",
    "demand_limited": "Demand validation is the priority before scaling visibility",
    "differentiation_limited": "Market is competitive; need clearer service or niche positioning",
    "capture_constrained": "Capture or visibility is the main constraint",
}


def _get_obj_layer(lead: Dict) -> Dict:
    return lead.get("objective_decision_layer") or {}


def _get_comp_snapshot(lead: Dict) -> Dict:
    comp = lead.get("competitive_snapshot")
    if comp:
        return comp
    obj = _get_obj_layer(lead)
    return obj.get("competitive_snapshot") or {}


def _get_service_intel(lead: Dict) -> Dict:
    si = lead.get("service_intelligence")
    if si:
        return si
    return _get_obj_layer(lead).get("service_intelligence") or {}


def _get_rev_intel(lead: Dict) -> Dict:
    return lead.get("revenue_intelligence") or {}


def _get_signals(lead: Dict) -> Dict:
    if lead.get("signals") and isinstance(lead["signals"], dict):
        return lead["signals"]
    return {k: v for k, v in lead.items() if k.startswith("signal_")}


def _category_norm(cat: Optional[str]) -> str:
    if not cat:
        return "Capture"
    c = (cat or "").strip()
    if c in ("Demand", "Capture", "Conversion", "Trust", "Visibility"):
        return c
    if c == "Trust":
        return "Trust"
    return "Capture"


def _service_in_missing(high_ticket: List[str], missing: List[str]) -> Optional[str]:
    """Return prioritized missing high-ticket service (Implants first), then fallback match."""
    if not high_ticket or not missing:
        return None
    missing_norm = [str(m).strip().lower() for m in missing if m]
    high_ticket_norm = [str(h).strip().lower() for h in high_ticket if h]

    priorities = [
        ("Implants", ("implant", "all-on-4", "all on 4")),
        ("Orthodontics", ("orthodont", "invisalign", "braces", "aligner")),
        ("Veneers", ("veneer",)),
        ("Cosmetic", ("cosmetic", "whitening", "smile makeover")),
        ("Sedation", ("sedation", "iv sedation", "nitrous", "oral sedation")),
        ("Crowns", ("crown", "same day crown", "same-day crown")),
        ("Sleep Apnea", ("sleep apnea", "snoring")),
        ("Emergency", ("emergency", "urgent", "same day", "same-day")),
    ]
    for label, needles in priorities:
        missing_hit = any(any(n in m for n in needles) for m in missing_norm)
        high_ticket_hit = any(any(n in h for n in needles) for h in high_ticket_norm)
        if missing_hit and high_ticket_hit:
            return label

    for h in high_ticket:
        if not h or not isinstance(h, str):
            continue
        hn = h.strip().lower()
        for m in missing_norm:
            if hn in m or m in hn:
                return h.strip()
    return None


def detect_competitive_service_gap(lead: Dict) -> Optional[Dict[str, Any]]:
    """
    Deterministic detection of competitive service capture gap.
    Trigger when: high-ticket service detected, that service is in missing_high_value_pages,
    schema missing, nearest_competitor exists, review_positioning_tier is Below Average or Weak.
    No LLM. No new API calls. Returns structured gap dict or None.
    """
    svc = _get_service_intel(lead)
    comp = _get_comp_snapshot(lead)
    signals = _get_signals(lead)
    schema = signals.get("signal_has_schema_microdata")
    if schema is True:
        return None
    high_ticket = list(svc.get("high_ticket_procedures_detected") or [])
    missing = list(svc.get("missing_high_value_pages") or [])
    service = _service_in_missing(high_ticket, missing)
    if not service:
        return None
    cs = comp.get("competitor_summary") or {}
    nearest = cs.get("nearest_competitor")
    if not nearest or not isinstance(nearest, dict):
        return None
    tier = (comp.get("review_positioning_tier") or "").strip()
    if tier not in ("Below Average", "Weak"):
        return None
    competitor_name = (nearest.get("name") or "").strip() or "—"
    competitor_reviews = int(nearest.get("reviews") or nearest.get("user_ratings_total") or 0)
    lead_reviews = int(comp.get("lead_review_count") or signals.get("signal_review_count") or lead.get("user_ratings_total") or 0)
    distance_miles = nearest.get("distance_miles")
    if distance_miles is not None:
        try:
            distance_miles = float(distance_miles)
        except (TypeError, ValueError):
            distance_miles = None
    return {
        "type": "High-Margin Capture Gap",
        "service": service,
        "competitor_name": competitor_name,
        "competitor_reviews": competitor_reviews,
        "lead_reviews": lead_reviews,
        "distance_miles": distance_miles,
        "schema_missing": True,
    }


def detect_strategic_gap(lead: Dict) -> Optional[Dict[str, Any]]:
    """
    Deterministic Service Capture Gap: trigger when missing_high_value_pages not empty,
    high_ticket_detected exists, market_density == High, and (schema_detected == False
    OR missing overlaps high_ticket). No LLM.
    """
    svc = _get_service_intel(lead)
    comp = _get_comp_snapshot(lead)
    signals = _get_signals(lead)
    high_ticket = list(svc.get("high_ticket_procedures_detected") or [])
    missing = list(svc.get("missing_high_value_pages") or [])
    if not missing or not high_ticket:
        return None
    market_density = (comp.get("market_density_score") or "").strip()
    if market_density != "High":
        return None
    schema_detected = signals.get("signal_has_schema_microdata") is True
    overlap = _service_in_missing(high_ticket, missing)
    if not schema_detected or overlap:
        pass
    else:
        return None
    cs = comp.get("competitor_summary") or {}
    nearest = cs.get("nearest_competitor")
    if not nearest or not isinstance(nearest, dict):
        return None
    service = overlap or (high_ticket[0] if high_ticket else "") or (missing[0] if missing else "")
    if not service:
        return None
    competitor_name = (nearest.get("name") or "").strip() or "—"
    competitor_reviews = int(nearest.get("reviews") or nearest.get("user_ratings_total") or 0)
    lead_reviews = int(comp.get("lead_review_count") or signals.get("signal_review_count") or lead.get("user_ratings_total") or 0)
    distance_miles = nearest.get("distance_miles")
    if distance_miles is not None:
        try:
            distance_miles = float(distance_miles)
        except (TypeError, ValueError):
            distance_miles = None
    return {
        "type": "Service Capture Gap",
        "service": service if isinstance(service, str) else str(service),
        "competitor_name": competitor_name,
        "competitor_reviews": competitor_reviews,
        "lead_reviews": lead_reviews,
        "distance_miles": distance_miles,
        "market_density": "High",
    }


def _normalize_intervention_step(item: Any, step_num: int) -> Optional[Dict[str, Any]]:
    """Normalize one LLM step to schema: step, category, action, time_to_signal_days, why."""
    if not isinstance(item, dict):
        return None
    action = (item.get("action") or "").strip()
    if not action:
        return None
    cat = (item.get("category") or "Capture").strip()
    if cat not in ("Trust", "Capture", "Conversion", "Demand"):
        cat = "Capture"
    days = item.get("time_to_signal_days")
    if days is not None:
        try:
            days = int(days)
        except (TypeError, ValueError):
            days = 30
    else:
        days = 30
    why = (item.get("why") or "").strip() or ""
    return {
        "step": step_num,
        "category": cat,
        "action": action,
        "time_to_signal_days": days,
        "why": why,
    }


def _replace_intervention_placeholders(
    text: str,
    practice_name: str,
    city: str,
    state: str,
) -> str:
    out = (text or "").strip()
    if not out:
        return out
    city_state = ", ".join([p for p in [city, state] if p]).strip()
    replacements = {
        "[Practice]": practice_name or "the practice",
        "[City]": city or city_state or "the local market",
        "[State]": state or "",
        "[Market]": city_state or city or "the local market",
    }
    for src, dst in replacements.items():
        if dst:
            out = out.replace(src, dst)
    return out


def generate_intervention_plan_from_intelligence(
    lead: Dict,
    objective_intelligence: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Generate a 3-step strategic intervention plan using one LLM call.
    Uses only structured inputs: root_constraint, primary_growth_vector, strategic_gap,
    competitive_profile, service_intel, cost_leakage_signals, conversion_profile. No raw signals.
    """
    oi = objective_intelligence if objective_intelligence is not None else lead.get("objective_intelligence") or {}
    svc = _get_service_intel(lead)
    rev = _get_rev_intel(lead)
    cost_leakage = list(rev.get("cost_leakage_signals") or [])

    root = oi.get("root_constraint") or {}
    root_text = f"Label: {root.get('label', '')}\nWhy: {root.get('why', '')}" if (root.get("label") or root.get("why")) else "—"

    pgv = oi.get("primary_growth_vector") or {}
    pgv_text = f"Label: {pgv.get('label', '')}\nWhy: {pgv.get('why', '')}"

    comp_prof = oi.get("competitive_profile") or {}
    comp_text = json.dumps(comp_prof, default=str) if comp_prof else "—"

    strategic_gap = oi.get("strategic_gap")
    gap_text = json.dumps(strategic_gap, default=str) if strategic_gap and isinstance(strategic_gap, dict) else "None"
    competitor_name = ""
    if isinstance(strategic_gap, dict):
        competitor_name = str(strategic_gap.get("competitor_name") or "").strip()

    service_intel = oi.get("service_intel") or {
        "high_ticket_detected": svc.get("high_ticket_procedures_detected") or [],
        "missing_high_value_pages": svc.get("missing_high_value_pages") or [],
    }
    service_text = json.dumps(service_intel, default=str)
    missing_services = list(service_intel.get("missing_high_value_pages") or svc.get("missing_high_value_pages") or [])
    missing_services = [str(x).strip() for x in missing_services if str(x).strip()]

    practice_name = str(lead.get("name") or "").strip()

    city = str(lead.get("city") or "").strip()
    state = str(lead.get("state") or "").strip()
    if not city:
        address_components = lead.get("address_components") or []
        if isinstance(address_components, list):
            for comp_item in address_components:
                if not isinstance(comp_item, dict):
                    continue
                comp_types = comp_item.get("types") or []
                if "locality" in comp_types and not city:
                    city = str(comp_item.get("long_name") or comp_item.get("short_name") or "").strip()
                if "administrative_area_level_1" in comp_types and not state:
                    state = str(comp_item.get("short_name") or comp_item.get("long_name") or "").strip()
    if not city:
        formatted = str(lead.get("formatted_address") or "").strip()
        if formatted:
            parts = [p.strip() for p in formatted.split(",") if p.strip()]
            if len(parts) >= 2:
                city = city or parts[-3] if len(parts) >= 3 else city
                state_part = parts[-2] if len(parts) >= 2 else ""
                if state_part and not state:
                    state = state_part.split(" ")[0]
    city_state = ", ".join([p for p in [city, state] if p]).strip() or "Unknown"
    missing_services_text = ", ".join(missing_services) if missing_services else "None"
    competitor_text = competitor_name or "None"

    conversion_profile = oi.get("conversion_profile") or {}
    conversion_text = json.dumps(conversion_profile, default=str) if conversion_profile else "—"

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.warning("OPENAI_API_KEY not set; skipping LLM intervention plan")
        return []
    try:
        from openai import OpenAI
        client = OpenAI()
    except ImportError:
        logger.warning("openai package not installed; skipping LLM intervention plan")
        return []

    # Build existing infrastructure context to prevent recommending what already exists
    signals = {k: v for k, v in lead.items() if k.startswith("signal_")}
    existing_infra = []
    if signals.get("signal_has_automated_scheduling") is True:
        existing_infra.append("Online booking: ALREADY EXISTS — do NOT recommend adding it")
    if signals.get("signal_has_contact_form") is True:
        existing_infra.append("Contact form: ALREADY EXISTS")
    if signals.get("signal_has_phone") is True:
        existing_infra.append("Phone prominent: YES")
    if signals.get("signal_has_schema_microdata") is True:
        existing_infra.append("Schema markup: ALREADY IMPLEMENTED — do NOT recommend adding it")
    if signals.get("signal_runs_paid_ads") is True:
        channels = signals.get("signal_paid_ads_channels") or []
        ch_str = ", ".join(str(c) for c in channels) if channels else "unknown"
        existing_infra.append(f"Paid ads: ACTIVE on {ch_str} — do NOT recommend launching paid ads, instead focus on optimizing existing campaigns")
    if signals.get("signal_mobile_friendly") is True:
        existing_infra.append("Mobile optimized: YES")
    existing_infra_text = "\n".join(f"- {x}" for x in existing_infra) if existing_infra else "No infrastructure data available"

    system = """You are generating a 3-step strategic intervention plan for a dental SEO opportunity.

Rules:
- Use only structured intelligence provided.
- Do not invent numbers.
- Do not repeat obvious data.
- Do not give generic SEO advice.
- Each step must directly address the root constraint.
- If strategic_gap exists, at least one step must explicitly address it.
- CRITICAL: Check "Existing Infrastructure" — NEVER recommend adding something that already exists (online booking, schema, paid ads, etc.). Instead, recommend OPTIMIZING or LEVERAGING what exists.
- If paid ads are active, recommend aligning ad traffic to specific service pages rather than launching new campaigns.
- If online booking exists, recommend optimizing the booking flow rather than adding booking.
- Keep actions concise and tactical.
- Maximum 3 steps.
- Be specific to this practice: name actual missing service(s) and the city/market.
- Do not use generic phrases like "high-value service" or "dedicated landing page" without naming the service.
- When a strategic gap exists, reference the competitor by name where relevant.
- Each action should be executable by an SEO for this exact practice and gap profile.
- Never use placeholders such as [Practice], [City], [State], or [Market] in output.
- Example specificity:
  - "Create a dental implants landing page for [Practice] in [City] with before/after and financing options, and add LocalBusiness schema."
  - "Add a dedicated Invisalign page for [Practice] and align existing Google Ads traffic to that page."
- Write at this level of specificity, naming the practice and actual services.
- Return JSON only.

Each step must return:
{
  "step": 1,
  "category": "Trust | Capture | Conversion",
  "action": "<concise tactical action>",
  "time_to_signal_days": <int>,
  "why": "<1 sentence directly tied to constraint or gap>"
}"""

    user = f"""Practice Context:
Practice: {practice_name or "Unknown"}
City: {city_state}
Missing service pages: {missing_services_text}
Nearest competitor in gap: {competitor_text}

Root Constraint:
{root_text}

Primary Growth Vector:
{pgv_text}

Strategic Gap:
{gap_text}

Competitive Profile:
{comp_text}

Service Intelligence:
{service_text}

Cost Leakage Signals:
{json.dumps(cost_leakage, default=str)}

Conversion Profile:
{conversion_text}

Existing Infrastructure (DO NOT recommend adding what already exists):
{existing_infra_text}

Generate exactly 3 steps. Return JSON only: a single JSON array of exactly 3 objects. No commentary."""

    try:
        model = os.getenv("OPENAI_MODEL", INTERVENTION_LLM_MODEL)
        r = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
            timeout=INTERVENTION_LLM_TIMEOUT,
        )
        text = (r.choices[0].message.content or "").strip()
        if text.startswith("```"):
            lines = text.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip().endswith("```"):
                lines[-1] = lines[-1].strip().rstrip("`").strip()
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)
        data = json.loads(text)
        if not isinstance(data, list):
            data = [data] if isinstance(data, dict) else []
        steps = []
        for i, item in enumerate(data[:3]):
            normalized = _normalize_intervention_step(item, i + 1)
            if normalized:
                normalized["action"] = _replace_intervention_placeholders(
                    normalized.get("action", ""),
                    practice_name=practice_name,
                    city=city,
                    state=state,
                )
                normalized["why"] = _replace_intervention_placeholders(
                    normalized.get("why", ""),
                    practice_name=practice_name,
                    city=city,
                    state=state,
                )
                steps.append(normalized)
        return steps[:3]
    except (json.JSONDecodeError, TypeError, KeyError, IndexError) as e:
        logger.warning("Intervention plan LLM parse error: %s", e)
        return []
    except Exception as e:
        logger.warning("Intervention plan LLM request failed: %s", e)
        return []


def build_objective_intelligence(lead: Dict) -> Dict[str, Any]:
    """
    Build single objective_intelligence dict from existing lead data.
    Deterministic only. Omit any subsection when required data is missing.
    """
    obj = _get_obj_layer(lead)
    comp = _get_comp_snapshot(lead)
    svc = _get_service_intel(lead)
    rev = _get_rev_intel(lead)
    signals = _get_signals(lead)

    out: Dict[str, Any] = {}

    # root_constraint
    rbc = obj.get("root_bottleneck_classification") or {}
    bottleneck = rbc.get("bottleneck")
    if bottleneck:
        label = BOTTLENECK_LABEL.get(bottleneck) or bottleneck.replace("_", " ").title()
        out["root_constraint"] = {
            "label": label,
            "why": (rbc.get("why_root_cause") or "").strip(),
            "evidence": list(rbc.get("evidence") or [])[:5],
        }

    # demand_profile, capture_profile, conversion_profile, trust_profile from DCM
    dcm = obj.get("demand_capture_conversion_model") or {}
    for key, dcm_key in (
        ("demand_profile", "demand_signals"),
        ("capture_profile", "capture_signals"),
        ("conversion_profile", "conversion_signals"),
        ("trust_profile", "trust_signals"),
    ):
        block = dcm.get(dcm_key)
        if block and isinstance(block, dict):
            out[key] = {
                "status": (block.get("status") or "").strip(),
                "evidence": list(block.get("evidence") or [])[:5],
            }

    # primary_growth_vector
    rla = obj.get("revenue_leverage_analysis") or {}
    psa = obj.get("primary_sales_anchor") or {}
    if rla.get("highest_leverage_growth_vector"):
        out["primary_growth_vector"] = {
            "label": (rla.get("highest_leverage_growth_vector") or "").strip(),
            "why": (rla.get("primary_revenue_driver_detected") or "").strip(),
        }
    elif psa.get("issue"):
        out["primary_growth_vector"] = {
            "label": (psa.get("issue") or "").strip(),
            "why": (psa.get("why_this_first") or "").strip(),
        }

    # service_intel
    high_ticket = list(svc.get("high_ticket_procedures_detected") or [])
    missing = list(svc.get("missing_high_value_pages") or [])
    schema = signals.get("signal_has_schema_microdata")
    if high_ticket or missing or schema is not None:
        out["service_intel"] = {
            "high_ticket_detected": high_ticket[:15],
            "missing_high_value_pages": missing[:15],
            "schema_detected": schema is True,
        }

    # competitive_profile (map from competitive_snapshot)
    if comp and (comp.get("dentists_sampled") or 0) > 0:
        cs = comp.get("competitor_summary") or {}
        nearest_list = cs.get("nearest_competitors") or []
        out["competitive_profile"] = {
            "dentists_sampled": int(comp.get("dentists_sampled") or 0),
            "radius_used_miles": float(comp.get("search_radius_used_miles") or 2),
            "avg_reviews": float(comp.get("avg_review_count") or 0),
            "avg_rating": float(comp.get("avg_rating") or 0),
            "market_density": (comp.get("market_density_score") or "Low").strip(),
            "review_tier": (comp.get("review_positioning_tier") or "—").strip(),
            "nearest_competitors": [
                {
                    "name": (c.get("name") or "").strip(),
                    "reviews": int(c.get("reviews") or c.get("user_ratings_total") or 0),
                    "rating": c.get("rating"),
                    "distance_miles": c.get("distance_miles"),
                }
                for c in nearest_list[:3]
            ],
        }

    # cost_leakage_signals (merge rev + summary_60s/agency_decision, dedupe)
    cost = list(rev.get("cost_leakage_signals") or [])
    s60 = (lead.get("agency_decision_v1") or {}).get("summary_60s") or lead.get("canonical_summary_v1")
    if s60 and isinstance(s60, dict):
        for s in s60.get("cost_leakage_signals") or []:
            if s and s not in cost:
                cost.append(s)
    seen_lower = set()
    deduped = []
    for s in cost:
        if not s or not isinstance(s, str):
            continue
        k = s.strip().lower()[:80]
        if k in seen_lower:
            continue
        seen_lower.add(k)
        deduped.append(s.strip())
    if deduped:
        out["cost_leakage_signals"] = deduped

    # strategic_gap (deterministic: service capture gap in high-density market)
    strategic = detect_strategic_gap(lead)
    if strategic:
        out["strategic_gap"] = strategic

    # intervention_plan: for non-dental use obj_layer; for dental use LLM only (do not store obj_layer plan)
    if not is_dental_practice(lead):
        plan = obj.get("intervention_plan") or []
        steps = []
        for i, item in enumerate((plan if isinstance(plan, list) else [])[:3]):
            if not isinstance(item, dict):
                continue
            action = (item.get("action") or "").strip()
            if not action:
                continue
            steps.append({
                "step": i + 1,
                "category": _category_norm(item.get("category")),
                "action": action,
                "time_to_signal_days": int(item.get("time_to_signal_days")) if isinstance(item.get("time_to_signal_days"), (int, float)) else 30,
                "why": (item.get("expected_impact") or item.get("why_not_secondaries_yet") or "").strip(),
            })
        if steps:
            out["intervention_plan"] = steps

    # de_risking_questions (list of strings)
    drq = obj.get("de_risking_questions") or []
    questions = []
    for q in drq if isinstance(drq, list) else []:
        if isinstance(q, dict) and q.get("question"):
            questions.append((q.get("question") or "").strip())
        elif isinstance(q, str) and q.strip():
            questions.append(q.strip())
    if questions:
        out["de_risking_questions"] = questions[:5]

    # competitive_service_gap (deterministic: high-margin service missing + schema missing + weak vs nearest competitor)
    gap = detect_competitive_service_gap(lead)
    if gap:
        out["competitive_service_gap"] = gap

    # Dental lead: LLM intervention plan is explicit opt-in (latency control).
    use_llm_intervention = (os.getenv("USE_LLM_INTERVENTION_PLAN", "").strip().lower() in ("1", "true", "yes"))
    if is_dental_practice(lead) and use_llm_intervention:
        llm_plan = generate_intervention_plan_from_intelligence(lead, objective_intelligence=out)
        if llm_plan:
            out["intervention_plan"] = llm_plan

    return out


def build_objective_intelligence_summary(objective_intelligence: Dict[str, Any]) -> str:
    """
    Produce a compact bullet-string for DecisionAgent input.
    No numbers invented; bullets from root_constraint, primary_growth_vector,
    intervention_plan, cost_leakage, competitive one-liner.
    """
    lines = []
    oi = objective_intelligence or {}

    rc = oi.get("root_constraint") or {}
    if rc.get("label"):
        lines.append(f"Root constraint: {rc['label']}")
    if rc.get("why"):
        lines.append(f"Why: {rc['why']}")

    pgv = oi.get("primary_growth_vector") or {}
    if pgv.get("label"):
        lines.append(f"Primary growth vector: {pgv['label']}")

    plan = oi.get("intervention_plan") or []
    for step in plan[:3]:
        if isinstance(step, dict) and step.get("action"):
            lines.append(f"Step {step.get('step', '')} ({step.get('category', '')}): {step['action']}")

    cost = oi.get("cost_leakage_signals") or []
    for c in cost[:3]:
        if c:
            lines.append(f"Cost leakage: {c}")

    comp = oi.get("competitive_profile") or {}
    if comp.get("dentists_sampled"):
        n = comp.get("dentists_sampled")
        r = comp.get("radius_used_miles")
        tier = comp.get("review_tier") or "—"
        lines.append(f"Competitive: {n} practices within {r} mi; review tier {tier}")

    gap = oi.get("competitive_service_gap")
    if gap and isinstance(gap, dict) and gap.get("service"):
        lines.append(f"Competitive service gap: {gap.get('service')} (vs {gap.get('competitor_name', '—')}; schema missing)")
    strat = oi.get("strategic_gap")
    if strat and isinstance(strat, dict) and strat.get("service"):
        lines.append(f"Strategic gap: {strat.get('service')} (vs {strat.get('competitor_name', '—')}; {strat.get('market_density', '')} market)")

    return "\n".join(lines) if lines else "No objective intelligence summary available."
