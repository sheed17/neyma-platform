"""
Dentist LLM reasoning layer: read-only synthesis for SEO agency opportunity.

Consumes enriched JSON + dentist_profile_v1 + context_dimensions + lead_score/priority.
Does NOT mutate scores or signals. Output is for interpretability only.
Guardrails: discard output if it invents data or contradicts deterministic flags.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gpt-4o-mini"
REQUEST_TIMEOUT = 45

SYSTEM_PROMPT = """You are NeymaDentistIntel, a strict evidence-bound strategist.

You MUST use ONLY the provided context payload.
Rules:
- Use ONLY Current Lead Facts for claims about THIS business.
- Cohort/Similar docs are pattern evidence only; never attribute their facts directly to this lead.
- Do not invent numbers, entities, services, tools, or outcomes.
- Do not output markdown or prose outside JSON.
- If evidence is weak, lower confidence and include explicit risks.

Return one JSON object that exactly follows the required keys."""

USER_PROMPT_TEMPLATE = """Produce intelligence for outbound prioritization from this payload.

Required JSON keys exactly:
- "primary_constraint": string
- "primary_leverage": string
- "contact_priority": string ("high" | "medium" | "low")
- "outreach_angle": string
- "confidence": number (0..1)
- "evidence": array of objects, each with:
  - "source_type": "current_lead_fact" | "cohort_stat" | "similar_doc" | "outcome_pattern"
  - "source_key": string
  - "note": string
- "risks": array of strings

Important:
- Evidence.source_key must reference keys present in the provided payload:
  - Current lead fact keys
  - cohort stats keys
  - similar doc ids as "similar_doc:<id>"
  - outcome pattern keys

Payload:
{{STRUCTURED_JSON}}"""

REQUIRED_KEYS = [
    "primary_constraint",
    "primary_leverage",
    "contact_priority",
    "outreach_angle",
    "evidence",
    "risks",
    "confidence",
]


def _get_client():
    try:
        from openai import OpenAI
        return OpenAI()
    except ImportError:
        return None


def _build_llm_input(
    business_snapshot: Dict[str, Any],
    dentist_profile_v1: Dict[str, Any],
    context_dimensions: List[Dict],
    lead_score: Optional[int],
    priority: Optional[str],
    confidence: Optional[float],
    rag_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the strict input contract for the LLM (no raw HTML, no scraped blobs)."""
    current_lead_facts = {
        "business_snapshot": {
            "name": business_snapshot.get("name"),
            "place_id": business_snapshot.get("place_id"),
            "signal_rating": business_snapshot.get("signal_rating"),
            "signal_review_count": business_snapshot.get("signal_review_count"),
            "signal_has_website": business_snapshot.get("signal_has_website"),
            "signal_has_automated_scheduling": business_snapshot.get("signal_has_automated_scheduling"),
            "signal_has_phone": business_snapshot.get("signal_has_phone"),
            "signal_has_contact_form": business_snapshot.get("signal_has_contact_form"),
            "signal_review_summary_text": (business_snapshot.get("signal_review_summary_text") or "")[:2000],
            "signal_last_review_days_ago": business_snapshot.get("signal_last_review_days_ago"),
            "signal_extraction_method": business_snapshot.get("signal_extraction_method"),
        },
        "dentist_profile_v1": dentist_profile_v1,
        "context_dimensions": context_dimensions,
        "lead_score": lead_score,
        "priority": priority,
        "confidence": confidence,
    }
    return {
        "current_lead_facts": current_lead_facts,
        "cohort_stats": (rag_context or {}).get("cohort", {}),
        "similar_lead_evidence": (rag_context or {}).get("similar_docs", []),
        "outcome_patterns": (rag_context or {}).get("outcome_patterns", {}),
        "guardrails": (rag_context or {}).get("guardrails", {}),
    }


def _contradicts_deterministic(llm_output: Dict[str, Any], dentist_profile_v1: Dict[str, Any], priority: Optional[str]) -> bool:
    """Return True if LLM output contradicts deterministic flags (discard)."""
    agency_fit = (dentist_profile_v1 or {}).get("agency_fit_reasoning") or {}
    ideal = agency_fit.get("ideal_for_seo_outreach")
    if ideal is None:
        return False
    # Heuristic: if we said ideal_for_seo_outreach is True, LLM should not say "not worth pursuing" in summary
    summary = (llm_output.get("executive_summary") or "").lower()
    if ideal is True and ("not worth" in summary or "do not pursue" in summary or "skip this" in summary):
        return True
    if ideal is False and ("highly recommended" in summary and "no risk" in summary):
        return True
    return False


def _references_nonexistent(
    llm_output: Dict[str, Any],
    business_snapshot: Dict,
    dentist_profile_v1: Dict,
    rag_context: Optional[Dict[str, Any]] = None,
) -> bool:
    """Return True if LLM invents data (e.g. specific numbers or facts not in input)."""
    conf = llm_output.get("confidence")
    if conf is not None and (not isinstance(conf, (int, float)) or conf < 0 or conf > 1):
        return True
    for key in REQUIRED_KEYS:
        if key not in llm_output:
            return True
    evidence = llm_output.get("evidence")
    if not isinstance(evidence, list):
        return True
    allowed_source_types = {"current_lead_fact", "cohort_stat", "similar_doc", "outcome_pattern"}
    for item in evidence:
        if not isinstance(item, dict):
            return True
        source_type = str(item.get("source_type") or "").strip()
        source_key = str(item.get("source_key") or "").strip()
        if source_type not in allowed_source_types or not source_key:
            return True
        if source_type == "similar_doc" and not source_key.startswith("similar_doc:"):
            return True
    return False


def _deterministic_fallback(
    business_snapshot: Dict[str, Any],
    priority: Optional[str],
    confidence: Optional[float],
    rag_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Fail-safe output when LLM is unavailable or response is invalid.
    Uses only deterministic fields already present in payload.
    """
    has_form = business_snapshot.get("signal_has_contact_form")
    has_booking = business_snapshot.get("signal_has_automated_scheduling")
    reviews = business_snapshot.get("signal_review_count")
    paid = business_snapshot.get("signal_runs_paid_ads")

    primary_constraint = "Conversion capture clarity"
    if has_form is False and has_booking is False:
        primary_constraint = "Lead capture friction"
    elif reviews is not None and isinstance(reviews, (int, float)) and reviews < 60:
        primary_constraint = "Review authority gap"

    primary_leverage = "Tighten high-intent service page + conversion path"
    if paid is True:
        primary_leverage = "Improve conversion path to monetize existing demand"

    priority_norm = str(priority or "").strip().lower()
    contact_priority = "medium"
    if priority_norm in {"priority", "high"}:
        contact_priority = "high"
    elif priority_norm in {"low", "deprioritize"}:
        contact_priority = "low"

    conf = 0.45
    if isinstance(confidence, (int, float)):
        conf = round(max(0.0, min(1.0, float(confidence))), 2)

    evidence = [
        {"source_type": "current_lead_fact", "source_key": "signal_has_contact_form", "note": f"value={has_form}"},
        {"source_type": "current_lead_fact", "source_key": "signal_has_automated_scheduling", "note": f"value={has_booking}"},
        {"source_type": "current_lead_fact", "source_key": "signal_review_count", "note": f"value={reviews}"},
    ]
    risks = ["Evidence limited; recommendation is conservative."]
    if rag_context and (rag_context.get("cohort") or {}).get("cohort_count", 0) == 0:
        risks.append("No historical cohort available for pattern weighting.")

    out = {
        "primary_constraint": primary_constraint,
        "primary_leverage": primary_leverage,
        "contact_priority": contact_priority,
        "outreach_angle": "Lead with one concrete growth bottleneck and a 30-day fix path.",
        "confidence": conf,
        "evidence": evidence,
        "risks": risks[:4],
    }
    out = _with_legacy_fields(out)
    out["rag_used"] = bool((rag_context or {}).get("metrics", {}).get("rag_used"))
    out["retrieval_time_ms"] = (rag_context or {}).get("metrics", {}).get("retrieval_time_ms")
    out["num_similar_docs"] = (rag_context or {}).get("metrics", {}).get("num_similar_docs")
    out["cohort_count"] = ((rag_context or {}).get("cohort") or {}).get("cohort_count", 0)
    out["cohort_close_rate"] = (((rag_context or {}).get("cohort") or {}).get("cohort_stats") or {}).get("close_rate")
    out["top_constraints"] = (((rag_context or {}).get("cohort") or {}).get("top_constraints") or [])[:3]
    out["top_outreach_angles"] = (((rag_context or {}).get("cohort") or {}).get("top_outreach_angles") or [])[:3]
    out["similar_leads_count"] = len((rag_context or {}).get("similar_docs") or [])
    return out


def _with_legacy_fields(out: Dict[str, Any]) -> Dict[str, Any]:
    """Add legacy keys expected by existing downstream renderers."""
    primary_constraint = out.get("primary_constraint") or "Unknown"
    primary_leverage = out.get("primary_leverage") or "Unknown"
    outreach_angle = out.get("outreach_angle") or ""
    risks = out.get("risks") if isinstance(out.get("risks"), list) else []
    evidence = out.get("evidence") if isinstance(out.get("evidence"), list) else []
    evidence_lines = [str((e or {}).get("note") or (e or {}).get("source_key") or "").strip() for e in evidence]
    evidence_lines = [x for x in evidence_lines if x][:4]

    out["executive_summary"] = (
        f"Primary constraint: {primary_constraint}. "
        f"Primary leverage: {primary_leverage}. "
        f"Recommended angle: {outreach_angle}".strip()
    )[:500]
    out["seo_viability_reasoning"] = evidence_lines[:5]
    out["revenue_opportunities"] = [primary_leverage][:3]
    out["risk_objections"] = [str(x) for x in risks[:4]]
    out["recommended_outreach_angle"] = outreach_angle[:300]
    return out


def dentist_llm_reasoning_layer(
    business_snapshot: Dict[str, Any],
    dentist_profile_v1: Dict[str, Any],
    context_dimensions: List[Dict],
    lead_score: Optional[int] = None,
    priority: Optional[str] = None,
    confidence: Optional[float] = None,
    rag_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Call LLM to produce llm_reasoning_layer for dentist opportunity.

    Returns the required schema or empty dict on failure/guardrail discard.
    Never mutates lead_score, priority, or any deterministic field.
    """
    if not dentist_profile_v1:
        return {}
    if os.getenv("USE_LLM_DENTIST_REASONING", "").strip().lower() not in ("1", "true", "yes"):
        return _deterministic_fallback(business_snapshot, priority=priority, confidence=confidence, rag_context=rag_context)
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.warning("OPENAI_API_KEY not set; skipping dentist LLM reasoning layer")
        return _deterministic_fallback(business_snapshot, priority=priority, confidence=confidence, rag_context=rag_context)

    client = _get_client()
    if not client:
        logger.warning("openai package not installed; skipping dentist LLM reasoning layer")
        return _deterministic_fallback(business_snapshot, priority=priority, confidence=confidence, rag_context=rag_context)

    payload = _build_llm_input(
        business_snapshot,
        dentist_profile_v1,
        context_dimensions,
        lead_score,
        priority,
        confidence,
        rag_context,
    )
    structured_json = json.dumps(payload, indent=2)
    user_prompt = USER_PROMPT_TEMPLATE.replace("{{STRUCTURED_JSON}}", structured_json)

    try:
        model = os.getenv("OPENAI_MODEL", DEFAULT_MODEL)
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            response_format={"type": "json_object"},
            timeout=REQUEST_TIMEOUT,
        )
        choice = response.choices[0] if response.choices else None
        if not choice or not getattr(choice, "message", None):
            return {}
        text = (choice.message.content or "").strip()
        if text.startswith("```"):
            lines = text.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)
        data = json.loads(text)
        # Normalize to required schema (map common LLM key variants)
        data = _normalize_llm_response_keys(data)
        out = {
            "primary_constraint": str(data.get("primary_constraint") or "").strip(),
            "primary_leverage": str(data.get("primary_leverage") or "").strip(),
            "contact_priority": _normalize_priority(data.get("contact_priority")),
            "outreach_angle": str(data.get("outreach_angle") or "").strip(),
            "evidence": _normalize_evidence(data.get("evidence")),
            "risks": _ensure_list(data.get("risks")),
            "confidence": _clamp_confidence(data.get("confidence")),
        }
        out = _with_legacy_fields(out)
        out["rag_used"] = bool((rag_context or {}).get("metrics", {}).get("rag_used"))
        out["retrieval_time_ms"] = (rag_context or {}).get("metrics", {}).get("retrieval_time_ms")
        out["num_similar_docs"] = (rag_context or {}).get("metrics", {}).get("num_similar_docs")
        out["cohort_count"] = ((rag_context or {}).get("cohort") or {}).get("cohort_count", 0)
        out["cohort_close_rate"] = (((rag_context or {}).get("cohort") or {}).get("cohort_stats") or {}).get("close_rate")
        out["top_constraints"] = (((rag_context or {}).get("cohort") or {}).get("top_constraints") or [])[:3]
        out["top_outreach_angles"] = (((rag_context or {}).get("cohort") or {}).get("top_outreach_angles") or [])[:3]
        out["similar_leads_count"] = len((rag_context or {}).get("similar_docs") or [])
        if _references_nonexistent(out, business_snapshot, dentist_profile_v1, rag_context=rag_context):
            logger.warning("Dentist LLM output failed guardrail: references nonexistent data; discarding")
            return _deterministic_fallback(business_snapshot, priority=priority, confidence=confidence, rag_context=rag_context)
        if _contradicts_deterministic(out, dentist_profile_v1, priority):
            logger.warning("Dentist LLM output failed guardrail: contradicts deterministic flags; discarding")
            return _deterministic_fallback(business_snapshot, priority=priority, confidence=confidence, rag_context=rag_context)
        return out
    except json.JSONDecodeError as e:
        logger.warning("Dentist LLM response parse error: %s; discarding. Response may not be valid JSON.", e)
        return _deterministic_fallback(business_snapshot, priority=priority, confidence=confidence, rag_context=rag_context)
    except Exception as e:
        logger.warning("Dentist LLM request failed: %s; discarding", e)
        return _deterministic_fallback(business_snapshot, priority=priority, confidence=confidence, rag_context=rag_context)


def _normalize_llm_response_keys(data: Dict[str, Any]) -> Dict[str, Any]:
    """Map common LLM key variants to our schema so we accept alternate responses."""
    key_map = {
        "constraint": "primary_constraint",
        "root_constraint": "primary_constraint",
        "leverage": "primary_leverage",
        "recommended_outreach_angle": "outreach_angle",
        "recommended_angle": "outreach_angle",
        "pitch_angle": "outreach_angle",
        "risk_objections": "risks",
        "objections": "risks",
    }
    list_keys = {"risks"}
    out = dict(data)
    for alt, canonical in key_map.items():
        if alt in out and canonical not in out:
            val = out[alt]
            if canonical in list_keys and isinstance(val, str):
                val = [val]
            out[canonical] = val
    return out


def _ensure_list(val: Any) -> List[str]:
    if isinstance(val, list):
        return [str(x).strip() for x in val if x][:10]
    return []


def _clamp_confidence(val: Any) -> float:
    if isinstance(val, (int, float)):
        return round(max(0.0, min(1.0, float(val))), 2)
    return 0.0


def _normalize_priority(val: Any) -> str:
    s = str(val or "").strip().lower()
    if s in {"high", "h"}:
        return "high"
    if s in {"low", "l"}:
        return "low"
    return "medium"


def _normalize_evidence(val: Any) -> List[Dict[str, str]]:
    if not isinstance(val, list):
        return []
    out: List[Dict[str, str]] = []
    for item in val[:10]:
        if not isinstance(item, dict):
            continue
        source_type = str(item.get("source_type") or "").strip()
        source_key = str(item.get("source_key") or "").strip()
        note = str(item.get("note") or "").strip()
        if not source_type or not source_key:
            continue
        if source_type == "similar_doc" and not source_key.startswith("similar_doc:"):
            source_key = f"similar_doc:{source_key}"
        out.append(
            {
                "source_type": source_type,
                "source_key": source_key,
                "note": note[:240],
            }
        )
    return out
