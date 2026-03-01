"""Natural-language prospect lookup parsing + deterministic Ask criteria helpers."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlsplit

from backend.services.criteria_registry import (
    CRITERIA_REGISTRY,
    SERVICE_SLUGS,
    intent_confidence,
    normalize_accuracy_mode,
    normalize_limit,
    normalize_vertical,
    sanitize_criteria,
    sanitize_must_not,
    semantic_criteria_for_query,
)

EMAIL_RE = re.compile(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})")

STATE_NAME_TO_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}
INVALID_CITY_TOKENS = {"near me", "somewhere", "area", "around", "california", "texas", "florida", "arizona"}
STATE_ABBRS = sorted({v for v in STATE_NAME_TO_ABBR.values()})
STATE_NAME_PATTERN = "|".join(sorted((re.escape(k) for k in STATE_NAME_TO_ABBR.keys()), key=len, reverse=True))
STATE_ABBR_PATTERN = "|".join(sorted((re.escape(x) for x in STATE_ABBRS), key=len, reverse=True))
STATE_TOKEN_PATTERN = rf"(?:{STATE_ABBR_PATTERN}|{STATE_NAME_PATTERN})"

INTENT_NORMALIZER_PROMPT = """You are NeymaIntentNormalizer.Your ONLY job is to convert a user's natural-language SEO prospecting query into a STRICT JSON object that matches the schema below. You must follow these rules:NON-NEGOTIABLE RULES1) Output MUST be valid JSON only. No prose, no markdown, no comments.2) You may ONLY use criteria types from ALLOWED_CRITERIA. Never invent new criteria.3) If the user requests something outside ALLOWED_CRITERIA, you must include it under \"unsupported_parts\" and continue with the closest supported interpretation when safe.4) City and state are REQUIRED. If missing, set \"missing_required\" with items describing what's missing; still return JSON.5) Limit must be an integer between 1 and 20. Default 10.6) Vertical must be one of ALLOWED_VERTICALS. If user asks for something else, map to closest or set \"vertical\" to null and add to unsupported_parts.7) Do NOT claim you verified anything. You are not executing the scan. You are only interpreting.8) Do NOT include any data about actual businesses. No browsing. No guessing.ALLOWED_VERTICALS:[\"dentist\",\"orthodontist\",\"med_spa\",\"chiropractor\",\"hvac\",\"plumber\",\"roofer\",\"law_firm\",\"restaurant\",\"gym\",\"general_local_business\"]ALLOWED_CRITERIA (types):[\"below_review_avg\",\"low_review_count\",\"high_review_gap\",\"runs_paid_ads\",\"no_paid_ads\",\"has_website\",\"no_website\",\"slow_website\",\"no_ssl\",\"mobile_unfriendly\",\"no_contact_form\",\"no_booking\",\"has_booking\",\"missing_service_page\",\"weak_service_depth\",\"no_trust_badges\",\"no_financing_option\",\"high_competition_density\",\"low_competition_density\",\"primary_constraint_visibility\",\"primary_constraint_conversion\",\"primary_constraint_authority\",\"primary_constraint_service_depth\",\"high_modeled_revenue_upside\",\"low_modeled_revenue_upside\"]SERVICES (for missing_service_page): Return a service slug if user names one. Examples: [\"implants\",\"invisalign\",\"orthodontics\",\"veneers\",\"whitening\",\"botox\",\"fillers\",\"iv_therapy\",\"acupuncture\",\"emergency\",\"pediatric\",\"root_canal\",\"crowns\"]ACCURACY MODE: If user mentions \"verified\", \"extreme accuracy\", \"validate\", \"confirm\", set \"accuracy_mode\" to \"verified\". Otherwise \"fast\".CONFIDENCE: Return intent_confidence as:- \"high\" if city+state+vertical+criteria are clearly specified.- \"medium\" if one element is inferred but reasonable (e.g. \"dentists\" implies dentist).- \"low\" if city/state missing or criteria unclear/contradictory.STRICT OUTPUT SCHEMA:{  \"query_raw\": string,  \"city\": string|null,  \"state\": string|null,  \"vertical\": string|null,  \"limit\": number,  \"accuracy_mode\": \"fast\"|\"verified\",  \"criteria\": [ { \"type\": string, \"service\": string|null } ],  \"must_not\": [ { \"type\": string, \"service\": string|null } ],  \"notes_for_executor\": { \"radius_miles_hint\": number|null, \"prioritize\": [string], \"sort_hint\": string|null },  \"unsupported_parts\": [string],  \"missing_required\": [string],  \"intent_confidence\": \"high\"|\"medium\"|\"low\"}MAPPING GUIDELINES:- \"missing implants page\" -> criteria: [{type:\"missing_service_page\", service:\"implants\"}]- \"poor visibility\" / \"low local presence\" -> include below_review_avg or high_review_gap- \"technical SEO issues\" -> include slow_website, no_ssl, mobile_unfriendly when requested- \"conversion issues\" -> include no_booking, no_contact_form, no_trust_badges- \"wasting ad spend\" -> runs_paid_ads + below_review_avg (and optionally high_competition_density)- \"high competition\" -> high_competition_density- If user says \"NOT X\", put it in must_not.Return the JSON now."""

SERVICE_ALIASES = {
    "implants": ["implant", "implants", "dental implant", "dental implants"],
    "invisalign": ["invisalign", "clear aligner", "clear aligners"],
    "orthodontics": ["orthodontic", "orthodontics", "braces"],
    "veneers": ["veneer", "veneers"],
    "whitening": ["whitening", "teeth whitening"],
    "botox": ["botox"],
    "fillers": ["filler", "fillers", "dermal fillers"],
    "iv_therapy": ["iv therapy", "iv drip", "infusion"],
    "acupuncture": ["acupuncture"],
    "emergency": ["emergency", "emergency dentist", "urgent dental"],
    "pediatric": ["pediatric", "kids dentist", "children dentist"],
    "root_canal": ["root canal", "endodontic"],
    "crowns": ["crown", "crowns", "dental crown", "dental crowns"],
}


def _normalize_state_token(token: str | None) -> Optional[str]:
    if not token:
        return None
    t = " ".join(str(token).strip().lower().replace(".", "").split())
    if not t:
        return None
    if len(t) == 2 and t.isalpha():
        return t.upper()
    return STATE_NAME_TO_ABBR.get(t)


def _is_valid_city(city: str | None) -> bool:
    if not city:
        return False
    c = " ".join(city.strip().lower().split(" "))
    if not c or c in INVALID_CITY_TOKENS:
        return False
    if c in {"near", "me"}:
        return False
    if c.endswith(" area"):
        return False
    if c.startswith("somewhere") or c.startswith("around ") or c.startswith("near "):
        return False
    if "near me" in c:
        return False
    if any(tok in c for tok in ("practice", "office", "provider")):
        return False
    return bool(re.search(r"[a-z]", c))


def _extract_city_state(query: str) -> tuple[Optional[str], Optional[str]]:
    q = re.sub(r"[–—]", " ", query)
    matches = list(
        re.finditer(
            rf"([A-Za-z][A-Za-z .'-]{{1,80}}?)\s*,?\s*\(?\s*\b({STATE_TOKEN_PATTERN})\b\s*\)?",
            q,
            flags=re.IGNORECASE,
        )
    )
    for m in reversed(matches):
        city_raw = " ".join(m.group(1).strip(" .,-").split())
        for _ in range(3):
            next_city = re.sub(
                r"^(?:find|show|give(?:\s+me)?|i\s+need|need|want|looking\s+for|top|as\s+many\s+as\s+you\s+can|dentists?|dental\s+practices?|orthodontists?|med\s*spas?|chiropractors?|hvac|plumbers?|roofers?|law\s+firms?|restaurants?|gyms?|practices?|offices?|providers?|in)\b[\s,-]*",
                "",
                city_raw,
                flags=re.IGNORECASE,
            ).strip()
            if next_city == city_raw:
                break
            city_raw = next_city
        city_raw = re.sub(r"^(?:\d{1,3}\s+)", "", city_raw).strip()
        city_raw = re.sub(r"\b(?:that|with|who|where|and)\b.*$", "", city_raw, flags=re.IGNORECASE).strip(" ,.-")
        state_norm = _normalize_state_token(m.group(2))
        if _is_valid_city(city_raw) and state_norm:
            return city_raw, state_norm
    m_city_only = re.search(r"\bin\s+([A-Za-z][A-Za-z .'-]+?)(?:\s+(?:that|with|who|where|and)\b|$)", q, flags=re.IGNORECASE)
    if m_city_only:
        city_only = " ".join(m_city_only.group(1).strip(" .,-").split())
        if _is_valid_city(city_only):
            return city_only, None
    return None, None


def _normalize_query(query: str) -> str:
    q = (query or "").lower()
    q = re.sub(r"[^a-z0-9_\s]", " ", q)
    return " ".join(q.split())


def _extract_limit(ql: str) -> int:
    limit = 10
    m_limit = re.search(r"\b(?:find|top|show|give(?:\s+me)?|need|want)\s+(\d{1,3})\b", ql)
    if not m_limit:
        m_limit = re.search(r"\b(\d{1,3})\s+(?:dentists?|orthodontists?|practices?|providers?|businesses?)\b", ql)
    if m_limit:
        limit = int(m_limit.group(1))
    return normalize_limit(limit)


def _extract_vertical(ql: str) -> str | None:
    vertical_patterns = [
        ("orthodontist", ("orthodontist", "orthodontists", "braces provider", "braces providers")),
        ("dentist", ("dentist", "dentists", "dental clinic", "dental office", "dental practices")),
        ("med_spa", ("med spa", "medspa", "medical spa", "injectables clinic")),
        ("chiropractor", ("chiropractor", "chiropractors", "chiro clinic")),
        ("hvac", ("hvac", "heating and cooling", "ac repair", "air conditioning")),
        ("plumber", ("plumber", "plumbing", "drain service")),
        ("roofer", ("roofer", "roofing", "roof repair")),
        ("law_firm", ("law firm", "attorney", "lawyers", "legal practice")),
        ("restaurant", ("restaurant", "restaurants", "dining")),
        ("gym", ("gym", "fitness center", "crossfit", "pilates studio")),
    ]
    for vertical, phrases in vertical_patterns:
        if any(p in ql for p in phrases):
            return vertical
    return None


def _extract_service_slug(ql: str) -> str | None:
    for slug, aliases in SERVICE_ALIASES.items():
        for alias in aliases:
            alias_re = re.escape(alias)
            if (
                re.search(rf"\b(?:missing|no|without)\s+{alias_re}\s+(?:service\s+)?(?:page|landing page|web ?page)\b", ql)
                or re.search(rf"\b(?:don\'t have|do not have|dont have)\s+(?:an?\s+)?{alias_re}\s+(?:service\s+)?(?:page|landing page|web ?page)\b", ql)
                or re.search(rf"\b{alias_re}\s+page\s+(?:missing|absent)\b", ql)
                or re.search(rf"\bmissing\s+{alias_re}\b", ql)
            ):
                return slug
    m_missing = re.search(r"\bmissing\s+([a-z_ ]+?)\s+page\b", ql)
    if m_missing:
        candidate = m_missing.group(1).strip().replace(" ", "_")
        if candidate in SERVICE_SLUGS:
            return candidate
    return None


def _deterministic_criteria(ql: str) -> list[dict[str, Any]]:
    criteria: list[dict[str, Any]] = []

    if any(
        phrase in ql
        for phrase in (
            "below review average",
            "below review avg",
            "low review",
            "low reviews",
            "bad review",
            "bad reviews",
            "fewer reviews than average",
            "review gap",
            "review deficit",
            "struggling with reviews",
            "under reviewed",
            "under-reviewed",
        )
    ):
        criteria.append({"type": "below_review_avg", "service": None})

    if any(phrase in ql for phrase in ("low review count", "few reviews", "not many reviews")):
        criteria.append({"type": "low_review_count", "service": None})

    if any(phrase in ql for phrase in ("high review gap", "big review gap", "authority gap")):
        criteria.append({"type": "high_review_gap", "service": None})

    if any(phrase in ql for phrase in ("running ads", "runs ads", "google ads", "paid ads", "ad spend")):
        criteria.append({"type": "runs_paid_ads", "service": None})

    if any(phrase in ql for phrase in ("no paid ads", "not running ads", "without ads", "no ads")):
        criteria.append({"type": "no_paid_ads", "service": None})

    if any(phrase in ql for phrase in ("no website", "without website", "without a website", "no site", "missing website", "no web presence")):
        criteria.append({"type": "no_website", "service": None})
    elif any(phrase in ql for phrase in ("has website", "with website", "with a website", "have a website", "has a site", "with site")):
        criteria.append({"type": "has_website", "service": None})

    if any(phrase in ql for phrase in ("slow website", "slow site", "performance issue", "pagespeed")):
        criteria.append({"type": "slow_website", "service": None})

    if any(phrase in ql for phrase in ("no ssl", "not secure", "http only")):
        criteria.append({"type": "no_ssl", "service": None})

    if any(phrase in ql for phrase in ("mobile unfriendly", "not mobile friendly", "mobile issues", "mobile usability")):
        criteria.append({"type": "mobile_unfriendly", "service": None})

    if any(phrase in ql for phrase in ("no contact form", "without contact form", "missing contact form")):
        criteria.append({"type": "no_contact_form", "service": None})

    if any(phrase in ql for phrase in ("no booking", "without booking", "missing booking", "no online booking")):
        criteria.append({"type": "no_booking", "service": None})

    if any(phrase in ql for phrase in ("has booking", "with booking", "online booking")):
        criteria.append({"type": "has_booking", "service": None})

    if any(phrase in ql for phrase in ("weak service depth", "thin service pages", "shallow service pages")):
        criteria.append({"type": "weak_service_depth", "service": None})

    if any(phrase in ql for phrase in ("no trust badges", "missing trust badges")):
        criteria.append({"type": "no_trust_badges", "service": None})

    if any(phrase in ql for phrase in ("no financing", "without financing", "missing financing")):
        criteria.append({"type": "no_financing_option", "service": None})

    if any(phrase in ql for phrase in ("high competition", "competitive market", "vulnerable to competitors")):
        criteria.append({"type": "high_competition_density", "service": None})

    if any(phrase in ql for phrase in ("low competition", "light competition")):
        criteria.append({"type": "low_competition_density", "service": None})

    if "primary constraint is visibility" in ql or "constraint visibility" in ql:
        criteria.append({"type": "primary_constraint_visibility", "service": None})
    if "primary constraint is conversion" in ql or "constraint conversion" in ql:
        criteria.append({"type": "primary_constraint_conversion", "service": None})
    if "primary constraint is authority" in ql or "constraint authority" in ql:
        criteria.append({"type": "primary_constraint_authority", "service": None})
    if "primary constraint is service depth" in ql or "constraint service depth" in ql:
        criteria.append({"type": "primary_constraint_service_depth", "service": None})

    if any(phrase in ql for phrase in ("high modeled upside", "high revenue upside")):
        criteria.append({"type": "high_modeled_revenue_upside", "service": None})

    if any(phrase in ql for phrase in ("low modeled upside", "low revenue upside")):
        criteria.append({"type": "low_modeled_revenue_upside", "service": None})

    service_slug = _extract_service_slug(ql)
    if service_slug:
        criteria.append({"type": "missing_service_page", "service": service_slug})

    return criteria


def _deterministic_must_not(ql: str) -> list[dict[str, Any]]:
    must_not: list[dict[str, Any]] = []
    mapping = {
        "below_review_avg": ["not below review avg", "not below review average"],
        "runs_paid_ads": ["not running ads", "not runs paid ads"],
        "has_website": ["not has website"],
        "no_website": ["not no website"],
        "no_booking": ["not no booking"],
        "no_contact_form": ["not no contact form"],
        "high_competition_density": ["not high competition"],
    }
    for ctype, phrases in mapping.items():
        if any(p in ql for p in phrases):
            must_not.append({"type": ctype, "service": None})
    return must_not


def _unsupported_query_parts(ql: str) -> list[str]:
    out: list[str] = []
    for phrase in (
        "medicaid",
        "weekend",
        "open weekends",
        "saturday",
        "sunday",
        "insurance",
        "ppo",
        "hmo",
        "open late",
        "24 7",
        "parking",
    ):
        if phrase in ql:
            out.append(phrase)
    return out


def _extract_json(text: str) -> dict[str, Any]:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        raw = raw[start:end + 1]
    obj = json.loads(raw)
    if isinstance(obj, dict):
        return obj
    return {}


def normalize_query_with_llm(query: str) -> dict[str, Any]:
    cleaned = " ".join((query or "").strip().split())
    if not cleaned:
        return {}
    try:
        from openai import OpenAI
    except Exception:
        return {}

    try:
        client = OpenAI()
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        r = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": INTENT_NORMALIZER_PROMPT},
                {"role": "user", "content": f'User query: "{cleaned}"'},
            ],
        )
        txt = (r.choices[0].message.content or "") if getattr(r, "choices", None) else ""
        return _extract_json(txt)
    except Exception:
        return {}


def _merge_intents(
    query: str,
    deterministic: dict[str, Any],
    llm_intent: dict[str, Any],
) -> dict[str, Any]:
    unsupported = list(deterministic.get("unsupported_parts") or [])

    city = deterministic.get("city") or llm_intent.get("city")
    state = deterministic.get("state") or llm_intent.get("state")
    vertical = deterministic.get("vertical") or normalize_vertical(llm_intent.get("vertical")) or "general_local_business"
    limit = normalize_limit(deterministic.get("limit") or llm_intent.get("limit") or 10)

    llm_criteria_raw = llm_intent.get("criteria") or []
    llm_must_not_raw = llm_intent.get("must_not") or []
    combined_criteria = list(deterministic.get("criteria") or []) + list(llm_criteria_raw if isinstance(llm_criteria_raw, list) else [])
    combined_must_not = list(deterministic.get("must_not") or []) + list(llm_must_not_raw if isinstance(llm_must_not_raw, list) else [])

    criteria, unsupported = sanitize_criteria(combined_criteria, unsupported)
    must_not, unsupported = sanitize_must_not(combined_must_not, unsupported)

    if isinstance(llm_intent.get("unsupported_parts"), list):
        unsupported.extend(str(x) for x in llm_intent.get("unsupported_parts") if str(x).strip())

    ql = _normalize_query(query)
    acc_from_query = "verified" if any(x in ql for x in ("verified", "extreme accuracy", "validate", "confirm")) else None
    accuracy_mode = normalize_accuracy_mode(acc_from_query or llm_intent.get("accuracy_mode") or deterministic.get("accuracy_mode") or "fast")

    missing_required: list[str] = []
    if not city:
        missing_required.append("city")
    if not state:
        missing_required.append("state")

    confidence = llm_intent.get("intent_confidence") if isinstance(llm_intent.get("intent_confidence"), str) else None
    if confidence not in {"high", "medium", "low"}:
        confidence = intent_confidence(city, state, vertical, criteria)

    notes = llm_intent.get("notes_for_executor") if isinstance(llm_intent.get("notes_for_executor"), dict) else {}
    radius_hint = notes.get("radius_miles_hint")
    try:
        radius_hint = float(radius_hint) if radius_hint is not None else None
    except (TypeError, ValueError):
        radius_hint = None

    prioritize = notes.get("prioritize") if isinstance(notes.get("prioritize"), list) else []
    prioritize = [str(x) for x in prioritize if str(x).strip()][:8]
    sort_hint = str(notes.get("sort_hint") or "").strip() or None

    # deterministic dedupe for unsupported
    unsupported = [str(x).strip() for x in unsupported if str(x).strip()]
    dedup_unsupported: list[str] = []
    seen = set()
    for item in unsupported:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup_unsupported.append(item)

    return {
        "query": " ".join((query or "").strip().split()),
        "query_raw": " ".join((query or "").strip().split()),
        "city": city,
        "state": state,
        "vertical": vertical,
        "limit": limit,
        "accuracy_mode": accuracy_mode,
        "criteria": criteria,
        "must_not": must_not,
        "unsupported_parts": dedup_unsupported,
        "unsupported_request_parts": dedup_unsupported,
        "missing_required": missing_required,
        "intent_confidence": confidence,
        "requires_lightweight": any(c.get("type") == "missing_service_page" for c in criteria),
        "requires_deep": False,
        "notes_for_executor": {
            "radius_miles_hint": radius_hint,
            "prioritize": prioritize,
            "sort_hint": sort_hint,
        },
    }


def resolve_ask_intent(query: str) -> Dict[str, Any]:
    q = " ".join((query or "").strip().split())
    if not q:
        raise ValueError("Query is required")
    ql = _normalize_query(q)

    city, state = _extract_city_state(q)
    vertical = _extract_vertical(ql) or "dentist"
    limit = _extract_limit(ql)

    deterministic = {
        "city": city,
        "state": state,
        "vertical": normalize_vertical(vertical) or "general_local_business",
        "limit": limit,
        "accuracy_mode": "verified" if any(x in ql for x in ("verified", "extreme accuracy", "validate", "confirm")) else "fast",
        "criteria": _deterministic_criteria(ql) + semantic_criteria_for_query(ql),
        "must_not": _deterministic_must_not(ql),
        "unsupported_parts": _unsupported_query_parts(ql),
    }

    deterministic["criteria"], deterministic["unsupported_parts"] = sanitize_criteria(
        deterministic.get("criteria"), deterministic.get("unsupported_parts")
    )
    deterministic["must_not"], deterministic["unsupported_parts"] = sanitize_must_not(
        deterministic.get("must_not"), deterministic.get("unsupported_parts")
    )

    deterministic_conf = intent_confidence(
        deterministic.get("city"),
        deterministic.get("state"),
        deterministic.get("vertical"),
        deterministic.get("criteria") or [],
    )

    # Semantic-map and regex resolved, no need for LLM if high confidence.
    if deterministic_conf == "high":
        out = _merge_intents(q, deterministic, {})
        out["intent_confidence"] = "high"
        return out

    llm_raw = normalize_query_with_llm(q)
    return _merge_intents(q, deterministic, llm_raw)


def parse_npl_query(query: str) -> Dict[str, Any]:
    """Backward-compatible parser alias used by existing routes/tests."""
    return resolve_ask_intent(query)


def classify_constraint(signals: Dict[str, Any]) -> str:
    """Deterministic primary-constraint classifier from available row signals."""
    reviews = float(signals.get("user_ratings_total") or 0)
    below_review_avg = bool(signals.get("below_review_avg"))
    has_website = bool(signals.get("has_website"))
    no_contact_form = not bool(signals.get("has_contact_form"))
    no_ssl = not bool(signals.get("ssl"))
    no_schema = not bool(signals.get("has_schema"))
    mobile_unfriendly = not bool(signals.get("has_viewport"))

    visibility_score = 1.0 if below_review_avg else 0.0
    conversion_score = (1.2 if no_contact_form else 0.0) + (0.8 if not has_website else 0.0)
    authority_score = (1.0 if below_review_avg else 0.0) + (0.7 if reviews < 50 else 0.0)
    service_depth_score = (0.8 if no_schema else 0.0) + (0.6 if no_ssl else 0.0) + (0.4 if mobile_unfriendly else 0.0)

    scores = {
        "visibility": visibility_score,
        "conversion": conversion_score,
        "authority": authority_score,
        "service_depth": service_depth_score,
    }
    return max(scores, key=lambda k: scores[k])


def _matches_one_criterion(criterion: Dict[str, Any], row: Dict[str, Any]) -> bool:
    ctype = str(criterion.get("type") or "").strip()
    if ctype not in CRITERIA_REGISTRY:
        return False

    reviews = int(row.get("user_ratings_total") or 0)
    avg_reviews = float(row.get("avg_market_reviews") or 0)
    rank_key = float(row.get("rank_key") or 0)
    primary_constraint = str(row.get("primary_constraint") or "").strip().lower()

    if ctype == "below_review_avg":
        return bool(row.get("below_review_avg"))
    if ctype == "low_review_count":
        return reviews < 50
    if ctype == "high_review_gap":
        return avg_reviews > 0 and reviews < (avg_reviews * 0.5)
    if ctype == "runs_paid_ads":
        return bool(row.get("runs_paid_ads"))
    if ctype == "no_paid_ads":
        return row.get("runs_paid_ads") is False
    if ctype == "has_website":
        return bool(row.get("has_website"))
    if ctype == "no_website":
        return not bool(row.get("has_website"))
    if ctype == "slow_website":
        val = row.get("page_load_ms")
        try:
            return float(val) >= 2500
        except (TypeError, ValueError):
            return False
    if ctype == "no_ssl":
        return not bool(row.get("ssl"))
    if ctype == "mobile_unfriendly":
        return not bool(row.get("has_viewport"))
    if ctype == "no_contact_form":
        return not bool(row.get("has_contact_form"))
    if ctype == "no_booking":
        return not bool(row.get("has_booking"))
    if ctype == "has_booking":
        return bool(row.get("has_booking"))
    if ctype == "missing_service_page":
        # This one is handled by lightweight/verified checks upstream.
        return bool(row.get("missing_service_page_match"))
    if ctype == "weak_service_depth":
        return not bool(row.get("has_schema"))
    if ctype == "no_trust_badges":
        return bool(row.get("no_trust_badges"))
    if ctype == "no_financing_option":
        return bool(row.get("no_financing_option"))
    if ctype == "high_competition_density":
        return str(row.get("market_density") or "").lower() == "high"
    if ctype == "low_competition_density":
        return str(row.get("market_density") or "").lower() == "low"
    if ctype == "primary_constraint_visibility":
        return primary_constraint == "visibility"
    if ctype == "primary_constraint_conversion":
        return primary_constraint == "conversion"
    if ctype == "primary_constraint_authority":
        return primary_constraint == "authority"
    if ctype == "primary_constraint_service_depth":
        return primary_constraint == "service_depth"
    if ctype == "high_modeled_revenue_upside":
        return rank_key >= 70.0
    if ctype == "low_modeled_revenue_upside":
        return rank_key < 50.0

    return False


def matches_tier1_criteria(criteria: List[Dict[str, Any]], row: Dict[str, Any]) -> bool:
    """Match criteria that can be evaluated from deterministic candidate row data."""
    if not criteria:
        return True
    for criterion in criteria:
        if not _matches_one_criterion(criterion, row):
            return False
    return True


def needs_lightweight_check(criteria: List[Dict[str, Any]]) -> bool:
    return any(str(c.get("type")) in {"missing_service_page", "missing_service_page_light"} for c in criteria)


def criterion_cache_key(criterion: Dict[str, Any]) -> str:
    ctype = str(criterion.get("type") or "unknown")
    if ctype == "missing_service_page_light":
        ctype = "missing_service_page"
    service = str(criterion.get("service") or "").strip().lower().replace(" ", "_")
    return f"{ctype}:{service}" if service else ctype


def run_lightweight_service_page_check(
    website: Optional[str],
    criterion: Dict[str, Any],
    timeout_seconds: int = 5,
) -> Dict[str, Any]:
    """Strict deterministic service page check.

    For "missing service page" criteria, matches=True means missing/weak_stub.
    """
    _ = timeout_seconds  # retained for compatibility
    service = str(criterion.get("service") or "").strip().lower()
    if not website or not service:
        return {
            "criterion": criterion,
            "matches": False,
            "reason": "missing website or service",
            "service": service,
            "service_mentioned": False,
            "dedicated_page_detected": False,
        }
    try:
        from pipeline.service_depth import run_strict_single_service_page_check
        strict = run_strict_single_service_page_check(
            website_url=website,
            service_slug=service,
            city=None,
            state=None,
            vertical="dentist",
        )
    except Exception:
        return {
            "criterion": criterion,
            "matches": False,
            "reason": "strict detector failed",
            "service": service,
            "service_mentioned": False,
            "dedicated_page_detected": False,
        }

    matches = bool(strict.get("matches"))
    status = str(strict.get("qualification_status") or "missing")
    return {
        "criterion": criterion,
        "matches": matches,
        "reason": str(strict.get("reason") or strict.get("detection_reason") or ""),
        "service": service,
        "service_mentioned": bool(status != "missing"),
        "dedicated_page_detected": bool(strict.get("page_detected")),
        "qualification_status": status,
        "url": strict.get("url"),
        "word_count": strict.get("word_count"),
        "keyword_density": strict.get("keyword_density"),
        "h1_match": strict.get("h1_match"),
        "structural_signals": strict.get("structural_signals"),
        "internal_links_to_page": strict.get("internal_links_to_page"),
        "debug": strict.get("debug"),
    }
