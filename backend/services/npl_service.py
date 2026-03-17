"""Natural-language prospect lookup parsing + deterministic Ask criteria helpers."""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlsplit

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

INTENT_NORMALIZER_PROMPT = """You are NeymaIntentNormalizer.Your ONLY job is to convert a user's natural-language SEO prospecting query into a STRICT JSON object that matches the schema below. You must follow these rules:NON-NEGOTIABLE RULES1) Output MUST be valid JSON only. No prose, no markdown, no comments.2) You may ONLY use criteria types from ALLOWED_CRITERIA. Never invent new criteria.3) If the user requests something outside ALLOWED_CRITERIA, you must include it under \"unsupported_parts\" and continue with the closest supported interpretation when safe.4) City and state are REQUIRED. If missing, set \"missing_required\" with items describing what's missing; still return JSON.5) Limit must be an integer between 1 and 20. Default 10.6) Vertical must be one of ALLOWED_VERTICALS. If user asks for something else, map to closest or set \"vertical\" to null and add to unsupported_parts.7) Do NOT claim you verified anything. You are not executing the scan. You are only interpreting.8) Do NOT include any data about actual businesses. No browsing. No guessing.ALLOWED_VERTICALS:[\"dentist\",\"orthodontist\",\"med_spa\",\"chiropractor\",\"hvac\",\"plumber\",\"roofer\",\"law_firm\",\"restaurant\",\"gym\",\"general_local_business\"]ALLOWED_CRITERIA (types):[\"below_review_avg\",\"low_review_count\",\"high_review_gap\",\"runs_paid_ads\",\"no_paid_ads\",\"has_website\",\"no_website\",\"slow_website\",\"no_ssl\",\"mobile_unfriendly\",\"no_contact_form\",\"no_booking\",\"has_booking\",\"missing_service_page\",\"weak_service_depth\",\"no_trust_badges\",\"no_financing_option\",\"high_competition_density\",\"low_competition_density\",\"primary_constraint_visibility\",\"primary_constraint_conversion\",\"primary_constraint_authority\",\"primary_constraint_service_depth\",\"high_modeled_revenue_upside\",\"low_modeled_revenue_upside\"]SERVICES (for missing_service_page): Return a service slug if user names one. Examples: [\"implants\",\"invisalign\",\"orthodontics\",\"veneers\",\"whitening\",\"botox\",\"fillers\",\"iv_therapy\",\"acupuncture\",\"emergency\",\"pediatric\",\"root_canal\",\"crowns\"]ACCURACY MODE: If user mentions \"verified\", \"extreme accuracy\", \"validate\", \"confirm\", set \"accuracy_mode\" to \"verified\". Otherwise \"fast\".CONFIDENCE: Return intent_confidence as:- \"high\" if city+state+vertical+criteria are clearly specified.- \"medium\" if one element is inferred but reasonable (e.g. \"dentists\" implies dentist).- \"low\" if city/state missing or criteria unclear/contradictory.STRICT OUTPUT SCHEMA:{  \"query_raw\": string,  \"city\": string|null,  \"state\": string|null,  \"vertical\": string|null,  \"limit\": number,  \"accuracy_mode\": \"fast\"|\"verified\",  \"criteria\": [ { \"type\": string, \"service\": string|null } ],  \"must_not\": [ { \"type\": string, \"service\": string|null } ],  \"notes_for_executor\": { \"radius_miles_hint\": number|null, \"prioritize\": [string], \"sort_hint\": string|null },  \"unsupported_parts\": [string],  \"missing_required\": [string],  \"intent_confidence\": \"high\"|\"medium\"|\"low\"}MAPPING GUIDELINES:- \"missing implants page\" -> criteria: [{type:\"missing_service_page\", service:\"implants\"}]- \"no website\", \"without a website\", \"don't have a website\" -> criteria: [{type:\"no_website\", service:null}]- \"poor visibility\" / \"low local presence\" -> include below_review_avg or high_review_gap- \"technical SEO issues\" -> include slow_website, no_ssl, mobile_unfriendly when requested- \"conversion issues\" -> include no_booking, no_contact_form, no_trust_badges- \"wasting ad spend\" -> runs_paid_ads + below_review_avg (and optionally high_competition_density)- \"high competition\" -> high_competition_density- If user says \"NOT X\", put it in must_not.Return the JSON now."""

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
                or re.search(rf"\b(?:don\'t have|don t have|do not have|dont have)\s+(?:an?\s+)?{alias_re}\s+(?:service\s+)?(?:page|landing page|web ?page)\b", ql)
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

    if (
        any(phrase in ql for phrase in ("no website", "without website", "without a website", "no site", "missing website", "no web presence"))
        or "without a site" in ql
        or re.search(r"\b(?:do not|don t|dont|don't)\s+have\s+(?:a\s+)?(?:website|site|web presence)\b", ql)
        or re.search(r"\b(?:has|have)\s+no\s+(?:website|site|web presence)\b", ql)
    ):
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


def _is_true(value: Any) -> bool:
    return value is True


def _is_false(value: Any) -> bool:
    return value is False


def classify_constraint(signals: Dict[str, Any]) -> str:
    """Deterministic primary-constraint classifier from available row signals."""
    reviews = float(signals.get("user_ratings_total") or 0)
    below_review_avg = bool(signals.get("below_review_avg"))
    has_website = bool(signals.get("has_website"))
    no_contact_form = _is_false(signals.get("has_contact_form"))
    no_ssl = _is_false(signals.get("ssl"))
    no_schema = _is_false(signals.get("has_schema"))
    mobile_unfriendly = _is_false(signals.get("has_viewport"))

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
    booking_path = str(row.get("booking_conversion_path") or "").strip()

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
        return _is_false(row.get("ssl"))
    if ctype == "mobile_unfriendly":
        return _is_false(row.get("has_viewport"))
    if ctype == "no_contact_form":
        return _is_false(row.get("has_contact_form"))
    if ctype == "no_booking":
        return _is_false(row.get("has_booking")) or booking_path in {"Phone-only", "Request form"}
    if ctype == "has_booking":
        return _is_true(row.get("has_booking")) or booking_path in {"Online booking (limited)", "Online booking (full)"}
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
    """Fast deterministic service page check.

    For "missing service page" criteria, matches=True means likely missing.
    This intentionally prefers speed; strict confirmation happens in deep verification.
    """
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
    aliases = list(SERVICE_ALIASES.get(service) or [service.replace("_", " ")])
    aliases = [str(a).strip().lower() for a in aliases if str(a).strip()]
    if not aliases:
        aliases = [service.replace("_", " ")]

    def _normalize_site(url: str) -> str:
        u = str(url or "").strip()
        if not u:
            return ""
        if not u.startswith(("http://", "https://")):
            u = "https://" + u
        parsed = urlsplit(u)
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc.lower()
        path = parsed.path or "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return f"{scheme}://{netloc}{path}"

    def _same_domain(a: str, b: str) -> bool:
        pa = urlsplit(a)
        pb = urlsplit(b)
        da = (pa.netloc or "").lower().replace("www.", "")
        db = (pb.netloc or "").lower().replace("www.", "")
        return bool(da and db and da == db)

    def _path_has_alias(path_or_url: str) -> bool:
        path = (urlsplit(path_or_url).path or "").lower().replace("_", "-")
        path_tokens = set(re.findall(r"[a-z0-9]+", path))
        compact = "".join(path_tokens)
        for alias in aliases:
            alias_tokens = re.findall(r"[a-z0-9]+", alias)
            if not alias_tokens:
                continue
            if len(alias_tokens) == 1:
                tok = alias_tokens[0]
                if tok in path_tokens or tok in path:
                    return True
            elif all(tok in path_tokens for tok in alias_tokens):
                return True
            alias_compact = "".join(alias_tokens)
            if alias_compact and alias_compact in compact:
                return True
        return False

    def _text_has_alias(text: str) -> bool:
        lower = str(text or "").lower()
        for alias in aliases:
            if re.search(rf"\b{re.escape(alias)}\b", lower):
                return True
        return False

    try:
        import requests
        base = _normalize_site(website)
        if not base:
            raise ValueError("invalid website")
        r = requests.get(
            base,
            timeout=max(2, int(timeout_seconds)),
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                )
            },
        )
        if int(r.status_code or 0) >= 400:
            return {
                "criterion": criterion,
                "matches": False,
                "reason": f"lightweight_http_{r.status_code}",
                "service": service,
                "service_mentioned": False,
                "dedicated_page_detected": False,
                "method": "fast_homepage",
                "evidence": {"homepage_url": base},
            }

        html = str(r.text or "")
        homepage_url = _normalize_site(str(r.url or base))
        title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
        title = re.sub(r"<[^>]+>", " ", title_m.group(1) if title_m else "")
        title = re.sub(r"\s+", " ", title).strip()
        h1_text = " ".join(
            re.sub(r"<[^>]+>", " ", m.group(1))
            for m in re.finditer(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S)
        )
        h1_text = re.sub(r"\s+", " ", h1_text).strip()
        plain = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", html, flags=re.I)
        plain = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", plain, flags=re.I)
        plain = re.sub(r"<[^>]+>", " ", plain)
        plain = re.sub(r"\s+", " ", plain).strip()
        excerpt = plain[:600]
        internal_paths: List[str] = []
        for m in re.finditer(r"<a[^>]*href\s*=\s*[\"']([^\"'#]+)[\"'][^>]*>", html, re.I | re.S):
            href = (m.group(1) or "").strip()
            if not href:
                continue
            abs_url = _normalize_site(urljoin(homepage_url, href))
            if not abs_url or not _same_domain(homepage_url, abs_url):
                continue
            path = (urlsplit(abs_url).path or "/").lower()
            if path not in internal_paths:
                internal_paths.append(path)
            if len(internal_paths) >= 20:
                break
        evidence = {
            "homepage_url": homepage_url,
            "title": title,
            "h1": h1_text,
            "internal_paths": internal_paths,
            "excerpt": excerpt,
        }
        if _path_has_alias(homepage_url):
            return {
                "criterion": criterion,
                "matches": False,
                "reason": "service_path_detected_on_home_or_canonical",
                "service": service,
                "service_mentioned": True,
                "dedicated_page_detected": True,
                "method": "fast_homepage",
                "url": homepage_url,
                "evidence": evidence,
            }

        # Fast same-page signals (title/h1)
        if _text_has_alias(title) or _text_has_alias(h1_text):
            return {
                "criterion": criterion,
                "matches": False,
                "reason": "strong_title_or_h1_signal",
                "service": service,
                "service_mentioned": True,
                "dedicated_page_detected": True,
                "method": "fast_homepage",
                "url": homepage_url,
                "evidence": evidence,
            }

        strong_link_found = False
        for m in re.finditer(r"<a[^>]*href\s*=\s*[\"']([^\"'#]+)[\"'][^>]*>(.*?)</a>", html, re.I | re.S):
            href = (m.group(1) or "").strip()
            if not href:
                continue
            abs_url = _normalize_site(urljoin(homepage_url, href))
            if not abs_url or not _same_domain(homepage_url, abs_url):
                continue
            anchor = re.sub(r"<[^>]+>", " ", m.group(2) or "")
            anchor = re.sub(r"\s+", " ", anchor).strip().lower()
            if _path_has_alias(abs_url) or _text_has_alias(anchor):
                strong_link_found = True
                break

        if strong_link_found:
            return {
                "criterion": criterion,
                "matches": False,
                "reason": "service_link_detected_on_homepage",
                "service": service,
                "service_mentioned": True,
                "dedicated_page_detected": True,
                "method": "fast_homepage",
                "evidence": evidence,
            }

        return {
            "criterion": criterion,
            "matches": True,
            "reason": "no_strong_service_page_signal_in_fast_check",
            "service": service,
            "service_mentioned": False,
            "dedicated_page_detected": False,
            "method": "fast_homepage",
            "evidence": evidence,
        }
    except Exception:
        return {
            "criterion": criterion,
            "matches": False,
            "reason": "fast detector failed",
            "service": service,
            "service_mentioned": False,
            "dedicated_page_detected": False,
        }


def review_lightweight_match_with_ai(
    *,
    website: Optional[str],
    criterion: Dict[str, Any],
    lightweight_result: Dict[str, Any],
) -> Dict[str, Any]:
    """Optional Ask-only AI review for borderline lightweight positives.

    Returns verdict metadata and never raises.
    """
    service = str(criterion.get("service") or "").strip().lower()
    if not website or not service:
        return {"enabled": False, "verdict": "skipped", "reason": "missing website or service"}
    if not os.getenv("OPENAI_API_KEY"):
        return {"enabled": False, "verdict": "skipped", "reason": "missing_openai_key"}
    try:
        from openai import OpenAI
    except Exception:
        return {"enabled": False, "verdict": "skipped", "reason": "openai_sdk_unavailable"}

    evidence = lightweight_result.get("evidence") if isinstance(lightweight_result.get("evidence"), dict) else {}
    payload = {
        "website": website,
        "criterion_type": str(criterion.get("type") or ""),
        "service": service,
        "lightweight_reason": str(lightweight_result.get("reason") or ""),
        "lightweight_method": str(lightweight_result.get("method") or ""),
        "lightweight_matches": bool(lightweight_result.get("matches")),
        "evidence": {
            "homepage_url": evidence.get("homepage_url"),
            "title": evidence.get("title"),
            "h1": evidence.get("h1"),
            "internal_paths": evidence.get("internal_paths") if isinstance(evidence.get("internal_paths"), list) else [],
            "excerpt": evidence.get("excerpt"),
        },
    }

    prompt = (
        "You are reviewing whether a dental lead likely matches the request "
        "'missing service page' for a specific service.\n"
        "Use only the provided evidence, do not browse.\n"
        "Return JSON only with schema: "
        '{"verdict":"likely_match|unclear|likely_not_match","confidence":"low|medium|high","reason":"..."}.\n'
        "Interpretation:\n"
        "- likely_match = service page likely missing or not findable from evidence.\n"
        "- likely_not_match = service page likely present from evidence.\n"
        "- unclear = insufficient/conflicting evidence.\n"
        f"Evidence JSON: {json.dumps(payload, ensure_ascii=True)}"
    )
    try:
        client = OpenAI()
        model = os.getenv("ASK_AI_REVIEW_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        r = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": "Output valid JSON only."}, {"role": "user", "content": prompt}],
        )
        txt = (r.choices[0].message.content or "") if getattr(r, "choices", None) else ""
        obj = _extract_json(txt)
        verdict = str(obj.get("verdict") or "").strip().lower()
        if verdict not in {"likely_match", "unclear", "likely_not_match"}:
            verdict = "unclear"
        confidence = str(obj.get("confidence") or "").strip().lower()
        if confidence not in {"low", "medium", "high"}:
            confidence = "low"
        reason = str(obj.get("reason") or "").strip()[:240]
        return {
            "enabled": True,
            "model": model,
            "verdict": verdict,
            "confidence": confidence,
            "reason": reason or "AI review completed.",
        }
    except Exception:
        return {"enabled": False, "verdict": "skipped", "reason": "openai_call_failed"}


def ai_batch_rerank_candidates(
    *,
    rows: List[Dict[str, Any]],
    criteria: List[Dict[str, Any]],
    purpose: str,
    max_items: int = 20,
) -> Dict[str, Dict[str, Any]]:
    """Single-call LLM rerank adjustments for top rows.

    Returns map: place_id -> {"delta": float, "confidence": str, "reason": str}
    """
    if not rows:
        return {}
    if not os.getenv("OPENAI_API_KEY"):
        return {}
    try:
        from openai import OpenAI
    except Exception:
        return {}

    subset = rows[: max(1, min(int(max_items), len(rows)))]
    slim_rows: List[Dict[str, Any]] = []
    for r in subset:
        pid = str(r.get("place_id") or "").strip()
        if not pid:
            continue
        lightweight_reason = None
        light = r.get("_light_results")
        if isinstance(light, dict) and light:
            # Keep one reason for compact prompt payload.
            one = next(iter(light.values()))
            if isinstance(one, dict):
                lightweight_reason = str(one.get("reason") or "")[:120]
        slim_rows.append(
            {
                "id": pid,
                "business_name": str(r.get("business_name") or "")[:120],
                "city": str(r.get("city") or "")[:80],
                "state": str(r.get("state") or "")[:10],
                "rank_key": float(r.get("rank_key") or 0.0),
                "rating": r.get("rating"),
                "reviews": int(r.get("user_ratings_total") or 0),
                "has_website": bool(r.get("has_website")),
                "has_contact_form": r.get("has_contact_form"),
                "ssl": r.get("ssl"),
                "has_schema": r.get("has_schema"),
                "lightweight_reason": lightweight_reason,
            }
        )
    if not slim_rows:
        return {}

    criteria_text = ", ".join(
        f"{str(c.get('type') or '')}:{str(c.get('service') or '').strip()}"
        for c in (criteria or [])
        if str(c.get("type") or "").strip()
    )[:400]
    payload = {
        "purpose": purpose[:60],
        "criteria": criteria_text,
        "rows": slim_rows,
    }
    prompt = (
        "You are reranking local-business leads for an operator.\n"
        "Use only provided data. No browsing.\n"
        "Return JSON only with schema:\n"
        '{"adjustments":[{"id":"...", "delta": -2.0..2.0, "confidence":"low|medium|high", "reason":"..."}]}\n'
        "Rules:\n"
        "- Keep deltas small and conservative.\n"
        "- Positive delta only when evidence strongly supports criteria fit.\n"
        "- Negative delta for obvious mismatch/low trust signals.\n"
        f"Input: {json.dumps(payload, ensure_ascii=True)}"
    )
    try:
        client = OpenAI()
        model = os.getenv("ASK_AI_REVIEW_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        r = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": "Output valid JSON only."}, {"role": "user", "content": prompt}],
        )
        txt = (r.choices[0].message.content or "") if getattr(r, "choices", None) else ""
        obj = _extract_json(txt)
        out: Dict[str, Dict[str, Any]] = {}
        for item in (obj.get("adjustments") or []):
            if not isinstance(item, dict):
                continue
            pid = str(item.get("id") or "").strip()
            if not pid:
                continue
            try:
                delta = float(item.get("delta") or 0.0)
            except (TypeError, ValueError):
                delta = 0.0
            delta = max(-2.0, min(2.0, delta))
            conf = str(item.get("confidence") or "").strip().lower()
            if conf not in {"low", "medium", "high"}:
                conf = "low"
            reason = str(item.get("reason") or "").strip()[:180]
            out[pid] = {"delta": delta, "confidence": conf, "reason": reason}
        return out
    except Exception:
        return {}


def ai_batch_explain_matches(
    *,
    rows: List[Dict[str, Any]],
    criteria: List[Dict[str, Any]],
    max_items: int = 10,
) -> Dict[str, str]:
    """Single-call short explanations for top rows."""
    if not rows:
        return {}
    if not os.getenv("OPENAI_API_KEY"):
        return {}
    try:
        from openai import OpenAI
    except Exception:
        return {}
    subset = rows[: max(1, min(int(max_items), len(rows)))]
    slim_rows: List[Dict[str, Any]] = []
    for r in subset:
        pid = str(r.get("place_id") or "").strip()
        if not pid:
            continue
        slim_rows.append(
            {
                "id": pid,
                "business_name": str(r.get("business_name") or "")[:120],
                "rating": r.get("rating"),
                "reviews": int(r.get("user_ratings_total") or 0),
                "rank_key": float(r.get("rank_key") or 0.0),
                "has_website": bool(r.get("has_website")),
                "has_contact_form": r.get("has_contact_form"),
                "ssl": r.get("ssl"),
            }
        )
    if not slim_rows:
        return {}
    criteria_text = ", ".join(
        f"{str(c.get('type') or '')}:{str(c.get('service') or '').strip()}"
        for c in (criteria or [])
        if str(c.get("type") or "").strip()
    )[:400]
    prompt = (
        "Generate concise operator-facing reasons each lead matches target criteria.\n"
        "Return JSON only: {\"explanations\":[{\"id\":\"...\",\"text\":\"...\"}]}\n"
        "Constraints: 1 sentence, <= 140 chars, factual, no hype.\n"
        f"Criteria: {criteria_text}\n"
        f"Rows: {json.dumps(slim_rows, ensure_ascii=True)}"
    )
    try:
        client = OpenAI()
        model = os.getenv("ASK_AI_REVIEW_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        r = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": "Output valid JSON only."}, {"role": "user", "content": prompt}],
        )
        txt = (r.choices[0].message.content or "") if getattr(r, "choices", None) else ""
        obj = _extract_json(txt)
        out: Dict[str, str] = {}
        for item in (obj.get("explanations") or []):
            if not isinstance(item, dict):
                continue
            pid = str(item.get("id") or "").strip()
            text = str(item.get("text") or "").strip()
            if pid and text:
                out[pid] = text[:160]
        return out
    except Exception:
        return {}


def ai_validate_brief_service_rows(
    *,
    website: Optional[str],
    service_rows: List[Dict[str, Any]],
    max_items: int = 8,
) -> Dict[str, Dict[str, Any]]:
    """Validate brief service-page signals in one bounded AI call.

    Returns map: service_slug -> {"verdict","confidence","reason","model"}.
    Deterministic pipeline remains source of truth; this is advisory.
    """
    if not website or not service_rows:
        return {}
    if not os.getenv("OPENAI_API_KEY"):
        return {}
    try:
        from openai import OpenAI
    except Exception:
        return {}

    subset = service_rows[: max(1, min(int(max_items), len(service_rows)))]
    compact: List[Dict[str, Any]] = []
    for row in subset:
        slug = str(row.get("service") or "").strip().lower()
        if not slug:
            continue
        compact.append(
            {
                "service": slug,
                "display_name": str(row.get("display_name") or slug)[:80],
                "service_status": str(row.get("service_status") or ""),
                "detection_reason": str(row.get("detection_reason") or "")[:180],
                "url": row.get("url"),
                "word_count": int(row.get("word_count") or 0),
                "h1_match": bool(row.get("h1_match")),
                "confidence_level": str(row.get("confidence_level") or ""),
            }
        )
    if not compact:
        return {}

    payload = {
        "website": website,
        "rows": compact,
    }
    prompt = (
        "You are validating website service-page detection quality for a sales brief.\n"
        "Use only provided evidence. Do not browse.\n"
        "Return JSON only with schema:\n"
        '{"items":[{"service":"...","verdict":"likely_missing|likely_present|unclear","confidence":"low|medium|high","reason":"..."}]}\n'
        "Guidance:\n"
        "- likely_present: evidence strongly indicates service page exists.\n"
        "- likely_missing: evidence strongly indicates service page missing.\n"
        "- unclear: weak or conflicting evidence.\n"
        f"Input: {json.dumps(payload, ensure_ascii=True)}"
    )
    try:
        client = OpenAI()
        model = os.getenv("BRIEF_AI_VERIFY_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
        r = client.chat.completions.create(
            model=model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": "Output valid JSON only."}, {"role": "user", "content": prompt}],
        )
        txt = (r.choices[0].message.content or "") if getattr(r, "choices", None) else ""
        obj = _extract_json(txt)
        out: Dict[str, Dict[str, Any]] = {}
        for item in (obj.get("items") or []):
            if not isinstance(item, dict):
                continue
            slug = str(item.get("service") or "").strip().lower()
            if not slug:
                continue
            verdict = str(item.get("verdict") or "").strip().lower()
            if verdict not in {"likely_missing", "likely_present", "unclear"}:
                verdict = "unclear"
            conf = str(item.get("confidence") or "").strip().lower()
            if conf not in {"low", "medium", "high"}:
                conf = "low"
            reason = str(item.get("reason") or "").strip()[:220]
            out[slug] = {
                "verdict": verdict,
                "confidence": conf,
                "reason": reason or "AI validation completed.",
                "model": model,
            }
        return out
    except Exception:
        return {}
