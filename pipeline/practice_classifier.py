"""
Deterministic weighted dental practice subtype classifier.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urlparse


def _extract_domain(place_data: Dict[str, Any]) -> str:
    candidates = [
        place_data.get("signal_website_url"),
        place_data.get("website"),
        (place_data.get("_place_details") or {}).get("website") if isinstance(place_data.get("_place_details"), dict) else None,
    ]
    for raw in candidates:
        if not raw:
            continue
        url = str(raw).strip()
        if not url:
            continue
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        host = (urlparse(url).netloc or "").lower().replace("www.", "")
        if host:
            return host
    return ""


def _iter_category_text(place_data: Dict[str, Any]) -> str:
    vals: List[str] = []
    for key in ("types", "category", "categories", "primary_type", "business_type", "service_category"):
        raw = place_data.get(key)
        if isinstance(raw, list):
            vals.extend(str(x) for x in raw if x is not None)
        elif raw is not None:
            vals.append(str(raw))
    return " ".join(vals).lower()


def _count_token(text: str, token: str) -> int:
    return len(re.findall(r"\b" + re.escape(token.lower()) + r"\b", text.lower()))


def classify_practice_type(place_data: dict, website_content: str) -> dict:
    """
    Returns:
    {
        "practice_type": "general_dentist" | "orthodontist" | "oral_surgeon" | "pediatric_dentist" | "periodontist",
        "confidence": float (0-1),
        "signals": {...}
    }
    """
    place_data = place_data or {}
    text = (website_content or "").lower()
    category_text = _iter_category_text(place_data)
    domain = _extract_domain(place_data)

    scores: Dict[str, int] = {
        "general_dentist": 0,
        "orthodontist": 0,
        "oral_surgeon": 0,
        "pediatric_dentist": 0,
        "periodontist": 0,
    }

    signals: Dict[str, Any] = {
        "category_text": category_text,
        "domain": domain,
        "keyword_counts": {},
    }

    # Orthodontist signals
    if "orthodontist" in category_text:
        scores["orthodontist"] += 3
    if "ortho" in domain:
        scores["orthodontist"] += 2
    braces_count = _count_token(text, "braces")
    aligners_count = _count_token(text, "aligners")
    if braces_count > 5:
        scores["orthodontist"] += 1
    if aligners_count > 5:
        scores["orthodontist"] += 1

    # General dentist signals
    if re.search(r"\bdentist\b", category_text):
        scores["general_dentist"] += 3
    if "dental" in domain:
        scores["general_dentist"] += 2
    implants_count = _count_token(text, "implants")
    crowns_count = _count_token(text, "crowns")
    root_canal_count = _count_token(text, "root canal")
    veneers_count = _count_token(text, "veneers")
    whitening_count = _count_token(text, "whitening")
    if implants_count > 3:
        scores["general_dentist"] += 1
    if crowns_count > 3:
        scores["general_dentist"] += 1
    if root_canal_count > 3:
        scores["general_dentist"] += 1
    if veneers_count > 3:
        scores["general_dentist"] += 1
    if whitening_count > 3:
        scores["general_dentist"] += 1

    # Lightweight specialist signals so non-general options still work deterministically.
    if "oral surgeon" in category_text or "oral surgery" in category_text:
        scores["oral_surgeon"] += 3
    if any(x in domain for x in ("surgery", "oralsurgery", "oral-surgery")):
        scores["oral_surgeon"] += 2
    if _count_token(text, "extractions") > 3 or _count_token(text, "wisdom teeth") > 3 or _count_token(text, "bone graft") > 3:
        scores["oral_surgeon"] += 1

    if "pediatric dentist" in category_text or "pediatric dentistry" in category_text:
        scores["pediatric_dentist"] += 3
    if "pediatric" in domain:
        scores["pediatric_dentist"] += 2
    if _count_token(text, "kids") > 3 or _count_token(text, "children") > 3:
        scores["pediatric_dentist"] += 1

    if "periodontist" in category_text or "periodont" in category_text:
        scores["periodontist"] += 3
    if any(x in domain for x in ("perio", "periodont")):
        scores["periodontist"] += 2
    if _count_token(text, "gum disease") > 3 or _count_token(text, "periodontal treatment") > 3:
        scores["periodontist"] += 1

    signals["keyword_counts"] = {
        "braces": braces_count,
        "aligners": aligners_count,
        "implants": implants_count,
        "crowns": crowns_count,
        "root_canal": root_canal_count,
        "veneers": veneers_count,
        "whitening": whitening_count,
    }
    signals["scores"] = dict(scores)

    total = sum(scores.values())
    if total == 0:
        return {
            "practice_type": "general_dentist",
            "confidence": 0.3,
            "signals": signals,
        }

    top_type = max(scores, key=scores.get)
    if abs(scores["orthodontist"] - scores["general_dentist"]) <= 1 and top_type == "orthodontist":
        top_type = "general_dentist"

    confidence = float(scores.get(top_type, 0)) / float(total) if total > 0 else 0.3
    return {
        "practice_type": top_type,
        "confidence": round(max(0.0, min(1.0, confidence)), 3),
        "signals": signals,
    }
