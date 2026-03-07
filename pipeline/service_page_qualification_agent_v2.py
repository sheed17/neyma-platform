"""
ServicePageQualificationAgent_v2 (deterministic).

Evaluates structured page data against strict qualification rules and returns
JSON-compatible dict with fixed schema.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urlparse


def _norm_path(url_or_path: str) -> str:
    u = str(url_or_path or "").strip()
    if not u:
        return ""
    parsed = urlparse(u)
    path = parsed.path if (parsed.scheme or parsed.netloc) else u
    if not path.startswith("/"):
        path = "/" + path
    return path


def _contains_numeric_slug(path: str) -> bool:
    segs = [s for s in (path or "").split("/") if s]
    return any(re.search(r"\d", s or "") for s in segs)


def qualify_service_page_candidate_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    service_slug = str(payload.get("service_slug") or "").strip().lower().replace(" ", "_")
    service_display = str(payload.get("service_display") or "").strip()
    page = payload.get("page") or {}

    url_raw = str(page.get("url") or "").strip()
    canonical_raw = str(page.get("canonical_url") or "").strip()
    title = str(page.get("title") or "").strip().lower()
    h1 = str(page.get("h1") or "").strip().lower()
    wc = int(page.get("word_count") or 0)
    kd = float(page.get("keyword_density") or 0.0)
    faq_present = bool(page.get("faq_present"))
    h2_sections = int(page.get("h2_sections") or 0)
    financing_section = bool(page.get("financing_section"))
    before_after_section = bool(page.get("before_after_section"))
    umbrella_page = bool(page.get("umbrella_page"))
    service_mentioned = bool(page.get("service_mentioned"))

    if not url_raw and not canonical_raw:
        return {
            "service_slug": service_slug,
            "qualified": False,
            "prediction_status": "missing",
            "qualification_reason": "No candidate page exists.",
            "final_url_evaluated": "",
            "core_rules_passed": {
                "canonical_valid": False,
                "homepage_excluded": False,
                "blog_excluded": False,
                "slug_alignment": False,
                "content_depth": False,
                "structural_depth": False,
                "keyword_presence": False,
            },
        }

    url_path = _norm_path(url_raw)
    canonical_path = _norm_path(canonical_raw) if canonical_raw else url_path
    final_path = canonical_path if (canonical_path and canonical_path != url_path) else url_path

    canonical_valid = not (canonical_path == "/" and canonical_path != url_path)
    homepage_excluded = final_path not in {"", "/"}

    q = str(urlparse(str(canonical_raw or url_raw)).query or "").lower()
    segs = [s for s in final_path.split("/") if s]
    deep_numeric = len(segs) > 4 and _contains_numeric_slug(final_path)
    path_lower = final_path.lower()
    blog_excluded = not (
        any(tok in path_lower for tok in ("/blog/", "/blog", "/article/", "/news/", "/post/", "/category/", "/archive/", "/insights/"))
        or bool(re.search(r"/20\d{2}", path_lower))
        or ("utm_" in q)
        or deep_numeric
    )

    slug_token = service_slug.replace("_", "-")
    display_l = service_display.lower()
    slug_alignment = (
        (slug_token and slug_token in final_path.lower())
        or (display_l and display_l in h1)
        or (display_l and display_l in title)
    )

    content_depth = wc >= 500
    structural_depth = bool(h2_sections >= 2 or faq_present or financing_section or before_after_section)
    keyword_presence = bool(kd >= 0.02 or (display_l and display_l in h1))

    core = {
        "canonical_valid": bool(canonical_valid),
        "homepage_excluded": bool(homepage_excluded),
        "blog_excluded": bool(blog_excluded),
        "slug_alignment": bool(slug_alignment),
        "content_depth": bool(content_depth),
        "structural_depth": bool(structural_depth),
        "keyword_presence": bool(keyword_presence),
    }

    qualified = all(core.values())
    if qualified:
        status = "strong"
        reason = "All core rules passed."
    elif umbrella_page and service_mentioned:
        status = "umbrella_only"
        reason = "Service only mentioned within umbrella page."
    elif (not canonical_valid) or (not homepage_excluded) or (not blog_excluded) or (not slug_alignment):
        status = "rejected_non_service"
        if not canonical_valid:
            reason = "Canonical URL resolves to homepage."
        elif not homepage_excluded:
            reason = "Homepage excluded."
        elif not blog_excluded:
            reason = "Blog/article/non-service URL excluded."
        else:
            reason = "Slug-service alignment failed."
    elif (slug_token and slug_token in final_path.lower()) and (not content_depth or not structural_depth or not keyword_presence):
        status = "weak_stub_page"
        reason = "Slug matched but depth/content rules failed."
    else:
        status = "missing"
        reason = "No candidate page exists."

    # Final guard: deterministic fail-closed.
    if not qualified:
        qualified = False

    return {
        "service_slug": service_slug,
        "qualified": bool(qualified),
        "prediction_status": status,
        "qualification_reason": reason,
        "final_url_evaluated": final_path,
        "core_rules_passed": core,
    }

