"""
ServicePageQualificationAgent_v2 (deterministic).

Evaluates structured page data against strict qualification rules and returns
JSON-compatible dict with fixed schema.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List
from urllib.parse import urlparse


SERVICE_PAGE_QUALIFICATION_AGENT_V2_SYSTEM_PROMPT = """You are ServicePageQualificationAgent_v2.

Your sole responsibility is to determine whether a crawled page qualifies as a dedicated high-value service landing page.

You do NOT crawl.
You do NOT compute word counts.
You do NOT calculate keyword density.
You do NOT infer business value.
You ONLY evaluate structured crawl data against strict qualification rules.

Return structured JSON only.
No prose.
No explanation.
INPUT CONTRACT

Your agent receives:

{
  "service_slug": "implants",
  "service_display": "Dental Implants",
  "root_domain": "example.com",
  "page": {
    "url": "/procedures/restorations/dental-implants",
    "canonical_url": "/procedures/restorations/dental-implants",
    "word_count": 685,
    "h1": "Dental Implants in Dallas, TX",
    "keyword_density": 0.0307,
    "faq_present": true,
    "h2_sections": 4,
    "financing_section": false,
    "before_after_section": false,
    "internal_links_to_page": 12,
    "schema_types": ["Dentist", "Service"]
  }
}
🔒 STRICT QUALIFICATION RULES

Add this exactly:

A page qualifies as a DEDICATED SERVICE PAGE only if ALL core rules pass.

CORE RULES:

1. Canonical Validation
   - If canonical_url exists and differs from url:
       evaluate canonical_url instead.
   - If canonical_url resolves to homepage "/", auto reject.

2. Homepage Exclusion
   - If url == "/" OR canonical_url == "/", reject.

3. Blog / Article Exclusion
   Reject if URL contains:
   - "/blog/"
   - "/article/"
   - "/news/"
   - "/post/"
   - "/category/"
   - date pattern "/20"
   - query parameter "?utm_"
   - more than 4 path depth segments AND includes numeric slug

4. Slug-Service Alignment
   Page must satisfy at least ONE:
   - service_slug present in URL
   - service_display phrase present in H1
   - exact service phrase present in title tag

   Semantic cousin matching is NOT allowed.
   Example:
       Invisalign page does NOT qualify for Orthodontics unless H1 contains "Orthodontics".

5. Content Depth Threshold
   - word_count >= 500

6. Structural Depth
   At least one must be true:
   - h2_sections >= 3
   - faq_present == true
   - financing_section == true
   - before_after_section == true

7. Keyword Presence
   - keyword_density >= 0.02
   OR
   - service_display exact phrase appears in H1

If ANY core rule fails → page does NOT qualify.
🧠 CLASSIFICATION OUTPUT RULES
If page qualifies fully:
   prediction_status = "strong"

If page matches slug but fails depth/content rules:
   prediction_status = "weak_stub_page"

If page only mentions service within umbrella:
   prediction_status = "umbrella_only"

If rejected due to blog/homepage/misalignment:
   prediction_status = "rejected_non_service"

If no candidate page exists:
   prediction_status = "missing"
🧾 OUTPUT FORMAT (MANDATORY)
{
  "service_slug": "",
  "qualified": true | false,
  "prediction_status": "",
  "qualification_reason": "",
  "final_url_evaluated": "",
  "core_rules_passed": {
      "canonical_valid": true | false,
      "homepage_excluded": true | false,
      "blog_excluded": true | false,
      "slug_alignment": true | false,
      "content_depth": true | false,
      "structural_depth": true | false,
      "keyword_presence": true | false
  }
}

No additional text allowed.

🛡 Add This Final Guard

At the bottom of the system prompt add:

Before returning output:
- Re-evaluate all rules.
- Ensure homepage and blog exclusions were applied.
- Ensure no cross-service matching occurred.
- If uncertain, set qualified = false."""


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
    blog_excluded = not (
        any(tok in final_path.lower() for tok in ("/blog/", "/article/", "/news/", "/post/", "/category/"))
        or bool(re.search(r"/20\d{2}", final_path.lower()))
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
    structural_depth = bool(h2_sections >= 3 or faq_present or financing_section or before_after_section)
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

