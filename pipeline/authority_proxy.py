"""
Internal authority proxy (clearly labeled, deterministic).
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _norm(v: Optional[float], low: float, high: float) -> float:
    if v is None:
        return 0.0
    if high <= low:
        return 0.0
    x = (float(v) - low) / (high - low)
    if x < 0:
        return 0.0
    if x > 1:
        return 1.0
    return x


def build_authority_proxy(
    service_intelligence: Dict[str, Any],
    domain_age_years: Optional[float] = None,
) -> Dict[str, Any]:
    page_count = int(service_intelligence.get("pages_crawled") or 0)
    blog_page_count = int(service_intelligence.get("blog_page_count") or 0)

    # Methodology:
    # score = 50% page count + 25% blog coverage + 25% domain age
    score = (
        0.50 * _norm(page_count, 0, 120)
        + 0.25 * _norm(blog_page_count, 0, 50)
        + 0.25 * _norm(domain_age_years, 0, 15)
    )
    authority_score = round(score * 100, 1)

    return {
        "page_count": page_count,
        "blog_page_count": blog_page_count,
        "domain_age_years": domain_age_years,
        "authority_proxy_score": authority_score,
        "methodology": "Weighted proxy: 50% page count, 25% blog pages, 25% domain age (when available). Not a Domain Rating.",
    }
