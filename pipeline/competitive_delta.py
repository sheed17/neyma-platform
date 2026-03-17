"""
Competitive delta builder.

Deterministic, additive metrics comparing target service-page infrastructure to
competitor averages when available.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def build_competitive_delta(
    lead: Dict[str, Any],
    service_intelligence: Dict[str, Any],
    competitors: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Build competitive delta block.

    v1 behavior:
    - Always computes target metrics from service_intelligence.
    - Uses competitor averages only if precomputed metrics are present on competitor
      rows (no additional API calls required).
    """
    svc = service_intelligence or {}
    target_service_pages = int(svc.get("service_page_count") or 0)
    target_with_schema = int(svc.get("service_pages_with_faq_or_schema") or 0)
    target_avg_words = _to_float(svc.get("avg_word_count_service_pages"))
    target_min_words = _to_float(svc.get("min_word_count_service_pages"))
    target_max_words = _to_float(svc.get("max_word_count_service_pages"))

    competitor_metric_rows = [c for c in (competitors or []) if isinstance(c, dict) and c.get("service_page_count") is not None]
    c_service_vals = [_to_float(c.get("service_page_count")) for c in competitor_metric_rows]
    c_schema_vals = [_to_float(c.get("pages_with_schema")) for c in competitor_metric_rows]
    c_word_vals = [_to_float(c.get("avg_word_count")) for c in competitor_metric_rows]
    c_service_vals = [v for v in c_service_vals if v is not None]
    c_schema_vals = [v for v in c_schema_vals if v is not None]
    c_word_vals = [v for v in c_word_vals if v is not None]

    competitor_avg_service_pages = (sum(c_service_vals) / len(c_service_vals)) if c_service_vals else None
    competitor_avg_pages_with_schema = (sum(c_schema_vals) / len(c_schema_vals)) if c_schema_vals else None
    competitor_avg_word_count = (sum(c_word_vals) / len(c_word_vals)) if c_word_vals else None
    competitor_site_metrics_count = int(len(competitor_metric_rows))
    competitor_crawl_note = None
    if not competitor_metric_rows:
        competitor_crawl_note = "Competitor website metrics not run for this brief."

    return {
        "target_service_page_count": target_service_pages,
        "target_pages_with_faq_schema": target_with_schema,
        "target_avg_word_count_service_pages": target_avg_words,
        "target_min_word_count_service_pages": target_min_words,
        "target_max_word_count_service_pages": target_max_words,
        "competitor_avg_service_pages": competitor_avg_service_pages,
        "competitor_avg_pages_with_schema": competitor_avg_pages_with_schema,
        "competitor_avg_word_count": competitor_avg_word_count,
        "delta_service_pages": (target_service_pages - competitor_avg_service_pages) if competitor_avg_service_pages is not None else None,
        "delta_schema_coverage": (target_with_schema - competitor_avg_pages_with_schema) if competitor_avg_pages_with_schema is not None else None,
        "competitors_sampled": int(len(competitors or [])),
        "competitor_site_metrics_count": competitor_site_metrics_count,
        "competitor_crawl_note": competitor_crawl_note,
        "competitor_metrics_available": bool(c_service_vals or c_schema_vals or c_word_vals),
        "source_note": "Target metrics from on-site crawl; competitor averages shown when comparable data is available.",
    }
