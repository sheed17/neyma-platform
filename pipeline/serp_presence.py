"""
SERP presence snapshot (optional).

Uses SerpAPI when SERPAPI_API_KEY is available. Non-blocking; returns None on
missing configuration or request errors.
"""

from __future__ import annotations

import datetime as _dt
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from pipeline.high_value_services_config import SERVICE_SERP_MAX_CONSECUTIVE_EMPTY, SERVICE_SERP_MAX_QUERIES


def _normalize_domain(url_or_domain: str) -> str:
    val = (url_or_domain or "").strip().lower()
    if not val:
        return ""
    if "://" not in val:
        val = "https://" + val
    parsed = urlparse(val)
    return (parsed.netloc or "").replace("www.", "")


def _seed_keywords(city: str) -> List[str]:
    c = (city or "").strip()
    if not c:
        return []
    return [
        f"dentist in {c}",
        f"invisalign {c}",
        f"dental implants {c}",
    ]


def _page_type_from_url(url: str) -> str:
    p = (urlparse(url).path or "").lower()
    if any(x in p for x in ("/blog", "/article", "/news")):
        return "blog"
    if any(x in p for x in ("/service", "/services", "/treatment", "/procedure")):
        return "service"
    if any(x in p for x in ("/location", "/locations", "/area", "/areas-we-serve")):
        return "location"
    return "other"


def _service_query(service_display: str, city: str, state: Optional[str]) -> str:
    c = (city or "").strip()
    s = (state or "").strip()
    if s:
        return f"{service_display} {c}, {s}"
    return f"{service_display} {c}"


def _service_rank_from_serp(
    data: Dict[str, Any],
    target_domain: str,
) -> Dict[str, Any]:
    organic = data.get("organic_results") or []
    local = data.get("local_results") or data.get("local_map") or {}
    local_places = local.get("places") if isinstance(local, dict) else (local if isinstance(local, list) else [])

    pos: Optional[int] = None
    for i, r in enumerate(organic[:20], start=1):
        link = str(r.get("link") or "").strip()
        if link and _normalize_domain(link) == target_domain:
            pos = i
            break
    competitors_top_10 = 0
    for r in organic[:10]:
        link = str(r.get("link") or "").strip()
        if link and _normalize_domain(link) != target_domain:
            competitors_top_10 += 1

    return {
        "position_top_3": bool(pos is not None and pos <= 3),
        "position_top_10": bool(pos is not None and pos <= 10),
        "average_position": int(pos) if pos is not None else None,
        "map_pack_presence": bool(local_places),
        "competitors_in_top_10": int(competitors_top_10),
    }


def build_serp_presence(
    city: str,
    state: Optional[str],
    website_url: Optional[str],
    keywords: Optional[List[str]] = None,
    service_queries: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    api_key = os.getenv("SERPAPI_API_KEY")
    domain = _normalize_domain(website_url or "")
    if not api_key or not domain:
        return None

    kws = keywords or _seed_keywords(city)
    if not kws:
        return None

    rows: List[Dict[str, Any]] = []
    for kw in kws[:6]:
        try:
            params = {
                "engine": "google",
                "q": kw,
                "num": 10,
                "api_key": api_key,
                "gl": "us",
                "hl": "en",
            }
            resp = requests.get("https://serpapi.com/search.json", params=params, timeout=15)
            if resp.status_code != 200:
                rows.append({"keyword": kw, "position": None, "in_top_10": False, "page_type": None})
                continue
            data = resp.json() if resp.content else {}
            organic = data.get("organic_results") or []
            pos = None
            found_url = None
            for i, r in enumerate(organic[:10], start=1):
                link = str(r.get("link") or "").strip()
                if not link:
                    continue
                if _normalize_domain(link) == domain:
                    pos = i
                    found_url = link
                    break
            rows.append(
                {
                    "keyword": kw,
                    "position": pos,
                    "in_top_10": bool(pos is not None),
                    "page_type": _page_type_from_url(found_url) if found_url else None,
                }
            )
        except Exception:
            rows.append({"keyword": kw, "position": None, "in_top_10": False, "page_type": None})

    service_rankings: Dict[str, Dict[str, Any]] = {}
    if service_queries:
        requests_made = 0
        consecutive_empty = 0
        for svc in service_queries:
            if requests_made >= SERVICE_SERP_MAX_QUERIES:
                break
            if not isinstance(svc, dict):
                continue
            slug = str(svc.get("slug") or "").strip().lower()
            display = str(svc.get("display_name") or slug.replace("_", " ").title()).strip()
            if not slug or not display:
                continue
            q = _service_query(display, city, state)
            try:
                params = {
                    "engine": "google",
                    "q": q,
                    "num": 10,
                    "api_key": api_key,
                    "gl": "us",
                    "hl": "en",
                }
                resp = requests.get("https://serpapi.com/search.json", params=params, timeout=15)
                requests_made += 1
                if resp.status_code != 200:
                    service_rankings[slug] = {
                        "position_top_3": False,
                        "position_top_10": False,
                        "average_position": None,
                        "map_pack_presence": False,
                        "competitors_in_top_10": 0,
                    }
                    consecutive_empty += 1
                else:
                    data = resp.json() if resp.content else {}
                    organic = data.get("organic_results") or []
                    if not organic:
                        consecutive_empty += 1
                    else:
                        consecutive_empty = 0
                    service_rankings[slug] = _service_rank_from_serp(data, domain)
            except Exception:
                requests_made += 1
                consecutive_empty += 1
                service_rankings[slug] = {
                    "position_top_3": False,
                    "position_top_10": False,
                    "average_position": None,
                    "map_pack_presence": False,
                    "competitors_in_top_10": 0,
                }
            if consecutive_empty >= SERVICE_SERP_MAX_CONSECUTIVE_EMPTY:
                break

    return {
        "domain": domain,
        "keywords": rows,
        "service_rankings": service_rankings,
        "as_of_date": _dt.datetime.utcnow().date().isoformat(),
        "provider": "serpapi",
    }
