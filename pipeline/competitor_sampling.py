"""
Lightweight competitor sampling for dental leads.

Uses Nearby Search with tiered radius (2 mi → 5 mi → 8 mi). Tracks search_radius_used_miles.
Distance-aware competitor dicts (haversine). competitive_profile labels only; no numeric scores.
No percentiles. No LLM. Deterministic.
"""

import os
import math
import logging
import statistics
import json
import re
import time
from typing import Dict, Any, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)

# Tiered radius (meters): 2 mi → 5 mi → 8 mi (hard stop)
RADIUS_BASE_M = 3218   # 2 miles
RADIUS_MID_M = 8046    # 5 miles
RADIUS_MAX_M = 12874   # 8 miles
MAX_COMPETITORS = 8
KEYWORD = "dentist"
MIN_COMPETITORS_BEFORE_EXPAND = 5

KM_PER_MILE = 1.609344
MILES_PER_KM = 1.0 / KM_PER_MILE

SERVICE_PATH_TOKENS = {
    "implant", "implants", "invisalign", "veneer", "veneers",
    "cosmetic", "sedation", "emergency", "crown", "crowns",
    "sleep", "apnea", "orthodontic", "orthodontics", "braces",
    "service", "services", "treatment", "treatments", "procedure",
    "procedures", "whitening", "makeover", "dental", "dentistry",
    "root-canal", "pediatric",
}


def _haversine_miles(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Distance between two points in miles (deterministic)."""
    R_KM = 6371.0
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lng = math.radians(lng2 - lng1)
    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R_KM * c * MILES_PER_KM, 1)


def fetch_competitors_nearby(
    lat: float,
    lng: float,
    exclude_place_id: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch up to MAX_COMPETITORS dentists using tiered radius.
    Base radius = 2 mi. If fewer than 5 competitors → expand to 5 mi.
    If still fewer than 5 → expand to 8 mi. Hard stop after 8 mi.

    Returns (competitors, search_radius_used_miles) where competitors have:
    name, rating, reviews, distance_miles (from lead; 1 decimal).
    search_radius_used_miles is 2, 5, or 8.
    """
    try:
        from pipeline.fetch import PlacesFetcher  # noqa: avoid circular import
    except ImportError:
        logger.warning("PlacesFetcher not available; competitor sampling skipped")
        return ([], 2)
    key = api_key or os.getenv("GOOGLE_PLACES_API_KEY")
    if not key:
        return ([], 2)

    fetcher = PlacesFetcher(api_key=key)
    radius_used_miles = 2

    def raw_to_competitor(p: Dict, lead_lat: float, lead_lng: float) -> Optional[Dict[str, Any]]:
        if not p.get("place_id"):
            return None
        loc = (p.get("geometry") or {}).get("location") or {}
        plat, plng = loc.get("lat"), loc.get("lng")
        if plat is None or plng is None:
            return None
        dist = _haversine_miles(lead_lat, lead_lng, float(plat), float(plng))
        rating = p.get("rating")
        reviews = p.get("user_ratings_total") or 0
        return {
            "place_id": p.get("place_id"),
            "name": p.get("name") or "",
            "rating": float(rating) if rating is not None else None,
            "reviews": int(reviews),
            "distance_miles": dist,
        }

    def fetch_at_radius(radius_m: int) -> List[Dict[str, Any]]:
        raw = fetcher.fetch_nearby_places(lat=lat, lng=lng, radius_m=radius_m, keyword=KEYWORD)
        out = []
        seen_pids = set()
        for p in raw:
            pid = p.get("place_id")
            if pid == exclude_place_id or pid in seen_pids:
                continue
            c = raw_to_competitor(p, lat, lng)
            if c:
                seen_pids.add(pid)
                out.append(c)
        return out

    seen_names: set = set()
    out: List[Dict[str, Any]] = []

    def add_from(candidates: List[Dict[str, Any]]) -> None:
        for c in candidates:
            key = (c.get("name") or "").strip() or str(c.get("distance_miles"))
            if key in seen_names:
                continue
            seen_names.add(key)
            out.append(c)
            if len(out) >= MAX_COMPETITORS:
                return

    base_list = fetch_at_radius(RADIUS_BASE_M)
    add_from(base_list)
    if len(out) >= MIN_COMPETITORS_BEFORE_EXPAND:
        radius_used_miles = 2
    else:
        mid_list = fetch_at_radius(RADIUS_MID_M)
        add_from(mid_list)
        if len(out) >= MIN_COMPETITORS_BEFORE_EXPAND:
            radius_used_miles = 5
        else:
            max_list = fetch_at_radius(RADIUS_MAX_M)
            add_from(max_list)
            radius_used_miles = 8

    return (out[:MAX_COMPETITORS], radius_used_miles)


def _same_domain(base: str, url: str) -> bool:
    try:
        b = urlparse(base).netloc.lower().replace("www.", "")
        u = urlparse(url).netloc.lower().replace("www.", "")
        return b == u
    except Exception:
        return False


def _normalize_url(base: str, href: str) -> Optional[str]:
    try:
        full = urljoin(base, href)
        parsed = urlparse(full)
        if parsed.scheme not in ("http", "https"):
            return None
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except Exception:
        return None


def _fetch_html_fast(url: str, timeout_sec: float = 3.0) -> Optional[str]:
    try:
        import requests
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        r = requests.get(
            url,
            timeout=timeout_sec,
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        return None
    return None


def _extract_links(html: str, base_url: str) -> List[str]:
    if not html:
        return []
    out: List[str] = []
    seen = set()
    for m in re.finditer(r'href\s*=\s*["\']([^"\'#]+)["\']', html, re.I):
        full = _normalize_url(base_url, m.group(1).strip())
        if full and _same_domain(base_url, full) and full not in seen:
            seen.add(full)
            out.append(full)
    return out[:200]


def _is_service_like_path(url: str) -> bool:
    path = (urlparse(url).path or "").lower()
    tokens = set(re.findall(r"[a-z0-9]+", path))
    if not tokens:
        return False
    return any(t in tokens for t in SERVICE_PATH_TOKENS)


def _fetch_sitemap_urls_fast(base_url: str, timeout_sec: float = 3.0) -> List[str]:
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    candidates = ["/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml"]
    out: List[str] = []
    for sp in candidates:
        xml = _fetch_html_fast(root + sp, timeout_sec=timeout_sec)
        if not xml:
            continue
        for m in re.finditer(r"<loc>\s*(https?://[^<]+?)\s*</loc>", xml, re.I):
            u = m.group(1).strip()
            if _same_domain(base_url, u):
                norm = _normalize_url(base_url, u)
                if norm and norm not in out:
                    out.append(norm)
                    if len(out) >= 80:
                        return out
        if out:
            break
    return out


def _strip_html(html: str) -> str:
    text = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", html or "", flags=re.I)
    text = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.lower().strip()


def _word_count(text: str) -> int:
    return len(re.findall(r"[a-zA-Z0-9']+", text or ""))


def _has_schema(html: str) -> bool:
    lower = (html or "").lower()

    # Strict JSON-LD parsing: only count relevant schema types.
    for m in re.finditer(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html or "",
        re.I | re.S,
    ):
        raw = (m.group(1) or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        nodes = []
        if isinstance(parsed, dict):
            if isinstance(parsed.get("@graph"), list):
                nodes.extend([n for n in parsed.get("@graph") if isinstance(n, dict)])
            nodes.append(parsed)
        elif isinstance(parsed, list):
            nodes.extend([n for n in parsed if isinstance(n, dict)])
        for node in nodes:
            types = node.get("@type")
            type_vals: List[str] = []
            if isinstance(types, str):
                type_vals = [types.lower()]
            elif isinstance(types, list):
                type_vals = [str(t).lower() for t in types]
            if any(
                t in ("service", "faqpage", "localbusiness", "medicalbusiness", "dentist")
                or t.endswith("service")
                for t in type_vals
            ):
                return True

    # Microdata fallback (strict markers only)
    return any(
        marker in lower
        for marker in (
            "schema.org/service",
            "schema.org/faqpage",
            "schema.org/localbusiness",
            "schema.org/medicalbusiness",
            "schema.org/dentist",
        )
    )


def _compute_lightweight_site_metrics(
    website_url: str,
    page_timeout_sec: float,
    max_pages: int,
) -> Optional[Dict[str, float]]:
    base = website_url if website_url.startswith(("http://", "https://")) else f"https://{website_url}"
    homepage = _fetch_html_fast(base, timeout_sec=page_timeout_sec)
    if not homepage:
        return None

    candidate_urls: List[str] = []
    for u in _extract_links(homepage, base):
        if _is_service_like_path(u):
            candidate_urls.append(u)
    for u in _fetch_sitemap_urls_fast(base, timeout_sec=page_timeout_sec):
        if _is_service_like_path(u) and u not in candidate_urls:
            candidate_urls.append(u)
        if len(candidate_urls) >= max_pages * 3:
            break

    if not candidate_urls:
        return {
            "service_page_count": 0.0,
            "pages_with_schema": 0.0,
            "avg_word_count": 0.0,
        }

    sampled = candidate_urls[:max_pages]
    words: List[int] = []
    with_schema = 0
    fetched = 0
    for u in sampled:
        html = _fetch_html_fast(u, timeout_sec=page_timeout_sec)
        if not html:
            continue
        fetched += 1
        words.append(_word_count(_strip_html(html)))
        if _has_schema(html):
            with_schema += 1

    if fetched == 0:
        return {
            "service_page_count": 0.0,
            "pages_with_schema": 0.0,
            "avg_word_count": 0.0,
        }
    return {
        "service_page_count": float(fetched),
        "pages_with_schema": float(with_schema),
        "avg_word_count": float(sum(words) / len(words)) if words else 0.0,
    }


def enrich_competitors_with_site_metrics(
    competitors: List[Dict[str, Any]],
    vertical: str = "dentist",
    max_competitors_to_crawl: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Enrich competitor rows with lightweight website-derived metrics used by competitive_delta:
    - service_page_count
    - pages_with_schema
    - avg_word_count

    Gated by env:
    - NEYMA_ENABLE_COMPETITOR_SITE_CRAWL: true/false (default true)
    - NEYMA_COMPETITOR_SITE_CRAWL_MAX: max competitors to crawl (default 1)
    - NEYMA_COMPETITOR_SITE_CRAWL_BUDGET_SEC: global wall-clock budget (default 8)
    - NEYMA_COMPETITOR_SITE_CRAWL_PAGE_TIMEOUT_SEC: per-page timeout (default 3)
    - NEYMA_COMPETITOR_SITE_CRAWL_MAX_PAGES_PER_SITE: sampled pages/site (default 6)
    """
    raw_flag = (os.getenv("NEYMA_ENABLE_COMPETITOR_SITE_CRAWL", "true") or "").strip().lower()
    if raw_flag not in ("1", "true", "yes", "on"):
        return competitors
    if not competitors:
        return competitors

    crawl_max = max_competitors_to_crawl
    if crawl_max is None:
        try:
            crawl_max = int(os.getenv("NEYMA_COMPETITOR_SITE_CRAWL_MAX", "1"))
        except ValueError:
            crawl_max = 1
    crawl_max = max(0, min(int(crawl_max), len(competitors)))
    if crawl_max <= 0:
        return competitors

    try:
        from pipeline.enrich import PlaceDetailsEnricher
        enricher = PlaceDetailsEnricher()
    except Exception as e:
        logger.warning("Could not initialize PlaceDetailsEnricher for competitor site metrics: %s", e)
        return competitors

    try:
        budget_sec = float(os.getenv("NEYMA_COMPETITOR_SITE_CRAWL_BUDGET_SEC", "8"))
    except ValueError:
        budget_sec = 8.0
    try:
        page_timeout_sec = float(os.getenv("NEYMA_COMPETITOR_SITE_CRAWL_PAGE_TIMEOUT_SEC", "3"))
    except ValueError:
        page_timeout_sec = 3.0
    try:
        max_pages_per_site = int(os.getenv("NEYMA_COMPETITOR_SITE_CRAWL_MAX_PAGES_PER_SITE", "6"))
    except ValueError:
        max_pages_per_site = 6
    max_pages_per_site = max(1, min(max_pages_per_site, 12))

    t0 = time.monotonic()
    out: List[Dict[str, Any]] = []
    crawled = 0
    for idx, comp in enumerate(competitors):
        row = dict(comp or {})
        row["competitor_site_metric_status"] = "not_attempted"
        if idx >= crawl_max:
            out.append(row)
            continue
        if (time.monotonic() - t0) >= budget_sec:
            row["competitor_site_metric_status"] = "budget_exceeded"
            out.append(row)
            continue

        place_id = row.get("place_id")
        if not place_id:
            row["competitor_site_metric_status"] = "missing_place_id"
            out.append(row)
            continue

        try:
            details = enricher.get_place_details(place_id, fields=["website"])
        except Exception as e:
            row["competitor_site_metric_status"] = f"place_details_error:{type(e).__name__}"
            out.append(row)
            continue

        website = (details or {}).get("website") if isinstance(details, dict) else None
        if not website:
            row["competitor_site_metric_status"] = "no_website"
            out.append(row)
            continue

        try:
            metrics = _compute_lightweight_site_metrics(
                website_url=str(website),
                page_timeout_sec=page_timeout_sec,
                max_pages=max_pages_per_site,
            )
            row["competitor_website"] = website
            if metrics is None:
                row["competitor_site_metric_status"] = "crawl_failed"
            else:
                row["service_page_count"] = int(metrics.get("service_page_count") or 0)
                row["pages_with_schema"] = int(metrics.get("pages_with_schema") or 0)
                row["avg_word_count"] = float(metrics.get("avg_word_count") or 0)
                row["competitor_site_metric_status"] = "ok"
                crawled += 1
        except Exception as e:
            row["competitor_site_metric_status"] = f"crawl_error:{type(e).__name__}"

        out.append(row)

    logger.info(
        "Competitor site metrics (fast): crawled=%d/%d attempted=%d total=%d elapsed=%.2fs",
        crawled,
        crawl_max,
        min(crawl_max, len(competitors)),
        len(competitors),
        (time.monotonic() - t0),
    )
    return out


def _review_positioning_tier(review_ratio: Optional[float]) -> Optional[str]:
    if review_ratio is None:
        return None
    if review_ratio >= 1.75:
        return "Dominant"
    if review_ratio >= 1.2:
        return "Above Average"
    if review_ratio >= 0.8:
        return "Competitive"
    if review_ratio >= 0.5:
        return "Below Average"
    return "Weak"


def _review_positioning_label_from_tier(tier: Optional[str]) -> Optional[str]:
    if not tier:
        return None
    if tier in ("Dominant", "Above Average"):
        return "Above sample average"
    if tier == "Competitive":
        return "In line with sample average"
    return "Below sample average"


def build_competitive_snapshot(
    lead: Dict,
    competitors: List[Dict[str, Any]],
    search_radius_used_miles: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Build competitive_snapshot. No percentiles, no numeric pressure scores.
    Schema: dentists_sampled, search_radius_used_miles, avg_review_count, avg_rating,
    lead_review_count, review_positioning_tier, review_positioning, market_density_score,
    competitive_profile, competitor_summary (nearest, strongest by reviews),
    competitive_context_summary, confidence.
    """
    competitor_site_checked = sum(
        1
        for c in (competitors or [])
        if isinstance(c, dict) and c.get("service_page_count") is not None
    )
    out: Dict[str, Any] = {
        "dentists_sampled": 0,
        "search_radius_used_miles": search_radius_used_miles if search_radius_used_miles is not None else 2,
        "avg_review_count": 0.0,
        "avg_rating": 0.0,
        "lead_review_count": None,
        "review_positioning_tier": None,
        "review_positioning": None,
        "market_density_score": "Low",
        "competitive_profile": {},
        "competitor_summary": {},
        "competitive_context_summary": None,
        "confidence": 0.0,
        "top_5_avg_reviews": None,
        "competitor_median_reviews": None,
        "target_gap_from_median": None,
        "pct_competitors_with_blog": None,
        "pct_competitors_with_booking": None,
        "competitors_with_website_checked": int(competitor_site_checked),
    }

    if not competitors:
        out["competitor_summary"] = {}
        return out

    lead_count = lead.get("signal_review_count") or lead.get("review_count") or lead.get("user_ratings_total") or 0
    lead_count = int(lead_count)
    counts = [c.get("reviews") or c.get("user_ratings_total") or 0 for c in competitors]
    ratings = [c.get("rating") for c in competitors if c.get("rating") is not None]

    n = len(competitors)
    out["dentists_sampled"] = n
    out["lead_review_count"] = lead_count
    if search_radius_used_miles is not None:
        out["search_radius_used_miles"] = search_radius_used_miles

    avg_rev = 0.0
    if counts:
        avg_rev = round(sum(counts) / len(counts), 1)
        out["avg_review_count"] = avg_rev
        review_ratio = (lead_count / avg_rev) if avg_rev > 0 else None
        tier = _review_positioning_tier(review_ratio)
        out["review_positioning_tier"] = tier
        out["review_positioning"] = _review_positioning_label_from_tier(tier)
        top5 = sorted(counts, reverse=True)[:5]
        out["top_5_avg_reviews"] = round(sum(top5) / len(top5), 1) if top5 else None
        med = statistics.median(counts) if counts else None
        out["competitor_median_reviews"] = round(float(med), 1) if med is not None else None
        if med is not None:
            out["target_gap_from_median"] = round(float(lead_count) - float(med), 1)

    avg_rating = 0.0
    if ratings:
        avg_rating = round(sum(ratings) / len(ratings), 2)
        out["avg_rating"] = avg_rating

    # Market density
    if n >= 6 and avg_rev >= 100:
        out["market_density_score"] = "High"
    elif n >= 4 or avg_rev >= 60:
        out["market_density_score"] = "Moderate"
    else:
        out["market_density_score"] = "Low"

    # competitive_profile (labels only)
    review_volume_profile = "Low Volume Market"
    if avg_rev >= 150:
        review_volume_profile = "High Volume Market"
    elif avg_rev >= 60:
        review_volume_profile = "Moderate Volume Market"

    competitor_strength_profile = "Weak"
    if avg_rating >= 4.5:
        competitor_strength_profile = "Strong"
    elif avg_rating >= 4.0:
        competitor_strength_profile = "Mixed"

    competitive_intensity = "Fragmented"
    if n >= 6 and avg_rev >= 100:
        competitive_intensity = "Crowded & Established"
    elif n >= 4:
        competitive_intensity = "Competitive"

    out["competitive_profile"] = {
        "review_volume_profile": review_volume_profile,
        "competitor_strength_profile": competitor_strength_profile,
        "competitive_intensity": competitive_intensity,
    }

    # competitor_summary: nearest_competitor, strongest_competitor_by_reviews, nearest_competitors (top 3)
    entries = []
    nearest = None
    strongest = None
    for c in competitors:
        dist = c.get("distance_miles")
        revs = c.get("reviews") or c.get("user_ratings_total") or 0
        entry = {
            "place_id": c.get("place_id"),
            "name": c.get("name") or "",
            "rating": c.get("rating"),
            "reviews": revs,
            "distance_miles": dist,
        }
        entries.append(entry)
        if dist is not None and (nearest is None or dist < nearest.get("distance_miles", float("inf"))):
            nearest = entry.copy()
        if strongest is None or revs > (strongest.get("reviews") or 0):
            strongest = entry.copy()

    if nearest is not None:
        out["competitor_summary"]["nearest_competitor"] = nearest
    if strongest is not None:
        out["competitor_summary"]["strongest_competitor_by_reviews"] = strongest

    # nearest_competitors: top 3 by distance_miles (if available), else top 3 by reviews
    has_dist = any(e.get("distance_miles") is not None for e in entries)
    if has_dist:
        sorted_entries = sorted(entries, key=lambda e: (e.get("distance_miles") if e.get("distance_miles") is not None else float("inf"), -(e.get("reviews") or 0)))
    else:
        sorted_entries = sorted(entries, key=lambda e: -(e.get("reviews") or 0))
    out["competitor_summary"]["nearest_competitors"] = sorted_entries[:3]

    # competitive_context_summary
    tier = out.get("review_positioning_tier") or "—"
    density = out["market_density_score"]
    radius = out.get("search_radius_used_miles") or 2
    out["competitive_context_summary"] = (
        f"{lead_count} reviews vs {avg_rev:.0f} local avg across {n} practices within {radius} miles. "
        f"Market is {density}. Review tier: {tier}."
    )

    out["confidence"] = round(min(1.0, 0.4 + 0.15 * n), 2)
    return out
