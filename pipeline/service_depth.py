"""
Service depth detection for dental leads.

Crawls homepage, sitemap, nav links (two levels deep), detects high-ticket
vs general service pages, flags truly missing high-value pages.
Used for revenue leverage and intervention quality.
"""

import re
import logging
import json
from typing import Dict, Any, List, Optional, Set
from urllib.parse import urljoin, urlparse

from pipeline.high_value_services_config import (
    HIGH_VALUE_SERVICES,
    SERVICE_PAGE_CONFIG,
    DEPTH_MODERATE_MIN_WORDS,
    DEPTH_STRONG_MIN_WORDS,
    INTERNAL_LINKS_MIN_STRONG,
    STRONG_COVERAGE_RATIO_FOR_LOW_LEVERAGE,
    STRONG_SERP_RATIO_FOR_LOW_LEVERAGE,
)
from pipeline.service_page_qualification_agent_v2 import qualify_service_page_candidate_v2
from pipeline.internal_link_support import (
    CrawlConfig as InternalLinkCrawlConfig,
    build_internal_link_graph,
    compute_inbound_internal_link_support,
    wiring_status,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical service buckets — single source of truth for aliasing
# ---------------------------------------------------------------------------
CANONICAL_BUCKETS: Dict[str, List[str]] = {
    "implants": ["implant", "implants", "dental implant", "dental implants", "all-on-4", "all on 4"],
    "orthodontics": ["orthodontic", "orthodontics", "invisalign", "braces", "clear aligner", "clear aligners"],
    "veneers": ["veneer", "veneers", "porcelain veneer", "porcelain veneers"],
    "cosmetic": ["cosmetic", "cosmetic dentistry", "smile makeover", "teeth whitening", "whitening"],
    "sedation": ["sedation", "sedation dentistry", "iv sedation", "nitrous", "nitrous oxide", "oral sedation", "sleep dentistry"],
    "emergency": ["emergency", "emergency dentist", "emergency dental", "same day", "same-day", "urgent dental"],
    "crowns": ["crown", "crowns", "same day crown", "same-day crown", "dental crown", "dental crowns"],
    "sleep_apnea": ["sleep apnea", "sleep-apnea", "snoring"],
}

CANONICAL_DISPLAY: Dict[str, str] = {
    "implants": "Implants",
    "orthodontics": "Orthodontics",
    "veneers": "Veneers",
    "cosmetic": "Cosmetic",
    "sedation": "Sedation",
    "emergency": "Emergency",
    "crowns": "Crowns",
    "sleep_apnea": "Sleep Apnea",
}

# Flat lookup: keyword → canonical bucket
_KW_TO_BUCKET: Dict[str, str] = {}
_ALL_KEYWORDS: List[str] = []
for _bucket, _aliases in CANONICAL_BUCKETS.items():
    for _alias in _aliases:
        _KW_TO_BUCKET[_alias.lower()] = _bucket
        if _alias.lower() not in _ALL_KEYWORDS:
            _ALL_KEYWORDS.append(_alias.lower())
_ALL_KEYWORDS.sort(key=len, reverse=True)

# URL slug fragments that indicate a service page
SERVICE_PATH_TOKENS = {
    "implant", "implants", "invisalign", "veneer", "veneers",
    "cosmetic", "sedation", "emergency", "crown", "crowns",
    "sleep", "apnea", "orthodontic", "orthodontics", "braces",
    "service", "services", "treatment", "treatments", "procedure",
    "procedures", "whitening", "makeover", "dental", "dentistry",
    "restorative", "preventive", "periodontal", "endodontic",
}

GENERAL_KEYWORDS = [
    "cleaning", "family dentist", "checkup", "filling", "fillings",
    "general dentistry", "preventive", "exam", "x-ray", "hygiene",
    "periodontal", "root canal",
]

# Minimum keyword mentions in body text to count as content-dedicated
CONTENT_DEDICATION_THRESHOLD = 3
# Minimum page text length (chars) to be considered substantial
SUBSTANTIAL_PAGE_LENGTH = 300


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_html(url: str) -> Optional[str]:
    try:
        import requests
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        r = requests.get(
            url, timeout=12, allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"},
        )
        if r.status_code == 200:
            return r.text
    except Exception as e:
        logger.debug("Fetch failed %s: %s", url[:60], e)
    return None


def _normalize_url(base: str, href: str) -> Optional[str]:
    try:
        full = urljoin(base, href)
        parsed = urlparse(full)
        if parsed.scheme not in ("http", "https"):
            return None
        clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if clean.endswith("/"):
            clean = clean[:-1]
        return clean
    except Exception:
        return None


def _same_domain(base: str, url: str) -> bool:
    try:
        b = urlparse(base).netloc.lower().replace("www.", "")
        u = urlparse(url).netloc.lower().replace("www.", "")
        return b == u
    except Exception:
        return False


def _extract_links(html: str, base_url: str) -> List[str]:
    """Extract unique same-domain links from HTML."""
    if not html:
        return []
    seen = set()
    out = []
    for m in re.finditer(r'href\s*=\s*["\']([^"\'#]+)["\']', html, re.I):
        full = _normalize_url(base_url, m.group(1).strip())
        if full and _same_domain(base_url, full) and full not in seen:
            seen.add(full)
            out.append(full)
    return out[:120]


def _extract_link_targets_with_anchor(html: str, base_url: str) -> List[Dict[str, str]]:
    if not html:
        return []
    out: List[Dict[str, str]] = []
    for m in re.finditer(r"<a[^>]*href\s*=\s*[\"']([^\"'#]+)[\"'][^>]*>(.*?)</a>", html, re.I | re.S):
        href = (m.group(1) or "").strip()
        target = _normalize_url(base_url, href)
        if not target or not _same_domain(base_url, target):
            continue
        anchor = re.sub(r"<[^>]+>", " ", m.group(2) or "")
        anchor = re.sub(r"\s+", " ", anchor).strip().lower()
        out.append({"target": target, "anchor": anchor})
    return out[:300]


def _path_slugs(url: str) -> Set[str]:
    path = urlparse(url).path.lower()
    return set(re.findall(r"[a-z0-9]+", path))


def _stem(word: str) -> str:
    w = word.lower()
    if w.endswith("ics"):
        return w[:-1]
    if w.endswith("es") and len(w) > 3:
        return w[:-2]
    if w.endswith("s") and len(w) > 3:
        return w[:-1]
    return w


def _stems(words: Set[str]) -> Set[str]:
    return {_stem(w) for w in words}


def _is_service_like_path(url: str) -> bool:
    slugs = _path_slugs(url)
    stemmed = _stems(slugs)
    for token in SERVICE_PATH_TOKENS:
        if token in slugs or _stem(token) in stemmed:
            return True
    return False


def _is_asset_like_url(url: str) -> bool:
    path = (urlparse(url).path or "").lower()
    return bool(
        re.search(
            r"\.(?:jpg|jpeg|png|gif|svg|webp|ico|pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|css|js|map|woff|woff2|ttf|eot|mp4|mov|avi)$",
            path,
        )
    )


def _is_pricing_like_path(url: str) -> bool:
    slugs = _path_slugs(url)
    for s in ("pricing", "price", "cost", "fees", "insurance", "payment"):
        if s in slugs:
            return True
    return False


def _strip_html(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", html, flags=re.I)
    text = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.lower().strip()


def _strip_html_fragment(html_fragment: str) -> str:
    text = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", html_fragment or "", flags=re.I)
    text = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.lower().strip()


def _extract_primary_content_text(html: str) -> str:
    if not html:
        return ""
    cleaned = re.sub(r"<(nav|header|footer|aside)[^>]*>[\s\S]*?</\1>", " ", html, flags=re.I)
    main_match = re.search(r"<main[^>]*>([\s\S]*?)</main>", cleaned, re.I)
    if main_match:
        text = _strip_html_fragment(main_match.group(1))
        if len(text) > 80:
            return text
    article_match = re.search(r"<article[^>]*>([\s\S]*?)</article>", cleaned, re.I)
    if article_match:
        text = _strip_html_fragment(article_match.group(1))
        if len(text) > 80:
            return text
    body_match = re.search(r"<body[^>]*>([\s\S]*?)</body>", cleaned, re.I)
    if body_match:
        return _strip_html_fragment(body_match.group(1))
    return _strip_html_fragment(cleaned)


def _extract_headings(html: str) -> str:
    """Extract all h1/h2/h3 text, stripping nested tags."""
    if not html:
        return ""
    parts = []
    for m in re.finditer(r"<h[123][^>]*>(.*?)</h[123]>", html, re.I | re.S):
        inner = re.sub(r"<[^>]+>", " ", m.group(1))
        inner = re.sub(r"\s+", " ", inner).strip()
        if inner:
            parts.append(inner)
    title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if title_m:
        t = re.sub(r"<[^>]+>", " ", title_m.group(1)).strip()
        if t:
            parts.insert(0, t)
    return " ".join(parts).lower()


def _extract_title(html: str) -> str:
    if not html:
        return ""
    title_m = re.search(r"<title[^>]*>(.*?)</title>", html, re.I | re.S)
    if not title_m:
        return ""
    t = re.sub(r"<[^>]+>", " ", title_m.group(1) or "")
    return re.sub(r"\s+", " ", t).strip().lower()


def _extract_canonical_path(html: str, page_url: str) -> Optional[str]:
    if not html:
        return None
    m = re.search(r"<link[^>]+rel=[\"']canonical[\"'][^>]+href=[\"']([^\"']+)[\"']", html, re.I)
    if not m:
        return None
    href = (m.group(1) or "").strip()
    if not href:
        return None
    full = _normalize_url(page_url, href) if href.startswith("/") else href
    parsed = urlparse(full)
    path = parsed.path or "/"
    if not path.startswith("/"):
        path = "/" + path
    return path


def _extract_h1_text(html: str) -> str:
    if not html:
        return ""
    parts = []
    for m in re.finditer(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S):
        inner = re.sub(r"<[^>]+>", " ", m.group(1))
        inner = re.sub(r"\s+", " ", inner).strip()
        if inner:
            parts.append(inner.lower())
    return " ".join(parts).strip()


def _extract_h1_list(html: str) -> List[str]:
    if not html:
        return []
    out: List[str] = []
    for m in re.finditer(r"<h1[^>]*>(.*?)</h1>", html, re.I | re.S):
        inner = re.sub(r"<[^>]+>", " ", m.group(1))
        inner = re.sub(r"\s+", " ", inner).strip().lower()
        if inner:
            out.append(inner)
    return out


def _extract_h2_list(html: str) -> List[str]:
    if not html:
        return []
    out: List[str] = []
    for m in re.finditer(r"<h2[^>]*>(.*?)</h2>", html, re.I | re.S):
        inner = re.sub(r"<[^>]+>", " ", m.group(1))
        inner = re.sub(r"\s+", " ", inner).strip().lower()
        if inner:
            out.append(inner)
    return out


def _flatten_jsonld(value: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if isinstance(value, dict):
        if isinstance(value.get("@graph"), list):
            for node in value.get("@graph") or []:
                out.extend(_flatten_jsonld(node))
        else:
            out.append(value)
    elif isinstance(value, list):
        for item in value:
            out.extend(_flatten_jsonld(item))
    return out


def _extract_schema_flags(html: str) -> Dict[str, bool]:
    lower = (html or "").lower()
    has_faq = "faqpage" in lower
    has_service = False
    has_localbusiness = False

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
        for node in _flatten_jsonld(parsed):
            types = node.get("@type")
            type_vals: List[str] = []
            if isinstance(types, str):
                type_vals = [types.lower()]
            elif isinstance(types, list):
                type_vals = [str(t).lower() for t in types]
            if any(t == "faqpage" for t in type_vals):
                has_faq = True
            if any("service" == t or t.endswith("service") for t in type_vals):
                has_service = True
            if any(t in ("dentist", "medicalbusiness", "localbusiness") for t in type_vals):
                has_localbusiness = True

    # Microdata fallback
    if not has_faq and "schema.org/faqpage" in lower:
        has_faq = True
    if not has_service and "schema.org/service" in lower:
        has_service = True
    if not has_localbusiness and any(x in lower for x in ("schema.org/localbusiness", "schema.org/medicalbusiness", "schema.org/dentist")):
        has_localbusiness = True

    return {
        "has_service_schema": bool(has_service),
        "has_faq_schema": bool(has_faq),
        "has_localbusiness_schema": bool(has_localbusiness),
    }


def _has_faq_or_schema(html: str) -> bool:
    flags = _extract_schema_flags(html)
    return bool(flags["has_service_schema"] or flags["has_faq_schema"] or flags["has_localbusiness_schema"])


def _word_count(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"[a-zA-Z0-9']+", text))


def _extract_internal_links(html: str, base_url: str) -> List[str]:
    links = _extract_links(html, base_url)
    out = []
    for link in links:
        if link.rstrip("/") != base_url.rstrip("/"):
            out.append(link)
    return out


def _extract_conversion_metrics(html: str, page_text: str, base_url: str) -> Dict[str, Any]:
    lower = (html or "").lower()
    cta_patterns = [
        r"\bbook\b",
        r"\bschedule\b",
        r"\brequest appointment\b",
        r"\bcontact\b",
        r"\bcall now\b",
        r"\bget started\b",
        r"\bsubmit\b",
    ]
    cta_count = 0
    for pat in cta_patterns:
        cta_count += len(re.findall(pat, lower))
    booking_link = bool(re.search(r'href\s*=\s*["\'][^"\']*(book|schedule|appointment|calendly|zocdoc)[^"\']*["\']', lower))
    financing_mentioned = bool(re.search(r"\b(financing|payment plan|monthly payment|carecredit|affirm)\b", page_text))
    faq_section = bool(re.search(r"\bfaq\b|frequently asked questions", lower))
    before_after = bool(re.search(r"before\s*(and|&)\s*after|smile gallery", lower))
    internal_links = len(_extract_internal_links(html, base_url))
    return {
        "cta_count": int(cta_count),
        "booking_link": bool(booking_link),
        "financing_mentioned": bool(financing_mentioned),
        "faq_section": bool(faq_section),
        "before_after_section": bool(before_after),
        "internal_links": int(internal_links),
    }


def _is_homepage_url(url: str) -> bool:
    path = (urlparse(url).path or "").strip("/")
    return path == ""


def _count_service_mentions(text: str, aliases: List[str]) -> int:
    total = 0
    lower = (text or "").lower()
    for alias in aliases:
        a = (alias or "").strip().lower()
        if not a:
            continue
        total += len(re.findall(rf"\b{re.escape(a)}\b", lower))
    return total


def _keyword_density(mention_count: int, word_count: int) -> float:
    if word_count <= 0:
        return 0.0
    return float(mention_count) / float(word_count)


def is_umbrella_page(page_text: str, service_keywords_by_slug: Dict[str, List[str]]) -> bool:
    """
    True if page appears to be a broad services page, not single-service.
    """
    counts: Dict[str, int] = {}
    for slug, keywords in (service_keywords_by_slug or {}).items():
        cnt = _count_service_mentions(page_text or "", keywords or [])
        counts[str(slug)] = int(cnt)
    meaningful_min = int(SERVICE_PAGE_CONFIG["umbrella_meaningful_mentions"])
    meaningful = [v for v in counts.values() if v >= meaningful_min]
    if len(meaningful) >= int(SERVICE_PAGE_CONFIG["umbrella_service_threshold"]):
        return True
    top = sorted(meaningful, reverse=True)[:3]
    sim_ratio = float(SERVICE_PAGE_CONFIG["umbrella_top_similarity_ratio"])
    if len(top) >= 2 and top[0] > 0 and (float(top[1]) / float(top[0])) >= sim_ratio and len(meaningful) >= 3:
        return True
    return False


def _city_tokens(city: Optional[str], state: Optional[str]) -> Set[str]:
    toks: Set[str] = set()
    for raw in (city or "", state or ""):
        for t in re.findall(r"[a-z0-9]+", raw.lower()):
            if len(t) >= 3:
                toks.add(t)
    return toks


def _is_geo_or_location_page(url: str, headings: str, city_tokens: Set[str]) -> bool:
    path = (urlparse(url).path or "").lower()
    path_tokens = set(re.findall(r"[a-z0-9]+", path))
    geo_markers = {"near", "me", "location", "locations", "area", "areas", "serve", "serving", "neighborhood"}
    if path_tokens.intersection(geo_markers):
        return True
    if any(marker in headings for marker in ("near me", "locations", "areas we serve", "service area", "neighborhood")):
        return True
    if city_tokens and path_tokens.intersection(city_tokens):
        return True
    if city_tokens and any(tok in headings for tok in city_tokens):
        return True
    return False


# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------

def _fetch_sitemap_urls(base_url: str) -> List[str]:
    """Fetch sitemap.xml and extract same-domain URLs."""
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    urls: List[str] = []

    for sitemap_path in ("/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml"):
        xml = _fetch_html(root + sitemap_path)
        if not xml:
            continue
        for m in re.finditer(r"<loc>\s*(https?://[^<]+?)\s*</loc>", xml, re.I):
            url = m.group(1).strip()
            if _same_domain(base_url, url):
                normalized = _normalize_url(base_url, url)
                if normalized and normalized not in urls:
                    urls.append(normalized)
        if urls:
            break

    return urls[:200]


# ---------------------------------------------------------------------------
# Canonical bucket resolution
# ---------------------------------------------------------------------------

def _to_canonical(keyword: str) -> Optional[str]:
    """Map a raw keyword to its canonical bucket name."""
    return _KW_TO_BUCKET.get(keyword.lower().strip())


def _detect_buckets_in_text(text: str) -> Set[str]:
    """Return set of canonical bucket names mentioned in text."""
    buckets = set()
    for kw in _ALL_KEYWORDS:
        if kw in text:
            b = _to_canonical(kw)
            if b:
                buckets.add(b)
    return buckets


def _count_bucket_mentions(text: str, bucket: str) -> int:
    """Count how many times any alias of a bucket appears in text."""
    count = 0
    for alias in CANONICAL_BUCKETS.get(bucket, []):
        count += len(re.findall(re.escape(alias), text))
    return count


def _service_catalog(vertical: str) -> List[Dict[str, Any]]:
    catalog = HIGH_VALUE_SERVICES.get((vertical or "").strip().lower()) or HIGH_VALUE_SERVICES.get("dentist") or []
    out: List[Dict[str, Any]] = []
    for svc in catalog:
        if not isinstance(svc, dict):
            continue
        slug = str(svc.get("slug") or "").strip().lower()
        if not slug:
            continue
        aliases = [str(a).strip().lower() for a in (svc.get("aliases") or []) if str(a).strip()]
        aliases.append(slug.replace("_", " "))
        out.append(
            {
                "slug": slug,
                "display_name": str(svc.get("display_name") or slug.replace("_", " ").title()),
                "revenue_weight": int(svc.get("revenue_weight") or 1),
                "min_word_threshold": int(svc.get("min_word_threshold") or DEPTH_STRONG_MIN_WORDS),
                "min_internal_links": int(svc.get("min_internal_links") or INTERNAL_LINKS_MIN_STRONG),
                "aliases": sorted(set(aliases), key=len, reverse=True),
            }
        )
    return out


def _depth_score(word_count: int) -> str:
    if word_count >= DEPTH_STRONG_MIN_WORDS:
        return "strong"
    if word_count >= DEPTH_MODERATE_MIN_WORDS:
        return "moderate"
    return "weak"


def _classify_optimization_tier(
    page_exists: bool,
    word_count: int,
    has_schema: bool,
    internal_links: int,
    position_top_10: Optional[bool],
    min_word_threshold: int,
    min_internal_links: int,
) -> str:
    if not page_exists:
        return "missing"
    if (
        page_exists
        and word_count >= min_word_threshold
        and has_schema
        and internal_links >= min_internal_links
        and position_top_10 is True
    ):
        return "strong"
    if (
        page_exists
        and word_count >= DEPTH_MODERATE_MIN_WORDS
        and (position_top_10 is False or position_top_10 is None)
        and (not has_schema or internal_links < min_internal_links)
    ):
        return "moderate"
    if page_exists and (word_count < DEPTH_MODERATE_MIN_WORDS or position_top_10 is None or (not has_schema)):
        return "weak"
    return "moderate"


_STATUS_RANK = {
    "strong": 4,
    "moderate": 3,
    "weak": 2,
    "weak_stub_page": 1,
    "missing": 0,
}


def _status_rank(status: str) -> int:
    return int(_STATUS_RANK.get(str(status or "").strip().lower(), 0))


def _strict_service_qualification_status(
    *,
    rule1_match: bool,
    homepage_allowed: bool,
    word_count: int,
    keyword_density: float,
    h1_match: bool,
    structure_signal_any: bool,
    internal_links_to_page: int,
) -> str:
    min_word_count = int(SERVICE_PAGE_CONFIG["min_word_count"])
    stub_word_count_max = int(SERVICE_PAGE_CONFIG["stub_word_count_max"])
    min_density = float(SERVICE_PAGE_CONFIG["min_keyword_density"])
    min_internal_links = int(SERVICE_PAGE_CONFIG["min_internal_links"])

    if not rule1_match:
        return "missing"
    if not homepage_allowed:
        return "missing"

    if word_count < stub_word_count_max or not h1_match or not structure_signal_any:
        return "weak_stub_page"

    if word_count < min_word_count or keyword_density < min_density:
        return "weak"

    if internal_links_to_page < min_internal_links:
        return "moderate"

    return "strong"


def _safe_int(val: Any) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _tri_state(detected_count: int, scope_count: int) -> str:
    if scope_count <= 0:
        return "unknown"
    return "detected" if detected_count > 0 else "not_detected"


def _crawl_confidence(pages_checked: int) -> str:
    if pages_checked <= 0:
        return "unknown"
    if pages_checked < 4:
        return "low"
    if pages_checked < 10:
        return "medium"
    return "high"


# ---------------------------------------------------------------------------
# Page-level detection
# ---------------------------------------------------------------------------

def _dedicated_buckets_for_page(
    url: str, path_slugs: Set[str], headings: str, page_text: str,
) -> Set[str]:
    """
    Determine which canonical buckets this page is DEDICATED to.
    Three signals (any one is sufficient):
      1. URL path slugs match bucket aliases
      2. Headings (h1/h2/h3/title) contain bucket keywords
      3. Body text mentions a bucket's keywords >= CONTENT_DEDICATION_THRESHOLD times
         AND the page has substantial content (not just a nav listing)
    """
    dedicated: Set[str] = set()
    stemmed_slugs = _stems(path_slugs)

    for bucket, aliases in CANONICAL_BUCKETS.items():
        # Signal 1: URL slug match
        for alias in aliases:
            alias_parts = alias.replace("-", " ").split()
            alias_stems = {_stem(p) for p in alias_parts}
            if alias_stems and alias_stems.issubset(stemmed_slugs):
                dedicated.add(bucket)
                break

        if bucket in dedicated:
            continue

        # Signal 2: Heading match
        for alias in aliases:
            if alias in headings:
                dedicated.add(bucket)
                break

        if bucket in dedicated:
            continue

        # Signal 3: Content-frequency dedication
        if len(page_text) >= SUBSTANTIAL_PAGE_LENGTH:
            mentions = _count_bucket_mentions(page_text, bucket)
            if mentions >= CONTENT_DEDICATION_THRESHOLD:
                dedicated.add(bucket)

    return dedicated


def _mentioned_buckets_in_page(page_text: str) -> Set[str]:
    """Return canonical buckets mentioned at least once in page body."""
    return _detect_buckets_in_text(page_text)


# ---------------------------------------------------------------------------
# Main intelligence builder
# ---------------------------------------------------------------------------

def build_service_intelligence(
    website_url: Optional[str],
    website_html: Optional[str] = None,
    procedure_mentions_from_reviews: Optional[List[str]] = None,
    city: Optional[str] = None,
    state: Optional[str] = None,
    vertical: str = "dentist",
) -> Dict[str, Any]:
    """
    Build service_intelligence block with canonical bucket resolution.

    Crawls homepage → sitemap → nav links (two levels) to build a comprehensive
    page inventory. Detects which high-ticket services have dedicated pages vs.
    are merely mentioned, and flags truly missing opportunities.
    """
    out: Dict[str, Any] = {
        "high_ticket_procedures_detected": [],
        "general_services_detected": [],
        "missing_high_value_pages": [],
        "procedure_confidence": 0.0,
        "pages_crawled": 0,
        "service_page_count": 0,
        "service_pages_with_faq_or_schema": 0,
        "avg_word_count_service_pages": 0,
        "min_word_count_service_pages": 0,
        "max_word_count_service_pages": 0,
        "city_or_near_me_page_count": 0,
        "has_multi_location_page": False,
        "geo_page_examples": [],
        "blog_page_count": 0,
        "high_value_services": [],
        "high_value_summary": {
            "total_high_value_services": 0,
            "services_present": 0,
            "services_missing": 0,
            "services_strong": 0,
            "services_moderate": 0,
            "services_weak": 0,
            "service_coverage_ratio": 0.0,
            "optimized_ratio": 0.0,
            "average_word_count": 0,
            "serp_visibility_ratio": 0.0,
        },
        "high_value_service_leverage": "low",
    }
    if not (website_url or "").strip():
        return out

    base_url = website_url if website_url.startswith(("http://", "https://")) else "https://" + website_url
    html = website_html or _fetch_html(base_url)
    if not html:
        return out

    # -- Phase 1: Collect all known URLs --------------------------------
    all_urls: Set[str] = set()
    homepage_links = _extract_links(html, base_url)
    all_urls.update(homepage_links)

    sitemap_urls = _fetch_sitemap_urls(base_url)
    all_urls.update(sitemap_urls)
    logger.debug("Sitemap yielded %d URLs", len(sitemap_urls))

    # -- Phase 2: Identify service-like URLs to crawl -------------------
    service_urls = [u for u in all_urls if _is_service_like_path(u) and (not _is_asset_like_url(u)) and u != base_url]
    service_urls_deduped: List[str] = []
    seen_paths: Set[str] = set()
    for u in service_urls:
        p = urlparse(u).path.lower().rstrip("/")
        if p not in seen_paths:
            seen_paths.add(p)
            service_urls_deduped.append(u)

    # -- Phase 3: Crawl pages (homepage + up to 20 service pages) -------
    pages: List[Dict[str, Any]] = []
    geo_tokens = _city_tokens(city, state)

    homepage_text = _extract_primary_content_text(html)
    homepage_headings = _extract_headings(html)
    homepage_schema = _extract_schema_flags(html)
    homepage_conversion = _extract_conversion_metrics(html, homepage_text, base_url)
    pages.append(
        {
            "url": base_url,
            "html": html,
            "text": homepage_text,
            "h1": _extract_h1_text(html),
            "headings": homepage_headings,
            "path_slugs": _path_slugs(base_url),
            "schema": homepage_schema,
            "has_faq_or_schema": bool(
                homepage_schema["has_service_schema"] or homepage_schema["has_faq_schema"] or homepage_schema["has_localbusiness_schema"]
            ),
            "word_count": _word_count(homepage_text),
            "is_geo": _is_geo_or_location_page(base_url, homepage_headings, geo_tokens),
            "conversion": homepage_conversion,
        }
    )

    level1_fetched: Set[str] = {base_url}
    level2_candidates: List[str] = []

    for url in service_urls_deduped[:20]:
        if url in level1_fetched:
            continue
        h = _fetch_html(url)
        if not h:
            continue
        level1_fetched.add(url)
        page_text = _extract_primary_content_text(h)
        headings = _extract_headings(h)
        schema_flags = _extract_schema_flags(h)
        pages.append(
            {
                "url": url,
                "html": h,
                "text": page_text,
                "h1": _extract_h1_text(h),
                "headings": headings,
                "path_slugs": _path_slugs(url),
                "schema": schema_flags,
                "has_faq_or_schema": bool(
                    schema_flags["has_service_schema"] or schema_flags["has_faq_schema"] or schema_flags["has_localbusiness_schema"]
                ),
                "word_count": _word_count(page_text),
                "is_geo": _is_geo_or_location_page(url, headings, geo_tokens),
                "conversion": _extract_conversion_metrics(h, page_text, base_url),
            }
        )

        # Two-level crawl: extract links from this sub-page
        sub_links = _extract_links(h, url)
        for sl in sub_links:
            if sl not in level1_fetched and _is_service_like_path(sl) and (not _is_asset_like_url(sl)):
                level2_candidates.append(sl)

    # Crawl level-2 pages (deeper service pages linked from index pages)
    level2_seen: Set[str] = set()
    for url in level2_candidates:
        if len(pages) >= 35:
            break
        p = urlparse(url).path.lower().rstrip("/")
        if p in seen_paths or url in level2_seen:
            continue
        level2_seen.add(url)
        seen_paths.add(p)
        h = _fetch_html(url)
        if not h:
            continue
        page_text = _extract_primary_content_text(h)
        headings = _extract_headings(h)
        schema_flags = _extract_schema_flags(h)
        pages.append(
            {
                "url": url,
                "html": h,
                "text": page_text,
                "h1": _extract_h1_text(h),
                "headings": headings,
                "path_slugs": _path_slugs(url),
                "schema": schema_flags,
                "has_faq_or_schema": bool(
                    schema_flags["has_service_schema"] or schema_flags["has_faq_schema"] or schema_flags["has_localbusiness_schema"]
                ),
                "word_count": _word_count(page_text),
                "is_geo": _is_geo_or_location_page(url, headings, geo_tokens),
                "conversion": _extract_conversion_metrics(h, page_text, base_url),
            }
        )

    out["pages_crawled"] = len(pages)

    # -- Phase 4: Detect services per page (canonical buckets) ----------
    all_dedicated_buckets: Set[str] = set()
    all_mentioned_buckets: Set[str] = set()
    all_general: Set[str] = set()

    service_page_word_counts: List[int] = []
    service_pages_with_schema = 0
    geo_examples: List[str] = []
    blog_count = 0
    for page in pages:
        url = str(page.get("url") or "")
        page_text = str(page.get("text") or "")
        headings = str(page.get("headings") or "")
        pslugs = set(page.get("path_slugs") or set())
        has_faq_schema = bool(page.get("has_faq_or_schema"))
        page_word_count = _safe_int(page.get("word_count"))
        is_geo = bool(page.get("is_geo"))
        dedicated = _dedicated_buckets_for_page(url, pslugs, headings, page_text)
        mentioned = _mentioned_buckets_in_page(page_text)
        general = set()
        for gkw in GENERAL_KEYWORDS:
            if gkw in page_text:
                general.add(gkw)

        all_dedicated_buckets |= dedicated
        all_mentioned_buckets |= mentioned
        all_general |= general
        if dedicated:
            service_page_word_counts.append(page_word_count)
            if has_faq_schema:
                service_pages_with_schema += 1
        if is_geo:
            if len(geo_examples) < 5:
                geo_examples.append(url)
        path_lower = (urlparse(url).path or "").lower()
        if any(tok in path_lower for tok in ("/blog", "/article", "/news", "/insights")):
            blog_count += 1

    # -- Phase 5: Also check sitemap URLs by slug (no fetch needed) -----
    # If a sitemap URL clearly matches a bucket via path, count it as dedicated
    for url in sitemap_urls:
        pslugs = _path_slugs(url)
        for bucket, aliases in CANONICAL_BUCKETS.items():
            if bucket in all_dedicated_buckets:
                continue
            stemmed = _stems(pslugs)
            for alias in aliases:
                parts = alias.replace("-", " ").split()
                if {_stem(p) for p in parts}.issubset(stemmed):
                    all_dedicated_buckets.add(bucket)
                    break

    # -- Phase 6: Build output using canonical display names ------------
    all_detected = all_dedicated_buckets | all_mentioned_buckets
    out["high_ticket_procedures_detected"] = sorted(
        [CANONICAL_DISPLAY[b] for b in all_detected if b in CANONICAL_DISPLAY]
    )
    out["general_services_detected"] = sorted(list(all_general))[:10]
    out["service_page_count"] = int(len(service_page_word_counts))
    out["service_pages_with_faq_or_schema"] = int(service_pages_with_schema)
    if service_page_word_counts:
        out["avg_word_count_service_pages"] = int(sum(service_page_word_counts) / len(service_page_word_counts))
        out["min_word_count_service_pages"] = int(min(service_page_word_counts))
        out["max_word_count_service_pages"] = int(max(service_page_word_counts))
    out["city_or_near_me_page_count"] = int(len(geo_examples))
    out["has_multi_location_page"] = any("locations" in (urlparse(u).path or "").lower() for u in geo_examples)
    out["geo_page_examples"] = geo_examples[:3]
    out["blog_page_count"] = int(blog_count)

    # Missing = mentioned (on site or in reviews) but NO dedicated page
    review_buckets: Set[str] = set()
    for mention in (procedure_mentions_from_reviews or []):
        b = _to_canonical(mention)
        if b:
            review_buckets.add(b)

    all_mentioned_or_review = all_mentioned_buckets | review_buckets
    missing_buckets = all_mentioned_or_review - all_dedicated_buckets
    out["missing_high_value_pages"] = sorted(
        [CANONICAL_DISPLAY[b] for b in missing_buckets if b in CANONICAL_DISPLAY]
    )

    n_pages = len(pages)
    n_high = len(out["high_ticket_procedures_detected"])
    out["procedure_confidence"] = round(min(1.0, 0.3 + 0.1 * n_pages + 0.1 * n_high), 2)

    # -- Phase 7: Strict per-service intelligence ------------------------
    catalog = _service_catalog(vertical)
    service_keywords_by_slug: Dict[str, List[str]] = {
        str(svc["slug"]): sorted(set([str(a).lower() for a in svc.get("aliases", [])] + [str(svc["slug"]).replace("_", " ")]))
        for svc in catalog
    }
    service_display_by_slug = {str(svc["slug"]): str(svc["display_name"]) for svc in catalog}

    incoming_anchor_map: Dict[str, List[str]] = {}
    page_meta: Dict[str, Dict[str, Any]] = {}
    umbrella_triggered = False
    pages_crawled_urls: List[str] = []
    pages_rejected: List[Dict[str, str]] = []

    for page in pages:
        page_url = str(page.get("url") or "")
        page_path = str(urlparse(page_url).path or "/")
        pages_crawled_urls.append(page_url)
        html_raw = str(page.get("html") or "")
        title = _extract_title(html_raw)
        h1_list = _extract_h1_list(html_raw)
        h2_list = _extract_h2_list(html_raw)
        text = str(page.get("text") or "")
        wc = _safe_int(page.get("word_count"))
        page_umbrella = is_umbrella_page(text, service_keywords_by_slug)
        if page_umbrella:
            umbrella_triggered = True
        link_targets = _extract_link_targets_with_anchor(html_raw, page_url)
        for link in link_targets:
            target = str(link.get("target") or "").rstrip("/")
            if not target:
                continue
            incoming_anchor_map.setdefault(target, []).append(str(link.get("anchor") or ""))
        homepage_counts: Dict[str, int] = {}
        if _is_homepage_url(page_url):
            for slug, kws in service_keywords_by_slug.items():
                homepage_counts[slug] = _count_service_mentions(text, kws)
        page_meta[page_url] = {
            "url": page_url,
            "path": page_path,
            "canonical_path": _extract_canonical_path(html_raw, page_url) or page_path,
            "title": title,
            "h1_list": h1_list,
            "h2_list": h2_list,
            "text": text,
            "word_count": wc,
            "schema": page.get("schema") or {},
            "conversion": page.get("conversion") or {},
            "umbrella": page_umbrella,
            "homepage_counts": homepage_counts,
            "is_homepage": _is_homepage_url(page_url),
        }

    min_word_count = int(SERVICE_PAGE_CONFIG["min_word_count"])
    min_density = float(SERVICE_PAGE_CONFIG["min_keyword_density"])
    min_h2_sections = int(SERVICE_PAGE_CONFIG["min_h2_sections"])
    min_internal_links = int(SERVICE_PAGE_CONFIG["min_internal_links"])
    stub_word_count_max = int(SERVICE_PAGE_CONFIG["stub_word_count_max"])
    homepage_single_service_ratio = float(SERVICE_PAGE_CONFIG["homepage_single_service_ratio"])

    def _h1_match_fn(h1_list: List[str], aliases: List[str]) -> bool:
        return any(any(alias in h1 for alias in aliases) for h1 in (h1_list or []))

    def _title_match_fn(title: str, aliases: List[str]) -> bool:
        return any(alias in (title or "") for alias in aliases)

    def _path_match_fn(path: str, slug: str, aliases: List[str]) -> bool:
        path_lower = (path or "").lower().replace("_", "-")
        slug_token = slug.replace("_", "-")
        if re.search(rf"(^|/){re.escape(slug_token)}($|/|-)", path_lower):
            return True
        return any(alias.replace(" ", "-") in path_lower for alias in aliases if alias)

    def _incoming_links_to_page(target_url: str, aliases: List[str]) -> int:
        anchors = incoming_anchor_map.get(target_url.rstrip("/"), [])
        return int(sum(1 for a in anchors if any(alias in a for alias in aliases)))

    service_rows: List[Dict[str, Any]] = []
    for svc in catalog:
        slug = str(svc["slug"])
        aliases = service_keywords_by_slug.get(slug) or [slug.replace("_", " ")]
        best_candidate: Optional[Dict[str, Any]] = None
        best_rank = -1
        best_wc = -1
        saw_umbrella_mention = False
        saw_any_mention = False
        reasons_seen: List[str] = []

        for page_url, p in page_meta.items():
            text = str(p["text"] or "")
            wc = int(p["word_count"] or 0)
            mention_count = _count_service_mentions(text, aliases)
            if mention_count > 0:
                saw_any_mention = True
            rule1_path = _path_match_fn(str(p["path"] or ""), slug, aliases)
            rule1_title_h1 = _title_match_fn(str(p["title"] or ""), aliases) or _h1_match_fn(list(p.get("h1_list") or []), aliases)
            rule1_match = bool(rule1_path or rule1_title_h1)

            if bool(p.get("umbrella")) and mention_count > 0:
                saw_umbrella_mention = True
                pages_rejected.append({"url": str(p["url"]), "rejection_reason": "Umbrella page (>= threshold services mentioned)."})
                continue

            if not rule1_match:
                continue

            density = _keyword_density(mention_count, wc)
            h1_match = _h1_match_fn(list(p.get("h1_list") or []), aliases)
            h2_sections = len(p.get("h2_list") or [])
            schema = p.get("schema") or {}
            conversion = p.get("conversion") or {}
            faq_present = bool(schema.get("has_faq_schema")) or bool(conversion.get("faq_section"))
            financing_section = bool(conversion.get("financing_mentioned")) and mention_count > 0
            before_after_section = bool(conversion.get("before_after_section"))
            internal_links_to_page = _incoming_links_to_page(page_url, aliases)
            title = str(p.get("title") or "")
            agent_out = qualify_service_page_candidate_v2(
                {
                    "service_slug": slug,
                    "service_display": service_display_by_slug.get(slug, slug.replace("_", " ").title()),
                    "root_domain": urlparse(base_url).netloc,
                    "page": {
                        "url": str(p.get("path") or "/"),
                        "canonical_url": str(p.get("canonical_path") or p.get("path") or "/"),
                        "word_count": wc,
                        "h1": (list(p.get("h1_list") or [""])[0] if list(p.get("h1_list") or []) else ""),
                        "title": title,
                        "keyword_density": density,
                        "faq_present": faq_present,
                        "h2_sections": h2_sections,
                        "financing_section": financing_section,
                        "before_after_section": before_after_section,
                        "internal_links_to_page": internal_links_to_page,
                        "schema_types": [
                            x
                            for x, ok in (
                                ("Service", bool(schema.get("has_service_schema"))),
                                ("FAQPage", bool(schema.get("has_faq_schema"))),
                                ("Dentist", bool(schema.get("has_localbusiness_schema"))),
                            )
                            if ok
                        ],
                        "umbrella_page": bool(p.get("umbrella")),
                        "service_mentioned": bool(mention_count > 0),
                    },
                }
            )
            prediction_status = str(agent_out.get("prediction_status") or "missing")
            status = (
                "strong"
                if prediction_status == "strong"
                else "weak_stub_page"
                if prediction_status == "weak_stub_page"
                else "missing"
            )
            detection_reason = str(agent_out.get("qualification_reason") or "Does not qualify as dedicated page.")

            if prediction_status != "strong":
                pages_rejected.append({"url": str(p["url"]), "rejection_reason": detection_reason})

            candidate = {
                "service": slug,
                "display_name": service_display_by_slug.get(slug, slug.replace("_", " ").title()),
                "revenue_weight": int(svc["revenue_weight"]),
                "page_detected": bool(status != "missing"),
                "page_exists": bool(status != "missing"),
                "qualified": bool(agent_out.get("qualified")),
                "prediction_status": prediction_status,
                "detection_reason": detection_reason,
                "url": str(agent_out.get("final_url_evaluated") or p["path"] or "/"),
                "word_count": int(wc),
                "keyword_density": round(float(density), 4),
                "h1_match": bool(h1_match),
                "depth_score": _depth_score(wc) if status != "missing" else "missing",
                "schema": {
                    "service_schema": bool(schema.get("has_service_schema")),
                    "faq_schema": bool(schema.get("has_faq_schema")),
                    "localbusiness_schema": bool(schema.get("has_localbusiness_schema")),
                },
                "structural_signals": {
                    "faq_present": bool(faq_present),
                    "service_h2_sections": int(h2_sections),
                    "financing_section": bool(financing_section),
                    "before_after_section": bool(before_after_section),
                },
                "conversion": {
                    "cta_count": int(_safe_int(conversion.get("cta_count"))),
                    "booking_link": bool(conversion.get("booking_link")),
                    "financing_mentioned": bool(conversion.get("financing_mentioned")),
                    "faq_section": bool(conversion.get("faq_section")),
                    "before_after_section": bool(conversion.get("before_after_section")),
                    "internal_links": int(_safe_int(conversion.get("internal_links"))),
                },
                "internal_links_to_page": int(internal_links_to_page),
                "serp": {
                    "position_top_3": None,
                    "position_top_10": None,
                    "average_position": None,
                    "map_pack_presence": None,
                    "competitors_in_top_10": None,
                },
                "qualification_status": status,
                "optimization_tier": status,
                "core_rules_passed": dict(agent_out.get("core_rules_passed") or {}),
                "min_word_threshold": int(svc["min_word_threshold"]),
                "min_internal_links": int(svc["min_internal_links"]),
            }
            rank = _status_rank(status)
            if rank > best_rank or (rank == best_rank and wc > best_wc):
                best_candidate = candidate
                best_rank = rank
                best_wc = wc

        if best_candidate is None:
            if saw_umbrella_mention:
                reason = "Service only mentioned within umbrella page."
                prediction_status = "umbrella_only"
            elif saw_any_mention:
                reason = "Service mentioned but no page passed strict qualification."
                prediction_status = "rejected_non_service"
            elif reasons_seen:
                reason = reasons_seen[0]
                prediction_status = "rejected_non_service"
            else:
                reason = "No dedicated page found in crawled HTML."
                prediction_status = "missing"
            best_candidate = {
                "service": slug,
                "display_name": service_display_by_slug.get(slug, slug.replace("_", " ").title()),
                "revenue_weight": int(svc["revenue_weight"]),
                "page_detected": False,
                "page_exists": False,
                "qualified": False,
                "prediction_status": prediction_status,
                "detection_reason": reason,
                "url": None,
                "word_count": 0,
                "keyword_density": 0.0,
                "h1_match": False,
                "depth_score": "missing",
                "schema": {
                    "service_schema": False,
                    "faq_schema": False,
                    "localbusiness_schema": False,
                },
                "structural_signals": {
                    "faq_present": False,
                    "service_h2_sections": 0,
                    "financing_section": False,
                    "before_after_section": False,
                },
                "conversion": {
                    "cta_count": 0,
                    "booking_link": False,
                    "financing_mentioned": False,
                    "faq_section": False,
                    "before_after_section": False,
                    "internal_links": 0,
                },
                "internal_links_to_page": 0,
                "core_rules_passed": {
                    "canonical_valid": False,
                    "homepage_excluded": False,
                    "blog_excluded": False,
                    "slug_alignment": False,
                    "content_depth": False,
                    "structural_depth": False,
                    "keyword_presence": False,
                },
                "serp": {
                    "position_top_3": None,
                    "position_top_10": None,
                    "average_position": None,
                    "map_pack_presence": None,
                    "competitors_in_top_10": None,
                },
                "qualification_status": "missing",
                "optimization_tier": "missing",
                "min_word_threshold": int(svc["min_word_threshold"]),
                "min_internal_links": int(svc["min_internal_links"]),
            }
        service_rows.append(best_candidate)

    out["service_page_detection_debug"] = {
        "pages_crawled": pages_crawled_urls,
        "pages_rejected": pages_rejected,
        "umbrella_detection_triggered": bool(umbrella_triggered),
        "config": {
            "min_word_count": min_word_count,
            "min_keyword_density": min_density,
            "min_h2_sections": min_h2_sections,
            "umbrella_service_threshold": int(SERVICE_PAGE_CONFIG["umbrella_service_threshold"]),
            "min_internal_links": min_internal_links,
            "stub_word_count_max": stub_word_count_max,
            "homepage_single_service_ratio": homepage_single_service_ratio,
        },
    }

    out["missing_high_value_pages"] = [
        str(r["display_name"])
        for r in service_rows
        if str(r.get("qualification_status")) == "missing"
    ]

    # Inbound internal-link wiring support (crawl-based, deterministic)
    try:
        graph = build_internal_link_graph(
            base_url,
            config=InternalLinkCrawlConfig(
                max_pages=max(35, len(pages)),
                max_depth=2,
                timeout_ms=12000,
                mode="fast_html",
            ),
            pre_crawled_pages=[
                {"url": p.get("url"), "html": p.get("html"), "depth": 0}
                for p in pages
                if p.get("url") and p.get("html")
            ],
        )
    except Exception:
        graph = {"pages": {}, "canonical_map": {}, "crawl_meta": {}}

    for row in service_rows:
        page_url = row.get("url")
        if not page_url:
            row["inbound_link_support"] = {
                "inbound": {
                    "unique_pages_all": 0,
                    "unique_pages_body_only": 0,
                    "total_links_all": 0,
                    "contexts": {"nav": 0, "footer": 0, "header": 0, "body": 0, "sidebar": 0, "unknown": 0},
                },
                "orphan": {"is_orphan_all": True, "is_orphan_body_only": True},
                "crawl_meta": dict(graph.get("crawl_meta") or {}),
            }
            row["inbound_unique_pages_all"] = 0
            row["inbound_unique_pages_body_only"] = 0
            row["inbound_total_links_all"] = 0
            row["orphan_status"] = True
            row["wiring_status"] = "orphan"
            continue
        inbound = compute_inbound_internal_link_support(str(page_url), graph)
        inbound_block = inbound.get("inbound") or {}
        row["inbound_link_support"] = inbound
        row["inbound_unique_pages_all"] = int(inbound_block.get("unique_pages_all") or 0)
        row["inbound_unique_pages_body_only"] = int(inbound_block.get("unique_pages_body_only") or 0)
        row["inbound_total_links_all"] = int(inbound_block.get("total_links_all") or 0)
        row["orphan_status"] = bool((inbound.get("orphan") or {}).get("is_orphan_body_only"))
        row["wiring_status"] = wiring_status(row["inbound_unique_pages_body_only"])

    total = len(service_rows)
    present_rows = [r for r in service_rows if bool(r.get("page_detected"))]
    strong_rows = [r for r in service_rows if str(r.get("qualification_status")) == "strong"]
    moderate_rows = [r for r in service_rows if str(r.get("qualification_status")) == "moderate"]
    weak_rows = [r for r in service_rows if str(r.get("qualification_status")) in {"weak", "weak_stub_page"}]
    wc_present = [int(r["word_count"]) for r in present_rows if int(r.get("word_count") or 0) > 0]
    top10_rows = [r for r in service_rows if (r.get("serp") or {}).get("position_top_10") is True]
    coverage_ratio = round((len(present_rows) / total), 3) if total else 0.0
    optimized_ratio = round((len(strong_rows) / total), 3) if total else 0.0
    serp_ratio = round((len(top10_rows) / total), 3) if total else 0.0

    leverage = "low"
    if any(str(r.get("qualification_status")) == "missing" and int(r.get("revenue_weight") or 0) >= 4 for r in service_rows):
        leverage = "high"
    elif any(str(r.get("qualification_status")) in {"weak", "weak_stub_page"} and int(r.get("revenue_weight") or 0) >= 4 for r in service_rows):
        leverage = "moderate"
    elif coverage_ratio >= STRONG_COVERAGE_RATIO_FOR_LOW_LEVERAGE and serp_ratio >= STRONG_SERP_RATIO_FOR_LOW_LEVERAGE:
        leverage = "low"
    elif total > 0:
        leverage = "moderate"

    out["high_value_services"] = service_rows
    out["high_value_summary"] = {
        "total_high_value_services": int(total),
        "services_present": int(len(present_rows)),
        "services_missing": int(total - len(present_rows)),
        "services_strong": int(len(strong_rows)),
        "services_moderate": int(len(moderate_rows)),
        "services_weak": int(len(weak_rows)),
        "service_coverage_ratio": coverage_ratio,
        "optimized_ratio": optimized_ratio,
        "average_word_count": int(sum(wc_present) / len(wc_present)) if wc_present else 0,
        "serp_visibility_ratio": serp_ratio,
    }
    thin_threshold = 300
    thin_pages = [r for r in present_rows if _safe_int(r.get("word_count")) < thin_threshold]
    cta_counts = [_safe_int((r.get("conversion") or {}).get("cta_count")) for r in present_rows]
    internal_links = [_safe_int((r.get("conversion") or {}).get("internal_links")) for r in present_rows]
    inbound_body_links = [_safe_int(r.get("inbound_unique_pages_body_only")) for r in present_rows]
    booking_pages = [r for r in present_rows if bool((r.get("conversion") or {}).get("booking_link"))]
    financing_pages = [r for r in present_rows if bool((r.get("conversion") or {}).get("financing_mentioned"))]
    faq_pages = [
        r for r in present_rows
        if bool((r.get("conversion") or {}).get("faq_section"))
        or bool((r.get("schema") or {}).get("faq_schema"))
    ]
    cta_pages = [r for r in present_rows if _safe_int((r.get("conversion") or {}).get("cta_count")) > 0]
    internal_linked_pages = [r for r in present_rows if _safe_int((r.get("conversion") or {}).get("internal_links")) > 0]
    pages_checked = int(len(pages_crawled_urls))
    schema_pages_detected = int(service_pages_with_schema)
    faq_detected_count = int(len(faq_pages))
    local_pages_detected = int(out.get("city_or_near_me_page_count") or 0)
    conversion_detected_count = int(len(booking_pages) + len(financing_pages) + len(cta_pages))
    out["service_page_analysis_v2"] = {
        "service_coverage": {
            "present": int(len(present_rows)),
            "total": int(total),
            "ratio": coverage_ratio,
            "status": _tri_state(int(len(present_rows)), int(total)),
        },
        "content_depth": {
            "average_words": int(sum(wc_present) / len(wc_present)) if wc_present else 0,
            "min_words": int(min(wc_present)) if wc_present else 0,
            "max_words": int(max(wc_present)) if wc_present else 0,
            "thin_page_threshold_words": thin_threshold,
            "thin_pages": int(len(thin_pages)),
            "thin_page_rate": round((len(thin_pages) / len(present_rows)), 3) if present_rows else 0.0,
            "status": "detected" if present_rows else "unknown",
        },
        "conversion_readiness": {
            "pages_with_booking": int(len(booking_pages)),
            "pages_with_financing": int(len(financing_pages)),
            "pages_with_cta": int(len(cta_pages)),
            "average_cta_count": round((sum(cta_counts) / len(cta_counts)), 2) if cta_counts else 0.0,
            "average_internal_links": round((sum(internal_links) / len(internal_links)), 2) if internal_links else 0.0,
            "status": _tri_state(conversion_detected_count, int(len(present_rows))),
        },
        "local_intent_coverage": {
            "city_or_near_me_pages": int(out.get("city_or_near_me_page_count") or 0),
            "has_multi_location_page": bool(out.get("has_multi_location_page")),
            "examples": list(out.get("geo_page_examples") or [])[:3],
            "status": _tri_state(local_pages_detected, pages_checked),
        },
        "structured_trust_signals": {
            "faq_section_present": bool(len(faq_pages) > 0),
            "internal_linked_service_pages": int(len(internal_linked_pages)),
            "contact_cta_service_pages": int(len(cta_pages)),
            "faq_status": _tri_state(faq_detected_count, int(len(present_rows))),
            "avg_inbound_unique_pages_body_only": round((sum(inbound_body_links) / len(inbound_body_links)), 2) if inbound_body_links else 0.0,
            "pages_with_wiring_support": int(len([x for x in inbound_body_links if x > 0])),
        },
        "schema_bonus": {
            "pages_with_schema": schema_pages_detected,
            "detected": bool(schema_pages_detected > 0),
            "status": _tri_state(schema_pages_detected, pages_checked),
        },
        "crawl_coverage": {
            "pages_checked": pages_checked,
            "service_pages_evaluated": int(len(present_rows)),
            "confidence": _crawl_confidence(pages_checked),
        },
    }
    out["high_value_service_leverage"] = leverage
    return out


def merge_service_serp_validation(
    service_intelligence: Dict[str, Any],
    service_serp_rankings: Optional[Dict[str, Dict[str, Any]]],
) -> Dict[str, Any]:
    """
    Merge deterministic SERP metrics into high_value_services and recompute tiers/summary.
    """
    if not isinstance(service_intelligence, dict):
        return {}
    rows = service_intelligence.get("high_value_services")
    if not isinstance(rows, list) or not rows:
        return service_intelligence

    rankings = service_serp_rankings or {}
    updated_rows: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        slug = str(row.get("service") or "").strip().lower()
        serp = dict(row.get("serp") or {})
        serp_update = rankings.get(slug) or {}
        if isinstance(serp_update, dict):
            serp["position_top_3"] = serp_update.get("position_top_3")
            serp["position_top_10"] = serp_update.get("position_top_10")
            serp["average_position"] = serp_update.get("average_position")
            serp["map_pack_presence"] = serp_update.get("map_pack_presence")
            serp["competitors_in_top_10"] = serp_update.get("competitors_in_top_10")
        row["serp"] = serp
        has_strict_inputs = (
            "keyword_density" in row
            and "h1_match" in row
            and "structural_signals" in row
            and "internal_links_to_page" in row
        )
        if has_strict_inputs:
            min_word_count = int(SERVICE_PAGE_CONFIG["min_word_count"])
            min_density = float(SERVICE_PAGE_CONFIG["min_keyword_density"])
            min_h2_sections = int(SERVICE_PAGE_CONFIG["min_h2_sections"])
            min_internal_links = int(SERVICE_PAGE_CONFIG["min_internal_links"])
            wc = _safe_int(row.get("word_count"))
            kd = float(row.get("keyword_density") or 0.0)
            h1_match = bool(row.get("h1_match"))
            schema_ok = bool((row.get("schema") or {}).get("service_schema") or (row.get("schema") or {}).get("faq_schema"))
            struct = row.get("structural_signals") or {}
            structure_ok = bool(
                bool(struct.get("faq_present"))
                or _safe_int(struct.get("service_h2_sections")) >= min_h2_sections
                or bool(struct.get("financing_section"))
                or bool(struct.get("before_after_section"))
            )
            links_ok = _safe_int(row.get("internal_links_to_page")) >= min_internal_links
            strong_now = bool(
                row.get("page_detected")
                and wc >= min_word_count
                and kd >= min_density
                and h1_match
                and schema_ok
                and structure_ok
                and links_ok
                and row.get("serp", {}).get("position_top_10") is True
            )
            if strong_now:
                row["qualification_status"] = "strong"
            row["optimization_tier"] = row.get("qualification_status") or row.get("optimization_tier") or "missing"
        else:
            row["optimization_tier"] = _classify_optimization_tier(
                page_exists=bool(row.get("page_exists")),
                word_count=_safe_int(row.get("word_count")),
                has_schema=bool((row.get("schema") or {}).get("service_schema") or (row.get("schema") or {}).get("faq_schema")),
                internal_links=_safe_int((row.get("conversion") or {}).get("internal_links")),
                position_top_10=row.get("serp", {}).get("position_top_10"),
                min_word_threshold=_safe_int(row.get("min_word_threshold") or DEPTH_STRONG_MIN_WORDS),
                min_internal_links=_safe_int(row.get("min_internal_links") or INTERNAL_LINKS_MIN_STRONG),
            )
            row["qualification_status"] = row.get("optimization_tier")
        updated_rows.append(row)

    total = len(updated_rows)
    present_rows = [r for r in updated_rows if r.get("page_exists")]
    strong_rows = [r for r in updated_rows if r.get("optimization_tier") == "strong"]
    moderate_rows = [r for r in updated_rows if r.get("optimization_tier") == "moderate"]
    weak_rows = [r for r in updated_rows if r.get("optimization_tier") == "weak"]
    wc_present = [_safe_int(r.get("word_count")) for r in present_rows if _safe_int(r.get("word_count")) > 0]
    top10_rows = [r for r in updated_rows if (r.get("serp") or {}).get("position_top_10") is True]

    coverage_ratio = round((len(present_rows) / total), 3) if total else 0.0
    optimized_ratio = round((len(strong_rows) / total), 3) if total else 0.0
    serp_ratio = round((len(top10_rows) / total), 3) if total else 0.0

    leverage = "low"
    if any((not bool(r.get("page_exists"))) and _safe_int(r.get("revenue_weight")) >= 4 for r in updated_rows):
        leverage = "high"
    elif any(bool(r.get("page_exists")) and r.get("optimization_tier") == "weak" and _safe_int(r.get("revenue_weight")) >= 4 for r in updated_rows):
        leverage = "moderate"
    elif coverage_ratio >= STRONG_COVERAGE_RATIO_FOR_LOW_LEVERAGE and serp_ratio >= STRONG_SERP_RATIO_FOR_LOW_LEVERAGE:
        leverage = "low"
    elif total > 0:
        leverage = "moderate"

    service_intelligence["high_value_services"] = updated_rows
    service_intelligence["high_value_summary"] = {
        "total_high_value_services": int(total),
        "services_present": int(len(present_rows)),
        "services_missing": int(total - len(present_rows)),
        "services_strong": int(len(strong_rows)),
        "services_moderate": int(len(moderate_rows)),
        "services_weak": int(len(weak_rows)),
        "service_coverage_ratio": coverage_ratio,
        "optimized_ratio": optimized_ratio,
        "average_word_count": int(sum(wc_present) / len(wc_present)) if wc_present else 0,
        "serp_visibility_ratio": serp_ratio,
    }
    thin_threshold = 300
    thin_pages = [r for r in present_rows if _safe_int(r.get("word_count")) < thin_threshold]
    cta_counts = [_safe_int((r.get("conversion") or {}).get("cta_count")) for r in present_rows]
    internal_links = [_safe_int((r.get("conversion") or {}).get("internal_links")) for r in present_rows]
    inbound_body_links = [_safe_int(r.get("inbound_unique_pages_body_only")) for r in present_rows]
    booking_pages = [r for r in present_rows if bool((r.get("conversion") or {}).get("booking_link"))]
    financing_pages = [r for r in present_rows if bool((r.get("conversion") or {}).get("financing_mentioned"))]
    faq_pages = [
        r for r in present_rows
        if bool((r.get("conversion") or {}).get("faq_section"))
        or bool((r.get("schema") or {}).get("faq_schema"))
    ]
    cta_pages = [r for r in present_rows if _safe_int((r.get("conversion") or {}).get("cta_count")) > 0]
    internal_linked_pages = [r for r in present_rows if _safe_int((r.get("conversion") or {}).get("internal_links")) > 0]
    pages_checked = int(
        len((service_intelligence.get("service_page_detection_debug") or {}).get("pages_crawled") or [])
        or _safe_int(service_intelligence.get("pages_crawled"))
    )
    schema_pages_detected = int(
        sum(
            1
            for r in present_rows
            if bool((r.get("schema") or {}).get("service_schema"))
            or bool((r.get("schema") or {}).get("faq_schema"))
            or bool((r.get("schema") or {}).get("localbusiness_schema"))
        )
    )
    faq_detected_count = int(len(faq_pages))
    local_pages_detected = int(service_intelligence.get("city_or_near_me_page_count") or 0)
    conversion_detected_count = int(len(booking_pages) + len(financing_pages) + len(cta_pages))
    service_intelligence["service_page_analysis_v2"] = {
        "service_coverage": {
            "present": int(len(present_rows)),
            "total": int(total),
            "ratio": coverage_ratio,
            "status": _tri_state(int(len(present_rows)), int(total)),
        },
        "content_depth": {
            "average_words": int(sum(wc_present) / len(wc_present)) if wc_present else 0,
            "min_words": int(min(wc_present)) if wc_present else 0,
            "max_words": int(max(wc_present)) if wc_present else 0,
            "thin_page_threshold_words": thin_threshold,
            "thin_pages": int(len(thin_pages)),
            "thin_page_rate": round((len(thin_pages) / len(present_rows)), 3) if present_rows else 0.0,
            "status": "detected" if present_rows else "unknown",
        },
        "conversion_readiness": {
            "pages_with_booking": int(len(booking_pages)),
            "pages_with_financing": int(len(financing_pages)),
            "pages_with_cta": int(len(cta_pages)),
            "average_cta_count": round((sum(cta_counts) / len(cta_counts)), 2) if cta_counts else 0.0,
            "average_internal_links": round((sum(internal_links) / len(internal_links)), 2) if internal_links else 0.0,
            "status": _tri_state(conversion_detected_count, int(len(present_rows))),
        },
        "local_intent_coverage": {
            "city_or_near_me_pages": int(service_intelligence.get("city_or_near_me_page_count") or 0),
            "has_multi_location_page": bool(service_intelligence.get("has_multi_location_page")),
            "examples": list(service_intelligence.get("geo_page_examples") or [])[:3],
            "status": _tri_state(local_pages_detected, pages_checked),
        },
        "structured_trust_signals": {
            "faq_section_present": bool(len(faq_pages) > 0),
            "internal_linked_service_pages": int(len(internal_linked_pages)),
            "contact_cta_service_pages": int(len(cta_pages)),
            "faq_status": _tri_state(faq_detected_count, int(len(present_rows))),
            "avg_inbound_unique_pages_body_only": round((sum(inbound_body_links) / len(inbound_body_links)), 2) if inbound_body_links else 0.0,
            "pages_with_wiring_support": int(len([x for x in inbound_body_links if x > 0])),
        },
        "schema_bonus": {
            "pages_with_schema": schema_pages_detected,
            "detected": bool(schema_pages_detected > 0),
            "status": _tri_state(schema_pages_detected, pages_checked),
        },
        "crawl_coverage": {
            "pages_checked": pages_checked,
            "service_pages_evaluated": int(len(present_rows)),
            "confidence": _crawl_confidence(pages_checked),
        },
    }
    service_intelligence["high_value_service_leverage"] = leverage
    return service_intelligence


def run_strict_single_service_page_check(
    website_url: Optional[str],
    service_slug: str,
    *,
    city: Optional[str] = None,
    state: Optional[str] = None,
    vertical: str = "dentist",
) -> Dict[str, Any]:
    """
    Lightweight strict check wrapper used by Ask/Territory missing_service_page filter.
    Uses the same deterministic detector as full diagnostics.
    """
    slug = str(service_slug or "").strip().lower().replace(" ", "_")
    if not website_url or not slug:
        return {
            "service": slug,
            "matches": False,
            "reason": "missing website or service",
            "page_detected": False,
            "qualification_status": "missing",
            "detection_reason": "Missing website or service.",
            "debug": {},
        }
    intel = build_service_intelligence(
        website_url=website_url,
        website_html=None,
        procedure_mentions_from_reviews=None,
        city=city,
        state=state,
        vertical=vertical,
    )
    rows = intel.get("high_value_services") if isinstance(intel.get("high_value_services"), list) else []
    row = next((r for r in rows if str(r.get("service") or "").strip().lower() == slug), None)
    if not row:
        return {
            "service": slug,
            "matches": False,
            "reason": "service slug not in configured high-value catalog",
            "page_detected": False,
            "qualification_status": "missing",
            "detection_reason": "Service slug not configured.",
            "debug": intel.get("service_page_detection_debug") or {},
        }
    status = str(row.get("qualification_status") or "missing")
    prediction_status = str(row.get("prediction_status") or status)
    # For missing_service_page criterion, match means service page is effectively missing.
    is_missing = status in {"missing", "weak_stub_page"}
    return {
        "service": slug,
        "matches": bool(is_missing),
        "reason": str(row.get("detection_reason") or ""),
        "page_detected": bool(row.get("page_detected")),
        "qualification_status": status,
        "prediction_status": prediction_status,
        "detection_reason": str(row.get("detection_reason") or ""),
        "url": row.get("url"),
        "word_count": row.get("word_count"),
        "keyword_density": row.get("keyword_density"),
        "h1_match": row.get("h1_match"),
        "structural_signals": row.get("structural_signals"),
        "internal_links_to_page": row.get("internal_links_to_page"),
        "debug": intel.get("service_page_detection_debug") or {},
    }


# ---------------------------------------------------------------------------
# LLM page text helper (unchanged interface)
# ---------------------------------------------------------------------------

def get_page_texts_for_llm(
    website_url: Optional[str],
    website_html: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """
    Return homepage_text, services_page_text, pricing_page_text for LLM structured extraction.
    """
    out = {"homepage_text": None, "services_page_text": None, "pricing_page_text": None}
    if not (website_url or "").strip():
        return out
    base_url = website_url if website_url.startswith(("http://", "https://")) else "https://" + website_url
    html = website_html or _fetch_html(base_url)
    if not html:
        return out
    out["homepage_text"] = _strip_html(html)
    links = _extract_links(html, base_url)
    service_like = [u for u in links if _is_service_like_path(u)]
    pricing_like = [u for u in links if _is_pricing_like_path(u)]
    for url in service_like[:1]:
        if url == base_url:
            continue
        h = _fetch_html(url)
        if h:
            out["services_page_text"] = _strip_html(h)
            break
    for url in pricing_like[:1]:
        if url == base_url:
            continue
        h = _fetch_html(url)
        if h:
            out["pricing_page_text"] = _strip_html(h)
            break
    return out
