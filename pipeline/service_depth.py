"""
Service depth detection for dental leads.

Crawls homepage, sitemap, nav links (two levels deep), detects high-ticket
vs general service pages, flags truly missing high-value pages.
Used for revenue leverage and intervention quality.
"""

import re
import logging
import json
import html as html_lib
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
from pipeline.practice_classifier import classify_practice_type
from pipeline.service_taxonomy import get_expected_services
from pipeline.crawl_manager import CrawlManager
from pipeline.service_page_qualification_agent_v2 import qualify_service_page_candidate_v2

logger = logging.getLogger(__name__)

_DEFAULT_REVENUE_WEIGHTS: Dict[str, int] = {
    "implants": 5,
    "invisalign": 4,
    "orthodontics": 4,
    "veneers": 4,
    "cosmetic_dentistry": 3,
    "all_on_4": 5,
    "full_mouth_reconstruction": 5,
    "emergency_dentistry": 3,
    "crowns": 3,
    "root_canal": 3,
    "pediatric_dentistry": 2,
    "teeth_whitening": 2,
    "braces": 4,
    "clear_aligners": 4,
    "retainers": 3,
    "early_orthodontic_treatment": 3,
    "surgical_orthodontics": 4,
    "dental_implants": 5,
    "wisdom_teeth_removal": 4,
    "bone_grafting": 4,
    "full_arch_implants": 5,
    "sedation_dentistry": 3,
    "children_dentistry": 2,
    "fluoride_treatment": 2,
    "sealants": 2,
    "space_maintainers": 2,
    "pediatric_cleanings": 2,
    "gum_disease_treatment": 4,
    "scaling_and_root_planing": 3,
    "laser_gum_therapy": 3,
}

_SERVICE_ALIASES_BY_NAME: Dict[str, List[str]] = {
    "implants": ["implants", "dental implants", "implant"],
    "invisalign": ["invisalign", "clear aligners", "aligners"],
    "orthodontics": ["orthodontics", "orthodontic", "braces"],
    "veneers": ["veneers", "veneer"],
    "cosmetic dentistry": ["cosmetic dentistry", "cosmetic", "smile makeover"],
    "all-on-4": ["all-on-4", "all on 4", "all on four"],
    "full mouth reconstruction": ["full mouth reconstruction", "full-mouth reconstruction", "full mouth rehab"],
    "emergency dentistry": ["emergency dentistry", "emergency dentist", "urgent dental"],
    "crowns": ["crowns", "crown", "dental crowns"],
    "root canal": ["root canal", "root canals", "endodontics"],
    "pediatric dentistry": ["pediatric dentistry", "kids dentist", "children dentist"],
    "teeth whitening": ["teeth whitening", "whitening"],
    "braces": ["braces", "orthodontic braces"],
    "clear aligners": ["clear aligners", "clear aligner", "aligners"],
    "retainers": ["retainers", "retainer"],
    "early orthodontic treatment": ["early orthodontic treatment", "phase 1 orthodontics", "interceptive orthodontics"],
    "surgical orthodontics": ["surgical orthodontics", "orthognathic surgery"],
    "dental implants": ["dental implants", "implants", "implant"],
    "wisdom teeth removal": ["wisdom teeth removal", "wisdom teeth extraction", "third molar extraction"],
    "bone grafting": ["bone grafting", "bone graft"],
    "full arch implants": ["full arch implants", "full-arch implants", "all-on-x"],
    "sedation dentistry": ["sedation dentistry", "iv sedation", "oral sedation"],
    "children dentistry": ["children dentistry", "kids dentistry", "pediatric dentistry"],
    "fluoride treatment": ["fluoride treatment", "fluoride"],
    "sealants": ["sealants", "dental sealants"],
    "space maintainers": ["space maintainers", "space maintainer"],
    "pediatric cleanings": ["pediatric cleanings", "kids cleaning", "children cleaning"],
    "gum disease treatment": ["gum disease treatment", "periodontal treatment", "periodontics"],
    "scaling and root planing": ["scaling and root planing", "deep cleaning"],
    "laser gum therapy": ["laser gum therapy", "laser periodontal therapy"],
}

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


class ServiceStatus:
    DEDICATED_PAGE = "dedicated"
    STRONG_UMBRELLA = "mention_only"
    WEAK_PRESENCE = "mention_only"
    MISSING = "missing"
    NOT_EVALUATED = "unknown"

    CANONICAL = {"dedicated", "mention_only", "missing", "unknown"}


EXCLUDED_PATTERNS = [
    "/feed",
    "/category/",
    "/tag/",
    "/author/",
    "/blog/",
    "/blog",
    "/article/",
    "/news/",
    "/archive/",
    "/insights/",
    "wp-json",
    "?",
    ".xml",
]

VERTICAL_EXPECTED_PAGES: Dict[str, List[Dict[str, str]]] = {
    "dental": [
        {"slug": "invisalign", "title": "Invisalign", "priority": "high"},
        {"slug": "implants", "title": "Dental Implants", "priority": "high"},
        {"slug": "all-on-4", "title": "All-on-4", "priority": "high"},
        {"slug": "veneers", "title": "Veneers", "priority": "medium"},
        {"slug": "whitening", "title": "Teeth Whitening", "priority": "medium"},
        {"slug": "emergency", "title": "Emergency Dental", "priority": "high"},
        {"slug": "pediatric", "title": "Pediatric Dentistry", "priority": "medium"},
        {"slug": "orthodontics", "title": "Orthodontics", "priority": "medium"},
        {"slug": "cleanings", "title": "Teeth Cleanings", "priority": "low"},
        {"slug": "crowns", "title": "Crowns & Bridges", "priority": "low"},
    ]
}

CTA_PATTERN_BY_TYPE: Dict[str, List[str]] = {
    "Book": [
        r"\bbook\b",
        r"\bbook now\b",
        r"\bbook appointment\b",
    ],
    "Schedule": [
        r"\bschedule\b",
        r"\brequest appointment\b",
        r"\bappointment\b",
    ],
    "Contact": [
        r"\bcontact\b",
        r"\bget started\b",
        r"\bsubmit\b",
    ],
    "Call": [
        r"\bcall\b",
        r"\bcall now\b",
        r"tel:",
    ],
}

CTA_HREF_HINTS_BY_TYPE: Dict[str, List[str]] = {
    "Book": ["book", "book-now", "booknow"],
    "Schedule": ["schedule", "appointment", "request-appointment"],
    "Contact": ["contact", "get-started", "contact-us"],
    "Call": ["tel:", "call", "phone"],
}

SERVICE_STATUS_WEIGHTS: Dict[str, float] = {
    "dedicated": 1.0,
    "mention_only": 0.0,
    "missing": 0.0,
    "unknown": 0.0,
}


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


def _strip_nav_header_footer(html: str) -> str:
    out = html or ""
    out = re.sub(r"<nav[\s>][\s\S]*?</nav>", " ", out, flags=re.I)
    out = re.sub(r"<header[\s>][\s\S]*?</header>", " ", out, flags=re.I)
    out = re.sub(r"<footer[\s>][\s\S]*?</footer>", " ", out, flags=re.I)
    return out


def _extract_conversion_metrics(html: str, page_text: str, base_url: str) -> Dict[str, Any]:
    main_html = _strip_nav_header_footer(html)
    lower = main_html.lower()
    cta_type_counts: Dict[str, int] = {}
    cta_count = 0
    for cta_type, patterns in CTA_PATTERN_BY_TYPE.items():
        type_hits = 0
        for pat in patterns:
            type_hits += len(re.findall(pat, lower))
        cta_type_counts[cta_type] = int(type_hits)
        cta_count += int(type_hits)
    booking_link = bool(re.search(r'href\s*=\s*["\'][^"\']*(book|schedule|appointment|calendly|zocdoc)[^"\']*["\']', lower))
    financing_mentioned = bool(re.search(r"\b(financing|payment plan|monthly payment|carecredit|affirm)\b", page_text))
    faq_section = bool(re.search(r"\bfaq\b|frequently asked questions", lower))
    before_after = bool(re.search(r"before\s*(and|&)\s*after|smile gallery", lower))
    internal_links = len(_extract_internal_links(html, base_url))
    clickable_cta_metrics = _extract_clickable_cta_metrics(main_html)
    return {
        "cta_count": int(cta_count),
        "booking_link": bool(booking_link),
        "financing_mentioned": bool(financing_mentioned),
        "faq_section": bool(faq_section),
        "before_after_section": bool(before_after),
        "internal_links": int(internal_links),
        "cta_type_counts": cta_type_counts,
        "clickable_cta_by_type": dict(clickable_cta_metrics.get("clickable_cta_by_type") or {}),
        "clickable_cta_count": int(clickable_cta_metrics.get("clickable_cta_count") or 0),
    }


def _extract_attr_value(tag_html: str, attr_name: str) -> str:
    m = re.search(
        rf'{re.escape(attr_name)}\s*=\s*["\']([^"\']*)["\']',
        tag_html or "",
        re.I,
    )
    return html_lib.unescape(str(m.group(1) or "")).strip() if m else ""


def _strip_element_text(raw_html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw_html or "", flags=re.I)
    text = html_lib.unescape(text)
    return re.sub(r"\s+", " ", text).strip().lower()


def _classify_clickable_cta_type(text: str, href: str) -> Optional[str]:
    normalized_text = str(text or "").lower()
    normalized_href = str(href or "").strip().lower()
    if normalized_href.startswith("tel:"):
        return "Call"
    for cta_type in ("Book", "Schedule", "Contact", "Call"):
        for pat in CTA_PATTERN_BY_TYPE.get(cta_type, []):
            if re.search(pat, normalized_text, re.I):
                return cta_type
        for hint in CTA_HREF_HINTS_BY_TYPE.get(cta_type, []):
            if hint and hint in normalized_href:
                return cta_type
    return None


def _extract_clickable_cta_metrics(html: str) -> Dict[str, Any]:
    counts: Dict[str, int] = {cta_type: 0 for cta_type in CTA_PATTERN_BY_TYPE.keys()}

    # Counts each matched clickable element occurrence (no cross-page dedupe).
    for m in re.finditer(r"<a\b([^>]*)>([\s\S]*?)</a>", html or "", re.I):
        attrs = str(m.group(1) or "")
        href = _extract_attr_value(attrs, "href")
        text = _strip_element_text(m.group(2) or "")
        cta_type = _classify_clickable_cta_type(text, href)
        if cta_type:
            counts[cta_type] += 1

    for m in re.finditer(r"<button\b([^>]*)>([\s\S]*?)</button>", html or "", re.I):
        attrs = str(m.group(1) or "")
        text = _strip_element_text(m.group(2) or "")
        href = _extract_attr_value(attrs, "href")
        cta_type = _classify_clickable_cta_type(text, href)
        if cta_type:
            counts[cta_type] += 1

    for m in re.finditer(
        r"<([a-z0-9]+)\b([^>]*)role\s*=\s*[\"']button[\"'][^>]*>([\s\S]*?)</\1>",
        html or "",
        re.I,
    ):
        tag_name = str(m.group(1) or "").lower()
        if tag_name == "button":
            continue
        attrs = str(m.group(2) or "")
        href = _extract_attr_value(attrs, "href")
        text = _strip_element_text(m.group(3) or "")
        cta_type = _classify_clickable_cta_type(text, href)
        if cta_type:
            counts[cta_type] += 1

    for m in re.finditer(r"<input\b([^>]*)>", html or "", re.I):
        attrs = str(m.group(1) or "")
        input_type = _extract_attr_value(attrs, "type").lower()
        if input_type not in {"submit", "button"}:
            continue
        value_text = _extract_attr_value(attrs, "value")
        cta_type = _classify_clickable_cta_type(value_text, "")
        if cta_type:
            counts[cta_type] += 1

    return {
        "clickable_cta_by_type": counts,
        "clickable_cta_count": int(sum(counts.values())),
    }


def _sitewide_contact_form_detected(pages: List[Dict[str, Any]]) -> bool:
    tokens = (
        "<form",
        "gravityforms",
        "wpforms",
        "jotform",
        "calendly",
        "smilesnap",
        "orthofi",
        "appointment",
    )
    for page in pages:
        html = str(page.get("html") or "").lower()
        if not html:
            continue
        if any(tok in html for tok in tokens):
            return True
    return False


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


def _extract_meta_text(html: str) -> str:
    parts: List[str] = []
    for m in re.finditer(
        r"<meta[^>]+name\s*=\s*[\"'](?:description|keywords)[\"'][^>]+content\s*=\s*[\"']([^\"']+)[\"'][^>]*>",
        html or "",
        re.I,
    ):
        parts.append(str(m.group(1) or ""))
    return " ".join(parts).lower()


def _has_geo_schema_signal(html: str) -> bool:
    lower = (html or "").lower()
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
        nodes: List[Dict[str, Any]] = []
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
            is_local = any(t in ("localbusiness", "dentist", "medicalbusiness") for t in type_vals)
            if not is_local:
                continue
            if node.get("areaServed") is not None or node.get("areaserved") is not None or node.get("geo") is not None:
                return True
    return (
        any(x in lower for x in ("schema.org/localbusiness", "schema.org/dentist", "schema.org/medicalbusiness"))
        and any(x in lower for x in ("areaserved", "\"geo\"", " itemprop=\"geo\""))
    )


def _geo_signals_for_page(
    *,
    url: str,
    html: str,
    headings: str,
    page_text: str,
    city: Optional[str],
) -> List[str]:
    signals: List[str] = []
    lower_url = str(url or "").lower()
    lower_headings = str(headings or "").lower()
    lower_body = str(page_text or "").lower()
    lower_meta = _extract_meta_text(html)
    city_name = str(city or "").strip().lower()

    h1_text = _extract_h1_text(html)
    title_text = _extract_title(html)

    if city_name:
        if (
            city_name in lower_url
            or city_name in title_text
            or city_name in h1_text
        ):
            signals.append("city")

    near_phrases = ["near me", "near you"]
    near_match = any(p in lower_url or p in title_text or p in h1_text or p in lower_body for p in near_phrases)
    if city_name and any(f"{prefix} {city_name}" in (lower_url + " " + title_text + " " + h1_text + " " + lower_body) for prefix in ("in", "serving")):
        near_match = True
    if near_match:
        signals.append("near-me")

    if _has_geo_schema_signal(html):
        signals.append("schema")

    if city_name and (city_name in lower_meta):
        signals.append("meta")
    elif any(p in lower_meta for p in near_phrases):
        signals.append("meta")

    return signals


_EXPECTED_PAGE_ALIASES: Dict[str, List[str]] = {
    "invisalign": ["invisalign", "clear aligner", "clear aligners", "aligner", "aligners"],
    "implants": ["implant", "implants", "dental implant", "dental implants"],
    "all-on-4": ["all-on-4", "all on 4", "all-on-four", "all on four", "full arch implant", "full-arch implant"],
    "veneers": ["veneer", "veneers", "porcelain veneer", "porcelain veneers"],
    "whitening": ["whitening", "teeth whitening", "tooth whitening"],
    "emergency": ["emergency", "emergency dental", "emergency dentist", "urgent dental"],
    "pediatric": ["pediatric", "pediatric dentistry", "children dentistry", "kids dentist", "kids dentistry"],
    "orthodontics": ["orthodontic", "orthodontics", "braces", "clear aligner", "clear aligners", "invisalign"],
    "cleanings": ["cleaning", "cleanings", "teeth cleaning", "dental cleaning", "prophylaxis"],
    "crowns": ["crown", "crowns", "dental crown", "dental crowns", "bridge", "bridges"],
}


def _alias_tokens(alias: str) -> List[str]:
    return [t for t in re.findall(r"[a-z0-9]+", str(alias or "").lower()) if t]


def _path_matches_alias(path_or_url: str, aliases: List[str]) -> bool:
    path = urlparse(path_or_url).path.lower()
    path_tokens = set(re.findall(r"[a-z0-9]+", path.replace("_", "-")))
    for alias in aliases:
        tokens = _alias_tokens(alias)
        if not tokens:
            continue
        if len(tokens) == 1:
            if tokens[0] in path_tokens:
                return True
            continue
        if all(tok in path_tokens for tok in tokens):
            return True
    return False


def _expected_aliases_for_slug(slug: str, title: str) -> List[str]:
    base = list(_EXPECTED_PAGE_ALIASES.get(slug) or [])
    if title:
        base.append(title.lower())
    slug_words = slug.replace("-", " ").replace("_", " ").strip().lower()
    if slug_words:
        base.append(slug_words)
    # Stable order + dedupe
    seen: Set[str] = set()
    out: List[str] = []
    for a in base:
        cleaned = re.sub(r"\s+", " ", str(a or "").strip().lower())
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            out.append(cleaned)
    return out


def _text_contains_alias(text: str, aliases: List[str]) -> bool:
    lower = str(text or "").lower()
    for alias in aliases:
        if not alias:
            continue
        if re.search(rf"\b{re.escape(alias)}\b", lower):
            return True
    return False


def _detect_missing_service_pages(
    crawled_urls: List[str],
    vertical: str,
    pages: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, str]]:
    vertical_key = str(vertical or "").strip().lower()
    if vertical_key == "dentist":
        vertical_key = "dental"
    expected = VERTICAL_EXPECTED_PAGES.get(vertical_key) or []
    lower_urls = [str(u or "").strip().lower() for u in (crawled_urls or []) if str(u or "").strip()]
    page_rows = list(pages or [])
    out: List[Dict[str, str]] = []
    for page in expected:
        slug = str(page.get("slug") or "").strip().lower()
        if not slug:
            continue
        title = str(page.get("title") or slug)
        aliases = _expected_aliases_for_slug(slug, title)

        found = any(_path_matches_alias(u, aliases) for u in lower_urls)
        if not found:
            for p in page_rows:
                p_url = str(p.get("url") or "")
                p_h1 = str(p.get("h1") or "")
                p_headings = str(p.get("headings") or "")
                p_text = str(p.get("text") or "")
                p_wc = int(p.get("word_count") or 0)
                # Accept URL/path match OR clear heading/title evidence.
                if _path_matches_alias(p_url, aliases):
                    found = True
                    break
                if _text_contains_alias(p_h1, aliases) or _text_contains_alias(p_headings, aliases):
                    found = True
                    break
                # Fallback for generic treatment pages: require substantial content mention.
                if p_wc >= SUBSTANTIAL_PAGE_LENGTH and _text_contains_alias(p_text, aliases):
                    found = True
                    break
        if found:
            continue
        out.append(
            {
                "slug": slug,
                "title": title,
                "priority": str(page.get("priority") or "low"),
                "reason": (
                    f"No crawled URL or strong on-page signal matched service '{slug}' "
                    "— page may not exist, be weakly represented, or be inaccessible in crawl."
                ),
            }
        )
    return out


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


def _slugify_service_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(name or "").strip().lower()).strip("_")
    return slug or "service"


def _taxonomy_service_catalog(expected_services: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for display_name in expected_services or []:
        display = str(display_name or "").strip()
        if not display:
            continue
        slug = _slugify_service_name(display)
        aliases = list(_SERVICE_ALIASES_BY_NAME.get(display.lower()) or [display.lower(), display.lower().replace("-", " ")])
        if slug.replace("_", " ") not in aliases:
            aliases.append(slug.replace("_", " "))
        out.append(
            {
                "slug": slug,
                "display_name": display,
                "revenue_weight": int(_DEFAULT_REVENUE_WEIGHTS.get(slug, 3)),
                "min_word_threshold": int(DEPTH_STRONG_MIN_WORDS),
                "min_internal_links": int(INTERNAL_LINKS_MIN_STRONG),
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


def _tiered_service_status(
    *,
    url_match: bool,
    h1_match: bool,
    keyword_count: int,
    word_count: int,
    cta_count: int,
) -> str:
    # Dedicated requires strong semantic/URL alignment and substantial content.
    if word_count >= SUBSTANTIAL_PAGE_LENGTH and (url_match or h1_match):
        return ServiceStatus.DEDICATED_PAGE
    # Any keyword/nav/call-to-action presence is mention-only, never dedicated.
    if keyword_count > 0 or cta_count > 0 or h1_match or url_match:
        return ServiceStatus.STRONG_UMBRELLA
    return ServiceStatus.MISSING


def _canonical_service_status(raw_status: str) -> str:
    s = str(raw_status or "").strip().lower()
    if s in {ServiceStatus.DEDICATED_PAGE, "dedicated_page", "strong"}:
        return ServiceStatus.DEDICATED_PAGE
    if s in {
        ServiceStatus.STRONG_UMBRELLA,
        ServiceStatus.WEAK_PRESENCE,
        "strong_umbrella",
        "weak_presence",
        "weak",
        "weak_stub_page",
        "umbrella_only",
        "content_match",
        "evidence_match",
        "moderate",
    }:
        return ServiceStatus.STRONG_UMBRELLA
    if s in {ServiceStatus.NOT_EVALUATED, "not_evaluated"}:
        return ServiceStatus.NOT_EVALUATED
    return ServiceStatus.MISSING


def _qualification_from_service_status(service_status: str) -> str:
    return _canonical_service_status(service_status)


def _is_excluded_candidate_url(url_or_path: str) -> bool:
    lower = str(url_or_path or "").lower()
    return any(p in lower for p in EXCLUDED_PATTERNS)


def _compute_service_confidence(
    *,
    matched_url: bool,
    title_match: bool,
    h1_match: bool,
    keyword_frequency: int,
    word_count: int,
) -> float:
    score = 0.0
    if matched_url:
        score += 0.3
    if title_match:
        score += 0.2
    if h1_match:
        score += 0.2
    if keyword_frequency >= 5:
        score += 0.2
    if word_count > 400:
        score += 0.1
    return round(min(1.0, score), 2)


def _confidence_level(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def classify_page_strength(word_count: int, confidence_score: float) -> str:
    if float(confidence_score or 0.0) < 0.3:
        return "Not Evaluated"
    if int(word_count or 0) < 300:
        return "Thin"
    if int(word_count or 0) < 800:
        return "Moderate"
    return "Strong"


def _weighted_coverage_score(rows: List[Dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    total = len(rows)
    weighted = 0.0
    for r in rows:
        status = str(r.get("service_status") or "").strip().lower()
        weighted += float(SERVICE_STATUS_WEIGHTS.get(status, 0.0))
    return round(weighted / float(total), 3)


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
    ServiceStatus.DEDICATED_PAGE: 4,
    ServiceStatus.STRONG_UMBRELLA: 3,
    "missing": 0,
    ServiceStatus.NOT_EVALUATED: 1,
}


def _status_rank(status: str) -> int:
    return int(_STATUS_RANK.get(str(status or "").strip().lower(), 0))


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
    place_data: Optional[Dict[str, Any]] = None,
    use_playwright: bool = False,
    playwright_mode: str = "full",
) -> Dict[str, Any]:
    """
    Build service_intelligence block with canonical bucket resolution.

    Crawls homepage → sitemap → nav links (two levels) to build a comprehensive
    page inventory. Detects which high-ticket services have dedicated pages vs.
    are merely mentioned, and flags truly missing opportunities.
    """
    out: Dict[str, Any] = {
        "practice_type": "general_dentist",
        "practice_classification_confidence": 0.3,
        "practice_classification_signals": {},
        "expected_services": [],
        "expected_service_count": 0,
        "crawl_confidence": "low",
        "js_detected": False,
        "suppress_service_gap": True,
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
        "crawl_method": "requests",
        "deep_scan": False,
        "cta_elements": [],
        "cta_clickable_by_type": {},
        "cta_clickable_count": 0,
        "geo_intent_pages": [],
        "missing_geo_pages": [],
    }
    if not (website_url or "").strip():
        return out

    base_url = website_url if website_url.startswith(("http://", "https://")) else "https://" + website_url
    fetch_fn = _fetch_html
    homepage_html = website_html
    playwright_fetcher = None
    playwright_fetch_summary: Optional[Dict[str, int]] = None
    playwright_mode_norm = str(playwright_mode or "full").strip().lower()
    if playwright_mode_norm not in {"full", "landing_only"}:
        playwright_mode_norm = "full"
    if use_playwright:
        try:
            from pipeline.playwright_fetch import PlaywrightFetcher

            playwright_fetcher = PlaywrightFetcher()
            homepage_html = None
            out["crawl_method"] = "playwright" if playwright_mode_norm == "full" else "hybrid_playwright_landing_only"
            out["deep_scan"] = True

            def _playwright_with_fallback(url: str) -> Optional[str]:
                rendered = playwright_fetcher.fetch(url)
                if rendered:
                    return rendered
                logger.warning(
                    "Playwright returned no HTML for %s; falling back to requests.",
                    str(url)[:120],
                )
                return _fetch_html(url)

            if playwright_mode_norm == "landing_only":
                js_page_cap = 8
                js_page_count = 0
                js_fallback_count = 0
                requests_page_count = 0
                playwright_fetch_summary = {
                    "playwright_pages": 0,
                    "requests_pages": 0,
                    "playwright_fallback_to_requests": 0,
                }

                def _is_landing_candidate_url(url: str) -> bool:
                    norm = str(url or "").strip()
                    if not norm:
                        return False
                    if not norm.startswith(("http://", "https://")):
                        norm = "https://" + norm
                    parsed = urlparse(norm)
                    path = (parsed.path or "/").lower()
                    # Always render homepage via JS path in deep scan mode.
                    if path in {"", "/"}:
                        return True
                    if _is_service_like_path(norm) or _is_pricing_like_path(norm):
                        return True
                    landing_tokens = (
                        "service", "services", "treatment", "treatments", "procedure", "procedures",
                        "implant", "invisalign", "veneer", "cosmetic", "emergency", "crown",
                        "orthodont", "braces", "aligner", "sedation", "whitening", "sleep-apnea",
                    )
                    return any(tok in path for tok in landing_tokens)

                def _hybrid_fetch(url: str) -> Optional[str]:
                    nonlocal js_page_count, requests_page_count, js_fallback_count
                    if _is_landing_candidate_url(url) and js_page_count < js_page_cap:
                        js_page_count += 1
                        rendered = playwright_fetcher.fetch(url)
                        if rendered:
                            if playwright_fetch_summary is not None:
                                playwright_fetch_summary["playwright_pages"] = js_page_count
                            return rendered
                        js_fallback_count += 1
                        logger.warning(
                            "Playwright landing-page fetch failed for %s; using requests fallback.",
                            str(url)[:120],
                        )
                    requests_page_count += 1
                    if playwright_fetch_summary is not None:
                        playwright_fetch_summary["playwright_pages"] = js_page_count
                        playwright_fetch_summary["requests_pages"] = requests_page_count
                        playwright_fetch_summary["playwright_fallback_to_requests"] = js_fallback_count
                    return _fetch_html(url)

                fetch_fn = _hybrid_fetch
            else:
                fetch_fn = _playwright_with_fallback
        except Exception as exc:
            logger.warning(
                "Playwright unavailable for %s, falling back to requests crawl: %s",
                base_url[:120],
                exc,
            )
            out["crawl_method"] = "requests_fallback_playwright_unavailable"
            out["crawl_warning"] = "Playwright unavailable; used requests fallback."

    html = homepage_html or fetch_fn(base_url)
    if not html:
        if playwright_fetcher is not None:
            try:
                playwright_fetcher.close()
            except Exception as exc:
                logger.warning("Failed to close Playwright fetcher for %s: %s", base_url[:120], exc)
        return out

    website_content = _strip_html(html)
    practice_type = "general_dentist"
    expected_services: List[str] = []
    if (vertical or "").strip().lower() == "dentist":
        classification = classify_practice_type(place_data or {}, website_content)
        if isinstance(classification, dict):
            practice_type = str(classification.get("practice_type") or "general_dentist")
            out["practice_classification_confidence"] = float(classification.get("confidence") or 0.3)
            out["practice_classification_signals"] = dict(classification.get("signals") or {})
        else:
            # Backward-compat safety if any legacy caller/mocked test returns string.
            practice_type = str(classification or "general_dentist")
        expected_services = get_expected_services(practice_type)
    out["practice_type"] = practice_type
    out["expected_services"] = list(expected_services)
    out["expected_service_count"] = int(len(expected_services))

    crawl_manager = CrawlManager(base_url, fetch_fn=fetch_fn)
    try:
        crawl_result = crawl_manager.crawl(practice_type=practice_type, homepage_html=homepage_html)
    finally:
        if playwright_fetcher is not None:
            try:
                playwright_fetcher.close()
            except Exception as exc:
                logger.warning("Failed to close Playwright fetcher for %s: %s", base_url[:120], exc)
    sitemap_urls = list(crawl_result.get("sitemap_urls") or [])
    out["crawl_confidence"] = str(crawl_result.get("confidence") or "low").lower()
    out["js_detected"] = bool(crawl_result.get("js_detected"))
    out["suppress_service_gap"] = out["crawl_confidence"] == "low"
    if playwright_fetch_summary is not None:
        out["playwright_fetch_summary"] = dict(playwright_fetch_summary)

    pages: List[Dict[str, Any]] = []
    geo_tokens = _city_tokens(city, state)
    crawled_pages = crawl_result.get("pages") or {}
    if isinstance(crawled_pages, dict):
        for url, payload in crawled_pages.items():
            h = str((payload or {}).get("html") or "")
            if not h:
                continue
            page_text = _extract_primary_content_text(h)
            headings = _extract_headings(h)
            schema_flags = _extract_schema_flags(h)
            pages.append(
                {
                    "url": str(url),
                    "html": h,
                    "text": page_text,
                    "h1": _extract_h1_text(h),
                    "headings": headings,
                    "path_slugs": _path_slugs(str(url)),
                    "schema": schema_flags,
                    "has_faq_or_schema": bool(
                        schema_flags["has_service_schema"] or schema_flags["has_faq_schema"] or schema_flags["has_localbusiness_schema"]
                    ),
                    "word_count": _word_count(page_text),
                    "is_geo": _is_geo_or_location_page(str(url), headings, geo_tokens),
                    "conversion": _extract_conversion_metrics(h, page_text, base_url),
                }
            )
    if not pages:
        # Fallback: at least evaluate homepage when crawl discovery is sparse.
        homepage_text = _extract_primary_content_text(html)
        homepage_headings = _extract_headings(html)
        homepage_schema = _extract_schema_flags(html)
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
                "conversion": _extract_conversion_metrics(html, homepage_text, base_url),
            }
        )

    out["pages_crawled"] = int(len(pages))

    # CTA provenance and geo-intent evidence are computed from the same page set used for service evaluation.
    cta_pages_by_type: Dict[str, List[str]] = {k: [] for k in CTA_PATTERN_BY_TYPE.keys()}
    cta_seen_pages_by_type: Dict[str, Set[str]] = {k: set() for k in CTA_PATTERN_BY_TYPE.keys()}
    cta_hit_count_by_type: Dict[str, int] = {k: 0 for k in CTA_PATTERN_BY_TYPE.keys()}
    cta_clickable_count_by_type: Dict[str, int] = {k: 0 for k in CTA_PATTERN_BY_TYPE.keys()}
    geo_intent_pages: List[Dict[str, Any]] = []
    for page in pages:
        page_url = str(page.get("url") or "")
        html_raw = str(page.get("html") or "")
        page_text = str(page.get("text") or "")
        headings = str(page.get("headings") or "")
        conversion = dict(page.get("conversion") or {})
        cta_type_counts = dict(conversion.get("cta_type_counts") or {})
        clickable_cta_by_type = dict(conversion.get("clickable_cta_by_type") or {})
        has_cta = False
        for cta_type in CTA_PATTERN_BY_TYPE.keys():
            hits = _safe_int(cta_type_counts.get(cta_type))
            cta_hit_count_by_type[cta_type] += hits
            cta_clickable_count_by_type[cta_type] += _safe_int(clickable_cta_by_type.get(cta_type))
            if hits > 0:
                has_cta = True
                if page_url and page_url not in cta_seen_pages_by_type[cta_type]:
                    cta_seen_pages_by_type[cta_type].add(page_url)
                    cta_pages_by_type[cta_type].append(page_url)
        geo_signals = _geo_signals_for_page(
            url=page_url,
            html=html_raw,
            headings=headings,
            page_text=page_text,
            city=city,
        )
        if geo_signals:
            geo_intent_pages.append(
                {
                    "url": page_url,
                    "title": _extract_title(html_raw) or urlparse(page_url).path or "/",
                    "signals": geo_signals,
                    "hasCTA": bool(has_cta),
                }
            )

    out["cta_elements"] = [
        {
            "type": cta_type,
            "count": int(cta_hit_count_by_type.get(cta_type) or 0),
            "pages": list(cta_pages_by_type.get(cta_type) or []),
            "clickable_count": int(cta_clickable_count_by_type.get(cta_type) or 0),
        }
        for cta_type in CTA_PATTERN_BY_TYPE.keys()
    ]
    out["cta_clickable_by_type"] = {
        cta_type: int(cta_clickable_count_by_type.get(cta_type) or 0)
        for cta_type in CTA_PATTERN_BY_TYPE.keys()
    }
    out["cta_clickable_count"] = int(sum(cta_clickable_count_by_type.values()))
    out["geo_intent_pages"] = geo_intent_pages

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

    # -- Phase 5: Also check sitemap URLs by slug — only URLs we actually fetched -----
    crawled_url_set = set(crawled_pages.keys()) if isinstance(crawled_pages, dict) else set()
    for url in sitemap_urls:
        if url not in crawled_url_set:
            continue
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
    detected_display = sorted(
        [CANONICAL_DISPLAY[b] for b in all_detected if b in CANONICAL_DISPLAY]
    )
    out["high_ticket_procedures_detected"] = detected_display
    out["high_ticket_services_detected"] = detected_display
    out["general_services_detected"] = sorted(list(all_general))[:10]
    out["service_page_count"] = int(len(service_page_word_counts))
    out["service_pages_with_faq_or_schema"] = int(service_pages_with_schema)
    if service_page_word_counts:
        out["avg_word_count_service_pages"] = int(sum(service_page_word_counts) / len(service_page_word_counts))
        out["min_word_count_service_pages"] = int(min(service_page_word_counts))
        out["max_word_count_service_pages"] = int(max(service_page_word_counts))
    geo_urls = [str(p.get("url") or "") for p in geo_intent_pages if str(p.get("url") or "")]
    if geo_urls:
        out["city_or_near_me_page_count"] = int(len(geo_urls))
        out["has_multi_location_page"] = any("locations" in (urlparse(u).path or "").lower() for u in geo_urls)
        out["geo_page_examples"] = geo_urls[:3]
    else:
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
    if (vertical or "").strip().lower() == "dentist":
        catalog = _taxonomy_service_catalog(expected_services)
    else:
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
            path_val = str(p.get("path") or "")
            if _is_excluded_candidate_url(path_val) or _is_excluded_candidate_url(page_url):
                pages_rejected.append({"url": str(p["url"]), "rejection_reason": "Excluded URL pattern."})
                continue
            mention_count = _count_service_mentions(text, aliases)
            if mention_count > 0:
                saw_any_mention = True
            rule1_path = _path_match_fn(str(p["path"] or ""), slug, aliases)
            title_match = _title_match_fn(str(p.get("title") or ""), aliases)
            h1_match = _h1_match_fn(list(p.get("h1_list") or []), aliases)
            rule1_title_h1 = title_match or h1_match
            # Accept strong content-only pages even when URL/title are generic.
            rule1_content = bool(mention_count >= 4 and wc >= DEPTH_MODERATE_MIN_WORDS)
            rule1_match = bool(rule1_path or rule1_title_h1 or rule1_content)

            if bool(p.get("umbrella")) and mention_count >= 2:
                saw_umbrella_mention = True
                pages_rejected.append({"url": str(p["url"]), "rejection_reason": "Umbrella page (>= threshold services mentioned)."})
                continue

            if not rule1_match:
                continue

            density = _keyword_density(mention_count, wc)
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
            cta_count = _safe_int(conversion.get("cta_count"))
            service_conf = _compute_service_confidence(
                matched_url=bool(rule1_path),
                title_match=bool(title_match),
                h1_match=bool(h1_match),
                keyword_frequency=int(mention_count),
                word_count=int(wc),
            )
            service_conf_level = _confidence_level(service_conf)
            tiered_status = _tiered_service_status(
                url_match=bool(rule1_path),
                h1_match=bool(h1_match),
                keyword_count=int(mention_count),
                word_count=int(wc),
                cta_count=int(cta_count),
            )
            cc = str(out.get("crawl_confidence") or "").lower()
            if cc in ("low", "unknown", ""):
                tiered_status = ServiceStatus.NOT_EVALUATED
            tiered_status = _canonical_service_status(tiered_status)
            status = _qualification_from_service_status(tiered_status)
            detection_reason = str(agent_out.get("qualification_reason") or "Does not qualify as dedicated page.")
            core_rules = dict(agent_out.get("core_rules_passed") or {})
            booking_link = bool(conversion.get("booking_link"))

            # Keep strict agent output as metadata only; canonical service presence is deterministic.
            if status != ServiceStatus.MISSING and prediction_status in {"rejected_non_service", "umbrella_only", "missing"}:
                prediction_status = tiered_status
                if tiered_status == ServiceStatus.DEDICATED_PAGE:
                    detection_reason = "Dedicated service page detected by URL/title/H1 and substantial content."
                elif tiered_status == ServiceStatus.STRONG_UMBRELLA:
                    detection_reason = "Service mention detected, but no dedicated page evidence."
                elif tiered_status == ServiceStatus.NOT_EVALUATED:
                    detection_reason = "Not evaluated due to low crawl confidence."
            elif status == ServiceStatus.MISSING and booking_link and mention_count > 0 and bool(core_rules.get("blog_excluded", True)):
                tiered_status = ServiceStatus.STRONG_UMBRELLA
                status = _qualification_from_service_status(tiered_status)
                prediction_status = tiered_status
                detection_reason = "Service mention inferred from booking and keyword evidence."

            if status == ServiceStatus.MISSING:
                pages_rejected.append({"url": str(p["url"]), "rejection_reason": detection_reason})

            candidate = {
                "service": slug,
                "display_name": service_display_by_slug.get(slug, slug.replace("_", " ").title()),
                "revenue_weight": int(svc["revenue_weight"]),
                "page_detected": bool(status in {ServiceStatus.DEDICATED_PAGE, ServiceStatus.STRONG_UMBRELLA}),
                "page_exists": bool(status in {ServiceStatus.DEDICATED_PAGE, ServiceStatus.STRONG_UMBRELLA}),
                "qualified": bool(agent_out.get("qualified")),
                "prediction_status": _canonical_service_status(tiered_status),
                "service_status": _canonical_service_status(tiered_status),
                "confidence_score": service_conf,
                "confidence_level": service_conf_level,
                "page_strength": classify_page_strength(int(wc), service_conf),
                "detection_reason": detection_reason,
                "url": str(agent_out.get("final_url_evaluated") or p["path"] or "/"),
                "word_count": int(wc),
                "keyword_density": round(float(density), 4),
                "h1_match": bool(h1_match),
                "depth_score": _depth_score(wc) if status != ServiceStatus.MISSING else "missing",
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
                "qualification_status": _canonical_service_status(status),
                "optimization_tier": _canonical_service_status(status),
                "core_rules_passed": core_rules,
                "min_word_threshold": int(svc["min_word_threshold"]),
                "min_internal_links": int(svc["min_internal_links"]),
            }
            rank = _status_rank(_canonical_service_status(status))
            if rank > best_rank or (rank == best_rank and wc > best_wc):
                best_candidate = candidate
                best_rank = rank
                best_wc = wc

        if best_candidate is None:
            if saw_umbrella_mention:
                reason = "Service only mentioned within umbrella page."
                prediction_status = ServiceStatus.STRONG_UMBRELLA
                service_status = ServiceStatus.STRONG_UMBRELLA
                qual_status = _qualification_from_service_status(service_status)
            elif saw_any_mention:
                reason = "Service mentioned but no page passed strict qualification."
                prediction_status = ServiceStatus.STRONG_UMBRELLA
                service_status = ServiceStatus.STRONG_UMBRELLA
                qual_status = _qualification_from_service_status(service_status)
            elif reasons_seen:
                reason = reasons_seen[0]
                prediction_status = ServiceStatus.STRONG_UMBRELLA
                service_status = ServiceStatus.STRONG_UMBRELLA
                qual_status = _qualification_from_service_status(service_status)
            else:
                reason = "No dedicated page found in crawled HTML."
                prediction_status = ServiceStatus.MISSING
                service_status = ServiceStatus.MISSING
                qual_status = _qualification_from_service_status(service_status)
            best_candidate = {
                "service": slug,
                "display_name": service_display_by_slug.get(slug, slug.replace("_", " ").title()),
                "revenue_weight": int(svc["revenue_weight"]),
                "page_detected": bool(service_status in {ServiceStatus.DEDICATED_PAGE, ServiceStatus.STRONG_UMBRELLA}),
                "page_exists": bool(service_status in {ServiceStatus.DEDICATED_PAGE, ServiceStatus.STRONG_UMBRELLA}),
                "qualified": False,
                "prediction_status": _canonical_service_status(prediction_status),
                "service_status": _canonical_service_status(service_status),
                "confidence_score": 0.0,
                "confidence_level": "low",
                "page_strength": classify_page_strength(0, 0.0),
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
                "qualification_status": _canonical_service_status(qual_status),
                "optimization_tier": _canonical_service_status(qual_status),
                "min_word_threshold": int(svc["min_word_threshold"]),
                "min_internal_links": int(svc["min_internal_links"]),
            }
        service_rows.append(best_candidate)

    out["service_page_detection_debug"] = {
        "pages_crawled": pages_crawled_urls,
        "pages_rejected": pages_rejected,
        "umbrella_detection_triggered": bool(umbrella_triggered),
        "crawl_confidence": out.get("crawl_confidence"),
        "js_detected": out.get("js_detected"),
        "crawl_method": out.get("crawl_method"),
        "sitemap_detected": bool(crawl_result.get("sitemap_found")),
        "crawl_log_file": "crawl_log.json",
        "crawl_errors": list(crawl_result.get("errors") or [])[:50],
        "playwright_fetch_summary": dict(out.get("playwright_fetch_summary") or {}),
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

    out["missing_geo_pages"] = _detect_missing_service_pages(
        crawled_urls=pages_crawled_urls,
        vertical=vertical,
        pages=pages,
    )

    if str(out.get("crawl_confidence") or "").lower() in {"low", "unknown", ""}:
        for r in service_rows:
            r["service_status"] = ServiceStatus.NOT_EVALUATED
            r["qualification_status"] = _qualification_from_service_status(ServiceStatus.NOT_EVALUATED)
            r["optimization_tier"] = _qualification_from_service_status(ServiceStatus.NOT_EVALUATED)
            r["detection_reason"] = "Not Evaluated (Low Crawl Confidence)"
    out["missing_high_value_pages"] = [
        str(r["display_name"])
        for r in service_rows
        if str(r.get("service_status") or "").lower() == "missing"
    ]

    total = len(service_rows)
    dedicated_rows = [r for r in service_rows if str(r.get("service_status") or "").lower() == "dedicated"]
    mention_rows = [r for r in service_rows if str(r.get("service_status") or "").lower() == "mention_only"]
    missing_rows = [r for r in service_rows if str(r.get("service_status") or "").lower() == "missing"]
    present_rows = dedicated_rows + mention_rows
    wc_present = [int(r["word_count"]) for r in present_rows if int(r.get("word_count") or 0) > 0]
    top10_rows = [r for r in service_rows if (r.get("serp") or {}).get("position_top_10") is True]
    coverage_score = round(len(dedicated_rows) / total, 3) if total else 0.0
    optimized_ratio = coverage_score
    serp_ratio = round((len(top10_rows) / total), 3) if total else 0.0

    review_gap_high = bool((place_data or {}).get("review_gap_high") or (place_data or {}).get("signal_review_gap_high"))
    if coverage_score < 0.5 and review_gap_high:
        leverage = "high"
    elif coverage_score < 0.7:
        leverage = "moderate"
    else:
        leverage = "low"

    out["high_value_services"] = service_rows
    out["coverage_score"] = coverage_score
    out["high_value_summary"] = {
        "total_high_value_services": int(total),
        "services_dedicated": int(len(dedicated_rows)),
        "services_mention_only": int(len(mention_rows)),
        "services_missing": int(len(missing_rows)),
        "services_present": int(len(dedicated_rows)),
        "services_strong": int(len(dedicated_rows)),
        "services_moderate": int(len(mention_rows)),
        "services_weak": 0,
        "coverage_score": coverage_score,
        "service_coverage_ratio": coverage_score,
        "optimized_ratio": optimized_ratio,
        "average_word_count": int(sum(wc_present) / len(wc_present)) if wc_present else 0,
        "serp_visibility_ratio": serp_ratio,
    }
    thin_threshold = 300
    thin_pages = [r for r in present_rows if _safe_int(r.get("word_count")) < thin_threshold]
    cta_counts = [_safe_int((r.get("conversion") or {}).get("cta_count")) for r in present_rows]
    internal_links = [_safe_int((r.get("conversion") or {}).get("internal_links")) for r in present_rows]
    booking_pages = [r for r in present_rows if bool((r.get("conversion") or {}).get("booking_link"))]
    financing_pages = [r for r in present_rows if bool((r.get("conversion") or {}).get("financing_mentioned"))]
    faq_pages = [
        r for r in present_rows
        if bool((r.get("conversion") or {}).get("faq_section"))
        or bool((r.get("schema") or {}).get("faq_schema"))
    ]
    cta_pages = [r for r in present_rows if _safe_int((r.get("conversion") or {}).get("cta_count")) > 0]
    internal_linked_pages = [r for r in present_rows if _safe_int((r.get("conversion") or {}).get("internal_links")) > 0]
    contact_form_sitewide = _sitewide_contact_form_detected(pages)
    pages_checked = int(len(pages_crawled_urls))
    schema_pages_detected = int(service_pages_with_schema)
    faq_detected_count = int(len(faq_pages))
    local_pages_detected = int(out.get("city_or_near_me_page_count") or 0)
    conversion_detected_count = int(len(booking_pages) + len(financing_pages) + len(cta_pages))
    out["service_page_analysis_v2"] = {
        "service_coverage": {
            "present": int(len(dedicated_rows)),
            "total": int(total),
            "ratio": coverage_score,
            "status": _tri_state(int(len(dedicated_rows)), int(total)),
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
            "contact_form_detected_sitewide": bool(contact_form_sitewide),
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
    if out.get("crawl_confidence") == "low":
        out["service_page_analysis_v2"]["service_coverage"]["status"] = "unknown"
        out["service_page_analysis_v2"]["service_coverage"]["note"] = "Not Evaluated (Low Crawl Confidence)"
        out["service_page_analysis_v2"]["conversion_readiness"]["status"] = "unknown"
        out["service_page_analysis_v2"]["conversion_readiness"]["note"] = "Not Evaluated (Low Crawl Confidence)"
        leverage = "low"
    out["contact_form_detected_sitewide"] = bool(contact_form_sitewide)
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
        canonical_status = _canonical_service_status(
            row.get("service_status")
            or row.get("qualification_status")
            or row.get("optimization_tier")
            or ("dedicated" if bool(row.get("page_exists")) else "missing")
        )
        row["service_status"] = canonical_status
        row["qualification_status"] = canonical_status
        row["optimization_tier"] = canonical_status
        row["page_strength"] = classify_page_strength(
            _safe_int(row.get("word_count")),
            float(row.get("confidence_score") or 0.0),
        )
        updated_rows.append(row)

    total = len(updated_rows)
    dedicated_rows = [r for r in updated_rows if str(r.get("service_status") or "").lower() == "dedicated"]
    mention_rows = [r for r in updated_rows if str(r.get("service_status") or "").lower() == "mention_only"]
    missing_rows = [r for r in updated_rows if str(r.get("service_status") or "").lower() == "missing"]
    present_rows = dedicated_rows + mention_rows
    wc_present = [_safe_int(r.get("word_count")) for r in present_rows if _safe_int(r.get("word_count")) > 0]
    top10_rows = [r for r in updated_rows if (r.get("serp") or {}).get("position_top_10") is True]

    coverage_ratio = round(len(dedicated_rows) / total, 3) if total else 0.0
    optimized_ratio = coverage_ratio
    serp_ratio = round((len(top10_rows) / total), 3) if total else 0.0

    leverage = "low"
    if any(
        str(r.get("service_status") or "").lower() in {ServiceStatus.MISSING, ServiceStatus.STRONG_UMBRELLA}
        and _safe_int(r.get("revenue_weight")) >= 4
        for r in updated_rows
    ):
        leverage = "high"
    elif coverage_ratio >= STRONG_COVERAGE_RATIO_FOR_LOW_LEVERAGE and serp_ratio >= STRONG_SERP_RATIO_FOR_LOW_LEVERAGE:
        leverage = "low"
    elif total > 0:
        leverage = "moderate"
    if str(service_intelligence.get("crawl_confidence") or "").lower() == "low":
        leverage = "low"

    service_intelligence["high_value_services"] = updated_rows
    service_intelligence["coverage_score"] = coverage_ratio
    service_intelligence["high_value_summary"] = {
        "total_high_value_services": int(total),
        "services_dedicated": int(len(dedicated_rows)),
        "services_mention_only": int(len(mention_rows)),
        "services_missing": int(len(missing_rows)),
        "services_present": int(len(dedicated_rows)),
        "services_strong": int(len(dedicated_rows)),
        "services_moderate": int(len(mention_rows)),
        "services_weak": 0,
        "coverage_score": coverage_ratio,
        "service_coverage_ratio": coverage_ratio,
        "optimized_ratio": optimized_ratio,
        "average_word_count": int(sum(wc_present) / len(wc_present)) if wc_present else 0,
        "serp_visibility_ratio": serp_ratio,
    }
    thin_threshold = 300
    thin_pages = [r for r in present_rows if _safe_int(r.get("word_count")) < thin_threshold]
    cta_counts = [_safe_int((r.get("conversion") or {}).get("cta_count")) for r in present_rows]
    internal_links = [_safe_int((r.get("conversion") or {}).get("internal_links")) for r in present_rows]
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
            "present": int(len(dedicated_rows)),
            "total": int(total),
            "ratio": coverage_ratio,
            "status": _tri_state(int(len(dedicated_rows)), int(total)),
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
            "contact_form_detected_sitewide": bool(service_intelligence.get("contact_form_detected_sitewide")),
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
    if str(service_intelligence.get("crawl_confidence") or "").lower() == "low":
        service_intelligence["service_page_analysis_v2"]["service_coverage"]["status"] = "unknown"
        service_intelligence["service_page_analysis_v2"]["service_coverage"]["note"] = "Not Evaluated (Low Crawl Confidence)"
        service_intelligence["service_page_analysis_v2"]["conversion_readiness"]["status"] = "unknown"
        service_intelligence["service_page_analysis_v2"]["conversion_readiness"]["note"] = "Not Evaluated (Low Crawl Confidence)"
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
    crawl_conf = str(intel.get("crawl_confidence") or "").strip().lower()
    is_missing = (crawl_conf in {"medium", "high"}) and status in {"missing", "weak_stub_page"}
    return {
        "service": slug,
        "matches": bool(is_missing),
        "crawl_confidence": crawl_conf or None,
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
