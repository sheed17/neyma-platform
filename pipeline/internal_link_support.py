"""
Deterministic inbound internal link support for service pages.
"""

from __future__ import annotations

import re
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse


TRACKING_PARAMS = {"gclid", "fbclid", "msclkid"}


@dataclass
class CrawlConfig:
    max_pages: int = 120
    max_depth: int = 3
    timeout_ms: int = 12000
    mode: str = "auto"  # auto | fast_html | verified_rendered


def _same_domain(a: str, b: str) -> bool:
    try:
        na = urlparse(a).netloc.lower().replace("www.", "")
        nb = urlparse(b).netloc.lower().replace("www.", "")
        return na == nb
    except Exception:
        return False


def _strip_tracking_params(parsed) -> str:
    q = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=False):
        lk = k.lower()
        if lk.startswith("utm_") or lk in TRACKING_PARAMS:
            continue
        q.append((k, v))
    return urlencode(q, doseq=True)


def normalize_url(url: str, base_url: Optional[str] = None) -> Optional[str]:
    if not url:
        return None
    abs_url = urljoin(base_url, url) if base_url else url
    p = urlparse(abs_url)
    if p.scheme not in ("http", "https"):
        return None
    query = _strip_tracking_params(p)
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    p2 = p._replace(fragment="", path=path, query=query)
    return urlunparse(p2)


def _slash_variants(url: str) -> Tuple[str, str]:
    p = urlparse(url)
    path = p.path or "/"
    if path == "/":
        return (url, url)
    if path.endswith("/"):
        a = urlunparse(p._replace(path=path[:-1]))
        b = url
        return (a, b)
    a = url
    b = urlunparse(p._replace(path=path + "/"))
    return (a, b)


def _extract_canonical_url(page_url: str, html: str) -> Optional[str]:
    if not html:
        return None
    m = re.search(r"<link[^>]+rel=[\"']canonical[\"'][^>]+href=[\"']([^\"']+)[\"']", html, re.I)
    if not m:
        return None
    href = (m.group(1) or "").strip()
    if not href:
        return None
    return normalize_url(href, base_url=page_url)


def _extract_links(html: str, page_url: str) -> List[Dict[str, Any]]:
    if not html:
        return []
    ranges: Dict[str, List[Tuple[int, int]]] = {
        "nav": _tag_ranges(html, "nav"),
        "footer": _tag_ranges(html, "footer"),
        "header": _tag_ranges(html, "header"),
        "main": _tag_ranges(html, "main"),
        "aside": _tag_ranges(html, "aside"),
        "sidebar": _sidebar_ranges(html),
        "body": _tag_ranges(html, "body"),
    }
    out: List[Dict[str, Any]] = []
    for m in re.finditer(r"<a\b[^>]*href\s*=\s*[\"']([^\"']+)[\"'][^>]*>", html, re.I | re.S):
        href = (m.group(1) or "").strip()
        target = normalize_url(href, base_url=page_url)
        if not target:
            continue
        idx = m.start()
        context = _classify_link_context(idx, ranges)
        out.append({"target": target, "context": context})
    return out


def _tag_ranges(html: str, tag: str) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    pat = re.compile(rf"<{tag}\b[^>]*>[\s\S]*?</{tag}>", re.I)
    for m in pat.finditer(html or ""):
        ranges.append((m.start(), m.end()))
    return ranges


def _sidebar_ranges(html: str) -> List[Tuple[int, int]]:
    ranges: List[Tuple[int, int]] = []
    pat = re.compile(
        r"<(aside|div|section)\b[^>]*(class|id)\s*=\s*[\"'][^\"']*(sidebar|widget|rail)[^\"']*[\"'][^>]*>[\s\S]*?</\1>",
        re.I,
    )
    for m in pat.finditer(html or ""):
        ranges.append((m.start(), m.end()))
    return ranges


def _in_ranges(idx: int, ranges: Iterable[Tuple[int, int]]) -> bool:
    for s, e in ranges:
        if s <= idx <= e:
            return True
    return False


def _classify_link_context(idx: int, ranges: Dict[str, List[Tuple[int, int]]]) -> str:
    if _in_ranges(idx, ranges.get("nav", [])):
        return "nav"
    if _in_ranges(idx, ranges.get("footer", [])):
        return "footer"
    if _in_ranges(idx, ranges.get("header", [])):
        return "header"
    if _in_ranges(idx, ranges.get("sidebar", [])) or _in_ranges(idx, ranges.get("aside", [])):
        return "sidebar"
    if _in_ranges(idx, ranges.get("main", [])):
        return "body"
    if _in_ranges(idx, ranges.get("body", [])):
        return "body"
    return "unknown"


def _discover_sitemap_urls(base_url: str, fetch_html: Callable[[str], Optional[str]]) -> Tuple[List[str], bool]:
    root = urlparse(base_url)
    root_url = f"{root.scheme}://{root.netloc}"
    sitemap_candidates = ["/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml"]
    all_urls: List[str] = []
    used = False
    for path in sitemap_candidates:
        xml = fetch_html(root_url + path)
        if not xml:
            continue
        used = True
        locs = re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", xml, flags=re.I)
        for loc in locs:
            n = normalize_url(loc)
            if n and _same_domain(base_url, n) and n not in all_urls:
                all_urls.append(n)
        # Support one-level sitemap index recursion
        if "<sitemapindex" in xml.lower():
            for sm in locs[:20]:
                sub = fetch_html(sm)
                if not sub:
                    continue
                sub_locs = re.findall(r"<loc>\s*(https?://[^<]+?)\s*</loc>", sub, flags=re.I)
                for loc in sub_locs:
                    n = normalize_url(loc)
                    if n and _same_domain(base_url, n) and n not in all_urls:
                        all_urls.append(n)
        if all_urls:
            break
    return all_urls, used


def _looks_js_heavy(html: str, link_count: int) -> bool:
    h = (html or "").lower()
    markers = (
        "__next_data__",
        "data-reactroot",
        "id=\"__next\"",
        "ng-version",
        "window.__nuxt",
        "webpack",
    )
    return link_count <= 2 or any(m in h for m in markers)


def _fetch_html_fast(url: str, timeout_ms: int) -> Optional[str]:
    try:
        import requests

        r = requests.get(
            url,
            timeout=max(3.0, timeout_ms / 1000.0),
            allow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        return None
    return None


def _fetch_html_rendered(url: str, timeout_ms: int) -> Optional[str]:
    try:
        from pipeline.headless_browser import render_page

        html, _ = render_page(url, timeout_ms=timeout_ms)
        if html:
            return html
    except Exception:
        pass
    return None


def build_internal_link_graph(
    base_url: str,
    config: Optional[CrawlConfig] = None,
    pre_crawled_pages: Optional[List[Dict[str, Any]]] = None,
    fetch_html_fn: Optional[Callable[[str, int], Optional[str]]] = None,
    render_html_fn: Optional[Callable[[str, int], Optional[str]]] = None,
) -> Dict[str, Any]:
    cfg = config or CrawlConfig()
    base = normalize_url(base_url)
    if not base:
        return {
            "pages": {},
            "canonical_map": {},
            "crawl_meta": {
                "pages_crawled_n": 0,
                "pages_skipped_n": 0,
                "crawl_depth_stats": {},
                "max_depth": cfg.max_depth,
                "used_rendered_mode": False,
                "sitemap_used": False,
                "accuracy_limited": True,
                "crawl_coverage_score": 0.0,
            },
        }

    fetch_fast = fetch_html_fn or _fetch_html_fast
    fetch_rendered = render_html_fn or _fetch_html_rendered
    started = time.monotonic()

    pages: Dict[str, Dict[str, Any]] = {}
    pages_skipped_n = 0
    used_rendered = False
    discovered_urls: Set[str] = set()
    depth_stats: Dict[int, int] = {}

    if pre_crawled_pages:
        for p in pre_crawled_pages:
            u = normalize_url(str(p.get("url") or ""), base)
            h = p.get("html")
            if not u or not h or not _same_domain(base, u):
                continue
            pages[u] = {"url": u, "html": str(h), "depth": int(p.get("depth") or 0)}
            depth = int(p.get("depth") or 0)
            depth_stats[depth] = depth_stats.get(depth, 0) + 1
        sitemap_urls: List[str] = list(pages.keys())
        sitemap_used = False
    else:
        sitemap_urls, sitemap_used = _discover_sitemap_urls(base, lambda u: fetch_fast(u, cfg.timeout_ms))
        for u in sitemap_urls:
            discovered_urls.add(u)

        q: Deque[Tuple[str, int]] = deque()
        q.append((base, 0))
        seen: Set[str] = set()

        while q and len(pages) < cfg.max_pages:
            if (time.monotonic() - started) * 1000 > cfg.timeout_ms:
                break
            url, depth = q.popleft()
            if url in seen:
                continue
            seen.add(url)
            if depth > cfg.max_depth:
                pages_skipped_n += 1
                continue
            if not _same_domain(base, url):
                pages_skipped_n += 1
                continue

            html = fetch_fast(url, cfg.timeout_ms)
            link_count = len(re.findall(r"<a\b", html or "", flags=re.I))
            must_render = cfg.mode == "verified_rendered"
            auto_render = cfg.mode == "auto" and _looks_js_heavy(html or "", link_count)
            if (must_render or auto_render) and (time.monotonic() - started) * 1000 <= cfg.timeout_ms:
                rendered = fetch_rendered(url, cfg.timeout_ms)
                if rendered:
                    html = rendered
                    used_rendered = True

            if not html:
                pages_skipped_n += 1
                continue

            pages[url] = {"url": url, "html": html, "depth": depth}
            depth_stats[depth] = depth_stats.get(depth, 0) + 1
            discovered_urls.add(url)

            for link in _extract_links(html, url):
                t = link.get("target")
                if not t or not _same_domain(base, str(t)):
                    continue
                if t not in discovered_urls:
                    discovered_urls.add(t)
                if t not in seen and depth + 1 <= cfg.max_depth and len(q) < (cfg.max_pages * 3):
                    q.append((str(t), depth + 1))

        # If sitemap exists, opportunistically fetch sitemap URLs not yet crawled.
        for u in sitemap_urls:
            if len(pages) >= cfg.max_pages:
                break
            if (time.monotonic() - started) * 1000 > cfg.timeout_ms:
                break
            if u in pages:
                continue
            html = fetch_fast(u, cfg.timeout_ms)
            if not html:
                pages_skipped_n += 1
                continue
            pages[u] = {"url": u, "html": html, "depth": 1}
            depth_stats[1] = depth_stats.get(1, 0) + 1
            discovered_urls.add(u)

    canonical_map: Dict[str, str] = {}
    for u, page in pages.items():
        canonical = _extract_canonical_url(u, page.get("html") or "")
        if canonical and _same_domain(base, canonical):
            canonical_map[u] = canonical
        else:
            canonical_map[u] = u

    # Expand map with slash variants
    for k, v in list(canonical_map.items()):
        a, b = _slash_variants(k)
        canonical_map[a] = v
        canonical_map[b] = v

    crawled_n = len(pages)
    discovered_n = max(crawled_n, len(discovered_urls))
    coverage_ratio = float(crawled_n) / float(discovered_n) if discovered_n > 0 else 0.0
    render_bonus = 0.1 if used_rendered else 0.0
    depth_reached = max(depth_stats.keys()) if depth_stats else 0
    depth_bonus = min(0.2, 0.05 * depth_reached)
    score = min(1.0, (coverage_ratio * 0.7) + render_bonus + depth_bonus)
    accuracy_limited = bool((cfg.mode == "fast_html" and not used_rendered and crawled_n < 10) or crawled_n == 0)

    graph_pages: Dict[str, Dict[str, Any]] = {}
    for u, page in pages.items():
        links = _extract_links(page.get("html") or "", u)
        graph_pages[u] = {
            "url": u,
            "canonical_url": canonical_map.get(u, u),
            "depth": int(page.get("depth") or 0),
            "links": links,
        }

    return {
        "pages": graph_pages,
        "canonical_map": canonical_map,
        "crawl_meta": {
            "pages_crawled_n": crawled_n,
            "pages_skipped_n": pages_skipped_n,
            "crawl_depth_stats": depth_stats,
            "max_depth": cfg.max_depth,
            "used_rendered_mode": used_rendered,
            "sitemap_used": bool(pre_crawled_pages is None and sitemap_used),
            "accuracy_limited": accuracy_limited,
            "crawl_coverage_score": round(score, 3),
        },
    }


def _canonicalize(url: str, canonical_map: Dict[str, str]) -> str:
    n = normalize_url(url) or url
    if n in canonical_map:
        return canonical_map[n]
    a, b = _slash_variants(n)
    return canonical_map.get(a) or canonical_map.get(b) or n


def compute_inbound_internal_link_support(target_url: str, graph: Dict[str, Any]) -> Dict[str, Any]:
    pages = graph.get("pages") if isinstance(graph.get("pages"), dict) else {}
    canonical_map = graph.get("canonical_map") if isinstance(graph.get("canonical_map"), dict) else {}
    crawl_meta = graph.get("crawl_meta") if isinstance(graph.get("crawl_meta"), dict) else {}

    target_norm = normalize_url(target_url) or target_url
    canonical_target = _canonicalize(target_norm, canonical_map)

    source_all: Set[str] = set()
    source_body: Set[str] = set()
    total_links_all = 0
    contexts = {"nav": 0, "footer": 0, "header": 0, "body": 0, "sidebar": 0, "unknown": 0}

    for source_url, page in pages.items():
        source_canonical = _canonicalize(source_url, canonical_map)
        for link in page.get("links") or []:
            t = str(link.get("target") or "")
            context = str(link.get("context") or "unknown")
            if context not in contexts:
                context = "unknown"
            target_canonical = _canonicalize(t, canonical_map)
            if target_canonical != canonical_target:
                continue
            if source_canonical == canonical_target:
                continue
            source_all.add(source_canonical)
            total_links_all += 1
            contexts[context] = contexts.get(context, 0) + 1
            if context == "body":
                source_body.add(source_canonical)

    return {
        "target_url": target_norm,
        "canonical_target_url": canonical_target,
        "inbound": {
            "unique_pages_all": int(len(source_all)),
            "unique_pages_body_only": int(len(source_body)),
            "total_links_all": int(total_links_all),
            "contexts": contexts,
        },
        "orphan": {
            "is_orphan_all": bool(len(source_all) == 0),
            "is_orphan_body_only": bool(len(source_body) == 0),
        },
        "crawl_meta": {
            "pages_crawled_n": int(crawl_meta.get("pages_crawled_n") or 0),
            "max_depth": int(crawl_meta.get("max_depth") or 0),
            "used_rendered_mode": bool(crawl_meta.get("used_rendered_mode")),
            "sitemap_used": bool(crawl_meta.get("sitemap_used")),
            "pages_skipped_n": int(crawl_meta.get("pages_skipped_n") or 0),
            "crawl_depth_stats": dict(crawl_meta.get("crawl_depth_stats") or {}),
            "accuracy_limited": bool(crawl_meta.get("accuracy_limited")),
            "crawl_coverage_score": float(crawl_meta.get("crawl_coverage_score") or 0.0),
        },
    }


def wiring_status(body_only_unique_pages: int) -> str:
    n = int(body_only_unique_pages or 0)
    if n <= 0:
        return "orphan"
    if n <= 2:
        return "weak"
    if n <= 7:
        return "linked"
    return "core"
