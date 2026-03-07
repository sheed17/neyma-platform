"""
Deterministic multi-page crawl manager.
"""

from __future__ import annotations

import json
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse


def normalize(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urlparse(raw)
    scheme = p.scheme or "https"
    netloc = p.netloc.lower()
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return f"{scheme}://{netloc}{path}"


class CrawlManager:
    def __init__(self, base_url: str, fetch_fn: Optional[Callable[[str], Optional[str]]] = None):
        self.base_url = normalize(base_url)
        self.visited = set()
        self.to_visit: List[str] = []
        self.pages: Dict[str, Dict[str, Any]] = {}
        self.max_pages = 30
        self.js_site_detected = False
        self.sitemap_found = False
        self.sitemap_urls: List[str] = []
        self.fetch_fn = fetch_fn
        self.errors: List[Dict[str, str]] = []
        self.skipped: List[Dict[str, str]] = []
        self._redirects: Dict[str, str] = {}
        self._queued_reason: Dict[str, str] = {}

    def _same_domain(self, url: str) -> bool:
        a = urlparse(self.base_url).netloc.lower().replace("www.", "")
        b = urlparse(url).netloc.lower().replace("www.", "")
        return bool(a and b and a == b)

    def _is_asset_like(self, url: str) -> bool:
        path = (urlparse(url).path or "").lower()
        return bool(
            re.search(
                r"\.(?:jpg|jpeg|png|gif|svg|webp|ico|pdf|doc|docx|xls|xlsx|ppt|pptx|zip|rar|css|js|map|woff|woff2|ttf|eot|mp4|mov|avi)$",
                path,
            )
        )

    def _headers(self) -> Dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

    _SOFT_404_PHRASES = ("page not found", "404", "page doesn't exist",
                         "page does not exist", "can't be found", "no longer available")

    def _is_soft_404(self, html: str) -> bool:
        if self._text_len(html) > 500:
            return False
        lower = (html or "").lower()
        return any(phrase in lower for phrase in self._SOFT_404_PHRASES)

    def fetch_page(self, url: str) -> Optional[str]:
        if self.fetch_fn:
            try:
                return self.fetch_fn(url)
            except Exception as e:
                self.errors.append({"url": url, "error": f"fetch_fn_error:{type(e).__name__}"})
                return None
        try:
            import requests

            r = requests.get(url, timeout=10, headers=self._headers(), allow_redirects=True)
            if r.status_code != 200:
                self.errors.append({"url": url, "error": f"http_{r.status_code}"})
                return None
            final_url = normalize(r.url)
            if final_url and final_url != normalize(url):
                base_root = normalize(f"{urlparse(self.base_url).scheme}://{urlparse(self.base_url).netloc}")
                if final_url == base_root or final_url == base_root + "/":
                    self.skipped.append({"url": url, "reason": "redirected_to_homepage"})
                    return None
                self._redirects[normalize(url)] = final_url
            if self._is_soft_404(r.text):
                self.skipped.append({"url": url, "reason": "soft_404"})
                return None
            return r.text
        except Exception as e:
            self.errors.append({"url": url, "error": f"request_error:{type(e).__name__}"})
        return None

    _SKIP_HREF_PREFIXES = ("javascript:", "mailto:", "data:", "tel:", "sms:", "fax:")

    def _extract_links(self, html: str, current_url: str) -> List[str]:
        if not html:
            return []
        seen = set()
        out: List[str] = []
        patterns = [
            r'href\s*=\s*["\']([^"\'#]+)["\']',
            r'href\s*=\s*([^\s>"\'#]+)',
            r'data-href\s*=\s*["\']([^"\'#]+)["\']',
            r'data-url\s*=\s*["\']([^"\'#]+)["\']',
        ]
        for pat in patterns:
            for m in re.finditer(pat, html, re.I):
                href = (m.group(1) or "").strip()
                if not href or href.lower().startswith(self._SKIP_HREF_PREFIXES):
                    continue
                full = normalize(urljoin(current_url, href))
                if not full or not self._same_domain(full):
                    continue
                if self._is_asset_like(full):
                    continue
                if full in seen:
                    continue
                seen.add(full)
                out.append(full)
        return out

    def _text_len(self, html: str) -> int:
        text = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", html or "", flags=re.I)
        text = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return len(text)

    def _detect_js_site(self, html: str) -> bool:
        lower = (html or "").lower()
        low_text = self._text_len(html) < 500
        script_count = len(re.findall(r"<script", lower))
        has_root_div = ('<div id="root"' in lower) or ('<div id="app"' in lower)
        large_bundles = bool(re.search(r"(chunk|bundle|webpack|main\.[a-f0-9]{6,})", lower))
        return bool((low_text and script_count >= 8) or has_root_div or large_bundles)

    def _parse_sitemap(self, xml_text: str) -> Tuple[List[str], List[str]]:
        urls: List[str] = []
        nested_sitemaps: List[str] = []
        try:
            root = ET.fromstring(xml_text)
        except Exception:
            return urls, nested_sitemaps
        for elem in root.iter():
            tag = elem.tag.lower()
            txt = (elem.text or "").strip()
            if not txt:
                continue
            if tag.endswith("loc"):
                norm = normalize(txt)
                if not norm or not self._same_domain(norm):
                    continue
                if "/sitemap" in norm and norm.endswith(".xml"):
                    nested_sitemaps.append(norm)
                else:
                    urls.append(norm)
        return urls, nested_sitemaps

    def _fetch_sitemap_urls(self) -> List[str]:
        root = f"{urlparse(self.base_url).scheme}://{urlparse(self.base_url).netloc}"
        candidates = [f"{root}/sitemap.xml", f"{root}/sitemap_index.xml"]
        all_urls: List[str] = []
        seen_sitemaps = set()
        while candidates:
            sm = candidates.pop(0)
            if sm in seen_sitemaps:
                continue
            seen_sitemaps.add(sm)
            xml_text = self.fetch_page(sm)
            if not xml_text:
                continue
            self.sitemap_found = True
            urls, nested = self._parse_sitemap(xml_text)
            for u in urls:
                if u not in all_urls:
                    all_urls.append(u)
            for n in nested:
                if n not in seen_sitemaps and n not in candidates:
                    candidates.append(n)
        return all_urls[:500]

    _SKIP_PATH_TOKENS = ("/blog/", "/blog", "/article/", "/news/", "/archive/",
                         "/category/", "/tag/", "/author/", "/feed", "/wp-json")

    def _is_non_service_path(self, url: str) -> bool:
        path = (urlparse(url).path or "").lower()
        if any(tok in path for tok in self._SKIP_PATH_TOKENS):
            return True
        if re.search(r"/20\d{2}/", path):
            return True
        return False

    def _enqueue(self, url: str, reason: str) -> None:
        norm = normalize(url)
        if not norm:
            return
        if not self._same_domain(norm):
            self.skipped.append({"url": norm, "reason": "external"})
            return
        if self._is_asset_like(norm):
            self.skipped.append({"url": norm, "reason": "asset"})
            return
        if self._is_non_service_path(norm):
            self.skipped.append({"url": norm, "reason": "blog_or_archive"})
            return
        if norm in self.visited:
            return
        if norm not in self.to_visit:
            self.to_visit.append(norm)
            self._queued_reason[norm] = reason
            return
        prev = self._queued_reason.get(norm)
        if self._reason_priority(reason) > self._reason_priority(prev or ""):
            self._queued_reason[norm] = reason

    def _reason_priority(self, reason: str) -> int:
        r = str(reason or "").strip().lower()
        if r == "forced_expansion":
            return 4
        if r == "homepage_links":
            return 3
        if r == "sitemap":
            return 2
        if r == "internal_links":
            return 1
        return 0

    def _url_priority_score(self, url: str, practice_type: str) -> int:
        lower = str(url or "").lower()
        base_keywords = [
            "service", "services", "contact", "appointment", "schedule", "book",
            "braces", "invisalign", "implant",
        ]
        ortho = ["braces", "invisalign", "aligner", "orthodontic", "retainers"]
        general = ["implants", "cosmetic", "emergency", "root-canal", "whitening", "veneers"]
        keywords = list(base_keywords)
        if practice_type == "orthodontist":
            keywords.extend(ortho)
        elif practice_type == "general_dentist":
            keywords.extend(general)
        return int(sum(1 for k in keywords if k in lower))

    def _pop_next_url(self, practice_type: str) -> Optional[str]:
        if not self.to_visit:
            return None
        best_idx = 0
        best_key = (-1, -1, 0)
        for i, u in enumerate(self.to_visit):
            reason = self._queued_reason.get(u, "")
            reason_score = self._reason_priority(reason)
            keyword_score = self._url_priority_score(u, practice_type)
            path_depth = len([p for p in (urlparse(u).path or "").split("/") if p])
            key = (reason_score, keyword_score, -path_depth)
            if key > best_key:
                best_key = key
                best_idx = i
        chosen = self.to_visit.pop(best_idx)
        self._queued_reason.pop(chosen, None)
        return chosen

    def _prioritize(self, urls: List[str], practice_type: str) -> List[str]:
        base_keywords = [
            "service", "contact", "appointment", "schedule", "book", "braces", "invisalign", "implant",
        ]
        ortho = ["braces", "invisalign", "aligner", "orthodontic", "retainers"]
        general = ["implants", "cosmetic", "emergency", "root-canal", "whitening", "veneers"]
        keywords = list(base_keywords)
        if practice_type == "orthodontist":
            keywords.extend(ortho)
        elif practice_type == "general_dentist":
            keywords.extend(general)

        def score(u: str) -> Tuple[int, int]:
            lower = u.lower()
            kscore = sum(1 for k in keywords if k in lower)
            path_len = len((urlparse(u).path or "").split("/"))
            return (kscore, -path_len)

        return sorted(list(dict.fromkeys(urls)), key=score, reverse=True)

    def _force_expansion_paths(self, practice_type: str) -> List[str]:
        root = f"{urlparse(self.base_url).scheme}://{urlparse(self.base_url).netloc}"
        always = [
            "/contact", "/request-appointment", "/schedule", "/book", "/appointments",
            "/services", "/our-services", "/dental-services", "/procedures", "/treatments",
        ]
        if practice_type == "orthodontist":
            extra = [
                "/braces", "/invisalign", "/aligners", "/orthodontics", "/retainers",
                "/clear-aligners", "/early-treatment", "/adult-orthodontics",
            ]
        else:
            extra = [
                "/implants", "/dental-implants",
                "/cosmetic-dentistry", "/cosmetic",
                "/emergency-dentistry", "/emergency-dental", "/emergency",
                "/root-canal", "/root-canals",
                "/whitening", "/teeth-whitening",
                "/veneers", "/porcelain-veneers",
                "/crowns", "/dental-crowns", "/crowns-bridges",
                "/invisalign", "/orthodontics", "/braces",
                "/all-on-4", "/full-mouth-reconstruction",
                "/pediatric", "/pediatric-dentistry",
                "/general-dentistry", "/restorative-dentistry",
            ]
        return [normalize(root + p) for p in always + extra]

    def _write_crawl_log(self, confidence: str) -> None:
        import os
        if not os.environ.get("NEYMA_CRAWL_DEBUG"):
            return
        payload = {
            "base_url": self.base_url,
            "visited_urls": sorted(self.visited),
            "pages_crawled": len(self.pages),
            "pages_skipped": self.skipped[:200],
            "errors": self.errors[:200],
            "sitemap_detected": bool(self.sitemap_found),
            "sitemap_urls_sample": self.sitemap_urls[:100],
            "confidence": confidence,
            "js_detected": bool(self.js_site_detected),
            "timestamp_unix": int(time.time()),
        }
        try:
            with open("crawl_log.json", "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=True)
        except Exception:
            pass

    def crawl(self, practice_type: str = "general_dentist", homepage_html: Optional[str] = None) -> Dict[str, Any]:
        if not self.base_url:
            return {"pages": {}, "pages_crawled": 0, "confidence": "low", "js_detected": False}

        home_html = homepage_html or self.fetch_page(self.base_url)
        if not home_html:
            parsed = urlparse(self.base_url)
            if parsed.path == "/":
                alt = f"{parsed.scheme}://{parsed.netloc}"
                if alt != self.base_url:
                    home_html = self.fetch_page(alt)
        homepage_links: List[str] = []
        if home_html:
            self.pages[self.base_url] = {"url": self.base_url, "html": home_html}
            self.visited.add(self.base_url)
            self.js_site_detected = self._detect_js_site(home_html)
            homepage_links = self._extract_links(home_html, self.base_url)

        self.sitemap_urls = self._fetch_sitemap_urls()
        for u in self._prioritize(self.sitemap_urls, practice_type):
            self._enqueue(u, "sitemap")

        for u in self._force_expansion_paths(practice_type):
            self._enqueue(u, "forced_expansion")

        for u in homepage_links:
            self._enqueue(u, "homepage_links")

        # Keep crawling until cap. Expand from every fetched page.
        while self.to_visit and len(self.pages) < self.max_pages:
            url = self._pop_next_url(practice_type)
            if not url:
                break
            if url in self.visited:
                continue
            self.visited.add(url)
            html = self.fetch_page(url)
            if not html:
                continue
            self.pages[url] = {"url": url, "html": html}
            for nxt in self._extract_links(html, url):
                self._enqueue(nxt, "internal_links")

        pages_crawled = len(self.pages)
        if pages_crawled >= 10:
            level = 2  # high
        elif pages_crawled >= 5:
            level = 1  # medium
        else:
            level = 0  # low
        if self.sitemap_found:
            level += 1
        if self.js_site_detected:
            level = min(level, 1)
        level = max(0, min(2, level))
        confidence = ["low", "medium", "high"][level]
        self._write_crawl_log(confidence)
        return {
            "pages": dict(self.pages),
            "pages_crawled": int(pages_crawled),
            "confidence": confidence,
            "js_detected": bool(self.js_site_detected),
            "sitemap_found": bool(self.sitemap_found),
            "sitemap_urls": list(self.sitemap_urls),
            "errors": list(self.errors),
            "skipped": list(self.skipped),
            "redirects": dict(self._redirects),
        }
