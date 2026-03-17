"""
Enhanced Google Ads detection.

Multi-layered approach to accurately detect whether a practice runs Google Ads:
  1. Main site HTML scan (tracking pixels, GTM, conversion tags)
  2. Booking/landing subdomain check (book.*, go.*, lp.*, app.*)
  3. Google Tag Manager container inspection
  4. Google Ads redirect/gclid indicators in links

This catches practices whose ads point to subdomains or booking platforms,
not just those with tracking on the homepage.
"""

import re
import logging
from typing import Dict, Any, List, Optional, Set
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 10

# Google Ads tracking patterns (expanded from signals.py)
GOOGLE_ADS_PATTERNS = [
    r'googleads\.g\.doubleclick\.net',
    r'google_conversion',
    r'gtag\s*\(\s*["\']config["\']',
    r'AW-\d{5,}',
    r'google[-_]?ads',
    r'adwords',
    r'googlesyndication\.com',
    r'googleadservices\.com',
    r'google_remarketing',
    r'conversion[-_]?label',
    r'conversion[-_]?id',
    r'ads/ga-audiences',
    r'pagead/conversion',
    r'/pagead/',
    r'gclid',
    r'gcl_aw',
    r'wbraid',
    r'gbraid',
    r'ads\.google\.com',
]

# Google Tag Manager patterns
GTM_PATTERNS = [
    r'googletagmanager\.com/gtm\.js',
    r'googletagmanager\.com/ns\.html',
    r'GTM-[A-Z0-9]{5,}',
    r'google_tag_manager',
    r'gtm\.start',
]

# Common booking/landing subdomains dental practices use for ad landing pages
BOOKING_SUBDOMAINS = [
    'book', 'booking', 'go', 'lp', 'app', 'get', 'schedule',
    'appointment', 'offer', 'promo', 'landing', 'start',
]


def _fetch_html(url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[str]:
    try:
        import requests as req
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        r = req.get(
            url, timeout=timeout, allow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            },
        )
        if r.status_code == 200:
            return r.text
    except Exception as e:
        logger.debug("Google Ads check fetch failed %s: %s", url[:60], e)
    return None


def _extract_domain(url_or_domain: str) -> str:
    s = url_or_domain.strip()
    if not s:
        return ""
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    parsed = urlparse(s)
    domain = (parsed.netloc or parsed.path).lower().strip("/")
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def _check_patterns(html: str, patterns: List[str]) -> List[str]:
    """Return list of pattern names that matched in the HTML."""
    if not html:
        return []
    matched = []
    for pat in patterns:
        if re.search(pat, html, re.I):
            matched.append(pat)
    return matched


def _extract_gtm_ids(html: str) -> List[str]:
    """Extract GTM container IDs from HTML."""
    if not html:
        return []
    return list(set(re.findall(r'GTM-[A-Z0-9]{4,}', html)))


def _scan_for_google_ads(html: str) -> Dict[str, Any]:
    """Scan HTML for Google Ads indicators. Returns detection result."""
    if not html:
        return {"detected": False, "signals": [], "gtm_ids": []}

    ad_signals = _check_patterns(html, GOOGLE_ADS_PATTERNS)
    gtm_signals = _check_patterns(html, GTM_PATTERNS)
    gtm_ids = _extract_gtm_ids(html)

    detected = len(ad_signals) > 0
    has_gtm = len(gtm_signals) > 0

    return {
        "detected": detected,
        "has_gtm": has_gtm,
        "signals": ad_signals[:10],
        "gtm_ids": gtm_ids,
        "signal_count": len(ad_signals),
    }


def _inspect_gtm_container(gtm_id: str) -> Dict[str, Any]:
    """
    Fetch a GTM container's JS and look for Google Ads conversion IDs,
    conversion labels, and remarketing tags.
    """
    result: Dict[str, Any] = {
        "has_google_ads": False,
        "conversion_ids": [],
        "has_remarketing": False,
    }
    try:
        import requests as req
        url = f"https://www.googletagmanager.com/gtm.js?id={gtm_id}"
        r = req.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        if r.status_code != 200:
            return result
        text = r.text

        aw_ids = list(set(re.findall(r'AW-\d{5,}', text)))
        if aw_ids:
            result["has_google_ads"] = True
            result["conversion_ids"] = aw_ids[:5]

        gads_refs = re.findall(r'googleads|googleadservices|google[-_]conversion|adwords', text, re.I)
        if gads_refs:
            result["has_google_ads"] = True

        if re.search(r'remarketing|retargeting|ga[-_]audiences', text, re.I):
            result["has_remarketing"] = True
            result["has_google_ads"] = True

    except Exception as e:
        logger.debug("GTM inspection failed for %s: %s", gtm_id, e)

    return result


def check_google_ads(
    website_url: str,
    website_html: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Multi-layered Google Ads detection.

    Layer 1: Check main site HTML for Google Ads tracking
    Layer 2: Check booking/landing subdomains for Google Ads tracking
    Layer 3: GTM presence as a strong indicator

    Args:
        website_url: Website URL or bare domain.
        website_html: Pre-fetched HTML of the main site (optional).

    Returns:
        {
            "runs_google_ads": True/False/None,
            "confidence": "high" | "medium" | "low",
            "ad_signals_count": int,
            "sources": list of detection sources,
            "has_gtm": bool,
            "gtm_ids": list,
            "subdomains_checked": int,
            "subdomains_with_ads": list,
            "error": str or None,
        }
    """
    domain = _extract_domain(website_url)
    if not domain:
        return {
            "runs_google_ads": None,
            "confidence": "low",
            "ad_signals_count": 0,
            "sources": [],
            "has_gtm": False,
            "gtm_ids": [],
            "subdomains_checked": 0,
            "subdomains_with_ads": [],
            "error": "No domain provided",
        }

    sources: List[str] = []
    all_ad_signals: List[str] = []
    all_gtm_ids: List[str] = []
    has_gtm = False
    subdomains_with_ads: List[str] = []
    subdomains_checked = 0

    # Layer 1: Main site
    main_html = website_html
    if not main_html:
        main_html = _fetch_html(f"https://www.{domain}") or _fetch_html(f"https://{domain}")

    if main_html:
        result = _scan_for_google_ads(main_html)
        if result["detected"]:
            sources.append("main_site")
            all_ad_signals.extend(result["signals"])
        if result.get("has_gtm"):
            has_gtm = True
            sources.append("main_site_gtm")
        all_gtm_ids.extend(result.get("gtm_ids", []))

    # Layer 2: Check booking/landing subdomains
    for sub in BOOKING_SUBDOMAINS:
        subdomain_url = f"https://{sub}.{domain}"
        sub_html = _fetch_html(subdomain_url, timeout=6)
        if sub_html:
            subdomains_checked += 1
            result = _scan_for_google_ads(sub_html)
            if result["detected"]:
                subdomains_with_ads.append(f"{sub}.{domain}")
                sources.append(f"subdomain:{sub}")
                all_ad_signals.extend(result["signals"])
            if result.get("has_gtm"):
                has_gtm = True
            all_gtm_ids.extend(result.get("gtm_ids", []))

    # Layer 3: Inspect GTM containers for Google Ads conversion IDs
    unique_gtm_ids = list(set(all_gtm_ids))
    gtm_conversion_ids: List[str] = []
    gtm_has_remarketing = False

    for gtm_id in unique_gtm_ids[:3]:
        gtm_result = _inspect_gtm_container(gtm_id)
        if gtm_result.get("has_google_ads"):
            has_gtm = True
            sources.append(f"gtm_container:{gtm_id}")
            all_ad_signals.append(f"gtm_container_ads:{gtm_id}")
            gtm_conversion_ids.extend(gtm_result.get("conversion_ids", []))
        if gtm_result.get("has_remarketing"):
            gtm_has_remarketing = True

    # Determine overall result
    total_signals = len(all_ad_signals)
    runs_google_ads: Optional[bool] = None
    confidence = "low"

    if total_signals >= 3:
        runs_google_ads = True
        confidence = "high"
    elif total_signals >= 1:
        runs_google_ads = True
        confidence = "medium"
    elif has_gtm:
        runs_google_ads = None
        confidence = "low"
    elif main_html and not total_signals:
        runs_google_ads = False
        confidence = "medium"

    return {
        "runs_google_ads": runs_google_ads,
        "confidence": confidence,
        "ad_signals_count": total_signals,
        "sources": list(dict.fromkeys(sources)),
        "has_gtm": has_gtm,
        "gtm_ids": unique_gtm_ids,
        "google_ads_conversion_ids": list(set(gtm_conversion_ids)),
        "has_remarketing": gtm_has_remarketing,
        "subdomains_checked": subdomains_checked,
        "subdomains_with_ads": subdomains_with_ads,
        "error": None,
    }


def augment_lead_with_google_ads(lead: Dict[str, Any]) -> None:
    """
    Check for Google Ads across main site and subdomains,
    then update paid-ads signals if confirmed.

    Only upgrades signals — never downgrades an existing True to False.
    """
    url = lead.get("signal_website_url") or lead.get("website") or ""
    if not url:
        return

    # Pass pre-fetched HTML if available to avoid double-fetching
    website_html = lead.get("_website_html")

    result = check_google_ads(url, website_html=website_html)
    lead["google_ads_check"] = result

    if result.get("runs_google_ads") is True:
        lead["signal_runs_paid_ads"] = True
        channels = lead.get("signal_paid_ads_channels")
        if isinstance(channels, list):
            if "google" not in channels:
                lead["signal_paid_ads_channels"] = channels + ["google"]
        else:
            lead["signal_paid_ads_channels"] = ["google"]
        lead["signal_google_ads_source"] = ", ".join(result.get("sources", []))
        lead["signal_google_ads_count"] = result.get("ad_signals_count", 0)

        domain = _extract_domain(url)
        subs = result.get("subdomains_with_ads", [])
        if subs:
            logger.info(
                "  Google Ads: %s — confirmed via %s (signals: %d, subdomains: %s)",
                domain, ", ".join(result["sources"]), result["ad_signals_count"],
                ", ".join(subs),
            )
        else:
            logger.info(
                "  Google Ads: %s — confirmed on main site (signals: %d)",
                domain, result["ad_signals_count"],
            )
    elif result.get("runs_google_ads") is False:
        if lead.get("signal_runs_paid_ads") is not True:
            logger.info("  Google Ads: %s — not detected", _extract_domain(url))
    else:
        logger.debug(
            "  Google Ads: %s — inconclusive (GTM: %s)",
            _extract_domain(url), result.get("has_gtm"),
        )
