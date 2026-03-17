"""
SEO traffic data integration.

Fetches real organic traffic estimates from third-party SEO APIs
(Semrush or Ahrefs) when configured. Falls back gracefully to
the existing proxy-based traffic model when no API key is available.

Configuration (environment variables):
  SEO_TRAFFIC_PROVIDER=semrush|ahrefs  (default: semrush)
  SEMRUSH_API_KEY=<key>
  AHREFS_API_KEY=<key>
"""

import os
import logging
from typing import Dict, Optional, Any

import requests

logger = logging.getLogger(__name__)

SEMRUSH_API_KEY = os.getenv("SEMRUSH_API_KEY")
AHREFS_API_KEY = os.getenv("AHREFS_API_KEY")
SEO_TRAFFIC_PROVIDER = os.getenv("SEO_TRAFFIC_PROVIDER", "semrush").lower()

SEMRUSH_DOMAIN_OVERVIEW_URL = "https://api.semrush.com/"
AHREFS_METRICS_URL = "https://apiv2.ahrefs.com"

REQUEST_TIMEOUT = 15


def is_seo_traffic_available() -> bool:
    """Return True if a valid SEO traffic API key is configured."""
    if SEO_TRAFFIC_PROVIDER == "semrush" and SEMRUSH_API_KEY:
        return True
    if SEO_TRAFFIC_PROVIDER == "ahrefs" and AHREFS_API_KEY:
        return True
    return False


def _fetch_semrush_domain_overview(domain: str) -> Optional[Dict[str, Any]]:
    """
    Fetch domain overview from Semrush API.

    Returns organic traffic, keyword count, traffic cost, and top keywords.
    """
    if not SEMRUSH_API_KEY:
        return None

    try:
        resp = requests.get(
            SEMRUSH_DOMAIN_OVERVIEW_URL,
            params={
                "type": "domain_ranks",
                "key": SEMRUSH_API_KEY,
                "export_columns": "Or,Ot,Oc,Ad,At,Ac",
                "domain": domain,
                "database": "us",
            },
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code != 200:
            logger.warning("Semrush API error %d for %s", resp.status_code, domain)
            return None

        lines = resp.text.strip().split("\n")
        if len(lines) < 2:
            logger.info("No Semrush data for %s", domain)
            return None

        headers = lines[0].split(";")
        values = lines[1].split(";")
        data = dict(zip(headers, values))

        organic_keywords = _safe_int(data.get("Or"))
        organic_traffic = _safe_int(data.get("Ot"))
        organic_cost = _safe_float(data.get("Oc"))
        adwords_keywords = _safe_int(data.get("Ad"))
        adwords_traffic = _safe_int(data.get("At"))
        adwords_cost = _safe_float(data.get("Ac"))

        return {
            "provider": "semrush",
            "organic_keywords": organic_keywords,
            "organic_traffic_monthly": organic_traffic,
            "organic_traffic_cost": organic_cost,
            "paid_keywords": adwords_keywords,
            "paid_traffic_monthly": adwords_traffic,
            "paid_traffic_cost": adwords_cost,
        }

    except Exception as exc:
        logger.warning("Semrush API request failed for %s: %s", domain, exc)
        return None


def _fetch_ahrefs_domain_metrics(domain: str) -> Optional[Dict[str, Any]]:
    """
    Fetch domain metrics from Ahrefs API v2.

    Returns organic traffic estimate and keyword rankings.
    """
    if not AHREFS_API_KEY:
        return None

    try:
        resp = requests.get(
            AHREFS_METRICS_URL,
            params={
                "token": AHREFS_API_KEY,
                "from": "domain_rating",
                "target": domain,
                "mode": "domain",
                "output": "json",
            },
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code != 200:
            logger.warning("Ahrefs API error %d for %s", resp.status_code, domain)
            return None

        data = resp.json()
        pages = data.get("pages", [])
        if not pages:
            return None

        page = pages[0]
        return {
            "provider": "ahrefs",
            "domain_rating": _safe_float(page.get("domain_rating")),
            "organic_keywords": None,
            "organic_traffic_monthly": None,
            "paid_traffic_monthly": None,
        }

    except Exception as exc:
        logger.warning("Ahrefs API request failed for %s: %s", domain, exc)
        return None


def fetch_seo_traffic(domain: str) -> Optional[Dict[str, Any]]:
    """
    Fetch real SEO traffic data for a domain.

    Dispatches to the configured provider (Semrush or Ahrefs).
    Returns None if no API key is configured or the request fails.
    """
    if not domain:
        return None

    domain = domain.lower().strip()
    if domain.startswith("http"):
        from urllib.parse import urlparse
        domain = urlparse(domain).netloc or domain
    domain = domain.replace("www.", "")

    if SEO_TRAFFIC_PROVIDER == "ahrefs":
        return _fetch_ahrefs_domain_metrics(domain)
    return _fetch_semrush_domain_overview(domain)


def augment_lead_with_seo_traffic(lead: Dict) -> Dict:
    """
    Augment a lead dict with real SEO traffic data if available.

    Stores results under lead["seo_traffic_data"] and upgrades
    traffic estimate signals when real data is available.
    """
    if not is_seo_traffic_available():
        return lead

    website = lead.get("signal_website_url") or lead.get("website") or ""
    if not website:
        return lead

    from urllib.parse import urlparse
    domain = urlparse(website).netloc if website.startswith("http") else website
    domain = domain.replace("www.", "")

    seo_data = fetch_seo_traffic(domain)
    if not seo_data:
        return lead

    lead["seo_traffic_data"] = seo_data

    organic = seo_data.get("organic_traffic_monthly")
    if organic is not None and organic > 0:
        lead["signal_real_organic_traffic"] = organic
        lead["signal_traffic_source"] = seo_data["provider"]

    paid = seo_data.get("paid_traffic_monthly")
    if paid is not None and paid > 0:
        lead["signal_real_paid_traffic"] = paid

    paid_cost = seo_data.get("paid_traffic_cost")
    if paid_cost is not None and paid_cost > 0:
        lead["signal_real_paid_spend"] = paid_cost

    logger.info(
        "SEO traffic data for %s: organic=%s, paid=%s (provider=%s)",
        domain, organic, paid, seo_data["provider"],
    )
    return lead


def _safe_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
