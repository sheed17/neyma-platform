"""
Headless browser module for JavaScript-rendered website analysis.

Uses Playwright to render pages and extract signals that are invisible
to simple HTTP requests (JS-rendered forms, dynamic booking widgets,
lazy-loaded ad pixels, SPA content).

Design:
- Optional enhancement layer — system works without it
- Activated when HEADLESS_ENABLED=true in environment
- Falls back gracefully if Playwright is not installed
- Shares signal extraction logic with signals.py via _analyze_html_content
"""

import os
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

HEADLESS_ENABLED = os.getenv("HEADLESS_ENABLED", "false").lower() in ("true", "1", "yes")
HEADLESS_TIMEOUT_MS = int(os.getenv("HEADLESS_TIMEOUT_MS", "15000"))

_playwright_available: Optional[bool] = None


def _check_playwright() -> bool:
    """Lazy-check whether Playwright is importable and browsers are installed."""
    global _playwright_available
    if _playwright_available is not None:
        return _playwright_available
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        _playwright_available = True
    except ImportError:
        _playwright_available = False
        logger.info("Playwright not installed — headless browser disabled")
    return _playwright_available


def is_headless_available() -> bool:
    """Return True if headless browser is both enabled and importable."""
    return HEADLESS_ENABLED and _check_playwright()


def render_page(url: str, timeout_ms: int = HEADLESS_TIMEOUT_MS) -> Tuple[Optional[str], int]:
    """
    Render a page using a headless Chromium browser and return the
    fully-rendered HTML plus page load time in milliseconds.

    Returns:
        (html_content, load_time_ms) — html is None on failure
    """
    if not is_headless_available():
        return None, 0

    import time as _time
    from playwright.sync_api import sync_playwright

    html = None
    load_ms = 0

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 720},
                java_script_enabled=True,
            )
            page = context.new_page()

            start = _time.time()
            page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            page.wait_for_timeout(2000)
            load_ms = int((_time.time() - start) * 1000)

            html = page.content()
            browser.close()
    except Exception as exc:
        logger.warning("Headless render failed for %s: %s", url, exc)

    return html, load_ms


def extract_headless_signals(url: str) -> Optional[Dict]:
    """
    Render the page in a headless browser and extract signals from
    the fully-rendered DOM.

    Returns None if headless is unavailable or rendering fails.
    Returns a dict of signals (same shape as _analyze_html_content)
    when successful.
    """
    if not is_headless_available():
        return None

    html, load_ms = render_page(url)
    if not html:
        return None

    from pipeline.signals import _analyze_html_content
    signals = _analyze_html_content(html)
    signals["_headless_rendered"] = True
    signals["_headless_load_ms"] = load_ms
    return signals


def enhance_signals(url: str, existing_signals: Dict) -> Dict:
    """
    Enhance existing signals with headless browser rendering.

    Only overwrites signals that were null/unknown in the static analysis
    but could be detected after JS rendering. Never downgrades a
    confident True/False back to None.

    Returns the updated signals dict (mutated in-place for efficiency).
    """
    if not is_headless_available():
        return existing_signals

    headless = extract_headless_signals(url)
    if not headless:
        return existing_signals

    UPGRADEABLE_SIGNALS = [
        "has_contact_form",
        "has_email",
        "email_address",
        "has_automated_scheduling",
        "booking_conversion_path",
        "has_trust_badges",
        "runs_paid_ads",
        "paid_ads_channels",
        "has_schema_microdata",
        "schema_types",
        "has_social_links",
        "social_platforms",
        "has_phone_in_html",
        "has_address_in_html",
        "linkedin_company_url",
        "hiring_active",
        "hiring_roles",
        "hiring_signal_source",
    ]

    upgraded = []
    for key in UPGRADEABLE_SIGNALS:
        old_val = existing_signals.get(key)
        new_val = headless.get(key)

        if old_val is None and new_val is not None:
            existing_signals[key] = new_val
            upgraded.append(key)
        elif old_val is False and new_val is True:
            existing_signals[key] = new_val
            upgraded.append(key)

    if upgraded:
        logger.info("Headless browser upgraded %d signals for %s: %s", len(upgraded), url, upgraded)

    existing_signals["_headless_rendered"] = True
    existing_signals["_headless_upgrades"] = upgraded

    return existing_signals
