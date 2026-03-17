"""
Playwright-backed HTML fetcher for JS-rendered crawling.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class PlaywrightFetcher:
    """
    Reuses one headless Chromium instance for a crawl run.
    """

    def __init__(self, timeout_ms: int = 15000):
        from playwright.sync_api import sync_playwright

        self.timeout_ms = int(timeout_ms)
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)

    def fetch(self, url: str) -> Optional[str]:
        target = str(url or "").strip()
        if not target:
            return None
        if not target.startswith(("http://", "https://")):
            target = "https://" + target

        page = self._browser.new_page()
        try:
            page.goto(target, wait_until="domcontentloaded", timeout=self.timeout_ms)
            page.wait_for_load_state("networkidle", timeout=self.timeout_ms)
            return page.content()
        except Exception as exc:
            logger.warning("Playwright fetch failed for %s: %s", target[:120], exc)
            return None
        finally:
            try:
                page.close()
            except Exception:
                pass

    def close(self) -> None:
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._playwright.stop()
        except Exception:
            pass

