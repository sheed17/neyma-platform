"""Strict dedicated-page detection tests (umbrella exclusion + strict lightweight path)."""

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline import service_depth
from backend.services import npl_service


def test_umbrella_homepage_does_not_count_as_dedicated_page():
    original_fetch_html = service_depth._fetch_html
    original_fetch_sitemap_urls = service_depth._fetch_sitemap_urls
    try:
        base = "https://strict-demo-dental.com"
        homepage = """
        <html><head><title>Dental Services</title></head><body>
          <h1>All Services</h1>
          <ul>
            <li>Implants</li><li>Invisalign</li><li>Veneers</li><li>Cosmetic Dentistry</li>
            <li>Emergency</li><li>Crowns</li><li>Whitening</li><li>Root Canal</li>
          </ul>
          <a href="/invisalign">Invisalign page</a>
          <p>implants invisalign veneers cosmetic emergency crowns whitening root canal</p>
        </body></html>
        """
        invisalign_page = """
        <html><head><title>Invisalign in San Jose</title></head><body>
          <h1>Invisalign</h1>
          <h2>Invisalign Benefits</h2>
          <h2>Invisalign Process</h2>
          <section>FAQ</section>
          <p>""" + ("invisalign " * 1200) + """</p>
        </body></html>
        """
        page_map = {
            base: homepage,
            f"{base}/invisalign": invisalign_page,
        }

        service_depth._fetch_html = lambda url: page_map.get(url)
        service_depth._fetch_sitemap_urls = lambda _url: [f"{base}/invisalign"]

        out = service_depth.build_service_intelligence(
            website_url=base,
            website_html=homepage,
            city="San Jose",
            state="CA",
            vertical="dentist",
        )
        assert out["service_page_detection_debug"]["umbrella_detection_triggered"] is True

        implants = next(r for r in out["high_value_services"] if r["service"] == "implants")
        assert implants["qualification_status"] == "missing"
        assert "umbrella" in implants["detection_reason"].lower()
        assert implants["page_detected"] is False

        invisalign = next(r for r in out["high_value_services"] if r["service"] == "invisalign")
        assert invisalign["page_detected"] is True
        assert invisalign["qualification_status"] in {"strong", "moderate"}
        assert invisalign["h1_match"] is True
        assert invisalign["word_count"] >= 500
    finally:
        service_depth._fetch_html = original_fetch_html
        service_depth._fetch_sitemap_urls = original_fetch_sitemap_urls


def test_lightweight_missing_service_uses_strict_detector(monkeypatch):
    monkeypatch.setattr(
        service_depth,
        "run_strict_single_service_page_check",
        lambda **kwargs: {
            "service": "implants",
            "matches": True,
            "reason": "Service only mentioned within umbrella page.",
            "page_detected": False,
            "qualification_status": "missing",
            "detection_reason": "Service only mentioned within umbrella page.",
            "url": None,
            "word_count": 0,
            "keyword_density": 0.0,
            "h1_match": False,
            "structural_signals": {
                "faq_present": False,
                "service_h2_sections": 0,
                "financing_section": False,
                "before_after_section": False,
            },
            "internal_links_to_page": 0,
            "debug": {"umbrella_detection_triggered": True},
        },
    )
    out = npl_service.run_lightweight_service_page_check(
        website="https://strict-demo-dental.com",
        criterion={"type": "missing_service_page", "service": "implants"},
    )
    assert out["matches"] is True
    assert out["qualification_status"] == "missing"
    assert out["dedicated_page_detected"] is False
    assert "umbrella" in str(out["reason"]).lower()

