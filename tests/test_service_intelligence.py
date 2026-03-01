"""Deterministic high-value service intelligence tests."""

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline import service_depth


def test_build_service_intelligence_outputs_structured_high_value_rows():
    original_fetch_html = service_depth._fetch_html
    original_fetch_sitemap_urls = service_depth._fetch_sitemap_urls
    try:
        base = "https://exampledental.com"
        page_map = {
            base: """
                <html><body>
                    <a href="/services/dental-implants">Implants</a>
                    <a href="/services/invisalign">Invisalign</a>
                </body></html>
            """,
            f"{base}/services/dental-implants": """
                <html>
                  <head>
                    <script type="application/ld+json">
                      {"@context":"https://schema.org","@type":"Service","name":"Dental Implants"}
                    </script>
                  </head>
                  <body>
                    <h1>Dental Implants in San Jose</h1>
                    <a href="/contact">Contact</a><a href="/book-now">Book</a><a href="/financing">Financing</a>
                    <section>FAQ</section>
                    <p>""" + ("implants " * 1200) + """</p>
                  </body>
                </html>
            """,
            f"{base}/services/invisalign": """
                <html>
                  <body>
                    <h1>Invisalign</h1>
                    <a href="/contact">Contact</a>
                    <p>""" + ("invisalign " * 650) + """</p>
                  </body>
                </html>
            """,
        }

        def fake_fetch_html(url):
            return page_map.get(url)

        def fake_sitemap_urls(_):
            return [f"{base}/services/dental-implants", f"{base}/services/invisalign"]

        service_depth._fetch_html = fake_fetch_html
        service_depth._fetch_sitemap_urls = fake_sitemap_urls

        out = service_depth.build_service_intelligence(
            website_url=base,
            website_html=page_map[base],
            city="San Jose",
            state="CA",
            vertical="dentist",
        )

        assert isinstance(out.get("high_value_services"), list)
        assert out["high_value_summary"]["total_high_value_services"] > 0

        implants = next((r for r in out["high_value_services"] if r.get("service") == "implants"), None)
        assert implants is not None
        assert implants["page_exists"] is True
        assert implants["word_count"] >= 900
        assert implants["depth_score"] == "strong"
        assert implants["schema"]["service_schema"] is True
        assert "inbound_unique_pages_body_only" in implants
        assert "wiring_status" in implants
        assert "optimization_tier" in implants
    finally:
        service_depth._fetch_html = original_fetch_html
        service_depth._fetch_sitemap_urls = original_fetch_sitemap_urls


def test_merge_service_serp_validation_recomputes_tiers_and_summary():
    service_intelligence = {
        "high_value_services": [
            {
                "service": "implants",
                "revenue_weight": 5,
                "page_exists": True,
                "word_count": 1200,
                "schema": {"service_schema": True, "faq_schema": False, "localbusiness_schema": False},
                "conversion": {"internal_links": 4},
                "serp": {},
                "optimization_tier": "moderate",
                "min_word_threshold": 900,
                "min_internal_links": 3,
            },
            {
                "service": "veneers",
                "revenue_weight": 4,
                "page_exists": False,
                "word_count": 0,
                "schema": {"service_schema": False, "faq_schema": False, "localbusiness_schema": False},
                "conversion": {"internal_links": 0},
                "serp": {},
                "optimization_tier": "missing",
                "min_word_threshold": 900,
                "min_internal_links": 3,
            },
        ],
    }
    rankings = {
        "implants": {
            "position_top_3": True,
            "position_top_10": True,
            "average_position": 2,
            "map_pack_presence": True,
            "competitors_in_top_10": 7,
        }
    }

    out = service_depth.merge_service_serp_validation(service_intelligence, rankings)
    implants = next(r for r in out["high_value_services"] if r["service"] == "implants")
    assert implants["serp"]["position_top_10"] is True
    assert implants["optimization_tier"] == "strong"
    assert out["high_value_summary"]["serp_visibility_ratio"] == 0.5
    assert out["high_value_service_leverage"] == "high"
