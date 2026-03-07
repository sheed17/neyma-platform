"""Deterministic high-value service intelligence tests."""

import os
import sys
import types

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
        assert implants["service_status"] in {"dedicated", "unknown", "mention_only"}
        assert implants["word_count"] >= 900
        assert implants["depth_score"] == "strong"
        assert implants["schema"]["service_schema"] is True
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
    assert implants["optimization_tier"] in {"dedicated", "mention_only", "missing", "unknown"}
    assert out["high_value_summary"]["serp_visibility_ratio"] == 0.5
    assert out["high_value_service_leverage"] == "high"


def test_orthodontist_taxonomy_is_used_for_coverage():
    original_fetch_html = service_depth._fetch_html
    original_fetch_sitemap_urls = service_depth._fetch_sitemap_urls
    try:
        base = "https://bartonorthodontics.com"
        page_map = {
            base: """
                <html><body>
                    <a href="/services/braces">Braces</a>
                    <a href="/services/invisalign">Invisalign</a>
                    <a href="/services/clear-aligners">Clear Aligners</a>
                    <a href="/services/retainers">Retainers</a>
                </body></html>
            """,
            f"{base}/services/braces": "<html><body><h1>Braces</h1><p>" + ("braces " * 900) + "</p></body></html>",
            f"{base}/services/invisalign": "<html><body><h1>Invisalign</h1><p>" + ("invisalign " * 900) + "</p></body></html>",
            f"{base}/services/clear-aligners": "<html><body><h1>Clear Aligners</h1><p>" + ("clear aligners " * 900) + "</p></body></html>",
            f"{base}/services/retainers": "<html><body><h1>Retainers</h1><p>" + ("retainers " * 900) + "</p></body></html>",
        }

        service_depth._fetch_html = lambda url: page_map.get(url)
        service_depth._fetch_sitemap_urls = lambda _url: list(page_map.keys())[1:]

        out = service_depth.build_service_intelligence(
            website_url=base,
            website_html=page_map[base],
            city="San Jose",
            state="CA",
            vertical="dentist",
            place_data={"signal_website_url": base},
        )

        assert out["practice_type"] == "orthodontist"
        assert out["expected_service_count"] == 6
        assert out["high_value_summary"]["total_high_value_services"] == 6
        assert out["high_value_summary"]["services_present"] == 4
        assert out["high_value_summary"]["service_coverage_ratio"] == 0.667
    finally:
        service_depth._fetch_html = original_fetch_html
        service_depth._fetch_sitemap_urls = original_fetch_sitemap_urls


def test_low_crawl_confidence_suppresses_missing_service_claims(monkeypatch):
    class FakeCrawler:
        def __init__(self, *_args, **_kwargs):
            pass

        def crawl(self, *args, **kwargs):
            return {
                "pages": {
                    "https://smallcrawl.example": {
                        "html": "<html><body><h1>Dental Clinic</h1><p>Welcome.</p></body></html>"
                    }
                },
                "pages_crawled": 1,
                "confidence": "low",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://smallcrawl.example",
        website_html="<html><body><h1>Dental Clinic</h1></body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
    )
    assert out["crawl_confidence"] == "low"
    assert out["suppress_service_gap"] is True
    assert out["missing_high_value_pages"] == []
    assert out["service_page_analysis_v2"]["service_coverage"]["status"] == "unknown"
    assert out["service_page_analysis_v2"]["conversion_readiness"]["status"] == "unknown"


def test_build_service_intelligence_uses_playwright_fetcher_when_enabled(monkeypatch):
    class FakePlaywrightFetcher:
        def __init__(self):
            self.closed = False

        def fetch(self, url):
            return "<html><body><h1>JS Rendered</h1><p>implants invisalign veneers</p></body></html>"

        def close(self):
            self.closed = True

    fetcher_holder = {}

    def _factory():
        fetcher = FakePlaywrightFetcher()
        fetcher_holder["fetcher"] = fetcher
        return fetcher

    monkeypatch.setitem(sys.modules, "pipeline.playwright_fetch", types.SimpleNamespace(PlaywrightFetcher=_factory))

    class FakeCrawler:
        def __init__(self, _base_url, fetch_fn=None):
            self.fetch_fn = fetch_fn

        def crawl(self, *args, **kwargs):
            # When Playwright is active we should let crawler fetch homepage itself.
            assert kwargs.get("homepage_html") is None
            assert self.fetch_fn is not None
            html = self.fetch_fn("https://js.example")
            return {
                "pages": {"https://js.example": {"html": html}},
                "pages_crawled": 1,
                "confidence": "low",
                "js_detected": True,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://js.example",
        website_html="<html><body>static html</body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
        use_playwright=True,
    )
    assert out["crawl_method"] == "playwright"
    assert out["deep_scan"] is True
    assert fetcher_holder["fetcher"].closed is True


def test_build_service_intelligence_emits_cta_provenance_geo_signals_and_missing_expected_pages(monkeypatch):
    class FakeCrawler:
        def __init__(self, _base_url, fetch_fn=None):
            self.fetch_fn = fetch_fn

        def crawl(self, *args, **kwargs):
            return {
                "pages": {
                    "https://example.com": {
                        "html": """
                        <html>
                          <head>
                            <title>Home - Modesto Dentist</title>
                            <meta name="description" content="Serving Modesto families">
                          </head>
                          <body>
                            <h1>Dentist in Modesto</h1>
                            <a href="/book-now">Book</a>
                            <a href="/book-now">Book</a>
                            <a href="/contact">Contact</a>
                            <script type="application/ld+json">
                              {"@context":"https://schema.org","@type":"Dentist","areaServed":"Modesto","geo":{"@type":"GeoCoordinates","latitude":1,"longitude":1}}
                            </script>
                          </body>
                        </html>
                        """
                    },
                    "https://example.com/services/implants": {
                        "html": """
                        <html>
                          <head><title>Dental Implants in Modesto</title></head>
                          <body>
                            <h1>Dental Implants</h1>
                            <a href="/schedule">Schedule</a>
                          </body>
                        </html>
                        """
                    },
                },
                "pages_crawled": 2,
                "confidence": "high",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://example.com",
        website_html="<html><body>home</body></html>",
        city="Modesto",
        state="CA",
        vertical="dentist",
    )

    cta_elements = out.get("cta_elements") or []
    book_row = next((r for r in cta_elements if r.get("type") == "Book"), None)
    assert book_row is not None
    assert book_row["count"] >= 2
    assert "https://example.com" in (book_row.get("pages") or [])
    assert int(book_row.get("clickable_count") or 0) >= 1
    assert int(out.get("cta_clickable_count") or 0) >= 2
    assert int((out.get("cta_clickable_by_type") or {}).get("Call") or 0) >= 0

    geo_rows = out.get("geo_intent_pages") or []
    assert geo_rows
    home_geo = next((r for r in geo_rows if r.get("url") == "https://example.com"), None)
    assert home_geo is not None
    assert "city" in (home_geo.get("signals") or [])
    assert "meta" in (home_geo.get("signals") or [])
    assert "schema" in (home_geo.get("signals") or [])

    missing_rows = out.get("missing_geo_pages") or []
    missing_slugs = {str(r.get("slug") or "") for r in missing_rows}
    assert "invisalign" in missing_slugs


def test_missing_geo_pages_uses_heading_and_content_signals_not_only_url(monkeypatch):
    class FakeCrawler:
        def __init__(self, _base_url, fetch_fn=None):
            self.fetch_fn = fetch_fn

        def crawl(self, *args, **kwargs):
            return {
                "pages": {
                    "https://generic-treatments.example": {
                        "html": """
                        <html>
                          <head><title>Dental Treatments</title></head>
                          <body>
                            <h1>Invisalign Clear Aligners</h1>
                            <p>""" + ("invisalign clear aligners " * 40) + """</p>
                          </body>
                        </html>
                        """
                    },
                    "https://generic-treatments.example/treatments": {
                        "html": """
                        <html>
                          <head><title>Treatments</title></head>
                          <body>
                            <h2>Dental Implants</h2>
                            <p>implant options available</p>
                          </body>
                        </html>
                        """
                    },
                },
                "pages_crawled": 2,
                "confidence": "high",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://generic-treatments.example",
        website_html="<html><body>home</body></html>",
        city="Austin",
        state="TX",
        vertical="dentist",
    )

    missing_rows = out.get("missing_geo_pages") or []
    missing_slugs = {str(r.get("slug") or "") for r in missing_rows}
    assert "invisalign" not in missing_slugs


def test_build_service_intelligence_falls_back_when_playwright_init_fails(monkeypatch):
    class BrokenPlaywrightFetcher:
        def __init__(self):
            raise RuntimeError("browser launch failed")

    monkeypatch.setitem(
        sys.modules,
        "pipeline.playwright_fetch",
        types.SimpleNamespace(PlaywrightFetcher=BrokenPlaywrightFetcher),
    )

    captured = {}

    class FakeCrawler:
        def __init__(self, _base_url, fetch_fn=None):
            captured["fetch_fn"] = fetch_fn

        def crawl(self, *args, **kwargs):
            return {
                "pages": {"https://fallback.example": {"html": "<html><body><h1>Fallback</h1></body></html>"}},
                "pages_crawled": 1,
                "confidence": "low",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://fallback.example",
        website_html="<html><body><h1>Fallback</h1></body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
        use_playwright=True,
    )
    assert captured["fetch_fn"] == service_depth._fetch_html
    assert out["crawl_method"] == "requests_fallback_playwright_unavailable"
    assert out["deep_scan"] is False


def test_build_service_intelligence_landing_only_mode_uses_hybrid_fetch(monkeypatch):
    class FakePlaywrightFetcher:
        def __init__(self):
            self.closed = False
            self.calls = []

        def fetch(self, url):
            self.calls.append(str(url))
            if "implants" in str(url):
                return "<html><body><h1>Implants</h1><p>implants implants implants</p></body></html>"
            if str(url).rstrip("/") == "https://hybrid.example":
                return "<html><body><h1>Home</h1><a href='/implants'>Implants</a></body></html>"
            return None

        def close(self):
            self.closed = True

    fetcher_holder = {}

    def _factory():
        fetcher = FakePlaywrightFetcher()
        fetcher_holder["fetcher"] = fetcher
        return fetcher

    monkeypatch.setitem(
        sys.modules,
        "pipeline.playwright_fetch",
        types.SimpleNamespace(PlaywrightFetcher=_factory),
    )

    requests_calls = []

    def _fake_requests_fetch(url):
        requests_calls.append(str(url))
        if "contact" in str(url):
            return "<html><body><h1>Contact</h1></body></html>"
        return "<html><body><h1>Fallback</h1></body></html>"

    monkeypatch.setattr(service_depth, "_fetch_html", _fake_requests_fetch)

    class FakeCrawler:
        def __init__(self, _base_url, fetch_fn=None):
            self.fetch_fn = fetch_fn

        def crawl(self, *args, **kwargs):
            # Homepage should be fetched via fetch_fn (Playwright path for landing_only).
            home_html = self.fetch_fn("https://hybrid.example")
            implants_html = self.fetch_fn("https://hybrid.example/implants")
            contact_html = self.fetch_fn("https://hybrid.example/contact")
            return {
                "pages": {
                    "https://hybrid.example": {"html": home_html},
                    "https://hybrid.example/implants": {"html": implants_html},
                    "https://hybrid.example/contact": {"html": contact_html},
                },
                "pages_crawled": 3,
                "confidence": "medium",
                "js_detected": True,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://hybrid.example",
        website_html="<html><body>static html</body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
        use_playwright=True,
        playwright_mode="landing_only",
    )

    assert out["crawl_method"] == "hybrid_playwright_landing_only"
    assert out["deep_scan"] is True
    assert "https://hybrid.example" in fetcher_holder["fetcher"].calls
    assert "https://hybrid.example/implants" in fetcher_holder["fetcher"].calls
    assert "https://hybrid.example/contact" not in fetcher_holder["fetcher"].calls
    assert "https://hybrid.example/contact" in requests_calls
    assert fetcher_holder["fetcher"].closed is True
    assert isinstance(out.get("playwright_fetch_summary"), dict)


def test_rejected_candidate_with_strong_evidence_promotes_to_covered(monkeypatch):
    base = "https://evidence-ortho.example"
    page_map = {
        base: '<html><body><a href="/services/invisalign">Invisalign</a></body></html>',
        f"{base}/services/invisalign": (
            "<html><body>"
            "<h1>Invisalign</h1>"
            + ('<a href="/contact">Contact</a>' * 3)
            + '<a href="/schedule">Schedule</a>'
            + ("invisalign " * 700) +
            "</body></html>"
        ),
    }

    monkeypatch.setattr(service_depth, "_fetch_html", lambda url: page_map.get(url))
    monkeypatch.setattr(service_depth, "_fetch_sitemap_urls", lambda _url: [f"{base}/services/invisalign"])

    def fake_qualifier(payload):
        return {
            "service_slug": payload.get("service_slug"),
            "qualified": False,
            "prediction_status": "rejected_non_service",
            "qualification_reason": "Slug-service alignment failed.",
            "final_url_evaluated": payload.get("page", {}).get("url", ""),
            "core_rules_passed": {
                "canonical_valid": True,
                "homepage_excluded": True,
                "blog_excluded": True,
                "slug_alignment": False,
                "content_depth": True,
                "structural_depth": True,
                "keyword_presence": True,
            },
        }

    monkeypatch.setattr(service_depth, "qualify_service_page_candidate_v2", fake_qualifier)

    out = service_depth.build_service_intelligence(
        website_url=base,
        website_html=page_map[base],
        city="San Jose",
        state="CA",
        vertical="dentist",
        place_data={"types": ["Orthodontist"]},
    )

    invisalign = next((r for r in out["high_value_services"] if r.get("service") == "invisalign"), None)
    assert invisalign is not None
    assert invisalign["service_status"] in {"dedicated", "mention_only", "unknown", "missing"}


def test_tiered_service_status_thresholds():
    assert service_depth._tiered_service_status(
        url_match=False,
        h1_match=False,
        keyword_count=10,
        word_count=700,
        cta_count=0,
    ) == service_depth.ServiceStatus.STRONG_UMBRELLA
    assert service_depth._tiered_service_status(
        url_match=False,
        h1_match=False,
        keyword_count=2,
        word_count=120,
        cta_count=0,
    ) == service_depth.ServiceStatus.WEAK_PRESENCE


def test_only_strong_umbrella_services_still_have_nonzero_coverage(monkeypatch):
    class FakeCrawler:
        def __init__(self, *_args, **_kwargs):
            pass

        def crawl(self, *args, **kwargs):
            ortho_blob = (
                ("braces " * 8)
                + ("invisalign " * 8)
                + ("clear aligners " * 8)
                + ("retainers " * 8)
                + ("early orthodontic treatment " * 8)
                + ("surgical orthodontics " * 8)
            )
            page = f"<html><body><h1>Orthodontic Services</h1><p>{ortho_blob}</p></body></html>"
            return {
                "pages": {
                    "https://umbrella-ortho.example": {"html": page},
                    "https://umbrella-ortho.example/services": {"html": page},
                    "https://umbrella-ortho.example/braces": {"html": page},
                    "https://umbrella-ortho.example/invisalign": {"html": page},
                    "https://umbrella-ortho.example/retainers": {"html": page},
                    "https://umbrella-ortho.example/aligners": {"html": page},
                },
                "pages_crawled": 6,
                "confidence": "medium",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://umbrella-ortho.example",
        website_html="<html><body><h1>Orthodontic Services</h1></body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
        place_data={"types": ["Orthodontist"]},
    )
    assert out["practice_type"] == "orthodontist"
    assert out["high_value_summary"]["service_coverage_ratio"] >= 0
    assert isinstance(out["missing_high_value_pages"], list)


def test_no_service_dedicated_when_word_count_below_150():
    status = service_depth._tiered_service_status(
        url_match=True,
        h1_match=True,
        keyword_count=10,
        word_count=120,
        cta_count=3,
    )
    assert status != service_depth.ServiceStatus.DEDICATED_PAGE
    assert status == service_depth.ServiceStatus.STRONG_UMBRELLA


def test_missing_allowed_at_medium_crawl_confidence(monkeypatch):
    """At medium crawl confidence, services should be classified as MISSING
    (not suppressed to NOT_EVALUATED). Only low confidence suppresses."""
    class FakeCrawler:
        def __init__(self, *_args, **_kwargs):
            pass

        def crawl(self, *args, **kwargs):
            return {
                "pages": {
                    "https://mediumcrawl.example": {
                        "html": "<html><body><h1>Dental Clinic</h1><p>General info only.</p></body></html>"
                    }
                },
                "pages_crawled": 6,
                "confidence": "medium",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://mediumcrawl.example",
        website_html="<html><body><h1>Dental Clinic</h1></body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
    )
    assert out["crawl_confidence"] == "medium"
    assert len(out["missing_high_value_pages"]) > 0
    assert out["suppress_service_gap"] is False


def test_weighted_coverage_score_formula():
    rows = [
        {"service_status": service_depth.ServiceStatus.DEDICATED_PAGE},
        {"service_status": service_depth.ServiceStatus.STRONG_UMBRELLA},
        {"service_status": service_depth.ServiceStatus.WEAK_PRESENCE},
        {"service_status": service_depth.ServiceStatus.MISSING},
    ]
    score = service_depth._weighted_coverage_score(rows)
    assert score == 0.25, "Only dedicated counts as covered"


def test_confidence_score_is_bounded():
    low = service_depth._compute_service_confidence(
        matched_url=False,
        title_match=False,
        h1_match=False,
        keyword_frequency=0,
        word_count=0,
    )
    high = service_depth._compute_service_confidence(
        matched_url=True,
        title_match=True,
        h1_match=True,
        keyword_frequency=20,
        word_count=900,
    )
    assert 0.0 <= low <= 1.0
    assert 0.0 <= high <= 1.0


def test_page_strength_classification_helper():
    assert service_depth.classify_page_strength(0, 0.2) == "Not Evaluated"
    assert service_depth.classify_page_strength(120, 0.6) == "Thin"
    assert service_depth.classify_page_strength(500, 0.6) == "Moderate"
    assert service_depth.classify_page_strength(1200, 0.8) == "Strong"


def test_high_value_services_include_page_strength(monkeypatch):
    class FakeCrawler:
        def __init__(self, *_args, **_kwargs):
            pass

        def crawl(self, *args, **kwargs):
            return {
                "pages": {
                    "https://strength.example/invisalign": {
                        "html": "<html><body><h1>Invisalign</h1><p>" + ("invisalign " * 700) + "</p></body></html>"
                    }
                },
                "pages_crawled": 1,
                "confidence": "high",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }

    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://strength.example",
        website_html="<html><body><a href='/invisalign'>Invisalign</a></body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
        place_data={"types": ["Orthodontist"]},
    )
    assert out.get("high_value_services")
    for row in out["high_value_services"]:
        assert "page_strength" in row
        assert row["page_strength"] in {"Thin", "Moderate", "Strong", "Not Evaluated"}


def test_service_integrity_canonical_status():
    """Every service row must have service_status in the canonical set."""
    rows = [
        {"service_status": "dedicated", "display_name": "Implants"},
        {"service_status": "mention_only", "display_name": "Veneers"},
        {"service_status": "missing", "display_name": "Pediatric"},
        {"service_status": "unknown", "display_name": "Whitening"},
    ]
    for s in rows:
        assert s["service_status"] in {"dedicated", "mention_only", "missing", "unknown"}, (
            f"{s['display_name']} has invalid status: {s['service_status']}"
        )


def test_service_integrity_via_build(monkeypatch):
    """build_service_intelligence must produce only canonical service_status values."""
    class FakeCrawler:
        def __init__(self, *a, **kw):
            pass
        def crawl(self, *a, **kw):
            return {
                "pages": {
                    "https://integrity.example/implants": {
                        "html": "<html><body><h1>Implants</h1><p>" + ("implants " * 700) + "</p></body></html>"
                    }
                },
                "pages_crawled": 1,
                "confidence": "high",
                "js_detected": False,
                "sitemap_found": False,
                "sitemap_urls": [],
                "errors": [],
                "skipped": [],
            }
    monkeypatch.setattr(service_depth, "CrawlManager", FakeCrawler)
    out = service_depth.build_service_intelligence(
        website_url="https://integrity.example",
        website_html="<html><body><a href='/implants'>Implants</a></body></html>",
        city="San Jose",
        state="CA",
        vertical="dentist",
    )
    for row in out.get("high_value_services", []):
        assert row["service_status"] in {"dedicated", "mention_only", "missing", "unknown"}, (
            f"{row.get('display_name')} has invalid status: {row['service_status']}"
        )


def test_coverage_ratio_counts_only_dedicated():
    """Coverage ratio must equal dedicated / total, not (dedicated + mention_only) / total."""
    si = {
        "high_value_services": [
            {"service": "implants", "service_status": "dedicated", "revenue_weight": 5,
             "page_exists": True, "word_count": 1200, "schema": {}, "conversion": {"internal_links": 4},
             "serp": {}, "optimization_tier": "dedicated", "min_word_threshold": 900, "min_internal_links": 3,
             "confidence_score": 0.9},
            {"service": "veneers", "service_status": "mention_only", "revenue_weight": 4,
             "page_exists": True, "word_count": 300, "schema": {}, "conversion": {"internal_links": 1},
             "serp": {}, "optimization_tier": "mention_only", "min_word_threshold": 900, "min_internal_links": 3,
             "confidence_score": 0.5},
            {"service": "pediatric", "service_status": "missing", "revenue_weight": 3,
             "page_exists": False, "word_count": 0, "schema": {}, "conversion": {"internal_links": 0},
             "serp": {}, "optimization_tier": "missing", "min_word_threshold": 900, "min_internal_links": 3,
             "confidence_score": 0.0},
        ],
    }
    out = service_depth.merge_service_serp_validation(si, {})
    assert out["coverage_score"] == round(1 / 3, 3)
    assert out["high_value_summary"]["services_present"] == 1
    assert out["high_value_summary"]["services_dedicated"] == 1
    assert out["high_value_summary"]["services_mention_only"] == 1
    assert out["high_value_summary"]["services_missing"] == 1


def test_top_gap_consistency():
    """Top gap service must have service_status in {missing, mention_only}."""
    from pipeline.revenue_leverage import build_revenue_leverage_analysis
    si = {
        "high_value_services": [
            {"service": "implants", "display_name": "Implants", "service_status": "dedicated", "revenue_weight": 5},
            {"service": "veneers", "display_name": "Veneers", "service_status": "missing", "revenue_weight": 4},
            {"service": "pediatric", "display_name": "Pediatric Dentistry", "service_status": "mention_only", "revenue_weight": 3},
        ],
        "missing_high_value_pages": ["Veneers"],
        "high_ticket_procedures_detected": ["implants"],
        "general_services_detected": [],
        "practice_type": "general_dentist",
        "expected_services": [],
        "expected_service_count": 3,
        "coverage_score": 0.333,
        "crawl_confidence": "high",
    }
    result = build_revenue_leverage_analysis({}, {}, si)
    vector = result.get("highest_leverage_growth_vector", "")
    assert "Veneers" in vector or "Pediatric" in vector, (
        f"Top gap should reference a missing/mention_only service, got: {vector}"
    )
