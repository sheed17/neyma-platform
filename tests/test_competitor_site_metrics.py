import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline import competitor_sampling as cs


def test_enrich_competitors_with_site_metrics_populates_fields(monkeypatch):
    class _FakeEnricher:
        def get_place_details(self, place_id, fields=None):
            if place_id == "p1":
                return {"website": "https://one.example.com"}
            if place_id == "p2":
                return {"website": "https://two.example.com"}
            return {}

    def _fake_compute_lightweight_site_metrics(website_url, page_timeout_sec=3.0, max_pages=6):
        if "one.example.com" in website_url:
            return {
                "service_page_count": 3,
                "pages_with_schema": 2,
                "avg_word_count": 850,
            }
        return {
            "service_page_count": 1,
            "pages_with_schema": 0,
            "avg_word_count": 420,
        }

    monkeypatch.setenv("NEYMA_ENABLE_COMPETITOR_SITE_CRAWL", "true")
    monkeypatch.setenv("NEYMA_COMPETITOR_SITE_CRAWL_MAX", "2")
    monkeypatch.setattr("pipeline.enrich.PlaceDetailsEnricher", _FakeEnricher)
    monkeypatch.setattr(cs, "_compute_lightweight_site_metrics", _fake_compute_lightweight_site_metrics)

    comps = [
        {"place_id": "p1", "name": "C1", "reviews": 100},
        {"place_id": "p2", "name": "C2", "reviews": 90},
        {"place_id": "p3", "name": "C3", "reviews": 80},
    ]
    out = cs.enrich_competitors_with_site_metrics(comps, vertical="dentist")
    assert len(out) == 3

    assert out[0]["service_page_count"] == 3
    assert out[0]["pages_with_schema"] == 2
    assert out[0]["avg_word_count"] == 850.0
    assert out[0]["competitor_site_metric_status"] == "ok"

    assert out[1]["service_page_count"] == 1
    assert out[1]["pages_with_schema"] == 0
    assert out[1]["avg_word_count"] == 420.0
    assert out[1]["competitor_site_metric_status"] == "ok"

    # Beyond crawl max, should not be attempted
    assert out[2]["competitor_site_metric_status"] == "not_attempted"
    assert out[2].get("service_page_count") is None


def test_build_competitive_snapshot_counts_competitor_sites_checked():
    competitors = [
        {"name": "A", "reviews": 120, "rating": 4.8, "distance_miles": 0.4, "service_page_count": 3},
        {"name": "B", "reviews": 100, "rating": 4.6, "distance_miles": 0.7, "service_page_count": 0},
        {"name": "C", "reviews": 80, "rating": 4.5, "distance_miles": 0.9},
    ]
    lead = {"signal_review_count": 50}
    snap = cs.build_competitive_snapshot(lead, competitors, search_radius_used_miles=2)
    assert snap["competitors_with_website_checked"] == 2


def test_has_schema_strict_ignores_generic_jsonld():
    html = """
    <html><head>
      <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"Organization","name":"Generic Org"}
      </script>
    </head><body></body></html>
    """
    assert cs._has_schema(html) is False


def test_has_schema_strict_accepts_service_and_faq():
    html = """
    <html><head>
      <script type="application/ld+json">
        {
          "@context":"https://schema.org",
          "@graph":[
            {"@type":"Service","name":"Dental Implants"},
            {"@type":"FAQPage","name":"FAQ"}
          ]
        }
      </script>
    </head><body></body></html>
    """
    assert cs._has_schema(html) is True
