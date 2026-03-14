import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.ml.feature_builder import build_tier1_feature_vector, build_tier2_feature_vector
from backend.ml.labeler import generate_lead_quality_label
from backend.ml.runtime import score_territory_row, score_diagnostic_response


def test_tier1_feature_vector_shape():
    row = {
        "rating": 4.2,
        "user_ratings_total": 42,
        "avg_market_reviews": 118,
        "avg_market_rating": 4.5,
        "has_website": True,
        "ssl": False,
        "has_contact_form": False,
        "has_phone": True,
        "has_viewport": True,
        "has_schema": False,
        "market_candidate_count": 20,
        "market_density": "high",
    }
    features = build_tier1_feature_vector(row)
    assert features["review_gap_pct"] > 0
    assert features["website_quality_score_t1"] >= 0
    assert features["feature_scope"] == "tier1"
    assert 0 <= features["signal_completeness_ratio"] <= 1


def test_tier2_label_good_for_investable_gap():
    response = {
        "website": "https://example.com",
        "phone": "+1-555-555-1212",
        "market_density": "high",
        "paid_status": "Active",
        "service_intelligence": {
            "missing_services": ["implants", "invisalign"],
            "detected_services": ["emergency", "crowns"],
            "pages_crawled": 8,
            "crawl_confidence": "high",
            "service_page_count": 4,
            "cta_elements": [{"type": "Contact", "count": 2, "pages": ["/"]}],
            "cta_clickable_count": 2,
            "geo_intent_pages": [{"url": "/san-jose", "title": "San Jose Dentist", "signals": ["city"], "hasCTA": True}],
            "missing_geo_pages": [{"slug": "campbell", "title": "Campbell Dentist", "priority": "high", "reason": "missing"}],
            "schema_detected": True,
        },
        "conversion_infrastructure": {
            "online_booking": False,
            "contact_form": True,
            "phone_prominent": True,
            "mobile_optimized": True,
            "page_load_ms": 2800,
        },
        "brief": {
            "market_position": {"reviews": "42", "local_avg": "118", "market_density": "high"},
            "demand_signals": {"google_ads_line": "Active", "paid_channels_detected": ["google_ads"], "review_velocity_30d": 2.5},
            "market_saturation": {"top_5_avg_reviews": 165, "competitor_median_reviews": 110},
            "geo_coverage": {"city_or_near_me_page_count": 1},
            "competitive_delta": {"competitors_sampled": 12},
        },
        "competitors": [{"isYou": True, "reviews": 42, "rating": 4.2}],
        "local_avg_rating": 4.5,
    }
    features = build_tier2_feature_vector(response)
    label = generate_lead_quality_label(features)
    assert label["lead_quality_class_heuristic_v1"] in {"good", "decent"}
    assert label["benefit_score_v1"] > 0.4


def test_runtime_scoring_returns_payload():
    territory_score = score_territory_row(
        {
            "rating": 4.2,
            "user_ratings_total": 42,
            "avg_market_reviews": 118,
            "avg_market_rating": 4.5,
            "has_website": True,
            "ssl": False,
            "has_contact_form": False,
            "has_phone": True,
            "has_viewport": True,
            "has_schema": False,
            "market_candidate_count": 20,
            "market_density": "high",
        }
    )
    assert territory_score["class"] in {"bad", "decent", "good"}
    assert territory_score["feature_scope"] == "tier1"
    assert isinstance(territory_score["reasons"], list)
    assert "decision_threshold" in territory_score
    assert "is_priority_prospect_predicted" in territory_score

    diagnostic_score = score_diagnostic_response(
        {
            "website": "https://example.com",
            "phone": "+1-555-555-1212",
            "market_density": "medium",
            "paid_status": "Inactive",
            "service_intelligence": {"missing_services": [], "detected_services": [], "pages_crawled": 0, "crawl_confidence": "low"},
            "conversion_infrastructure": {"online_booking": None, "contact_form": None, "phone_prominent": True, "mobile_optimized": True, "page_load_ms": None},
            "brief": {"market_position": {"reviews": "25", "local_avg": "80", "market_density": "medium"}},
            "competitors": [{"isYou": True, "reviews": 25, "rating": 4.1}],
            "local_avg_rating": 4.4,
        }
    )
    assert diagnostic_score["feature_scope"] == "tier2"
    assert "components" in diagnostic_score
    assert "decision_threshold" in diagnostic_score
    assert "is_priority_prospect_predicted" in diagnostic_score
