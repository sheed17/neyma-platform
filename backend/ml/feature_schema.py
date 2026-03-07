"""Canonical feature schema for Neyma lead-quality scoring."""

from __future__ import annotations

FEATURE_VERSION = "lead_features_v1"
LABEL_VERSION = "heuristic_lead_quality_v1"
TERRITORY_MODEL_NAME = "territory_lead_quality"
DIAGNOSTIC_MODEL_NAME = "diagnostic_lead_quality"
HEURISTIC_MODEL_VERSION = "heuristic_v1"
TRAINING_TARGET_COLUMN = "is_priority_prospect_heuristic_v1"
TRAINING_PRIORITY_SCORE_THRESHOLD = 0.55
TRAINING_PRIORITY_MIN_BENEFIT = 0.45
TRAINING_PRIORITY_MIN_BUYABILITY = 0.45

TERRITORY_FEATURE_COLUMNS = [
    "rating",
    "review_count",
    "review_count_log",
    "local_avg_reviews",
    "local_avg_reviews_log",
    "review_gap_abs",
    "review_gap_pct",
    "review_ratio_to_market",
    "local_avg_rating",
    "rating_gap_to_market",
    "has_website",
    "has_ssl",
    "has_contact_form",
    "has_phone",
    "has_viewport",
    "has_schema",
    "website_quality_score_t1",
    "website_quality_deficit_t1",
    "market_candidate_count",
    "competitor_count_radius",
    "market_pressure_score_t1",
    "market_density_ord",
    "below_review_avg",
    "below_rating_avg",
    "dominant_clinic_flag_t1",
    "distressed_clinic_flag_t1",
    "mid_market_fit_flag_t1",
    "signal_completeness_ratio",
    "data_confidence",
]

DIAGNOSTIC_FEATURE_COLUMNS = TERRITORY_FEATURE_COLUMNS + [
    "has_online_booking",
    "phone_prominent",
    "mobile_optimized",
    "page_load_ms",
    "page_speed_score",
    "website_quality_score_t2",
    "website_quality_deficit_t2",
    "service_page_count",
    "pages_crawled",
    "crawl_confidence_ord",
    "high_value_services_present_count",
    "high_value_services_missing_count",
    "high_value_service_coverage_ratio",
    "missing_implants_page",
    "missing_invisalign_page",
    "missing_veneers_page",
    "missing_cosmetic_page",
    "missing_emergency_page",
    "service_gap_weighted_score",
    "runs_google_ads",
    "runs_meta_ads",
    "paid_channels_count",
    "ads_without_booking_flag",
    "ads_without_service_depth_flag",
    "geo_intent_page_count",
    "missing_geo_page_count",
    "geo_coverage_ratio",
    "cta_count",
    "cta_clickable_count",
    "cta_clickable_ratio",
    "competitor_avg_reviews",
    "top5_avg_reviews",
    "competitor_median_reviews",
    "dominant_competitor_review_ratio",
    "market_pressure_score_t2",
    "review_velocity_30d",
    "review_velocity_bucket",
    "dominant_clinic_flag_t2",
    "distressed_clinic_flag_t2",
    "mid_market_fit_flag_t2",
    "investable_gap_flag",
    "service_scan_observed_flag",
    "conversion_scan_observed_flag",
]

ALL_FEATURE_COLUMNS = sorted(set(DIAGNOSTIC_FEATURE_COLUMNS))

HIGH_VALUE_SERVICE_WEIGHTS = {
    "implants": 1.00,
    "invisalign": 0.95,
    "veneers": 0.75,
    "cosmetic": 0.70,
    "emergency": 0.60,
    "whitening": 0.45,
    "pediatric": 0.40,
    "root_canal": 0.55,
    "crowns": 0.50,
}

POSITIVE_REASON_CODES = {
    "review_gap_high": "Large review gap versus local market",
    "website_quality_low": "Weak website quality baseline",
    "missing_implants_page": "Missing implants service page",
    "missing_invisalign_page": "Missing Invisalign service page",
    "missing_veneers_page": "Missing veneers service page",
    "missing_cosmetic_page": "Missing cosmetic dentistry service page",
    "missing_emergency_page": "Missing emergency dentistry service page",
    "no_online_booking": "No online booking detected",
    "ads_running_with_conversion_gap": "Paid demand appears to leak into a weak conversion path",
    "ads_running_with_service_gap": "Paid demand appears to hit shallow service coverage",
    "high_market_pressure": "Competitive local market increases upside from stronger capture",
    "mid_market_fit": "Clinic sits in the investable middle of the market",
}

NEGATIVE_REASON_CODES = {
    "dominant_clinic_penalty": "Clinic already appears strong relative to market",
    "distressed_clinic_penalty": "Clinic may be too weak operationally for strong buyer fit",
    "low_review_count_penalty": "Very low review base weakens buyer fit",
    "low_rating_penalty": "Low rating weakens buyer fit",
    "weak_market_viability_penalty": "Market viability looks weaker than top outreach targets",
    "low_signal_completeness_penalty": "Signals were incomplete, lowering confidence",
}

CAVEAT_CODES = {
    "partial_site_observability": "Some website signals were only partially observed",
    "low_service_observability": "Service depth was observed with limited confidence",
    "unknown_page_speed": "Page speed was not observed",
    "limited_market_context": "Market context was limited for this clinic",
}
