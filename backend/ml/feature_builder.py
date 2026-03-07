"""Feature builders for Neyma lead-quality scoring."""

from __future__ import annotations

import math
from typing import Any, Dict, Iterable, Mapping

from .feature_schema import FEATURE_VERSION, HIGH_VALUE_SERVICE_WEIGHTS


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_bool_int(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if value in {1, "1", "true", "True", "yes", "on"}:
        return 1
    return 0


def _market_density_to_ord(value: Any) -> int:
    raw = str(value or "").strip().lower()
    if raw == "high":
        return 2
    if raw == "medium":
        return 1
    return 0


def _safe_log1p(value: float) -> float:
    return math.log1p(max(value, 0.0))


def _count_present_services(high_value_services: Iterable[Any]) -> int:
    count = 0
    for row in high_value_services or []:
        if not isinstance(row, Mapping):
            continue
        status = str(row.get("service_status") or row.get("final_verdict") or "").strip().lower()
        if status in {"present", "dedicated", "mention_only"}:
            count += 1
    return count


def _count_missing_services(missing_services: Iterable[Any]) -> int:
    return len([s for s in (missing_services or []) if str(s).strip()])


def _weighted_service_gap(missing_services: Iterable[Any], detected_services: Iterable[Any], service_observed: bool) -> float:
    missing = [str(s).strip().lower() for s in (missing_services or []) if str(s).strip()]
    detected = [str(s).strip().lower() for s in (detected_services or []) if str(s).strip()]

    all_services = set(missing) | set(detected)
    if not all_services:
        return 0.0

    denom = sum(HIGH_VALUE_SERVICE_WEIGHTS.get(service, 0.50) for service in all_services)
    numer = sum(HIGH_VALUE_SERVICE_WEIGHTS.get(service, 0.50) for service in missing)
    score = numer / max(denom, 1.0)
    if not service_observed:
        score *= 0.35
    return round(_clip(score, 0.0, 1.0), 6)


def _compute_t1_quality_score(has_website: int, has_ssl: int, has_contact_form: int, has_phone: int, has_viewport: int, has_schema: int) -> float:
    if not has_website:
        return 0.0
    return round(
        100.0
        * (
            0.22 * has_ssl
            + 0.22 * has_contact_form
            + 0.22 * has_phone
            + 0.17 * has_viewport
            + 0.17 * has_schema
        ),
        3,
    )


def _page_speed_score(page_load_ms: float | None) -> float:
    if page_load_ms is None or page_load_ms <= 0:
        return 0.5
    if page_load_ms <= 2500:
        return 1.0
    if page_load_ms <= 3500:
        return 0.75
    if page_load_ms <= 5000:
        return 0.4
    return 0.1


def _compute_t2_quality_score(
    has_website: int,
    has_ssl: int,
    mobile_optimized: int,
    has_contact_form: int,
    phone_prominent: int,
    has_online_booking: int,
    has_schema: int,
    page_load_ms: float | None,
) -> float:
    if not has_website:
        return 0.0
    return round(
        100.0
        * (
            0.15 * has_ssl
            + 0.17 * mobile_optimized
            + 0.17 * has_contact_form
            + 0.16 * phone_prominent
            + 0.20 * has_online_booking
            + 0.07 * has_schema
            + 0.08 * _page_speed_score(page_load_ms)
        ),
        3,
    )


def _completeness_ratio(values: Dict[str, Any], keys: list[str]) -> float:
    observed = 0
    for key in keys:
        value = values.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        observed += 1
    return round(observed / max(len(keys), 1), 6)


def _base_market_features(
    *,
    rating: float,
    review_count: int,
    local_avg_reviews: float,
    local_avg_rating: float,
    market_density_ord: int,
    competitor_count_radius: int,
    has_website: int,
    has_ssl: int,
    has_contact_form: int,
    has_phone: int,
    has_viewport: int,
    has_schema: int,
) -> Dict[str, Any]:
    review_gap_abs = max(local_avg_reviews - review_count, 0.0)
    review_gap_pct = _clip(review_gap_abs / max(local_avg_reviews, 1.0), 0.0, 1.0)
    review_ratio_to_market = _clip(review_count / max(local_avg_reviews, 1.0), 0.0, 3.0)
    rating_gap_to_market = _clip(rating - local_avg_rating, -2.0, 2.0) if local_avg_rating else 0.0
    quality_t1 = _compute_t1_quality_score(
        has_website=has_website,
        has_ssl=has_ssl,
        has_contact_form=has_contact_form,
        has_phone=has_phone,
        has_viewport=has_viewport,
        has_schema=has_schema,
    )
    market_pressure_score_t1 = _clip(
        0.55 * min(competitor_count_radius, 50) / 50.0
        + 0.45 * (_clip(local_avg_reviews / max(review_count, 1), 0.0, 3.0) / 3.0),
        0.0,
        1.0,
    )
    return {
        "rating": round(rating, 6),
        "review_count": review_count,
        "review_count_log": round(_safe_log1p(review_count), 6),
        "local_avg_reviews": round(local_avg_reviews, 6),
        "local_avg_reviews_log": round(_safe_log1p(local_avg_reviews), 6),
        "review_gap_abs": round(review_gap_abs, 6),
        "review_gap_pct": round(review_gap_pct, 6),
        "review_ratio_to_market": round(review_ratio_to_market, 6),
        "local_avg_rating": round(local_avg_rating, 6),
        "rating_gap_to_market": round(rating_gap_to_market, 6),
        "has_website": has_website,
        "has_ssl": has_ssl,
        "has_contact_form": has_contact_form,
        "has_phone": has_phone,
        "has_viewport": has_viewport,
        "has_schema": has_schema,
        "website_quality_score_t1": quality_t1,
        "website_quality_deficit_t1": round(100.0 - quality_t1, 6),
        "market_candidate_count": competitor_count_radius,
        "competitor_count_radius": competitor_count_radius,
        "market_pressure_score_t1": round(market_pressure_score_t1, 6),
        "market_density_ord": market_density_ord,
        "below_review_avg": 1 if review_count < local_avg_reviews else 0,
        "below_rating_avg": 1 if rating < local_avg_rating else 0,
        "dominant_clinic_flag_t1": 1 if review_gap_pct <= 0.05 and rating >= 4.6 and review_ratio_to_market >= 1.0 and quality_t1 >= 80 else 0,
        "distressed_clinic_flag_t1": 1 if review_count < 10 and (rating <= 0 or rating < 3.5) and quality_t1 < 35 else 0,
        "mid_market_fit_flag_t1": 1 if 0.15 <= review_gap_pct <= 0.50 and 20 <= review_count <= 250 and rating >= 3.9 and has_website == 1 else 0,
    }


def build_tier1_feature_vector(row: Mapping[str, Any]) -> Dict[str, Any]:
    rating = _to_float(row.get("rating"))
    review_count = _to_int(row.get("user_ratings_total"))
    local_avg_reviews = _to_float(row.get("avg_market_reviews") or row.get("local_avg_reviews"))
    local_avg_rating = _to_float(row.get("avg_market_rating") or row.get("local_avg_rating"))
    competitor_count_radius = _to_int(row.get("market_candidate_count") or row.get("candidate_count") or 0)
    if competitor_count_radius <= 0:
        competitor_count_radius = max(_to_int(row.get("scored_candidates")), 0)
    market_density_ord = _market_density_to_ord(row.get("market_density"))
    if market_density_ord == 0 and local_avg_reviews >= 120:
        market_density_ord = 2
    elif market_density_ord == 0 and local_avg_reviews >= 50:
        market_density_ord = 1

    has_website = _to_bool_int(row.get("has_website"))
    has_ssl = _to_bool_int(row.get("ssl"))
    has_contact_form = _to_bool_int(row.get("has_contact_form"))
    has_phone = _to_bool_int(row.get("has_phone"))
    has_viewport = _to_bool_int(row.get("has_viewport"))
    has_schema = _to_bool_int(row.get("has_schema"))

    features = _base_market_features(
        rating=rating,
        review_count=review_count,
        local_avg_reviews=local_avg_reviews,
        local_avg_rating=local_avg_rating,
        market_density_ord=market_density_ord,
        competitor_count_radius=competitor_count_radius,
        has_website=has_website,
        has_ssl=has_ssl,
        has_contact_form=has_contact_form,
        has_phone=has_phone,
        has_viewport=has_viewport,
        has_schema=has_schema,
    )
    feature_keys = [
        "rating",
        "review_count",
        "local_avg_reviews",
        "local_avg_rating",
        "has_website",
        "has_ssl",
        "has_contact_form",
        "has_phone",
        "has_viewport",
        "has_schema",
        "competitor_count_radius",
    ]
    features.update(
        {
            "feature_scope": "tier1",
            "feature_version": FEATURE_VERSION,
            "signal_completeness_ratio": _completeness_ratio(features, feature_keys),
        }
    )
    features["data_confidence"] = round(features["signal_completeness_ratio"], 6)
    return features


def build_tier2_feature_vector(response: Mapping[str, Any]) -> Dict[str, Any]:
    service_intel = response.get("service_intelligence") or {}
    conversion = response.get("conversion_infrastructure") or {}
    brief = response.get("brief") or {}
    demand_signals = brief.get("demand_signals") or {}
    market_position = brief.get("market_position") or {}
    market_saturation = brief.get("market_saturation") or {}
    geo_coverage = brief.get("geo_coverage") or {}

    rating = _to_float(response.get("rating"))
    if rating <= 0:
        competitors = response.get("competitors") or []
        you = next((row for row in competitors if isinstance(row, Mapping) and row.get("isYou")), None)
        rating = _to_float((you or {}).get("rating"))

    review_count = _to_int(response.get("user_ratings_total"))
    mp_reviews = str(market_position.get("reviews") or "").strip()
    try:
        if review_count <= 0 and mp_reviews:
            review_count = int(mp_reviews.split()[0].replace(",", ""))
    except Exception:
        review_count = review_count or 0
    if review_count <= 0:
        competitors = response.get("competitors") or []
        you = next((row for row in competitors if isinstance(row, Mapping) and row.get("isYou")), None)
        review_count = _to_int((you or {}).get("reviews"))
    local_avg_reviews = _to_float(market_position.get("local_avg"))
    if local_avg_reviews <= 0:
        local_avg_reviews = _to_float(market_saturation.get("top_5_avg_reviews") or market_saturation.get("competitor_median_reviews"))
    local_avg_rating = _to_float(response.get("local_avg_rating"))

    market_density_ord = _market_density_to_ord(response.get("market_density") or market_position.get("market_density"))
    competitor_count_radius = _to_int((brief.get("competitive_delta") or {}).get("competitors_sampled"))

    has_website = 1 if str(response.get("website") or "").strip() else 0
    has_ssl = 1 if str(response.get("website") or "").startswith("https://") else 0
    has_contact_form = _to_bool_int(conversion.get("contact_form"))
    has_phone = 1 if str(response.get("phone") or "").strip() else 0
    has_viewport = 1
    has_schema = _to_bool_int(service_intel.get("schema_detected"))

    features = _base_market_features(
        rating=rating,
        review_count=review_count,
        local_avg_reviews=local_avg_reviews,
        local_avg_rating=local_avg_rating,
        market_density_ord=market_density_ord,
        competitor_count_radius=competitor_count_radius,
        has_website=has_website,
        has_ssl=has_ssl,
        has_contact_form=has_contact_form,
        has_phone=has_phone,
        has_viewport=has_viewport,
        has_schema=has_schema,
    )

    high_value_rows = service_intel.get("high_value_services") or []
    missing_services = [str(s).strip().lower() for s in (service_intel.get("missing_services") or []) if str(s).strip()]
    detected_services = [str(s).strip().lower() for s in (service_intel.get("detected_services") or []) if str(s).strip()]
    pages_crawled = _to_int(service_intel.get("pages_crawled"))
    crawl_confidence_ord = _market_density_to_ord(service_intel.get("crawl_confidence"))
    service_observed = 1 if crawl_confidence_ord >= 1 and pages_crawled >= 3 else 0

    page_load_raw = conversion.get("page_load_ms")
    page_load_ms = _to_float(page_load_raw, default=-1.0)
    page_load_val = None if page_load_ms <= 0 else page_load_ms
    has_online_booking = _to_bool_int(conversion.get("online_booking"))
    phone_prominent = _to_bool_int(conversion.get("phone_prominent"))
    mobile_optimized = _to_bool_int(conversion.get("mobile_optimized"))
    quality_t2 = _compute_t2_quality_score(
        has_website=has_website,
        has_ssl=has_ssl,
        mobile_optimized=mobile_optimized,
        has_contact_form=has_contact_form,
        phone_prominent=phone_prominent,
        has_online_booking=has_online_booking,
        has_schema=has_schema,
        page_load_ms=page_load_val,
    )

    paid_status = str(response.get("paid_status") or demand_signals.get("google_ads_line") or "").strip().lower()
    runs_google_ads = 1 if any(token in paid_status for token in ("active", "running")) else 0
    meta_line = str(demand_signals.get("meta_ads_line") or "").strip().lower()
    runs_meta_ads = 1 if any(token in meta_line for token in ("active", "running")) else 0
    paid_channels = demand_signals.get("paid_channels_detected") or []
    paid_channels_count = len([c for c in paid_channels if str(c).strip()])

    geo_intent_page_count = _to_int(geo_coverage.get("city_or_near_me_page_count") or len(service_intel.get("geo_intent_pages") or []))
    missing_geo_page_count = _to_int(len(service_intel.get("missing_geo_pages") or []))
    geo_coverage_ratio = geo_intent_page_count / max(geo_intent_page_count + missing_geo_page_count, 1)

    cta_elements = service_intel.get("cta_elements") or []
    cta_count = sum(_to_int(item.get("count")) for item in cta_elements if isinstance(item, Mapping))
    cta_clickable_count = _to_int(service_intel.get("cta_clickable_count"))
    cta_clickable_ratio = cta_clickable_count / max(cta_count, 1)

    competitor_avg_reviews = _to_float((brief.get("competitive_delta") or {}).get("competitor_avg_service_pages"))
    top5_avg_reviews = _to_float(market_saturation.get("top_5_avg_reviews"))
    competitor_median_reviews = _to_float(market_saturation.get("competitor_median_reviews"))
    if competitor_avg_reviews <= 0:
        competitor_avg_reviews = top5_avg_reviews or competitor_median_reviews or local_avg_reviews
    dominant_competitor_review_ratio = _clip((top5_avg_reviews or local_avg_reviews or 0.0) / max(review_count, 1), 0.0, 5.0)
    market_pressure_score_t2 = _clip(
        0.40 * min(competitor_count_radius, 50) / 50.0
        + 0.35 * (_clip(competitor_avg_reviews / max(review_count, 1), 0.0, 3.0) / 3.0)
        + 0.25 * (1.0 - geo_coverage_ratio),
        0.0,
        1.0,
    )

    review_velocity_30d = _to_float(demand_signals.get("review_velocity_30d"), default=-1.0)
    if review_velocity_30d < 0:
        review_velocity_bucket = 0
        review_velocity = None
    else:
        review_velocity = review_velocity_30d
        if review_velocity_30d < 1:
            review_velocity_bucket = 1
        elif review_velocity_30d < 4:
            review_velocity_bucket = 2
        else:
            review_velocity_bucket = 3

    high_value_services_present_count = _count_present_services(high_value_rows)
    high_value_services_missing_count = _count_missing_services(missing_services)
    high_value_service_coverage_ratio = high_value_services_present_count / max(
        high_value_services_present_count + high_value_services_missing_count, 1
    )
    service_gap_weighted_score = _weighted_service_gap(
        missing_services=missing_services,
        detected_services=detected_services,
        service_observed=bool(service_observed),
    )

    features.update(
        {
            "has_online_booking": has_online_booking,
            "phone_prominent": phone_prominent,
            "mobile_optimized": mobile_optimized,
            "page_load_ms": page_load_val,
            "page_speed_score": round(_page_speed_score(page_load_val), 6),
            "website_quality_score_t2": quality_t2,
            "website_quality_deficit_t2": round(100.0 - quality_t2, 6),
            "service_page_count": _to_int(service_intel.get("service_page_count")),
            "pages_crawled": pages_crawled,
            "crawl_confidence_ord": crawl_confidence_ord,
            "high_value_services_present_count": high_value_services_present_count,
            "high_value_services_missing_count": high_value_services_missing_count,
            "high_value_service_coverage_ratio": round(high_value_service_coverage_ratio, 6),
            "missing_implants_page": 1 if "implants" in missing_services else 0,
            "missing_invisalign_page": 1 if "invisalign" in missing_services else 0,
            "missing_veneers_page": 1 if "veneers" in missing_services else 0,
            "missing_cosmetic_page": 1 if "cosmetic" in missing_services else 0,
            "missing_emergency_page": 1 if "emergency" in missing_services else 0,
            "service_gap_weighted_score": service_gap_weighted_score,
            "runs_google_ads": runs_google_ads,
            "runs_meta_ads": runs_meta_ads,
            "paid_channels_count": paid_channels_count,
            "ads_without_booking_flag": 1 if runs_google_ads == 1 and has_online_booking == 0 else 0,
            "ads_without_service_depth_flag": 1 if runs_google_ads == 1 and service_gap_weighted_score >= 0.30 else 0,
            "geo_intent_page_count": geo_intent_page_count,
            "missing_geo_page_count": missing_geo_page_count,
            "geo_coverage_ratio": round(geo_coverage_ratio, 6),
            "cta_count": cta_count,
            "cta_clickable_count": cta_clickable_count,
            "cta_clickable_ratio": round(cta_clickable_ratio, 6),
            "competitor_avg_reviews": round(competitor_avg_reviews, 6),
            "top5_avg_reviews": round(top5_avg_reviews, 6),
            "competitor_median_reviews": round(competitor_median_reviews, 6),
            "dominant_competitor_review_ratio": round(dominant_competitor_review_ratio, 6),
            "market_pressure_score_t2": round(market_pressure_score_t2, 6),
            "review_velocity_30d": review_velocity,
            "review_velocity_bucket": review_velocity_bucket,
            "dominant_clinic_flag_t2": 1
            if features["review_gap_pct"] <= 0.05
            and rating >= 4.6
            and features["review_ratio_to_market"] >= 1.0
            and high_value_service_coverage_ratio >= 0.8
            and quality_t2 >= 80
            and has_online_booking == 1
            else 0,
            "distressed_clinic_flag_t2": 1 if review_count < 10 and (rating <= 0 or rating < 3.5) and quality_t2 < 35 else 0,
            "mid_market_fit_flag_t2": 1 if 0.15 <= features["review_gap_pct"] <= 0.50 and 20 <= review_count <= 250 and rating >= 3.9 and quality_t2 >= 45 else 0,
            "investable_gap_flag": 1 if (features["review_gap_pct"] >= 0.15 or service_gap_weighted_score >= 0.25 or has_online_booking == 0) and review_count >= 20 and rating >= 3.9 and quality_t2 >= 45 else 0,
            "service_scan_observed_flag": service_observed,
            "conversion_scan_observed_flag": 1 if has_website == 1 and page_load_val is not None else 0,
            "feature_scope": "tier2",
            "feature_version": FEATURE_VERSION,
        }
    )

    feature_keys = [
        "rating",
        "review_count",
        "local_avg_reviews",
        "local_avg_rating",
        "has_website",
        "has_ssl",
        "has_contact_form",
        "has_phone",
        "has_schema",
        "has_online_booking",
        "mobile_optimized",
        "service_page_count",
        "pages_crawled",
        "runs_google_ads",
        "geo_intent_page_count",
        "cta_count",
    ]
    features["signal_completeness_ratio"] = _completeness_ratio(features, feature_keys)
    crawl_conf_component = crawl_confidence_ord / 2.0 if crawl_confidence_ord > 0 else 0.35
    features["data_confidence"] = round(
        _clip(0.6 * features["signal_completeness_ratio"] + 0.4 * crawl_conf_component, 0.0, 1.0),
        6,
    )
    return features
