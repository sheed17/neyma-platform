"""Weak-label generator for Neyma lead-quality scoring."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from .feature_schema import (
    TRAINING_PRIORITY_MIN_BENEFIT,
    TRAINING_PRIORITY_MIN_BUYABILITY,
    TRAINING_PRIORITY_SCORE_THRESHOLD,
)

GOOD_THRESHOLD = 0.70
DECENT_THRESHOLD = 0.40


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _get_float(features: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = features.get(key, default)
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_int(features: Mapping[str, Any], key: str, default: int = 0) -> int:
    try:
        value = features.get(key, default)
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _harmonic_mean_weighted(a: float, b: float, wa: float = 0.55, wb: float = 0.45) -> float:
    if a <= 0 or b <= 0:
        return 0.0
    return (wa + wb) / ((wa / a) + (wb / b))


def _score_review_count_band(review_count: int) -> float:
    if review_count < 10:
        return 0.05
    if review_count < 25:
        return 0.20
    if review_count < 75:
        return 0.60
    if review_count < 250:
        return 0.85
    if review_count < 600:
        return 0.75
    return 0.55


def _score_rating_band(rating: float) -> float:
    if rating <= 0:
        return 0.45
    if rating < 3.5:
        return 0.10
    if rating < 3.9:
        return 0.35
    if rating < 4.3:
        return 0.70
    if rating <= 4.7:
        return 0.85
    return 0.60


def _score_market_viability(market_density_ord: int) -> float:
    if market_density_ord == 2:
        return 0.85
    if market_density_ord == 1:
        return 0.65
    return 0.40


def _tri_score(features: Mapping[str, Any], key: str, known_key: str | None = None, fallback_key: str | None = None) -> float:
    if known_key and _get_int(features, known_key, 1) == 0:
        return 0.5
    if fallback_key:
        return float(_get_int(features, key, _get_int(features, fallback_key)))
    return float(_get_int(features, key))


def _score_digital_maturity(features: Mapping[str, Any]) -> float:
    vals = [
        _tri_score(features, "has_website"),
        _tri_score(features, "has_ssl"),
        _tri_score(features, "mobile_optimized"),
        _tri_score(features, "has_contact_form", known_key="has_contact_form_known"),
        _tri_score(features, "phone_prominent", fallback_key="has_phone"),
        _tri_score(features, "has_online_booking", known_key="has_online_booking_known"),
        _tri_score(features, "has_schema"),
    ]
    return sum(vals) / max(len(vals), 1)


def _score_trust_baseline(features: Mapping[str, Any]) -> float:
    vals = [
        _tri_score(features, "has_website"),
        _tri_score(features, "has_ssl"),
        _tri_score(features, "has_contact_form", known_key="has_contact_form_known"),
        _tri_score(features, "phone_prominent", fallback_key="has_phone"),
    ]
    return sum(vals) / max(len(vals), 1)


def generate_lead_quality_label(features: Mapping[str, Any]) -> Dict[str, Any]:
    review_gap = _clip(_get_float(features, "review_gap_pct"), 0.0, 1.0)

    website_quality = _get_float(features, "website_quality_score_t2", -1.0)
    if website_quality < 0:
        website_quality = _get_float(features, "website_quality_score_t1", 0.0)
    website_deficit = _clip((100.0 - website_quality) / 100.0, 0.0, 1.0)

    has_online_booking = _get_int(features, "has_online_booking")
    booking_known = _get_int(features, "has_online_booking_known", 1)
    booking_gap = 1.0 if booking_known == 1 and has_online_booking == 0 else 0.0

    service_gap = _clip(_get_float(features, "service_gap_weighted_score"), 0.0, 1.0)
    if _get_int(features, "service_scan_observed_flag", 1) == 0:
        service_gap *= 0.35

    runs_ads = max(_get_int(features, "runs_google_ads"), _get_int(features, "runs_meta_ads"))
    ads_leakage = 1.0 if runs_ads == 1 and (booking_gap == 1.0 or service_gap >= 0.30 or website_quality < 60.0) else 0.0

    market_pressure = _clip(
        _get_float(features, "market_pressure_score_t2", _get_float(features, "market_pressure_score_t1")),
        0.0,
        1.0,
    )

    benefit_score = (
        0.25 * review_gap
        + 0.30 * service_gap
        + 0.20 * website_deficit
        + 0.10 * booking_gap
        + 0.10 * ads_leakage
        + 0.05 * market_pressure
    )

    review_count_score = _score_review_count_band(_get_int(features, "review_count"))
    rating_score = _score_rating_band(_get_float(features, "rating"))
    trust_baseline = _score_trust_baseline(features)
    digital_maturity = _score_digital_maturity(features)
    market_viability = _score_market_viability(_get_int(features, "market_density_ord"))

    buyability_score = (
        0.30 * review_count_score
        + 0.25 * rating_score
        + 0.20 * trust_baseline
        + 0.15 * digital_maturity
        + 0.10 * market_viability
    )

    raw_score = _harmonic_mean_weighted(benefit_score, buyability_score, wa=0.55, wb=0.45)

    dominant_flag = max(_get_int(features, "dominant_clinic_flag_t2"), _get_int(features, "dominant_clinic_flag_t1"))
    distressed_flag = max(_get_int(features, "distressed_clinic_flag_t2"), _get_int(features, "distressed_clinic_flag_t1"))
    completeness = _clip(_get_float(features, "signal_completeness_ratio"), 0.0, 1.0)

    if distressed_flag == 1:
        raw_score = min(raw_score, 0.22)
    if dominant_flag == 1:
        raw_score = min(raw_score, 0.59)
    if completeness < 0.55:
        raw_score *= 0.85

    raw_score = _clip(raw_score, 0.0, 1.0)

    priority_binary = int(
        raw_score >= TRAINING_PRIORITY_SCORE_THRESHOLD
        and benefit_score >= TRAINING_PRIORITY_MIN_BENEFIT
        and buyability_score >= TRAINING_PRIORITY_MIN_BUYABILITY
        and dominant_flag == 0
        and distressed_flag == 0
    )

    if raw_score >= GOOD_THRESHOLD and benefit_score >= 0.50 and buyability_score >= 0.50:
        label_3class = "good"
        high_value_binary = 1
    elif raw_score >= DECENT_THRESHOLD:
        label_3class = "decent"
        high_value_binary = 0
    else:
        label_3class = "bad"
        high_value_binary = 0

    return {
        "lead_quality_score_heuristic_v1": round(raw_score, 6),
        "lead_quality_class_heuristic_v1": label_3class,
        "is_high_value_prospect_heuristic_v1": high_value_binary,
        "is_priority_prospect_heuristic_v1": priority_binary,
        "benefit_score_v1": round(benefit_score, 6),
        "buyability_score_v1": round(buyability_score, 6),
        "guardrails": {
            "dominant_clinic_flag": dominant_flag,
            "distressed_clinic_flag": distressed_flag,
            "low_completeness_penalty": completeness < 0.55,
        },
    }
