"""Reason-code mapping for Neyma lead-quality predictions."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

from .feature_schema import CAVEAT_CODES, NEGATIVE_REASON_CODES, POSITIVE_REASON_CODES


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


def _reason(code: str, value: str | None = None, direction: str = "positive", evidence_refs: List[str] | None = None) -> Dict[str, Any]:
    label = POSITIVE_REASON_CODES.get(code) or NEGATIVE_REASON_CODES.get(code) or CAVEAT_CODES.get(code) or code
    payload: Dict[str, Any] = {
        "code": code,
        "label": label,
        "direction": direction,
    }
    if value is not None:
        payload["value"] = value
    if evidence_refs:
        payload["evidence_refs"] = evidence_refs
    return payload


def build_reason_payload(
    features: Mapping[str, Any],
    label_data: Mapping[str, Any],
    feature_scope: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    reasons: List[Dict[str, Any]] = []
    caveats: List[Dict[str, Any]] = []

    review_gap_pct = _get_float(features, "review_gap_pct")
    if review_gap_pct >= 0.25:
        reasons.append(
            _reason(
                "review_gap_high",
                value=f"{int(_get_float(features, 'review_count'))} vs local avg {int(_get_float(features, 'local_avg_reviews'))}",
                evidence_refs=["review_count", "local_avg_reviews"],
            )
        )

    if feature_scope == "tier2":
        if _get_float(features, "website_quality_score_t2") < 60 and _get_int(features, "has_website") == 1:
            reasons.append(
                _reason(
                    "website_quality_low",
                    value=f"quality {round(_get_float(features, 'website_quality_score_t2'), 1)}/100",
                    evidence_refs=["website_quality_score_t2"],
                )
            )
        for key, code, service in [
            ("missing_implants_page", "missing_implants_page", "implants"),
            ("missing_invisalign_page", "missing_invisalign_page", "invisalign"),
            ("missing_veneers_page", "missing_veneers_page", "veneers"),
            ("missing_cosmetic_page", "missing_cosmetic_page", "cosmetic"),
            ("missing_emergency_page", "missing_emergency_page", "emergency"),
        ]:
            if _get_int(features, key) == 1:
                reasons.append(
                    _reason(
                        code,
                        value=service,
                        evidence_refs=["service_intelligence.missing_services"],
                    )
                )
        if _get_int(features, "has_online_booking") == 0:
            reasons.append(
                _reason(
                    "no_online_booking",
                    value="false",
                    evidence_refs=["conversion_infrastructure.online_booking"],
                )
            )
        if _get_int(features, "ads_without_booking_flag") == 1:
            reasons.append(
                _reason(
                    "ads_running_with_conversion_gap",
                    evidence_refs=["paid_status", "conversion_infrastructure.online_booking"],
                )
            )
        elif _get_int(features, "ads_without_service_depth_flag") == 1:
            reasons.append(
                _reason(
                    "ads_running_with_service_gap",
                    evidence_refs=["paid_status", "service_intelligence.missing_services"],
                )
            )
        if _get_int(features, "service_scan_observed_flag") == 0:
            caveats.append(
                _reason(
                    "low_service_observability",
                    direction="caution",
                    evidence_refs=["service_intelligence.crawl_confidence", "service_intelligence.pages_crawled"],
                )
            )
        if features.get("page_load_ms") is None:
            caveats.append(
                _reason(
                    "unknown_page_speed",
                    direction="caution",
                    evidence_refs=["conversion_infrastructure.page_load_ms"],
                )
            )
    else:
        if _get_float(features, "website_quality_score_t1") < 60 and _get_int(features, "has_website") == 1:
            reasons.append(
                _reason(
                    "website_quality_low",
                    value=f"quality {round(_get_float(features, 'website_quality_score_t1'), 1)}/100",
                    evidence_refs=["tier1_signals"],
                )
            )

    if _get_float(features, "market_pressure_score_t2", _get_float(features, "market_pressure_score_t1")) >= 0.65:
        reasons.append(_reason("high_market_pressure", evidence_refs=["market_density", "competitor_count_radius"]))

    if max(_get_int(features, "mid_market_fit_flag_t2"), _get_int(features, "mid_market_fit_flag_t1")) == 1:
        reasons.append(_reason("mid_market_fit", evidence_refs=["review_gap_pct", "review_count", "rating"]))

    if max(_get_int(features, "dominant_clinic_flag_t2"), _get_int(features, "dominant_clinic_flag_t1")) == 1:
        reasons.append(_reason("dominant_clinic_penalty", direction="negative", evidence_refs=["review_gap_pct", "rating"]))
    if max(_get_int(features, "distressed_clinic_flag_t2"), _get_int(features, "distressed_clinic_flag_t1")) == 1:
        reasons.append(_reason("distressed_clinic_penalty", direction="negative", evidence_refs=["review_count", "rating"]))
    if _get_int(features, "review_count") < 10:
        reasons.append(_reason("low_review_count_penalty", direction="negative", evidence_refs=["review_count"]))
    if 0 < _get_float(features, "rating") < 3.5:
        reasons.append(_reason("low_rating_penalty", direction="negative", evidence_refs=["rating"]))
    if _get_int(features, "market_density_ord") == 0:
        reasons.append(_reason("weak_market_viability_penalty", direction="negative", evidence_refs=["market_density"]))
    if _get_float(features, "signal_completeness_ratio") < 0.55:
        caveats.append(_reason("limited_market_context", direction="caution", evidence_refs=["signal_completeness_ratio"]))
        reasons.append(_reason("low_signal_completeness_penalty", direction="negative", evidence_refs=["signal_completeness_ratio"]))

    # Keep the payload short for product use.
    positive = [r for r in reasons if r["direction"] == "positive"][:3]
    negative = [r for r in reasons if r["direction"] == "negative"][:2]
    return positive + negative, caveats[:2]
