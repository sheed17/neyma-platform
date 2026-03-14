"""
Deterministic evidence ID registry. UI maps IDs → human-readable descriptions.
No narrative evidence in pipeline output; only ID references.
"""

# Evidence IDs (frozen set for validation)
EVID_REVIEW_COUNT_HIGH = "EVID_REVIEW_COUNT_HIGH"
EVID_REVIEW_COUNT_LOW = "EVID_REVIEW_COUNT_LOW"
EVID_BOOKING_PRESENT = "EVID_BOOKING_PRESENT"
EVID_BOOKING_ABSENT = "EVID_BOOKING_ABSENT"
EVID_SCHEMA_MISSING = "EVID_SCHEMA_MISSING"
EVID_SCHEMA_PRESENT = "EVID_SCHEMA_PRESENT"
EVID_HIGH_TICKET_MISSING_PAGE = "EVID_HIGH_TICKET_MISSING_PAGE"
EVID_PAID_ADS_ACTIVE = "EVID_PAID_ADS_ACTIVE"
EVID_PAID_ADS_ABSENT = "EVID_PAID_ADS_ABSENT"
EVID_MARKET_SATURATED = "EVID_MARKET_SATURATED"
EVID_MARKET_MODERATE = "EVID_MARKET_MODERATE"
EVID_MARKET_LOW_DENSITY = "EVID_MARKET_LOW_DENSITY"
EVID_WEBSITE_PRESENT = "EVID_WEBSITE_PRESENT"
EVID_WEBSITE_ABSENT = "EVID_WEBSITE_ABSENT"
EVID_SERVICES_DETECTED = "EVID_SERVICES_DETECTED"
EVID_SERVICES_ABSENT = "EVID_SERVICES_ABSENT"
EVID_ABOVE_SAMPLE_AVERAGE = "EVID_ABOVE_SAMPLE_AVERAGE"
EVID_BELOW_SAMPLE_AVERAGE = "EVID_BELOW_SAMPLE_AVERAGE"
EVID_TRUST_LIMITED = "EVID_TRUST_LIMITED"
EVID_VISIBILITY_LIMITED = "EVID_VISIBILITY_LIMITED"
EVID_CONVERSION_LIMITED = "EVID_CONVERSION_LIMITED"
EVID_DIFFERENTIATION_LIMITED = "EVID_DIFFERENTIATION_LIMITED"
EVID_SEO_PRIMARY_LEVER = "EVID_SEO_PRIMARY_LEVER"
EVID_SEO_SECONDARY_LEVER = "EVID_SEO_SECONDARY_LEVER"
EVID_REVENUE_BAND_INDICATIVE = "EVID_REVENUE_BAND_INDICATIVE"
EVID_TRAFFIC_LOW_CONFIDENCE = "EVID_TRAFFIC_LOW_CONFIDENCE"

ALL_EVID_IDS = frozenset({
    EVID_REVIEW_COUNT_HIGH,
    EVID_REVIEW_COUNT_LOW,
    EVID_BOOKING_PRESENT,
    EVID_BOOKING_ABSENT,
    EVID_SCHEMA_MISSING,
    EVID_SCHEMA_PRESENT,
    EVID_HIGH_TICKET_MISSING_PAGE,
    EVID_PAID_ADS_ACTIVE,
    EVID_PAID_ADS_ABSENT,
    EVID_MARKET_SATURATED,
    EVID_MARKET_MODERATE,
    EVID_MARKET_LOW_DENSITY,
    EVID_WEBSITE_PRESENT,
    EVID_WEBSITE_ABSENT,
    EVID_SERVICES_DETECTED,
    EVID_SERVICES_ABSENT,
    EVID_ABOVE_SAMPLE_AVERAGE,
    EVID_BELOW_SAMPLE_AVERAGE,
    EVID_TRUST_LIMITED,
    EVID_VISIBILITY_LIMITED,
    EVID_CONVERSION_LIMITED,
    EVID_DIFFERENTIATION_LIMITED,
    EVID_SEO_PRIMARY_LEVER,
    EVID_SEO_SECONDARY_LEVER,
    EVID_REVENUE_BAND_INDICATIVE,
    EVID_TRAFFIC_LOW_CONFIDENCE,
})


def collect_evidence_ids(
    signals: dict,
    competitive_snapshot: dict,
    service_intelligence: dict,
    revenue_intelligence: dict,
    objective_layer: dict,
) -> list:
    """Determine evidence_ids from Layer A (signals + models). Deterministic; IDs only."""
    ids = []
    rev_count = int(signals.get("signal_review_count") or signals.get("user_ratings_total") or 0)
    if rev_count >= 50:
        ids.append(EVID_REVIEW_COUNT_HIGH)
    elif rev_count < 15:
        ids.append(EVID_REVIEW_COUNT_LOW)
    booking_path = signals.get("signal_booking_conversion_path")
    has_booking = booking_path in ("Online booking (limited)", "Online booking (full)")
    booking_absent = booking_path in ("Phone-only", "Request form")
    if not has_booking and signals.get("signal_has_automated_scheduling") is True:
        has_booking = True
    if not booking_absent and signals.get("signal_has_automated_scheduling") is False:
        booking_absent = True
    if has_booking:
        ids.append(EVID_BOOKING_PRESENT)
    elif booking_absent:
        ids.append(EVID_BOOKING_ABSENT)
    schema = bool(signals.get("signal_has_schema_microdata")) or bool(signals.get("signal_schema_types") or [])
    if schema:
        ids.append(EVID_SCHEMA_PRESENT)
    else:
        ids.append(EVID_SCHEMA_MISSING)
    missing = (service_intelligence or {}).get("missing_high_value_pages") or []
    if missing:
        ids.append(EVID_HIGH_TICKET_MISSING_PAGE)
    if signals.get("signal_runs_paid_ads") is True:
        ids.append(EVID_PAID_ADS_ACTIVE)
    else:
        ids.append(EVID_PAID_ADS_ABSENT)
    comp = competitive_snapshot or {}
    density = (comp.get("market_density_score") or "").lower()
    if density == "high" or (comp.get("visibility_gap") == "Saturated"):
        ids.append(EVID_MARKET_SATURATED)
    elif density == "medium" or density == "moderate":
        ids.append(EVID_MARKET_MODERATE)
    elif density == "low":
        ids.append(EVID_MARKET_LOW_DENSITY)
    pos = (comp.get("review_positioning") or "").lower()
    if "above" in pos:
        ids.append(EVID_ABOVE_SAMPLE_AVERAGE)
    elif "below" in pos:
        ids.append(EVID_BELOW_SAMPLE_AVERAGE)
    if signals.get("signal_has_website") is True:
        ids.append(EVID_WEBSITE_PRESENT)
    else:
        ids.append(EVID_WEBSITE_ABSENT)
    svc = service_intelligence or {}
    if svc.get("high_ticket_procedures_detected") or svc.get("general_services_detected"):
        ids.append(EVID_SERVICES_DETECTED)
    else:
        ids.append(EVID_SERVICES_ABSENT)
    root = (objective_layer or {}).get("root_bottleneck_classification") or {}
    bn = (root.get("bottleneck") or "").lower()
    if "trust" in bn:
        ids.append(EVID_TRUST_LIMITED)
    elif "visibility" in bn:
        ids.append(EVID_VISIBILITY_LIMITED)
    elif "conversion" in bn:
        ids.append(EVID_CONVERSION_LIMITED)
    elif "differentiation" in bn or "saturation" in bn:
        ids.append(EVID_DIFFERENTIATION_LIMITED)
    seo_lever = (objective_layer or {}).get("seo_lever_assessment") or {}
    if seo_lever.get("is_primary_growth_lever"):
        ids.append(EVID_SEO_PRIMARY_LEVER)
    else:
        ids.append(EVID_SEO_SECONDARY_LEVER)
    if revenue_intelligence.get("revenue_indicative_only") or revenue_intelligence.get("revenue_reliability_grade") == "C":
        ids.append(EVID_REVENUE_BAND_INDICATIVE)
    if (revenue_intelligence.get("traffic_confidence_score") or 50) < 50:
        ids.append(EVID_TRAFFIC_LOW_CONFIDENCE)
    return ids
