import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline.revenue_leverage import build_revenue_leverage_analysis


def test_low_classification_confidence_caps_asymmetry_to_moderate():
    service_intelligence = {
        "practice_type": "orthodontist",
        "practice_classification_confidence": 0.45,
        "expected_services": ["Braces", "Invisalign"],
        "expected_service_count": 2,
        "high_ticket_procedures_detected": ["Invisalign", "Braces"],
        "high_value_services": [
            {"display_name": "Braces", "service_status": "weak_presence"},
            {"display_name": "Invisalign", "service_status": "weak_presence"},
        ],
        "missing_high_value_pages": [],
        "crawl_confidence": "high",
        "procedure_confidence": 0.8,
    }

    out = build_revenue_leverage_analysis(
        lead={},
        dentist_profile={},
        service_intelligence=service_intelligence,
        competitive_snapshot={},
    )
    assert out["estimated_revenue_asymmetry"] == "Moderate"
    assert out["practice_classification_confidence"] == 0.45
