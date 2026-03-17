import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline.consistency import normalize_conversion_infrastructure, normalize_diagnostic_payload


def test_normalize_diagnostic_payload_recomputes_missing_services_from_service_rows():
    raw = {
        "service_intelligence": {
            "high_value_services": [
                {"service": "implants", "display_name": "Implants", "service_status": "dedicated", "revenue_weight": 5, "word_count": 150},
                {"service": "veneers", "display_name": "Veneers", "service_status": "missing", "revenue_weight": 4, "word_count": 0},
            ],
            "missing_high_value_pages": ["Implants", "Veneers"],
            "high_value_summary": {"services_missing": 2},
        },
        "brief": {
            "high_ticket_gaps": {
                "missing_landing_pages": ["Implants", "Veneers"],
            },
            "service_page_analysis": {
                "services": [],
                "summary": {"services_missing": 2},
            },
        },
    }

    out = normalize_diagnostic_payload(raw)
    assert out["service_intelligence"]["missing_high_value_pages"] == ["Veneers"]
    assert out["brief"]["high_ticket_gaps"]["missing_landing_pages"] == ["Veneers"]
    assert out["brief"]["service_page_analysis"]["summary"]["services_missing"] == 1


def test_normalize_conversion_infrastructure_prefers_capture_verification():
    raw = {
        "online_booking": False,
        "contact_form": None,
        "booking_flow_type": None,
        "capture_verification": {
            "booking_flow": {"value": "online_self_scheduling", "confidence": "high"},
            "contact_form": {"value": True, "confidence": "high"},
        },
    }

    out = normalize_conversion_infrastructure(raw, service_intel={}, signals={})
    assert out["booking_flow_type"] == "online_self_scheduling"
    assert out["online_booking"] is True
    assert out["contact_form"] is True
    assert out["booking_flow_confidence"] == "high"
    assert out["contact_form_confidence"] == "high"
