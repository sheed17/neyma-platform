"""Snapshot-style API payload test for service/page analysis blocks."""

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)


def test_build_diagnostic_response_service_page_analysis_snapshot(monkeypatch):
    from backend.services import enrichment_service as es
    from pipeline import revenue_brief_renderer as rbr

    high_value_services = [
        {
            "service": "implants",
            "display_name": "Implants",
            "revenue_weight": 5,
            "page_exists": True,
            "url": "/services/dental-implants",
            "word_count": 780,
            "depth_score": "moderate",
            "schema": {
                "service_schema": False,
                "faq_schema": False,
                "localbusiness_schema": False,
            },
            "conversion": {
                "cta_count": 4,
                "booking_link": True,
                "financing_mentioned": False,
                "faq_section": True,
                "before_after_section": False,
                "internal_links": 2,
            },
            "serp": {
                "position_top_3": False,
                "position_top_10": False,
                "average_position": 18,
                "map_pack_presence": False,
                "competitors_in_top_10": 8,
            },
            "optimization_tier": "moderate",
        }
    ]
    high_value_summary = {
        "total_high_value_services": 7,
        "services_present": 3,
        "services_missing": 4,
        "services_strong": 0,
        "services_moderate": 2,
        "services_weak": 1,
        "service_coverage_ratio": 0.429,
        "optimized_ratio": 0.0,
        "average_word_count": 640,
        "serp_visibility_ratio": 0.0,
    }

    def fake_vm(_merged):
        return {
            "executive_diagnosis": {"constraint": "Conversion Constrained"},
            "service_page_analysis": {
                "services": high_value_services,
                "summary": high_value_summary,
                "leverage": "high",
            },
            "intervention_plan": [],
            "intervention_plan_structured": [],
        }

    monkeypatch.setattr(rbr, "build_revenue_brief_view_model", fake_vm)
    monkeypatch.setattr(rbr, "compute_opportunity_profile", lambda _m: {"label": "High-Leverage", "why": "snapshot"})
    monkeypatch.setattr(rbr, "compute_paid_demand_status", lambda _m: {"status": "Active"})

    merged = {
        "name": "Snapshot Dental",
        "service_intelligence": {
            "high_ticket_procedures_detected": ["Implants"],
            "missing_high_value_pages": ["Veneers"],
            "high_value_services": high_value_services,
            "high_value_summary": high_value_summary,
            "high_value_service_leverage": "high",
            "service_page_analysis_v2": {"service_coverage": {"present": 3, "total": 7, "ratio": 0.429}},
        },
        "objective_intelligence": {"risk_flags": []},
        "competitive_snapshot": {"lead_review_count": 40, "avg_review_count": 95},
        "signal_has_schema_microdata": False,
        "signal_has_contact_form": True,
        "signal_has_phone": True,
        "signal_has_automated_scheduling": False,
        "signal_mobile_friendly": True,
        "signal_page_load_time_ms": 920,
    }

    out = es._build_diagnostic_response(
        lead_id=123,
        merged=merged,
        city="San Jose",
        state="CA",
    )

    assert out["brief"]["service_page_analysis"] == {
        "services": high_value_services,
        "summary": high_value_summary,
        "leverage": "high",
    }
    assert out["service_intelligence"] == {
        "detected_services": ["Implants"],
        "missing_services": ["Veneers"],
        "high_value_services": high_value_services,
        "high_value_summary": high_value_summary,
        "high_value_service_leverage": "high",
        "service_page_analysis_v2": {"service_coverage": {"present": 3, "total": 7, "ratio": 0.429}},
        "suppress_service_gap": False,
        "suppress_conversion_absence_claims": False,
        "suppress_revenue_modeling": False,
        "pages_crawled": None,
    }
