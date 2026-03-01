import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from backend.routes.diagnostics import _brief_pdf_lines
from pipeline.revenue_brief_renderer import build_revenue_brief_view_model, render_revenue_brief_html


def test_brief_view_model_excludes_serp_presence_block():
    lead = {
        "name": "Test Dental",
        "signals": {
            "signal_review_count": 120,
            "signal_rating": 4.8,
            "signal_has_website": True,
            "signal_has_schema_microdata": False,
        },
        "competitive_snapshot": {
            "avg_review_count": 100,
            "avg_rating": 4.6,
            "market_density_score": "High",
            "dentists_sampled": 8,
        },
        "objective_intelligence": {
            "service_intel": {
                "high_ticket_procedures_detected": ["implants"],
                "missing_high_value_pages": ["implants"],
            }
        },
        "serp_presence": {
            "keywords": [{"keyword": "implants san jose", "position": 4, "in_top_10": True}],
            "as_of_date": "2026-03-01",
        },
    }

    vm = build_revenue_brief_view_model(lead)
    assert "serp_presence" not in vm


def test_render_revenue_brief_html_strategic_gap_comparison_is_data_driven():
    lead = {
        "name": "Test Dental",
        "signals": {"signal_review_count": 150, "signal_rating": 4.9},
        "competitive_snapshot": {"avg_review_count": 120, "market_density_score": "High"},
        "objective_intelligence": {
            "strategic_gap": {
                "service": "Implants",
                "competitor_name": "Nearby Dental",
                "competitor_reviews": 98,
                "lead_reviews": 150,
                "distance_miles": 0.2,
                "market_density": "High",
            }
        },
    }
    html = render_revenue_brief_html(lead)
    assert "review position is above that competitor." in html
    assert "SERP Presence" not in html


def test_pdf_lines_show_serp_excluded_and_strategic_gap_comparison():
    resp = {
        "business_name": "Test Dental",
        "city": "San Jose",
        "state": "CA",
        "brief": {
            "executive_diagnosis": {},
            "market_position": {},
            "demand_signals": {},
            "high_ticket_gaps": {},
            "strategic_gap": {
                "competitor_name": "Nearby Dental",
                "competitor_reviews": 98,
                "lead_reviews": 150,
                "distance_miles": 0.2,
                "market_density": "High",
            },
        },
    }
    lines = _brief_pdf_lines(resp)
    joined = "\n".join(lines)
    assert "Keywords/SERP: excluded from V1" in joined
    assert "review position is above that competitor." in joined
    assert "SERP Presence (Full Detail)" not in joined
