import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline.validation import enforce_diagnostic_consistency


def test_enforce_diagnostic_consistency_corrects_review_position_and_strategic_gap_relation():
    out = {
        "review_position": "Below sample average",
        "brief": {
            "market_position": {
                "reviews": "139 (5)",
                "local_avg": "143",
            },
            "strategic_gap": {
                "competitor_name": "Nearest Dental",
                "competitor_reviews": 98,
                "lead_reviews": 139,
            },
            "executive_diagnosis": {
                "opportunity_profile": {
                    "leverage_drivers": {"review_deficit": True}
                }
            },
            "demand_signals": {
                "google_ads_line": "Not detected"
            },
        },
    }
    merged = {
        "signal_runs_paid_ads": True,
    }

    warnings = enforce_diagnostic_consistency(out, merged=merged)

    assert out["review_position"] == "In line with sample average"
    sg = out["brief"]["strategic_gap"]
    assert sg["review_position_vs_competitor"] == "above that competitor"
    assert "above that competitor" in sg["review_position_sentence"]
    assert out["brief"]["executive_diagnosis"]["opportunity_profile"]["leverage_drivers"]["review_deficit"] is False
    assert out["brief"]["demand_signals"]["google_ads_line"] == "Active (signal detected)"
    assert len(warnings) >= 3


def test_enforce_diagnostic_consistency_no_warnings_when_aligned():
    out = {
        "review_position": "In line with sample average",
        "brief": {
            "market_position": {
                "reviews": "80",
                "local_avg": "100",
            },
            "strategic_gap": {
                "competitor_reviews": 120,
                "lead_reviews": 80,
            },
            "executive_diagnosis": {
                "opportunity_profile": {
                    "leverage_drivers": {"review_deficit": False}
                }
            },
            "demand_signals": {
                "google_ads_line": "Not detected"
            },
        },
    }

    warnings = enforce_diagnostic_consistency(out, merged={"signal_runs_paid_ads": False})
    assert warnings == []
    assert "consistency_warnings" not in out["brief"]
