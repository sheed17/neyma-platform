"""
Unit tests for the canonical 60-second summary builder.

Fixtures:
  a) Strong practice: website + ads + high reviews -> worth_pursuing Yes/Maybe, numeric traffic range, confidence_notes
  b) Weak practice: no website, low reviews -> worth_pursuing Maybe/No, indicative_only, confidence_notes
  c) Mid practice: website, no ads -> full summary_60s shape, traffic_estimate with range
"""

import os
import sys
import importlib.util

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_root, path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Load canonical_summary and its deps (revenue_intelligence -> revenue_model_v2, traffic_model_v2)
canonical = _load_module("canonical_summary", "pipeline/canonical_summary.py")
revenue_intel = _load_module("revenue_intelligence", "pipeline/revenue_intelligence.py")
build_canonical_summary_60s = canonical.build_canonical_summary_60s
build_revenue_intelligence = revenue_intel.build_revenue_intelligence


def _objective_layer(bottleneck="visibility_limited", has_services=True, competitive=True):
    svc = {}
    if has_services:
        svc = {
            "high_ticket_procedures_detected": [{"procedure": "Implants"}],
            "general_services_detected": ["General", "Cleaning"],
            "missing_high_value_pages": [],
        }
    comp = {}
    if competitive:
        comp = {
            "dentists_sampled": 5,
            "review_positioning": "Above average",
            "market_density_score": "Medium",
            "lead_review_count": 80,
            "avg_review_count": 60,
        }
    return {
        "root_bottleneck_classification": {"bottleneck": bottleneck, "why_root_cause": "Low visibility"},
        "seo_lever_assessment": {"is_primary_growth_lever": True, "alternative_primary_lever": ""},
        "competitive_snapshot": comp,
        "service_intelligence": svc,
        "revenue_leverage_analysis": {"primary_revenue_driver_detected": "implants"},
        "intervention_plan": [{"action": "Add service pages", "expected_impact": "Capture intent", "time_to_signal_days": 45}],
        "primary_sales_anchor": {"issue": "Visibility", "why_this_first": "Root constraint"},
        "seo_sales_value_score": 65,
    }


def test_strong_practice():
    """Strong: website + ads + high reviews -> summary has traffic_estimate range, worth_pursuing Yes/Maybe."""
    lead = {
        "signal_has_website": True,
        "signal_review_count": 120,
        "signal_rating": 4.7,
        "signal_runs_paid_ads": True,
        "signal_paid_ads_channels": ["google"],
        "verdict": "HIGH",
        "user_ratings_total": 120,
    }
    obj = _objective_layer(bottleneck="visibility_limited", has_services=True, competitive=True)
    rev = build_revenue_intelligence(lead, {}, obj)
    summary = build_canonical_summary_60s(lead, {}, obj, rev)

    assert "worth_pursuing" in summary
    assert summary["worth_pursuing"] in ("Yes", "Maybe")
    assert "traffic_estimate" in summary
    monthly = summary["traffic_estimate"].get("traffic_estimate_monthly")
    assert monthly is not None
    assert monthly.get("unit") == "visits/month"
    assert isinstance(monthly.get("lower"), (int, type(None)))
    assert "supporting_evidence" in summary
    assert "reputation_signals" in summary["supporting_evidence"]
    assert "revenue_signals" in summary["supporting_evidence"]
    assert "confidence_notes" in summary
    assert "model_versions" in summary
    assert "disclaimers" in summary
    assert "highest_leverage_move" in summary
    assert "lever" in summary["highest_leverage_move"]


def test_weak_practice():
    """Weak: no website, low reviews -> indicative_only, confidence_notes mention limited data."""
    lead = {
        "signal_has_website": False,
        "signal_review_count": 8,
        "signal_rating": 4.2,
        "signal_runs_paid_ads": False,
        "verdict": "LOW",
        "user_ratings_total": 8,
    }
    obj = _objective_layer(bottleneck="trust_limited", has_services=False, competitive=False)
    rev = build_revenue_intelligence(lead, {}, obj)
    summary = build_canonical_summary_60s(lead, {}, obj, rev)

    assert summary["worth_pursuing"] in ("Maybe", "No")
    assert "confidence_notes" in summary
    notes = " ".join(summary["confidence_notes"]).lower()
    assert "indicative" in notes or "no website" in notes or "low review" in notes
    assert summary.get("revenue_band")


def test_mid_practice():
    """Mid: website, no ads -> full summary_60s shape, traffic_estimate with range, no paid_clicks."""
    lead = {
        "signal_has_website": True,
        "signal_review_count": 45,
        "signal_rating": 4.5,
        "signal_runs_paid_ads": False,
        "verdict": "MEDIUM",
        "user_ratings_total": 45,
    }
    obj = _objective_layer(bottleneck="conversion_limited", has_services=True, competitive=True)
    rev = build_revenue_intelligence(lead, {}, obj)
    summary = build_canonical_summary_60s(lead, {}, obj, rev)

    assert summary["traffic_estimate"]["traffic_estimate_monthly"]["unit"] == "visits/month"
    assert summary["traffic_estimate"].get("paid_clicks_estimate_monthly") is None
    assert "root_constraint" in summary
    assert "right_lever_summary" in summary
    assert "market_position_one_line" in summary
    assert "cost_leakage_signals" in summary
    assert "primary_revenue_driver" in summary
    assert "paid_spend_range_estimate" in summary


def test_canonical_summary_suppresses_absence_claims_under_low_confidence():
    lead = {
        "signal_has_website": True,
        "signal_review_count": 30,
        "signal_rating": 4.5,
        "signal_runs_paid_ads": True,
        "signal_has_automated_scheduling": False,
        "user_ratings_total": 30,
    }
    obj = _objective_layer(bottleneck="visibility_limited", has_services=True, competitive=True)
    obj["service_intelligence"]["missing_high_value_pages"] = ["Implants"]
    obj["service_intelligence"]["suppress_service_gap"] = True
    obj["service_intelligence"]["suppress_conversion_absence_claims"] = True
    rev = build_revenue_intelligence(lead, {}, obj)
    summary = build_canonical_summary_60s(lead, {}, obj, rev)

    digital = " | ".join(summary["supporting_evidence"]["digital_signals"]).lower()
    assert "missing dedicated" not in digital
    assert "no online booking" not in digital
    assert "conversion infrastructure not fully evaluated" in digital
    assert "service visibility not fully evaluated" in digital
