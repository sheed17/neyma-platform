"""
Unit tests for compute_opportunity_profile in revenue_brief_renderer.

Scenarios:
  a) missing_high_value=True, high_density=True -> High-Leverage
  b) missing_high_value=True, high_density=False -> Moderate
  c) missing_high_value=False -> Low-Leverage
"""

import os
import sys
import importlib.util

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

_spec = importlib.util.spec_from_file_location(
    "revenue_brief_renderer",
    os.path.join(_root, "pipeline", "revenue_brief_renderer.py"),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
compute_opportunity_profile = _mod.compute_opportunity_profile


def _lead(**overrides):
    base = {
        "name": "Test Dental",
        "place_id": "test-1",
        "objective_intelligence": {},
        "competitive_snapshot": {},
        "revenue_intelligence": {},
        "signals": {},
    }
    base.update(overrides)
    return base


def test_high_leverage_missing_high_value_high_density():
    """missing_high_value=True, high_density=True -> High-Leverage"""
    lead = _lead(
        objective_intelligence={
            "service_intel": {
                "missing_high_value_pages": ["implants"],
                "schema_detected": False,
            },
            "competitive_profile": {"market_density": "High"},
        },
        competitive_snapshot={"market_density_score": "High"},
    )
    result = compute_opportunity_profile(lead)
    assert result.get("label") == "High-Leverage"
    assert result.get("why")
    assert "high-ticket" in result["why"] or "structured" in result["why"]


def test_moderate_missing_high_value_no_high_density():
    """missing_high_value=True, high_density=False, paid_active=True -> Moderate"""
    lead = _lead(
        objective_intelligence={
            "service_intel": {
                "missing_high_value_pages": ["implants"],
                "schema_detected": True,
            },
            "competitive_profile": {"market_density": "Low"},
        },
        competitive_snapshot={"market_density_score": "Low"},
        signals={"signal_has_schema_microdata": True, "signal_runs_paid_ads": True},
    )
    result = compute_opportunity_profile(lead)
    assert result.get("label") == "Moderate"
    assert result.get("why")


def test_low_leverage_no_missing_high_value():
    """missing_high_value=False -> Low-Leverage"""
    lead = _lead(
        objective_intelligence={
            "service_intel": {
                "missing_high_value_pages": [],
                "schema_detected": False,
            },
            "competitive_profile": {"market_density": "High"},
        },
    )
    result = compute_opportunity_profile(lead)
    assert result.get("label") == "Low-Leverage"
    assert result.get("why")
