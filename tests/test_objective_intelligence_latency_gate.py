import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from pipeline import objective_intelligence as oi


def _base_lead():
    return {
        "name": "Fast Dental",
        "dentist_profile_v1": {"ok": True},
        "objective_decision_layer": {},
        "service_intelligence": {},
        "competitive_snapshot": {},
        "revenue_intelligence": {},
    }


def test_llm_intervention_plan_not_called_by_default(monkeypatch):
    monkeypatch.delenv("USE_LLM_INTERVENTION_PLAN", raising=False)
    called = {"v": False}

    def _fake_plan(_lead, objective_intelligence=None):
        called["v"] = True
        return [{"step": 1, "category": "SEO", "action": "x", "time_to_signal_days": 30, "why": "y"}]

    monkeypatch.setattr(oi, "generate_intervention_plan_from_intelligence", _fake_plan)
    out = oi.build_objective_intelligence(_base_lead())
    assert isinstance(out, dict)
    assert called["v"] is False


def test_llm_intervention_plan_called_when_enabled(monkeypatch):
    monkeypatch.setenv("USE_LLM_INTERVENTION_PLAN", "true")
    called = {"v": False}

    def _fake_plan(_lead, objective_intelligence=None):
        called["v"] = True
        return [{"step": 1, "category": "SEO", "action": "x", "time_to_signal_days": 30, "why": "y"}]

    monkeypatch.setattr(oi, "generate_intervention_plan_from_intelligence", _fake_plan)
    out = oi.build_objective_intelligence(_base_lead())
    assert isinstance(out, dict)
    assert called["v"] is True
    assert isinstance(out.get("intervention_plan"), list)
