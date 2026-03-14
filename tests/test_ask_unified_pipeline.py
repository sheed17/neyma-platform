import os
import sys
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

from backend.routes import ask as ask_route
import backend.services.job_worker as job_worker


class _FakeRequest:
    def __init__(self, user_id: int = 1):
        self.state = SimpleNamespace(user_id=user_id)


def _base_row() -> dict:
    return {
        "id": 1,
        "place_id": "p1",
        "business_name": "Demo Dental",
        "city": "Austin",
        "state": "TX",
        "website": "https://example.com",
        "rating": 4.1,
        "user_ratings_total": 12,
        "rank_key": 81.0,
        "below_review_avg": True,
        "has_website": True,
        "has_contact_form": True,
        "ssl": True,
        "has_schema": True,
        "has_viewport": True,
    }


def test_ask_rejects_missing_city_state(monkeypatch):
    monkeypatch.setattr(ask_route, "moderate_text", lambda _text: (True, None))
    monkeypatch.setattr(
        ask_route,
        "resolve_ask_intent",
        lambda _q: {
            "city": None,
            "state": None,
            "criteria": [{"type": "below_review_avg", "service": None}],
            "missing_required": ["city", "state"],
        },
    )

    with pytest.raises(HTTPException) as exc:
        ask_route.ask_find(ask_route.AskRequest(query="find dentists"), _FakeRequest())

    assert exc.value.status_code == 400
    assert "city and state" in str(exc.value.detail).lower()


def test_low_confidence_requires_confirmation_without_job(monkeypatch):
    create_calls = {"count": 0}

    monkeypatch.setattr(ask_route, "moderate_text", lambda _text: (True, None))
    monkeypatch.setattr(
        ask_route,
        "resolve_ask_intent",
        lambda _q: {
            "city": "Austin",
            "state": "TX",
            "vertical": "dentist",
            "limit": 5,
            "criteria": [{"type": "below_review_avg", "service": None}],
            "must_not": [],
            "intent_confidence": "low",
            "unsupported_parts": ["medicaid"],
        },
    )

    def _fake_create_job(**_kwargs):
        create_calls["count"] += 1
        return "job-1"

    monkeypatch.setattr(ask_route, "create_job", _fake_create_job)

    out = ask_route.ask_find(ask_route.AskRequest(query="find dentists"), _FakeRequest())
    assert out["requires_confirmation"] is True
    assert out["job_id"] is None
    assert out["confidence"] == "low"
    assert "medicaid" in out["unsupported_parts"]
    assert create_calls["count"] == 0


def test_ask_queues_verified_mode_with_deep_verification_for_missing_service(monkeypatch):
    create_kwargs = {}

    monkeypatch.setattr(ask_route, "moderate_text", lambda _text: (True, None))
    monkeypatch.setattr(
        ask_route,
        "resolve_ask_intent",
        lambda _q: {
            "city": "Phoenix",
            "state": "AZ",
            "vertical": "dentist",
            "limit": 4,
            "accuracy_mode": "verified",
            "criteria": [{"type": "missing_service_page", "service": "implants"}],
            "must_not": [],
            "intent_confidence": "high",
            "unsupported_parts": [],
        },
    )

    def _fake_create_job(**kwargs):
        create_kwargs.update(kwargs)
        return "job-verified-1"

    monkeypatch.setattr(ask_route, "create_job", _fake_create_job)

    out = ask_route.ask_find(
        ask_route.AskRequest(query="Find 4 dentists in Phoenix AZ missing implants page with verified accuracy"),
        _FakeRequest(),
    )

    assert out["job_id"] == "job-verified-1"
    queued = create_kwargs["input_data"]
    assert queued["accuracy_mode"] == "verified"
    assert queued["require_deep_verification"] is True
    assert int(queued["adaptive_limits"]["deep_top_k"]) >= 20


def test_deep_verification_only_for_required_criteria(monkeypatch):
    deep_calls = {"count": 0}

    monkeypatch.setattr(job_worker, "update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(job_worker, "get_ask_places_cache", lambda _key: None)
    monkeypatch.setattr(job_worker, "upsert_ask_places_cache", lambda _key, _data: None)
    monkeypatch.setattr(job_worker, "get_ask_lightweight_cache", lambda _pid, _ckey: None)
    monkeypatch.setattr(job_worker, "upsert_ask_lightweight_cache", lambda _pid, _ckey, _res: None)
    monkeypatch.setattr(job_worker, "_fetch_territory_candidates", lambda **_kwargs: [_base_row()])
    monkeypatch.setattr(job_worker, "_build_tier1_rows", lambda *_args, **_kwargs: ([_base_row()], 0))
    monkeypatch.setattr(job_worker, "run_lightweight_service_page_check", lambda *_args, **_kwargs: {"matches": True})

    def _fake_diag(**_kwargs):
        deep_calls["count"] += 1
        return {"service_intelligence": {"missing_services": ["implants"]}}

    monkeypatch.setattr(job_worker, "run_diagnostic", _fake_diag)

    no_deep_job = {
        "id": "job-fast",
        "input": {
            "resolved_intent": {"city": "Austin", "state": "TX", "vertical": "dentist", "limit": 1},
            "criteria": [{"type": "below_review_avg", "service": None}],
            "must_not": [],
            "limit": 1,
            "require_deep_verification": False,
            "adaptive_limits": {"max_iterations": 1, "max_minutes": 1.5, "min_results": 1},
        },
    }
    out_fast = job_worker.handle_ask_scan(no_deep_job)
    assert out_fast["require_deep_verification"] is False
    assert deep_calls["count"] == 0

    no_deep_missing_job = {
        "id": "job-no-deep-missing",
        "input": {
            "resolved_intent": {"city": "Austin", "state": "TX", "vertical": "dentist", "limit": 1},
            "criteria": [{"type": "missing_service_page", "service": "implants"}],
            "must_not": [],
            "limit": 1,
            "require_deep_verification": False,
            "adaptive_limits": {"max_iterations": 1, "max_minutes": 1.5, "min_results": 1},
        },
    }
    out_no_deep_missing = job_worker.handle_ask_scan(no_deep_missing_job)
    assert out_no_deep_missing["require_deep_verification"] is False
    assert deep_calls["count"] == 0

    deep_job = {
        "id": "job-deep",
        "input": {
            "resolved_intent": {"city": "Austin", "state": "TX", "vertical": "dentist", "limit": 1},
            "criteria": [{"type": "missing_service_page", "service": "implants"}],
            "must_not": [],
            "limit": 1,
            "require_deep_verification": True,
            "adaptive_limits": {"max_iterations": 1, "max_minutes": 1.5, "min_results": 1, "deep_top_k": 5},
        },
    }
    out_deep = job_worker.handle_ask_scan(deep_job)
    assert out_deep["require_deep_verification"] is True
    assert deep_calls["count"] > 0


def test_adaptive_expansion_records_iterations_and_stop_reason(monkeypatch):
    monkeypatch.setattr(job_worker, "update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(job_worker, "get_ask_places_cache", lambda _key: None)
    monkeypatch.setattr(job_worker, "upsert_ask_places_cache", lambda _key, _data: None)
    monkeypatch.setattr(job_worker, "get_ask_lightweight_cache", lambda _pid, _ckey: None)
    monkeypatch.setattr(job_worker, "upsert_ask_lightweight_cache", lambda _pid, _ckey, _res: None)

    row = _base_row()
    row["below_review_avg"] = False
    monkeypatch.setattr(job_worker, "_fetch_territory_candidates", lambda **_kwargs: [row])
    monkeypatch.setattr(job_worker, "_build_tier1_rows", lambda *_args, **_kwargs: ([row], 0))

    job = {
        "id": "job-expand",
        "input": {
            "resolved_intent": {"city": "Austin", "state": "TX", "vertical": "dentist", "limit": 5},
            "criteria": [{"type": "below_review_avg", "service": None}],
            "must_not": [],
            "limit": 5,
            "require_deep_verification": False,
            "adaptive_limits": {
                "max_iterations": 3,
                "max_minutes": 1.5,
                "radius_start": 2,
                "radius_step": 1,
                "max_radius": 3,
                "cap_start": 150,
                "cap_step": 100,
                "max_cap": 250,
                "min_results": 5,
            },
        },
    }

    out = job_worker.handle_ask_scan(job)
    assert out["stop_reason"] == "max_cap_reached"
    assert len(out["iterations"]) == 3
    assert all("elapsed_ms" in it for it in out["iterations"])


def test_ask_queues_qa_signal_verification_sample(monkeypatch):
    monkeypatch.setenv("QA_VERIFY_ENABLED", "1")
    monkeypatch.setenv("QA_VERIFY_SAMPLE_RATE", "1")
    monkeypatch.setenv("QA_VERIFY_MAX_PER_JOB", "10")
    monkeypatch.setenv("ASK_AI_REVIEW_ENABLED", "0")
    monkeypatch.setenv("ASK_AI_RERANK_ENABLED", "0")
    monkeypatch.setenv("ASK_AI_EXPLAIN_ENABLED", "0")

    monkeypatch.setattr(job_worker, "update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(job_worker, "get_ask_places_cache", lambda _key: None)
    monkeypatch.setattr(job_worker, "upsert_ask_places_cache", lambda _key, _data: None)
    monkeypatch.setattr(job_worker, "get_ask_lightweight_cache", lambda _pid, _ckey: None)
    monkeypatch.setattr(job_worker, "upsert_ask_lightweight_cache", lambda _pid, _ckey, _res: None)
    monkeypatch.setattr(job_worker, "_fetch_territory_candidates", lambda **_kwargs: [_base_row()])
    monkeypatch.setattr(job_worker, "_build_tier1_rows", lambda *_args, **_kwargs: ([_base_row()], 0))
    monkeypatch.setattr(job_worker, "run_lightweight_service_page_check", lambda *_args, **_kwargs: {"matches": True, "reason": "qa-test", "method": "fast_homepage", "service": "implants", "evidence": {"title": "home"}})

    monkeypatch.setattr(job_worker, "insert_qa_signal_checks", lambda rows: [101 + i for i in range(len(rows))])
    monkeypatch.setattr(job_worker, "create_job", lambda **_kwargs: "qa-job-1")

    out = job_worker.handle_ask_scan(
        {
            "id": "job-qa-ask",
            "user_id": 1,
            "input": {
                "resolved_intent": {"city": "Austin", "state": "TX", "vertical": "dentist", "limit": 1},
                "criteria": [{"type": "missing_service_page", "service": "implants"}],
                "must_not": [],
                "limit": 1,
                "require_deep_verification": False,
                "adaptive_limits": {"max_iterations": 1, "max_minutes": 1.5, "min_results": 1},
            },
        }
    )

    qa = out.get("qa_verification") or {}
    assert int(qa.get("sampled") or 0) >= 1
    assert qa.get("job_id") == "qa-job-1"


def test_verified_mode_skips_ai_rerank_and_returns_deep_evidence(monkeypatch):
    monkeypatch.setenv("ASK_AI_REVIEW_ENABLED", "0")
    monkeypatch.setenv("ASK_AI_RERANK_ENABLED", "1")
    monkeypatch.setenv("ASK_AI_EXPLAIN_ENABLED", "0")
    monkeypatch.setattr(job_worker, "update_job_status", lambda *args, **kwargs: None)
    monkeypatch.setattr(job_worker, "get_ask_places_cache", lambda _key: None)
    monkeypatch.setattr(job_worker, "upsert_ask_places_cache", lambda _key, _data: None)
    monkeypatch.setattr(job_worker, "get_ask_lightweight_cache", lambda _pid, _ckey: None)
    monkeypatch.setattr(job_worker, "upsert_ask_lightweight_cache", lambda _pid, _ckey, _res: None)
    monkeypatch.setattr(job_worker, "_fetch_territory_candidates", lambda **_kwargs: [_base_row()])
    monkeypatch.setattr(job_worker, "_build_tier1_rows", lambda *_args, **_kwargs: ([_base_row()], 0))
    monkeypatch.setattr(
        job_worker,
        "run_lightweight_service_page_check",
        lambda *_args, **_kwargs: {
            "matches": True,
            "reason": "fast_homepage_match",
            "method": "fast_homepage",
            "service": "implants",
            "evidence": {"title": "Implants"},
        },
    )
    monkeypatch.setattr(
        job_worker,
        "run_diagnostic",
        lambda **_kwargs: {
            "service_intelligence": {"missing_services": ["implants"]},
            "opportunity_profile": "service gap",
            "primary_leverage": "implants",
            "constraint": "service_depth",
        },
    )

    rerank_calls = {"count": 0}

    def _fake_rerank(**_kwargs):
        rerank_calls["count"] += 1
        return {"p1": {"delta": 4.0}}

    monkeypatch.setattr(job_worker, "ai_batch_rerank_candidates", _fake_rerank)

    out = job_worker.handle_ask_scan(
        {
            "id": "job-verified-evidence",
            "input": {
                "resolved_intent": {
                    "city": "Austin",
                    "state": "TX",
                    "vertical": "dentist",
                    "limit": 1,
                    "accuracy_mode": "verified",
                },
                "accuracy_mode": "verified",
                "criteria": [{"type": "missing_service_page", "service": "implants"}],
                "must_not": [],
                "limit": 1,
                "require_deep_verification": True,
                "adaptive_limits": {"max_iterations": 1, "max_minutes": 1.5, "min_results": 1},
            },
        }
    )

    assert out["accuracy_mode"] == "verified"
    assert out["require_deep_verification"] is True
    assert rerank_calls["count"] == 0
    prospect = (out.get("prospects") or [])[0]
    assert prospect["match_evidence_level"] == "deep_verified"
    assert any(
        item.get("source") == "deep_verified_diagnostic" and item.get("matched") is True
        for item in (prospect.get("match_evidence") or [])
    )
