"""Integration-style tests for agentic Ask scan loop with planner fallback."""

import os
import sys

_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _root)

import backend.services.job_worker as job_worker


def test_agentic_scan_fallback_reaches_min_results_without_planner_llm():
    original_run_npl = job_worker._run_npl_find_job
    original_update = job_worker.update_job_status
    original_planner = job_worker.planner_llm_update

    calls = []
    status_updates = []

    def fake_run_npl_find_job(job: dict) -> dict:
        intent = (job.get("input") or {}).get("intent") or {}
        overrides = intent.get("execution_overrides") or {}
        criteria = intent.get("criteria") or []
        soft_filters = intent.get("soft_filters_rank_only") or []

        calls.append(
            {
                "radius": int(overrides.get("radius_miles") or 0),
                "cap": int(overrides.get("candidate_cap") or 0),
                "criteria": [str(c.get("type")) for c in criteria if isinstance(c, dict)],
                "soft": [str(c.get("type")) for c in soft_filters if isinstance(c, dict)],
            }
        )

        # First iteration returns no matches -> triggers deterministic fallback plan update.
        # Second iteration returns enough matches -> loop stops at min_results.
        matched = 0 if len(calls) == 1 else 6
        return {
            "phase": "completed",
            "total_matches": matched,
            "total_scanned": 40,
            "filtered_out_by_criterion": {"below_review_avg": 30, "missing_service_page": 10},
            "criterion_that_eliminated_most": "below_review_avg",
            "no_results_suggestion": "No matches. Try relaxing below_review_avg or expanding the area.",
            "prospects": [] if matched == 0 else [{"place_id": "p1", "rank_score": 80, "user_ratings_total": 12}],
        }

    def fake_update_job_status(job_id: str, status: str, result=None, **kwargs):
        status_updates.append({"job_id": job_id, "status": status, "result": result or {}})

    try:
        job_worker._run_npl_find_job = fake_run_npl_find_job
        job_worker.update_job_status = fake_update_job_status
        # Empty planner output forces deterministic fallback update path (no OpenAI dependency).
        job_worker.planner_llm_update = lambda normalized_intent, telemetry, system_limits: {}

        job = {
            "id": "job-agentic-1",
            "input": {
                "intent": {
                    "city": "Austin",
                    "state": "TX",
                    "vertical": "dentist",
                    "limit": 5,
                    "criteria": [
                        {"type": "below_review_avg", "service": None},
                        {"type": "missing_service_page", "service": "implants"},
                    ],
                    "must_not": [],
                },
                "accuracy_mode": "fast",
                "system_limits": {
                    "max_radius_miles": 40,
                    "max_candidate_cap": 140,
                    "max_iterations": 4,
                    "max_verify_per_iteration": 10,
                    "min_results": 5,
                    "max_minutes": 15,
                },
            },
        }

        out = job_worker._run_agentic_scan_job(job)

        assert out.get("stop_reason") == "min_results_reached"
        assert out.get("total_matches") == 6
        assert len(out.get("iterations") or []) == 2

        # Verify fallback changed scan breadth after zero-result iteration.
        assert calls[1]["radius"] >= calls[0]["radius"]
        assert calls[1]["cap"] >= calls[0]["cap"]

        # Ensure no unsupported criteria were introduced during planning.
        assert set(calls[0]["criteria"]) == {"below_review_avg", "missing_service_page"}
        assert set(calls[1]["criteria"]) == {"below_review_avg", "missing_service_page"}

        assert out.get("agentic", {}).get("enabled") is True
        assert any(u["status"] == "running" for u in status_updates)

    finally:
        job_worker._run_npl_find_job = original_run_npl
        job_worker.update_job_status = original_update
        job_worker.planner_llm_update = original_planner


def test_deep_brief_job_continues_after_item_timeout():
    original_list = job_worker.list_territory_prospects
    original_runner = job_worker._run_deep_brief_diagnostic_with_timeout
    original_save = job_worker.save_diagnostic
    original_persist = job_worker.persist_saved_diagnostic_response
    original_link = job_worker.link_territory_prospect_diagnostic
    original_update = job_worker.update_job_status

    status_updates = []
    persisted = []
    linked = []

    rows = [
        {
            "id": 1,
            "place_id": "place-1",
            "business_name": "Alpha Dental",
            "city": "Charlotte",
            "state": "NC",
            "website": "https://alpha.example",
            "full_brief_ready": 0,
        },
        {
            "id": 2,
            "place_id": "place-2",
            "business_name": "Beta Dental",
            "city": "Charlotte",
            "state": "NC",
            "website": "https://beta.example",
            "full_brief_ready": 0,
        },
    ]

    def fake_runner(task: dict, deep_audit: bool, timeout_seconds: int) -> dict:
        assert deep_audit is True
        assert timeout_seconds >= 60
        if task["business_name"] == "Beta Dental":
            raise TimeoutError("timed out")
        return {
            "place_id": task["place_id"],
            "business_name": task["business_name"],
            "city": task["city"],
            "state": task["state"],
            "brief": {"summary": "ok"},
        }

    def fake_save_diagnostic(**kwargs):
        persisted.append(kwargs)
        return 101

    def fake_persist_saved_diagnostic_response(diagnostic_id: int, place_id: str, response: dict):
        persisted.append(
            {
                "diagnostic_id": diagnostic_id,
                "place_id": place_id,
                "response": response,
            }
        )

    def fake_link_territory_prospect_diagnostic(prospect_id: int, diagnostic_id: int, full_brief_ready: bool):
        linked.append(
            {
                "prospect_id": prospect_id,
                "diagnostic_id": diagnostic_id,
                "full_brief_ready": full_brief_ready,
            }
        )

    def fake_update_job_status(job_id: str, status: str, result=None, **kwargs):
        status_updates.append({"job_id": job_id, "status": status, "result": result or {}})

    try:
        job_worker.list_territory_prospects = lambda scan_id, user_id: rows
        job_worker._run_deep_brief_diagnostic_with_timeout = fake_runner
        job_worker.save_diagnostic = fake_save_diagnostic
        job_worker.persist_saved_diagnostic_response = fake_persist_saved_diagnostic_response
        job_worker.link_territory_prospect_diagnostic = fake_link_territory_prospect_diagnostic
        job_worker.update_job_status = fake_update_job_status

        out = job_worker._run_deep_brief_job(
            {
                "id": "job-deep-1",
                "user_id": 1,
                "type": "territory_deep_scan",
                "input": {
                    "scan_id": "scan-1",
                    "max_prospects": 2,
                    "concurrency": 2,
                    "deep_audit": True,
                    "prospect_ids": [1, 2],
                    "diagnostic_timeout_seconds": 60,
                },
            }
        )

        assert out["processed"] == 2
        assert out["created"] == 1
        assert out["failed"] == 1
        assert out["total"] == 2
        assert out["diagnostic_ids"] == [101]
        assert len(linked) == 1
        assert linked[0]["prospect_id"] == 1
        assert any(u["result"].get("processed") == 2 for u in status_updates)
    finally:
        job_worker.list_territory_prospects = original_list
        job_worker._run_deep_brief_diagnostic_with_timeout = original_runner
        job_worker.save_diagnostic = original_save
        job_worker.persist_saved_diagnostic_response = original_persist
        job_worker.link_territory_prospect_diagnostic = original_link
        job_worker.update_job_status = original_update
