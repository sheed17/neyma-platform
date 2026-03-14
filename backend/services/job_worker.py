"""
Background job worker — runs in a daemon thread.

Polls for pending jobs and executes them sequentially.
For production, replace with Celery + Redis.
"""

import json
import logging
import os
import random
import subprocess
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pipeline.db import (
    get_ask_lightweight_cache,
    get_ask_places_cache,
    get_qa_signal_checks_by_ids,
    get_prospect_list,
    list_members_for_list,
    insert_qa_signal_checks,
    list_territory_prospects,
    create_job,
    get_pending_jobs,
    link_territory_prospect_diagnostic,
    save_diagnostic,
    update_qa_signal_check_result,
    summarize_qa_signal_checks,
    upsert_ask_lightweight_cache,
    upsert_ask_places_cache,
    upsert_list_member,
    update_job_status,
    update_territory_scan_status,
)
from backend.services.enrichment_service import run_diagnostic
from backend.ml.store import persist_saved_diagnostic_response
from backend.services.ask_config import ADAPTIVE_LIMITS_DEFAULTS
from backend.services.npl_service import (
    ai_batch_explain_matches,
    ai_batch_rerank_candidates,
    classify_constraint,
    criterion_cache_key,
    matches_tier1_criteria,
    needs_lightweight_check,
    review_lightweight_match_with_ai,
    run_lightweight_service_page_check,
)
from backend.services.criteria_registry import (
    normalize_accuracy_mode,
    sanitize_criteria,
    sanitize_must_not,
)
from backend.services.ask_agentic_planner import planner_llm_update
from backend.services.territory_service import (
    _build_tier1_rows,
    _fetch_territory_candidates,
    run_list_rescan_job,
    run_territory_scan_job,
)

logger = logging.getLogger(__name__)

_worker_thread: threading.Thread | None = None
_stop_event = threading.Event()

POLL_INTERVAL = 2  # seconds
ASK_PLACES_CACHE_TTL_SECONDS = 15 * 60
ASK_LIGHT_CACHE_TTL_SECONDS = 10 * 60
DEEP_BRIEF_DIAGNOSTIC_TIMEOUT_SECONDS = max(
    60,
    int(os.getenv("DEEP_BRIEF_DIAGNOSTIC_TIMEOUT_SECONDS") or 12 * 60),
)


def _is_fresh_iso(updated_at: str | None, ttl_seconds: int) -> bool:
    if not updated_at:
        return False
    try:
        ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts).total_seconds() <= ttl_seconds
    except Exception:
        return False


def _run_deep_brief_diagnostic_with_timeout(task: dict, deep_audit: bool, timeout_seconds: int) -> dict:
    payload = {
        "business_name": task["business_name"],
        "city": task["city"],
        "state": task.get("state"),
        "website": task.get("website"),
        "deep_audit": deep_audit,
    }
    cmd = [
        sys.executable,
        "-c",
        """
import json
import sys
import traceback

from backend.services.enrichment_service import run_diagnostic

payload = json.loads(sys.stdin.read())
try:
    result = run_diagnostic(
        business_name=str(payload["business_name"]),
        city=str(payload["city"]),
        state=str(payload.get("state") or ""),
        website=payload.get("website"),
        deep_audit=bool(payload.get("deep_audit", True)),
    )
    sys.stdout.write(json.dumps({"ok": True, "result": result}, default=str))
except Exception as exc:
    sys.stdout.write(
        json.dumps(
            {
                "ok": False,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )
    )
    sys.exit(1)
""",
    ]
    try:
        completed = subprocess.run(
            cmd,
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
            cwd=os.getcwd(),
        )
    except subprocess.TimeoutExpired:
        raise TimeoutError(
            f"Deep brief diagnostic timed out after {timeout_seconds}s for "
            f"{task.get('business_name')} ({task.get('city')})"
        )
    raw = (completed.stdout or "").strip()
    if not raw:
        err = (completed.stderr or "").strip()
        raise RuntimeError(
            f"Deep brief diagnostic subprocess returned no result for "
            f"{task.get('business_name')} ({task.get('city')})"
            + (f": {err}" if err else "")
        )
    payload = json.loads(raw)
    if not payload.get("ok"):
        tb = payload.get("traceback")
        if tb:
            logger.error("Deep brief subprocess failed for %s\n%s", task.get("business_name"), tb)
        raise RuntimeError(str(payload.get("error") or "Unknown deep brief subprocess failure"))
    return payload["result"]


def _run_deep_brief_job(job: dict) -> dict:
    job_id = job["id"]
    user_id = job.get("user_id", 1)
    inp = job.get("input", {}) or {}
    job_type = str(job.get("type") or "")
    max_prospects = int(inp.get("max_prospects") or 25)
    max_prospects = max(1, min(max_prospects, 25))
    concurrency = int(inp.get("concurrency") or 3)
    concurrency = max(1, min(concurrency, 5))
    deep_audit = bool(inp.get("deep_audit", True))
    timeout_seconds = max(
        60,
        int(inp.get("diagnostic_timeout_seconds") or DEEP_BRIEF_DIAGNOSTIC_TIMEOUT_SECONDS),
    )

    tasks: list[dict] = []
    if job_type == "territory_deep_scan":
        scan_id = str(inp.get("scan_id") or "")
        requested_prospect_ids = [int(item) for item in (inp.get("prospect_ids") or []) if str(item).strip()]
        requested_order = {prospect_id: idx for idx, prospect_id in enumerate(requested_prospect_ids)}
        rows = list_territory_prospects(scan_id, user_id) if scan_id else []
        if requested_order:
            rows = [r for r in rows if int(r.get("id") or 0) in requested_order]
            rows.sort(key=lambda r: requested_order.get(int(r.get("id") or 0), len(requested_order)))
        for r in rows:
            if r.get("full_brief_ready"):
                continue
            tasks.append(
                {
                    "kind": "territory",
                    "prospect_id": int(r["id"]),
                    "place_id": r.get("place_id"),
                    "business_name": r.get("business_name"),
                    "city": r.get("city"),
                    "state": r.get("state"),
                    "website": r.get("website"),
                }
            )
    elif job_type == "list_deep_briefs":
        list_id = int(inp.get("list_id") or 0)
        lst = get_prospect_list(list_id, user_id) if list_id else None
        rows = list_members_for_list(list_id) if lst else []
        for r in rows:
            resp = r.get("response") or {}
            if isinstance(resp, dict) and resp.get("brief"):
                continue
            tasks.append(
                {
                    "kind": "list",
                    "list_id": list_id,
                    "place_id": r.get("place_id"),
                    "business_name": (resp.get("business_name") or r.get("business_name")),
                    "city": (resp.get("city") or r.get("city")),
                    "state": (resp.get("state") or r.get("state")),
                }
            )
    else:
        return {"processed": 0, "created": 0, "failed": 0, "total": 0}

    tasks = [t for t in tasks if t.get("business_name") and t.get("city")][:max_prospects]
    total = len(tasks)
    if total == 0:
        return {"processed": 0, "created": 0, "failed": 0, "total": 0, "message": "No prospects required deep brief build."}

    def _run_one(t: dict) -> dict:
        result = _run_deep_brief_diagnostic_with_timeout(
            t,
            deep_audit=deep_audit,
            timeout_seconds=timeout_seconds,
        )
        diag_id = save_diagnostic(
            user_id=user_id,
            job_id=job_id,
            place_id=result.get("place_id") or t.get("place_id"),
            business_name=result.get("business_name") or str(t["business_name"]),
            city=result.get("city") or str(t["city"]),
            brief=result.get("brief"),
            response=result,
            state=result.get("state") or t.get("state"),
        )
        persist_saved_diagnostic_response(
            diagnostic_id=int(diag_id),
            place_id=str(result.get("place_id") or t.get("place_id") or ""),
            response=result,
        )
        if t["kind"] == "territory":
            link_territory_prospect_diagnostic(int(t["prospect_id"]), int(diag_id), full_brief_ready=True)
        elif t["kind"] == "list":
            upsert_list_member(
                list_id=int(t["list_id"]),
                diagnostic_id=int(diag_id),
                place_id=t.get("place_id"),
                business_name=result.get("business_name") or str(t["business_name"]),
                city=result.get("city") or str(t["city"]),
                state=result.get("state") or t.get("state"),
            )
        return {"diagnostic_id": int(diag_id), "place_id": t.get("place_id")}

    processed = 0
    created = 0
    failed = 0
    diag_ids: list[int] = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        future_map = {pool.submit(_run_one, t): t for t in tasks}
        for fut in as_completed(future_map):
            processed += 1
            try:
                one = fut.result()
                created += 1
                if one.get("diagnostic_id"):
                    diag_ids.append(int(one["diagnostic_id"]))
            except Exception:
                failed += 1
                logger.exception("Deep brief item failed")

            update_job_status(
                job_id,
                "running",
                result={
                    "phase": "deep_brief_build",
                    "processed": processed,
                    "created": created,
                    "failed": failed,
                    "total": total,
                },
            )

    return {
        "phase": "deep_brief_build",
        "processed": processed,
        "created": created,
        "failed": failed,
        "total": total,
        "diagnostic_ids": diag_ids,
        "message": f"Deep brief build complete: {created}/{total} created ({failed} failed).",
    }


def _criterion_key_for_payload(criterion: dict) -> str:
    ctype = str(criterion.get("type") or "").strip()
    service = str(criterion.get("service") or "").strip()
    if ctype == "missing_service_page" and service:
        return f"{ctype}:{service}"
    return ctype or "unknown"


def _build_match_evidence(row: dict, diagnostic: dict | None, criteria: list[dict] | None) -> list[dict]:
    if not criteria:
        return []

    resp = (diagnostic or {}).get("response") if isinstance(diagnostic, dict) else None
    light_results = row.get("_light_results") if isinstance(row.get("_light_results"), dict) else {}
    ai_reviews = row.get("_ai_reviews") if isinstance(row.get("_ai_reviews"), dict) else {}
    verified_missing = [
        str(s).strip().lower()
        for s in ((resp or {}).get("service_intelligence") or {}).get("missing_services", [])
        if str(s).strip()
    ]
    evidence: list[dict] = []

    for criterion in criteria:
        ctype = str(criterion.get("type") or "").strip()
        if not ctype:
            continue

        service = str(criterion.get("service") or "").strip() or None
        item = {
            "criterion_key": _criterion_key_for_payload(criterion),
            "criterion_type": ctype,
            "service": service,
        }

        if ctype == "missing_service_page":
            service_slug = str(service or "").strip().lower()
            ckey = criterion_cache_key(criterion)
            lw = light_results.get(ckey) if isinstance(light_results, dict) else {}
            ai = ai_reviews.get(ckey) if isinstance(ai_reviews, dict) else {}

            if resp is not None:
                item["source"] = "deep_verified_diagnostic"
                item["matched"] = bool(service_slug) and any(service_slug in missing for missing in verified_missing)
                item["details"] = {
                    "missing_services": verified_missing,
                }
            elif lw:
                item["source"] = "lightweight_check"
                item["matched"] = bool(lw.get("matches"))
                details = {
                    "reason": lw.get("reason"),
                    "method": lw.get("method"),
                    "service": lw.get("service"),
                }
                if isinstance(lw.get("evidence"), dict):
                    details["evidence"] = lw.get("evidence")
                if ai:
                    details["ai_review"] = {
                        "verdict": ai.get("verdict"),
                        "reason": ai.get("reason"),
                    }
                item["details"] = details
            else:
                item["source"] = "inferred"
                item["matched"] = bool(row.get("missing_service_page_match"))
        else:
            item["source"] = "deterministic_signal"
            item["matched"] = matches_tier1_criteria([criterion], row)

        evidence.append(item)

    return evidence


def _match_evidence_level(match_evidence: list[dict]) -> str:
    if any(item.get("matched") and item.get("source") == "deep_verified_diagnostic" for item in match_evidence):
        return "deep_verified"
    if any(item.get("matched") and item.get("source") == "lightweight_check" for item in match_evidence):
        return "lightweight_verified"
    if any(item.get("matched") and item.get("source") == "deterministic_signal" for item in match_evidence):
        return "deterministic"
    return "inferred"


def _build_npl_payload(row: dict, diagnostic: dict | None, criteria: list[dict] | None = None) -> dict:
    resp = (diagnostic or {}).get("response") if isinstance(diagnostic, dict) else None
    match_evidence = _build_match_evidence(row, diagnostic, criteria)
    return {
        "prospect_id": row.get("id"),
        "diagnostic_id": row.get("diagnostic_id"),
        "place_id": row.get("place_id"),
        "business_name": row.get("business_name"),
        "city": row.get("city"),
        "state": row.get("state"),
        "website": row.get("website"),
        "rating": row.get("rating"),
        "user_ratings_total": row.get("user_ratings_total"),
        "rank": row.get("rank"),
        "rank_score": row.get("rank_key"),
        "full_brief_ready": bool(row.get("full_brief_ready")),
        "opportunity_profile": (resp or {}).get("opportunity_profile"),
        "primary_leverage": (resp or {}).get("primary_leverage"),
        "constraint": (resp or {}).get("constraint"),
        "match_evidence_level": _match_evidence_level(match_evidence),
        "match_evidence": match_evidence,
        "ai_review": row.get("ai_review"),
        "ai_rerank": row.get("ai_rerank"),
        "ai_explanation": row.get("ai_explanation"),
    }


def _run_npl_find_job(job: dict) -> dict:
    job_id = job["id"]
    inp = job.get("input", {}) or {}
    intent = inp.get("intent") or {}
    accuracy_mode = normalize_accuracy_mode(inp.get("accuracy_mode") or intent.get("accuracy_mode") or "verified")
    intent["accuracy_mode"] = accuracy_mode
    city = str(intent.get("city") or "").strip()
    state = intent.get("state") or None
    vertical = str(intent.get("vertical") or "dentist").strip()
    limit = int(intent.get("limit") or 10)
    limit = max(1, min(limit, 20))
    raw_criteria = [c for c in (intent.get("criteria") or []) if isinstance(c, dict)]
    raw_must_not = [c for c in (intent.get("must_not") or []) if isinstance(c, dict)]
    criteria, criteria_unsupported = sanitize_criteria(raw_criteria, [])
    must_not, must_not_unsupported = sanitize_must_not(raw_must_not, [])
    criteria = [c for c in criteria if isinstance(c, dict)]
    must_not = [c for c in must_not if isinstance(c, dict)]
    applied_criteria = []
    for c in criteria:
        ctype = str(c.get("type") or "").strip()
        if not ctype:
            continue
        if ctype in {"missing_service_page", "missing_service_page_light"}:
            svc = str(c.get("service") or "").strip()
            applied_criteria.append(f"{ctype}:{svc}" if svc else ctype)
        else:
            applied_criteria.append(ctype)
    unsupported_request_parts = intent.get("unsupported_request_parts") or intent.get("unsupported_parts") or []
    unsupported_request_parts = [str(x).strip() for x in unsupported_request_parts if str(x).strip()]
    unsupported_request_parts.extend(criteria_unsupported)
    unsupported_request_parts.extend(must_not_unsupported)
    execution_overrides = intent.get("execution_overrides") if isinstance(intent.get("execution_overrides"), dict) else {}
    try:
        radius_miles_override = float(execution_overrides.get("radius_miles")) if execution_overrides.get("radius_miles") is not None else None
    except (TypeError, ValueError):
        radius_miles_override = None
    try:
        candidate_cap_override = int(execution_overrides.get("candidate_cap")) if execution_overrides.get("candidate_cap") is not None else None
    except (TypeError, ValueError):
        candidate_cap_override = None
    try:
        verify_top_n_override = int(execution_overrides.get("verify_top_n")) if execution_overrides.get("verify_top_n") is not None else None
    except (TypeError, ValueError):
        verify_top_n_override = None
    try:
        verify_next_n_override = int(execution_overrides.get("verify_next_n")) if execution_overrides.get("verify_next_n") is not None else None
    except (TypeError, ValueError):
        verify_next_n_override = None

    raw_soft_filters = [c for c in (intent.get("soft_filters_rank_only") or []) if isinstance(c, dict)]
    soft_filters, soft_unsupported = sanitize_criteria(raw_soft_filters, [])
    unsupported_request_parts.extend(soft_unsupported)
    missing_service_criteria = [c for c in criteria if c.get("type") == "missing_service_page"]
    requires_light = bool(intent.get("requires_lightweight")) or needs_lightweight_check(criteria)
    requires_verified = accuracy_mode == "verified" and bool(missing_service_criteria)
    scored_cap = min(max(limit * 2, limit), 50)
    if candidate_cap_override is not None:
        scored_cap = max(20, min(candidate_cap_override, 200))

    def _stable_sort_rows(rows_to_sort: list[dict]) -> list[dict]:
        rows_to_sort.sort(
            key=lambda x: (
                -float(x.get("rank_key") or 0),
                -int(x.get("user_ratings_total") or 0),
                str(x.get("place_id") or ""),
            ),
        )
        return rows_to_sort

    update_job_status(
        job_id,
        "running",
        result={
            "phase": "candidate_fetch",
            "intent": intent,
            "accuracy_mode": accuracy_mode,
            "criteria": criteria,
            "soft_filters_rank_only": soft_filters,
            "progress": {"candidates_found": 0, "scored": 0, "list_count": 0},
            "partial_results": [],
        },
    )

    places_cache_key = f"{city.lower()}|{str(state or '').upper()}|{vertical.lower()}|{scored_cap}|{radius_miles_override or 'default'}"
    candidates: list[dict] = []
    candidates_cached = False
    cached_places = get_ask_places_cache(places_cache_key)
    if cached_places and _is_fresh_iso(cached_places.get("updated_at"), ASK_PLACES_CACHE_TTL_SECONDS):
        payload = cached_places.get("data") or {}
        rows_cached = payload.get("candidates") if isinstance(payload, dict) else None
        if isinstance(rows_cached, list):
            candidates = rows_cached
            candidates_cached = True

    if not candidates:
        def _candidate_progress_cb(queries_done: int, queries_total: int, raw_collected: int) -> None:
            update_job_status(
                job_id,
                "running",
                result={
                    "phase": "candidate_fetch",
                    "intent": intent,
                    "accuracy_mode": accuracy_mode,
                    "criteria": criteria,
                    "soft_filters_rank_only": soft_filters,
                    "progress": {
                        "candidate_queries_done": queries_done,
                        "candidate_queries_total": queries_total,
                        "raw_candidates_collected": raw_collected,
                        "candidates_found": 0,
                        "scored": 0,
                        "list_count": 0,
                    },
                    "partial_results": [],
                },
            )

        candidates = _fetch_territory_candidates(
            city=city,
            state=state,
            vertical=vertical,
            limit=scored_cap,
            radius_miles=radius_miles_override,
            progress_cb=_candidate_progress_cb,
        )
        upsert_ask_places_cache(places_cache_key, {"candidates": candidates})

    candidates = candidates[:scored_cap]
    total_candidates = len(candidates)

    update_job_status(
        job_id,
        "running",
        result={
            "phase": "details_scoring",
            "intent": intent,
            "accuracy_mode": accuracy_mode,
            "criteria": criteria,
            "soft_filters_rank_only": soft_filters,
            "progress": {
                "candidates_found": total_candidates,
                "scored": 0,
                "list_count": 0,
                "details_cache_hit": candidates_cached,
            },
            "partial_results": [],
        },
    )

    score_progress = {"processed": 0, "failed": 0}

    def _score_progress_cb(processed: int, failed_count: int) -> None:
        score_progress["processed"] = processed
        score_progress["failed"] = failed_count

    rows, failed_count = _build_tier1_rows(
        candidates,
        city=city,
        state=state,
        filters={},
        progress_cb=_score_progress_cb,
    )
    score_progress["processed"] = len(candidates)
    score_progress["failed"] = failed_count

    ranked_rows = rows[:]
    total_review_count = sum(int(r.get("user_ratings_total") or 0) for r in ranked_rows)
    market_avg_reviews = (float(total_review_count) / float(len(ranked_rows))) if ranked_rows else 0.0
    if market_avg_reviews >= 120:
        market_density = "high"
    elif market_avg_reviews >= 50:
        market_density = "medium"
    else:
        market_density = "low"
    for row in ranked_rows:
        row["avg_market_reviews"] = market_avg_reviews
        row["market_density"] = market_density
        row["primary_constraint"] = classify_constraint(row)
        if soft_filters:
            bonus = 0.0
            for criterion in soft_filters:
                if matches_tier1_criteria([criterion], row):
                    bonus += 1.5
            row["rank_key"] = round(float(row.get("rank_key") or 0) + bonus, 2)

    ranked_rows = _stable_sort_rows(ranked_rows)
    for idx, row in enumerate(ranked_rows, start=1):
        row["rank"] = idx

    filtered_rows: list[dict] = []
    partial_payload: list[dict] = []
    filtered_out_by_criterion: dict[str, int] = {}

    non_missing_criteria = [c for c in criteria if c.get("type") != "missing_service_page"]

    def _criterion_key(c: dict) -> str:
        return str(c.get("type") or "").strip() or "unknown"

    def _passes_non_missing(row: dict) -> bool:
        for criterion in non_missing_criteria:
            if not matches_tier1_criteria([criterion], row):
                k = _criterion_key(criterion)
                filtered_out_by_criterion[k] = int(filtered_out_by_criterion.get(k) or 0) + 1
                return False
        for criterion in must_not:
            if matches_tier1_criteria([criterion], row):
                k = _criterion_key(criterion)
                filtered_out_by_criterion[k] = int(filtered_out_by_criterion.get(k) or 0) + 1
                return False
        return True

    if not requires_light:
        for row in ranked_rows:
            if _passes_non_missing(row):
                filtered_rows.append(row)
                payload = _build_npl_payload(row, diagnostic=None, criteria=criteria)
                partial_payload.append(payload)
                if len(partial_payload) >= limit:
                    break
            if len(partial_payload) and len(partial_payload) % 5 == 0:
                update_job_status(
                    job_id,
                    "running",
                    result={
                        "phase": "filtering",
                        "intent": intent,
                        "accuracy_mode": accuracy_mode,
                        "criteria": criteria,
                        "progress": {
                            "candidates_found": total_candidates,
                            "scored": score_progress["processed"],
                            "failed": score_progress["failed"],
                            "list_count": len(partial_payload),
                        },
                        "partial_results": partial_payload[:],
                    },
                )
    else:
        checked = 0
        with ThreadPoolExecutor(max_workers=6) as pool:
            future_map = {}
            for row in ranked_rows:
                if not _passes_non_missing(row):
                    continue
                place_id = str(row.get("place_id") or "")
                if not place_id:
                    continue
                criterion = missing_service_criteria[0] if missing_service_criteria else {}
                ckey = criterion_cache_key(criterion)
                cached = get_ask_lightweight_cache(place_id, ckey)
                if cached and _is_fresh_iso(cached.get("updated_at"), ASK_LIGHT_CACHE_TTL_SECONDS):
                    lw = cached.get("result") or {}
                    checked += 1
                    if lw.get("matches"):
                        row["missing_service_page_match"] = True
                        filtered_rows.append(row)
                        partial_payload.append(_build_npl_payload(row, diagnostic=None, criteria=criteria))
                    else:
                        filtered_out_by_criterion["missing_service_page"] = int(filtered_out_by_criterion.get("missing_service_page") or 0) + 1
                    continue
                future = pool.submit(run_lightweight_service_page_check, row.get("website"), criterion, 5)
                future_map[future] = (row, ckey)

            for fut in as_completed(future_map):
                row, ckey = future_map[fut]
                checked += 1
                try:
                    lw = fut.result()
                except Exception:
                    lw = {"matches": False}
                place_id = str(row.get("place_id") or "")
                if place_id:
                    upsert_ask_lightweight_cache(place_id, ckey, lw)
                if lw.get("matches"):
                    row["missing_service_page_match"] = True
                    filtered_rows.append(row)
                    partial_payload.append(_build_npl_payload(row, diagnostic=None, criteria=criteria))
                else:
                    filtered_out_by_criterion["missing_service_page"] = int(filtered_out_by_criterion.get("missing_service_page") or 0) + 1
                if checked % 5 == 0 or len(partial_payload) == limit:
                    update_job_status(
                        job_id,
                        "running",
                        result={
                            "phase": "lightweight_check",
                            "intent": intent,
                            "accuracy_mode": accuracy_mode,
                            "criteria": criteria,
                            "progress": {
                                "candidates_found": total_candidates,
                                "scored": score_progress["processed"],
                                "lightweight_checked": checked,
                                "failed": score_progress["failed"],
                                "list_count": min(len(partial_payload), limit),
                            },
                            "partial_results": partial_payload[:limit],
                        },
                    )
                if len(partial_payload) >= limit:
                    break

    verified_progress: dict[str, int] = {"processed": 0, "failed": 0}
    verified_payload: list[dict] = []

    if requires_verified:
        candidates_sorted_for_verify = sorted(
            filtered_rows,
            key=lambda x: (
                -float(x.get("rank_key") or 0),
                -int(x.get("user_ratings_total") or 0),
                str(x.get("place_id") or ""),
            ),
        )
        verify_top_n = min(max(verify_top_n_override or min(max(limit * 3, limit), 25), 1), 60)
        verify_next_n = min(max(verify_next_n_override or 0, 0), 60)
        candidates_for_verify = candidates_sorted_for_verify[:verify_top_n]

        def _verify_row(row: dict) -> dict:
            result = run_diagnostic(
                business_name=str(row.get("business_name") or ""),
                city=str(row.get("city") or ""),
                state=str(row.get("state") or ""),
                website=row.get("website"),
            )
            verified_missing = [
                str(s).strip().lower()
                for s in (result.get("service_intelligence") or {}).get("missing_services", [])
            ]
            must_have_missing = [str(c.get("service") or "").strip().lower() for c in missing_service_criteria]
            verified_match = all(any(ms in vm for vm in verified_missing) for ms in must_have_missing if ms)
            if not verified_match:
                return {"verified_match": False}
            out = _build_npl_payload(
                row,
                diagnostic={"response": result},
                criteria=criteria,
            )
            return {"verified_match": True, "payload": out}

        update_job_status(
            job_id,
            "running",
            result={
                "phase": "verified_diagnostic",
                "intent": intent,
                "accuracy_mode": accuracy_mode,
                "criteria": criteria,
                "progress": {
                    "candidates_found": total_candidates,
                    "scored": score_progress["processed"],
                    "verifying": len(candidates_for_verify),
                    "processed": 0,
                    "failed": 0,
                    "list_count": 0,
                },
                "partial_results": [],
            },
        )

        with ThreadPoolExecutor(max_workers=3) as pool:
            future_map = {pool.submit(_verify_row, r): r for r in candidates_for_verify}
            for fut in as_completed(future_map):
                verified_progress["processed"] += 1
                try:
                    one = fut.result()
                    if one.get("verified_match") and one.get("payload"):
                        verified_payload.append(one["payload"])
                except Exception:
                    verified_progress["failed"] += 1
                    logger.exception("Verified ask row failed")

                update_job_status(
                    job_id,
                    "running",
                    result={
                        "phase": "verified_diagnostic",
                        "intent": intent,
                        "accuracy_mode": accuracy_mode,
                        "criteria": criteria,
                        "progress": {
                            "candidates_found": total_candidates,
                            "scored": score_progress["processed"],
                            "verifying": len(candidates_for_verify),
                            "processed": verified_progress["processed"],
                            "failed": verified_progress["failed"],
                            "list_count": min(len(verified_payload), limit),
                        },
                        "partial_results": verified_payload[:limit],
                    },
                )
                if len(verified_payload) >= limit:
                    break

        if len(verified_payload) < limit and verify_next_n > 0:
            next_batch = candidates_sorted_for_verify[verify_top_n: verify_top_n + verify_next_n]
            with ThreadPoolExecutor(max_workers=3) as pool:
                future_map = {pool.submit(_verify_row, r): r for r in next_batch}
                for fut in as_completed(future_map):
                    verified_progress["processed"] += 1
                    try:
                        one = fut.result()
                        if one.get("verified_match") and one.get("payload"):
                            verified_payload.append(one["payload"])
                    except Exception:
                        verified_progress["failed"] += 1
                        logger.exception("Verified ask row failed (fallback batch)")
                    if len(verified_payload) >= limit:
                        break

        verified_payload.sort(
            key=lambda x: (
                -float(x.get("rank_score") or 0),
                -int(x.get("user_ratings_total") or 0),
                str(x.get("place_id") or ""),
            ),
        )
        prospects = verified_payload[:limit]
        total_matches = len(verified_payload)
    else:
        filtered_rows = _stable_sort_rows(filtered_rows)
        top_rows = filtered_rows[:limit]
        prospects = [_build_npl_payload(r, diagnostic=None, criteria=criteria) for r in top_rows]
        total_matches = len(filtered_rows)

    criterion_that_eliminated_most = None
    if filtered_out_by_criterion:
        criterion_that_eliminated_most = max(
            filtered_out_by_criterion.items(),
            key=lambda kv: int(kv[1] or 0),
        )[0]

    return {
        "phase": "completed",
        "intent": intent,
        "accuracy_mode": accuracy_mode,
        "criteria": criteria,
        "applied_criteria": applied_criteria,
        "must_not": must_not,
        "unsupported_request_parts": unsupported_request_parts,
        "unsupported_parts": unsupported_request_parts,
        "unsupported_message": (
            f"We don't check {', '.join(unsupported_request_parts)}; results were filtered only by supported Neyma criteria."
            if unsupported_request_parts
            else None
        ),
        "requires_lightweight": requires_light,
        "requires_verified": requires_verified,
        "total_matches": total_matches,
        "total_scanned": score_progress["processed"] or total_candidates,
        "filtered_out_by_criterion": filtered_out_by_criterion,
        "criterion_that_eliminated_most": criterion_that_eliminated_most,
        "no_results_suggestion": (
            f"No matches. We scanned {score_progress['processed'] or total_candidates} prospects; most exclusions from {criterion_that_eliminated_most}. Try relaxing {criterion_that_eliminated_most} or expanding the area."
            if total_matches == 0 and criterion_that_eliminated_most
            else None
        ),
        "prospects": prospects[:20],
        "progress": {
            "candidates_found": total_candidates,
            "scored": score_progress["processed"],
            "failed": score_progress["failed"],
            "verified_processed": verified_progress["processed"] if requires_verified else 0,
            "list_count": len(prospects[:20]),
        },
    }


def handle_ask_scan(job: dict) -> dict:
    """Unified adaptive Ask pipeline behind one job type."""
    job_id = job["id"]
    inp = job.get("input", {}) or {}
    intent = dict(inp.get("resolved_intent") or inp.get("intent") or {})
    accuracy_mode = normalize_accuracy_mode(inp.get("accuracy_mode") or intent.get("accuracy_mode") or "fast")
    intent["accuracy_mode"] = accuracy_mode

    update_job_status(
        job_id,
        "running",
        result={
            "phase": "moderating",
            "intent": intent,
            "accuracy_mode": accuracy_mode,
            "progress": {"list_count": 0},
            "partial_results": [],
        },
    )
    update_job_status(
        job_id,
        "running",
        result={
            "phase": "resolving_intent",
            "intent": intent,
            "accuracy_mode": accuracy_mode,
            "progress": {"list_count": 0},
            "partial_results": [],
        },
    )

    city = str(intent.get("city") or "").strip()
    state = intent.get("state") or None
    vertical = str(intent.get("vertical") or "dentist").strip()
    limit = max(1, min(int(inp.get("limit") or intent.get("limit") or 10), 20))

    raw_criteria = [c for c in (inp.get("criteria") or intent.get("criteria") or []) if isinstance(c, dict)]
    raw_must_not = [c for c in (inp.get("must_not") or intent.get("must_not") or []) if isinstance(c, dict)]
    criteria, criteria_unsupported = sanitize_criteria(raw_criteria, [])
    must_not, must_not_unsupported = sanitize_must_not(raw_must_not, [])
    criteria = [c for c in criteria if isinstance(c, dict)]
    must_not = [c for c in must_not if isinstance(c, dict)]

    unsupported = [str(x).strip() for x in (inp.get("unsupported_parts") or intent.get("unsupported_parts") or []) if str(x).strip()]
    unsupported.extend(criteria_unsupported)
    unsupported.extend(must_not_unsupported)
    dedup_unsupported: list[str] = []
    seen_unsupported = set()
    for item in unsupported:
        key = item.lower()
        if key in seen_unsupported:
            continue
        seen_unsupported.add(key)
        dedup_unsupported.append(item)

    adaptive_cfg = dict(ADAPTIVE_LIMITS_DEFAULTS)
    if isinstance(inp.get("adaptive_limits"), dict):
        adaptive_cfg.update(inp.get("adaptive_limits") or {})

    max_iterations = max(1, min(int(adaptive_cfg.get("max_iterations") or 3), 8))
    max_minutes = max(0.2, min(float(adaptive_cfg.get("max_minutes") or 1.5), 45.0))
    radius = max(1.0, float(adaptive_cfg.get("radius_start") or 2.0))
    radius_step = max(0.5, float(adaptive_cfg.get("radius_step") or 1.0))
    max_radius = max(radius, float(adaptive_cfg.get("max_radius") or 6.0))
    cap = max(20, int(adaptive_cfg.get("cap_start") or 150))
    cap_step = max(10, int(adaptive_cfg.get("cap_step") or 100))
    max_cap = max(cap, int(adaptive_cfg.get("max_cap") or 500))
    deep_top_k = max(1, min(int(adaptive_cfg.get("deep_top_k") or 20), 60))
    shortlist_n = max(limit, min(int(adaptive_cfg.get("shortlist_n") or 50), 200))
    lightweight_probe_n = max(limit, min(shortlist_n * 2, 120))
    min_results = max(1, min(int(adaptive_cfg.get("min_results") or limit), 20))
    ask_ai_review_enabled = str(os.getenv("ASK_AI_REVIEW_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
    ask_ai_review_max_per_job = max(0, min(int(os.getenv("ASK_AI_REVIEW_MAX_PER_JOB", "20")), 200))
    ask_ai_rerank_enabled = (
        accuracy_mode != "verified"
        and str(os.getenv("ASK_AI_RERANK_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
    )
    ask_ai_rerank_top_n = max(0, min(int(os.getenv("ASK_AI_RERANK_TOP_N", "20")), 100))
    ask_ai_explain_enabled = str(os.getenv("ASK_AI_EXPLAIN_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
    ask_ai_explain_top_n = max(0, min(int(os.getenv("ASK_AI_EXPLAIN_TOP_N", "10")), 40))
    qa_verify_enabled = str(os.getenv("QA_VERIFY_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
    qa_verify_sample_rate = max(0.0, min(float(os.getenv("QA_VERIFY_SAMPLE_RATE", "0.05")), 1.0))
    qa_verify_max_per_job = max(0, min(int(os.getenv("QA_VERIFY_MAX_PER_JOB", "25")), 200))

    missing_service_criteria = [c for c in criteria if str(c.get("type") or "") == "missing_service_page"]
    requested_deep_verification = bool(inp.get("require_deep_verification"))
    require_deep_verification = requested_deep_verification or (
        accuracy_mode == "verified" and bool(missing_service_criteria)
    )
    lightweight_required = bool(missing_service_criteria) or needs_lightweight_check(criteria)
    if accuracy_mode == "verified" and missing_service_criteria:
        deep_top_k = max(deep_top_k, min(60, max(limit * 3, 20)))

    soft_filters: list[dict] = []
    relaxed_soft = False
    started_at = time.time()
    iterations: list[dict] = []
    all_filtered_out: dict[str, int] = {}
    best_payload: dict[str, dict] = {}
    stop_reason: str | None = None
    qa_evidence_pool: list[dict] = []

    def _stable_sort_rows(rows_to_sort: list[dict]) -> list[dict]:
        rows_to_sort.sort(
            key=lambda x: (
                -float(x.get("rank_key") or 0),
                -int(x.get("user_ratings_total") or 0),
                str(x.get("place_id") or ""),
            ),
        )
        return rows_to_sort

    def _preview_payload(rows_src: list[dict]) -> list[dict]:
        return [_build_npl_payload(r, diagnostic=None, criteria=criteria) for r in rows_src[:limit]]

    for idx in range(1, max_iterations + 1):
        elapsed_minutes = (time.time() - started_at) / 60.0
        if elapsed_minutes > max_minutes:
            stop_reason = "max_minutes_reached"
            break

        iter_started = time.time()
        update_job_status(
            job_id,
            "running",
            result={
                "phase": "discovering_candidates",
                "intent": intent,
                "accuracy_mode": accuracy_mode,
                "progress": {
                    "iteration": idx,
                    "max_iterations": max_iterations,
                    "radius_miles": radius,
                    "candidate_cap": cap,
                    "list_count": min(len(best_payload), limit),
                },
                "partial_results": list(best_payload.values())[:limit],
            },
        )

        cache_key = f"{city.lower()}|{str(state or '').upper()}|{vertical.lower()}|{int(cap)}|{round(radius, 2)}"
        candidates: list[dict] = []
        cached_places = get_ask_places_cache(cache_key)
        if cached_places and _is_fresh_iso(cached_places.get("updated_at"), ASK_PLACES_CACHE_TTL_SECONDS):
            payload = cached_places.get("data") or {}
            rows_cached = payload.get("candidates") if isinstance(payload, dict) else None
            if isinstance(rows_cached, list):
                candidates = rows_cached
        if not candidates:
            candidates = _fetch_territory_candidates(
                city=city,
                state=state,
                vertical=vertical,
                limit=int(cap),
                radius_miles=radius,
                progress_cb=None,
            )
            upsert_ask_places_cache(cache_key, {"candidates": candidates})
        candidates = candidates[: int(cap)]

        rows, failed_count = _build_tier1_rows(
            candidates,
            city=city,
            state=state,
            filters={},
            progress_cb=None,
        )
        ranked_rows = rows[:]
        total_review_count = sum(int(r.get("user_ratings_total") or 0) for r in ranked_rows)
        market_avg_reviews = (float(total_review_count) / float(len(ranked_rows))) if ranked_rows else 0.0
        if market_avg_reviews >= 120:
            market_density = "high"
        elif market_avg_reviews >= 50:
            market_density = "medium"
        else:
            market_density = "low"
        for row in ranked_rows:
            row["avg_market_reviews"] = market_avg_reviews
            row["market_density"] = market_density
            row["primary_constraint"] = classify_constraint(row)
            if soft_filters:
                bonus = 0.0
                for criterion in soft_filters:
                    if matches_tier1_criteria([criterion], row):
                        bonus += 1.5
                row["rank_key"] = round(float(row.get("rank_key") or 0) + bonus, 2)
        ranked_rows = _stable_sort_rows(ranked_rows)

        update_job_status(
            job_id,
            "running",
            result={
                "phase": "lightweight_checks",
                "intent": intent,
                "accuracy_mode": accuracy_mode,
                "progress": {
                    "iteration": idx,
                    "max_iterations": max_iterations,
                    "radius_miles": radius,
                    "candidate_cap": cap,
                    "candidates_found": len(candidates),
                    "scored": len(ranked_rows),
                    "failed": failed_count,
                    "list_count": min(len(best_payload), limit),
                },
                "partial_results": list(best_payload.values())[:limit],
            },
        )

        filtered_rows: list[dict] = []
        filtered_out_by_criterion: dict[str, int] = {}
        non_missing_criteria = [c for c in criteria if str(c.get("type") or "") != "missing_service_page"]

        def _criterion_key(c: dict) -> str:
            ctype = str(c.get("type") or "").strip()
            svc = str(c.get("service") or "").strip()
            return f"{ctype}:{svc}" if ctype == "missing_service_page" and svc else ctype or "unknown"

        def _passes_non_missing(row: dict) -> bool:
            for criterion in non_missing_criteria:
                if not matches_tier1_criteria([criterion], row):
                    k = _criterion_key(criterion)
                    filtered_out_by_criterion[k] = int(filtered_out_by_criterion.get(k) or 0) + 1
                    return False
            for criterion in must_not:
                if matches_tier1_criteria([criterion], row):
                    k = f"must_not:{_criterion_key(criterion)}"
                    filtered_out_by_criterion[k] = int(filtered_out_by_criterion.get(k) or 0) + 1
                    return False
            return True

        candidates_for_light = [r for r in ranked_rows if _passes_non_missing(r)]
        probe_rows = candidates_for_light[:lightweight_probe_n]
        if not lightweight_required:
            filtered_rows = candidates_for_light
        else:
            if not missing_service_criteria:
                filtered_rows = candidates_for_light
            else:
                lightweight_checked = 0
                lightweight_total = len(probe_rows)
                lightweight_cache_hits = 0
                with ThreadPoolExecutor(max_workers=6) as pool:
                    future_map = {}
                    for row in probe_rows:
                        place_id = str(row.get("place_id") or "")
                        if not place_id:
                            filtered_out_by_criterion["missing_service_page"] = int(filtered_out_by_criterion.get("missing_service_page") or 0) + 1
                            continue
                        for criterion in missing_service_criteria:
                            ckey = criterion_cache_key(criterion)
                            cached = get_ask_lightweight_cache(place_id, ckey)
                            if cached and _is_fresh_iso(cached.get("updated_at"), ASK_LIGHT_CACHE_TTL_SECONDS):
                                lw = cached.get("result") or {}
                                row.setdefault("_light_results", {})[ckey] = lw
                                lightweight_cache_hits += 1
                            else:
                                fut = pool.submit(run_lightweight_service_page_check, row.get("website"), criterion, 5)
                                future_map[fut] = (row, ckey, criterion)

                    for fut in as_completed(future_map):
                        row, ckey, criterion = future_map[fut]
                        try:
                            lw = fut.result()
                        except Exception:
                            lw = {"matches": False, "criterion": criterion}
                        place_id = str(row.get("place_id") or "")
                        if place_id:
                            upsert_ask_lightweight_cache(place_id, ckey, lw)
                        row.setdefault("_light_results", {})[ckey] = lw
                        lightweight_checked += 1
                        if (lightweight_checked % 10 == 0) or (lightweight_checked == len(future_map)):
                            update_job_status(
                                job_id,
                                "running",
                                result={
                                    "phase": "lightweight_checks",
                                    "intent": intent,
                                    "accuracy_mode": accuracy_mode,
                                    "progress": {
                                        "iteration": idx,
                                        "max_iterations": max_iterations,
                                        "radius_miles": radius,
                                        "candidate_cap": cap,
                                        "candidates_found": len(candidates),
                                        "scored": len(ranked_rows),
                                        "lightweight_total": lightweight_total,
                                        "lightweight_checked": lightweight_checked,
                                        "lightweight_cache_hits": lightweight_cache_hits,
                                        "list_count": min(len(best_payload), limit),
                                    },
                                    "partial_results": list(best_payload.values())[:limit],
                                },
                            )

                ai_review_total = 0
                ai_review_checked = 0
                ai_review_started = 0
                if ask_ai_review_enabled:
                    def _needs_ai_review(light_res: dict) -> bool:
                        if not bool(light_res.get("matches")):
                            return False
                        reason = str(light_res.get("reason") or "").lower()
                        if reason.startswith("lightweight_http_"):
                            return True
                        return reason in {"no_strong_service_page_signal_in_fast_check", "fast detector failed"}

                    with ThreadPoolExecutor(max_workers=3) as ai_pool:
                        ai_futures = {}
                        for row in probe_rows:
                            website = row.get("website")
                            if not website:
                                continue
                            light_results = row.get("_light_results") or {}
                            for criterion in missing_service_criteria:
                                ckey = criterion_cache_key(criterion)
                                lw = light_results.get(ckey) or {}
                                if not _needs_ai_review(lw):
                                    continue
                                if ai_review_started >= ask_ai_review_max_per_job:
                                    continue
                                ai_review_total += 1
                                ai_review_started += 1
                                fut = ai_pool.submit(
                                    review_lightweight_match_with_ai,
                                    website=website,
                                    criterion=criterion,
                                    lightweight_result=lw,
                                )
                                ai_futures[fut] = (row, ckey)
                        for fut in as_completed(ai_futures):
                            row, ckey = ai_futures[fut]
                            ai_review_checked += 1
                            try:
                                ai = fut.result()
                            except Exception:
                                ai = {"enabled": False, "verdict": "skipped", "reason": "ai_review_failed"}
                            row.setdefault("_ai_reviews", {})[ckey] = ai
                            verdict = str(ai.get("verdict") or "").lower()
                            if verdict == "likely_match":
                                row["rank_key"] = float(row.get("rank_key") or 0) + 1.0
                            elif verdict == "likely_not_match":
                                row["rank_key"] = float(row.get("rank_key") or 0) - 3.0
                            if (ai_review_checked % 5 == 0) or (ai_review_checked == ai_review_total):
                                update_job_status(
                                    job_id,
                                    "running",
                                    result={
                                        "phase": "lightweight_checks",
                                        "intent": intent,
                                        "accuracy_mode": accuracy_mode,
                                        "progress": {
                                            "iteration": idx,
                                            "max_iterations": max_iterations,
                                            "radius_miles": radius,
                                            "candidate_cap": cap,
                                            "candidates_found": len(candidates),
                                            "scored": len(ranked_rows),
                                            "lightweight_total": lightweight_total,
                                            "lightweight_checked": lightweight_total,
                                            "lightweight_cache_hits": lightweight_cache_hits,
                                            "ai_review_total": ai_review_total,
                                            "ai_review_checked": ai_review_checked,
                                            "list_count": min(len(best_payload), limit),
                                        },
                                        "partial_results": list(best_payload.values())[:limit],
                                    },
                                )

                for row in probe_rows:
                    light_results = row.get("_light_results") or {}
                    matches_all = True
                    for criterion in missing_service_criteria:
                        ckey = criterion_cache_key(criterion)
                        lw = light_results.get(ckey) or {}
                        if lw:
                            qa_evidence_pool.append(
                                {
                                    "source_type": "ask",
                                    "source_id": str(job_id),
                                    "place_id": row.get("place_id"),
                                    "website": row.get("website"),
                                    "criterion_key": ckey,
                                    "deterministic_match": bool(lw.get("matches")),
                                    "evidence": {
                                        "reason": lw.get("reason"),
                                        "method": lw.get("method"),
                                        "service": lw.get("service"),
                                        "evidence": lw.get("evidence") if isinstance(lw.get("evidence"), dict) else {},
                                    },
                                }
                            )
                        if not lw.get("matches"):
                            matches_all = False
                            filtered_out_by_criterion[_criterion_key(criterion)] = int(filtered_out_by_criterion.get(_criterion_key(criterion)) or 0) + 1
                            break
                    if matches_all:
                        row["missing_service_page_match"] = True
                        ai_reviews = row.get("_ai_reviews") or {}
                        if ai_reviews:
                            row["ai_review"] = ai_reviews
                        filtered_rows.append(row)
                filtered_rows = _stable_sort_rows(filtered_rows)
                if ask_ai_rerank_enabled and ask_ai_rerank_top_n > 0 and filtered_rows:
                    adjustments = ai_batch_rerank_candidates(
                        rows=filtered_rows,
                        criteria=criteria,
                        purpose="ask",
                        max_items=ask_ai_rerank_top_n,
                    )
                    if adjustments:
                        for row in filtered_rows:
                            pid = str(row.get("place_id") or "")
                            adj = adjustments.get(pid)
                            if not adj:
                                continue
                            delta = float(adj.get("delta") or 0.0)
                            row["rank_key"] = round(float(row.get("rank_key") or 0.0) + delta, 3)
                            row["ai_rerank"] = adj
                        filtered_rows = _stable_sort_rows(filtered_rows)
                if filtered_rows:
                    update_job_status(
                        job_id,
                        "running",
                        result={
                            "phase": "lightweight_checks",
                            "intent": intent,
                            "accuracy_mode": accuracy_mode,
                            "progress": {
                                "iteration": idx,
                                "max_iterations": max_iterations,
                                "radius_miles": radius,
                                "candidate_cap": cap,
                                "candidates_found": len(candidates),
                                "scored": len(ranked_rows),
                                "postfilter_count": len(filtered_rows),
                                "lightweight_total": lightweight_total,
                                "lightweight_checked": lightweight_total,
                                "lightweight_cache_hits": lightweight_cache_hits,
                                "list_count": min(len(filtered_rows), limit),
                            },
                            "partial_results": _preview_payload(filtered_rows[:]),
                        },
                    )

        filtered_rows = _stable_sort_rows(filtered_rows)
        shortlist = filtered_rows[:shortlist_n]

        verified_payload: list[dict] = []
        deep_verified_count = 0
        if require_deep_verification:
            if ((time.time() - started_at) / 60.0) > max_minutes:
                stop_reason = "max_minutes_reached"
                break
            update_job_status(
                job_id,
                "running",
                result={
                    "phase": "deep_verification",
                    "intent": intent,
                    "accuracy_mode": accuracy_mode,
                    "progress": {
                        "iteration": idx,
                        "max_iterations": max_iterations,
                        "radius_miles": radius,
                        "candidate_cap": cap,
                        "verifying": min(len(shortlist), deep_top_k),
                        "list_count": min(len(best_payload), limit),
                    },
                    "partial_results": list(best_payload.values())[:limit],
                },
            )
            verify_rows = shortlist[:deep_top_k]

            def _verify_row(row: dict) -> dict:
                result = run_diagnostic(
                    business_name=str(row.get("business_name") or ""),
                    city=str(row.get("city") or ""),
                    state=str(row.get("state") or ""),
                    website=row.get("website"),
                )
                verified_missing = [str(s).strip().lower() for s in (result.get("service_intelligence") or {}).get("missing_services", [])]
                required_missing = [str(c.get("service") or "").strip().lower() for c in missing_service_criteria]
                verified_match = all(any(req in vm for vm in verified_missing) for req in required_missing if req)
                if not verified_match:
                    return {"verified_match": False}
                payload = _build_npl_payload(row, diagnostic={"response": result}, criteria=criteria)
                return {"verified_match": True, "payload": payload}

            target_needed = max(1, min_results - len(best_payload))
            pool = ThreadPoolExecutor(max_workers=3)
            future_map = {pool.submit(_verify_row, row): row for row in verify_rows}
            checked = 0
            timed_out_mid_deep = False
            try:
                for fut in as_completed(future_map):
                    checked += 1
                    try:
                        one = fut.result()
                        if one.get("verified_match") and one.get("payload"):
                            verified_payload.append(one["payload"])
                    except Exception:
                        logger.exception("Ask deep verification row failed")
                    if (checked % 3 == 0) or (checked == len(verify_rows)):
                        partial = sorted(
                            list(best_payload.values()) + verified_payload,
                            key=lambda x: (
                                -float(x.get("rank_score") or 0),
                                -int(x.get("user_ratings_total") or 0),
                                str(x.get("place_id") or ""),
                            ),
                        )[:limit]
                        update_job_status(
                            job_id,
                            "running",
                            result={
                                "phase": "deep_verification",
                                "intent": intent,
                                "accuracy_mode": accuracy_mode,
                                "progress": {
                                    "iteration": idx,
                                    "max_iterations": max_iterations,
                                    "radius_miles": radius,
                                    "candidate_cap": cap,
                                    "verifying": len(verify_rows),
                                    "deep_checked": checked,
                                    "deep_matches": len(verified_payload),
                                    "list_count": min(len(partial), limit),
                                },
                                "partial_results": partial,
                            },
                        )
                    if len(verified_payload) >= target_needed:
                        break
                    if ((time.time() - started_at) / 60.0) > max_minutes:
                        timed_out_mid_deep = True
                        break
            finally:
                pool.shutdown(wait=False, cancel_futures=True)
            if timed_out_mid_deep:
                stop_reason = "max_minutes_reached"
            deep_verified_count = len(verified_payload)
        else:
            verified_payload = [_build_npl_payload(row, diagnostic=None, criteria=criteria) for row in shortlist]

        for payload in verified_payload:
            pkey = str(payload.get("place_id") or payload.get("business_name") or "")
            if not pkey:
                continue
            existing = best_payload.get(pkey)
            if not existing:
                best_payload[pkey] = payload
                continue
            if float(payload.get("rank_score") or 0) > float(existing.get("rank_score") or 0):
                best_payload[pkey] = payload

        for k, v in filtered_out_by_criterion.items():
            all_filtered_out[k] = int(all_filtered_out.get(k) or 0) + int(v or 0)

        iterations.append(
            {
                "iter": idx,
                "radius": radius,
                "cap": int(cap),
                "prefilter_count": len(ranked_rows),
                "postfilter_count": len(filtered_rows),
                "deep_verified_count": deep_verified_count,
                "elapsed_ms": int((time.time() - iter_started) * 1000),
            }
        )

        update_job_status(
            job_id,
            "running",
            result={
                "phase": "ranking",
                "intent": intent,
                "accuracy_mode": accuracy_mode,
                "iterations": iterations,
                "progress": {
                    "iteration": idx,
                    "max_iterations": max_iterations,
                    "radius_miles": radius,
                    "candidate_cap": cap,
                    "candidates_found": len(candidates),
                    "scored": len(ranked_rows),
                    "postfilter_count": len(filtered_rows),
                    "deep_verified_count": deep_verified_count,
                    "list_count": min(len(best_payload), limit),
                },
                "partial_results": sorted(
                    best_payload.values(),
                    key=lambda x: (
                        -float(x.get("rank_score") or 0),
                        -int(x.get("user_ratings_total") or 0),
                        str(x.get("place_id") or ""),
                    ),
                )[:limit],
            },
        )

        if stop_reason == "max_minutes_reached":
            break

        if len(best_payload) >= min_results:
            stop_reason = "min_results_reached"
            break

        update_job_status(
            job_id,
            "running",
            result={
                "phase": "expanding_search",
                "intent": intent,
                "accuracy_mode": accuracy_mode,
                "iterations": iterations,
                "progress": {
                    "iteration": idx,
                    "max_iterations": max_iterations,
                    "radius_miles": radius,
                    "candidate_cap": cap,
                    "list_count": min(len(best_payload), limit),
                },
                "partial_results": list(best_payload.values())[:limit],
            },
        )

        if radius < max_radius:
            radius = min(max_radius, radius + radius_step)
            continue
        if cap < max_cap:
            cap = min(max_cap, cap + cap_step)
            continue
        if soft_filters and not relaxed_soft:
            soft_filters = []
            relaxed_soft = True
            continue
        stop_reason = "max_cap_reached"
        break

    if stop_reason is None:
        stop_reason = "max_iterations_reached"

    prospects = sorted(
        best_payload.values(),
        key=lambda x: (
            -float(x.get("rank_score") or 0),
            -int(x.get("user_ratings_total") or 0),
            str(x.get("place_id") or ""),
        ),
    )[:limit]

    if ask_ai_explain_enabled and ask_ai_explain_top_n > 0 and prospects:
        explain_map = ai_batch_explain_matches(
            rows=prospects,
            criteria=criteria,
            max_items=ask_ai_explain_top_n,
        )
        if explain_map:
            for p in prospects:
                pid = str(p.get("place_id") or "")
                if pid in explain_map:
                    p["ai_explanation"] = explain_map[pid]

    qa_job_id = None
    qa_sampled = 0
    if qa_verify_enabled and qa_verify_max_per_job > 0 and qa_evidence_pool:
        sampled_rows = [r for r in qa_evidence_pool if random.random() <= qa_verify_sample_rate]
        if len(sampled_rows) > qa_verify_max_per_job:
            sampled_rows = sampled_rows[:qa_verify_max_per_job]
        if sampled_rows:
            check_ids = insert_qa_signal_checks(sampled_rows)
            qa_sampled = len(check_ids)
            if check_ids:
                qa_job_id = create_job(
                    user_id=job.get("user_id", 1),
                    job_type="qa_signal_verification",
                    input_data={"check_ids": check_ids, "source_type": "ask", "source_id": str(job_id)},
                )

    criterion_that_eliminated_most = None
    if all_filtered_out:
        criterion_that_eliminated_most = max(
            all_filtered_out.items(),
            key=lambda kv: int(kv[1] or 0),
        )[0]

    return {
        "phase": "done",
        "intent": intent,
        "accuracy_mode": accuracy_mode,
        "criteria": criteria,
        "must_not": must_not,
        "applied_criteria": [
            (
                f"missing_service_page:{str(c.get('service') or '').strip()}"
                if str(c.get("type") or "") == "missing_service_page"
                else str(c.get("type") or "")
            )
            for c in criteria
            if str(c.get("type") or "").strip()
        ],
        "require_deep_verification": require_deep_verification,
        "unsupported_parts": dedup_unsupported,
        "unsupported_request_parts": dedup_unsupported,
        "unsupported_message": (
            f"We don't check {', '.join(dedup_unsupported)}; results were filtered only by supported Neyma criteria."
            if dedup_unsupported
            else None
        ),
        "prospects": prospects[:20],
        "total_matches": len(best_payload),
        "total_scanned": sum(int(it.get("prefilter_count") or 0) for it in iterations),
        "filtered_out_by_criterion": all_filtered_out,
        "criterion_that_eliminated_most": criterion_that_eliminated_most,
        "no_results_suggestion": (
            f"No matches. We scanned {sum(int(it.get('prefilter_count') or 0) for it in iterations)} prospects; most exclusions from {criterion_that_eliminated_most}. Try expanding the area."
            if len(best_payload) == 0 and criterion_that_eliminated_most
            else None
        ),
        "iterations": iterations,
        "stop_reason": stop_reason,
        "progress": {
            "phase": "done",
            "list_count": len(prospects[:20]),
            "iterations": len(iterations),
        },
        "qa_verification": {
            "sampled": qa_sampled,
            "job_id": qa_job_id,
        },
    }


def _run_qa_signal_verification_job(job: dict) -> dict:
    job_id = str(job.get("id") or "")
    inp = job.get("input", {}) or {}
    check_ids = [int(x) for x in (inp.get("check_ids") or []) if str(x).isdigit()]
    checks = get_qa_signal_checks_by_ids(check_ids)
    total = len(checks)
    processed = 0
    completed = 0
    failed = 0

    for chk in checks:
        cid = int(chk["id"])
        criterion_key = str(chk.get("criterion_key") or "")
        ctype, _, service = criterion_key.partition(":")
        criterion = {"type": ctype, "service": service or None}
        evidence = chk.get("evidence") or {}
        lightweight_result = {
            "matches": bool(int(chk.get("deterministic_match") or 0)),
            "reason": evidence.get("reason"),
            "method": evidence.get("method"),
            "service": evidence.get("service"),
            "evidence": evidence.get("evidence") if isinstance(evidence.get("evidence"), dict) else {},
        }
        try:
            ai = review_lightweight_match_with_ai(
                website=chk.get("website"),
                criterion=criterion,
                lightweight_result=lightweight_result,
            )
            update_qa_signal_check_result(
                cid,
                status="completed",
                ai_verdict=str(ai.get("verdict") or "")[:32],
                ai_confidence=str(ai.get("confidence") or "")[:16],
                ai_reason=str(ai.get("reason") or "")[:240],
                ai_model=str(ai.get("model") or "")[:64] or None,
                error=None,
            )
            completed += 1
        except Exception as exc:
            update_qa_signal_check_result(
                cid,
                status="failed",
                ai_verdict=None,
                ai_confidence=None,
                ai_reason=None,
                ai_model=None,
                error=str(exc)[:240],
            )
            failed += 1
        processed += 1
        if processed % 10 == 0 or processed == total:
            update_job_status(
                job_id,
                "running",
                result={
                    "phase": "qa_signal_verification",
                    "processed": processed,
                    "completed": completed,
                    "failed": failed,
                    "total": total,
                },
            )

    summary = summarize_qa_signal_checks(days=30)
    return {
        "phase": "qa_signal_verification",
        "processed": processed,
        "completed": completed,
        "failed": failed,
        "total": total,
        "summary_30d": summary,
    }


def _criterion_to_key(c: dict) -> str:
    ctype = str(c.get("type") or "").strip()
    svc = str(c.get("service") or "").strip().lower()
    if ctype == "missing_service_page" and svc:
        return f"{ctype}:{svc}"
    return ctype


def _key_to_criterion(key: str) -> dict:
    raw = str(key or "").strip()
    if ":" in raw:
        ctype, svc = raw.split(":", 1)
        return {"type": ctype.strip(), "service": svc.strip() or None}
    return {"type": raw, "service": None}


def _validate_filter_keys(keys: list[str], allowed: set[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for k in keys:
        kk = str(k or "").strip()
        if not kk or kk in seen:
            continue
        if kk in allowed:
            out.append(kk)
            seen.add(kk)
    return out


def _fallback_plan_update(
    plan_state: dict,
    telemetry: dict,
    system_limits: dict,
    core_criteria: set[str],
) -> dict:
    radius = int(plan_state.get("radius_miles") or 12)
    cap = int(plan_state.get("candidate_cap") or 40)
    hard = list(plan_state.get("hard_filters") or [])
    soft = list(plan_state.get("soft_filters_rank_only") or [])
    matched = int(telemetry.get("matched_count") or 0)

    if matched > 0:
        return {
            "plan_update": {
                "radius_miles": radius,
                "candidate_cap": cap,
                "filter_strategy": {
                    "hard_filters": hard,
                    "soft_filters_rank_only": soft,
                    "relaxation_order": [],
                },
                "verification_strategy": {
                    "verify_top_n": int(plan_state.get("verify_top_n") or 5),
                    "fallback_if_too_few_verified": {"verify_next_n": int(plan_state.get("verify_next_n") or 5)},
                },
                "stop_when": {
                    "min_results": int(system_limits.get("min_results") or 5),
                    "max_iterations": int(system_limits.get("max_iterations") or 4),
                    "max_minutes": int(system_limits.get("max_minutes") or 15),
                },
            },
            "stop_reason": None,
        }

    max_radius = int(system_limits.get("max_radius_miles") or 30)
    max_cap = int(system_limits.get("max_candidate_cap") or 120)
    relaxation_order: list[dict] = []

    if radius < max_radius:
        radius = min(max_radius, radius + 5)
        relaxation_order.append({"action": "increase_radius_miles", "to": radius})
    elif cap < max_cap:
        cap = min(max_cap, cap + 20)
        relaxation_order.append({"action": "increase_candidate_cap", "to": cap})
    else:
        movable = [k for k in hard if k not in core_criteria]
        if movable:
            m = movable[0]
            hard = [k for k in hard if k != m]
            if m not in soft:
                soft.append(m)
            relaxation_order.append({"action": "move_hard_to_soft", "criterion": m})

    return {
        "plan_update": {
            "radius_miles": radius,
            "candidate_cap": cap,
            "filter_strategy": {
                "hard_filters": hard,
                "soft_filters_rank_only": soft,
                "relaxation_order": relaxation_order,
            },
            "verification_strategy": {
                "verify_top_n": int(plan_state.get("verify_top_n") or 5),
                "fallback_if_too_few_verified": {"verify_next_n": int(plan_state.get("verify_next_n") or 5)},
            },
            "stop_when": {
                "min_results": int(system_limits.get("min_results") or 5),
                "max_iterations": int(system_limits.get("max_iterations") or 4),
                "max_minutes": int(system_limits.get("max_minutes") or 15),
            },
        },
        "stop_reason": None,
    }


def _run_agentic_scan_job(job: dict) -> dict:
    job_id = job["id"]
    inp = job.get("input", {}) or {}
    normalized_intent = dict(inp.get("intent") or {})
    accuracy_mode = str(inp.get("accuracy_mode") or "fast").strip().lower()
    if accuracy_mode not in {"fast", "verified"}:
        accuracy_mode = "fast"

    limit = max(1, min(int(normalized_intent.get("limit") or 10), 20))
    base_criteria, _ = sanitize_criteria(normalized_intent.get("criteria") or [], [])
    must_not, _ = sanitize_must_not(normalized_intent.get("must_not") or [], [])
    allowed_keys = {_criterion_to_key(c) for c in base_criteria}
    core_criteria = {k for k in allowed_keys if k.startswith("missing_service_page") or k.startswith("primary_constraint_")}

    system_limits = {
        "max_radius_miles": int((inp.get("system_limits") or {}).get("max_radius_miles") or 40),
        "max_candidate_cap": int((inp.get("system_limits") or {}).get("max_candidate_cap") or 140),
        "max_iterations": int((inp.get("system_limits") or {}).get("max_iterations") or 4),
        "max_verify_per_iteration": int((inp.get("system_limits") or {}).get("max_verify_per_iteration") or 10),
        "min_results": int((inp.get("system_limits") or {}).get("min_results") or limit),
        "max_minutes": int((inp.get("system_limits") or {}).get("max_minutes") or 15),
    }

    plan_state = {
        "radius_miles": int((normalized_intent.get("notes_for_executor") or {}).get("radius_miles_hint") or 12),
        "candidate_cap": min(max(limit * 2, 20), system_limits["max_candidate_cap"]),
        "hard_filters": sorted(allowed_keys),
        "soft_filters_rank_only": [],
        "verify_top_n": min(max(limit, 3), system_limits["max_verify_per_iteration"]),
        "verify_next_n": min(max(limit, 3), system_limits["max_verify_per_iteration"]),
    }

    started_at = time.time()
    iterations: list[dict] = []
    best_result: dict | None = None
    stop_reason: str | None = None

    idx = 1
    while idx <= system_limits["max_iterations"]:
        elapsed = time.time() - started_at
        if elapsed / 60.0 > system_limits["max_minutes"]:
            stop_reason = "max_minutes_reached"
            break

        hard_keys = _validate_filter_keys(list(plan_state.get("hard_filters") or []), allowed_keys)
        soft_keys = _validate_filter_keys(list(plan_state.get("soft_filters_rank_only") or []), allowed_keys)
        hard_criteria = [_key_to_criterion(k) for k in hard_keys]
        soft_criteria = [_key_to_criterion(k) for k in soft_keys]

        iter_intent = dict(normalized_intent)
        iter_intent["criteria"] = hard_criteria
        iter_intent["must_not"] = must_not
        iter_intent["soft_filters_rank_only"] = soft_criteria
        iter_intent["execution_overrides"] = {
            "radius_miles": max(1, min(int(plan_state["radius_miles"]), system_limits["max_radius_miles"])),
            "candidate_cap": max(20, min(int(plan_state["candidate_cap"]), system_limits["max_candidate_cap"])),
            "verify_top_n": max(1, min(int(plan_state["verify_top_n"]), system_limits["max_verify_per_iteration"])),
            "verify_next_n": max(0, min(int(plan_state["verify_next_n"]), system_limits["max_verify_per_iteration"])),
        }

        scan_result = _run_npl_find_job({
            "id": job_id,
            "input": {
                "intent": iter_intent,
                "accuracy_mode": accuracy_mode,
            },
        })

        matched_count = int(scan_result.get("total_matches") or 0)
        total_scanned = int(scan_result.get("total_scanned") or 0)
        telemetry = {
            "iteration": idx,
            "candidate_count": total_scanned,
            "matched_count": matched_count,
            "breakdown_by_filter": scan_result.get("filtered_out_by_criterion") or {},
            "time_elapsed_seconds": int(time.time() - started_at),
            "accuracy_mode": accuracy_mode,
            "verification_yield": (matched_count / max(1, total_scanned)) if accuracy_mode == "verified" else None,
        }

        iter_summary = {
            "iteration": idx,
            "radius_miles": iter_intent["execution_overrides"]["radius_miles"],
            "candidate_cap": iter_intent["execution_overrides"]["candidate_cap"],
            "hard_filters": hard_keys,
            "soft_filters_rank_only": soft_keys,
            "telemetry": telemetry,
            "matches": matched_count,
        }
        iterations.append(iter_summary)

        if best_result is None or int(scan_result.get("total_matches") or 0) > int(best_result.get("total_matches") or 0):
            best_result = scan_result

        if total_scanned == 0 and matched_count == 0:
            stop_reason = "no_candidates_from_places_api"
            update_job_status(
                job_id,
                "running",
                result={
                    "phase": "agentic_iteration",
                    "intent": normalized_intent,
                    "accuracy_mode": accuracy_mode,
                    "iterations": iterations,
                    "progress": {
                        "iteration": idx,
                        "candidates_found": 0,
                        "matched_count": 0,
                        "max_iterations": system_limits["max_iterations"],
                        "fetch_stopped_reason": "no_results_after_bounded_queries",
                    },
                    "partial_results": [],
                },
            )
            break

        update_job_status(
            job_id,
            "running",
            result={
                "phase": "agentic_iteration",
                "intent": normalized_intent,
                "accuracy_mode": accuracy_mode,
                "iterations": iterations,
                "progress": {
                    "iteration": idx,
                    "candidates_found": total_scanned,
                    "matched_count": matched_count,
                    "max_iterations": system_limits["max_iterations"],
                },
                "partial_results": (scan_result.get("prospects") or [])[:limit],
            },
        )

        if matched_count >= system_limits["min_results"]:
            stop_reason = "min_results_reached"
            break

        llm_update = planner_llm_update(
            normalized_intent=normalized_intent,
            telemetry=telemetry,
            system_limits=system_limits,
        )
        plan_update = (llm_update.get("plan_update") if isinstance(llm_update, dict) else None) or {}
        if not plan_update:
            plan_update = _fallback_plan_update(plan_state, telemetry, system_limits, core_criteria).get("plan_update") or {}

        fs = plan_update.get("filter_strategy") if isinstance(plan_update.get("filter_strategy"), dict) else {}
        hard_new = _validate_filter_keys([str(x) for x in (fs.get("hard_filters") or [])], allowed_keys) or hard_keys
        soft_new = _validate_filter_keys([str(x) for x in (fs.get("soft_filters_rank_only") or [])], allowed_keys)

        plan_state["radius_miles"] = max(1, min(int(plan_update.get("radius_miles") or plan_state["radius_miles"]), system_limits["max_radius_miles"]))
        plan_state["candidate_cap"] = max(20, min(int(plan_update.get("candidate_cap") or plan_state["candidate_cap"]), system_limits["max_candidate_cap"]))
        plan_state["hard_filters"] = hard_new
        plan_state["soft_filters_rank_only"] = soft_new

        verify_strategy = plan_update.get("verification_strategy") if isinstance(plan_update.get("verification_strategy"), dict) else {}
        if verify_strategy:
            plan_state["verify_top_n"] = max(1, min(int(verify_strategy.get("verify_top_n") or plan_state["verify_top_n"]), system_limits["max_verify_per_iteration"]))
            fallback = verify_strategy.get("fallback_if_too_few_verified") if isinstance(verify_strategy.get("fallback_if_too_few_verified"), dict) else {}
            plan_state["verify_next_n"] = max(1, min(int(fallback.get("verify_next_n") or plan_state["verify_next_n"]), system_limits["max_verify_per_iteration"]))

        stop_when = plan_update.get("stop_when") if isinstance(plan_update.get("stop_when"), dict) else {}
        if stop_when:
            system_limits["min_results"] = max(1, min(int(stop_when.get("min_results") or system_limits["min_results"]), 20))
            system_limits["max_iterations"] = max(1, min(int(stop_when.get("max_iterations") or system_limits["max_iterations"]), 8))
            system_limits["max_minutes"] = max(1, min(int(stop_when.get("max_minutes") or system_limits["max_minutes"]), 45))
        idx += 1

    if stop_reason is None:
        stop_reason = "max_iterations_reached"

    final = best_result or {
        "prospects": [],
        "total_matches": 0,
        "total_scanned": 0,
        "filtered_out_by_criterion": {},
        "criterion_that_eliminated_most": None,
        "no_results_suggestion": "No matches. Try expanding the area or relaxing a filter.",
    }
    final["iterations"] = iterations
    final["stop_reason"] = stop_reason
    final["phase"] = "completed"
    final["agentic"] = {
        "enabled": True,
        "system_limits": system_limits,
        "final_plan": plan_state,
    }
    return final


def _process_job(job: dict) -> None:
    job_id = job["id"]
    user_id = job.get("user_id", 1)
    job_type = job.get("type", "diagnostic")
    inp = job.get("input", {})

    logger.info("Processing %s job %s for user %s", job_type, job_id, user_id)
    update_job_status(job_id, "running")

    try:
        if job_type == "territory_scan":
            result = run_territory_scan_job(job)
            update_job_status(job_id, "completed", result=result)
            logger.info("Territory job %s completed", job_id)
            return

        if job_type == "list_rescan":
            result = run_list_rescan_job(job)
            update_job_status(job_id, "completed", result=result)
            logger.info("List rescan job %s completed", job_id)
            return

        if job_type in {"territory_deep_scan", "list_deep_briefs"}:
            result = _run_deep_brief_job(job)
            update_job_status(job_id, "completed", result=result)
            logger.info("Deep brief job %s completed", job_id)
            return

        if job_type == "qa_signal_verification":
            result = _run_qa_signal_verification_job(job)
            update_job_status(job_id, "completed", result=result)
            logger.info("QA verification job %s completed", job_id)
            return

        if job_type in {"ask_scan", "npl_find", "ask_agentic_scan"}:
            result = handle_ask_scan(job)
            update_job_status(job_id, "completed", result=result)
            logger.info("Ask scan job %s completed", job_id)
            return

        result = run_diagnostic(
            business_name=inp["business_name"],
            city=inp["city"],
            state=inp.get("state"),
            website=inp.get("website"),
            deep_audit=bool(inp.get("deep_audit")),
        )

        diag_id = save_diagnostic(
            user_id=user_id,
            job_id=job_id,
            place_id=result.get("place_id"),
            business_name=result.get("business_name", inp["business_name"]),
            city=result.get("city", inp["city"]),
            brief=result.get("brief"),
            response=result,
            state=result.get("state") or inp.get("state"),
        )
        persist_saved_diagnostic_response(
            diagnostic_id=int(diag_id),
            place_id=str(result.get("place_id") or ""),
            response=result,
        )

        result["diagnostic_id"] = diag_id
        if inp.get("prospect_id"):
            try:
                link_territory_prospect_diagnostic(int(inp["prospect_id"]), diag_id, full_brief_ready=True)
            except Exception:
                logger.exception("Failed linking prospect %s to diagnostic %s", inp.get("prospect_id"), diag_id)

        source_diag_id = inp.get("source_diagnostic_id")
        if source_diag_id is not None:
            try:
                from pipeline.outcome_tracking import auto_record_rerun_outcome, ensure_outcome_tables
                ensure_outcome_tables()
                svc_intel = result.get("service_intelligence") or {}
                rerun_data = {
                    "review_count": result.get("review_count") or result.get("user_ratings_total"),
                    "missing_services": svc_intel.get("missing_services", []),
                    "detected_services": svc_intel.get("detected_services", []),
                    "has_booking": (result.get("conversion_infrastructure") or {}).get("online_booking"),
                    "has_ssl": result.get("signal_has_ssl"),
                    "runs_google_ads": result.get("paid_status", "").lower() in ("active", "running"),
                }
                auto_record_rerun_outcome(int(source_diag_id), result.get("place_id") or "", rerun_data)
                logger.info("Recorded rerun outcome for source diagnostic %s → new %s", source_diag_id, diag_id)
            except Exception:
                logger.exception("Failed recording rerun outcome for source diagnostic %s", source_diag_id)

        update_job_status(job_id, "completed", result=result)
        logger.info("Job %s completed → diagnostic %s", job_id, diag_id)

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("Job %s failed: %s\n%s", job_id, exc, tb)
        if job_type in {"territory_scan", "list_rescan"}:
            scan_id = inp.get("scan_id") or job_id
            try:
                update_territory_scan_status(scan_id, "failed", error=str(exc))
            except Exception:
                logger.exception("Failed to mark scan %s as failed", scan_id)
        update_job_status(job_id, "failed", error=str(exc))


def _worker_loop() -> None:
    logger.info("Job worker started")
    while not _stop_event.is_set():
        try:
            jobs = get_pending_jobs(limit=1)
            if jobs:
                _process_job(jobs[0])
            else:
                _stop_event.wait(timeout=POLL_INTERVAL)
        except Exception:
            logger.exception("Worker loop error")
            _stop_event.wait(timeout=POLL_INTERVAL)
    logger.info("Job worker stopped")


def start_worker() -> None:
    global _worker_thread
    if _worker_thread and _worker_thread.is_alive():
        return
    _stop_event.clear()
    _worker_thread = threading.Thread(target=_worker_loop, daemon=True, name="job-worker")
    _worker_thread.start()


def stop_worker() -> None:
    _stop_event.set()
    if _worker_thread:
        _worker_thread.join(timeout=5)
