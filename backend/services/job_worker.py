"""
Background job worker — runs in a daemon thread.

Polls for pending jobs and executes them sequentially.
For production, replace with Celery + Redis.
"""

import logging
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from pipeline.db import (
    get_ask_lightweight_cache,
    get_ask_places_cache,
    get_prospect_list,
    list_members_for_list,
    list_territory_prospects,
    get_pending_jobs,
    link_territory_prospect_diagnostic,
    save_diagnostic,
    upsert_ask_lightweight_cache,
    upsert_ask_places_cache,
    upsert_list_member,
    update_job_status,
    update_territory_scan_status,
)
from backend.services.enrichment_service import run_diagnostic
from backend.services.npl_service import (
    classify_constraint,
    criterion_cache_key,
    matches_tier1_criteria,
    needs_lightweight_check,
    run_lightweight_service_page_check,
)
from backend.services.criteria_registry import sanitize_criteria, sanitize_must_not
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


def _is_fresh_iso(updated_at: str | None, ttl_seconds: int) -> bool:
    if not updated_at:
        return False
    try:
        ts = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts).total_seconds() <= ttl_seconds
    except Exception:
        return False


def _run_deep_brief_job(job: dict) -> dict:
    job_id = job["id"]
    user_id = job.get("user_id", 1)
    inp = job.get("input", {}) or {}
    job_type = str(job.get("type") or "")
    max_prospects = int(inp.get("max_prospects") or 25)
    max_prospects = max(1, min(max_prospects, 25))
    concurrency = int(inp.get("concurrency") or 3)
    concurrency = max(1, min(concurrency, 5))

    tasks: list[dict] = []
    if job_type == "territory_deep_scan":
        scan_id = str(inp.get("scan_id") or "")
        rows = list_territory_prospects(scan_id, user_id) if scan_id else []
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
        result = run_diagnostic(
            business_name=str(t["business_name"]),
            city=str(t["city"]),
            state=str(t.get("state") or ""),
            website=t.get("website"),
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


def _build_npl_payload(row: dict, diagnostic: dict | None) -> dict:
    resp = (diagnostic or {}).get("response") if isinstance(diagnostic, dict) else None
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
    }


def _run_npl_find_job(job: dict) -> dict:
    job_id = job["id"]
    inp = job.get("input", {}) or {}
    intent = inp.get("intent") or {}
    accuracy_mode = str(inp.get("accuracy_mode") or "verified").strip().lower()
    if accuracy_mode not in {"fast", "verified"}:
        accuracy_mode = "verified"
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
                payload = _build_npl_payload(row, diagnostic=None)
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
                        partial_payload.append(_build_npl_payload(row, diagnostic=None))
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
                    partial_payload.append(_build_npl_payload(row, diagnostic=None))
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
        prospects = [_build_npl_payload(r, diagnostic=None) for r in top_rows]
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

        if job_type == "npl_find":
            result = _run_npl_find_job(job)
            update_job_status(job_id, "completed", result=result)
            logger.info("NPL job %s completed", job_id)
            return

        if job_type == "ask_agentic_scan":
            result = _run_agentic_scan_job(job)
            update_job_status(job_id, "completed", result=result)
            logger.info("Agentic ask job %s completed", job_id)
            return

        result = run_diagnostic(
            business_name=inp["business_name"],
            city=inp["city"],
            state=inp.get("state"),
            website=inp.get("website"),
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

        result["diagnostic_id"] = diag_id
        if inp.get("prospect_id"):
            try:
                link_territory_prospect_diagnostic(int(inp["prospect_id"]), diag_id, full_brief_ready=True)
            except Exception:
                logger.exception("Failed linking prospect %s to diagnostic %s", inp.get("prospect_id"), diag_id)
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
