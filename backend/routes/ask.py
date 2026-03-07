"""
Natural-language prospect finder endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from backend.services.ask_config import ADAPTIVE_LIMITS_DEFAULTS
from backend.services.criteria_registry import sanitize_criteria, sanitize_must_not
from backend.services.moderation import moderate_text
from backend.services.npl_service import resolve_ask_intent
from pipeline.db import create_job, get_job, get_latest_diagnostic_by_place_id

router = APIRouter(tags=["ask"])
MISSING_LOCATION_MSG = "Please include city and state in your query (e.g. 'Find 10 dentists in San Jose, CA')."


class AskRequest(BaseModel):
    query: str
    confirmed_low_confidence: bool = False


class AskEnsureBriefRequest(BaseModel):
    place_id: str | None = None
    business_name: str
    city: str
    state: str | None = None
    website: str | None = None


@router.post("/ask")
def ask_find(body: AskRequest, request: Request):
    user_id = getattr(request.state, "user_id", 1)

    is_safe, reject_message = moderate_text(body.query)
    if not is_safe:
        raise HTTPException(status_code=400, detail=reject_message)

    try:
        intent = resolve_ask_intent(body.query)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    missing_required = intent.get("missing_required") or []
    if missing_required:
        raise HTTPException(status_code=400, detail=MISSING_LOCATION_MSG)

    raw_criteria = [c for c in (intent.get("criteria") or []) if isinstance(c, dict)]
    raw_must_not = [c for c in (intent.get("must_not") or []) if isinstance(c, dict)]
    criteria, criteria_unsupported = sanitize_criteria(raw_criteria, [])
    must_not, must_not_unsupported = sanitize_must_not(raw_must_not, [])

    if not criteria:
        raise HTTPException(
            status_code=400,
            detail="No valid supported criteria found. Try criteria like below_review_avg, missing_service_page, no_contact_form, or high_competition_density.",
        )

    unsupported = [str(x).strip() for x in (intent.get("unsupported_parts") or []) if str(x).strip()]
    unsupported.extend(criteria_unsupported)
    unsupported.extend(must_not_unsupported)
    dedup_unsupported: list[str] = []
    seen = set()
    for item in unsupported:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        dedup_unsupported.append(item)

    confidence = str(intent.get("intent_confidence") or "").lower()
    if confidence == "low" and not body.confirmed_low_confidence:
        return {
            "job_id": None,
            "status": "requires_confirmation",
            "requires_confirmation": True,
            "normalized_intent": intent,
            "confidence": "low",
            "question": "Query interpretation confidence is low. Do you want to run this query anyway?",
            "message": "Query interpretation confidence is low. Please confirm before running.",
            "unsupported_parts": dedup_unsupported,
        }

    limit = max(1, min(int(intent.get("limit") or 10), 20))
    # Ask pipeline returns lightweight-matched shortlist fast.
    # Deep verification is deferred to on-demand brief generation.
    require_deep_verification = False
    adaptive_limits = dict(ADAPTIVE_LIMITS_DEFAULTS)
    adaptive_limits["min_results"] = limit

    job_id = create_job(
        user_id=user_id,
        job_type="ask_scan",
        input_data={
            "original_query": body.query,
            "resolved_intent": intent,
            "criteria": criteria,
            "must_not": must_not,
            "limit": limit,
            "require_deep_verification": require_deep_verification,
            "adaptive_limits": adaptive_limits,
            "unsupported_parts": dedup_unsupported,
        },
    )

    return {
        "job_id": job_id,
        "status": "pending",
        "message": f"Finding {intent.get('vertical', 'prospects')} in {intent.get('city')}{', ' + str(intent.get('state')) if intent.get('state') else ''}...",
        "requires_confirmation": False,
    }


@router.get("/ask/jobs/{job_id}/results")
def ask_results(job_id: str, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    job = get_job(job_id)
    if not job or job.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("type") not in {"ask_scan", "npl_find", "ask_agentic_scan"}:
        raise HTTPException(status_code=400, detail="Not an ask job")
    if job.get("status") != "completed":
        return {
            "job_id": job_id,
            "status": job.get("status"),
            "result": job.get("result") or None,
        }
    return {
        "job_id": job_id,
        "status": "completed",
        "result": job.get("result") or {},
    }


@router.post("/ask/prospects/ensure-brief")
def ask_ensure_brief(body: AskEnsureBriefRequest, request: Request):
    """Create/ensure a full diagnostic for one Ask row on demand."""
    user_id = getattr(request.state, "user_id", 1)
    place_id = str(body.place_id or "").strip() or None
    business_name = body.business_name.strip()
    city = body.city.strip()
    state = (body.state or "").strip() or None

    if not business_name or not city:
        raise HTTPException(status_code=400, detail="business_name and city are required")

    if place_id:
        existing = get_latest_diagnostic_by_place_id(user_id, place_id)
        if existing:
            return {
                "status": "ready",
                "diagnostic_id": int(existing["id"]),
            }

    job_id = create_job(
        user_id=user_id,
        job_type="diagnostic",
        input_data={
            "place_id": place_id,
            "business_name": business_name,
            "city": city,
            "state": state,
            "website": body.website,
            "deep_audit": True,
        },
    )
    return {
        "status": "building",
        "job_id": job_id,
    }
