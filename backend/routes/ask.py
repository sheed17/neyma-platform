"""
Natural-language prospect finder endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from backend.services.moderation import moderate_text
from backend.services.npl_service import resolve_ask_intent
from pipeline.db import create_job, get_job, get_latest_diagnostic_by_place_id

router = APIRouter(tags=["ask"])
MISSING_LOCATION_MSG = "Please include city and state in your query (e.g. 'Find 10 dentists in San Jose, CA')."


class AskRequest(BaseModel):
    query: str
    accuracy_mode: str | None = None
    confirmed_low_confidence: bool = False


class AskEnsureBriefRequest(BaseModel):
    place_id: str | None = None
    business_name: str
    city: str
    state: str | None = None
    website: str | None = None


class AskAgenticRequest(BaseModel):
    query: str
    accuracy_mode: str | None = None
    confirmed_low_confidence: bool = False
    min_results: int | None = None
    max_iterations: int | None = None
    max_minutes: int | None = None


@router.post("/ask")
def ask_find(body: AskRequest, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    # Moderate user input; reject without echoing content.
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
    if not intent.get("criteria"):
        raise HTTPException(
            status_code=400,
            detail="No valid supported criteria found. Try criteria like below_review_avg, missing_service_page, no_contact_form, or high_competition_density.",
        )

    accuracy_mode = (body.accuracy_mode or intent.get("accuracy_mode") or "fast").strip().lower()
    if accuracy_mode not in {"fast", "verified"}:
        raise HTTPException(status_code=400, detail="accuracy_mode must be 'fast' or 'verified'")

    low_conf = str(intent.get("intent_confidence") or "").lower() == "low"
    if low_conf and not body.confirmed_low_confidence:
        return {
            "job_id": None,
            "status": "requires_confirmation",
            "intent": intent,
            "message": "Query interpretation confidence is low. Please confirm before running.",
            "accuracy_mode": accuracy_mode,
            "requires_confirmation": True,
        }

    job_id = create_job(
        user_id=user_id,
        job_type="npl_find",
        input_data={"query": body.query, "intent": intent, "accuracy_mode": accuracy_mode},
    )
    return {
        "job_id": job_id,
        "status": "pending",
        "intent": intent,
        "message": (
            f"Verifying {intent.get('vertical', 'prospects')} in {intent.get('city')}{', ' + str(intent.get('state')) if intent.get('state') else ''}..."
            if accuracy_mode == "verified"
            else f"Finding {intent.get('vertical', 'prospects')} in {intent.get('city')}{', ' + str(intent.get('state')) if intent.get('state') else ''}..."
        ),
        "accuracy_mode": accuracy_mode,
        "requires_confirmation": False,
    }


@router.get("/ask/jobs/{job_id}/results")
def ask_results(job_id: str, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    job = get_job(job_id)
    if not job or job.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("type") not in {"npl_find", "ask_agentic_scan"}:
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


@router.post("/ask/agentic")
def ask_agentic(body: AskAgenticRequest, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    is_safe, reject_message = moderate_text(body.query)
    if not is_safe:
        raise HTTPException(status_code=400, detail=reject_message)
    try:
        intent = resolve_ask_intent(body.query)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if intent.get("missing_required"):
        raise HTTPException(status_code=400, detail=MISSING_LOCATION_MSG)
    if not intent.get("criteria"):
        raise HTTPException(
            status_code=400,
            detail="No valid supported criteria found. Try criteria like below_review_avg, missing_service_page, no_contact_form, or high_competition_density.",
        )

    accuracy_mode = (body.accuracy_mode or intent.get("accuracy_mode") or "fast").strip().lower()
    if accuracy_mode not in {"fast", "verified"}:
        raise HTTPException(status_code=400, detail="accuracy_mode must be 'fast' or 'verified'")

    low_conf = str(intent.get("intent_confidence") or "").lower() == "low"
    if low_conf and not body.confirmed_low_confidence:
        return {
            "job_id": None,
            "status": "requires_confirmation",
            "intent": intent,
            "message": "Query interpretation confidence is low. Please confirm before running.",
            "accuracy_mode": accuracy_mode,
            "requires_confirmation": True,
        }

    system_limits = {
        "max_radius_miles": 40,
        "max_candidate_cap": 140,
        "max_iterations": max(1, min(int(body.max_iterations or 4), 8)),
        "max_verify_per_iteration": 10,
        "min_results": max(1, min(int(body.min_results or int(intent.get("limit") or 10)), 20)),
        "max_minutes": max(1, min(int(body.max_minutes or 15), 45)),
    }
    job_id = create_job(
        user_id=user_id,
        job_type="ask_agentic_scan",
        input_data={
            "query": body.query,
            "intent": intent,
            "accuracy_mode": accuracy_mode,
            "system_limits": system_limits,
        },
    )
    return {
        "job_id": job_id,
        "status": "pending",
        "intent": intent,
        "message": (
            f"Agentic scan started for {intent.get('vertical', 'prospects')} in {intent.get('city')}{', ' + str(intent.get('state')) if intent.get('state') else ''}."
        ),
        "accuracy_mode": accuracy_mode,
        "requires_confirmation": False,
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
        },
    )
    return {
        "status": "building",
        "job_id": job_id,
    }
