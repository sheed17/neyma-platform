"""
Territory scanning and ranked prospect APIs.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from pipeline.db import (
    add_list_members,
    create_job,
    create_prospect_list,
    create_territory_scan,
    get_diagnostic,
    get_job,
    get_latest_diagnostic_by_place_id,
    get_latest_prospect_statuses,
    get_prospect_list,
    get_territory_prospect,
    get_territory_scan,
    link_territory_prospect_diagnostic,
    list_territory_scans,
    list_territory_prospects,
    list_members_for_list,
    list_prospect_lists,
    record_prospect_status,
    remove_list_member,
    set_territory_prospect_ensure_job,
)

router = APIRouter(tags=["territory"])


class TerritoryFilters(BaseModel):
    has_implant_gap: Optional[bool] = None
    below_review_avg: Optional[bool] = None


class TerritoryCreateRequest(BaseModel):
    city: str
    state: Optional[str] = None
    vertical: str
    limit: int = Field(default=20, ge=1, le=100)
    filters: Optional[TerritoryFilters] = None


class CreateListRequest(BaseModel):
    name: str


class AddMembersRequest(BaseModel):
    diagnostic_ids: List[int]


class OutcomeStatusRequest(BaseModel):
    status: str
    note: Optional[str] = None


class DeepBriefRequest(BaseModel):
    max_prospects: int = Field(default=25, ge=1, le=25)
    concurrency: int = Field(default=3, ge=1, le=5)


class EnsureBriefResponse(BaseModel):
    prospect_id: int
    status: str
    diagnostic_id: Optional[int] = None
    job_id: Optional[str] = None


@router.post("/territory")
def create_territory_scan_endpoint(body: TerritoryCreateRequest, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    scan_id = str(uuid4())
    job_id = create_job(
        user_id=user_id,
        job_type="territory_scan",
        input_data={
            "scan_id": scan_id,
            "city": body.city.strip(),
            "state": (body.state or "").strip() or None,
            "vertical": body.vertical.strip(),
            "limit": body.limit,
            "filters": body.filters.model_dump(exclude_none=True) if body.filters else {},
        },
    )

    create_territory_scan(
        scan_id=scan_id,
        user_id=user_id,
        job_id=job_id,
        city=body.city.strip(),
        state=(body.state or "").strip() or None,
        vertical=body.vertical.strip(),
        limit_count=body.limit,
        filters=body.filters.model_dump(exclude_none=True) if body.filters else {},
        scan_type="territory",
    )

    return {
        "scan_id": scan_id,
        "status": "pending",
        "message": f"Scanning {body.city.strip()} ({body.vertical.strip()})...",
    }


@router.get("/territory/scans")
def get_recent_scans(request: Request, limit: int = 20):
    user_id = getattr(request.state, "user_id", 1)
    clamped = max(1, min(limit, 100))
    return {"items": list_territory_scans(user_id=user_id, limit=clamped)}


@router.get("/territory/{scan_id}")
def get_scan_status(scan_id: str, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    scan = get_territory_scan(scan_id, user_id)
    if not scan:
        raise HTTPException(status_code=404, detail="Scan not found")
    return {
        "scan_id": scan_id,
        "status": scan.get("status"),
        "created_at": scan.get("created_at"),
        "completed_at": scan.get("completed_at"),
        "summary": scan.get("summary") or {},
        "error": scan.get("error"),
    }


@router.get("/territory/{scan_id}/results")
def get_scan_results(scan_id: str, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    scan = get_territory_scan(scan_id, user_id)
    if not scan:
        raise HTTPException(status_code=404, detail="Scan not found")

    ranked: List[Dict[str, Any]] = []
    if (scan.get("scan_type") or "territory") == "territory":
        rows = list_territory_prospects(scan_id, user_id)
        ranked = [_tier1_row_to_payload(r) for r in rows]
        diag_ids = [int(r["diagnostic_id"]) for r in ranked if r.get("diagnostic_id")]
        statuses = get_latest_prospect_statuses(diag_ids)
        for row in ranked:
            d_id = row.get("diagnostic_id")
            row["outcome_status"] = statuses.get(int(d_id)) if d_id else None
    else:
        # list_rescan polling only needs status/summary; keep compatibility.
        ranked = []

    return {
        "scan_id": scan_id,
        "city": scan.get("city"),
        "state": scan.get("state"),
        "vertical": scan.get("vertical"),
        "status": scan.get("status"),
        "created_at": scan.get("created_at"),
        "completed_at": scan.get("completed_at"),
        "summary": scan.get("summary") or {},
        "error": scan.get("error"),
        "prospects": ranked,
    }


@router.post("/territory/prospects/{prospect_id}/ensure-brief", response_model=EnsureBriefResponse)
def ensure_full_brief(prospect_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    prospect = get_territory_prospect(prospect_id, user_id)
    if not prospect:
        raise HTTPException(status_code=404, detail="Prospect not found")

    diagnostic_id = prospect.get("diagnostic_id")
    if diagnostic_id and prospect.get("full_brief_ready"):
        return EnsureBriefResponse(prospect_id=prospect_id, status="ready", diagnostic_id=int(diagnostic_id))

    place_id = str(prospect.get("place_id") or "")
    if place_id:
        existing = get_latest_diagnostic_by_place_id(user_id, place_id)
        if existing:
            link_territory_prospect_diagnostic(prospect_id, int(existing["id"]), full_brief_ready=True)
            return EnsureBriefResponse(prospect_id=prospect_id, status="ready", diagnostic_id=int(existing["id"]))

    ensure_job_id = prospect.get("ensure_job_id")
    if ensure_job_id:
        job = get_job(str(ensure_job_id))
        if job:
            if job.get("status") == "completed" and job.get("result") and job["result"].get("diagnostic_id"):
                diag_id = int(job["result"]["diagnostic_id"])
                link_territory_prospect_diagnostic(prospect_id, diag_id, full_brief_ready=True)
                return EnsureBriefResponse(prospect_id=prospect_id, status="ready", diagnostic_id=diag_id)
            if job.get("status") in {"pending", "running"}:
                return EnsureBriefResponse(prospect_id=prospect_id, status="building", job_id=str(ensure_job_id))
        set_territory_prospect_ensure_job(prospect_id, None)

    business_name = str(prospect.get("business_name") or "").strip()
    city = str(prospect.get("city") or "").strip()
    state = str(prospect.get("state") or "").strip()
    if not business_name or not city:
        raise HTTPException(status_code=400, detail="Prospect is missing business/city for full diagnostic")

    job_id = create_job(
        user_id=user_id,
        job_type="diagnostic",
        input_data={
            "prospect_id": prospect_id,
            "business_name": business_name,
            "city": city,
            "state": state,
            "website": prospect.get("website"),
            "deep_audit": True,
        },
    )
    set_territory_prospect_ensure_job(prospect_id, job_id)
    return EnsureBriefResponse(prospect_id=prospect_id, status="building", job_id=job_id)


@router.post("/lists")
def create_list(body: CreateListRequest, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="List name is required")
    list_id = create_prospect_list(user_id=user_id, name=name)
    return {"id": list_id, "name": name}


@router.get("/lists")
def get_lists(request: Request):
    user_id = getattr(request.state, "user_id", 1)
    return {"items": list_prospect_lists(user_id)}


@router.post("/lists/{list_id}/members")
def add_members(list_id: int, body: AddMembersRequest, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    lst = get_prospect_list(list_id, user_id)
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")

    members: List[Dict[str, Any]] = []
    for diag_id in body.diagnostic_ids:
        diag = get_diagnostic(diag_id, user_id)
        if not diag:
            continue
        members.append(
            {
                "diagnostic_id": diag_id,
                "place_id": diag.get("place_id"),
                "business_name": diag.get("business_name"),
                "city": diag.get("city"),
                "state": diag.get("state"),
            }
        )

    added = add_list_members(list_id, members)
    return {"added": added}


@router.get("/lists/{list_id}/members")
def get_members(list_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    lst = get_prospect_list(list_id, user_id)
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")

    rows = list_members_for_list(list_id)
    prospects = []
    for row in rows:
        resp = row.get("response") or {}
        prospects.append(
            {
                "diagnostic_id": row["diagnostic_id"],
                "place_id": row.get("place_id"),
                "business_name": resp.get("business_name") or row.get("business_name"),
                "city": resp.get("city") or row.get("city"),
                "state": resp.get("state") or row.get("state"),
                "revenue_band": ((resp.get("brief") or {}).get("market_position") or {}).get("revenue_band"),
                "primary_leverage": (((resp.get("brief") or {}).get("executive_diagnosis") or {}).get("primary_leverage") or resp.get("primary_leverage")),
                "constraint": (((resp.get("brief") or {}).get("executive_diagnosis") or {}).get("constraint") or resp.get("constraint")),
                "review_position_summary": resp.get("review_position"),
                "brief_url": f"/diagnostic/{row['diagnostic_id']}",
                "added_at": row.get("added_at"),
            }
        )

    statuses = get_latest_prospect_statuses([int(r["diagnostic_id"]) for r in prospects])
    for row in prospects:
        row["outcome_status"] = statuses.get(int(row["diagnostic_id"]))

    return {"list": {"id": lst["id"], "name": lst["name"]}, "members": prospects}


@router.delete("/lists/{list_id}/members/{diagnostic_id}")
def delete_member(list_id: int, diagnostic_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    lst = get_prospect_list(list_id, user_id)
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")
    deleted = remove_list_member(list_id, diagnostic_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Member not found")
    return {"deleted": True}


@router.post("/lists/{list_id}/rescan")
def rescan_list(list_id: int, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    lst = get_prospect_list(list_id, user_id)
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")
    scan_id = str(uuid4())

    job_id = create_job(
        user_id=user_id,
        job_type="list_rescan",
        input_data={"scan_id": scan_id, "list_id": list_id},
    )
    create_territory_scan(
        scan_id=scan_id,
        user_id=user_id,
        job_id=job_id,
        city="",
        state=None,
        vertical="list_rescan",
        limit_count=0,
        filters={},
        scan_type="list_rescan",
        list_id=list_id,
    )

    return {"scan_id": scan_id, "status": "pending", "message": f"Re-scanning list {lst['name']}..."}


@router.post("/territory/{scan_id}/deep-scan")
def start_territory_deep_scan(
    scan_id: str,
    body: DeepBriefRequest,
    request: Request,
):
    user_id = getattr(request.state, "user_id", 1)
    scan = get_territory_scan(scan_id, user_id)
    if not scan:
        raise HTTPException(status_code=404, detail="Scan not found")

    job_id = create_job(
        user_id=user_id,
        job_type="territory_deep_scan",
        input_data={
            "scan_id": scan_id,
            "max_prospects": body.max_prospects,
            "concurrency": body.concurrency,
            "deep_audit": True,
        },
    )
    return {
        "job_id": job_id,
        "status": "pending",
        "message": "Building full briefs in background (10–25 min).",
    }


@router.post("/lists/{list_id}/deep-briefs")
def start_list_deep_briefs(
    list_id: int,
    body: DeepBriefRequest,
    request: Request,
):
    user_id = getattr(request.state, "user_id", 1)
    lst = get_prospect_list(list_id, user_id)
    if not lst:
        raise HTTPException(status_code=404, detail="List not found")

    job_id = create_job(
        user_id=user_id,
        job_type="list_deep_briefs",
        input_data={
            "list_id": list_id,
            "max_prospects": body.max_prospects,
            "concurrency": body.concurrency,
            "deep_audit": True,
        },
    )
    return {
        "job_id": job_id,
        "status": "pending",
        "message": "Building full briefs for list in background (10–25 min).",
    }


@router.post("/diagnostics/{diagnostic_id}/outcome")
def mark_outcome(diagnostic_id: int, body: OutcomeStatusRequest, request: Request):
    user_id = getattr(request.state, "user_id", 1)
    diag = get_diagnostic(diagnostic_id, user_id)
    if not diag:
        raise HTTPException(status_code=404, detail="Diagnostic not found")

    allowed = {"contacted", "closed_won", "closed_lost"}
    status = body.status.strip().lower()
    if status not in allowed:
        raise HTTPException(status_code=400, detail="Invalid status")

    record_prospect_status(diagnostic_id=diagnostic_id, status=status, note=body.note)
    return {"diagnostic_id": diagnostic_id, "status": status, "note": body.note}


def _tier1_row_to_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = row.get("tier1_snapshot") or {}
    phone = snapshot.get("phone")
    email = snapshot.get("email")
    website = _normalize_website_for_display(row.get("website") or snapshot.get("website"))
    avg_rating = snapshot.get("avg_market_rating")
    avg_reviews = snapshot.get("avg_market_reviews")
    rating = row.get("rating")
    reviews = row.get("user_ratings_total")
    below_review_avg = False
    if avg_reviews is not None and reviews is not None:
        try:
            below_review_avg = float(reviews) < float(avg_reviews)
        except (TypeError, ValueError):
            below_review_avg = False
    ai_expl = str(snapshot.get("ai_explanation") or "").strip() or None
    if ai_expl:
        key_signal = ai_expl
    elif rating is not None and avg_rating is not None:
        key_signal = f"Rating {float(rating):.1f} vs local avg {float(avg_rating):.1f}"
    elif not row.get("has_website"):
        key_signal = "No website"
    else:
        key_signal = "Infrastructure gap"
    lead_quality = None
    lead_quality_raw = snapshot.get("lead_quality") if isinstance(snapshot.get("lead_quality"), dict) else None
    if not lead_quality_raw and (row.get("lead_quality_class") or row.get("lead_quality_score") is not None):
        lead_quality_raw = {
            "class": row.get("lead_quality_class"),
            "score": row.get("lead_quality_score"),
            "data_confidence": row.get("lead_data_confidence"),
            "model_version": row.get("lead_model_version"),
            "feature_version": row.get("lead_feature_version"),
            "feature_scope": "tier1",
            "reasons": row.get("lead_quality_reasons") or [],
        }
    if isinstance(lead_quality_raw, dict):
        lead_quality = {
            "class": lead_quality_raw.get("class"),
            "score": lead_quality_raw.get("score"),
            "data_confidence": lead_quality_raw.get("data_confidence"),
            "model_version": lead_quality_raw.get("model_version"),
            "feature_version": lead_quality_raw.get("feature_version"),
            "feature_scope": lead_quality_raw.get("feature_scope") or "tier1",
            "reasons": [
                {
                    "code": reason.get("code"),
                    "label": reason.get("label"),
                }
                for reason in (lead_quality_raw.get("reasons") or [])
                if isinstance(reason, dict) and reason.get("code") and reason.get("label")
            ][:2],
        }
    return {
        "prospect_id": row["id"],
        "diagnostic_id": row.get("diagnostic_id"),
        "place_id": row.get("place_id"),
        "rank": row.get("rank"),
        "rank_score": row.get("rank_key"),
        "business_name": row.get("business_name"),
        "city": row.get("city"),
        "state": row.get("state"),
        "website": website,
        "phone": phone,
        "email": email,
        "key_signal": key_signal,
        "rating": row.get("rating"),
        "user_ratings_total": row.get("user_ratings_total"),
        "revenue_band": None,
        "modeled_revenue_upside": None,
        "primary_leverage": "Review gap" if below_review_avg else "Infrastructure gap",
        "constraint": "—",
        "opportunity_profile": "—",
        "review_position_summary": row.get("review_position_summary"),
        "full_brief_ready": bool(row.get("full_brief_ready")),
        "brief_url": f"/diagnostic/{row['diagnostic_id']}" if row.get("diagnostic_id") else None,
        "tier1_signals": {
            "has_website": bool(row.get("has_website")),
            "ssl": row.get("ssl") if row.get("ssl") is not None else snapshot.get("ssl"),
            "has_contact_form": row.get("has_contact_form") if row.get("has_contact_form") is not None else snapshot.get("has_contact_form"),
            "has_phone": bool(row.get("has_phone")),
            "has_viewport": row.get("has_viewport") if row.get("has_viewport") is not None else snapshot.get("has_viewport"),
            "has_schema": row.get("has_schema") if row.get("has_schema") is not None else snapshot.get("has_schema"),
            "has_booking": snapshot.get("has_automated_scheduling"),
            "booking_conversion_path": snapshot.get("booking_conversion_path"),
            "capture_verification": snapshot.get("capture_verification"),
        },
        "lead_quality": lead_quality,
        "ai_explanation": ai_expl,
        "ai_rerank": snapshot.get("ai_rerank"),
    }


def _normalize_website_for_display(raw: Any) -> Optional[str]:
    if not raw:
        return None
    val = str(raw).strip()
    if not val:
        return None
    if not val.startswith(("http://", "https://")):
        val = f"https://{val}"
    try:
        parts = urlsplit(val)
        scheme = parts.scheme or "https"
        netloc = (parts.netloc or "").lower()
        path = parts.path or ""
        if path.endswith("/") and path != "/":
            path = path.rstrip("/")
        return urlunsplit((scheme, netloc, path, "", ""))
    except Exception:
        return val
