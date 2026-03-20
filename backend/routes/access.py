"""
Access-state and workspace membership endpoints.
"""

from __future__ import annotations

import os

import requests
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.access import access_state_for_request, require_signed_in, require_workspace_admin, request_user
from backend.middleware.auth import _bearer_token, _fetch_supabase_user
from pipeline.db import delete_user_account, get_workspace, invite_workspace_member, list_workspace_members, remove_workspace_member

router = APIRouter(prefix="/access", tags=["access"])


class InviteMemberRequest(BaseModel):
    email: str
    name: str | None = None
    role: str = Field(default="member")


class DeleteAccountRequest(BaseModel):
    confirmation: str = Field(min_length=1)


@router.post("/guest-session")
def bootstrap_guest_session(request: Request):
    payload = access_state_for_request(request)
    if getattr(request.state, "is_guest", False):
        payload["guest_session_id"] = getattr(request.state, "guest_session_id", None)
    return payload


@router.get("/me")
def get_access_me(request: Request):
    payload = access_state_for_request(request)
    if getattr(request.state, "is_guest", False):
        payload["guest_session_id"] = getattr(request.state, "guest_session_id", None)
    return payload


def _delete_supabase_auth_user(supabase_auth_user_id: str) -> None:
    service_role_key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    supabase_url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    if not service_role_key or not supabase_url:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "ACCOUNT_DELETION_UNAVAILABLE",
                "message": "Account deletion isn't ready right now. Please contact support.",
                "recommended_cta": "Email support",
            },
        )
    try:
        res = requests.delete(
            f"{supabase_url}/auth/v1/admin/users/{supabase_auth_user_id}",
            headers={
                "apikey": service_role_key,
                "Authorization": f"Bearer {service_role_key}",
            },
            timeout=12,
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "ACCOUNT_DELETION_FAILED",
                "message": "We couldn't finish account deletion right now. Please try again.",
                "recommended_cta": "Try again",
            },
        ) from exc
    if res.status_code >= 300:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "ACCOUNT_DELETION_FAILED",
                "message": "We couldn't finish account deletion right now. Please try again.",
                "recommended_cta": "Try again",
            },
        )


@router.delete("/me")
def delete_access_me(body: DeleteAccountRequest, request: Request):
    require_signed_in(request, message="Create a free account before managing account settings.")
    if body.confirmation.strip().upper() != "DELETE":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "CONFIRMATION_REQUIRED",
                "message": "Type DELETE to confirm account removal.",
                "recommended_cta": None,
            },
        )

    token = _bearer_token(request)
    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "SESSION_INVALID",
                "message": "We couldn't confirm your session. Please sign in again.",
                "recommended_cta": "Log in",
            },
        )
    supabase_user = _fetch_supabase_user(token)
    if not supabase_user or not supabase_user.get("id"):
        raise HTTPException(
            status_code=401,
            detail={
                "code": "SESSION_INVALID",
                "message": "We couldn't confirm your session. Please sign in again.",
                "recommended_cta": "Log in",
            },
        )

    user = request_user(request)
    if str(user.get("email") or "").strip().lower() != str(supabase_user.get("email") or "").strip().lower():
        raise HTTPException(
            status_code=409,
            detail={
                "code": "SESSION_MISMATCH",
                "message": "We couldn't verify this account against your session. Please sign in again.",
                "recommended_cta": "Log in",
            },
        )

    try:
        _delete_supabase_auth_user(str(supabase_user["id"]))
        deleted = delete_user_account(int(user["id"]))
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "ACCOUNT_DELETION_BLOCKED",
                "message": str(exc),
                "recommended_cta": "Email support",
            },
        ) from exc

    return {"deleted": bool(deleted)}


@router.get("/workspace/members")
def get_workspace_members(request: Request):
    access = require_workspace_admin(request)
    workspace = access.get("workspace") or {}
    return {"items": list_workspace_members(int(workspace["id"]))}


@router.post("/workspace/members")
def add_workspace_member(body: InviteMemberRequest, request: Request):
    access = require_workspace_admin(request)
    workspace = access.get("workspace") or {}
    workspace_row = get_workspace(int(workspace["id"]))
    if not workspace_row:
        raise HTTPException(status_code=404, detail="Workspace not found")
    if str(workspace_row.get("plan_tier") or "").lower() != "team":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "TEAM_REQUIRED",
                "message": "Team seats are only available on the team plan.",
                "recommended_cta": "Contact sales",
                "access": access,
            },
        )
    seat_limit = int(workspace_row.get("seat_limit") or 0)
    seat_count = int(workspace_row.get("active_members") or 0)
    if seat_limit and seat_count >= seat_limit:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "SEAT_LIMIT_REACHED",
                "message": "All paid seats are already in use.",
                "recommended_cta": "Contact sales",
                "access": access,
            },
        )
    invited = invite_workspace_member(
        int(workspace["id"]),
        email=body.email.strip(),
        name=(body.name or "").strip() or None,
        role=body.role.strip().lower() or "member",
    )
    return {"member": invited}


@router.delete("/workspace/members/{user_id}")
def delete_workspace_member(user_id: int, request: Request):
    access = require_workspace_admin(request)
    workspace = access.get("workspace") or {}
    deleted = remove_workspace_member(int(workspace["id"]), int(user_id))
    if not deleted:
        raise HTTPException(status_code=404, detail="Member not found")
    return {"removed": True}
