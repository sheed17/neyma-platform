"""
Access-state and workspace membership endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from backend.access import access_state_for_request, require_workspace_admin
from pipeline.db import get_workspace, invite_workspace_member, list_workspace_members, remove_workspace_member

router = APIRouter(prefix="/access", tags=["access"])


class InviteMemberRequest(BaseModel):
    email: str
    name: str | None = None
    role: str = Field(default="member")


@router.post("/guest-session")
def bootstrap_guest_session(request: Request):
    return access_state_for_request(request)


@router.get("/me")
def get_access_me(request: Request):
    return access_state_for_request(request)


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
