"""
Helpers for access-state lookups and entitlement enforcement.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request

from pipeline.db import consume_usage, get_access_state, get_user


def request_user_id(request: Request) -> int:
    user_id = getattr(request.state, "user_id", None)
    if not user_id:
        raise HTTPException(status_code=401, detail="Missing request identity")
    return int(user_id)


def request_user(request: Request) -> Dict[str, Any]:
    user = get_user(request_user_id(request))
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


def access_state_for_request(request: Request) -> Dict[str, Any]:
    return get_access_state(request_user_id(request))


def _raise_structured_access_error(
    *,
    code: str,
    message: str,
    access: Optional[Dict[str, Any]] = None,
    recommended_cta: Optional[str] = None,
    status_code: int = 403,
) -> None:
    raise HTTPException(
        status_code=status_code,
        detail={
            "code": code,
            "message": message,
            "recommended_cta": recommended_cta,
            "access": access,
        },
    )


def require_signed_in(
    request: Request,
    *,
    message: str = "Create a free account to use this workspace feature.",
) -> Dict[str, Any]:
    access = access_state_for_request(request)
    if access["viewer"]["is_guest"]:
        auth_failed = bool(getattr(request.state, "auth_failed", False))
        _raise_structured_access_error(
            code="SESSION_INVALID" if auth_failed else "AUTH_REQUIRED",
            message="We couldn't finish signing you in. Please sign in again." if auth_failed else message,
            access=access,
            recommended_cta="Log in" if auth_failed else "Sign up",
        )
    return access


def require_workspace_admin(request: Request) -> Dict[str, Any]:
    access = require_signed_in(request)
    role = str((access.get("workspace") or {}).get("role") or "").lower()
    if role not in {"owner", "admin"}:
        _raise_structured_access_error(
            code="FORBIDDEN",
            message="Only workspace admins can manage seats.",
            access=access,
            recommended_cta=None,
        )
    return access


def ensure_feature_available(request: Request, feature_key: str) -> Dict[str, Any]:
    access = access_state_for_request(request)
    feature = str(feature_key).strip().lower()
    if feature == "ask" and access["viewer"]["is_guest"]:
        _raise_structured_access_error(
            code="AUTH_REQUIRED",
            message="Ask Neyma requires a free account.",
            access=access,
            recommended_cta="Sign up",
        )
    remaining = access["remaining"].get(feature)
    if remaining is not None and remaining <= 0:
        _raise_structured_access_error(
            code="FREE_LIMIT_REACHED" if access["plan_tier"] == "free" else "GUEST_LIMIT_REACHED",
            message="Usage limit reached for this feature.",
            access=access,
            recommended_cta=access.get("recommended_cta"),
        )
    return access


def consume_feature(request: Request, feature_key: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        return consume_usage(request_user_id(request), feature_key, metadata=metadata)
    except PermissionError as exc:
        try:
            payload = json.loads(str(exc))
        except json.JSONDecodeError:
            payload = {
                "code": "FORBIDDEN",
                "message": str(exc) or "This action is not available right now.",
                "recommended_cta": None,
                "access": None,
            }
        raise HTTPException(status_code=403, detail=payload) from exc
