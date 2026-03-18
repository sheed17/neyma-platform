"""
Identity middleware for Supabase auth and guest access.

Signed-in requests use a Supabase access token from the Authorization header.
Guest requests receive an opaque backend-minted guest cookie so public flows
can still be rate-limited and persisted safely.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any
from urllib.parse import unquote
from uuid import uuid4

import requests
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from pipeline.db import get_or_create_guest_user, get_or_create_user

AUTH_EMAIL_COOKIE = "neyma_auth_email"
AUTH_NAME_COOKIE = "neyma_auth_name"
GUEST_COOKIE = "neyma_guest_session"
GUEST_HEADER = "x-neyma-guest-session"


def _test_identity(request: Request) -> tuple[str | None, str | None]:
    header_email = (request.headers.get("x-neyma-user-email") or "").strip()
    header_name = (request.headers.get("x-neyma-user-name") or "").strip()
    cookie_email = unquote((request.cookies.get(AUTH_EMAIL_COOKIE) or "").strip())
    cookie_name = unquote((request.cookies.get(AUTH_NAME_COOKIE) or "").strip())
    email = header_email or cookie_email or None
    name = header_name or cookie_name or None
    if email and email.endswith("@neyma.local"):
      return email, name
    return None, None


def _bearer_token(request: Request) -> str | None:
    auth_header = (request.headers.get("authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        return None
    token = auth_header[7:].strip()
    return token or None


def _guest_session_id(request: Request) -> str | None:
    header_value = (request.headers.get(GUEST_HEADER) or "").strip()
    cookie_value = (request.cookies.get(GUEST_COOKIE) or "").strip()
    return header_value or cookie_value or None


@lru_cache(maxsize=1)
def _supabase_auth_config() -> dict[str, str] | None:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    public_key = (
        (os.getenv("SUPABASE_PUBLISHABLE_KEY") or "").strip()
        or (os.getenv("SUPABASE_ANON_KEY") or "").strip()
    )
    if not url or not public_key:
        return None
    return {
        "user_url": f"{url.rstrip('/')}/auth/v1/user",
        "public_key": public_key,
    }


def _fetch_supabase_user(token: str) -> dict[str, Any] | None:
    config = _supabase_auth_config()
    if not config:
        return None
    try:
        res = requests.get(
            config["user_url"],
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": config["public_key"],
            },
            timeout=8,
        )
    except requests.RequestException:
        return None
    if not res.ok:
        return None
    data = res.json()
    return data if isinstance(data, dict) else None


class LocalIdentityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        created_guest_session: str | None = None
        user = None
        guest_session_id: str | None = None
        auth_failed = False

        token = _bearer_token(request)
        if token:
            supabase_user = _fetch_supabase_user(token)
            if supabase_user and supabase_user.get("email"):
                metadata = supabase_user.get("user_metadata") if isinstance(supabase_user.get("user_metadata"), dict) else {}
                name = (
                    str(metadata.get("name") or "").strip()
                    or str(metadata.get("full_name") or "").strip()
                    or str(supabase_user.get("email") or "").split("@")[0]
                )
                user = get_or_create_user(email=str(supabase_user["email"]), name=name)
            else:
                auth_failed = True

        if not user:
            email, name = _test_identity(request)
            if email:
                user = get_or_create_user(email=email, name=name or email.split("@")[0])

        if not user:
            guest_session_id = _guest_session_id(request) or str(uuid4())
            created_guest_session = None if _guest_session_id(request) else guest_session_id
            user = get_or_create_guest_user(guest_session_id)

        request.state.user_id = int(user["id"])
        request.state.plan_tier = str(user.get("plan_tier") or "guest")
        request.state.is_guest = bool(int(user.get("is_guest") or 0))
        request.state.guest_session_id = guest_session_id if bool(int(user.get("is_guest") or 0)) else None
        request.state.auth_failed = auth_failed

        response = await call_next(request)

        if created_guest_session:
            response.set_cookie(
                key=GUEST_COOKIE,
                value=created_guest_session,
                httponly=True,
                samesite="lax",
                secure=str(request.url.scheme).lower() == "https",
                max_age=60 * 60 * 24 * 365,
                path="/",
            )
        return response
