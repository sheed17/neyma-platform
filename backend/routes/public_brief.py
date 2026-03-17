"""
Public read-only brief endpoint via share token.
"""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from backend.routes.diagnostics import _response_from_saved
from pipeline.db import get_diagnostic_any_user, get_share_token_record

router = APIRouter(tags=["public-brief"])


@router.get("/brief/s/{token}")
def get_shared_brief(token: str):
    record = get_share_token_record(token)
    if not record:
        raise HTTPException(status_code=404, detail="Share link not found")

    expires_at = record.get("expires_at")
    if expires_at:
        try:
            if datetime.fromisoformat(str(expires_at)) < datetime.now(timezone.utc):
                raise HTTPException(status_code=404, detail="Share link expired")
        except ValueError:
            raise HTTPException(status_code=404, detail="Share link invalid")

    diag = get_diagnostic_any_user(int(record["diagnostic_id"]))
    if not diag:
        raise HTTPException(status_code=404, detail="Diagnostic not found")
    return _response_from_saved(diag.get("response") or {})
