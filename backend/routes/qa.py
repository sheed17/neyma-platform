"""QA verification reporting endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from pipeline.db import list_qa_signal_checks, summarize_qa_signal_checks

router = APIRouter(prefix="/qa", tags=["qa"])


@router.get("/signals/summary")
def qa_signal_summary(
    request: Request,  # keeps auth middleware path consistent
    days: int = Query(default=30, ge=1, le=365),
):
    _ = getattr(request.state, "user_id", 1)
    return summarize_qa_signal_checks(days=days)


@router.get("/signals/checks")
def qa_signal_checks(
    request: Request,  # keeps auth middleware path consistent
    limit: int = Query(default=200, ge=1, le=1000),
    source_type: str | None = None,
):
    _ = getattr(request.state, "user_id", 1)
    return {"items": list_qa_signal_checks(limit=limit, source_type=source_type)}

