"""
GET /jobs/{job_id} — poll job status.
"""

from fastapi import APIRouter, HTTPException, Request

from backend.models.schemas import JobStatusResponse
from pipeline.db import get_job

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.get("/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str, request: Request):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    user_id = getattr(request.state, "user_id", 1)
    if job.get("user_id") != user_id:
        raise HTTPException(status_code=404, detail="Job not found")

    diagnostic_id = None
    if job["status"] == "completed" and job.get("result"):
        diagnostic_id = job["result"].get("diagnostic_id")
    progress = job.get("result") if isinstance(job.get("result"), dict) else None

    return JobStatusResponse(
        job_id=job["id"],
        status=job["status"],
        created_at=job["created_at"],
        completed_at=job.get("completed_at"),
        error=job.get("error"),
        diagnostic_id=diagnostic_id,
        progress=progress,
    )
