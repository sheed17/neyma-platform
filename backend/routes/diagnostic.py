"""
POST /diagnostic — submit async diagnostic job.
"""

import logging
from fastapi import APIRouter, HTTPException, Request

from backend.models.schemas import DiagnosticRequest, JobSubmitResponse
from pipeline.db import create_job

router = APIRouter(prefix="/diagnostic", tags=["diagnostic"])
logger = logging.getLogger(__name__)


@router.post("", response_model=JobSubmitResponse)
def post_diagnostic(body: DiagnosticRequest, request: Request):
    """
    Submit a diagnostic job. Returns immediately with a job_id.
    Poll GET /jobs/{job_id} for status.
    """
    user_id = getattr(request.state, "user_id", 1)

    try:
        job_id = create_job(
            user_id=user_id,
            job_type="diagnostic",
            input_data={
                "business_name": body.business_name.strip(),
                "city": body.city.strip(),
                "state": body.state.strip(),
                "website": body.website.strip() if body.website else None,
                "deep_audit": bool(body.deep_audit),
                "source_diagnostic_id": int(body.source_diagnostic_id) if body.source_diagnostic_id else None,
            },
        )
    except Exception as e:
        logger.exception("Failed to create job: %s", e)
        raise HTTPException(status_code=500, detail="Failed to create job")

    return JobSubmitResponse(job_id=job_id, status="pending")
