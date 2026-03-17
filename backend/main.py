"""
FastAPI backend for lead scoring engine.
"""

import os
from pathlib import Path

# Load .env from project root so OPENAI_API_KEY, GOOGLE_PLACES_API_KEY, etc. are set
# when starting the server (no need to export in shell).
try:
    from dotenv import load_dotenv
    _project_root = Path(__file__).resolve().parent.parent
    load_dotenv(_project_root / ".env")
except ImportError:
    pass

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.middleware.auth import LocalIdentityMiddleware
from backend.routes.access import router as access_router
from backend.routes.diagnostic import router as diagnostic_router
from backend.routes.jobs import router as jobs_router
from backend.routes.diagnostics import router as diagnostics_router
from backend.routes.outcomes import router as outcomes_router
from backend.routes.territory import router as territory_router
from backend.routes.public_brief import router as public_brief_router
from backend.routes.ask import router as ask_router
from backend.routes.qa import router as qa_router
from backend.services.job_worker import start_worker, stop_worker
from pipeline.db import init_db

RUN_EMBEDDED_WORKER = os.getenv("RUN_EMBEDDED_WORKER", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]


def _get_cors_origins() -> list[str]:
    raw = os.getenv("CORS_ORIGINS", "")
    extra = [origin.strip() for origin in raw.split(",") if origin.strip()]
    return list(dict.fromkeys([*DEFAULT_CORS_ORIGINS, *extra]))


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if RUN_EMBEDDED_WORKER:
        start_worker()
    yield
    if RUN_EMBEDDED_WORKER:
        stop_worker()


app = FastAPI(
    title="Lead Scoring Engine API",
    description="Diagnostic endpoint for business lead enrichment",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(LocalIdentityMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(diagnostic_router)
app.include_router(jobs_router)
app.include_router(diagnostics_router)
app.include_router(outcomes_router)
app.include_router(territory_router)
app.include_router(public_brief_router)
app.include_router(ask_router)
app.include_router(qa_router)
app.include_router(access_router)


@app.get("/health")
def health():
    """Health check endpoint."""
    return {"status": "ok"}
