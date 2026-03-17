"""
SQLite persistence for Context-First Opportunity Intelligence.

Stores runs, leads, signals, context dimensions, and lead embeddings (Phase 2 RAG).
"""

import os
import sqlite3
import json
import uuid
import hashlib
import logging
from collections import Counter
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - optional until Postgres env is configured
    psycopg = None
    dict_row = None

logger = logging.getLogger(__name__)

# Default path; override with OPPORTUNITY_DB_PATH
DEFAULT_DB_DIR = "data"
DEFAULT_DB_NAME = "opportunity_intelligence.db"

ACCESS_PG_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS workspaces (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    plan_tier TEXT NOT NULL DEFAULT 'free',
    seat_limit INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT UNIQUE,
    name TEXT,
    plan_tier TEXT NOT NULL DEFAULT 'free',
    workspace_id BIGINT REFERENCES workspaces(id) ON DELETE SET NULL,
    seat_role TEXT NOT NULL DEFAULT 'owner',
    seat_status TEXT NOT NULL DEFAULT 'active',
    is_guest INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_members (
    workspace_id BIGINT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (workspace_id, user_id)
);

CREATE TABLE IF NOT EXISTS guest_sessions (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS access_entitlements (
    scope_type TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    plan_tier TEXT NOT NULL,
    seat_limit INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (scope_type, scope_id)
);

CREATE TABLE IF NOT EXISTS usage_events (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    workspace_id BIGINT REFERENCES workspaces(id) ON DELETE SET NULL,
    guest_session_id TEXT REFERENCES guest_sessions(id) ON DELETE SET NULL,
    feature_key TEXT NOT NULL,
    period_key TEXT NOT NULL,
    event_json JSONB,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS usage_counters (
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    period_key TEXT NOT NULL,
    territory_scans_used INTEGER NOT NULL DEFAULT 0,
    diagnostics_used INTEGER NOT NULL DEFAULT 0,
    ask_queries_used INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (subject_type, subject_id, period_key)
);

CREATE INDEX IF NOT EXISTS idx_users_workspace_id ON users(workspace_id);
CREATE INDEX IF NOT EXISTS idx_users_plan_tier ON users(plan_tier);
CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace_id ON workspace_members(workspace_id);
CREATE INDEX IF NOT EXISTS idx_workspace_members_user_id ON workspace_members(user_id);
CREATE INDEX IF NOT EXISTS idx_guest_sessions_user_id ON guest_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_usage_events_user_id ON usage_events(user_id);
CREATE INDEX IF NOT EXISTS idx_usage_events_feature_key ON usage_events(feature_key);
"""

RUNTIME_PG_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    config JSONB,
    leads_count INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    run_stats JSONB
);

CREATE TABLE IF NOT EXISTS leads (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    place_id TEXT NOT NULL,
    name TEXT,
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    raw_place_json JSONB,
    dentist_profile_v1_json JSONB,
    llm_reasoning_layer_json JSONB,
    sales_intervention_intelligence_json JSONB,
    objective_decision_layer_json JSONB,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, place_id)
);

CREATE TABLE IF NOT EXISTS lead_signals (
    id BIGSERIAL PRIMARY KEY,
    lead_id BIGINT NOT NULL UNIQUE REFERENCES leads(id) ON DELETE CASCADE,
    signals_json JSONB NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS context_dimensions (
    id BIGSERIAL PRIMARY KEY,
    lead_id BIGINT NOT NULL UNIQUE REFERENCES leads(id) ON DELETE CASCADE,
    dimensions_json JSONB NOT NULL,
    reasoning_summary TEXT NOT NULL,
    priority_suggestion TEXT,
    primary_themes_json JSONB,
    outreach_angles_json JSONB,
    overall_confidence DOUBLE PRECISION,
    reasoning_source TEXT DEFAULT 'deterministic',
    no_opportunity INTEGER,
    no_opportunity_reason TEXT,
    priority_derivation TEXT,
    validation_warnings JSONB,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    id BIGSERIAL PRIMARY KEY,
    lead_id BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    agency_type TEXT,
    signals_snapshot JSONB,
    verdict TEXT,
    confidence DOUBLE PRECISION,
    reasoning TEXT,
    primary_risks JSONB,
    what_would_change JSONB,
    prompt_version TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lead_embeddings_v2 (
    lead_id BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    embedding_json JSONB NOT NULL,
    text_snapshot TEXT,
    embedding_version TEXT NOT NULL,
    embedding_type TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (lead_id, embedding_version, embedding_type)
);

CREATE TABLE IF NOT EXISTS lead_docs_v1 (
    id BIGSERIAL PRIMARY KEY,
    lead_id BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    doc_type TEXT NOT NULL,
    content_text TEXT NOT NULL,
    metadata_json JSONB,
    embedding_version TEXT,
    embedding_type TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lead_intel_v1 (
    id BIGSERIAL PRIMARY KEY,
    lead_id BIGINT NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    vertical TEXT,
    primary_constraint TEXT,
    primary_leverage TEXT,
    contact_priority TEXT,
    outreach_angle TEXT,
    confidence DOUBLE PRECISION,
    risks_json JSONB,
    evidence_json JSONB,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS territory_tier1_cache (
    place_id TEXT PRIMARY KEY,
    details_json JSONB,
    website_signals_json JSONB,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ask_places_cache (
    cache_key TEXT PRIMARY KEY,
    data_json JSONB NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ask_lightweight_cache (
    place_id TEXT NOT NULL,
    criterion_key TEXT NOT NULL,
    result_json JSONB NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (place_id, criterion_key)
);

CREATE TABLE IF NOT EXISTS qa_signal_checks (
    id BIGSERIAL PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    place_id TEXT,
    website TEXT,
    criterion_key TEXT NOT NULL,
    deterministic_match INTEGER NOT NULL DEFAULT 0,
    evidence_json JSONB,
    ai_verdict TEXT,
    ai_confidence TEXT,
    ai_reason TEXT,
    ai_model TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_snapshots (
    id BIGSERIAL PRIMARY KEY,
    place_id TEXT NOT NULL,
    review_count INTEGER NOT NULL,
    rating DOUBLE PRECISION,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_feature_snapshots (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    place_id TEXT NOT NULL,
    feature_scope TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    feature_json JSONB NOT NULL,
    feature_hash TEXT NOT NULL,
    data_confidence DOUBLE PRECISION,
    signal_completeness_ratio DOUBLE PRECISION,
    created_at TEXT NOT NULL,
    UNIQUE(entity_type, entity_id, feature_version)
);

CREATE TABLE IF NOT EXISTS ml_predictions (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    place_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    label_version TEXT NOT NULL,
    calibration_version TEXT,
    score DOUBLE PRECISION NOT NULL,
    score_0_100 DOUBLE PRECISION NOT NULL,
    predicted_class TEXT NOT NULL,
    prob_bad DOUBLE PRECISION,
    prob_decent DOUBLE PRECISION,
    prob_good DOUBLE PRECISION,
    prob_high_value DOUBLE PRECISION,
    data_confidence DOUBLE PRECISION,
    reasons_json JSONB,
    caveats_json JSONB,
    components_json JSONB,
    top_features_json JSONB,
    created_at TEXT NOT NULL,
    UNIQUE(entity_type, entity_id, model_name, model_version)
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    type TEXT NOT NULL DEFAULT 'diagnostic',
    status TEXT NOT NULL DEFAULT 'pending',
    input_json JSONB,
    result_json JSONB,
    error TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS diagnostics (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    place_id TEXT,
    business_name TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT '',
    lead_quality_score DOUBLE PRECISION,
    lead_quality_class TEXT,
    lead_model_version TEXT,
    lead_feature_version TEXT,
    lead_quality_json JSONB,
    brief_json JSONB,
    response_json JSONB NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS territory_scans (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    job_id TEXT REFERENCES jobs(id) ON DELETE SET NULL,
    scan_type TEXT NOT NULL DEFAULT 'territory',
    city TEXT,
    state TEXT,
    vertical TEXT,
    limit_count INTEGER NOT NULL DEFAULT 20,
    filters_json JSONB,
    list_id BIGINT,
    status TEXT NOT NULL DEFAULT 'pending',
    summary_json JSONB,
    error TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS territory_scan_diagnostics (
    id BIGSERIAL PRIMARY KEY,
    scan_id TEXT NOT NULL REFERENCES territory_scans(id) ON DELETE CASCADE,
    diagnostic_id BIGINT NOT NULL REFERENCES diagnostics(id) ON DELETE CASCADE,
    place_id TEXT,
    business_name TEXT,
    city TEXT,
    state TEXT,
    previous_diagnostic_id BIGINT,
    change_json JSONB,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS territory_prospects (
    id BIGSERIAL PRIMARY KEY,
    scan_id TEXT NOT NULL REFERENCES territory_scans(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL,
    place_id TEXT NOT NULL,
    business_name TEXT NOT NULL,
    city TEXT,
    state TEXT,
    website TEXT,
    rating DOUBLE PRECISION,
    user_ratings_total INTEGER,
    has_website INTEGER DEFAULT 0,
    ssl INTEGER,
    has_contact_form INTEGER,
    has_phone INTEGER DEFAULT 0,
    has_viewport INTEGER,
    has_schema INTEGER,
    rank_key DOUBLE PRECISION DEFAULT 0,
    rank INTEGER,
    review_position_summary TEXT,
    lead_quality_score DOUBLE PRECISION,
    lead_quality_class TEXT,
    lead_quality_reasons_json JSONB,
    lead_model_version TEXT,
    lead_feature_version TEXT,
    lead_data_confidence DOUBLE PRECISION,
    tier1_snapshot_json JSONB,
    diagnostic_id BIGINT REFERENCES diagnostics(id) ON DELETE SET NULL,
    full_brief_ready INTEGER DEFAULT 0,
    ensure_job_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(scan_id, place_id)
);

CREATE TABLE IF NOT EXISTS prospect_lists (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prospect_list_members (
    id BIGSERIAL PRIMARY KEY,
    list_id BIGINT NOT NULL REFERENCES prospect_lists(id) ON DELETE CASCADE,
    diagnostic_id BIGINT NOT NULL REFERENCES diagnostics(id) ON DELETE CASCADE,
    place_id TEXT,
    business_name TEXT,
    city TEXT,
    state TEXT,
    added_at TEXT NOT NULL,
    UNIQUE(list_id, place_id)
);

CREATE TABLE IF NOT EXISTS diagnostic_predictions (
    diagnostic_id BIGINT PRIMARY KEY REFERENCES diagnostics(id) ON DELETE CASCADE,
    place_id TEXT NOT NULL,
    predictions_json JSONB NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS diagnostic_outcomes (
    id BIGSERIAL PRIMARY KEY,
    diagnostic_id BIGINT NOT NULL REFERENCES diagnostics(id) ON DELETE CASCADE,
    outcome_type TEXT NOT NULL,
    outcome_json JSONB NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS brief_share_tokens (
    id BIGSERIAL PRIMARY KEY,
    diagnostic_id BIGINT NOT NULL REFERENCES diagnostics(id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL,
    token TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    expires_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_id);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_leads_run_id ON leads(run_id);
CREATE INDEX IF NOT EXISTS idx_leads_place_id ON leads(place_id);
CREATE INDEX IF NOT EXISTS idx_lead_signals_lead_id ON lead_signals(lead_id);
CREATE INDEX IF NOT EXISTS idx_context_dimensions_lead_id ON context_dimensions(lead_id);
CREATE INDEX IF NOT EXISTS idx_decisions_lead_id ON decisions(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_docs_v1_lead_id ON lead_docs_v1(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_docs_v1_doc_type ON lead_docs_v1(doc_type);
CREATE INDEX IF NOT EXISTS idx_lead_intel_v1_lead_id ON lead_intel_v1(lead_id);
CREATE INDEX IF NOT EXISTS idx_territory_tier1_cache_updated ON territory_tier1_cache(updated_at);
CREATE INDEX IF NOT EXISTS idx_ask_places_cache_updated ON ask_places_cache(updated_at);
CREATE INDEX IF NOT EXISTS idx_ask_lightweight_cache_updated ON ask_lightweight_cache(updated_at);
CREATE INDEX IF NOT EXISTS idx_qa_signal_checks_created ON qa_signal_checks(created_at);
CREATE INDEX IF NOT EXISTS idx_qa_signal_checks_source ON qa_signal_checks(source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_qa_signal_checks_status ON qa_signal_checks(status);
CREATE INDEX IF NOT EXISTS idx_review_snapshots_place ON review_snapshots(place_id, created_at);
CREATE INDEX IF NOT EXISTS idx_ml_feature_snapshots_entity ON ml_feature_snapshots(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_ml_feature_snapshots_place ON ml_feature_snapshots(place_id, feature_scope, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_entity ON ml_predictions(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_ml_predictions_place ON ml_predictions(place_id, model_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_diagnostics_user ON diagnostics(user_id);
CREATE INDEX IF NOT EXISTS idx_scans_user ON territory_scans(user_id);
CREATE INDEX IF NOT EXISTS idx_scans_status ON territory_scans(status);
CREATE INDEX IF NOT EXISTS idx_scan_diagnostics_scan ON territory_scan_diagnostics(scan_id);
CREATE INDEX IF NOT EXISTS idx_territory_prospects_scan ON territory_prospects(scan_id);
CREATE INDEX IF NOT EXISTS idx_territory_prospects_place ON territory_prospects(place_id);
CREATE INDEX IF NOT EXISTS idx_list_members_list ON prospect_list_members(list_id);
CREATE INDEX IF NOT EXISTS idx_outcomes_diagnostic ON diagnostic_outcomes(diagnostic_id);
CREATE INDEX IF NOT EXISTS idx_predictions_place ON diagnostic_predictions(place_id);
CREATE INDEX IF NOT EXISTS idx_brief_share_diag ON brief_share_tokens(diagnostic_id);
CREATE INDEX IF NOT EXISTS idx_brief_share_token ON brief_share_tokens(token);
"""


def get_db_path() -> str:
    """Return path to SQLite DB file."""
    path = os.getenv("OPPORTUNITY_DB_PATH")
    if path:
        return path
    os.makedirs(DEFAULT_DB_DIR, exist_ok=True)
    return os.path.join(DEFAULT_DB_DIR, DEFAULT_DB_NAME)


def get_access_db_url() -> str | None:
    return (
        os.getenv("ACCESS_DATABASE_URL")
        or os.getenv("SUPABASE_DB_URL")
        or os.getenv("DATABASE_URL")
        or None
    )


def use_access_postgres() -> bool:
    return bool(get_access_db_url() and psycopg is not None)


def get_runtime_db_url() -> str | None:
    return (
        os.getenv("CORE_DATABASE_URL")
        or os.getenv("ACCESS_DATABASE_URL")
        or os.getenv("SUPABASE_DB_URL")
        or os.getenv("DATABASE_URL")
        or None
    )


def use_runtime_postgres() -> bool:
    return bool(get_runtime_db_url() and psycopg is not None)


def _get_access_conn():
    db_url = get_access_db_url()
    if not db_url or psycopg is None:
        raise RuntimeError("Postgres access store is not configured")
    conn = psycopg.connect(db_url, row_factory=dict_row)
    conn.autocommit = False
    return conn


def _init_access_postgres() -> None:
    if not use_access_postgres():
        return
    conn = _get_access_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(ACCESS_PG_SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def _get_runtime_conn():
    db_url = get_runtime_db_url()
    if not db_url or psycopg is None:
        raise RuntimeError("Postgres runtime store is not configured")
    conn = psycopg.connect(db_url, row_factory=dict_row)
    conn.autocommit = False
    return conn


def _init_runtime_postgres() -> None:
    if not use_runtime_postgres():
        return
    conn = _get_runtime_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(RUNTIME_PG_SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def _get_conn() -> sqlite3.Connection:
    """Get connection with row factory for dict-like rows."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables if they do not exist."""
    _init_access_postgres()
    _init_runtime_postgres()
    conn = _get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                config TEXT,
                leads_count INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'running'
            );

            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                place_id TEXT NOT NULL,
                name TEXT,
                address TEXT,
                latitude REAL,
                longitude REAL,
                raw_place_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (run_id) REFERENCES runs(id),
                UNIQUE(run_id, place_id)
            );

            CREATE TABLE IF NOT EXISTS lead_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL UNIQUE,
                signals_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS context_dimensions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL UNIQUE,
                dimensions_json TEXT NOT NULL,
                reasoning_summary TEXT NOT NULL,
                priority_suggestion TEXT,
                primary_themes_json TEXT,
                outreach_angles_json TEXT,
                overall_confidence REAL,
                reasoning_source TEXT DEFAULT 'deterministic',
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS lead_embeddings (
                lead_id INTEGER NOT NULL UNIQUE,
                embedding_json TEXT NOT NULL,
                text_snapshot TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL UNIQUE,
                agency_type TEXT NOT NULL,
                signals_snapshot TEXT,
                verdict TEXT NOT NULL,
                confidence REAL NOT NULL,
                reasoning TEXT NOT NULL,
                primary_risks TEXT,
                what_would_change TEXT,
                prompt_version TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE INDEX IF NOT EXISTS idx_leads_run ON leads(run_id);
            CREATE INDEX IF NOT EXISTS idx_context_lead ON context_dimensions(lead_id);
            CREATE INDEX IF NOT EXISTS idx_decisions_lead ON decisions(lead_id);

            CREATE TABLE IF NOT EXISTS lead_embeddings_v2 (
                lead_id INTEGER NOT NULL,
                embedding_json TEXT NOT NULL,
                text_snapshot TEXT NOT NULL,
                embedding_version TEXT NOT NULL,
                embedding_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (lead_id, embedding_version, embedding_type),
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS lead_outcomes (
                lead_id INTEGER NOT NULL UNIQUE,
                vertical TEXT,
                agency_type TEXT,
                contacted INTEGER DEFAULT 0,
                proposal_sent INTEGER DEFAULT 0,
                closed INTEGER DEFAULT 0,
                close_value_usd REAL,
                service_sold TEXT,
                notes TEXT,
                status TEXT,
                updated_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );

            CREATE TABLE IF NOT EXISTS lead_intel_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                vertical TEXT,
                primary_constraint TEXT,
                primary_leverage TEXT,
                contact_priority TEXT,
                outreach_angle TEXT,
                confidence REAL,
                risks_json TEXT,
                evidence_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );
            CREATE INDEX IF NOT EXISTS idx_lead_intel_v1_lead ON lead_intel_v1(lead_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS lead_docs_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id INTEGER NOT NULL,
                doc_type TEXT NOT NULL,
                content_text TEXT NOT NULL,
                metadata_json TEXT,
                embedding_version TEXT,
                embedding_type TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (lead_id) REFERENCES leads(id)
            );
            CREATE INDEX IF NOT EXISTS idx_lead_docs_v1_lead ON lead_docs_v1(lead_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_lead_docs_v1_type ON lead_docs_v1(doc_type, created_at DESC);

            CREATE TABLE IF NOT EXISTS workspaces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                plan_tier TEXT NOT NULL DEFAULT 'free',
                seat_limit INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                name TEXT,
                plan_tier TEXT NOT NULL DEFAULT 'free',
                workspace_id INTEGER,
                seat_role TEXT NOT NULL DEFAULT 'owner',
                seat_status TEXT NOT NULL DEFAULT 'active',
                is_guest INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
            );

            CREATE TABLE IF NOT EXISTS workspace_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL DEFAULT 'member',
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(workspace_id, user_id),
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS guest_sessions (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS access_entitlements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                plan_tier TEXT NOT NULL,
                seat_limit INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(scope_type, scope_id)
            );

            CREATE TABLE IF NOT EXISTS usage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                workspace_id INTEGER,
                guest_session_id TEXT,
                feature_key TEXT NOT NULL,
                period_key TEXT NOT NULL,
                event_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (workspace_id) REFERENCES workspaces(id),
                FOREIGN KEY (guest_session_id) REFERENCES guest_sessions(id)
            );

            CREATE TABLE IF NOT EXISTS usage_counters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                period_key TEXT NOT NULL,
                territory_scans_used INTEGER NOT NULL DEFAULT 0,
                diagnostics_used INTEGER NOT NULL DEFAULT 0,
                ask_queries_used INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(subject_type, subject_id, period_key)
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL DEFAULT 1,
                type TEXT NOT NULL DEFAULT 'diagnostic',
                status TEXT NOT NULL DEFAULT 'pending',
                input_json TEXT,
                result_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS diagnostics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL DEFAULT 1,
                job_id TEXT,
                place_id TEXT,
                business_name TEXT NOT NULL,
                city TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT '',
                lead_quality_score REAL,
                lead_quality_class TEXT,
                lead_model_version TEXT,
                lead_feature_version TEXT,
                lead_quality_json TEXT,
                brief_json TEXT,
                response_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            );

            CREATE TABLE IF NOT EXISTS territory_scans (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL DEFAULT 1,
                job_id TEXT,
                scan_type TEXT NOT NULL DEFAULT 'territory',
                city TEXT,
                state TEXT,
                vertical TEXT,
                limit_count INTEGER NOT NULL DEFAULT 20,
                filters_json TEXT,
                list_id INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                summary_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                completed_at TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs(id)
            );

            CREATE TABLE IF NOT EXISTS territory_scan_diagnostics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id TEXT NOT NULL,
                diagnostic_id INTEGER NOT NULL,
                place_id TEXT,
                business_name TEXT,
                city TEXT,
                state TEXT,
                previous_diagnostic_id INTEGER,
                change_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (scan_id) REFERENCES territory_scans(id),
                FOREIGN KEY (diagnostic_id) REFERENCES diagnostics(id)
            );

            CREATE TABLE IF NOT EXISTS territory_prospects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id TEXT NOT NULL,
                user_id INTEGER NOT NULL DEFAULT 1,
                place_id TEXT NOT NULL,
                business_name TEXT NOT NULL,
                city TEXT,
                state TEXT,
                website TEXT,
                rating REAL,
                user_ratings_total INTEGER,
                has_website INTEGER DEFAULT 0,
                ssl INTEGER,
                has_contact_form INTEGER,
                has_phone INTEGER DEFAULT 0,
                has_viewport INTEGER,
                has_schema INTEGER,
                rank_key REAL DEFAULT 0,
                rank INTEGER,
                review_position_summary TEXT,
                lead_quality_score REAL,
                lead_quality_class TEXT,
                lead_quality_reasons_json TEXT,
                lead_model_version TEXT,
                lead_feature_version TEXT,
                lead_data_confidence REAL,
                tier1_snapshot_json TEXT,
                diagnostic_id INTEGER,
                full_brief_ready INTEGER DEFAULT 0,
                ensure_job_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (scan_id) REFERENCES territory_scans(id),
                FOREIGN KEY (diagnostic_id) REFERENCES diagnostics(id),
                UNIQUE(scan_id, place_id)
            );

            CREATE TABLE IF NOT EXISTS territory_tier1_cache (
                place_id TEXT PRIMARY KEY,
                details_json TEXT,
                website_signals_json TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ask_places_cache (
                cache_key TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ask_lightweight_cache (
                place_id TEXT NOT NULL,
                criterion_key TEXT NOT NULL,
                result_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (place_id, criterion_key)
            );

            CREATE TABLE IF NOT EXISTS qa_signal_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                place_id TEXT,
                website TEXT,
                criterion_key TEXT NOT NULL,
                deterministic_match INTEGER NOT NULL DEFAULT 0,
                evidence_json TEXT,
                ai_verdict TEXT,
                ai_confidence TEXT,
                ai_reason TEXT,
                ai_model TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS prospect_lists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL DEFAULT 1,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS prospect_list_members (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                list_id INTEGER NOT NULL,
                diagnostic_id INTEGER NOT NULL,
                place_id TEXT,
                business_name TEXT,
                city TEXT,
                state TEXT,
                added_at TEXT NOT NULL,
                UNIQUE(list_id, place_id),
                FOREIGN KEY (list_id) REFERENCES prospect_lists(id),
                FOREIGN KEY (diagnostic_id) REFERENCES diagnostics(id)
            );

            CREATE TABLE IF NOT EXISTS review_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                place_id TEXT NOT NULL,
                review_count INTEGER NOT NULL,
                rating REAL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_review_snapshots_place ON review_snapshots(place_id, created_at);

            CREATE TABLE IF NOT EXISTS diagnostic_predictions (
                diagnostic_id INTEGER PRIMARY KEY,
                place_id TEXT NOT NULL,
                predictions_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS diagnostic_outcomes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                diagnostic_id INTEGER NOT NULL,
                outcome_type TEXT NOT NULL,
                outcome_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (diagnostic_id) REFERENCES diagnostics(id)
            );

            CREATE TABLE IF NOT EXISTS ml_feature_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                place_id TEXT NOT NULL,
                feature_scope TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                feature_json TEXT NOT NULL,
                feature_hash TEXT NOT NULL,
                data_confidence REAL,
                signal_completeness_ratio REAL,
                created_at TEXT NOT NULL,
                UNIQUE(entity_type, entity_id, feature_version)
            );

            CREATE TABLE IF NOT EXISTS ml_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_type TEXT NOT NULL,
                entity_id INTEGER NOT NULL,
                place_id TEXT NOT NULL,
                model_name TEXT NOT NULL,
                model_version TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                label_version TEXT NOT NULL,
                calibration_version TEXT,
                score REAL NOT NULL,
                score_0_100 REAL NOT NULL,
                predicted_class TEXT NOT NULL,
                prob_bad REAL,
                prob_decent REAL,
                prob_good REAL,
                prob_high_value REAL,
                data_confidence REAL,
                reasons_json TEXT,
                caveats_json TEXT,
                components_json TEXT,
                top_features_json TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(entity_type, entity_id, model_name, model_version)
            );

            CREATE TABLE IF NOT EXISTS ml_dataset_registry (
                dataset_version TEXT PRIMARY KEY,
                task_name TEXT NOT NULL,
                feature_scope TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                label_version TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                source_cutoff_ts TEXT,
                manifest_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ml_training_runs (
                run_id TEXT PRIMARY KEY,
                task_name TEXT NOT NULL,
                model_name TEXT NOT NULL,
                model_version TEXT NOT NULL,
                dataset_version TEXT NOT NULL,
                feature_version TEXT NOT NULL,
                label_version TEXT NOT NULL,
                params_json TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                artifact_path TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS brief_share_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                diagnostic_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL DEFAULT 1,
                token TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                FOREIGN KEY (diagnostic_id) REFERENCES diagnostics(id)
            );
            CREATE INDEX IF NOT EXISTS idx_outcomes_diagnostic ON diagnostic_outcomes(diagnostic_id);
            CREATE INDEX IF NOT EXISTS idx_predictions_place ON diagnostic_predictions(place_id);
            CREATE INDEX IF NOT EXISTS idx_ml_feature_snapshots_entity ON ml_feature_snapshots(entity_type, entity_id);
            CREATE INDEX IF NOT EXISTS idx_ml_feature_snapshots_place ON ml_feature_snapshots(place_id, feature_scope, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_ml_predictions_entity ON ml_predictions(entity_type, entity_id);
            CREATE INDEX IF NOT EXISTS idx_ml_predictions_place ON ml_predictions(place_id, model_name, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_users_workspace ON users(workspace_id);
            CREATE INDEX IF NOT EXISTS idx_guest_sessions_user ON guest_sessions(user_id);
            CREATE INDEX IF NOT EXISTS idx_usage_counters_subject ON usage_counters(subject_type, subject_id, period_key);
            CREATE INDEX IF NOT EXISTS idx_usage_events_user ON usage_events(user_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace ON workspace_members(workspace_id, status);
            CREATE INDEX IF NOT EXISTS idx_brief_share_diag ON brief_share_tokens(diagnostic_id);
            CREATE INDEX IF NOT EXISTS idx_brief_share_token ON brief_share_tokens(token);

            CREATE INDEX IF NOT EXISTS idx_jobs_user ON jobs(user_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
            CREATE INDEX IF NOT EXISTS idx_diagnostics_user ON diagnostics(user_id);
            CREATE INDEX IF NOT EXISTS idx_scans_user ON territory_scans(user_id);
            CREATE INDEX IF NOT EXISTS idx_scans_status ON territory_scans(status);
            CREATE INDEX IF NOT EXISTS idx_scan_diagnostics_scan ON territory_scan_diagnostics(scan_id);
            CREATE INDEX IF NOT EXISTS idx_territory_prospects_scan ON territory_prospects(scan_id);
            CREATE INDEX IF NOT EXISTS idx_territory_prospects_place ON territory_prospects(place_id);
            CREATE INDEX IF NOT EXISTS idx_territory_tier1_cache_updated ON territory_tier1_cache(updated_at);
            CREATE INDEX IF NOT EXISTS idx_ask_places_cache_updated ON ask_places_cache(updated_at);
            CREATE INDEX IF NOT EXISTS idx_ask_lightweight_cache_updated ON ask_lightweight_cache(updated_at);
            CREATE INDEX IF NOT EXISTS idx_qa_signal_checks_created ON qa_signal_checks(created_at);
            CREATE INDEX IF NOT EXISTS idx_qa_signal_checks_source ON qa_signal_checks(source_type, source_id);
            CREATE INDEX IF NOT EXISTS idx_qa_signal_checks_status ON qa_signal_checks(status);
            CREATE INDEX IF NOT EXISTS idx_list_members_list ON prospect_list_members(list_id);
        """)
        conn.commit()
        # Optional columns (migration for existing DBs)
        for sql in [
            "ALTER TABLE runs ADD COLUMN run_stats TEXT",
            "ALTER TABLE context_dimensions ADD COLUMN no_opportunity INTEGER",
            "ALTER TABLE context_dimensions ADD COLUMN no_opportunity_reason TEXT",
            "ALTER TABLE context_dimensions ADD COLUMN priority_derivation TEXT",
            "ALTER TABLE context_dimensions ADD COLUMN validation_warnings TEXT",
            "ALTER TABLE leads ADD COLUMN dentist_profile_v1_json TEXT",
            "ALTER TABLE leads ADD COLUMN llm_reasoning_layer_json TEXT",
            "ALTER TABLE leads ADD COLUMN sales_intervention_intelligence_json TEXT",
            "ALTER TABLE leads ADD COLUMN objective_decision_layer_json TEXT",
            "ALTER TABLE diagnostics ADD COLUMN state TEXT",
            "ALTER TABLE diagnostics ADD COLUMN lead_quality_score REAL",
            "ALTER TABLE diagnostics ADD COLUMN lead_quality_class TEXT",
            "ALTER TABLE diagnostics ADD COLUMN lead_model_version TEXT",
            "ALTER TABLE diagnostics ADD COLUMN lead_feature_version TEXT",
            "ALTER TABLE diagnostics ADD COLUMN lead_quality_json TEXT",
            "ALTER TABLE territory_prospects ADD COLUMN lead_quality_score REAL",
            "ALTER TABLE territory_prospects ADD COLUMN lead_quality_class TEXT",
            "ALTER TABLE territory_prospects ADD COLUMN lead_quality_reasons_json TEXT",
            "ALTER TABLE territory_prospects ADD COLUMN lead_model_version TEXT",
            "ALTER TABLE territory_prospects ADD COLUMN lead_feature_version TEXT",
            "ALTER TABLE territory_prospects ADD COLUMN lead_data_confidence REAL",
            # lead_outcomes migration (event-style fields for hybrid RAG)
            "ALTER TABLE lead_outcomes ADD COLUMN id INTEGER",
            "ALTER TABLE lead_outcomes ADD COLUMN list_id INTEGER",
            "ALTER TABLE lead_outcomes ADD COLUMN scan_id TEXT",
            "ALTER TABLE lead_outcomes ADD COLUMN outcome_status TEXT",
            "ALTER TABLE lead_outcomes ADD COLUMN outcome_note TEXT",
            "ALTER TABLE lead_outcomes ADD COLUMN timestamp TEXT",
            "ALTER TABLE users ADD COLUMN plan_tier TEXT NOT NULL DEFAULT 'free'",
            "ALTER TABLE users ADD COLUMN workspace_id INTEGER",
            "ALTER TABLE users ADD COLUMN seat_role TEXT NOT NULL DEFAULT 'owner'",
            "ALTER TABLE users ADD COLUMN seat_status TEXT NOT NULL DEFAULT 'active'",
            "ALTER TABLE users ADD COLUMN is_guest INTEGER NOT NULL DEFAULT 0",
        ]:
            try:
                conn.execute(sql)
                conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists
    finally:
        conn.close()


def create_run(config: Optional[Dict] = None) -> str:
    """Create a new run; return run_id (UUID)."""
    init_db()
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    config_json = json.dumps(config or {}) if config else None
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO runs (id, created_at, config, status) VALUES (%s, %s, %s::jsonb, %s)",
                    (run_id, now, config_json, "running"),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO runs (id, created_at, config, status) VALUES (?, ?, ?, ?)",
                (run_id, now, config_json, "running")
            )
            conn.commit()
        finally:
            conn.close()
    logger.info("Created run %s", run_id[:8])
    return run_id


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def current_usage_period_key() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}-{now.month:02d}"


def feature_limit_map(plan_tier: str) -> Dict[str, Optional[int]]:
    tier = str(plan_tier or "guest").strip().lower()
    if tier == "guest":
        return {
            "territory_scan": 2,
            "diagnostic": 1,
            "ask": 0,
        }
    if tier == "free":
        return {
            "territory_scan": 5,
            "diagnostic": 5,
            "ask": 3,
        }
    return {
        "territory_scan": None,
        "diagnostic": None,
        "ask": None,
    }


def _ensure_entitlement(scope_type: str, scope_id: str, plan_tier: str, seat_limit: Optional[int] = None) -> None:
    now = utc_now_iso()
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO access_entitlements (scope_type, scope_id, plan_tier, seat_limit, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT(scope_type, scope_id) DO UPDATE SET
                         plan_tier = EXCLUDED.plan_tier,
                         seat_limit = COALESCE(EXCLUDED.seat_limit, access_entitlements.seat_limit),
                         updated_at = EXCLUDED.updated_at""",
                    (scope_type, str(scope_id), plan_tier, seat_limit, now, now),
                )
            conn.commit()
            return
        finally:
            conn.close()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO access_entitlements (scope_type, scope_id, plan_tier, seat_limit, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(scope_type, scope_id) DO UPDATE SET
                 plan_tier = excluded.plan_tier,
                 seat_limit = COALESCE(excluded.seat_limit, access_entitlements.seat_limit),
                 updated_at = excluded.updated_at""",
            (scope_type, str(scope_id), plan_tier, seat_limit, now, now),
        )
        conn.commit()
    finally:
        conn.close()


def create_workspace(name: str, plan_tier: str = "free", seat_limit: int = 1) -> int:
    init_db()
    now = utc_now_iso()
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO workspaces (name, plan_tier, seat_limit, created_at, updated_at) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (name, plan_tier, seat_limit, now, now),
                )
                row = cur.fetchone()
            conn.commit()
            workspace_id = int(row["id"])
        finally:
            conn.close()
        _ensure_entitlement("workspace", str(workspace_id), plan_tier, seat_limit)
        return workspace_id
    conn = _get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO workspaces (name, plan_tier, seat_limit, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (name, plan_tier, seat_limit, now, now),
        )
        workspace_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()
    _ensure_entitlement("workspace", str(workspace_id), plan_tier, seat_limit)
    return workspace_id


def get_workspace(workspace_id: int) -> Optional[Dict[str, Any]]:
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT w.*,
                              (SELECT COUNT(*) FROM workspace_members wm WHERE wm.workspace_id = w.id AND wm.status = 'active') AS active_members
                       FROM workspaces w
                       WHERE w.id = %s""",
                    (int(workspace_id),),
                )
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        finally:
            conn.close()
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT w.*,
                      (SELECT COUNT(*) FROM workspace_members wm WHERE wm.workspace_id = w.id AND wm.status = 'active') AS active_members
               FROM workspaces w
               WHERE w.id = ?""",
            (int(workspace_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _ensure_workspace_member(workspace_id: int, user_id: int, role: str = "owner", status: str = "active") -> None:
    now = utc_now_iso()
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO workspace_members (workspace_id, user_id, role, status, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT(workspace_id, user_id) DO UPDATE SET
                         role = EXCLUDED.role,
                         status = EXCLUDED.status,
                         updated_at = EXCLUDED.updated_at""",
                    (workspace_id, user_id, role, status, now, now),
                )
            conn.commit()
            return
        finally:
            conn.close()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO workspace_members (workspace_id, user_id, role, status, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(workspace_id, user_id) DO UPDATE SET
                 role = excluded.role,
                 status = excluded.status,
                 updated_at = excluded.updated_at""",
            (workspace_id, user_id, role, status, now, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE lower(email) = lower(%s)", (email.strip(),))
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        finally:
            conn.close()
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE lower(email) = lower(?)", (email.strip(),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM users WHERE id = %s", (int(user_id),))
                row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None
        finally:
            conn.close()
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (int(user_id),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def create_user(
    *,
    email: Optional[str],
    name: Optional[str],
    plan_tier: str = "free",
    is_guest: bool = False,
    workspace_name: Optional[str] = None,
    seat_role: str = "owner",
    seat_status: str = "active",
    create_workspace_record: bool = True,
) -> int:
    init_db()
    now = utc_now_iso()
    workspace_id: Optional[int] = None
    if not is_guest and create_workspace_record:
        workspace_id = create_workspace(workspace_name or (name or email or "Neyma Workspace"), plan_tier=plan_tier, seat_limit=1 if plan_tier != "team" else 5)
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO users
                       (email, name, plan_tier, workspace_id, seat_role, seat_status, is_guest, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (email, name, plan_tier, workspace_id, seat_role, seat_status, 1 if is_guest else 0, now, now),
                )
                row = cur.fetchone()
            conn.commit()
            user_id = int(row["id"])
        finally:
            conn.close()
        _ensure_entitlement("user", str(user_id), plan_tier, 1 if plan_tier != "team" else None)
        if workspace_id:
            _ensure_workspace_member(workspace_id, user_id, role=seat_role, status=seat_status)
        return user_id
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO users
               (email, name, plan_tier, workspace_id, seat_role, seat_status, is_guest, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (email, name, plan_tier, workspace_id, seat_role, seat_status, 1 if is_guest else 0, now, now),
        )
        user_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()
    _ensure_entitlement("user", str(user_id), plan_tier, 1 if plan_tier != "team" else None)
    if workspace_id:
        _ensure_workspace_member(workspace_id, user_id, role=seat_role, status=seat_status)
    return user_id


def get_or_create_user(email: str, name: Optional[str] = None) -> Dict[str, Any]:
    existing = get_user_by_email(email)
    if existing:
        now = utc_now_iso()
        if use_access_postgres():
            conn = _get_access_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET name = COALESCE(%s, name), updated_at = %s WHERE id = %s",
                        (name, now, int(existing["id"])),
                    )
                conn.commit()
            finally:
                conn.close()
            updated = get_user(int(existing["id"]))
            return updated or existing
        conn = _get_conn()
        try:
            conn.execute(
                "UPDATE users SET name = COALESCE(?, name), updated_at = ? WHERE id = ?",
                (name, now, int(existing["id"])),
            )
            conn.commit()
        finally:
            conn.close()
        updated = get_user(int(existing["id"]))
        return updated or existing
    user_id = create_user(email=email, name=name or email.split("@")[0], plan_tier="free", is_guest=False)
    created = get_user(user_id)
    if not created:
        raise RuntimeError("Failed to create user")
    return created


def get_or_create_guest_user(session_id: str) -> Dict[str, Any]:
    init_db()
    now = utc_now_iso()
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM guest_sessions WHERE id = %s", (session_id,))
                row = cur.fetchone()
                if row:
                    cur.execute("UPDATE guest_sessions SET updated_at = %s WHERE id = %s", (now, session_id))
                    conn.commit()
                    user = get_user(int(row["user_id"]))
                    if user:
                        return user
            user_id = create_user(
                email=None,
                name="Guest",
                plan_tier="guest",
                is_guest=True,
                workspace_name=None,
                seat_role="owner",
                seat_status="active",
            )
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO guest_sessions (id, user_id, created_at, updated_at)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT(id) DO UPDATE SET user_id = EXCLUDED.user_id, updated_at = EXCLUDED.updated_at""",
                    (session_id, user_id, now, now),
                )
            conn.commit()
            user = get_user(user_id)
            if not user:
                raise RuntimeError("Failed to create guest user")
            return user
        finally:
            conn.close()
    conn = _get_conn()
    try:
        row = conn.execute("SELECT user_id FROM guest_sessions WHERE id = ?", (session_id,)).fetchone()
        if row:
            conn.execute("UPDATE guest_sessions SET updated_at = ? WHERE id = ?", (now, session_id))
            conn.commit()
            user = get_user(int(row["user_id"]))
            if user:
                return user
        user_id = create_user(
            email=None,
            name="Guest",
            plan_tier="guest",
            is_guest=True,
            workspace_name=None,
            seat_role="owner",
            seat_status="active",
        )
        conn.execute(
            "INSERT OR REPLACE INTO guest_sessions (id, user_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (session_id, user_id, now, now),
        )
        conn.commit()
        user = get_user(user_id)
        if not user:
            raise RuntimeError("Failed to create guest user")
        return user
    finally:
        conn.close()


def list_workspace_members(workspace_id: int) -> List[Dict[str, Any]]:
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT wm.workspace_id, wm.role, wm.status, wm.created_at, u.id AS user_id, u.email, u.name, u.plan_tier
                       FROM workspace_members wm
                       JOIN users u ON u.id = wm.user_id
                       WHERE wm.workspace_id = %s
                       ORDER BY wm.created_at ASC""",
                    (workspace_id,),
                )
                rows = cur.fetchall()
            conn.commit()
            return [dict(row) for row in rows]
        finally:
            conn.close()
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT wm.workspace_id, wm.role, wm.status, wm.created_at, u.id AS user_id, u.email, u.name, u.plan_tier
               FROM workspace_members wm
               JOIN users u ON u.id = wm.user_id
               WHERE wm.workspace_id = ?
               ORDER BY wm.created_at ASC""",
            (workspace_id,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def invite_workspace_member(workspace_id: int, email: str, name: Optional[str] = None, role: str = "member") -> Dict[str, Any]:
    existing = get_user_by_email(email)
    if existing:
        user_id = int(existing["id"])
        if use_access_postgres():
            conn = _get_access_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE users SET workspace_id = %s, plan_tier = 'team', seat_role = %s, seat_status = 'invited', updated_at = %s WHERE id = %s",
                        (workspace_id, role, utc_now_iso(), user_id),
                    )
                conn.commit()
            finally:
                conn.close()
        else:
            conn = _get_conn()
            try:
                conn.execute(
                    "UPDATE users SET workspace_id = ?, plan_tier = 'team', seat_role = ?, seat_status = 'invited', updated_at = ? WHERE id = ?",
                    (workspace_id, role, utc_now_iso(), user_id),
                )
                conn.commit()
            finally:
                conn.close()
    else:
        user_id = create_user(
            email=email,
            name=name or email.split("@")[0],
            plan_tier="team",
            is_guest=False,
            workspace_name=None,
            seat_role=role,
            seat_status="invited",
            create_workspace_record=False,
        )
        if use_access_postgres():
            conn = _get_access_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE users SET workspace_id = %s WHERE id = %s", (workspace_id, user_id))
                conn.commit()
            finally:
                conn.close()
        else:
            conn = _get_conn()
            try:
                conn.execute("UPDATE users SET workspace_id = ? WHERE id = ?", (workspace_id, user_id))
                conn.commit()
            finally:
                conn.close()
    _ensure_workspace_member(workspace_id, user_id, role=role, status="invited")
    invited = get_user(user_id)
    if not invited:
        raise RuntimeError("Failed to invite workspace member")
    return invited


def remove_workspace_member(workspace_id: int, user_id: int) -> bool:
    now = utc_now_iso()
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE workspace_members SET status = 'removed', updated_at = %s WHERE workspace_id = %s AND user_id = %s",
                    (now, workspace_id, user_id),
                )
                rowcount = cur.rowcount
                cur.execute(
                    "UPDATE users SET workspace_id = NULL, plan_tier = 'free', seat_role = 'owner', seat_status = 'removed', updated_at = %s WHERE id = %s",
                    (now, user_id),
                )
            conn.commit()
            return rowcount > 0
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "UPDATE workspace_members SET status = 'removed', updated_at = ? WHERE workspace_id = ? AND user_id = ?",
            (now, workspace_id, user_id),
        )
        conn.execute(
            "UPDATE users SET workspace_id = NULL, plan_tier = 'free', seat_role = 'owner', seat_status = 'removed', updated_at = ? WHERE id = ?",
            (now, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_usage_counter(subject_type: str, subject_id: str, period_key: str) -> Dict[str, int]:
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT territory_scans_used, diagnostics_used, ask_queries_used
                       FROM usage_counters
                       WHERE subject_type = %s AND subject_id = %s AND period_key = %s""",
                    (subject_type, str(subject_id), period_key),
                )
                row = cur.fetchone()
            conn.commit()
            if not row:
                return {"territory_scan": 0, "diagnostic": 0, "ask": 0}
            return {
                "territory_scan": int(row["territory_scans_used"] or 0),
                "diagnostic": int(row["diagnostics_used"] or 0),
                "ask": int(row["ask_queries_used"] or 0),
            }
        finally:
            conn.close()
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT territory_scans_used, diagnostics_used, ask_queries_used
               FROM usage_counters
               WHERE subject_type = ? AND subject_id = ? AND period_key = ?""",
            (subject_type, str(subject_id), period_key),
        ).fetchone()
        if not row:
            return {
                "territory_scan": 0,
                "diagnostic": 0,
                "ask": 0,
            }
        return {
            "territory_scan": int(row["territory_scans_used"] or 0),
            "diagnostic": int(row["diagnostics_used"] or 0),
            "ask": int(row["ask_queries_used"] or 0),
        }
    finally:
        conn.close()


def _usage_subject(user: Dict[str, Any]) -> tuple[str, str, str]:
    plan_tier = str(user.get("plan_tier") or "guest").strip().lower()
    if plan_tier == "guest":
        if use_access_postgres():
            conn = _get_access_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM guest_sessions WHERE user_id = %s", (int(user["id"]),))
                    row = cur.fetchone()
                conn.commit()
                session_id = str(row["id"]) if row else str(user["id"])
            finally:
                conn.close()
        else:
            conn = _get_conn()
            try:
                row = conn.execute("SELECT id FROM guest_sessions WHERE user_id = ?", (int(user["id"]),)).fetchone()
                session_id = str(row["id"]) if row else str(user["id"])
            finally:
                conn.close()
        return ("guest", session_id, "lifetime")
    return ("user", str(user["id"]), current_usage_period_key())


def get_access_state(user_id: int) -> Dict[str, Any]:
    user = get_user(int(user_id))
    if not user:
        raise ValueError("User not found")
    plan_tier = str(user.get("plan_tier") or "guest").strip().lower()
    email = str(user.get("email") or "").strip().lower()
    is_dev_test_user = email == "test@neyma.local"
    workspace = get_workspace(int(user["workspace_id"])) if user.get("workspace_id") else None
    limits = (
        {
            "territory_scan": None,
            "diagnostic": None,
            "ask": None,
        }
        if is_dev_test_user
        else feature_limit_map(plan_tier)
    )
    subject_type, subject_id, period_key = _usage_subject(user)
    usage = get_usage_counter(subject_type, subject_id, period_key)
    remaining = {
        key: None if limits[key] is None else max(0, int(limits[key]) - int(usage.get(key, 0)))
        for key in limits
    }
    is_guest = bool(int(user.get("is_guest") or 0))
    can_use = {
        "territory_scan": remaining["territory_scan"] is None or remaining["territory_scan"] > 0,
        "diagnostic": remaining["diagnostic"] is None or remaining["diagnostic"] > 0,
        "ask": (not is_guest) and (remaining["ask"] is None or remaining["ask"] > 0),
        "workspace": not is_guest,
        "save": not is_guest,
        "share": not is_guest,
        "export": not is_guest,
    }
    recommended_cta = None if is_dev_test_user else ("Upgrade to Pro" if not is_guest and plan_tier == "free" else "Sign up")
    return {
        "viewer": {
            "user_id": int(user["id"]),
            "email": user.get("email"),
            "name": user.get("name"),
            "is_guest": is_guest,
        },
        "plan_tier": "pro" if is_dev_test_user else plan_tier,
        "workspace": (
            {
                "id": int(workspace["id"]),
                "name": workspace.get("name"),
                "plan_tier": workspace.get("plan_tier"),
                "seat_limit": int(workspace.get("seat_limit") or 0),
                "seat_count": int(workspace.get("active_members") or 0),
                "role": user.get("seat_role"),
                "status": user.get("seat_status"),
            }
            if workspace
            else None
        ),
        "usage": usage,
        "limits": limits,
        "remaining": remaining,
        "period_key": period_key,
        "can_use": can_use,
        "recommended_cta": recommended_cta,
    }


def consume_usage(user_id: int, feature_key: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    user = get_user(int(user_id))
    if not user:
        raise ValueError("User not found")
    feature = str(feature_key).strip().lower()
    if feature not in {"territory_scan", "diagnostic", "ask"}:
        raise ValueError("Unsupported feature")
    state = get_access_state(int(user_id))
    plan_tier = str(state["plan_tier"])
    remaining = state["remaining"].get(feature)
    if feature == "ask" and bool(state["viewer"].get("is_guest")):
        raise PermissionError(json.dumps({
            "code": "AUTH_REQUIRED",
            "message": "Ask Neyma requires a free account.",
            "recommended_cta": "Sign up",
            "access": state,
        }))
    if remaining is not None and remaining <= 0:
        raise PermissionError(json.dumps({
            "code": "FREE_LIMIT_REACHED" if plan_tier == "free" else "GUEST_LIMIT_REACHED",
            "message": "Usage limit reached for this feature.",
            "recommended_cta": state.get("recommended_cta"),
            "access": state,
        }))

    subject_type, subject_id, period_key = _usage_subject(user)
    now = utc_now_iso()
    workspace_id = int(user["workspace_id"]) if user.get("workspace_id") else None
    event_json = json.dumps(metadata or {}, default=str)
    column = {
        "territory_scan": "territory_scans_used",
        "diagnostic": "diagnostics_used",
        "ask": "ask_queries_used",
    }[feature]
    if use_access_postgres():
        conn = _get_access_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO usage_counters
                       (subject_type, subject_id, period_key, territory_scans_used, diagnostics_used, ask_queries_used, updated_at)
                       VALUES (%s, %s, %s, 0, 0, 0, %s)
                       ON CONFLICT(subject_type, subject_id, period_key) DO NOTHING""",
                    (subject_type, subject_id, period_key, now),
                )
                cur.execute(
                    f"UPDATE usage_counters SET {column} = {column} + 1, updated_at = %s WHERE subject_type = %s AND subject_id = %s AND period_key = %s",
                    (now, subject_type, subject_id, period_key),
                )
                cur.execute("SELECT id FROM guest_sessions WHERE user_id = %s", (int(user["id"]),))
                guest_row = cur.fetchone()
                cur.execute(
                    """INSERT INTO usage_events (user_id, workspace_id, guest_session_id, feature_key, period_key, event_json, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)""",
                    (
                        int(user["id"]),
                        workspace_id,
                        str(guest_row["id"]) if guest_row else None,
                        feature,
                        period_key,
                        event_json,
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
                """INSERT INTO usage_counters
                   (subject_type, subject_id, period_key, territory_scans_used, diagnostics_used, ask_queries_used, updated_at)
                   VALUES (?, ?, ?, 0, 0, 0, ?)
                   ON CONFLICT(subject_type, subject_id, period_key) DO NOTHING""",
                (subject_type, subject_id, period_key, now),
            )
            conn.execute(
                f"UPDATE usage_counters SET {column} = {column} + 1, updated_at = ? WHERE subject_type = ? AND subject_id = ? AND period_key = ?",
                (now, subject_type, subject_id, period_key),
            )
            conn.execute(
                """INSERT INTO usage_events (user_id, workspace_id, guest_session_id, feature_key, period_key, event_json, created_at)
                   VALUES (?, ?, (SELECT id FROM guest_sessions WHERE user_id = ?), ?, ?, ?, ?)""",
                (int(user["id"]), workspace_id, int(user["id"]), feature, period_key, event_json, now),
            )
            conn.commit()
        finally:
            conn.close()
    return get_access_state(int(user_id))


def insert_lead(run_id: str, lead: Dict) -> int:
    """Insert a lead; return lead_id."""
    now = datetime.now(timezone.utc).isoformat()
    raw_json = json.dumps(lead, default=str) if lead.get("_place_details") else None
    params = (
        run_id,
        lead.get("place_id", ""),
        lead.get("name"),
        lead.get("address"),
        lead.get("latitude"),
        lead.get("longitude"),
        raw_json,
        now,
    )
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO leads (run_id, place_id, name, address, latitude, longitude, raw_place_json, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                       RETURNING id""",
                    params,
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"])
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO leads (run_id, place_id, name, address, latitude, longitude, raw_place_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            params
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insert_lead_signals(lead_id: int, signals: Dict) -> None:
    """Store signals JSON for a lead."""
    now = datetime.now(timezone.utc).isoformat()
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO lead_signals (lead_id, signals_json, created_at) VALUES (%s, %s::jsonb, %s)",
                    (lead_id, json.dumps(signals, default=str), now),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO lead_signals (lead_id, signals_json, created_at) VALUES (?, ?, ?)",
                (lead_id, json.dumps(signals, default=str), now)
            )
            conn.commit()
        finally:
            conn.close()


def insert_decision(
    lead_id: int,
    agency_type: str,
    signals_snapshot: Optional[Dict],
    verdict: str,
    confidence: float,
    reasoning: str,
    primary_risks: List[str],
    what_would_change: List[str],
    prompt_version: str,
) -> None:
    """Store one decision (Decision Agent output) for a lead. Verbatim for future learning."""
    now = datetime.now(timezone.utc).isoformat()
    signals_json = json.dumps(signals_snapshot, default=str) if signals_snapshot else None
    risks_json = json.dumps(primary_risks) if primary_risks else None
    change_json = json.dumps(what_would_change) if what_would_change else None
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO decisions (lead_id, agency_type, signals_snapshot, verdict, confidence,
                       reasoning, primary_risks, what_would_change, prompt_version, created_at)
                       VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)""",
                    (
                        lead_id,
                        agency_type,
                        signals_json,
                        verdict,
                        confidence,
                        reasoning,
                        risks_json,
                        change_json,
                        prompt_version,
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
            """INSERT INTO decisions (lead_id, agency_type, signals_snapshot, verdict, confidence,
               reasoning, primary_risks, what_would_change, prompt_version, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                lead_id,
                agency_type,
                signals_json,
                verdict,
                confidence,
                reasoning,
                risks_json,
                change_json,
                prompt_version,
                now,
            ),
            )
            conn.commit()
        finally:
            conn.close()


def insert_context_dimensions(
    lead_id: int,
    dimensions: List[Dict],
    reasoning_summary: str,
    overall_confidence: float,
    priority_suggestion: Optional[str] = None,
    primary_themes: Optional[List[str]] = None,
    outreach_angles: Optional[List[str]] = None,
    reasoning_source: str = "deterministic",
    no_opportunity: bool = False,
    no_opportunity_reason: Optional[str] = None,
    priority_derivation: Optional[str] = None,
    validation_warnings: Optional[List[str]] = None,
) -> None:
    """Store context dimensions and reasoning for a lead."""
    now = datetime.now(timezone.utc).isoformat()
    dimensions_json = json.dumps(dimensions, default=str)
    themes_json = json.dumps(primary_themes) if primary_themes is not None else None
    angles_json = json.dumps(outreach_angles) if outreach_angles is not None else None
    warnings_json = json.dumps(validation_warnings) if validation_warnings else None
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO context_dimensions
                       (lead_id, dimensions_json, reasoning_summary, priority_suggestion,
                        primary_themes_json, outreach_angles_json, overall_confidence, reasoning_source,
                        no_opportunity, no_opportunity_reason, priority_derivation, validation_warnings, created_at)
                       VALUES (%s, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s::jsonb, %s)
                       ON CONFLICT (lead_id) DO UPDATE SET
                         dimensions_json = EXCLUDED.dimensions_json,
                         reasoning_summary = EXCLUDED.reasoning_summary,
                         priority_suggestion = EXCLUDED.priority_suggestion,
                         primary_themes_json = EXCLUDED.primary_themes_json,
                         outreach_angles_json = EXCLUDED.outreach_angles_json,
                         overall_confidence = EXCLUDED.overall_confidence,
                         reasoning_source = EXCLUDED.reasoning_source,
                         no_opportunity = EXCLUDED.no_opportunity,
                         no_opportunity_reason = EXCLUDED.no_opportunity_reason,
                         priority_derivation = EXCLUDED.priority_derivation,
                         validation_warnings = EXCLUDED.validation_warnings,
                         created_at = EXCLUDED.created_at""",
                    (
                        lead_id,
                        dimensions_json,
                        reasoning_summary,
                        priority_suggestion,
                        themes_json,
                        angles_json,
                        overall_confidence,
                        reasoning_source,
                        1 if no_opportunity else 0,
                        no_opportunity_reason,
                        priority_derivation,
                        warnings_json,
                        now,
                    ),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
                """INSERT INTO context_dimensions
                   (lead_id, dimensions_json, reasoning_summary, priority_suggestion,
                    primary_themes_json, outreach_angles_json, overall_confidence, reasoning_source,
                    no_opportunity, no_opportunity_reason, priority_derivation, validation_warnings, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    lead_id,
                    dimensions_json,
                    reasoning_summary,
                    priority_suggestion,
                    themes_json,
                    angles_json,
                    overall_confidence,
                    reasoning_source,
                    1 if no_opportunity else 0,
                    no_opportunity_reason,
                    priority_derivation,
                    warnings_json,
                    now,
                )
            )
            conn.commit()
        except sqlite3.OperationalError:
            conn.rollback()
            conn.execute(
                """INSERT INTO context_dimensions
                   (lead_id, dimensions_json, reasoning_summary, priority_suggestion,
                    primary_themes_json, outreach_angles_json, overall_confidence, reasoning_source, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    lead_id,
                    dimensions_json,
                    reasoning_summary,
                    priority_suggestion,
                    themes_json,
                    angles_json,
                    overall_confidence,
                    reasoning_source,
                    now,
                )
            )
            conn.commit()
        finally:
            conn.close()


def update_lead_dentist_data(
    lead_id: int,
    dentist_profile_v1: Optional[Dict] = None,
    llm_reasoning_layer: Optional[Dict] = None,
    sales_intervention_intelligence: Optional[Dict] = None,
    objective_decision_layer: Optional[Dict] = None,
) -> None:
    """Store dentist vertical profile, LLM reasoning layer, sales intervention intelligence, and/or objective decision layer for a lead."""
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                if dentist_profile_v1 is not None:
                    cur.execute(
                        "UPDATE leads SET dentist_profile_v1_json = %s::jsonb WHERE id = %s",
                        (json.dumps(dentist_profile_v1, default=str), lead_id),
                    )
                if llm_reasoning_layer is not None:
                    cur.execute(
                        "UPDATE leads SET llm_reasoning_layer_json = %s::jsonb WHERE id = %s",
                        (json.dumps(llm_reasoning_layer, default=str), lead_id),
                    )
                if sales_intervention_intelligence is not None:
                    cur.execute(
                        "UPDATE leads SET sales_intervention_intelligence_json = %s::jsonb WHERE id = %s",
                        (json.dumps(sales_intervention_intelligence, default=str), lead_id),
                    )
                if objective_decision_layer is not None:
                    cur.execute(
                        "UPDATE leads SET objective_decision_layer_json = %s::jsonb WHERE id = %s",
                        (json.dumps(objective_decision_layer, default=str), lead_id),
                    )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            if dentist_profile_v1 is not None:
                conn.execute(
                    "UPDATE leads SET dentist_profile_v1_json = ? WHERE id = ?",
                    (json.dumps(dentist_profile_v1, default=str), lead_id),
                )
            if llm_reasoning_layer is not None:
                conn.execute(
                    "UPDATE leads SET llm_reasoning_layer_json = ? WHERE id = ?",
                    (json.dumps(llm_reasoning_layer, default=str), lead_id),
                )
            if sales_intervention_intelligence is not None:
                conn.execute(
                    "UPDATE leads SET sales_intervention_intelligence_json = ? WHERE id = ?",
                    (json.dumps(sales_intervention_intelligence, default=str), lead_id),
                )
            if objective_decision_layer is not None:
                conn.execute(
                    "UPDATE leads SET objective_decision_layer_json = ? WHERE id = ?",
                    (json.dumps(objective_decision_layer, default=str), lead_id),
                )
            conn.commit()
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()


def insert_lead_embedding(lead_id: int, embedding: List[float], text_snapshot: str) -> None:
    """Store embedding vector and text snapshot for RAG (Phase 2)."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO lead_embeddings (lead_id, embedding_json, text_snapshot, created_at)
               VALUES (?, ?, ?, ?)""",
            (lead_id, json.dumps(embedding), text_snapshot[:5000], now)
        )
        conn.commit()
    finally:
        conn.close()


def insert_lead_embedding_v2(
    lead_id: int,
    embedding: List[float],
    text: str,
    embedding_version: str,
    embedding_type: str,
) -> None:
    """Store embedding in lead_embeddings_v2 (versioned, typed)."""
    now = datetime.now(timezone.utc).isoformat()
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO lead_embeddings_v2
                       (lead_id, embedding_json, text_snapshot, embedding_version, embedding_type, created_at)
                       VALUES (%s, %s::jsonb, %s, %s, %s, %s)
                       ON CONFLICT (lead_id, embedding_version, embedding_type) DO UPDATE SET
                         embedding_json = EXCLUDED.embedding_json,
                         text_snapshot = EXCLUDED.text_snapshot,
                         created_at = EXCLUDED.created_at""",
                    (lead_id, json.dumps(embedding), text[:5000], embedding_version, embedding_type, now),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO lead_embeddings_v2
                   (lead_id, embedding_json, text_snapshot, embedding_version, embedding_type, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (lead_id, json.dumps(embedding), text[:5000], embedding_version, embedding_type, now)
            )
            conn.commit()
        finally:
            conn.close()


def get_lead_embedding_v2(
    lead_id: int,
    embedding_version: str,
    embedding_type: str,
) -> Optional[Dict]:
    """Return stored embedding row or None."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                """SELECT lead_id, embedding_json, text_snapshot, embedding_version, embedding_type, created_at
                   FROM lead_embeddings_v2
                   WHERE lead_id = %s AND embedding_version = %s AND embedding_type = %s""",
                (lead_id, embedding_version, embedding_type),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                """SELECT lead_id, embedding_json, text_snapshot, embedding_version, embedding_type, created_at
                   FROM lead_embeddings_v2
                   WHERE lead_id = ? AND embedding_version = ? AND embedding_type = ?""",
                (lead_id, embedding_version, embedding_type),
            ).fetchone()
        )
        if not row:
            return None
        return {
            "lead_id": row["lead_id"],
            "embedding_json": row["embedding_json"],
            "embedding": row["embedding_json"] if isinstance(row["embedding_json"], list) else (json.loads(row["embedding_json"]) if row["embedding_json"] else []),
            "text_snapshot": row["text_snapshot"],
            "embedding_version": row["embedding_version"],
            "embedding_type": row["embedding_type"],
            "created_at": row["created_at"],
        }
    finally:
        conn.close()


def insert_lead_doc_v1(
    lead_id: int,
    doc_type: str,
    content_text: str,
    metadata: Optional[Dict[str, Any]] = None,
    embedding: Optional[List[float]] = None,
    embedding_version: str = "v1_doc",
) -> Optional[int]:
    """
    Store a typed retrieval document for a lead.

    Embeddings are persisted in lead_embeddings_v2 (no duplicate blob storage).
    Returns doc id on success, None on failure.
    """
    if not lead_id or not doc_type or not (content_text or "").strip():
        return None
    now = datetime.now(timezone.utc).isoformat()
    embedding_type = None
    if embedding:
        embedding_type = f"doc_{doc_type.strip().lower()}"
        try:
            insert_lead_embedding_v2(
                lead_id=lead_id,
                embedding=embedding,
                text=content_text,
                embedding_version=embedding_version,
                embedding_type=embedding_type,
            )
        except Exception:
            # Safe-fail; keep doc row even if embedding insert fails.
            embedding_type = None

    params = (
        lead_id,
        doc_type,
        content_text[:8000],
        json.dumps(metadata or {}, default=str),
        embedding_version if embedding_type else None,
        embedding_type,
        now,
    )
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO lead_docs_v1
                       (lead_id, doc_type, content_text, metadata_json, embedding_version, embedding_type, created_at)
                       VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s)
                       RETURNING id""",
                    params,
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"])
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO lead_docs_v1
               (lead_id, doc_type, content_text, metadata_json, embedding_version, embedding_type, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            params,
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_lead_docs_v1(
    lead_id: int,
    doc_types: Optional[List[str]] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """List typed docs for a lead, newest first."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            params: List[Any] = [lead_id]
            where = "WHERE lead_id = %s"
            if doc_types:
                placeholders = ",".join(["%s"] * len(doc_types))
                where += f" AND doc_type IN ({placeholders})"
                params.extend(doc_types)
            params.append(max(1, int(limit)))
            rows = conn.cursor().execute(
                f"""SELECT id, lead_id, doc_type, content_text, metadata_json, embedding_version, embedding_type, created_at
                    FROM lead_docs_v1
                    {where}
                    ORDER BY created_at DESC
                    LIMIT %s""",
                params,
            ).fetchall()
        else:
            params = [lead_id]
            where = "WHERE lead_id = ?"
            if doc_types:
                placeholders = ",".join("?" for _ in doc_types)
                where += f" AND doc_type IN ({placeholders})"
                params.extend(doc_types)
            params.append(max(1, int(limit)))
            rows = conn.execute(
                f"""SELECT id, lead_id, doc_type, content_text, metadata_json, embedding_version, embedding_type, created_at
                    FROM lead_docs_v1
                    {where}
                    ORDER BY created_at DESC
                    LIMIT ?""",
                params,
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                metadata = row["metadata_json"] if isinstance(row["metadata_json"], dict) else (json.loads(row["metadata_json"]) if row["metadata_json"] else {})
            except (TypeError, json.JSONDecodeError):
                metadata = {}
            out.append(
                {
                    "id": row["id"],
                    "lead_id": row["lead_id"],
                    "doc_type": row["doc_type"],
                    "content_text": row["content_text"] or "",
                    "metadata_json": metadata,
                    "embedding_version": row["embedding_version"],
                    "embedding_type": row["embedding_type"],
                    "created_at": row["created_at"],
                }
            )
        return out
    finally:
        conn.close()


def list_docs_with_embeddings_v1(
    doc_types: Optional[List[str]] = None,
    exclude_lead_id: Optional[int] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """List retrieval docs with optional embedding payload from lead_embeddings_v2."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        params: List[Any] = []
        where_clauses: List[str] = []
        placeholder = "%s" if use_runtime_postgres() else "?"
        if doc_types:
            placeholders = ",".join([placeholder] * len(doc_types))
            where_clauses.append(f"d.doc_type IN ({placeholders})")
            params.extend(doc_types)
        if exclude_lead_id is not None:
            where_clauses.append(f"d.lead_id != {placeholder}")
            params.append(exclude_lead_id)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        params.append(max(1, int(limit)))
        query = (
            f"""SELECT d.id, d.lead_id, d.doc_type, d.content_text, d.metadata_json,
                       d.embedding_version, d.embedding_type, d.created_at,
                       e.embedding_json
                FROM lead_docs_v1 d
                LEFT JOIN lead_embeddings_v2 e
                  ON e.lead_id = d.lead_id
                 AND e.embedding_version = d.embedding_version
                 AND e.embedding_type = d.embedding_type
                {where_sql}
                ORDER BY d.created_at DESC
                LIMIT {placeholder}"""
        )
        rows = (
            conn.cursor().execute(query, params).fetchall()
            if use_runtime_postgres()
            else conn.execute(query, params).fetchall()
        )
        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                metadata = row["metadata_json"] if isinstance(row["metadata_json"], dict) else (json.loads(row["metadata_json"]) if row["metadata_json"] else {})
            except (TypeError, json.JSONDecodeError):
                metadata = {}
            try:
                emb = row["embedding_json"] if isinstance(row["embedding_json"], list) else (json.loads(row["embedding_json"]) if row["embedding_json"] else None)
            except (TypeError, json.JSONDecodeError):
                emb = None
            out.append(
                {
                    "id": row["id"],
                    "lead_id": row["lead_id"],
                    "doc_type": row["doc_type"],
                    "content_text": row["content_text"] or "",
                    "metadata_json": metadata,
                    "embedding_version": row["embedding_version"],
                    "embedding_type": row["embedding_type"],
                    "created_at": row["created_at"],
                    "embedding": emb,
                }
            )
        return out
    finally:
        conn.close()


def insert_lead_intel_v1(
    lead_id: int,
    vertical: str,
    primary_constraint: Optional[str],
    primary_leverage: Optional[str],
    contact_priority: Optional[str],
    outreach_angle: Optional[str],
    confidence: Optional[float],
    risks: Optional[List[str]] = None,
    evidence: Optional[List[Dict[str, Any]]] = None,
) -> Optional[int]:
    """Persist LLM intelligence row with explicit evidence references."""
    if not lead_id:
        return None
    now = datetime.now(timezone.utc).isoformat()
    conf = None
    if isinstance(confidence, (int, float)):
        conf = round(max(0.0, min(1.0, float(confidence))), 2)
    params = (
        lead_id,
        vertical,
        primary_constraint,
        primary_leverage,
        contact_priority,
        outreach_angle,
        conf,
        json.dumps(risks or [], default=str),
        json.dumps(evidence or [], default=str),
        now,
    )
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO lead_intel_v1
                       (lead_id, vertical, primary_constraint, primary_leverage, contact_priority,
                        outreach_angle, confidence, risks_json, evidence_json, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                       RETURNING id""",
                    params,
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"])
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO lead_intel_v1
               (lead_id, vertical, primary_constraint, primary_leverage, contact_priority,
                outreach_angle, confidence, risks_json, evidence_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            params,
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_latest_lead_intel_v1(lead_id: int) -> Optional[Dict[str, Any]]:
    """Return latest lead_intel_v1 row for a lead."""
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT id, lead_id, vertical, primary_constraint, primary_leverage, contact_priority,
                      outreach_angle, confidence, risks_json, evidence_json, created_at
               FROM lead_intel_v1
               WHERE lead_id = ?
               ORDER BY created_at DESC
               LIMIT 1""",
            (lead_id,),
        ).fetchone()
        if not row:
            return None
        try:
            risks = json.loads(row["risks_json"]) if row["risks_json"] else []
        except (TypeError, json.JSONDecodeError):
            risks = []
        try:
            evidence = json.loads(row["evidence_json"]) if row["evidence_json"] else []
        except (TypeError, json.JSONDecodeError):
            evidence = []
        return {
            "id": row["id"],
            "lead_id": row["lead_id"],
            "vertical": row["vertical"],
            "primary_constraint": row["primary_constraint"],
            "primary_leverage": row["primary_leverage"],
            "contact_priority": row["contact_priority"],
            "outreach_angle": row["outreach_angle"],
            "confidence": row["confidence"],
            "risks": risks,
            "evidence": evidence,
            "created_at": row["created_at"],
        }
    finally:
        conn.close()


def list_latest_lead_intel_v1_for_leads(lead_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Return latest intel rows for lead_ids keyed by lead_id."""
    if not lead_ids:
        return {}
    placeholders = ",".join("?" for _ in lead_ids)
    conn = _get_conn()
    try:
        rows = conn.execute(
            f"""SELECT li.*
                FROM lead_intel_v1 li
                JOIN (
                    SELECT lead_id, MAX(created_at) AS max_created
                    FROM lead_intel_v1
                    WHERE lead_id IN ({placeholders})
                    GROUP BY lead_id
                ) latest
                  ON latest.lead_id = li.lead_id AND latest.max_created = li.created_at""",
            lead_ids,
        ).fetchall()
        out: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            try:
                risks = json.loads(row["risks_json"]) if row["risks_json"] else []
            except (TypeError, json.JSONDecodeError):
                risks = []
            try:
                evidence = json.loads(row["evidence_json"]) if row["evidence_json"] else []
            except (TypeError, json.JSONDecodeError):
                evidence = []
            out[int(row["lead_id"])] = {
                "id": row["id"],
                "lead_id": row["lead_id"],
                "vertical": row["vertical"],
                "primary_constraint": row["primary_constraint"],
                "primary_leverage": row["primary_leverage"],
                "contact_priority": row["contact_priority"],
                "outreach_angle": row["outreach_angle"],
                "confidence": row["confidence"],
                "risks": risks,
                "evidence": evidence,
                "created_at": row["created_at"],
            }
        return out
    finally:
        conn.close()


def upsert_lead_outcome(
    lead_id: int,
    vertical: Optional[str] = None,
    agency_type: Optional[str] = None,
    contacted: Optional[bool] = None,
    proposal_sent: Optional[bool] = None,
    closed: Optional[bool] = None,
    close_value_usd: Optional[float] = None,
    service_sold: Optional[str] = None,
    status: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """Insert outcome row or update existing. Only provided fields are updated."""
    now = datetime.now(timezone.utc).isoformat()
    normalized_status = status or outcome_status_from_legacy(contacted, proposal_sent, closed, status)
    conn = _get_conn()
    try:
        existing = conn.execute(
            "SELECT lead_id FROM lead_outcomes WHERE lead_id = ?", (lead_id,)
        ).fetchone()
        if existing:
            updates = ["updated_at = ?"]
            params: List[Any] = [now]
            if vertical is not None:
                updates.append("vertical = ?")
                params.append(vertical)
            if agency_type is not None:
                updates.append("agency_type = ?")
                params.append(agency_type)
            if contacted is not None:
                updates.append("contacted = ?")
                params.append(1 if contacted else 0)
            if proposal_sent is not None:
                updates.append("proposal_sent = ?")
                params.append(1 if proposal_sent else 0)
            if closed is not None:
                updates.append("closed = ?")
                params.append(1 if closed else 0)
            if close_value_usd is not None:
                updates.append("close_value_usd = ?")
                params.append(close_value_usd)
            if service_sold is not None:
                updates.append("service_sold = ?")
                params.append(service_sold)
            if status is not None:
                updates.append("status = ?")
                params.append(status)
            if notes is not None:
                updates.append("notes = ?")
                params.append(notes)
            updates.append("outcome_status = ?")
            params.append(normalized_status)
            if notes is not None:
                updates.append("outcome_note = ?")
                params.append(notes)
            updates.append("timestamp = ?")
            params.append(now)
            params.append(lead_id)
            conn.execute(
                f"UPDATE lead_outcomes SET {', '.join(updates)} WHERE lead_id = ?",
                params,
            )
        else:
            _c = 1 if contacted else 0 if contacted is not None else 0
            _p = 1 if proposal_sent else 0 if proposal_sent is not None else 0
            _cl = 1 if closed else 0 if closed is not None else 0
            conn.execute(
                """INSERT INTO lead_outcomes
                   (lead_id, vertical, agency_type, contacted, proposal_sent, closed,
                    close_value_usd, service_sold, notes, status, outcome_status, outcome_note, timestamp, updated_at, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (lead_id, vertical, agency_type, _c, _p, _cl, close_value_usd,
                 service_sold, notes, status or "new", normalized_status, notes, now, now, now),
            )
        conn.commit()
    finally:
        conn.close()


def outcome_status_from_legacy(
    contacted: Optional[bool],
    proposal_sent: Optional[bool],
    closed: Optional[bool],
    status: Optional[str],
) -> str:
    """Map legacy booleans to normalized outcome_status for hybrid RAG grouping."""
    if status:
        s = str(status).strip().lower()
        if s in {
            "contacted",
            "replied",
            "booked",
            "closed_won",
            "closed_lost",
            "no_fit",
            "qualified",
            "won",
            "lost",
            "new",
        }:
            if s == "won":
                return "closed_won"
            if s == "lost":
                return "closed_lost"
            return s
    if closed is True:
        return "closed_won"
    if proposal_sent is True:
        return "booked"
    if contacted is True:
        return "contacted"
    return "new"


def get_lead_outcome(lead_id: int) -> Optional[Dict]:
    """Return outcome row for lead or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT lead_id, vertical, agency_type, contacted, proposal_sent, closed,
                      close_value_usd, service_sold, notes, status,
                      list_id, scan_id, outcome_status, outcome_note, timestamp,
                      updated_at, created_at
               FROM lead_outcomes WHERE lead_id = ?""",
            (lead_id,),
        ).fetchone()
        if not row:
            return None
        return dict(row)
    finally:
        conn.close()


def list_signal_profile_docs(limit: int = 1000, vertical: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Return signal_profile docs with metadata for cohort filtering.
    """
    conn = _get_conn()
    try:
        params: List[Any] = ["signal_profile"]
        extra = ""
        if vertical:
            extra = " AND l.vertical = ?"
            params.append(vertical)
        params.append(max(1, int(limit)))
        rows = conn.execute(
            f"""SELECT d.id, d.lead_id, d.doc_type, d.content_text, d.metadata_json, d.created_at,
                       l.vertical
                FROM lead_docs_v1 d
                LEFT JOIN lead_intel_v1 l ON l.lead_id = d.lead_id
                WHERE d.doc_type = ? {extra}
                ORDER BY d.created_at DESC
                LIMIT ?""",
            params,
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            try:
                metadata = json.loads(row["metadata_json"]) if row["metadata_json"] else {}
            except (TypeError, json.JSONDecodeError):
                metadata = {}
            out.append(
                {
                    "id": row["id"],
                    "lead_id": row["lead_id"],
                    "doc_type": row["doc_type"],
                    "content_text": row["content_text"] or "",
                    "metadata_json": metadata,
                    "created_at": row["created_at"],
                    "vertical": row["vertical"],
                }
            )
        return out
    finally:
        conn.close()


def get_outcomes_for_lead_ids(lead_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Return outcomes keyed by lead_id for a set of leads."""
    if not lead_ids:
        return {}
    placeholders = ",".join("?" for _ in lead_ids)
    conn = _get_conn()
    try:
        rows = conn.execute(
            f"""SELECT lead_id, contacted, proposal_sent, closed, status, notes, close_value_usd, service_sold,
                       list_id, scan_id, outcome_status, outcome_note, timestamp
                FROM lead_outcomes
                WHERE lead_id IN ({placeholders})""",
            lead_ids,
        ).fetchall()
        return {int(r["lead_id"]): dict(r) for r in rows}
    finally:
        conn.close()


def get_similar_lead_ids_v2(
    embedding: List[float],
    limit: int = 25,
    embedding_version: str = "v1_structural",
    embedding_type: str = "objective_state",
    exclude_lead_id: Optional[int] = None,
) -> List[tuple]:
    """
    Return (lead_id, similarity, text_snapshot) from lead_embeddings_v2,
    ordered by cosine similarity. Excludes exclude_lead_id if provided.
    """
    conn = _get_conn()
    try:
        if exclude_lead_id is not None:
            rows = conn.execute(
                """SELECT lead_id, embedding_json, text_snapshot
                   FROM lead_embeddings_v2
                   WHERE embedding_version = ? AND embedding_type = ? AND lead_id != ?""",
                (embedding_version, embedding_type, exclude_lead_id),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT lead_id, embedding_json, text_snapshot
                   FROM lead_embeddings_v2
                   WHERE embedding_version = ? AND embedding_type = ?""",
                (embedding_version, embedding_type),
            ).fetchall()
        scored = []
        for row in rows:
            try:
                other = json.loads(row["embedding_json"])
            except (json.JSONDecodeError, TypeError):
                continue
            sim = _cosine_similarity(embedding, other)
            scored.append((row["lead_id"], round(sim, 4), row["text_snapshot"] or ""))
        scored.sort(key=lambda x: -x[1])
        return scored[:limit]
    finally:
        conn.close()


def get_similar_outcome_stats(
    lead_embedding: List[float],
    limit: int = 25,
    embedding_version: str = "v1_structural",
    embedding_type: str = "objective_state",
) -> Dict:
    """
    Compute similarity-based outcome stats for similar leads.
    Returns n_similar, n_with_outcomes, close_rate, contacted_rate, proposal_rate, top_service_sold.
    If n_with_outcomes < 5, sets insufficient_outcomes: True.
    """
    similar = get_similar_lead_ids_v2(
        lead_embedding,
        limit=limit,
        embedding_version=embedding_version,
        embedding_type=embedding_type,
    )
    n_similar = len(similar)
    if n_similar == 0:
        return {"n_similar": 0, "n_with_outcomes": 0, "insufficient_outcomes": True}

    lead_ids = [x[0] for x in similar]
    placeholders = ",".join("?" * len(lead_ids))
    conn = _get_conn()
    try:
        rows = conn.execute(
            f"""SELECT lead_id, contacted, proposal_sent, closed, close_value_usd, service_sold
                FROM lead_outcomes WHERE lead_id IN ({placeholders})""",
            lead_ids,
        ).fetchall()
    finally:
        conn.close()

    n_with_outcomes = len(rows)
    if n_with_outcomes < 5:
        return {
            "n_similar": n_similar,
            "n_with_outcomes": n_with_outcomes,
            "insufficient_outcomes": True,
        }

    contacted_count = sum(1 for r in rows if r["contacted"])
    proposal_count = sum(1 for r in rows if r["proposal_sent"])
    closed_count = sum(1 for r in rows if r["closed"])
    services = [r["service_sold"] for r in rows if r["service_sold"]]
    top_service = None
    if services:
        top_service = Counter(services).most_common(1)[0][0]

    return {
        "n_similar": n_similar,
        "n_with_outcomes": n_with_outcomes,
        "contacted_rate": round(contacted_count / n_with_outcomes, 2) if n_with_outcomes else 0,
        "proposal_rate": round(proposal_count / n_with_outcomes, 2) if n_with_outcomes else 0,
        "close_rate": round(closed_count / n_with_outcomes, 2) if n_with_outcomes else 0,
        "top_service_sold": top_service,
        "insufficient_outcomes": False,
    }


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    """Cosine similarity; 0 if vectors invalid."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def get_similar_lead_ids(
    embedding: List[float],
    limit: int = 5,
    exclude_run_id: Optional[str] = None,
) -> List[tuple]:
    """
    Return (lead_id, similarity, text_snapshot) for leads with stored embeddings,
    ordered by cosine similarity (highest first). Excludes leads from exclude_run_id.
    """
    conn = _get_conn()
    try:
        if exclude_run_id:
            rows = conn.execute(
                """SELECT le.lead_id, le.embedding_json, le.text_snapshot
                   FROM lead_embeddings le
                   JOIN leads l ON l.id = le.lead_id
                   WHERE l.run_id != ?""",
                (exclude_run_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT lead_id, embedding_json, text_snapshot FROM lead_embeddings"
            ).fetchall()
        scored = []
        for row in rows:
            try:
                other = json.loads(row["embedding_json"])
            except (json.JSONDecodeError, TypeError):
                continue
            sim = _cosine_similarity(embedding, other)
            scored.append((row["lead_id"], round(sim, 4), row["text_snapshot"] or ""))
        scored.sort(key=lambda x: -x[1])
        return scored[:limit]
    finally:
        conn.close()


def update_run_completed(run_id: str, leads_count: int, run_stats: Optional[Dict] = None) -> None:
    """Set run status to completed, leads_count, and optional run_stats (health/coverage metrics)."""
    stats_json = json.dumps(run_stats) if run_stats else None
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE runs SET status = %s, leads_count = %s, run_stats = %s::jsonb WHERE id = %s",
                    ("completed", leads_count, stats_json, run_id),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
                "UPDATE runs SET status = ?, leads_count = ?, run_stats = ? WHERE id = ?",
                ("completed", leads_count, stats_json, run_id)
            )
            conn.commit()
        except sqlite3.OperationalError:
            conn.rollback()
            conn.execute(
                "UPDATE runs SET status = ?, leads_count = ? WHERE id = ?",
                ("completed", leads_count, run_id)
            )
            conn.commit()
        finally:
            conn.close()


def update_run_failed(run_id: str) -> None:
    """Set run status to failed."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute("UPDATE runs SET status = %s WHERE id = %s", ("failed", run_id))
        else:
            conn.execute("UPDATE runs SET status = ? WHERE id = ?", ("failed", run_id))
        conn.commit()
    finally:
        conn.close()


def get_leads_with_context_deduped_by_place_id(limit_runs: int = 10) -> List[Dict]:
    """
    Get leads from latest completed runs, one per place_id (most recent run wins).
    For export when --dedupe-by-place-id is set.
    """
    runs = list_runs(limit=limit_runs, status="completed")
    by_place = {}
    for r in runs:
        for lead in get_leads_with_context_by_run(r["id"]):
            pid = lead.get("place_id")
            if pid and pid not in by_place:
                by_place[pid] = lead
    return list(by_place.values())


def get_latest_run_id() -> Optional[str]:
    """Return the most recent run id by created_at."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                "SELECT id FROM runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT id FROM runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        )
        return row["id"] if row else None
    finally:
        conn.close()


def get_run(run_id: str) -> Optional[Dict]:
    """Get run by id."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute("SELECT * FROM runs WHERE id = %s", (run_id,)).fetchone()
            if use_runtime_postgres()
            else conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        )
        if not row:
            return None
        row_dict = dict(row)
        return {
            "id": row_dict["id"],
            "created_at": row_dict["created_at"],
            "config": row_dict["config"] if isinstance(row_dict.get("config"), dict) else (json.loads(row_dict["config"]) if row_dict.get("config") else None),
            "leads_count": row_dict["leads_count"],
            "status": row_dict["status"],
            "run_stats": row_dict["run_stats"] if isinstance(row_dict.get("run_stats"), dict) else (json.loads(row_dict["run_stats"]) if row_dict.get("run_stats") else None),
        }
    finally:
        conn.close()


def list_runs(limit: int = 50, status: Optional[str] = None) -> List[Dict]:
    """List runs, newest first. Optionally filter by status (e.g. 'completed', 'running', 'failed')."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if status:
            rows = (
                conn.cursor().execute(
                    "SELECT * FROM runs WHERE status = %s ORDER BY created_at DESC LIMIT %s",
                    (status, limit),
                ).fetchall()
                if use_runtime_postgres()
                else conn.execute(
                    "SELECT * FROM runs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit)
                ).fetchall()
            )
        else:
            rows = (
                conn.cursor().execute(
                    "SELECT * FROM runs ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                ).fetchall()
                if use_runtime_postgres()
                else conn.execute(
                    "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            )
        return [
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "config": row["config"] if isinstance(row["config"], dict) else (json.loads(row["config"]) if row["config"] else None),
                "leads_count": row["leads_count"],
                "status": row["status"],
                "run_stats": row["run_stats"] if isinstance(row.get("run_stats") if hasattr(row, "get") else dict(row).get("run_stats"), dict) else (json.loads((row.get("run_stats") if hasattr(row, "get") else dict(row).get("run_stats"))) if ((row.get("run_stats") if hasattr(row, "get") else dict(row).get("run_stats"))) else None),
            }
            for row in rows
        ]
    finally:
        conn.close()


def delete_run(run_id: str) -> int:
    """
    Delete a run and all its leads, signals, context, and embeddings.
    Returns number of leads deleted.
    """
    conn = _get_conn()
    try:
        lead_ids = [row["id"] for row in conn.execute("SELECT id FROM leads WHERE run_id = ?", (run_id,)).fetchall()]
        n = len(lead_ids)
        if not lead_ids:
            conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
            conn.commit()
            return 0
        placeholders = ",".join("?" * len(lead_ids))
        conn.execute(f"DELETE FROM decisions WHERE lead_id IN ({placeholders})", lead_ids)
        conn.execute(f"DELETE FROM lead_embeddings WHERE lead_id IN ({placeholders})", lead_ids)
        conn.execute(f"DELETE FROM context_dimensions WHERE lead_id IN ({placeholders})", lead_ids)
        conn.execute(f"DELETE FROM lead_signals WHERE lead_id IN ({placeholders})", lead_ids)
        conn.execute("DELETE FROM leads WHERE run_id = ?", (run_id,))
        conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
        conn.commit()
        return n
    finally:
        conn.close()


def prune_runs(
    keep_last_n: Optional[int] = None,
    older_than_days: Optional[int] = None,
) -> int:
    """
    Delete runs by retention policy. Returns total number of leads deleted.
    - keep_last_n: keep only the N most recent completed runs (by created_at).
    - older_than_days: delete runs created more than this many days ago.
    At least one of keep_last_n or older_than_days must be set.
    """
    runs = list_runs(limit=10000)
    to_delete = set()
    if keep_last_n is not None and keep_last_n > 0:
        completed = [r for r in runs if r.get("status") == "completed"]
        completed.sort(key=lambda r: r.get("created_at") or "", reverse=True)
        for r in completed[keep_last_n:]:
            to_delete.add(r["id"])
    if older_than_days is not None and older_than_days > 0:
        from datetime import datetime, timezone, timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=older_than_days)).isoformat()
        for r in runs:
            if r.get("created_at") and r["created_at"] < cutoff:
                to_delete.add(r["id"])
    total = 0
    for run_id in to_delete:
        total += delete_run(run_id)
    return total


def get_leads_with_context_by_run(run_id: str) -> List[Dict]:
    """
    Return all leads for a run with signals and context dimensions joined.
    Each item: lead fields + signals_json (parsed) + context fields (dimensions, reasoning, etc.)
    """
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            rows = conn.cursor().execute(
                """SELECT l.id AS lead_id, l.run_id, l.place_id, l.name, l.address, l.latitude, l.longitude,
                          ls.signals_json, cd.dimensions_json, cd.reasoning_summary, cd.priority_suggestion,
                          cd.primary_themes_json, cd.outreach_angles_json, cd.overall_confidence, cd.reasoning_source,
                          cd.no_opportunity, cd.no_opportunity_reason, cd.priority_derivation, cd.validation_warnings
                   FROM leads l
                   LEFT JOIN lead_signals ls ON ls.lead_id = l.id
                   LEFT JOIN context_dimensions cd ON cd.lead_id = l.id
                   WHERE l.run_id = %s
                   ORDER BY l.id""",
                (run_id,),
            ).fetchall()
            out = []
            for row in rows:
                row = dict(row)
                lead = {
                    "lead_id": row["lead_id"],
                    "run_id": row["run_id"],
                    "place_id": row["place_id"],
                    "name": row["name"],
                    "address": row["address"],
                    "latitude": row["latitude"],
                    "longitude": row["longitude"],
                    "raw_signals": row["signals_json"] if isinstance(row.get("signals_json"), dict) else (json.loads(row["signals_json"]) if row.get("signals_json") else {}),
                    "context_dimensions": row["dimensions_json"] if isinstance(row.get("dimensions_json"), list) else (json.loads(row["dimensions_json"]) if row.get("dimensions_json") else []),
                    "reasoning_summary": row["reasoning_summary"] or "",
                    "priority_suggestion": row["priority_suggestion"],
                    "primary_themes": row["primary_themes_json"] if isinstance(row.get("primary_themes_json"), list) else (json.loads(row["primary_themes_json"]) if row.get("primary_themes_json") else []),
                    "suggested_outreach_angles": row["outreach_angles_json"] if isinstance(row.get("outreach_angles_json"), list) else (json.loads(row["outreach_angles_json"]) if row.get("outreach_angles_json") else []),
                    "confidence": row["overall_confidence"],
                    "reasoning_source": row["reasoning_source"],
                }
                if row.get("no_opportunity") is not None:
                    lead["no_opportunity"] = bool(row["no_opportunity"])
                    lead["no_opportunity_reason"] = row.get("no_opportunity_reason")
                if row.get("priority_derivation") is not None:
                    lead["priority_derivation"] = row["priority_derivation"]
                if row.get("validation_warnings") is not None:
                    try:
                        lead["validation_warnings"] = row["validation_warnings"] if isinstance(row["validation_warnings"], list) else json.loads(row["validation_warnings"])
                    except (TypeError, json.JSONDecodeError):
                        lead["validation_warnings"] = []
                out.append(lead)
            return out
        finally:
            conn.close()
    conn = _get_conn()
    try:
        try:
            rows = conn.execute(
                """SELECT l.id AS lead_id, l.run_id, l.place_id, l.name, l.address, l.latitude, l.longitude,
                          ls.signals_json, cd.dimensions_json, cd.reasoning_summary, cd.priority_suggestion,
                          cd.primary_themes_json, cd.outreach_angles_json, cd.overall_confidence, cd.reasoning_source,
                          cd.no_opportunity, cd.no_opportunity_reason, cd.priority_derivation, cd.validation_warnings
                   FROM leads l
                   LEFT JOIN lead_signals ls ON ls.lead_id = l.id
                   LEFT JOIN context_dimensions cd ON cd.lead_id = l.id
                   WHERE l.run_id = ?
                   ORDER BY l.id""",
                (run_id,)
            ).fetchall()
        except sqlite3.OperationalError:
            rows = conn.execute(
                """SELECT l.id AS lead_id, l.run_id, l.place_id, l.name, l.address, l.latitude, l.longitude,
                          ls.signals_json, cd.dimensions_json, cd.reasoning_summary, cd.priority_suggestion,
                          cd.primary_themes_json, cd.outreach_angles_json, cd.overall_confidence, cd.reasoning_source
                   FROM leads l
                   LEFT JOIN lead_signals ls ON ls.lead_id = l.id
                   LEFT JOIN context_dimensions cd ON cd.lead_id = l.id
                   WHERE l.run_id = ?
                   ORDER BY l.id""",
                (run_id,)
            ).fetchall()
        out = []
        for row in rows:
            row = dict(row)
            lead = {
                "lead_id": row["lead_id"],
                "run_id": row["run_id"],
                "place_id": row["place_id"],
                "name": row["name"],
                "address": row["address"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "raw_signals": json.loads(row["signals_json"]) if row["signals_json"] else {},
                "context_dimensions": json.loads(row["dimensions_json"]) if row["dimensions_json"] else [],
                "reasoning_summary": row["reasoning_summary"] or "",
                "priority_suggestion": row["priority_suggestion"],
                "primary_themes": json.loads(row["primary_themes_json"]) if row["primary_themes_json"] else [],
                "suggested_outreach_angles": json.loads(row["outreach_angles_json"]) if row["outreach_angles_json"] else [],
                "confidence": row["overall_confidence"],
                "reasoning_source": row["reasoning_source"],
            }
            if row.get("no_opportunity") is not None:
                lead["no_opportunity"] = bool(row["no_opportunity"])
                lead["no_opportunity_reason"] = row.get("no_opportunity_reason")
            if row.get("priority_derivation") is not None:
                lead["priority_derivation"] = row["priority_derivation"]
            if row.get("validation_warnings") is not None:
                try:
                    lead["validation_warnings"] = json.loads(row["validation_warnings"])
                except (TypeError, json.JSONDecodeError):
                    lead["validation_warnings"] = []
            out.append(lead)
        return out
    finally:
        conn.close()


def get_leads_with_decisions_by_run(run_id: str) -> List[Dict]:
    """
    Return all leads for a run with signals and decision joined.
    Each item: lead fields + raw_signals + verdict, confidence, reasoning, primary_risks, what_would_change, agency_type, prompt_version.
    """
    conn = _get_conn()
    try:
        try:
            rows = conn.execute(
                """SELECT l.id AS lead_id, l.run_id, l.place_id, l.name, l.address, l.latitude, l.longitude,
                          ls.signals_json,
                          d.agency_type, d.verdict, d.confidence, d.reasoning, d.primary_risks, d.what_would_change, d.prompt_version,
                          l.dentist_profile_v1_json, l.llm_reasoning_layer_json, l.sales_intervention_intelligence_json, l.objective_decision_layer_json
                   FROM leads l
                   LEFT JOIN lead_signals ls ON ls.lead_id = l.id
                   LEFT JOIN decisions d ON d.lead_id = l.id
                   WHERE l.run_id = ?
                   ORDER BY l.id""",
                (run_id,),
            ).fetchall()
        except sqlite3.OperationalError:
            rows = conn.execute(
                """SELECT l.id AS lead_id, l.run_id, l.place_id, l.name, l.address, l.latitude, l.longitude,
                          ls.signals_json,
                          d.agency_type, d.verdict, d.confidence, d.reasoning, d.primary_risks, d.what_would_change, d.prompt_version
                   FROM leads l
                   LEFT JOIN lead_signals ls ON ls.lead_id = l.id
                   LEFT JOIN decisions d ON d.lead_id = l.id
                   WHERE l.run_id = ?
                   ORDER BY l.id""",
                (run_id,),
            ).fetchall()
        out = []
        for row in rows:
            lead = {
                "lead_id": row["lead_id"],
                "run_id": row["run_id"],
                "place_id": row["place_id"],
                "name": row["name"],
                "address": row["address"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "raw_signals": json.loads(row["signals_json"]) if row["signals_json"] else {},
                "verdict": row["verdict"] if row.get("verdict") else None,
                "confidence": row["confidence"] if row.get("confidence") is not None else None,
                "reasoning": row["reasoning"] or "",
                "primary_risks": json.loads(row["primary_risks"]) if row.get("primary_risks") else [],
                "what_would_change": json.loads(row["what_would_change"]) if row.get("what_would_change") else [],
                "agency_type": row["agency_type"] if row.get("agency_type") else None,
                "prompt_version": row["prompt_version"] if row.get("prompt_version") else None,
            }
            try:
                if row["dentist_profile_v1_json"] is not None:
                    lead["dentist_profile_v1"] = json.loads(row["dentist_profile_v1_json"])
            except (KeyError, TypeError, json.JSONDecodeError):
                pass
            try:
                if row["llm_reasoning_layer_json"] is not None:
                    lead["llm_reasoning_layer"] = json.loads(row["llm_reasoning_layer_json"])
            except (KeyError, TypeError, json.JSONDecodeError):
                pass
            try:
                if row["sales_intervention_intelligence_json"] is not None:
                    lead["sales_intervention_intelligence"] = json.loads(row["sales_intervention_intelligence_json"])
            except (KeyError, TypeError, json.JSONDecodeError):
                pass
            try:
                if row["objective_decision_layer_json"] is not None:
                    lead["objective_decision_layer"] = json.loads(row["objective_decision_layer_json"])
            except (KeyError, TypeError, json.JSONDecodeError):
                pass
            out.append(lead)
        return out
    finally:
        conn.close()


def get_leads_with_decisions_deduped_by_place_id(limit_runs: int = 10) -> List[Dict]:
    """Get leads from latest completed runs with decisions, one per place_id (most recent run wins)."""
    runs = list_runs(limit=limit_runs, status="completed")
    by_place = {}
    for r in runs:
        for lead in get_leads_with_decisions_by_run(r["id"]):
            pid = lead.get("place_id")
            if pid and pid not in by_place:
                by_place[pid] = lead
    return list(by_place.values())


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def create_job(user_id: int, job_type: str, input_data: Dict) -> str:
    """Create a pending job; return job_id (UUID)."""
    init_db()
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO jobs (id, user_id, type, status, input_json, created_at) VALUES (%s, %s, %s, %s, %s::jsonb, %s)",
                    (job_id, user_id, job_type, "pending", json.dumps(input_data, default=str), now),
                )
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO jobs (id, user_id, type, status, input_json, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (job_id, user_id, job_type, "pending", json.dumps(input_data, default=str), now),
            )
            conn.commit()
        finally:
            conn.close()
    return job_id


def get_job(job_id: str) -> Optional[Dict]:
    """Return job dict or None."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute("SELECT * FROM jobs WHERE id = %s", (job_id,)).fetchone()
            if use_runtime_postgres()
            else conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        )
        if not row:
            return None
        d = dict(row)
        if d.get("input_json") and not isinstance(d.get("input_json"), dict):
            d["input"] = json.loads(d["input_json"])
        elif d.get("input_json") is not None:
            d["input"] = d["input_json"]
        if d.get("result_json") and not isinstance(d.get("result_json"), dict):
            d["result"] = json.loads(d["result_json"])
        elif d.get("result_json") is not None:
            d["result"] = d["result_json"]
        return d
    finally:
        conn.close()


def update_job_status(job_id: str, status: str, result: Optional[Dict] = None, error: Optional[str] = None) -> None:
    """Update job status, optionally storing result or error."""
    now = datetime.now(timezone.utc).isoformat()
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                if result is not None:
                    if status in {"completed", "failed"}:
                        cur.execute(
                            "UPDATE jobs SET status = %s, result_json = %s::jsonb, completed_at = %s WHERE id = %s",
                            (status, json.dumps(result, default=str), now, job_id),
                        )
                    else:
                        cur.execute(
                            "UPDATE jobs SET status = %s, result_json = %s::jsonb WHERE id = %s",
                            (status, json.dumps(result, default=str), job_id),
                        )
                elif error is not None:
                    cur.execute(
                        "UPDATE jobs SET status = %s, error = %s, completed_at = %s WHERE id = %s",
                        (status, error, now, job_id),
                    )
                else:
                    cur.execute("UPDATE jobs SET status = %s WHERE id = %s", (status, job_id))
            conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_conn()
        try:
            if result is not None:
                if status in {"completed", "failed"}:
                    conn.execute(
                        "UPDATE jobs SET status = ?, result_json = ?, completed_at = ? WHERE id = ?",
                        (status, json.dumps(result, default=str), now, job_id),
                    )
                else:
                    conn.execute(
                        "UPDATE jobs SET status = ?, result_json = ? WHERE id = ?",
                        (status, json.dumps(result, default=str), job_id),
                    )
            elif error is not None:
                conn.execute(
                    "UPDATE jobs SET status = ?, error = ?, completed_at = ? WHERE id = ?",
                    (status, error, now, job_id),
                )
            else:
                conn.execute("UPDATE jobs SET status = ? WHERE id = ?", (status, job_id))
            conn.commit()
        finally:
            conn.close()


def get_pending_jobs(limit: int = 10) -> List[Dict]:
    """Return oldest pending jobs."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                "SELECT * FROM jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT %s",
                (limit,),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT * FROM jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT ?",
                (limit,),
            ).fetchall()
        )
        out = []
        for row in rows:
            d = dict(row)
            if d.get("input_json") and not isinstance(d.get("input_json"), dict):
                d["input"] = json.loads(d["input_json"])
            elif d.get("input_json") is not None:
                d["input"] = d["input_json"]
            out.append(d)
        return out
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Diagnostics (SaaS layer)
# ---------------------------------------------------------------------------

def save_diagnostic(user_id: int, job_id: Optional[str], place_id: Optional[str],
                    business_name: str, city: str, brief: Optional[Dict],
                    response: Dict, state: Optional[str] = None) -> int:
    """Save a completed diagnostic; return diagnostic_id."""
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    lead_quality = response.get("lead_quality") if isinstance(response, dict) else None
    params = (
        user_id, job_id, place_id, business_name, city, state,
        float(lead_quality.get("score")) if isinstance(lead_quality, dict) and lead_quality.get("score") is not None else None,
        lead_quality.get("class") if isinstance(lead_quality, dict) else None,
        lead_quality.get("model_version") if isinstance(lead_quality, dict) else None,
        lead_quality.get("feature_version") if isinstance(lead_quality, dict) else None,
        json.dumps(lead_quality, default=str) if isinstance(lead_quality, dict) else None,
        json.dumps(brief, default=str) if brief else None,
        json.dumps(response, default=str), now,
    )
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO diagnostics (user_id, job_id, place_id, business_name, city,
                       state, lead_quality_score, lead_quality_class, lead_model_version, lead_feature_version,
                       lead_quality_json, brief_json, response_json, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                       RETURNING id""",
                    params,
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"])
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO diagnostics (user_id, job_id, place_id, business_name, city,
               state, lead_quality_score, lead_quality_class, lead_model_version, lead_feature_version,
               lead_quality_json, brief_json, response_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            params,
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_diagnostics(user_id: int, limit: int = 50, offset: int = 0) -> List[Dict]:
    """Return diagnostics for a user, newest first."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                "SELECT * FROM diagnostics WHERE user_id = %s ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (user_id, limit, offset),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT * FROM diagnostics WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (user_id, limit, offset),
            ).fetchall()
        )
        out = []
        for row in rows:
            d = dict(row)
            if d.get("response_json") and not isinstance(d.get("response_json"), dict):
                d["response"] = json.loads(d["response_json"])
            elif d.get("response_json") is not None:
                d["response"] = d["response_json"]
            if d.get("lead_quality_json") and not isinstance(d.get("lead_quality_json"), dict):
                try:
                    lead_quality = json.loads(d["lead_quality_json"])
                except (TypeError, json.JSONDecodeError):
                    lead_quality = None
            else:
                lead_quality = d.get("lead_quality_json")
            if lead_quality and isinstance(d.get("response"), dict) and "lead_quality" not in d["response"]:
                d["response"]["lead_quality"] = lead_quality
            if d.get("brief_json") and not isinstance(d.get("brief_json"), dict):
                d["brief"] = json.loads(d["brief_json"])
            elif d.get("brief_json") is not None:
                d["brief"] = d["brief_json"]
            out.append(d)
        return out
    finally:
        conn.close()


def get_diagnostic(diagnostic_id: int, user_id: int) -> Optional[Dict]:
    """Return a single diagnostic owned by user_id, or None."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                "SELECT * FROM diagnostics WHERE id = %s AND user_id = %s",
                (diagnostic_id, user_id),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT * FROM diagnostics WHERE id = ? AND user_id = ?",
                (diagnostic_id, user_id),
            ).fetchone()
        )
        if not row:
            return None
        d = dict(row)
        if d.get("response_json") and not isinstance(d.get("response_json"), dict):
            d["response"] = json.loads(d["response_json"])
        elif d.get("response_json") is not None:
            d["response"] = d["response_json"]
        if d.get("lead_quality_json") and not isinstance(d.get("lead_quality_json"), dict):
            try:
                lead_quality = json.loads(d["lead_quality_json"])
            except (TypeError, json.JSONDecodeError):
                lead_quality = None
        else:
            lead_quality = d.get("lead_quality_json")
        if lead_quality and isinstance(d.get("response"), dict) and "lead_quality" not in d["response"]:
            d["response"]["lead_quality"] = lead_quality
        if d.get("brief_json") and not isinstance(d.get("brief_json"), dict):
            d["brief"] = json.loads(d["brief_json"])
        elif d.get("brief_json") is not None:
            d["brief"] = d["brief_json"]
        return d
    finally:
        conn.close()


def get_diagnostic_any_user(diagnostic_id: int) -> Optional[Dict]:
    """Return a single diagnostic by id, regardless of owner."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute("SELECT * FROM diagnostics WHERE id = %s", (diagnostic_id,)).fetchone()
            if use_runtime_postgres()
            else conn.execute("SELECT * FROM diagnostics WHERE id = ?", (diagnostic_id,)).fetchone()
        )
        if not row:
            return None
        d = dict(row)
        if d.get("response_json") and not isinstance(d.get("response_json"), dict):
            d["response"] = json.loads(d["response_json"])
        elif d.get("response_json") is not None:
            d["response"] = d["response_json"]
        if d.get("lead_quality_json") and not isinstance(d.get("lead_quality_json"), dict):
            try:
                lead_quality = json.loads(d["lead_quality_json"])
            except (TypeError, json.JSONDecodeError):
                lead_quality = None
        else:
            lead_quality = d.get("lead_quality_json")
        if lead_quality and isinstance(d.get("response"), dict) and "lead_quality" not in d["response"]:
            d["response"]["lead_quality"] = lead_quality
        if d.get("brief_json") and not isinstance(d.get("brief_json"), dict):
            d["brief"] = json.loads(d["brief_json"])
        elif d.get("brief_json") is not None:
            d["brief"] = d["brief_json"]
        return d
    finally:
        conn.close()


def create_brief_share_token(
    diagnostic_id: int,
    user_id: int,
    token: str,
    expires_at: Optional[str] = None,
) -> None:
    """Persist a share token for one diagnostic."""
    now = datetime.now(timezone.utc).isoformat()
    init_db()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO brief_share_tokens
                       (diagnostic_id, user_id, token, created_at, expires_at)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (diagnostic_id, user_id, token, now, expires_at),
                )
        else:
            conn.execute(
                """INSERT INTO brief_share_tokens
                   (diagnostic_id, user_id, token, created_at, expires_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (diagnostic_id, user_id, token, now, expires_at),
            )
        conn.commit()
    finally:
        conn.close()


def get_share_token_record(token: str) -> Optional[Dict[str, Any]]:
    """Return share token row if present."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute("SELECT * FROM brief_share_tokens WHERE token = %s", (token,)).fetchone()
            if use_runtime_postgres()
            else conn.execute("SELECT * FROM brief_share_tokens WHERE token = ?", (token,)).fetchone()
        )
        return dict(row) if row else None
    finally:
        conn.close()


def delete_diagnostic(diagnostic_id: int, user_id: int) -> bool:
    """Delete a diagnostic owned by user_id. Returns True if deleted."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute("DELETE FROM diagnostics WHERE id = %s AND user_id = %s", (diagnostic_id, user_id))
                deleted = cur.rowcount > 0
        else:
            cur = conn.execute(
                "DELETE FROM diagnostics WHERE id = ? AND user_id = ?",
                (diagnostic_id, user_id),
            )
            deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        conn.close()


def count_diagnostics(user_id: int) -> int:
    """Return total diagnostics for a user."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute("SELECT COUNT(*) as cnt FROM diagnostics WHERE user_id = %s", (user_id,)).fetchone()
            if use_runtime_postgres()
            else conn.execute("SELECT COUNT(*) as cnt FROM diagnostics WHERE user_id = ?", (user_id,)).fetchone()
        )
        return row["cnt"] if row else 0
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Territory scans + ranked prospects
# ---------------------------------------------------------------------------

def create_territory_scan(
    scan_id: str,
    user_id: int,
    job_id: Optional[str],
    city: str,
    state: Optional[str],
    vertical: str,
    limit_count: int,
    filters: Optional[Dict[str, Any]] = None,
    scan_type: str = "territory",
    list_id: Optional[int] = None,
) -> None:
    """Create a new territory scan metadata row."""
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    payload = (
        scan_id,
        user_id,
        job_id,
        scan_type,
        city,
        state,
        vertical,
        limit_count,
        json.dumps(filters or {}, default=str),
        list_id,
        "pending",
        now,
    )
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO territory_scans
                       (id, user_id, job_id, scan_type, city, state, vertical, limit_count,
                        filters_json, list_id, status, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)""",
                    payload,
                )
        else:
            conn.execute(
            """INSERT INTO territory_scans
               (id, user_id, job_id, scan_type, city, state, vertical, limit_count,
                filters_json, list_id, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            payload,
            )
        conn.commit()
    finally:
        conn.close()


def update_territory_scan_status(
    scan_id: str,
    status: str,
    summary: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """Update scan status and optional summary/error."""
    now = datetime.now(timezone.utc).isoformat()
    summary_json = json.dumps(summary, default=str) if summary else None
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                if status in {"completed", "failed"}:
                    cur.execute(
                        """UPDATE territory_scans
                           SET status = %s, summary_json = %s::jsonb, error = %s, completed_at = %s
                           WHERE id = %s""",
                        (status, summary_json, error, now, scan_id),
                    )
                else:
                    cur.execute(
                        """UPDATE territory_scans
                           SET status = %s, summary_json = %s::jsonb, error = %s
                           WHERE id = %s""",
                        (status, summary_json, error, scan_id),
                    )
        else:
            if status in {"completed", "failed"}:
                conn.execute(
                    """UPDATE territory_scans
                       SET status = ?, summary_json = ?, error = ?, completed_at = ?
                       WHERE id = ?""",
                    (status, summary_json, error, now, scan_id),
                )
            else:
                conn.execute(
                    """UPDATE territory_scans
                       SET status = ?, summary_json = ?, error = ?
                       WHERE id = ?""",
                    (status, summary_json, error, scan_id),
                )
        conn.commit()
    finally:
        conn.close()


def get_territory_scan(scan_id: str, user_id: int) -> Optional[Dict[str, Any]]:
    """Return scan metadata row for owner."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                "SELECT * FROM territory_scans WHERE id = %s AND user_id = %s",
                (scan_id, user_id),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT * FROM territory_scans WHERE id = ? AND user_id = ?",
                (scan_id, user_id),
            ).fetchone()
        )
        if not row:
            return None
        out = dict(row)
        if out.get("filters_json") and not isinstance(out.get("filters_json"), dict):
            out["filters"] = json.loads(out["filters_json"])
        elif out.get("filters_json") is not None:
            out["filters"] = out["filters_json"]
        if out.get("summary_json") and not isinstance(out.get("summary_json"), dict):
            out["summary"] = json.loads(out["summary_json"])
        elif out.get("summary_json") is not None:
            out["summary"] = out["summary_json"]
        return out
    finally:
        conn.close()


def list_territory_scans(user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    """List recent territory scans for a user with prospect counts."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                """SELECT ts.id, ts.city, ts.state, ts.vertical, ts.limit_count, ts.status,
                          ts.created_at, ts.completed_at, ts.summary_json,
                          COUNT(tp.id) AS prospects_count
                   FROM territory_scans ts
                   LEFT JOIN territory_prospects tp ON tp.scan_id = ts.id
                   WHERE ts.user_id = %s AND ts.scan_type = 'territory'
                   GROUP BY ts.id, ts.city, ts.state, ts.vertical, ts.limit_count, ts.status,
                            ts.created_at, ts.completed_at, ts.summary_json
                   ORDER BY ts.created_at DESC
                   LIMIT %s""",
                (user_id, limit),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
                """SELECT ts.id, ts.city, ts.state, ts.vertical, ts.limit_count, ts.status,
                          ts.created_at, ts.completed_at, ts.summary_json,
                          COUNT(tp.id) AS prospects_count
                   FROM territory_scans ts
                   LEFT JOIN territory_prospects tp ON tp.scan_id = ts.id
                   WHERE ts.user_id = ? AND ts.scan_type = 'territory'
                   GROUP BY ts.id
                   ORDER BY ts.created_at DESC
                   LIMIT ?""",
                (user_id, limit),
            ).fetchall()
        )
        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            if d.get("summary_json") and not isinstance(d.get("summary_json"), dict):
                d["summary"] = json.loads(d["summary_json"])
            elif d.get("summary_json") is not None:
                d["summary"] = d["summary_json"]
            out.append(d)
        return out
    finally:
        conn.close()


def add_scan_diagnostic(
    scan_id: str,
    diagnostic_id: int,
    place_id: Optional[str],
    business_name: Optional[str],
    city: Optional[str],
    state: Optional[str],
    previous_diagnostic_id: Optional[int] = None,
    change: Optional[Dict[str, Any]] = None,
) -> None:
    """Attach a diagnostic to a scan."""
    now = datetime.now(timezone.utc).isoformat()
    payload = (
        scan_id,
        diagnostic_id,
        place_id,
        business_name,
        city,
        state,
        previous_diagnostic_id,
        json.dumps(change, default=str) if change else None,
        now,
    )
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO territory_scan_diagnostics
                       (scan_id, diagnostic_id, place_id, business_name, city, state,
                        previous_diagnostic_id, change_json, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)""",
                    payload,
                )
        else:
            conn.execute(
            """INSERT INTO territory_scan_diagnostics
               (scan_id, diagnostic_id, place_id, business_name, city, state,
                previous_diagnostic_id, change_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            payload,
            )
        conn.commit()
    finally:
        conn.close()


def get_scan_diagnostics(scan_id: str) -> List[Dict[str, Any]]:
    """Return diagnostics linked to a scan with parsed diagnostic response."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                """SELECT tsd.*, d.response_json, d.created_at AS diagnostic_created_at
                   FROM territory_scan_diagnostics tsd
                   JOIN diagnostics d ON d.id = tsd.diagnostic_id
                   WHERE tsd.scan_id = %s
                   ORDER BY tsd.id ASC""",
                (scan_id,),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
            """SELECT tsd.*, d.response_json, d.created_at AS diagnostic_created_at
               FROM territory_scan_diagnostics tsd
               JOIN diagnostics d ON d.id = tsd.diagnostic_id
               WHERE tsd.scan_id = ?
               ORDER BY tsd.id ASC""",
            (scan_id,),
            ).fetchall()
        )
        out = []
        for row in rows:
            d = dict(row)
            if d.get("response_json") and not isinstance(d.get("response_json"), dict):
                d["response"] = json.loads(d["response_json"])
            elif d.get("response_json") is not None:
                d["response"] = d["response_json"]
            if d.get("change_json") and not isinstance(d.get("change_json"), dict):
                d["change"] = json.loads(d["change_json"])
            elif d.get("change_json") is not None:
                d["change"] = d["change_json"]
            out.append(d)
        return out
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Territory prospects (Tier 1 rows)
# ---------------------------------------------------------------------------

def save_territory_prospects(scan_id: str, user_id: int, prospects: List[Dict[str, Any]]) -> int:
    """Insert/update Tier 1 prospect rows for a scan."""
    if not prospects:
        return 0
    now = datetime.now(timezone.utc).isoformat()

    def _sqlite_bool(value: Any) -> Optional[int]:
        if value is True:
            return 1
        if value is False:
            return 0
        return None

    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        for p in prospects:
            params = (
                scan_id,
                user_id,
                p.get("place_id"),
                p.get("business_name"),
                p.get("city"),
                p.get("state"),
                p.get("website"),
                p.get("rating"),
                p.get("user_ratings_total"),
                1 if p.get("has_website") else 0,
                _sqlite_bool(p.get("ssl")),
                _sqlite_bool(p.get("has_contact_form")),
                1 if p.get("has_phone") else 0,
                _sqlite_bool(p.get("has_viewport")),
                _sqlite_bool(p.get("has_schema")),
                float(p.get("rank_key") or 0),
                p.get("rank"),
                p.get("review_position_summary"),
                float((p.get("lead_quality") or {}).get("score")) if isinstance(p.get("lead_quality"), dict) and (p.get("lead_quality") or {}).get("score") is not None else None,
                (p.get("lead_quality") or {}).get("class") if isinstance(p.get("lead_quality"), dict) else None,
                json.dumps((p.get("lead_quality") or {}).get("reasons") or [], default=str) if isinstance(p.get("lead_quality"), dict) else None,
                (p.get("lead_quality") or {}).get("model_version") if isinstance(p.get("lead_quality"), dict) else None,
                (p.get("lead_quality") or {}).get("feature_version") if isinstance(p.get("lead_quality"), dict) else None,
                float((p.get("lead_quality") or {}).get("data_confidence") or 0.0) if isinstance(p.get("lead_quality"), dict) else None,
                json.dumps(p, default=str),
                now,
                now,
            )
            if use_runtime_postgres():
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO territory_prospects
                           (scan_id, user_id, place_id, business_name, city, state, website,
                            rating, user_ratings_total, has_website, ssl, has_contact_form,
                            has_phone, has_viewport, has_schema, rank_key, rank,
                            review_position_summary, lead_quality_score, lead_quality_class,
                            lead_quality_reasons_json, lead_model_version, lead_feature_version,
                            lead_data_confidence, tier1_snapshot_json, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s, %s)
                           ON CONFLICT(scan_id, place_id) DO UPDATE SET
                              business_name = EXCLUDED.business_name,
                              city = EXCLUDED.city,
                              state = EXCLUDED.state,
                              website = EXCLUDED.website,
                              rating = EXCLUDED.rating,
                              user_ratings_total = EXCLUDED.user_ratings_total,
                              has_website = EXCLUDED.has_website,
                              ssl = EXCLUDED.ssl,
                              has_contact_form = EXCLUDED.has_contact_form,
                              has_phone = EXCLUDED.has_phone,
                              has_viewport = EXCLUDED.has_viewport,
                              has_schema = EXCLUDED.has_schema,
                              rank_key = EXCLUDED.rank_key,
                              rank = EXCLUDED.rank,
                              review_position_summary = EXCLUDED.review_position_summary,
                              lead_quality_score = EXCLUDED.lead_quality_score,
                              lead_quality_class = EXCLUDED.lead_quality_class,
                              lead_quality_reasons_json = EXCLUDED.lead_quality_reasons_json,
                              lead_model_version = EXCLUDED.lead_model_version,
                              lead_feature_version = EXCLUDED.lead_feature_version,
                              lead_data_confidence = EXCLUDED.lead_data_confidence,
                              tier1_snapshot_json = EXCLUDED.tier1_snapshot_json,
                              updated_at = EXCLUDED.updated_at""",
                        params,
                    )
            else:
                conn.execute(
                """INSERT INTO territory_prospects
                   (scan_id, user_id, place_id, business_name, city, state, website,
                    rating, user_ratings_total, has_website, ssl, has_contact_form,
                    has_phone, has_viewport, has_schema, rank_key, rank,
                    review_position_summary, lead_quality_score, lead_quality_class,
                    lead_quality_reasons_json, lead_model_version, lead_feature_version,
                    lead_data_confidence, tier1_snapshot_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(scan_id, place_id) DO UPDATE SET
                      business_name = excluded.business_name,
                      city = excluded.city,
                      state = excluded.state,
                      website = excluded.website,
                      rating = excluded.rating,
                      user_ratings_total = excluded.user_ratings_total,
                      has_website = excluded.has_website,
                      ssl = excluded.ssl,
                      has_contact_form = excluded.has_contact_form,
                      has_phone = excluded.has_phone,
                      has_viewport = excluded.has_viewport,
                      has_schema = excluded.has_schema,
                      rank_key = excluded.rank_key,
                      rank = excluded.rank,
                      review_position_summary = excluded.review_position_summary,
                      lead_quality_score = excluded.lead_quality_score,
                      lead_quality_class = excluded.lead_quality_class,
                      lead_quality_reasons_json = excluded.lead_quality_reasons_json,
                      lead_model_version = excluded.lead_model_version,
                      lead_feature_version = excluded.lead_feature_version,
                      lead_data_confidence = excluded.lead_data_confidence,
                      tier1_snapshot_json = excluded.tier1_snapshot_json,
                      updated_at = excluded.updated_at""",
                params,
                )
        conn.commit()
        return len(prospects)
    finally:
        conn.close()


def list_territory_prospects(scan_id: str, user_id: int) -> List[Dict[str, Any]]:
    """Return Tier 1 rows for a scan."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                """SELECT * FROM territory_prospects
                   WHERE scan_id = %s AND user_id = %s
                   ORDER BY rank ASC, rank_key DESC, id ASC""",
                (scan_id, user_id),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
                """SELECT * FROM territory_prospects
                   WHERE scan_id = ? AND user_id = ?
                   ORDER BY rank ASC, rank_key DESC, id ASC""",
                (scan_id, user_id),
            ).fetchall()
        )
        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            if d.get("tier1_snapshot_json") and not isinstance(d.get("tier1_snapshot_json"), dict):
                d["tier1_snapshot"] = json.loads(d["tier1_snapshot_json"])
            elif d.get("tier1_snapshot_json") is not None:
                d["tier1_snapshot"] = d["tier1_snapshot_json"]
            if d.get("lead_quality_reasons_json") and not isinstance(d.get("lead_quality_reasons_json"), list):
                try:
                    d["lead_quality_reasons"] = json.loads(d["lead_quality_reasons_json"])
                except (TypeError, json.JSONDecodeError):
                    d["lead_quality_reasons"] = []
            elif d.get("lead_quality_reasons_json") is not None:
                d["lead_quality_reasons"] = d["lead_quality_reasons_json"]
            out.append(d)
        return out
    finally:
        conn.close()


def get_territory_prospect(prospect_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Return one Tier 1 prospect row by id for user."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                "SELECT * FROM territory_prospects WHERE id = %s AND user_id = %s",
                (prospect_id, user_id),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT * FROM territory_prospects WHERE id = ? AND user_id = ?",
                (prospect_id, user_id),
            ).fetchone()
        )
        if not row:
            return None
        d = dict(row)
        if d.get("tier1_snapshot_json") and not isinstance(d.get("tier1_snapshot_json"), dict):
            d["tier1_snapshot"] = json.loads(d["tier1_snapshot_json"])
        elif d.get("tier1_snapshot_json") is not None:
            d["tier1_snapshot"] = d["tier1_snapshot_json"]
        if d.get("lead_quality_reasons_json") and not isinstance(d.get("lead_quality_reasons_json"), list):
            try:
                d["lead_quality_reasons"] = json.loads(d["lead_quality_reasons_json"])
            except (TypeError, json.JSONDecodeError):
                d["lead_quality_reasons"] = []
        elif d.get("lead_quality_reasons_json") is not None:
            d["lead_quality_reasons"] = d["lead_quality_reasons_json"]
        return d
    finally:
        conn.close()


def set_territory_prospect_ensure_job(prospect_id: int, job_id: Optional[str]) -> None:
    """Set/clear pending ensure-brief job on a prospect."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE territory_prospects SET ensure_job_id = %s, updated_at = %s WHERE id = %s",
                    (job_id, now, prospect_id),
                )
        else:
            conn.execute(
                "UPDATE territory_prospects SET ensure_job_id = ?, updated_at = ? WHERE id = ?",
                (job_id, now, prospect_id),
            )
        conn.commit()
    finally:
        conn.close()


def link_territory_prospect_diagnostic(
    prospect_id: int,
    diagnostic_id: int,
    full_brief_ready: bool = True,
) -> None:
    """Attach a full diagnostic to a Tier 1 prospect."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE territory_prospects
                       SET diagnostic_id = %s, full_brief_ready = %s, ensure_job_id = NULL, updated_at = %s
                       WHERE id = %s""",
                    (diagnostic_id, 1 if full_brief_ready else 0, now, prospect_id),
                )
        else:
            conn.execute(
                """UPDATE territory_prospects
                   SET diagnostic_id = ?, full_brief_ready = ?, ensure_job_id = NULL, updated_at = ?
                   WHERE id = ?""",
                (diagnostic_id, 1 if full_brief_ready else 0, now, prospect_id),
            )
        conn.commit()
    finally:
        conn.close()


def save_ml_feature_snapshot(
    *,
    entity_type: str,
    entity_id: int,
    place_id: str,
    feature_scope: str,
    feature_version: str,
    feature_payload: Dict[str, Any],
    data_confidence: Optional[float] = None,
    signal_completeness_ratio: Optional[float] = None,
) -> int:
    """Persist one feature snapshot for ML scoring."""
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    payload_json = json.dumps(feature_payload, default=str, sort_keys=True)
    feature_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO ml_feature_snapshots
                       (entity_type, entity_id, place_id, feature_scope, feature_version,
                        feature_json, feature_hash, data_confidence, signal_completeness_ratio, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                       ON CONFLICT(entity_type, entity_id, feature_version) DO UPDATE SET
                          place_id = EXCLUDED.place_id,
                          feature_scope = EXCLUDED.feature_scope,
                          feature_json = EXCLUDED.feature_json,
                          feature_hash = EXCLUDED.feature_hash,
                          data_confidence = EXCLUDED.data_confidence,
                          signal_completeness_ratio = EXCLUDED.signal_completeness_ratio,
                          created_at = EXCLUDED.created_at
                       RETURNING id""",
                    (
                        entity_type, entity_id, place_id, feature_scope, feature_version,
                        payload_json, feature_hash, data_confidence, signal_completeness_ratio, now,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"])
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO ml_feature_snapshots
               (entity_type, entity_id, place_id, feature_scope, feature_version,
                feature_json, feature_hash, data_confidence, signal_completeness_ratio, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(entity_type, entity_id, feature_version) DO UPDATE SET
                  place_id = excluded.place_id,
                  feature_scope = excluded.feature_scope,
                  feature_json = excluded.feature_json,
                  feature_hash = excluded.feature_hash,
                  data_confidence = excluded.data_confidence,
                  signal_completeness_ratio = excluded.signal_completeness_ratio,
                  created_at = excluded.created_at""",
            (
                entity_type,
                entity_id,
                place_id,
                feature_scope,
                feature_version,
                payload_json,
                feature_hash,
                data_confidence,
                signal_completeness_ratio,
                now,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def save_ml_prediction(
    *,
    entity_type: str,
    entity_id: int,
    place_id: str,
    model_name: str,
    model_version: str,
    feature_version: str,
    label_version: str,
    score: float,
    score_0_100: float,
    predicted_class: str,
    prob_high_value: Optional[float] = None,
    prob_bad: Optional[float] = None,
    prob_decent: Optional[float] = None,
    prob_good: Optional[float] = None,
    data_confidence: Optional[float] = None,
    reasons: Optional[List[Dict[str, Any]]] = None,
    caveats: Optional[List[Dict[str, Any]]] = None,
    components: Optional[Dict[str, Any]] = None,
    top_features: Optional[Dict[str, Any]] = None,
    calibration_version: Optional[str] = None,
) -> int:
    """Persist one ML prediction row."""
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO ml_predictions
                       (entity_type, entity_id, place_id, model_name, model_version, feature_version,
                        label_version, calibration_version, score, score_0_100, predicted_class,
                        prob_bad, prob_decent, prob_good, prob_high_value, data_confidence,
                        reasons_json, caveats_json, components_json, top_features_json, created_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s)
                       ON CONFLICT(entity_type, entity_id, model_name, model_version) DO UPDATE SET
                          place_id = EXCLUDED.place_id,
                          feature_version = EXCLUDED.feature_version,
                          label_version = EXCLUDED.label_version,
                          calibration_version = EXCLUDED.calibration_version,
                          score = EXCLUDED.score,
                          score_0_100 = EXCLUDED.score_0_100,
                          predicted_class = EXCLUDED.predicted_class,
                          prob_bad = EXCLUDED.prob_bad,
                          prob_decent = EXCLUDED.prob_decent,
                          prob_good = EXCLUDED.prob_good,
                          prob_high_value = EXCLUDED.prob_high_value,
                          data_confidence = EXCLUDED.data_confidence,
                          reasons_json = EXCLUDED.reasons_json,
                          caveats_json = EXCLUDED.caveats_json,
                          components_json = EXCLUDED.components_json,
                          top_features_json = EXCLUDED.top_features_json,
                          created_at = EXCLUDED.created_at
                       RETURNING id""",
                    (
                        entity_type, entity_id, place_id, model_name, model_version, feature_version,
                        label_version, calibration_version, score, score_0_100, predicted_class,
                        prob_bad, prob_decent, prob_good, prob_high_value, data_confidence,
                        json.dumps(reasons or [], default=str),
                        json.dumps(caveats or [], default=str),
                        json.dumps(components or {}, default=str),
                        json.dumps(top_features or {}, default=str),
                        now,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"])
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO ml_predictions
               (entity_type, entity_id, place_id, model_name, model_version, feature_version,
                label_version, calibration_version, score, score_0_100, predicted_class,
                prob_bad, prob_decent, prob_good, prob_high_value, data_confidence,
                reasons_json, caveats_json, components_json, top_features_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(entity_type, entity_id, model_name, model_version) DO UPDATE SET
                  place_id = excluded.place_id,
                  feature_version = excluded.feature_version,
                  label_version = excluded.label_version,
                  calibration_version = excluded.calibration_version,
                  score = excluded.score,
                  score_0_100 = excluded.score_0_100,
                  predicted_class = excluded.predicted_class,
                  prob_bad = excluded.prob_bad,
                  prob_decent = excluded.prob_decent,
                  prob_good = excluded.prob_good,
                  prob_high_value = excluded.prob_high_value,
                  data_confidence = excluded.data_confidence,
                  reasons_json = excluded.reasons_json,
                  caveats_json = excluded.caveats_json,
                  components_json = excluded.components_json,
                  top_features_json = excluded.top_features_json,
                  created_at = excluded.created_at""",
            (
                entity_type,
                entity_id,
                place_id,
                model_name,
                model_version,
                feature_version,
                label_version,
                calibration_version,
                score,
                score_0_100,
                predicted_class,
                prob_bad,
                prob_decent,
                prob_good,
                prob_high_value,
                data_confidence,
                json.dumps(reasons or [], default=str),
                json.dumps(caveats or [], default=str),
                json.dumps(components or {}, default=str),
                json.dumps(top_features or {}, default=str),
                now,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def update_diagnostic_ml_fields(
    diagnostic_id: int,
    *,
    lead_quality_score: Optional[float],
    lead_quality_class: Optional[str],
    lead_model_version: Optional[str],
    lead_feature_version: Optional[str],
    lead_quality_payload: Optional[Dict[str, Any]],
) -> None:
    """Cache latest ML output on diagnostics for hot-path reads."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE diagnostics
                       SET lead_quality_score = %s, lead_quality_class = %s, lead_model_version = %s,
                           lead_feature_version = %s, lead_quality_json = %s::jsonb
                       WHERE id = %s""",
                    (
                        lead_quality_score,
                        lead_quality_class,
                        lead_model_version,
                        lead_feature_version,
                        json.dumps(lead_quality_payload, default=str) if lead_quality_payload else None,
                        diagnostic_id,
                    ),
                )
        else:
            conn.execute(
            """UPDATE diagnostics
               SET lead_quality_score = ?, lead_quality_class = ?, lead_model_version = ?,
                   lead_feature_version = ?, lead_quality_json = ?
               WHERE id = ?""",
            (
                lead_quality_score,
                lead_quality_class,
                lead_model_version,
                lead_feature_version,
                json.dumps(lead_quality_payload, default=str) if lead_quality_payload else None,
                diagnostic_id,
            ),
            )
        conn.commit()
    finally:
        conn.close()


def get_latest_diagnostic_by_place_id(user_id: int, place_id: str) -> Optional[Dict[str, Any]]:
    """Return latest diagnostic row for a place_id/user, if any."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                """SELECT id, place_id, business_name, city, state, created_at
                   FROM diagnostics
                   WHERE user_id = %s AND place_id = %s
                   ORDER BY created_at DESC
                   LIMIT 1""",
                (user_id, place_id),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                """SELECT id, place_id, business_name, city, state, created_at
                   FROM diagnostics
                   WHERE user_id = ? AND place_id = ?
                   ORDER BY created_at DESC
                   LIMIT 1""",
                (user_id, place_id),
            ).fetchone()
        )
        return dict(row) if row else None
    finally:
        conn.close()


def get_territory_contact_for_diagnostic(
    diagnostic_id: int,
    place_id: Optional[str] = None,
) -> Dict[str, Optional[str]]:
    """Return best-effort phone/website for a diagnostic from territory prospect snapshots."""
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT website, tier1_snapshot_json
               FROM territory_prospects
               WHERE diagnostic_id = ?
               ORDER BY updated_at DESC
               LIMIT 1""",
            (diagnostic_id,),
        ).fetchone()
        if not row and place_id:
            row = conn.execute(
                """SELECT website, tier1_snapshot_json
                   FROM territory_prospects
                   WHERE place_id = ?
                   ORDER BY updated_at DESC
                   LIMIT 1""",
                (place_id,),
            ).fetchone()
        if not row:
            return {"phone": None, "website": None}
        snap = json.loads(row["tier1_snapshot_json"]) if row["tier1_snapshot_json"] else {}
        phone = (
            (snap.get("phone") if isinstance(snap, dict) else None)
            or (snap.get("international_phone_number") if isinstance(snap, dict) else None)
        )
        website = row["website"] or (snap.get("website") if isinstance(snap, dict) else None)
        return {
            "phone": str(phone).strip() if phone else None,
            "website": str(website).strip() if website else None,
        }
    finally:
        conn.close()


def get_tier1_cache(place_id: str) -> Optional[Dict[str, Any]]:
    """Return cached Tier1 data for place_id."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                "SELECT place_id, details_json, website_signals_json, updated_at FROM territory_tier1_cache WHERE place_id = %s",
                (place_id,),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT place_id, details_json, website_signals_json, updated_at FROM territory_tier1_cache WHERE place_id = ?",
                (place_id,),
            ).fetchone()
        )
        if not row:
            return None
        return {
            "place_id": row["place_id"],
            "details": row["details_json"] if isinstance(row["details_json"], dict) else (json.loads(row["details_json"]) if row["details_json"] else None),
            "website_signals": row["website_signals_json"] if isinstance(row["website_signals_json"], dict) else (json.loads(row["website_signals_json"]) if row["website_signals_json"] else None),
            "updated_at": row["updated_at"],
        }
    finally:
        conn.close()


def upsert_tier1_cache(
    place_id: str,
    details: Optional[Dict[str, Any]] = None,
    website_signals: Optional[Dict[str, Any]] = None,
) -> None:
    """Insert/update Tier1 cache row."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO territory_tier1_cache (place_id, details_json, website_signals_json, updated_at)
                       VALUES (%s, %s::jsonb, %s::jsonb, %s)
                       ON CONFLICT(place_id) DO UPDATE SET
                         details_json = COALESCE(EXCLUDED.details_json, territory_tier1_cache.details_json),
                         website_signals_json = COALESCE(EXCLUDED.website_signals_json, territory_tier1_cache.website_signals_json),
                         updated_at = EXCLUDED.updated_at""",
                    (
                        place_id,
                        json.dumps(details, default=str) if details is not None else None,
                        json.dumps(website_signals, default=str) if website_signals is not None else None,
                        now,
                    ),
                )
        else:
            conn.execute(
            """INSERT INTO territory_tier1_cache (place_id, details_json, website_signals_json, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(place_id) DO UPDATE SET
                 details_json = COALESCE(excluded.details_json, territory_tier1_cache.details_json),
                 website_signals_json = COALESCE(excluded.website_signals_json, territory_tier1_cache.website_signals_json),
                 updated_at = excluded.updated_at""",
            (
                place_id,
                json.dumps(details, default=str) if details is not None else None,
                json.dumps(website_signals, default=str) if website_signals is not None else None,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_ask_places_cache(cache_key: str) -> Optional[Dict[str, Any]]:
    """Return cached place candidates payload for an Ask query key."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                "SELECT cache_key, data_json, updated_at FROM ask_places_cache WHERE cache_key = %s",
                (cache_key,),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT cache_key, data_json, updated_at FROM ask_places_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        )
        if not row:
            return None
        return {
            "cache_key": row["cache_key"],
            "data": row["data_json"] if isinstance(row["data_json"], dict) else (json.loads(row["data_json"]) if row["data_json"] else {}),
            "updated_at": row["updated_at"],
        }
    finally:
        conn.close()


def upsert_ask_places_cache(cache_key: str, data: Dict[str, Any]) -> None:
    """Store/update ask places cache payload."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO ask_places_cache (cache_key, data_json, updated_at)
                       VALUES (%s, %s::jsonb, %s)
                       ON CONFLICT(cache_key) DO UPDATE SET
                         data_json = EXCLUDED.data_json,
                         updated_at = EXCLUDED.updated_at""",
                    (cache_key, json.dumps(data, default=str), now),
                )
        else:
            conn.execute(
            """INSERT INTO ask_places_cache (cache_key, data_json, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(cache_key) DO UPDATE SET
                 data_json = excluded.data_json,
                 updated_at = excluded.updated_at""",
            (cache_key, json.dumps(data, default=str), now),
        )
        conn.commit()
    finally:
        conn.close()


def get_ask_lightweight_cache(place_id: str, criterion_key: str) -> Optional[Dict[str, Any]]:
    """Return cached lightweight criterion check for a place."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                """SELECT place_id, criterion_key, result_json, updated_at
                   FROM ask_lightweight_cache
                   WHERE place_id = %s AND criterion_key = %s""",
                (place_id, criterion_key),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                """SELECT place_id, criterion_key, result_json, updated_at
                   FROM ask_lightweight_cache
                   WHERE place_id = ? AND criterion_key = ?""",
                (place_id, criterion_key),
            ).fetchone()
        )
        if not row:
            return None
        return {
            "place_id": row["place_id"],
            "criterion_key": row["criterion_key"],
            "result": row["result_json"] if isinstance(row["result_json"], dict) else (json.loads(row["result_json"]) if row["result_json"] else {}),
            "updated_at": row["updated_at"],
        }
    finally:
        conn.close()


def upsert_ask_lightweight_cache(place_id: str, criterion_key: str, result: Dict[str, Any]) -> None:
    """Store/update lightweight criterion result for a place."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO ask_lightweight_cache (place_id, criterion_key, result_json, updated_at)
                       VALUES (%s, %s, %s::jsonb, %s)
                       ON CONFLICT(place_id, criterion_key) DO UPDATE SET
                         result_json = EXCLUDED.result_json,
                         updated_at = EXCLUDED.updated_at""",
                    (place_id, criterion_key, json.dumps(result, default=str), now),
                )
        else:
            conn.execute(
            """INSERT INTO ask_lightweight_cache (place_id, criterion_key, result_json, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(place_id, criterion_key) DO UPDATE SET
                 result_json = excluded.result_json,
                 updated_at = excluded.updated_at""",
            (place_id, criterion_key, json.dumps(result, default=str), now),
        )
        conn.commit()
    finally:
        conn.close()


def insert_qa_signal_checks(rows: List[Dict[str, Any]]) -> List[int]:
    """Insert QA signal checks and return created ids."""
    if not rows:
        return []
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        out: List[int] = []
        for row in rows:
            if use_runtime_postgres():
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO qa_signal_checks
                       (source_type, source_id, place_id, website, criterion_key, deterministic_match,
                        evidence_json, status, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                       RETURNING id""",
                    (
                        str(row.get("source_type") or "ask"),
                        str(row.get("source_id") or ""),
                        row.get("place_id"),
                        row.get("website"),
                        str(row.get("criterion_key") or ""),
                        1 if row.get("deterministic_match") else 0,
                        json.dumps(row.get("evidence") or {}, default=str),
                        "pending",
                        now,
                        now,
                    ),
                )
                out.append(int(cur.fetchone()["id"]))
            else:
                cur = conn.execute(
                """INSERT INTO qa_signal_checks
                   (source_type, source_id, place_id, website, criterion_key, deterministic_match,
                    evidence_json, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(row.get("source_type") or "ask"),
                    str(row.get("source_id") or ""),
                    row.get("place_id"),
                    row.get("website"),
                    str(row.get("criterion_key") or ""),
                    1 if row.get("deterministic_match") else 0,
                    json.dumps(row.get("evidence") or {}, default=str),
                    "pending",
                    now,
                    now,
                ),
                )
                out.append(int(cur.lastrowid))
        conn.commit()
        return out
    finally:
        conn.close()


def get_qa_signal_checks_by_ids(check_ids: List[int]) -> List[Dict[str, Any]]:
    """Fetch QA signal checks by ids."""
    ids = [int(x) for x in (check_ids or []) if int(x) > 0]
    if not ids:
        return []
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            rows = conn.cursor().execute(
                "SELECT * FROM qa_signal_checks WHERE id = ANY(%s) ORDER BY id ASC",
                (ids,),
            ).fetchall()
        else:
            placeholders = ",".join("?" for _ in ids)
            rows = conn.execute(
                f"SELECT * FROM qa_signal_checks WHERE id IN ({placeholders}) ORDER BY id ASC",
                ids,
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            if d.get("evidence_json"):
                d["evidence"] = d["evidence_json"] if isinstance(d["evidence_json"], dict) else json.loads(d["evidence_json"])
            out.append(d)
        return out
    finally:
        conn.close()


def update_qa_signal_check_result(
    check_id: int,
    *,
    status: str,
    ai_verdict: Optional[str] = None,
    ai_confidence: Optional[str] = None,
    ai_reason: Optional[str] = None,
    ai_model: Optional[str] = None,
    error: Optional[str] = None,
) -> None:
    """Update one QA check result row."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE qa_signal_checks
                       SET status = %s, ai_verdict = %s, ai_confidence = %s, ai_reason = %s, ai_model = %s, error = %s, updated_at = %s
                       WHERE id = %s""",
                    (status, ai_verdict, ai_confidence, ai_reason, ai_model, error, now, int(check_id)),
                )
        else:
            conn.execute(
            """UPDATE qa_signal_checks
               SET status = ?, ai_verdict = ?, ai_confidence = ?, ai_reason = ?, ai_model = ?, error = ?, updated_at = ?
               WHERE id = ?""",
            (status, ai_verdict, ai_confidence, ai_reason, ai_model, error, now, int(check_id)),
            )
        conn.commit()
    finally:
        conn.close()


def list_qa_signal_checks(limit: int = 200, source_type: Optional[str] = None) -> List[Dict[str, Any]]:
    """List latest QA signal checks."""
    clamped = max(1, min(int(limit), 1000))
    conn = _get_conn()
    try:
        if source_type:
            rows = conn.execute(
                "SELECT * FROM qa_signal_checks WHERE source_type = ? ORDER BY created_at DESC LIMIT ?",
                (source_type, clamped),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM qa_signal_checks ORDER BY created_at DESC LIMIT ?",
                (clamped,),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            if d.get("evidence_json"):
                d["evidence"] = json.loads(d["evidence_json"])
            out.append(d)
        return out
    finally:
        conn.close()


def summarize_qa_signal_checks(days: int = 30) -> Dict[str, Any]:
    """Return aggregate QA metrics for recent checks."""
    clamped_days = max(1, min(int(days), 365))
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT source_type, deterministic_match, ai_verdict, status
               FROM qa_signal_checks
               WHERE created_at >= datetime('now', ?)""",
            (f"-{clamped_days} days",),
        ).fetchall()
        total = 0
        completed = 0
        disagreements = 0
        by_source: Dict[str, Dict[str, int]] = {}
        for row in rows:
            total += 1
            source = str(row["source_type"] or "unknown")
            bucket = by_source.setdefault(source, {"total": 0, "completed": 0, "disagreements": 0})
            bucket["total"] += 1
            if str(row["status"] or "") != "completed":
                continue
            completed += 1
            bucket["completed"] += 1
            dmatch = bool(int(row["deterministic_match"] or 0))
            verdict = str(row["ai_verdict"] or "").strip().lower()
            ai_match = verdict == "likely_match"
            ai_not = verdict == "likely_not_match"
            disagree = (dmatch and ai_not) or ((not dmatch) and ai_match)
            if disagree:
                disagreements += 1
                bucket["disagreements"] += 1
        disagreement_rate = (float(disagreements) / float(completed)) if completed else 0.0
        return {
            "window_days": clamped_days,
            "total_checks": total,
            "completed_checks": completed,
            "disagreements": disagreements,
            "disagreement_rate": round(disagreement_rate, 4),
            "by_source": by_source,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Prospect lists
# ---------------------------------------------------------------------------

def create_prospect_list(user_id: int, name: str) -> int:
    """Create a named saved list for prospects."""
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    if use_runtime_postgres():
        conn = _get_runtime_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO prospect_lists (user_id, name, created_at) VALUES (%s, %s, %s) RETURNING id",
                    (user_id, name, now),
                )
                row = cur.fetchone()
            conn.commit()
            return int(row["id"])
        finally:
            conn.close()
    conn = _get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO prospect_lists (user_id, name, created_at) VALUES (?, ?, ?)",
            (user_id, name, now),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_prospect_lists(user_id: int) -> List[Dict[str, Any]]:
    """List all prospect lists and member counts."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                """SELECT pl.id, pl.name, pl.created_at, COUNT(plm.id) AS members_count
                   FROM prospect_lists pl
                   LEFT JOIN prospect_list_members plm ON plm.list_id = pl.id
                   WHERE pl.user_id = %s
                   GROUP BY pl.id, pl.name, pl.created_at
                   ORDER BY pl.created_at DESC""",
                (user_id,),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
            """SELECT pl.id, pl.name, pl.created_at, COUNT(plm.id) AS members_count
               FROM prospect_lists pl
               LEFT JOIN prospect_list_members plm ON plm.list_id = pl.id
               WHERE pl.user_id = ?
               GROUP BY pl.id
               ORDER BY pl.created_at DESC""",
            (user_id,),
            ).fetchall()
        )
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_prospect_list(list_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Return one list for owner."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        row = (
            conn.cursor().execute(
                "SELECT id, user_id, name, created_at FROM prospect_lists WHERE id = %s AND user_id = %s",
                (list_id, user_id),
            ).fetchone()
            if use_runtime_postgres()
            else conn.execute(
                "SELECT id, user_id, name, created_at FROM prospect_lists WHERE id = ? AND user_id = ?",
                (list_id, user_id),
            ).fetchone()
        )
        return dict(row) if row else None
    finally:
        conn.close()


def upsert_list_member(
    list_id: int,
    diagnostic_id: int,
    place_id: Optional[str],
    business_name: Optional[str],
    city: Optional[str],
    state: Optional[str],
) -> None:
    """Add or update a list member by place_id uniqueness."""
    now = datetime.now(timezone.utc).isoformat()
    payload = (list_id, diagnostic_id, place_id, business_name, city, state, now)
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO prospect_list_members
                       (list_id, diagnostic_id, place_id, business_name, city, state, added_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT(list_id, place_id) DO UPDATE SET
                         diagnostic_id = EXCLUDED.diagnostic_id,
                         business_name = EXCLUDED.business_name,
                         city = EXCLUDED.city,
                         state = EXCLUDED.state,
                         added_at = EXCLUDED.added_at""",
                    payload,
                )
        else:
            conn.execute(
            """INSERT INTO prospect_list_members
               (list_id, diagnostic_id, place_id, business_name, city, state, added_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(list_id, place_id) DO UPDATE SET
                 diagnostic_id = excluded.diagnostic_id,
                 business_name = excluded.business_name,
                 city = excluded.city,
                 state = excluded.state,
                 added_at = excluded.added_at""",
            payload,
            )
        conn.commit()
    finally:
        conn.close()


def add_list_members(list_id: int, members: List[Dict[str, Any]]) -> int:
    """Bulk upsert members for a list. Returns number of input members."""
    for m in members:
        upsert_list_member(
            list_id=list_id,
            diagnostic_id=int(m["diagnostic_id"]),
            place_id=m.get("place_id"),
            business_name=m.get("business_name"),
            city=m.get("city"),
            state=m.get("state"),
        )
    return len(members)


def list_members_for_list(list_id: int) -> List[Dict[str, Any]]:
    """Return current member rows joined to latest diagnostic payloads."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                """SELECT plm.list_id, plm.diagnostic_id, plm.place_id, plm.business_name, plm.city, plm.state,
                          plm.added_at, d.response_json, d.created_at AS diagnostic_created_at
                   FROM prospect_list_members plm
                   JOIN diagnostics d ON d.id = plm.diagnostic_id
                   WHERE plm.list_id = %s
                   ORDER BY plm.added_at DESC""",
                (list_id,),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
            """SELECT plm.list_id, plm.diagnostic_id, plm.place_id, plm.business_name, plm.city, plm.state,
                      plm.added_at, d.response_json, d.created_at AS diagnostic_created_at
               FROM prospect_list_members plm
               JOIN diagnostics d ON d.id = plm.diagnostic_id
               WHERE plm.list_id = ?
               ORDER BY plm.added_at DESC""",
            (list_id,),
            ).fetchall()
        )
        out = []
        for row in rows:
            d = dict(row)
            if d.get("response_json") and not isinstance(d.get("response_json"), dict):
                d["response"] = json.loads(d["response_json"])
            elif d.get("response_json") is not None:
                d["response"] = d["response_json"]
            out.append(d)
        return out
    finally:
        conn.close()


def remove_list_member(list_id: int, diagnostic_id: int) -> bool:
    """Delete member by diagnostic id within a list."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM prospect_list_members WHERE list_id = %s AND diagnostic_id = %s",
                    (list_id, diagnostic_id),
                )
                removed = cur.rowcount > 0
        else:
            cur = conn.execute(
                "DELETE FROM prospect_list_members WHERE list_id = ? AND diagnostic_id = ?",
                (list_id, diagnostic_id),
            )
            removed = cur.rowcount > 0
        conn.commit()
        return removed
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Prospect outcome actions
# ---------------------------------------------------------------------------

def record_prospect_status(
    diagnostic_id: int,
    status: str,
    note: Optional[str] = None,
) -> None:
    """Store list workflow outcome status as diagnostic_outcomes event."""
    now = datetime.now(timezone.utc).isoformat()
    payload = {"status": status, "note": note}
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO diagnostic_outcomes
                       (diagnostic_id, outcome_type, outcome_json, created_at)
                       VALUES (%s, %s, %s::jsonb, %s)""",
                    (diagnostic_id, "prospect_status", json.dumps(payload, default=str), now),
                )
        else:
            conn.execute(
                """INSERT INTO diagnostic_outcomes
                   (diagnostic_id, outcome_type, outcome_json, created_at)
                   VALUES (?, ?, ?, ?)""",
                (diagnostic_id, "prospect_status", json.dumps(payload, default=str), now),
            )
        conn.commit()
    finally:
        conn.close()


def get_latest_prospect_statuses(diagnostic_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Return latest status payload for each diagnostic id."""
    if not diagnostic_ids:
        return {}
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            rows = conn.cursor().execute(
                """SELECT t1.diagnostic_id, t1.outcome_json, t1.created_at
                   FROM diagnostic_outcomes t1
                   JOIN (
                        SELECT diagnostic_id, MAX(created_at) AS max_created
                        FROM diagnostic_outcomes
                        WHERE outcome_type = 'prospect_status'
                          AND diagnostic_id = ANY(%s)
                        GROUP BY diagnostic_id
                   ) t2
                     ON t1.diagnostic_id = t2.diagnostic_id AND t1.created_at = t2.max_created
                   WHERE t1.outcome_type = 'prospect_status'""",
                (diagnostic_ids,),
            ).fetchall()
        else:
            placeholders = ",".join("?" for _ in diagnostic_ids)
            rows = conn.execute(
                f"""SELECT t1.diagnostic_id, t1.outcome_json, t1.created_at
                    FROM diagnostic_outcomes t1
                    JOIN (
                        SELECT diagnostic_id, MAX(created_at) AS max_created
                        FROM diagnostic_outcomes
                        WHERE outcome_type = 'prospect_status'
                          AND diagnostic_id IN ({placeholders})
                        GROUP BY diagnostic_id
                    ) t2
                      ON t1.diagnostic_id = t2.diagnostic_id AND t1.created_at = t2.max_created
                    WHERE t1.outcome_type = 'prospect_status'""",
                diagnostic_ids,
            ).fetchall()
        out: Dict[int, Dict[str, Any]] = {}
        for row in rows:
            payload = row["outcome_json"] if isinstance(row["outcome_json"], dict) else (json.loads(row["outcome_json"]) if row["outcome_json"] else {})
            out[int(row["diagnostic_id"])] = {
                "status": payload.get("status"),
                "note": payload.get("note"),
                "updated_at": row["created_at"],
            }
        return out
    finally:
        conn.close()


def get_outcome_summary_for_user(user_id: int) -> Dict[str, int]:
    """
    Aggregate latest prospect outcome statuses for a user's diagnostics.
    Returns contacted/closed_won/closed_lost/not_contacted/closed_this_month.
    """
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        total_row = (
            conn.cursor().execute("SELECT COUNT(*) AS cnt FROM diagnostics WHERE user_id = %s", (user_id,)).fetchone()
            if use_runtime_postgres()
            else conn.execute("SELECT COUNT(*) AS cnt FROM diagnostics WHERE user_id = ?", (user_id,)).fetchone()
        )
        total = int(total_row["cnt"]) if total_row else 0

        rows = (
            conn.cursor().execute(
                """SELECT d.id AS diagnostic_id, latest.created_at AS updated_at, latest.outcome_json
                   FROM diagnostics d
                   JOIN (
                        SELECT o1.diagnostic_id, o1.created_at, o1.outcome_json
                        FROM diagnostic_outcomes o1
                        JOIN (
                             SELECT diagnostic_id, MAX(created_at) AS max_created
                             FROM diagnostic_outcomes
                             WHERE outcome_type = 'prospect_status'
                             GROUP BY diagnostic_id
                        ) o2
                        ON o1.diagnostic_id = o2.diagnostic_id AND o1.created_at = o2.max_created
                        WHERE o1.outcome_type = 'prospect_status'
                   ) latest ON latest.diagnostic_id = d.id
                   WHERE d.user_id = %s""",
                (user_id,),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
            """SELECT d.id AS diagnostic_id, latest.created_at AS updated_at, latest.outcome_json
               FROM diagnostics d
               JOIN (
                    SELECT o1.diagnostic_id, o1.created_at, o1.outcome_json
                    FROM diagnostic_outcomes o1
                    JOIN (
                         SELECT diagnostic_id, MAX(created_at) AS max_created
                         FROM diagnostic_outcomes
                         WHERE outcome_type = 'prospect_status'
                         GROUP BY diagnostic_id
                    ) o2
                    ON o1.diagnostic_id = o2.diagnostic_id AND o1.created_at = o2.max_created
                    WHERE o1.outcome_type = 'prospect_status'
               ) latest ON latest.diagnostic_id = d.id
               WHERE d.user_id = ?""",
            (user_id,),
            ).fetchall()
        )

        summary = {
            "contacted": 0,
            "closed_won": 0,
            "closed_lost": 0,
            "not_contacted": total,
            "closed_this_month": 0,
        }
        now = datetime.now(timezone.utc)
        for row in rows:
            payload = row["outcome_json"] if isinstance(row["outcome_json"], dict) else (json.loads(row["outcome_json"]) if row["outcome_json"] else {})
            status = str(payload.get("status") or "").strip().lower()
            if status in ("contacted", "closed_won", "closed_lost"):
                summary[status] += 1
                summary["not_contacted"] = max(0, summary["not_contacted"] - 1)
            if status == "closed_won" and row["updated_at"]:
                try:
                    ts = datetime.fromisoformat(str(row["updated_at"]))
                    if (now - ts).days <= 30:
                        summary["closed_this_month"] += 1
                except (TypeError, ValueError):
                    pass
        return summary
    finally:
        conn.close()


def list_outcomes_for_user(user_id: int, limit: int = 200) -> List[Dict[str, Any]]:
    """Return diagnostics with their latest prospect status for a user."""
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                """SELECT d.id AS diagnostic_id, d.business_name, d.city, d.state, latest.created_at AS updated_at, latest.outcome_json
                   FROM diagnostics d
                   LEFT JOIN (
                        SELECT o1.diagnostic_id, o1.created_at, o1.outcome_json
                        FROM diagnostic_outcomes o1
                        JOIN (
                             SELECT diagnostic_id, MAX(created_at) AS max_created
                             FROM diagnostic_outcomes
                             WHERE outcome_type = 'prospect_status'
                             GROUP BY diagnostic_id
                        ) o2
                        ON o1.diagnostic_id = o2.diagnostic_id AND o1.created_at = o2.max_created
                        WHERE o1.outcome_type = 'prospect_status'
                   ) latest ON latest.diagnostic_id = d.id
                   WHERE d.user_id = %s
                   ORDER BY d.created_at DESC
                   LIMIT %s""",
                (user_id, limit),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
            """SELECT d.id AS diagnostic_id, d.business_name, d.city, d.state, latest.created_at AS updated_at, latest.outcome_json
               FROM diagnostics d
               LEFT JOIN (
                    SELECT o1.diagnostic_id, o1.created_at, o1.outcome_json
                    FROM diagnostic_outcomes o1
                    JOIN (
                         SELECT diagnostic_id, MAX(created_at) AS max_created
                         FROM diagnostic_outcomes
                         WHERE outcome_type = 'prospect_status'
                         GROUP BY diagnostic_id
                    ) o2
                    ON o1.diagnostic_id = o2.diagnostic_id AND o1.created_at = o2.max_created
                    WHERE o1.outcome_type = 'prospect_status'
               ) latest ON latest.diagnostic_id = d.id
               WHERE d.user_id = ?
               ORDER BY d.created_at DESC
               LIMIT ?""",
            (user_id, limit),
            ).fetchall()
        )
        out: List[Dict[str, Any]] = []
        for row in rows:
            payload = row["outcome_json"] if isinstance(row["outcome_json"], dict) else (json.loads(row["outcome_json"]) if row["outcome_json"] else {})
            out.append(
                {
                    "diagnostic_id": int(row["diagnostic_id"]),
                    "business_name": row["business_name"],
                    "city": row["city"],
                    "state": row["state"],
                    "status": payload.get("status"),
                    "note": payload.get("note"),
                    "updated_at": row["updated_at"],
                }
            )
        return out
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Review Snapshots (for real velocity calculation)
# ---------------------------------------------------------------------------

def save_review_snapshot(place_id: str, review_count: int, rating: Optional[float] = None) -> None:
    """Store a point-in-time review count for a place. Called after each diagnostic run."""
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        if use_runtime_postgres():
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO review_snapshots (place_id, review_count, rating, created_at) VALUES (%s, %s, %s, %s)",
                    (place_id, review_count, rating, now),
                )
        else:
            conn.execute(
                "INSERT INTO review_snapshots (place_id, review_count, rating, created_at) VALUES (?, ?, ?, ?)",
                (place_id, review_count, rating, now),
            )
        conn.commit()
    finally:
        conn.close()


def get_review_velocity(place_id: str, window_days: int = 30) -> Optional[Dict]:
    """
    Calculate real review velocity from stored snapshots.

    Compares the current review count to the oldest snapshot within
    the window to compute actual reviews gained over time.

    Returns None if no prior snapshots exist (first run for this place).
    Returns dict with velocity_per_30d, delta, days_between, snapshots_count.
    """
    conn = _get_runtime_conn() if use_runtime_postgres() else _get_conn()
    try:
        rows = (
            conn.cursor().execute(
                """SELECT review_count, rating, created_at
                   FROM review_snapshots
                   WHERE place_id = %s
                   ORDER BY created_at ASC""",
                (place_id,),
            ).fetchall()
            if use_runtime_postgres()
            else conn.execute(
                """SELECT review_count, rating, created_at
                   FROM review_snapshots
                   WHERE place_id = ?
                   ORDER BY created_at ASC""",
                (place_id,),
            ).fetchall()
        )

        if len(rows) < 2:
            return None

        oldest = rows[0]
        newest = rows[-1]

        oldest_dt = datetime.fromisoformat(oldest["created_at"].replace("Z", "+00:00"))
        newest_dt = datetime.fromisoformat(newest["created_at"].replace("Z", "+00:00"))

        days_between = (newest_dt - oldest_dt).total_seconds() / 86400
        if days_between < 1:
            return None

        delta = newest["review_count"] - oldest["review_count"]
        velocity_per_30d = round((delta / days_between) * 30, 1) if days_between > 0 else 0

        return {
            "velocity_per_30d": velocity_per_30d,
            "delta": delta,
            "days_between": round(days_between, 1),
            "snapshots_count": len(rows),
            "oldest_count": oldest["review_count"],
            "newest_count": newest["review_count"],
        }
    finally:
        conn.close()
