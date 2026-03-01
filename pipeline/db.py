"""
SQLite persistence for Context-First Opportunity Intelligence.

Stores runs, leads, signals, context dimensions, and lead embeddings (Phase 2 RAG).
"""

import os
import sqlite3
import json
import uuid
import logging
from collections import Counter
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Default path; override with OPPORTUNITY_DB_PATH
DEFAULT_DB_DIR = "data"
DEFAULT_DB_NAME = "opportunity_intelligence.db"


def get_db_path() -> str:
    """Return path to SQLite DB file."""
    path = os.getenv("OPPORTUNITY_DB_PATH")
    if path:
        return path
    os.makedirs(DEFAULT_DB_DIR, exist_ok=True)
    return os.path.join(DEFAULT_DB_DIR, DEFAULT_DB_NAME)


def _get_conn() -> sqlite3.Connection:
    """Get connection with row factory for dict-like rows."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Create tables if they do not exist."""
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
                ssl INTEGER DEFAULT 0,
                has_contact_form INTEGER DEFAULT 0,
                has_phone INTEGER DEFAULT 0,
                has_viewport INTEGER DEFAULT 0,
                has_schema INTEGER DEFAULT 0,
                rank_key REAL DEFAULT 0,
                rank INTEGER,
                review_position_summary TEXT,
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


def insert_lead(run_id: str, lead: Dict) -> int:
    """Insert a lead; return lead_id."""
    now = datetime.now(timezone.utc).isoformat()
    raw_json = json.dumps(lead, default=str) if lead.get("_place_details") else None
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO leads (run_id, place_id, name, address, latitude, longitude, raw_place_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                lead.get("place_id", ""),
                lead.get("name"),
                lead.get("address"),
                lead.get("latitude"),
                lead.get("longitude"),
                raw_json,
                now,
            )
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def insert_lead_signals(lead_id: int, signals: Dict) -> None:
    """Store signals JSON for a lead."""
    now = datetime.now(timezone.utc).isoformat()
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
        # Columns may not exist on old DBs
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
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT lead_id, embedding_json, text_snapshot, embedding_version, embedding_type, created_at
               FROM lead_embeddings_v2
               WHERE lead_id = ? AND embedding_version = ? AND embedding_type = ?""",
            (lead_id, embedding_version, embedding_type),
        ).fetchone()
        if not row:
            return None
        return {
            "lead_id": row["lead_id"],
            "embedding_json": row["embedding_json"],
            "embedding": json.loads(row["embedding_json"]) if row["embedding_json"] else [],
            "text_snapshot": row["text_snapshot"],
            "embedding_version": row["embedding_version"],
            "embedding_type": row["embedding_type"],
            "created_at": row["created_at"],
        }
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
                    close_value_usd, service_sold, notes, status, updated_at, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (lead_id, vertical, agency_type, _c, _p, _cl, close_value_usd,
                 service_sold, notes, status or "new", now, now),
            )
        conn.commit()
    finally:
        conn.close()


def get_lead_outcome(lead_id: int) -> Optional[Dict]:
    """Return outcome row for lead or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT lead_id, vertical, agency_type, contacted, proposal_sent, closed,
                      close_value_usd, service_sold, notes, status, updated_at, created_at
               FROM lead_outcomes WHERE lead_id = ?""",
            (lead_id,),
        ).fetchone()
        if not row:
            return None
        return dict(row)
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
    conn = _get_conn()
    try:
        stats_json = json.dumps(run_stats) if run_stats else None
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
    conn = _get_conn()
    try:
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
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM runs WHERE status = 'completed' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        return row["id"] if row else None
    finally:
        conn.close()


def get_run(run_id: str) -> Optional[Dict]:
    """Get run by id."""
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "config": json.loads(row["config"]) if row["config"] else None,
            "leads_count": row["leads_count"],
            "status": row["status"],
            "run_stats": json.loads(row["run_stats"]) if (row.keys() and "run_stats" in row.keys() and row["run_stats"]) else None,
        }
    finally:
        conn.close()


def list_runs(limit: int = 50, status: Optional[str] = None) -> List[Dict]:
    """List runs, newest first. Optionally filter by status (e.g. 'completed', 'running', 'failed')."""
    conn = _get_conn()
    try:
        if status:
            rows = conn.execute(
                "SELECT * FROM runs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "config": json.loads(row["config"]) if row["config"] else None,
                "leads_count": row["leads_count"],
                "status": row["status"],
                "run_stats": json.loads(row["run_stats"]) if (row.keys() and "run_stats" in row.keys() and row["run_stats"]) else None,
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
    conn = _get_conn()
    try:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("input_json"):
            d["input"] = json.loads(d["input_json"])
        if d.get("result_json"):
            d["result"] = json.loads(d["result_json"])
        return d
    finally:
        conn.close()


def update_job_status(job_id: str, status: str, result: Optional[Dict] = None, error: Optional[str] = None) -> None:
    """Update job status, optionally storing result or error."""
    now = datetime.now(timezone.utc).isoformat()
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
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            if d.get("input_json"):
                d["input"] = json.loads(d["input_json"])
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
    conn = _get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO diagnostics (user_id, job_id, place_id, business_name, city,
               state, brief_json, response_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id, job_id, place_id, business_name, city, state,
                json.dumps(brief, default=str) if brief else None,
                json.dumps(response, default=str), now,
            ),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_diagnostics(user_id: int, limit: int = 50, offset: int = 0) -> List[Dict]:
    """Return diagnostics for a user, newest first."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM diagnostics WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (user_id, limit, offset),
        ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            if d.get("response_json"):
                d["response"] = json.loads(d["response_json"])
            if d.get("brief_json"):
                d["brief"] = json.loads(d["brief_json"])
            out.append(d)
        return out
    finally:
        conn.close()


def get_diagnostic(diagnostic_id: int, user_id: int) -> Optional[Dict]:
    """Return a single diagnostic owned by user_id, or None."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM diagnostics WHERE id = ? AND user_id = ?",
            (diagnostic_id, user_id),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("response_json"):
            d["response"] = json.loads(d["response_json"])
        if d.get("brief_json"):
            d["brief"] = json.loads(d["brief_json"])
        return d
    finally:
        conn.close()


def get_diagnostic_any_user(diagnostic_id: int) -> Optional[Dict]:
    """Return a single diagnostic by id, regardless of owner."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM diagnostics WHERE id = ?",
            (diagnostic_id,),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("response_json"):
            d["response"] = json.loads(d["response_json"])
        if d.get("brief_json"):
            d["brief"] = json.loads(d["brief_json"])
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
    conn = _get_conn()
    try:
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
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM brief_share_tokens WHERE token = ?",
            (token,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def delete_diagnostic(diagnostic_id: int, user_id: int) -> bool:
    """Delete a diagnostic owned by user_id. Returns True if deleted."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "DELETE FROM diagnostics WHERE id = ? AND user_id = ?",
            (diagnostic_id, user_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def count_diagnostics(user_id: int) -> int:
    """Return total diagnostics for a user."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM diagnostics WHERE user_id = ?",
            (user_id,),
        ).fetchone()
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
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO territory_scans
               (id, user_id, job_id, scan_type, city, state, vertical, limit_count,
                filters_json, list_id, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
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
            ),
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
    conn = _get_conn()
    try:
        if status in {"completed", "failed"}:
            conn.execute(
                """UPDATE territory_scans
                   SET status = ?, summary_json = ?, error = ?, completed_at = ?
                   WHERE id = ?""",
                (status, json.dumps(summary, default=str) if summary else None, error, now, scan_id),
            )
        else:
            conn.execute(
                """UPDATE territory_scans
                   SET status = ?, summary_json = ?, error = ?
                   WHERE id = ?""",
                (status, json.dumps(summary, default=str) if summary else None, error, scan_id),
            )
        conn.commit()
    finally:
        conn.close()


def get_territory_scan(scan_id: str, user_id: int) -> Optional[Dict[str, Any]]:
    """Return scan metadata row for owner."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM territory_scans WHERE id = ? AND user_id = ?",
            (scan_id, user_id),
        ).fetchone()
        if not row:
            return None
        out = dict(row)
        if out.get("filters_json"):
            out["filters"] = json.loads(out["filters_json"])
        if out.get("summary_json"):
            out["summary"] = json.loads(out["summary_json"])
        return out
    finally:
        conn.close()


def list_territory_scans(user_id: int, limit: int = 20) -> List[Dict[str, Any]]:
    """List recent territory scans for a user with prospect counts."""
    conn = _get_conn()
    try:
        rows = conn.execute(
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
        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            if d.get("summary_json"):
                d["summary"] = json.loads(d["summary_json"])
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
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO territory_scan_diagnostics
               (scan_id, diagnostic_id, place_id, business_name, city, state,
                previous_diagnostic_id, change_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                scan_id,
                diagnostic_id,
                place_id,
                business_name,
                city,
                state,
                previous_diagnostic_id,
                json.dumps(change, default=str) if change else None,
                now,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_scan_diagnostics(scan_id: str) -> List[Dict[str, Any]]:
    """Return diagnostics linked to a scan with parsed diagnostic response."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT tsd.*, d.response_json, d.created_at AS diagnostic_created_at
               FROM territory_scan_diagnostics tsd
               JOIN diagnostics d ON d.id = tsd.diagnostic_id
               WHERE tsd.scan_id = ?
               ORDER BY tsd.id ASC""",
            (scan_id,),
        ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            if d.get("response_json"):
                d["response"] = json.loads(d["response_json"])
            if d.get("change_json"):
                d["change"] = json.loads(d["change_json"])
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
    conn = _get_conn()
    try:
        for p in prospects:
            conn.execute(
                """INSERT INTO territory_prospects
                   (scan_id, user_id, place_id, business_name, city, state, website,
                    rating, user_ratings_total, has_website, ssl, has_contact_form,
                    has_phone, has_viewport, has_schema, rank_key, rank,
                    review_position_summary, tier1_snapshot_json, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                      tier1_snapshot_json = excluded.tier1_snapshot_json,
                      updated_at = excluded.updated_at""",
                (
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
                    1 if p.get("ssl") else 0,
                    1 if p.get("has_contact_form") else 0,
                    1 if p.get("has_phone") else 0,
                    1 if p.get("has_viewport") else 0,
                    1 if p.get("has_schema") else 0,
                    float(p.get("rank_key") or 0),
                    p.get("rank"),
                    p.get("review_position_summary"),
                    json.dumps(p, default=str),
                    now,
                    now,
                ),
            )
        conn.commit()
        return len(prospects)
    finally:
        conn.close()


def list_territory_prospects(scan_id: str, user_id: int) -> List[Dict[str, Any]]:
    """Return Tier 1 rows for a scan."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT * FROM territory_prospects
               WHERE scan_id = ? AND user_id = ?
               ORDER BY rank ASC, rank_key DESC, id ASC""",
            (scan_id, user_id),
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            if d.get("tier1_snapshot_json"):
                d["tier1_snapshot"] = json.loads(d["tier1_snapshot_json"])
            out.append(d)
        return out
    finally:
        conn.close()


def get_territory_prospect(prospect_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Return one Tier 1 prospect row by id for user."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM territory_prospects WHERE id = ? AND user_id = ?",
            (prospect_id, user_id),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("tier1_snapshot_json"):
            d["tier1_snapshot"] = json.loads(d["tier1_snapshot_json"])
        return d
    finally:
        conn.close()


def set_territory_prospect_ensure_job(prospect_id: int, job_id: Optional[str]) -> None:
    """Set/clear pending ensure-brief job on a prospect."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
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
    conn = _get_conn()
    try:
        conn.execute(
            """UPDATE territory_prospects
               SET diagnostic_id = ?, full_brief_ready = ?, ensure_job_id = NULL, updated_at = ?
               WHERE id = ?""",
            (diagnostic_id, 1 if full_brief_ready else 0, now, prospect_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_latest_diagnostic_by_place_id(user_id: int, place_id: str) -> Optional[Dict[str, Any]]:
    """Return latest diagnostic row for a place_id/user, if any."""
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT id, place_id, business_name, city, state, created_at
               FROM diagnostics
               WHERE user_id = ? AND place_id = ?
               ORDER BY created_at DESC
               LIMIT 1""",
            (user_id, place_id),
        ).fetchone()
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
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT place_id, details_json, website_signals_json, updated_at FROM territory_tier1_cache WHERE place_id = ?",
            (place_id,),
        ).fetchone()
        if not row:
            return None
        return {
            "place_id": row["place_id"],
            "details": json.loads(row["details_json"]) if row["details_json"] else None,
            "website_signals": json.loads(row["website_signals_json"]) if row["website_signals_json"] else None,
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
    conn = _get_conn()
    try:
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
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT cache_key, data_json, updated_at FROM ask_places_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            return None
        return {
            "cache_key": row["cache_key"],
            "data": json.loads(row["data_json"]) if row["data_json"] else {},
            "updated_at": row["updated_at"],
        }
    finally:
        conn.close()


def upsert_ask_places_cache(cache_key: str, data: Dict[str, Any]) -> None:
    """Store/update ask places cache payload."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
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
    conn = _get_conn()
    try:
        row = conn.execute(
            """SELECT place_id, criterion_key, result_json, updated_at
               FROM ask_lightweight_cache
               WHERE place_id = ? AND criterion_key = ?""",
            (place_id, criterion_key),
        ).fetchone()
        if not row:
            return None
        return {
            "place_id": row["place_id"],
            "criterion_key": row["criterion_key"],
            "result": json.loads(row["result_json"]) if row["result_json"] else {},
            "updated_at": row["updated_at"],
        }
    finally:
        conn.close()


def upsert_ask_lightweight_cache(place_id: str, criterion_key: str, result: Dict[str, Any]) -> None:
    """Store/update lightweight criterion result for a place."""
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
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


# ---------------------------------------------------------------------------
# Prospect lists
# ---------------------------------------------------------------------------

def create_prospect_list(user_id: int, name: str) -> int:
    """Create a named saved list for prospects."""
    init_db()
    now = datetime.now(timezone.utc).isoformat()
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
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT pl.id, pl.name, pl.created_at, COUNT(plm.id) AS members_count
               FROM prospect_lists pl
               LEFT JOIN prospect_list_members plm ON plm.list_id = pl.id
               WHERE pl.user_id = ?
               GROUP BY pl.id
               ORDER BY pl.created_at DESC""",
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_prospect_list(list_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    """Return one list for owner."""
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT id, user_id, name, created_at FROM prospect_lists WHERE id = ? AND user_id = ?",
            (list_id, user_id),
        ).fetchone()
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
    conn = _get_conn()
    try:
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
            (list_id, diagnostic_id, place_id, business_name, city, state, now),
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
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT plm.list_id, plm.diagnostic_id, plm.place_id, plm.business_name, plm.city, plm.state,
                      plm.added_at, d.response_json, d.created_at AS diagnostic_created_at
               FROM prospect_list_members plm
               JOIN diagnostics d ON d.id = plm.diagnostic_id
               WHERE plm.list_id = ?
               ORDER BY plm.added_at DESC""",
            (list_id,),
        ).fetchall()
        out = []
        for row in rows:
            d = dict(row)
            if d.get("response_json"):
                d["response"] = json.loads(d["response_json"])
            out.append(d)
        return out
    finally:
        conn.close()


def remove_list_member(list_id: int, diagnostic_id: int) -> bool:
    """Delete member by diagnostic id within a list."""
    conn = _get_conn()
    try:
        cur = conn.execute(
            "DELETE FROM prospect_list_members WHERE list_id = ? AND diagnostic_id = ?",
            (list_id, diagnostic_id),
        )
        conn.commit()
        return cur.rowcount > 0
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
    conn = _get_conn()
    try:
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
    placeholders = ",".join("?" for _ in diagnostic_ids)
    conn = _get_conn()
    try:
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
            payload = json.loads(row["outcome_json"]) if row["outcome_json"] else {}
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
    conn = _get_conn()
    try:
        total_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM diagnostics WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        total = int(total_row["cnt"]) if total_row else 0

        rows = conn.execute(
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

        summary = {
            "contacted": 0,
            "closed_won": 0,
            "closed_lost": 0,
            "not_contacted": total,
            "closed_this_month": 0,
        }
        now = datetime.now(timezone.utc)
        for row in rows:
            payload = json.loads(row["outcome_json"]) if row["outcome_json"] else {}
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
    conn = _get_conn()
    try:
        rows = conn.execute(
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
        out: List[Dict[str, Any]] = []
        for row in rows:
            payload = json.loads(row["outcome_json"]) if row["outcome_json"] else {}
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
    conn = _get_conn()
    try:
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
    conn = _get_conn()
    try:
        rows = conn.execute(
            """SELECT review_count, rating, created_at
               FROM review_snapshots
               WHERE place_id = ?
               ORDER BY created_at ASC""",
            (place_id,),
        ).fetchall()

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
