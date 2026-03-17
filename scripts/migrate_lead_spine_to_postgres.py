"""
Copy lead-spine and embedding tables from local SQLite into Postgres runtime store.

Usage:
    python scripts/migrate_lead_spine_to_postgres.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import db


TABLES = [
    "runs",
    "leads",
    "lead_signals",
    "context_dimensions",
    "decisions",
    "lead_embeddings_v2",
    "lead_docs_v1",
    "lead_intel_v1",
]

POSTGRES_COLUMNS = {
    "runs": ("id", "created_at", "config", "leads_count", "status", "run_stats"),
    "leads": (
        "id", "run_id", "place_id", "name", "address", "latitude", "longitude", "raw_place_json",
        "dentist_profile_v1_json", "llm_reasoning_layer_json", "sales_intervention_intelligence_json",
        "objective_decision_layer_json", "created_at"
    ),
    "lead_signals": ("id", "lead_id", "signals_json", "created_at"),
    "context_dimensions": (
        "id", "lead_id", "dimensions_json", "reasoning_summary", "priority_suggestion",
        "primary_themes_json", "outreach_angles_json", "overall_confidence", "reasoning_source",
        "no_opportunity", "no_opportunity_reason", "priority_derivation", "validation_warnings", "created_at"
    ),
    "decisions": (
        "id", "lead_id", "agency_type", "signals_snapshot", "verdict", "confidence",
        "reasoning", "primary_risks", "what_would_change", "prompt_version", "created_at"
    ),
    "lead_embeddings_v2": ("lead_id", "embedding_json", "text_snapshot", "embedding_version", "embedding_type", "created_at"),
    "lead_docs_v1": ("id", "lead_id", "doc_type", "content_text", "metadata_json", "embedding_version", "embedding_type", "created_at"),
    "lead_intel_v1": (
        "id", "lead_id", "vertical", "primary_constraint", "primary_leverage", "contact_priority",
        "outreach_angle", "confidence", "risks_json", "evidence_json", "created_at"
    ),
}

CONFLICT_TARGET = {
    "runs": "(id)",
    "leads": "(id)",
    "lead_signals": "(lead_id)",
    "context_dimensions": "(lead_id)",
    "decisions": "(id)",
    "lead_embeddings_v2": "(lead_id, embedding_version, embedding_type)",
    "lead_docs_v1": "(id)",
    "lead_intel_v1": "(id)",
}

SEQUENCE_TABLES = ("leads", "lead_signals", "context_dimensions", "decisions", "lead_docs_v1", "lead_intel_v1")


def main() -> None:
    if not db.use_runtime_postgres():
        raise SystemExit("Postgres runtime store is not configured.")

    db.init_db()
    sqlite_conn = db._get_conn()
    pg_conn = db._get_runtime_conn()
    try:
        sqlite_cur = sqlite_conn.cursor()
        pg_cur = pg_conn.cursor()

        existing_runs = {row["id"] for row in sqlite_cur.execute("SELECT id FROM runs").fetchall()}
        existing_leads = {row["id"] for row in sqlite_cur.execute("SELECT id FROM leads").fetchall()}

        for table in TABLES:
            rows = sqlite_cur.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                continue
            columns = POSTGRES_COLUMNS[table]
            col_list = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            updates = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in {"id", "created_at"}])
            sql = (
                f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT {CONFLICT_TARGET[table]} DO UPDATE SET {updates}"
            )

            migrated = 0
            for row in rows:
                data = dict(row)
                if table == "leads" and data.get("run_id") not in existing_runs:
                    continue
                if table in {"lead_signals", "context_dimensions", "decisions", "lead_embeddings_v2", "lead_docs_v1", "lead_intel_v1"} and data.get("lead_id") not in existing_leads:
                    continue
                values = tuple(data.get(col) for col in columns)
                pg_cur.execute(sql, values)
                migrated += 1
            pg_conn.commit()
            print(f"migrated {migrated} rows -> {table}", flush=True)

        for table in SEQUENCE_TABLES:
            pg_cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE((SELECT MAX(id) FROM {table}), 1), true)"
            )
        pg_conn.commit()
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
