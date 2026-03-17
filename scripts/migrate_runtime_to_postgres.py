"""
Copy production runtime tables from local SQLite into Postgres.

Usage:
    python scripts/migrate_runtime_to_postgres.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import db


TABLES = [
    "jobs",
    "diagnostics",
    "territory_scans",
    "territory_scan_diagnostics",
    "territory_prospects",
    "prospect_lists",
    "prospect_list_members",
    "diagnostic_predictions",
    "diagnostic_outcomes",
    "brief_share_tokens",
]

POSTGRES_COLUMNS = {
    "jobs": ("id", "user_id", "type", "status", "input_json", "result_json", "error", "created_at", "completed_at"),
    "diagnostics": (
        "id", "user_id", "job_id", "place_id", "business_name", "city", "state",
        "lead_quality_score", "lead_quality_class", "lead_model_version", "lead_feature_version",
        "lead_quality_json", "brief_json", "response_json", "created_at"
    ),
    "territory_scans": (
        "id", "user_id", "job_id", "scan_type", "city", "state", "vertical", "limit_count",
        "filters_json", "list_id", "status", "summary_json", "error", "created_at", "completed_at"
    ),
    "territory_scan_diagnostics": (
        "id", "scan_id", "diagnostic_id", "place_id", "business_name", "city", "state",
        "previous_diagnostic_id", "change_json", "created_at"
    ),
    "territory_prospects": (
        "id", "scan_id", "user_id", "place_id", "business_name", "city", "state", "website", "rating",
        "user_ratings_total", "has_website", "ssl", "has_contact_form", "has_phone", "has_viewport",
        "has_schema", "rank_key", "rank", "review_position_summary", "lead_quality_score",
        "lead_quality_class", "lead_quality_reasons_json", "lead_model_version", "lead_feature_version",
        "lead_data_confidence", "tier1_snapshot_json", "diagnostic_id", "full_brief_ready", "ensure_job_id",
        "created_at", "updated_at"
    ),
    "prospect_lists": ("id", "user_id", "name", "created_at"),
    "prospect_list_members": ("id", "list_id", "diagnostic_id", "place_id", "business_name", "city", "state", "added_at"),
    "diagnostic_predictions": ("diagnostic_id", "place_id", "predictions_json", "created_at"),
    "diagnostic_outcomes": ("id", "diagnostic_id", "outcome_type", "outcome_json", "created_at"),
    "brief_share_tokens": ("id", "diagnostic_id", "user_id", "token", "created_at", "expires_at"),
}

CONFLICT_TARGET = {
    "jobs": "(id)",
    "diagnostics": "(id)",
    "territory_scans": "(id)",
    "territory_scan_diagnostics": "(id)",
    "territory_prospects": "(scan_id, place_id)",
    "prospect_lists": "(id)",
    "prospect_list_members": "(list_id, place_id)",
    "diagnostic_predictions": "(diagnostic_id)",
    "diagnostic_outcomes": "(id)",
    "brief_share_tokens": "(token)",
}

SEQUENCE_TABLES = [
    "diagnostics",
    "territory_scan_diagnostics",
    "territory_prospects",
    "prospect_lists",
    "prospect_list_members",
    "diagnostic_outcomes",
    "brief_share_tokens",
]


def main() -> None:
    if not db.use_runtime_postgres():
        raise SystemExit("Postgres runtime store is not configured. Set CORE_DATABASE_URL or ACCESS_DATABASE_URL first.")

    db.init_db()
    sqlite_conn = db._get_conn()
    pg_conn = db._get_runtime_conn()
    try:
        sqlite_cur = sqlite_conn.cursor()
        pg_cur = pg_conn.cursor()

        existing = {
            "jobs": {row["id"] for row in sqlite_cur.execute("SELECT id FROM jobs").fetchall()},
            "diagnostics": {row["id"] for row in sqlite_cur.execute("SELECT id FROM diagnostics").fetchall()},
            "territory_scans": {row["id"] for row in sqlite_cur.execute("SELECT id FROM territory_scans").fetchall()},
            "prospect_lists": {row["id"] for row in sqlite_cur.execute("SELECT id FROM prospect_lists").fetchall()},
        }

        for table in TABLES:
            rows = sqlite_cur.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                continue
            columns = POSTGRES_COLUMNS[table]
            col_list = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            updates = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in columns if col not in {"id", "created_at"}]
            )
            sql = (
                f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT {CONFLICT_TARGET[table]} DO UPDATE SET {updates}"
            )
            migrated = 0
            for row in rows:
                data = dict(row)
                if table == "diagnostics" and data.get("job_id") and data["job_id"] not in existing["jobs"]:
                    data["job_id"] = None
                if table == "territory_scans" and data.get("job_id") and data["job_id"] not in existing["jobs"]:
                    data["job_id"] = None
                if table == "territory_scan_diagnostics":
                    if data.get("scan_id") not in existing["territory_scans"] or data.get("diagnostic_id") not in existing["diagnostics"]:
                        continue
                    if data.get("previous_diagnostic_id") and data["previous_diagnostic_id"] not in existing["diagnostics"]:
                        data["previous_diagnostic_id"] = None
                if table == "territory_prospects" and data.get("diagnostic_id") and data["diagnostic_id"] not in existing["diagnostics"]:
                    data["diagnostic_id"] = None
                if table == "prospect_list_members":
                    if data.get("list_id") not in existing["prospect_lists"] or data.get("diagnostic_id") not in existing["diagnostics"]:
                        continue
                if table in {"diagnostic_predictions", "diagnostic_outcomes", "brief_share_tokens"} and data.get("diagnostic_id") not in existing["diagnostics"]:
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
