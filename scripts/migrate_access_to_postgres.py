"""
Copy access/workspace/usage tables from local SQLite to Postgres.

Usage:
    python scripts/migrate_access_to_postgres.py

Requires one of:
    ACCESS_DATABASE_URL
    SUPABASE_DB_URL
    DATABASE_URL
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import db


TABLES = [
    "workspaces",
    "users",
    "workspace_members",
    "guest_sessions",
    "access_entitlements",
    "usage_counters",
    "usage_events",
]

SEQUENCE_TABLES = ("workspaces", "users", "usage_events")

POSTGRES_COLUMNS = {
    "workspaces": ("id", "name", "plan_tier", "seat_limit", "created_at", "updated_at"),
    "users": (
        "id",
        "email",
        "name",
        "plan_tier",
        "workspace_id",
        "seat_role",
        "seat_status",
        "is_guest",
        "created_at",
        "updated_at",
    ),
    "workspace_members": ("workspace_id", "user_id", "role", "status", "created_at", "updated_at"),
    "guest_sessions": ("id", "user_id", "created_at", "updated_at"),
    "access_entitlements": ("scope_type", "scope_id", "plan_tier", "seat_limit", "created_at", "updated_at"),
    "usage_counters": (
        "subject_type",
        "subject_id",
        "period_key",
        "territory_scans_used",
        "diagnostics_used",
        "ask_queries_used",
        "updated_at",
    ),
    "usage_events": ("user_id", "workspace_id", "guest_session_id", "feature_key", "period_key", "event_json", "created_at"),
}


def normalize_row(table: str, row) -> dict:
    data = dict(row)
    if table == "workspaces":
        updated_at = data.get("updated_at") or data.get("created_at")
        return {
            "id": data["id"],
            "name": data.get("name"),
            "plan_tier": data.get("plan_tier") or "free",
            "seat_limit": data.get("seat_limit") or 1,
            "created_at": data.get("created_at"),
            "updated_at": updated_at,
        }
    if table == "users":
        updated_at = data.get("updated_at") or data.get("created_at")
        return {
            "id": data["id"],
            "email": data.get("email"),
            "name": data.get("name"),
            "plan_tier": data.get("plan_tier") or ("guest" if int(data.get("is_guest") or 0) else "free"),
            "workspace_id": data.get("workspace_id"),
            "seat_role": data.get("seat_role") or "owner",
            "seat_status": data.get("seat_status") or "active",
            "is_guest": int(data.get("is_guest") or 0),
            "created_at": data.get("created_at"),
            "updated_at": updated_at,
        }
    if table == "workspace_members":
        updated_at = data.get("updated_at") or data.get("created_at")
        return {
            "workspace_id": data["workspace_id"],
            "user_id": data["user_id"],
            "role": data.get("role") or "member",
            "status": data.get("status") or "active",
            "created_at": data.get("created_at"),
            "updated_at": updated_at,
        }
    if table == "guest_sessions":
        updated_at = data.get("updated_at") or data.get("created_at")
        return {
            "id": data["id"],
            "user_id": data.get("user_id"),
            "created_at": data.get("created_at"),
            "updated_at": updated_at,
        }
    if table == "access_entitlements":
        updated_at = data.get("updated_at") or data.get("created_at")
        return {
            "scope_type": data["scope_type"],
            "scope_id": data["scope_id"],
            "plan_tier": data.get("plan_tier") or "free",
            "seat_limit": data.get("seat_limit") or 1,
            "created_at": data.get("created_at"),
            "updated_at": updated_at,
        }
    if table == "usage_counters":
        return {
            "subject_type": data["subject_type"],
            "subject_id": data["subject_id"],
            "period_key": data["period_key"],
            "territory_scans_used": data.get("territory_scans_used") or 0,
            "diagnostics_used": data.get("diagnostics_used") or 0,
            "ask_queries_used": data.get("ask_queries_used") or 0,
            "updated_at": data.get("updated_at"),
        }
    if table == "usage_events":
        return {
            "user_id": data["user_id"],
            "workspace_id": data.get("workspace_id"),
            "guest_session_id": data.get("guest_session_id"),
            "feature_key": data["feature_key"],
            "period_key": data["period_key"],
            "event_json": data.get("event_json") or "{}",
            "created_at": data.get("created_at"),
        }
    return data


def main() -> None:
    if not db.use_access_postgres():
        raise SystemExit("Postgres access store is not configured. Set ACCESS_DATABASE_URL or SUPABASE_DB_URL first.")

    db.init_db()
    sqlite_conn = db._get_conn()
    pg_conn = db._get_access_conn()
    try:
        sqlite_cur = sqlite_conn.cursor()
        pg_cur = pg_conn.cursor()

        for table in TABLES:
            rows = sqlite_cur.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                continue
            normalized_rows = [normalize_row(table, row) for row in rows]
            columns = list(POSTGRES_COLUMNS[table])
            col_list = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            updates = ", ".join(
                [f"{col} = EXCLUDED.{col}" for col in columns if col not in {"id", "created_at"}]
            )
            conflict_target = {
                "workspaces": "(id)",
                "users": "(id)",
                "workspace_members": "(workspace_id, user_id)",
                "guest_sessions": "(id)",
                "access_entitlements": "(scope_type, scope_id)",
                "usage_counters": "(subject_type, subject_id, period_key)",
                "usage_events": "(id)",
            }[table]
            sql = (
                f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
                f"ON CONFLICT {conflict_target} DO UPDATE SET {updates}"
            )
            for row in normalized_rows:
                pg_cur.execute(sql, tuple(row.get(col) for col in columns))
            print(f"migrated {len(rows)} rows -> {table}")

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
