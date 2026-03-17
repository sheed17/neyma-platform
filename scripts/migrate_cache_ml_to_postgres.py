"""
Copy operational caches and ML support tables from local SQLite into Postgres runtime store.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline import db

TABLES = [
    "territory_tier1_cache",
    "ask_places_cache",
    "ask_lightweight_cache",
    "qa_signal_checks",
    "review_snapshots",
    "ml_feature_snapshots",
    "ml_predictions",
]

POSTGRES_COLUMNS = {
    "territory_tier1_cache": ("place_id", "details_json", "website_signals_json", "updated_at"),
    "ask_places_cache": ("cache_key", "data_json", "updated_at"),
    "ask_lightweight_cache": ("place_id", "criterion_key", "result_json", "updated_at"),
    "qa_signal_checks": (
        "id", "source_type", "source_id", "place_id", "website", "criterion_key", "deterministic_match",
        "evidence_json", "ai_verdict", "ai_confidence", "ai_reason", "ai_model", "status", "error", "created_at", "updated_at"
    ),
    "review_snapshots": ("id", "place_id", "review_count", "rating", "created_at"),
    "ml_feature_snapshots": (
        "id", "entity_type", "entity_id", "place_id", "feature_scope", "feature_version",
        "feature_json", "feature_hash", "data_confidence", "signal_completeness_ratio", "created_at"
    ),
    "ml_predictions": (
        "id", "entity_type", "entity_id", "place_id", "model_name", "model_version", "feature_version",
        "label_version", "calibration_version", "score", "score_0_100", "predicted_class",
        "prob_bad", "prob_decent", "prob_good", "prob_high_value", "data_confidence",
        "reasons_json", "caveats_json", "components_json", "top_features_json", "created_at"
    ),
}

CONFLICT_TARGET = {
    "territory_tier1_cache": "(place_id)",
    "ask_places_cache": "(cache_key)",
    "ask_lightweight_cache": "(place_id, criterion_key)",
    "qa_signal_checks": "(id)",
    "review_snapshots": "(id)",
    "ml_feature_snapshots": "(entity_type, entity_id, feature_version)",
    "ml_predictions": "(entity_type, entity_id, model_name, model_version)",
}

SEQUENCE_TABLES = ("qa_signal_checks", "review_snapshots", "ml_feature_snapshots", "ml_predictions")


def main() -> None:
    if not db.use_runtime_postgres():
        raise SystemExit("Postgres runtime store is not configured.")

    db.init_db()
    sqlite_conn = db._get_conn()
    pg_conn = db._get_runtime_conn()
    try:
        sqlite_cur = sqlite_conn.cursor()
        pg_cur = pg_conn.cursor()
        for table in TABLES:
            rows = sqlite_cur.execute(f"SELECT * FROM {table}").fetchall()
            if not rows:
                continue
            columns = POSTGRES_COLUMNS[table]
            col_list = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            updates = ", ".join([f"{c}=EXCLUDED.{c}" for c in columns if c not in {"id", "created_at"}])
            sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT {CONFLICT_TARGET[table]} DO UPDATE SET {updates}"
            for row in rows:
                data = dict(row)
                pg_cur.execute(sql, tuple(data.get(col) for col in columns))
            pg_conn.commit()
            print(f"migrated {len(rows)} rows -> {table}", flush=True)
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
