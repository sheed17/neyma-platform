#!/usr/bin/env python3
"""Build versioned lead-quality datasets from saved diagnostics."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.ml.feature_builder import build_tier2_feature_vector
from backend.ml.feature_schema import (
    DIAGNOSTIC_FEATURE_COLUMNS,
    FEATURE_VERSION,
    LABEL_VERSION,
    TERRITORY_FEATURE_COLUMNS,
)
from backend.ml.labeler import generate_lead_quality_label

META_COLUMNS = [
    "place_id",
    "diagnostic_id",
    "snapshot_ts",
    "feature_scope",
    "feature_version",
    "label_version",
]
LABEL_COLUMNS = [
    "lead_quality_score_heuristic_v1",
    "lead_quality_class_heuristic_v1",
    "is_high_value_prospect_heuristic_v1",
    "is_priority_prospect_heuristic_v1",
    "benefit_score_v1",
    "buyability_score_v1",
    "guardrails_json",
]


def _db_path() -> str:
    path = os.getenv("OPPORTUNITY_DB_PATH")
    if path:
        return path
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", "opportunity_intelligence.db")


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_registry_tables() -> None:
    conn = _get_conn()
    try:
        conn.executescript(
            """
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
            """
        )
        conn.commit()
    finally:
        conn.close()


def _write_csv(path: str, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def _project_row(
    *,
    place_id: str,
    diagnostic_id: int,
    snapshot_ts: str,
    features: Dict[str, Any],
    feature_columns: List[str],
    feature_scope: str,
) -> Dict[str, Any]:
    labels = generate_lead_quality_label(features)
    row: Dict[str, Any] = {
        "place_id": place_id,
        "diagnostic_id": diagnostic_id,
        "snapshot_ts": snapshot_ts,
        "feature_scope": feature_scope,
        "feature_version": FEATURE_VERSION,
        "label_version": LABEL_VERSION,
    }
    for key in feature_columns:
        value = features.get(key)
        if isinstance(value, (dict, list)):
            row[key] = json.dumps(value, default=str)
        else:
            row[key] = value
    for key, value in labels.items():
        if key == "guardrails":
            row["guardrails_json"] = json.dumps(value, default=str, sort_keys=True)
        else:
            row[key] = value
    return row


def build_dataset_rows(limit: int | None = None, cutoff: str | None = None) -> tuple[list[dict], list[dict]]:
    _ensure_registry_tables()
    conn = _get_conn()
    try:
        sql = "SELECT id, place_id, response_json, created_at FROM diagnostics WHERE response_json IS NOT NULL"
        params: List[Any] = []
        if cutoff:
            sql += " AND created_at <= ?"
            params.append(cutoff)
        sql += " ORDER BY created_at ASC"
        if limit:
            sql += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    tier2_rows: list[dict] = []
    tier1_rows: list[dict] = []
    for row in rows:
        try:
            response = json.loads(row["response_json"])
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(response, dict):
            continue
        features_t2 = build_tier2_feature_vector(response)
        place_id = str(row["place_id"] or response.get("place_id") or f"diagnostic-{row['id']}")
        snapshot_ts = str(row["created_at"])
        diagnostic_id = int(row["id"])
        tier2_rows.append(
            _project_row(
                place_id=place_id,
                diagnostic_id=diagnostic_id,
                snapshot_ts=snapshot_ts,
                features=features_t2,
                feature_columns=DIAGNOSTIC_FEATURE_COLUMNS,
                feature_scope="tier2",
            )
        )
        tier1_rows.append(
            _project_row(
                place_id=place_id,
                diagnostic_id=diagnostic_id,
                snapshot_ts=snapshot_ts,
                features=features_t2,
                feature_columns=TERRITORY_FEATURE_COLUMNS,
                feature_scope="tier1",
            )
        )
    return tier1_rows, tier2_rows


def register_dataset(dataset_version: str, task_name: str, feature_scope: str, row_count: int, source_cutoff_ts: str | None, manifest: Dict[str, Any]) -> None:
    conn = sqlite3.connect(_db_path())
    try:
        conn.execute(
            """INSERT OR REPLACE INTO ml_dataset_registry
               (dataset_version, task_name, feature_scope, feature_version, label_version,
                row_count, source_cutoff_ts, manifest_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                dataset_version,
                task_name,
                feature_scope,
                FEATURE_VERSION,
                LABEL_VERSION,
                row_count,
                source_cutoff_ts,
                json.dumps(manifest, default=str),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Neyma lead-quality datasets.")
    parser.add_argument("--output-dir", default=os.path.join("output", "ml_datasets"), help="Dataset output root")
    parser.add_argument("--limit", type=int, default=None, help="Optional max diagnostics to export")
    parser.add_argument("--cutoff", default=None, help="Optional ISO timestamp cutoff")
    args = parser.parse_args()

    built_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dataset_root = os.path.join(args.output_dir, built_at)
    os.makedirs(dataset_root, exist_ok=True)

    tier1_rows, tier2_rows = build_dataset_rows(limit=args.limit, cutoff=args.cutoff)

    tier1_version = f"territory_lead_quality_v1__{built_at}"
    tier2_version = f"diagnostic_lead_quality_v1__{built_at}"

    tier1_path = os.path.join(dataset_root, "territory_lead_quality_v1.csv")
    tier2_path = os.path.join(dataset_root, "diagnostic_lead_quality_v1.csv")

    tier1_fields = META_COLUMNS + TERRITORY_FEATURE_COLUMNS + LABEL_COLUMNS
    tier2_fields = META_COLUMNS + DIAGNOSTIC_FEATURE_COLUMNS + LABEL_COLUMNS
    tier1_count = _write_csv(tier1_path, tier1_fields, tier1_rows)
    tier2_count = _write_csv(tier2_path, tier2_fields, tier2_rows)

    manifest = {
        "built_at": built_at,
        "db_path": _db_path(),
        "feature_version": FEATURE_VERSION,
        "label_version": LABEL_VERSION,
        "cutoff": args.cutoff,
        "limit": args.limit,
        "territory": {
            "dataset_version": tier1_version,
            "path": tier1_path,
            "rows": tier1_count,
        },
        "diagnostic": {
            "dataset_version": tier2_version,
            "path": tier2_path,
            "rows": tier2_count,
        },
    }
    with open(os.path.join(dataset_root, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    register_dataset(tier1_version, "territory_lead_quality_v1", "tier1", tier1_count, args.cutoff, manifest)
    register_dataset(tier2_version, "diagnostic_lead_quality_v1", "tier2", tier2_count, args.cutoff, manifest)

    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
