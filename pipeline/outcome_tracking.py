"""
Outcome tracking and model calibration.

Tracks whether intervention recommendations produced measurable results.
Over time, uses this data to:
- Calculate accuracy rates for each prediction type
- Adjust confidence scores based on historical performance
- Identify which signals are most predictive of success

Outcome flow:
  1. User runs diagnostic → predictions stored
  2. User implements recommendations
  3. User records outcome (via API or re-run)
  4. System compares prediction vs outcome
  5. Calibration adjusts future predictions
"""

import json
import logging
from typing import Dict, Optional, List, Any
from datetime import datetime, timezone

from pipeline.db import _get_conn, init_db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prediction Storage
# ---------------------------------------------------------------------------

def save_diagnostic_predictions(
    diagnostic_id: int,
    place_id: str,
    predictions: Dict[str, Any],
) -> None:
    """
    Store the predictions made during a diagnostic for later comparison.

    predictions dict should include:
        - revenue_band (e.g. {"lower": 460000, "upper": 1000000})
        - revenue_upside (e.g. {"lower": 69000, "upper": 207000})
        - constraint (e.g. "Trust")
        - missing_services (list of strings)
        - has_booking (bool)
        - has_schema (bool)
        - runs_google_ads (bool)
        - review_count (int)
        - review_velocity_30d (float)
        - traffic_estimate (dict)
    """
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO diagnostic_predictions
               (diagnostic_id, place_id, predictions_json, created_at)
               VALUES (?, ?, ?, ?)""",
            (diagnostic_id, place_id, json.dumps(predictions, default=str), now),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("Failed to save predictions for diagnostic %d: %s", diagnostic_id, exc)
    finally:
        conn.close()


def record_outcome(
    diagnostic_id: int,
    outcome_type: str,
    outcome_data: Dict[str, Any],
) -> None:
    """
    Record a real-world outcome for a diagnostic prediction.

    outcome_type: one of
        - "re_run" (automatic, from re-running the diagnostic)
        - "user_reported" (manual, from user input)
        - "revenue_reported" (user reports actual revenue)
        - "conversion_reported" (user reports conversion metrics)

    outcome_data: dict with outcome-specific fields, e.g.:
        For re_run: new_review_count, new_services_detected, new_signals
        For user_reported: implemented_steps, result_description, revenue_change
        For revenue_reported: actual_revenue, actual_new_patients
    """
    init_db()
    now = datetime.now(timezone.utc).isoformat()
    conn = _get_conn()
    try:
        conn.execute(
            """INSERT INTO diagnostic_outcomes
               (diagnostic_id, outcome_type, outcome_json, created_at)
               VALUES (?, ?, ?, ?)""",
            (diagnostic_id, outcome_type, json.dumps(outcome_data, default=str), now),
        )
        conn.commit()
    except Exception as exc:
        logger.warning("Failed to record outcome for diagnostic %d: %s", diagnostic_id, exc)
    finally:
        conn.close()


def auto_record_rerun_outcome(
    original_diagnostic_id: int,
    place_id: str,
    new_diagnostic_data: Dict[str, Any],
) -> Optional[Dict]:
    """
    Automatically compare a re-run diagnostic against the original predictions
    and record the deltas as an outcome.

    Returns the comparison result or None if no prior predictions exist.
    """
    conn = _get_conn()
    try:
        row = conn.execute(
            "SELECT predictions_json FROM diagnostic_predictions WHERE diagnostic_id = ?",
            (original_diagnostic_id,),
        ).fetchone()
        if not row:
            return None
        original = json.loads(row["predictions_json"])
    finally:
        conn.close()

    comparison = _compare_predictions_to_actuals(original, new_diagnostic_data)

    record_outcome(original_diagnostic_id, "re_run", {
        "comparison": comparison,
        "new_diagnostic": {k: v for k, v in new_diagnostic_data.items() if k in (
            "review_count", "review_velocity_30d", "detected_services",
            "missing_services", "has_booking", "has_schema", "runs_google_ads",
        )},
    })

    return comparison


def _compare_predictions_to_actuals(
    original: Dict[str, Any],
    current: Dict[str, Any],
) -> Dict[str, Any]:
    """Compare original predictions to current state and compute accuracy metrics."""
    comparison = {"metrics": {}}

    orig_reviews = original.get("review_count", 0)
    curr_reviews = current.get("review_count", 0)
    if orig_reviews and curr_reviews:
        comparison["metrics"]["review_delta"] = curr_reviews - orig_reviews

    orig_missing = set(s.lower() for s in (original.get("missing_services") or []))
    curr_missing = set(s.lower() for s in (current.get("missing_services") or []))
    if orig_missing:
        fixed_services = orig_missing - curr_missing
        comparison["metrics"]["services_fixed"] = list(fixed_services)
        comparison["metrics"]["services_fix_rate"] = len(fixed_services) / len(orig_missing) if orig_missing else 0

    for signal in ["has_booking", "has_ssl", "runs_google_ads"]:
        orig_val = original.get(signal)
        curr_val = current.get(signal)
        if orig_val is not None and curr_val is not None:
            if orig_val is False and curr_val is True:
                comparison["metrics"][f"{signal}_added"] = True

    return comparison


# ---------------------------------------------------------------------------
# Calibration Metrics
# ---------------------------------------------------------------------------

def get_calibration_stats() -> Dict[str, Any]:
    """
    Compute calibration metrics from all recorded outcomes.

    Returns aggregate accuracy stats:
        - total_diagnostics_with_outcomes
        - avg_review_growth_rate
        - service_fix_rate
        - booking_adoption_rate
        - schema_adoption_rate
        - prediction_accuracy_by_type
    """
    init_db()
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT outcome_json, outcome_type FROM diagnostic_outcomes ORDER BY created_at DESC LIMIT 1000"
        ).fetchall()
    except Exception:
        return {"total_outcomes": 0, "insufficient_data": True}
    finally:
        conn.close()

    if len(rows) < 5:
        return {"total_outcomes": len(rows), "insufficient_data": True}

    review_deltas = []
    service_fix_rates = []
    booking_added = 0
    schema_added = 0
    total_rerun = 0

    for row in rows:
        try:
            data = json.loads(row["outcome_json"])
        except (json.JSONDecodeError, TypeError):
            continue

        if row["outcome_type"] == "re_run":
            total_rerun += 1
            metrics = data.get("comparison", {}).get("metrics", {})

            rd = metrics.get("review_delta")
            if rd is not None:
                review_deltas.append(rd)

            sfr = metrics.get("services_fix_rate")
            if sfr is not None:
                service_fix_rates.append(sfr)

            if metrics.get("has_booking_added"):
                booking_added += 1

    return {
        "total_outcomes": len(rows),
        "total_rerun_outcomes": total_rerun,
        "insufficient_data": total_rerun < 5,
        "avg_review_delta": round(sum(review_deltas) / len(review_deltas), 1) if review_deltas else None,
        "avg_service_fix_rate": round(sum(service_fix_rates) / len(service_fix_rates), 2) if service_fix_rates else None,
        "booking_adoption_count": booking_added,
    }


# ---------------------------------------------------------------------------
# DB Schema (added via migration in init_db)
# ---------------------------------------------------------------------------

def ensure_outcome_tables() -> None:
    """Create outcome tracking tables if they don't exist."""
    init_db()
    conn = _get_conn()
    try:
        conn.executescript("""
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

            CREATE INDEX IF NOT EXISTS idx_outcomes_diagnostic ON diagnostic_outcomes(diagnostic_id);
            CREATE INDEX IF NOT EXISTS idx_predictions_place ON diagnostic_predictions(place_id);
        """)
        conn.commit()
    finally:
        conn.close()
