#!/usr/bin/env python3
"""Train Neyma lead-quality models from exported CSV datasets."""

from __future__ import annotations

import argparse
import csv
import json
import os
import pickle
import random
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backend.ml.feature_schema import (
    DIAGNOSTIC_FEATURE_COLUMNS,
    FEATURE_VERSION,
    LABEL_VERSION,
    TERRITORY_FEATURE_COLUMNS,
    TRAINING_PRIORITY_MIN_BENEFIT,
    TRAINING_PRIORITY_MIN_BUYABILITY,
    TRAINING_PRIORITY_SCORE_THRESHOLD,
    TRAINING_TARGET_COLUMN,
)


def _db_path() -> str:
    path = os.getenv("OPPORTUNITY_DB_PATH")
    if path:
        return path
    os.makedirs("data", exist_ok=True)
    return os.path.join("data", "opportunity_intelligence.db")


def _require_sklearn() -> None:
    try:
        import sklearn  # noqa: F401
        import numpy  # noqa: F401
    except Exception as exc:  # pragma: no cover - env-specific
        raise SystemExit(
            "Training requires scikit-learn and numpy. Install requirements first: "
            "pip install -r requirements.txt"
        ) from exc


def _load_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _to_float(value: Any) -> float:
    if value in (None, "", "None"):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _split_keys(keys: Sequence[str], rng: random.Random) -> Tuple[List[str], List[str], List[str]]:
    keys = list(keys)
    rng.shuffle(keys)
    n = len(keys)
    if n <= 0:
        return [], [], []
    if n == 1:
        return keys, [], []
    if n == 2:
        return [keys[0]], [keys[1]], []
    train_n = max(1, int(round(n * 0.70)))
    val_n = max(1, int(round(n * 0.15)))
    test_n = n - train_n - val_n
    if test_n <= 0:
        test_n = 1
        if train_n > val_n and train_n > 1:
            train_n -= 1
        elif val_n > 1:
            val_n -= 1
    train_keys = keys[:train_n]
    val_keys = keys[train_n:train_n + val_n]
    test_keys = keys[train_n + val_n:]
    return train_keys, val_keys, test_keys


def _group_split(rows: Sequence[Dict[str, Any]], seed: int, target_column: str) -> Tuple[List[int], List[int], List[int]]:
    groups: Dict[str, List[int]] = {}
    for idx, row in enumerate(rows):
        groups.setdefault(str(row.get("place_id") or f"row-{idx}"), []).append(idx)
    rng = random.Random(seed)
    positive_keys: List[str] = []
    negative_keys: List[str] = []
    for key, idxs in groups.items():
        group_positive = any(int(float(rows[idx].get(target_column) or 0)) == 1 for idx in idxs)
        if group_positive:
            positive_keys.append(key)
        else:
            negative_keys.append(key)

    pos_train, pos_val, pos_test = _split_keys(positive_keys, rng)
    neg_train, neg_val, neg_test = _split_keys(negative_keys, rng)
    train_keys = set(pos_train + neg_train)
    val_keys = set(pos_val + neg_val)
    test_keys = set(pos_test + neg_test)
    train_idx = [idx for key in train_keys for idx in groups[key]]
    val_idx = [idx for key in val_keys for idx in groups[key]]
    test_idx = [idx for key in test_keys for idx in groups[key]]
    return train_idx, val_idx, test_idx


def _matrix(rows: Sequence[Dict[str, Any]], feature_columns: Sequence[str], target_column: str) -> Tuple[List[List[float]], List[int]]:
    X: List[List[float]] = []
    y: List[int] = []
    for row in rows:
        X.append([_to_float(row.get(col)) for col in feature_columns])
        y.append(int(float(row.get(target_column) or 0)))
    return X, y


def _validate_binary_labels(name: str, labels: Sequence[int]) -> None:
    classes = sorted(set(int(x) for x in labels))
    if len(classes) < 2:
        raise SystemExit(
            f"{name} split has insufficient class diversity for training/evaluation. "
            f"Observed classes: {classes}. Build a larger dataset or adjust the labeler thresholds."
        )


def _precision_at_k(labels: Sequence[int], probs: Sequence[float], k: int) -> float:
    pairs = sorted(zip(probs, labels), key=lambda x: x[0], reverse=True)[:k]
    if not pairs:
        return 0.0
    return sum(label for _, label in pairs) / len(pairs)


def _evaluate(model, X: Sequence[Sequence[float]], y: Sequence[int]) -> Dict[str, float]:
    from sklearn.metrics import accuracy_score, average_precision_score, f1_score, precision_score, recall_score

    probs = model.predict_proba(X)[:, 1]
    preds = [1 if p >= 0.5 else 0 for p in probs]
    return {
        "accuracy": float(accuracy_score(y, preds)),
        "precision": float(precision_score(y, preds, zero_division=0)),
        "recall": float(recall_score(y, preds, zero_division=0)),
        "f1": float(f1_score(y, preds, zero_division=0)),
        "average_precision": float(average_precision_score(y, probs)) if len(set(y)) > 1 else 0.0,
        "precision_at_10": float(_precision_at_k(y, probs, 10)),
        "precision_at_20": float(_precision_at_k(y, probs, 20)),
    }


def _train_candidates(X_train, y_train):
    from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
    from sklearn.linear_model import LogisticRegression

    models = {
        "logistic_regression": LogisticRegression(max_iter=1000, class_weight="balanced"),
        "random_forest": RandomForestClassifier(n_estimators=300, min_samples_leaf=3, class_weight="balanced", random_state=42),
        "hist_gradient_boosting": HistGradientBoostingClassifier(max_depth=6, learning_rate=0.05, max_iter=250, random_state=42),
    }
    trained = {}
    for name, model in models.items():
        model.fit(X_train, y_train)
        trained[name] = model
    return trained


def _artifact_root() -> str:
    return os.getenv("NEYMA_ML_ARTIFACT_DIR", os.path.join("data", "ml_artifacts"))


def _ensure_training_registry_table() -> None:
    conn = sqlite3.connect(_db_path())
    try:
        conn.executescript(
            """
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
            """
        )
        conn.commit()
    finally:
        conn.close()


def _persist_training_run(task_name: str, model_name: str, model_version: str, dataset_version: str, metrics: Dict[str, Any], artifact_path: str) -> None:
    _ensure_training_registry_table()
    conn = sqlite3.connect(_db_path())
    try:
        conn.execute(
            """INSERT OR REPLACE INTO ml_training_runs
               (run_id, task_name, model_name, model_version, dataset_version, feature_version,
                label_version, params_json, metrics_json, artifact_path, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                f"{task_name}__{model_version}",
                task_name,
                model_name,
                model_version,
                dataset_version,
                FEATURE_VERSION,
                LABEL_VERSION,
                json.dumps({"selected_model": model_name}, default=str),
                json.dumps(metrics, default=str),
                artifact_path,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    _require_sklearn()

    parser = argparse.ArgumentParser(description="Train Neyma lead-quality model.")
    parser.add_argument("--dataset-csv", required=True)
    parser.add_argument("--task-name", required=True, choices=["territory_lead_quality", "diagnostic_lead_quality"])
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--model-version", default="v1.0.0")
    parser.add_argument("--target-column", default=TRAINING_TARGET_COLUMN)
    parser.add_argument("--promote-latest", action="store_true", help="Copy this trained artifact into the runtime latest/ slot")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rows = _load_csv(args.dataset_csv)
    feature_columns = TERRITORY_FEATURE_COLUMNS if args.task_name == "territory_lead_quality" else DIAGNOSTIC_FEATURE_COLUMNS
    target_column = args.target_column

    train_idx, val_idx, test_idx = _group_split(rows, args.seed, target_column)
    train_rows = [rows[i] for i in train_idx]
    val_rows = [rows[i] for i in val_idx]
    test_rows = [rows[i] for i in test_idx]

    X_train, y_train = _matrix(train_rows, feature_columns, target_column)
    X_val, y_val = _matrix(val_rows, feature_columns, target_column)
    X_test, y_test = _matrix(test_rows, feature_columns, target_column)

    _validate_binary_labels("train", y_train)
    _validate_binary_labels("validation", y_val)
    _validate_binary_labels("test", y_test)

    trained = _train_candidates(X_train, y_train)
    val_metrics = {name: _evaluate(model, X_val, y_val) for name, model in trained.items()}
    best_name = max(val_metrics, key=lambda name: (val_metrics[name]["average_precision"], val_metrics[name]["precision_at_20"]))
    best_model = trained[best_name]

    test_metrics = _evaluate(best_model, X_test, y_test)
    artifact_root = _artifact_root()
    version_dir = os.path.join(artifact_root, args.task_name, args.model_version)
    latest_dir = os.path.join(artifact_root, args.task_name, "latest")
    os.makedirs(version_dir, exist_ok=True)

    metadata = {
        "task_name": args.task_name,
        "dataset_version": args.dataset_version,
        "model_version": args.model_version,
        "feature_version": FEATURE_VERSION,
        "label_version": LABEL_VERSION,
        "target_column": target_column,
        "target_definition": {
            "score_threshold": TRAINING_PRIORITY_SCORE_THRESHOLD,
            "min_benefit_score": TRAINING_PRIORITY_MIN_BENEFIT,
            "min_buyability_score": TRAINING_PRIORITY_MIN_BUYABILITY,
        } if target_column == TRAINING_TARGET_COLUMN else {"target_column": target_column},
        "feature_columns": feature_columns,
        "best_model_family": best_name,
        "validation_metrics": val_metrics,
        "test_metrics": test_metrics,
        "class_balance": {
            "train_positive_rate": round(sum(y_train) / max(len(y_train), 1), 6),
            "val_positive_rate": round(sum(y_val) / max(len(y_val), 1), 6),
            "test_positive_rate": round(sum(y_test) / max(len(y_test), 1), 6),
            "train_positive_count": int(sum(y_train)),
            "val_positive_count": int(sum(y_val)),
            "test_positive_count": int(sum(y_test)),
        },
        "built_at": datetime.now(timezone.utc).isoformat(),
        "promoted_to_latest": bool(args.promote_latest),
    }

    for out_dir in (version_dir,):
        with open(os.path.join(out_dir, "model.pkl"), "wb") as fh:
            pickle.dump(best_model, fh)
        with open(os.path.join(out_dir, "metadata.json"), "w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2)
        with open(os.path.join(out_dir, "feature_columns.json"), "w", encoding="utf-8") as fh:
            json.dump(feature_columns, fh, indent=2)

    if args.promote_latest:
        os.makedirs(latest_dir, exist_ok=True)
        if os.path.realpath(version_dir) != os.path.realpath(latest_dir):
            for filename in ("model.pkl", "metadata.json", "feature_columns.json"):
                shutil.copy2(os.path.join(version_dir, filename), os.path.join(latest_dir, filename))

    _persist_training_run(args.task_name, best_name, args.model_version, args.dataset_version, test_metrics, version_dir)

    print(json.dumps({
        "task_name": args.task_name,
        "best_model": best_name,
        "dataset_version": args.dataset_version,
        "model_version": args.model_version,
        "test_metrics": test_metrics,
        "artifact_dir": version_dir,
        "promoted_to_latest": bool(args.promote_latest),
    }, indent=2))


if __name__ == "__main__":
    main()
