"""Runtime scoring for Neyma lead-quality models."""

from __future__ import annotations

import json
import os
import pickle
from functools import lru_cache
from typing import Any, Dict, Mapping, Optional

from .feature_builder import build_tier1_feature_vector, build_tier2_feature_vector
from .feature_schema import (
    DIAGNOSTIC_FEATURE_COLUMNS,
    DIAGNOSTIC_MODEL_NAME,
    FEATURE_VERSION,
    HEURISTIC_MODEL_VERSION,
    LABEL_VERSION,
    TERRITORY_FEATURE_COLUMNS,
    TERRITORY_MODEL_NAME,
)
from .labeler import generate_lead_quality_label
from .reason_codes import build_reason_payload


def _artifact_root() -> str:
    return os.getenv("NEYMA_ML_ARTIFACT_DIR", os.path.join("data", "ml_artifacts"))


@lru_cache(maxsize=8)
def _load_bundle(model_name: str) -> Optional[Dict[str, Any]]:
    model_dir = os.path.join(_artifact_root(), model_name, "latest")
    metadata_path = os.path.join(model_dir, "metadata.json")
    model_path = os.path.join(model_dir, "model.pkl")
    if not (os.path.exists(metadata_path) and os.path.exists(model_path)):
        return None
    try:
        with open(metadata_path, "r", encoding="utf-8") as fh:
            metadata = json.load(fh)
        with open(model_path, "rb") as fh:
            model = pickle.load(fh)
        return {"metadata": metadata, "model": model}
    except Exception:
        return None


def _predict_from_bundle(bundle: Dict[str, Any], features: Mapping[str, Any], feature_columns: list[str]) -> Optional[float]:
    model = bundle.get("model")
    try:
        row = [[float(features.get(col, 0.0) or 0.0) for col in feature_columns]]
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(row)
            if proba is not None and len(proba) > 0:
                row0 = list(proba[0])
                if len(row0) >= 2:
                    return float(row0[-1])
        if hasattr(model, "predict"):
            pred = model.predict(row)
            if pred is not None and len(pred) > 0:
                return float(pred[0])
    except Exception:
        return None
    return None


def _decision_threshold(bundle: Optional[Dict[str, Any]]) -> float:
    if not bundle:
        return 0.5
    try:
        threshold = float((bundle.get("metadata") or {}).get("calibration", {}).get("decision_threshold") or 0.5)
    except Exception:
        return 0.5
    return max(0.0, min(1.0, threshold))


def _class_from_score(score: float, *, decision_threshold: float) -> str:
    if score >= decision_threshold:
        return "good"
    if score >= max(0.4, decision_threshold * 0.7):
        return "decent"
    return "bad"


def _components_from_label(label_data: Mapping[str, Any]) -> Dict[str, float]:
    return {
        "benefit_score": round(float(label_data.get("benefit_score_v1") or 0.0), 6),
        "buyability_score": round(float(label_data.get("buyability_score_v1") or 0.0), 6),
    }


def _score_common(features: Mapping[str, Any], *, model_name: str, feature_scope: str, feature_columns: list[str]) -> Dict[str, Any]:
    heuristic = generate_lead_quality_label(features)
    reasons, caveats = build_reason_payload(features, heuristic, feature_scope=feature_scope)

    bundle = _load_bundle(model_name)
    decision_threshold = _decision_threshold(bundle)
    if bundle is not None:
        score = _predict_from_bundle(bundle, features, feature_columns)
    else:
        score = None

    if score is None:
        score = float(heuristic["lead_quality_score_heuristic_v1"])
        resolved_class = str(heuristic["lead_quality_class_heuristic_v1"])
        model_version = HEURISTIC_MODEL_VERSION
        decision_threshold = 0.5
    else:
        score = max(0.0, min(1.0, score))
        resolved_class = _class_from_score(score, decision_threshold=decision_threshold)
        model_version = str(bundle.get("metadata", {}).get("model_version") or "artifact_v1")

    return {
        "class": resolved_class,
        "score": round(score * 100.0, 2),
        "prob_high_value": round(score, 6),
        "decision_threshold": round(decision_threshold, 6),
        "is_priority_prospect_predicted": bool(score >= decision_threshold),
        "data_confidence": round(float(features.get("data_confidence") or 0.0), 6),
        "feature_scope": feature_scope,
        "model_name": model_name,
        "model_version": model_version,
        "feature_version": FEATURE_VERSION,
        "label_version": LABEL_VERSION,
        "components": _components_from_label(heuristic),
        "reasons": reasons,
        "caveats": caveats,
        "heuristic_label": {
            "class": heuristic["lead_quality_class_heuristic_v1"],
            "score": round(float(heuristic["lead_quality_score_heuristic_v1"]) * 100.0, 2),
        },
    }


def score_territory_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    features = build_tier1_feature_vector(row)
    scored = _score_common(
        features,
        model_name=TERRITORY_MODEL_NAME,
        feature_scope="tier1",
        feature_columns=TERRITORY_FEATURE_COLUMNS,
    )
    scored["features"] = dict(features)
    return scored


def score_diagnostic_response(response: Mapping[str, Any]) -> Dict[str, Any]:
    features = build_tier2_feature_vector(response)
    scored = _score_common(
        features,
        model_name=DIAGNOSTIC_MODEL_NAME,
        feature_scope="tier2",
        feature_columns=DIAGNOSTIC_FEATURE_COLUMNS,
    )
    scored["features"] = dict(features)
    return scored
