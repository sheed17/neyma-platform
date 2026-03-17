"""Persistence helpers for Neyma lead-quality scoring."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from backend.ml.runtime import score_diagnostic_response
from pipeline.db import (
    save_ml_feature_snapshot,
    save_ml_prediction,
    update_diagnostic_ml_fields,
)


def persist_scored_entity(
    *,
    entity_type: str,
    entity_id: int,
    place_id: str,
    feature_scope: str,
    feature_version: str,
    score_payload: Dict[str, Any],
    feature_payload: Dict[str, Any],
) -> Optional[int]:
    if not entity_id or not place_id:
        return None

    save_ml_feature_snapshot(
        entity_type=entity_type,
        entity_id=entity_id,
        place_id=place_id,
        feature_scope=feature_scope,
        feature_version=feature_version,
        feature_payload=feature_payload,
        data_confidence=float(score_payload.get("data_confidence") or 0.0),
        signal_completeness_ratio=float(feature_payload.get("signal_completeness_ratio") or 0.0),
    )

    return save_ml_prediction(
        entity_type=entity_type,
        entity_id=entity_id,
        place_id=place_id,
        model_name=str(score_payload.get("model_name") or ""),
        model_version=str(score_payload.get("model_version") or ""),
        feature_version=str(score_payload.get("feature_version") or ""),
        label_version=str(score_payload.get("label_version") or ""),
        score=float(score_payload.get("prob_high_value") or 0.0),
        score_0_100=float(score_payload.get("score") or 0.0),
        predicted_class=str(score_payload.get("class") or "decent"),
        prob_high_value=float(score_payload.get("prob_high_value") or 0.0),
        data_confidence=float(score_payload.get("data_confidence") or 0.0),
        reasons=score_payload.get("reasons") or [],
        caveats=score_payload.get("caveats") or [],
        components=score_payload.get("components") or {},
        top_features=feature_payload,
    )


def persist_saved_diagnostic_response(
    *,
    diagnostic_id: int,
    place_id: str,
    response: Mapping[str, Any],
) -> Optional[int]:
    """Score and persist lead-quality for a saved diagnostic."""
    if not diagnostic_id:
        return None
    scored = score_diagnostic_response(response)
    prediction_id = persist_scored_entity(
        entity_type="diagnostic",
        entity_id=int(diagnostic_id),
        place_id=place_id,
        feature_scope="tier2",
        feature_version=str(scored.get("feature_version") or ""),
        score_payload=scored,
        feature_payload=dict(scored.get("features") or {}),
    )
    payload = {k: v for k, v in scored.items() if k != "features"}
    update_diagnostic_ml_fields(
        diagnostic_id,
        lead_quality_score=float(payload.get("score") or 0.0),
        lead_quality_class=str(payload.get("class") or ""),
        lead_model_version=str(payload.get("model_version") or ""),
        lead_feature_version=str(payload.get("feature_version") or ""),
        lead_quality_payload=payload,
    )
    if isinstance(response, dict):
        response["lead_quality"] = payload
    return prediction_id
