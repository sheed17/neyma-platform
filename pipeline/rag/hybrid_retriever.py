"""
Hybrid RAG retriever: cohort SQL + vector similarity + outcome patterns.

Design goals:
- deterministic and explainable
- safe with empty history
- minimal compute; embed only when needed
"""

from __future__ import annotations

import hashlib
import os
import time
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

from pipeline.db import (
    get_outcomes_for_lead_ids,
    list_docs_with_embeddings_v1,
    list_latest_lead_intel_v1_for_leads,
    list_signal_profile_docs,
)
from pipeline.embeddings import get_embedding

# Local in-process cache to avoid repeated query embeddings in one run.
_QUERY_EMBED_CACHE: Dict[str, List[float]] = {}


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a <= 0 or norm_b <= 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _norm_str(v: Any) -> str:
    return str(v or "").strip().lower()


def _bucket_review_gap(value: Any) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if v < 50:
        return "0-50"
    if v < 150:
        return "50-150"
    if v < 300:
        return "150-300"
    return "300+"


def _cohort_match_score(meta: Dict[str, Any], criteria: Dict[str, Any]) -> int:
    score = 0
    if _norm_str(meta.get("vertical")) == _norm_str(criteria.get("vertical")):
        score += 3
    if _norm_str(meta.get("city")) == _norm_str(criteria.get("city")):
        score += 3
    elif _norm_str(meta.get("state")) == _norm_str(criteria.get("state")):
        score += 1

    if _norm_str(meta.get("review_gap_bucket")) == _norm_str(criteria.get("review_gap_bucket")):
        score += 2
    if _norm_str(meta.get("market_density")) == _norm_str(criteria.get("market_density")):
        score += 2

    for key in ("has_booking", "runs_paid_ads"):
        expected = criteria.get(key)
        if expected is None:
            continue
        if meta.get(key) is expected:
            score += 1
    return score


def _derive_rates(outcomes_by_lead: Dict[int, Dict[str, Any]], lead_ids: List[int]) -> Dict[str, Optional[float]]:
    if not lead_ids:
        return {"close_rate": None, "reply_rate": None, "booked_rate": None}

    n = len(lead_ids)
    closed_won = 0
    replied = 0
    booked = 0

    for lead_id in lead_ids:
        row = outcomes_by_lead.get(lead_id) or {}
        outcome_status = _norm_str(row.get("outcome_status") or row.get("status"))

        is_reply = outcome_status in {"replied", "booked", "closed_won", "won", "qualified"}
        is_booked = outcome_status in {"booked", "closed_won", "won"} or bool(row.get("proposal_sent"))
        is_closed = outcome_status in {"closed_won", "won"} or bool(row.get("closed"))

        if is_reply:
            replied += 1
        if is_booked:
            booked += 1
        if is_closed:
            closed_won += 1

    return {
        "close_rate": round(closed_won / n, 3) if n else None,
        "reply_rate": round(replied / n, 3) if n else None,
        "booked_rate": round(booked / n, 3) if n else None,
    }


def get_cohort_leads(criteria: Dict[str, Any], limit: int = 50) -> Dict[str, Any]:
    """
    Cohort retrieval over stored signal_profile docs + lead_intel/outcomes.

    Returns safe empty stats when historical data is unavailable.
    """
    docs = list_signal_profile_docs(limit=2000, vertical=criteria.get("vertical"))
    current_lead_id = criteria.get("lead_id")

    scored: List[Dict[str, Any]] = []
    for doc in docs:
        if current_lead_id and doc.get("lead_id") == current_lead_id:
            continue
        meta = doc.get("metadata_json") or {}
        score = _cohort_match_score(meta, criteria)
        if score <= 0:
            continue
        scored.append({"lead_id": doc.get("lead_id"), "score": score, "metadata": meta})

    scored.sort(key=lambda x: x["score"], reverse=True)
    selected = scored[: max(1, int(limit))]
    lead_ids = [int(x["lead_id"]) for x in selected if x.get("lead_id") is not None]

    result: Dict[str, Any] = {
        "cohort_count": len(lead_ids),
        "cohort_stats": {
            "close_rate": None,
            "reply_rate": None,
            "booked_rate": None,
        },
        "top_constraints": [],
        "top_outreach_angles": [],
        "notable_patterns": [],
        "lead_ids": lead_ids,
    }
    if not lead_ids:
        return result

    outcomes = get_outcomes_for_lead_ids(lead_ids)
    rates = _derive_rates(outcomes, lead_ids)
    result["cohort_stats"] = rates

    intel_by_lead = list_latest_lead_intel_v1_for_leads(lead_ids)
    constraints = Counter()
    angles = Counter()
    for lead_id, intel in intel_by_lead.items():
        if intel.get("primary_constraint"):
            constraints[str(intel["primary_constraint"]).strip()] += 1
        if intel.get("outreach_angle"):
            angles[str(intel["outreach_angle"]).strip()] += 1

    result["top_constraints"] = [
        {"value": v, "count": c} for v, c in constraints.most_common(5)
    ]
    result["top_outreach_angles"] = [
        {"value": v, "count": c} for v, c in angles.most_common(5)
    ]

    notes: List[str] = []
    if rates.get("close_rate") is not None:
        notes.append(f"Cohort close_rate={rates['close_rate']:.2f} across {len(lead_ids)} leads")
    if result["top_constraints"]:
        notes.append(f"Most common constraint: {result['top_constraints'][0]['value']}")
    if result["top_outreach_angles"]:
        notes.append(f"Most common outreach angle: {result['top_outreach_angles'][0]['value']}")
    result["notable_patterns"] = notes[:3]

    return result


def build_retrieval_criteria(current_lead: Dict[str, Any], query_docs: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Compute deterministic cohort criteria from current lead facts."""
    snapshot = current_lead.get("competitive_snapshot") or {}
    lead_reviews = current_lead.get("signal_review_count") or current_lead.get("user_ratings_total")
    avg_reviews = snapshot.get("avg_review_count")
    review_gap = None
    try:
        if lead_reviews is not None and avg_reviews is not None:
            review_gap = float(avg_reviews) - float(lead_reviews)
    except (TypeError, ValueError):
        review_gap = None

    return {
        "lead_id": current_lead.get("lead_id"),
        "vertical": "dentist",
        "city": current_lead.get("city"),
        "state": current_lead.get("state"),
        "review_gap_bucket": _bucket_review_gap(review_gap),
        "market_density": snapshot.get("market_density") or snapshot.get("market_density_score"),
        "has_booking": current_lead.get("signal_has_automated_scheduling"),
        "runs_paid_ads": current_lead.get("signal_runs_paid_ads"),
        "query_docs": query_docs or [],
    }


def _pick_query_text(query_docs: List[Dict[str, Any]]) -> str:
    preferred = ["signal_profile", "service_coverage", "market_context"]
    by_type = {d.get("doc_type"): d for d in query_docs if isinstance(d, dict)}
    parts: List[str] = []
    for key in preferred:
        d = by_type.get(key)
        if d and d.get("content_text"):
            parts.append(str(d["content_text"]))
    if not parts:
        for d in query_docs[:3]:
            txt = (d or {}).get("content_text")
            if txt:
                parts.append(str(txt))
    return "\n".join(parts).strip()


def _get_or_embed_query_text(query_text: str) -> Optional[List[float]]:
    if not query_text:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        return None
    cache_key = hashlib.sha256(query_text.encode("utf-8")).hexdigest()
    if cache_key in _QUERY_EMBED_CACHE:
        return _QUERY_EMBED_CACHE[cache_key]
    emb = get_embedding(query_text)
    if emb:
        _QUERY_EMBED_CACHE[cache_key] = emb
    return emb


def get_similar_docs(lead_id: int, query_docs: List[Dict[str, Any]], k: int = 6) -> List[Dict[str, Any]]:
    """
    Vector retrieval over typed doc embeddings from other leads.
    """
    query_text = _pick_query_text(query_docs)
    if not query_text:
        return []

    query_embedding = _get_or_embed_query_text(query_text)
    if not query_embedding:
        return []

    candidates = list_docs_with_embeddings_v1(
        doc_types=["signal_profile", "llm_brief_summary", "conversion_path"],
        exclude_lead_id=lead_id,
        limit=1200,
    )
    scored: List[Dict[str, Any]] = []
    for doc in candidates:
        emb = doc.get("embedding")
        if not isinstance(emb, list) or not emb:
            continue
        sim = _cosine_similarity(query_embedding, emb)
        if sim <= 0:
            continue
        scored.append(
            {
                "id": doc.get("id"),
                "lead_id": doc.get("lead_id"),
                "doc_type": doc.get("doc_type"),
                "content_text": doc.get("content_text") or "",
                "similarity_score": round(float(sim), 4),
                "metadata_json": doc.get("metadata_json") or {},
            }
        )

    scored.sort(key=lambda x: x["similarity_score"], reverse=True)
    return scored[: max(0, int(k))]


def get_best_patterns(criteria: Dict[str, Any], cohort: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Outcome-weighted pattern extraction from cohort + intel history.
    """
    cohort_data = cohort or get_cohort_leads(criteria, limit=80)
    lead_ids = cohort_data.get("lead_ids") or []
    if not lead_ids:
        return {}

    outcomes = get_outcomes_for_lead_ids(lead_ids)
    intel_by_lead = list_latest_lead_intel_v1_for_leads(lead_ids)

    win_counts_by_constraint = defaultdict(lambda: {"wins": 0, "total": 0})
    booked_counts_by_angle = defaultdict(lambda: {"booked_or_replied": 0, "total": 0})

    qualified_rows = 0
    for lead_id in lead_ids:
        intel = intel_by_lead.get(lead_id)
        outcome = outcomes.get(lead_id)
        if not intel or not outcome:
            continue
        qualified_rows += 1

        outcome_status = _norm_str(outcome.get("outcome_status") or outcome.get("status"))
        is_win = outcome_status in {"closed_won", "won"} or bool(outcome.get("closed"))
        is_booked_or_replied = (
            outcome_status in {"booked", "replied", "closed_won", "won", "qualified"}
            or bool(outcome.get("proposal_sent"))
            or bool(outcome.get("contacted"))
        )

        constraint = (intel.get("primary_constraint") or "").strip()
        if constraint:
            win_counts_by_constraint[constraint]["total"] += 1
            if is_win:
                win_counts_by_constraint[constraint]["wins"] += 1

        angle = (intel.get("outreach_angle") or "").strip()
        if angle:
            booked_counts_by_angle[angle]["total"] += 1
            if is_booked_or_replied:
                booked_counts_by_angle[angle]["booked_or_replied"] += 1

    if qualified_rows < 5:
        return {}

    best_constraints = []
    for constraint, stats in win_counts_by_constraint.items():
        total = stats["total"]
        if total < 2:
            continue
        rate = stats["wins"] / total if total else 0
        best_constraints.append({"value": constraint, "sample": total, "closed_won_rate": round(rate, 3)})
    best_constraints.sort(key=lambda x: (x["closed_won_rate"], x["sample"]), reverse=True)

    best_angles = []
    for angle, stats in booked_counts_by_angle.items():
        total = stats["total"]
        if total < 2:
            continue
        rate = stats["booked_or_replied"] / total if total else 0
        best_angles.append({"value": angle, "sample": total, "booked_or_replied_rate": round(rate, 3)})
    best_angles.sort(key=lambda x: (x["booked_or_replied_rate"], x["sample"]), reverse=True)

    return {
        "sufficient_data": bool(best_constraints or best_angles),
        "best_constraints_for_closed_won": best_constraints[:5],
        "best_outreach_angles_for_booked_or_replied": best_angles[:5],
        "sample_size": qualified_rows,
    }


def build_rag_context(current_lead: Dict[str, Any], criteria: Dict[str, Any], k_similar: int = 6) -> Dict[str, Any]:
    """
    Build unified hybrid RAG context object.
    """
    t0 = time.time()
    lead_id = int(criteria.get("lead_id") or current_lead.get("lead_id") or 0)
    query_docs = criteria.get("query_docs") or []

    cohort = get_cohort_leads(criteria, limit=int(criteria.get("cohort_limit") or 50))
    similar_docs = get_similar_docs(lead_id=lead_id, query_docs=query_docs, k=k_similar) if lead_id else []
    outcome_patterns = get_best_patterns(criteria, cohort=cohort)

    retrieval_time_ms = int((time.time() - t0) * 1000)
    rag_used = bool(cohort.get("cohort_count") or similar_docs or outcome_patterns)

    return {
        "cohort": {
            "cohort_count": cohort.get("cohort_count", 0),
            "cohort_stats": cohort.get("cohort_stats") or {},
            "top_constraints": cohort.get("top_constraints") or [],
            "top_outreach_angles": cohort.get("top_outreach_angles") or [],
            "notable_patterns": cohort.get("notable_patterns") or [],
        },
        "similar_docs": similar_docs,
        "outcome_patterns": outcome_patterns or {},
        "guardrails": {
            "claims_about_current_lead_must_use_current_facts_only": True,
            "similar_docs_are_pattern_evidence_only": True,
            "output_must_reference_provided_evidence_keys": True,
        },
        "metrics": {
            "retrieval_time_ms": retrieval_time_ms,
            "num_similar_docs": len(similar_docs),
            "cohort_count": cohort.get("cohort_count", 0),
            "rag_used": rag_used,
        },
    }
