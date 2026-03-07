#!/usr/bin/env python3
"""
Smoke test for Hybrid RAG (cohort + similarity + outcomes) without external APIs.

Scenarios:
1) Empty-history style run (first lead) should not fail.
2) Second lead should retrieve similar docs.
3) Outcomes + intel present should produce cohort stats/pattern signals.
"""

from __future__ import annotations

import os
from typing import Dict, Any, List
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.db import (
    create_run,
    init_db,
    insert_lead,
    insert_lead_doc_v1,
    insert_lead_intel_v1,
    insert_lead_signals,
    upsert_lead_outcome,
)
from pipeline.doc_builder import build_typed_docs_for_lead
from pipeline.rag.hybrid_retriever import build_rag_context, build_retrieval_criteria
import pipeline.rag.hybrid_retriever as hr


def fake_embed(text: str) -> List[float]:
    s = sum(ord(c) for c in (text or ""))
    ln = len(text or "")
    vowels = sum(1 for c in (text or "").lower() if c in "aeiou")
    return [float((s % 997) / 997.0), float((ln % 251) / 251.0), float((vowels % 113) / 113.0)]


def _build_lead(name: str, city: str, state: str, review_count: int, has_booking: bool, runs_ads: bool) -> Dict[str, Any]:
    return {
        "name": name,
        "place_id": f"smoke:{name.lower().replace(' ', '_')}",
        "city": city,
        "state": state,
        "signal_review_count": review_count,
        "signal_rating": 4.6,
        "signal_has_contact_form": True,
        "signal_has_automated_scheduling": has_booking,
        "signal_runs_paid_ads": runs_ads,
        "signal_has_ssl": True,
        "signal_page_load_time_ms": 1200,
        "signal_extraction_method": "http",
        "competitive_snapshot": {
            "avg_review_count": 220,
            "market_density": "high",
            "competitor_count": 12,
            "review_positioning": "Below market",
        },
        "service_intelligence": {
            "high_ticket_services_detected": ["implants", "veneers"],
            "missing_high_value_pages": ["invisalign"],
            "confidence": "high",
        },
    }


def _insert_docs_for_lead(lead_id: int, lead: Dict[str, Any]) -> List[Dict[str, Any]]:
    docs = build_typed_docs_for_lead(lead, lead, {"vertical": "dentist", "city": lead.get("city"), "state": lead.get("state")})
    for d in docs:
        emb = fake_embed(d["content_text"])
        insert_lead_doc_v1(
            lead_id=lead_id,
            doc_type=d["doc_type"],
            content_text=d["content_text"],
            metadata=d["metadata_json"],
            embedding=emb,
        )
    return docs


def main() -> None:
    init_db()

    # Force local embedding path for similarity channel in this smoke.
    os.environ["OPENAI_API_KEY"] = "smoke"
    hr.get_embedding = fake_embed

    run_id = create_run({"source": "smoke_hybrid_rag"})

    # Lead 1 (history starts here)
    lead1 = _build_lead("Smile One Dental", "San Jose", "CA", review_count=40, has_booking=False, runs_ads=True)
    lead1_id = insert_lead(run_id, lead1)
    insert_lead_signals(lead1_id, lead1)
    docs1 = _insert_docs_for_lead(lead1_id, lead1)

    criteria1 = build_retrieval_criteria({**lead1, "lead_id": lead1_id}, query_docs=docs1)
    rag1 = build_rag_context({**lead1, "lead_id": lead1_id}, criteria1)
    print("Scenario 1:", {"cohort_count": rag1["cohort"]["cohort_count"], "similar": len(rag1["similar_docs"])})

    # Lead 2 (should find lead1 docs as similar)
    lead2 = _build_lead("Smile Two Dental", "San Jose", "CA", review_count=55, has_booking=False, runs_ads=True)
    lead2_id = insert_lead(run_id, lead2)
    insert_lead_signals(lead2_id, lead2)
    docs2 = _insert_docs_for_lead(lead2_id, lead2)

    criteria2 = build_retrieval_criteria({**lead2, "lead_id": lead2_id}, query_docs=docs2)
    rag2 = build_rag_context({**lead2, "lead_id": lead2_id}, criteria2)
    print("Scenario 2:", {"cohort_count": rag2["cohort"]["cohort_count"], "similar": len(rag2["similar_docs"])})

    # Add outcomes + intel for pattern stats
    insert_lead_intel_v1(
        lead_id=lead1_id,
        vertical="dentist",
        primary_constraint="Review authority gap",
        primary_leverage="Conversion path fix",
        contact_priority="high",
        outreach_angle="Show review-to-booking lift plan",
        confidence=0.82,
        risks=["small sample"],
        evidence=[{"source_type": "current_lead_fact", "source_key": "signal_review_count", "note": "40 reviews"}],
    )
    insert_lead_intel_v1(
        lead_id=lead2_id,
        vertical="dentist",
        primary_constraint="Review authority gap",
        primary_leverage="Conversion path fix",
        contact_priority="high",
        outreach_angle="Show review-to-booking lift plan",
        confidence=0.79,
        risks=["small sample"],
        evidence=[{"source_type": "current_lead_fact", "source_key": "signal_review_count", "note": "55 reviews"}],
    )
    upsert_lead_outcome(lead1_id, vertical="dentist", status="closed_won", closed=True, proposal_sent=True, contacted=True)
    upsert_lead_outcome(lead2_id, vertical="dentist", status="booked", proposal_sent=True, contacted=True)

    # Lead 3 to test cohort stats with outcomes populated
    lead3 = _build_lead("Smile Three Dental", "San Jose", "CA", review_count=50, has_booking=False, runs_ads=True)
    lead3_id = insert_lead(run_id, lead3)
    insert_lead_signals(lead3_id, lead3)
    docs3 = _insert_docs_for_lead(lead3_id, lead3)

    criteria3 = build_retrieval_criteria({**lead3, "lead_id": lead3_id}, query_docs=docs3)
    rag3 = build_rag_context({**lead3, "lead_id": lead3_id}, criteria3)
    print("Scenario 3:", {
        "cohort_count": rag3["cohort"]["cohort_count"],
        "close_rate": (rag3["cohort"].get("cohort_stats") or {}).get("close_rate"),
        "top_constraints": rag3["cohort"].get("top_constraints"),
        "similar": len(rag3["similar_docs"]),
    })


if __name__ == "__main__":
    main()
