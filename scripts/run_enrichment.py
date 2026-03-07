#!/usr/bin/env python3
"""
Lead Enrichment & Signal Extraction Pipeline

Enriches leads from Step 3 with:
- Place Details (website, phone, reviews)
- Website signals (SSL, mobile-friendly, contact forms, booking widgets)
- Phone normalization
- Review recency analysis

Usage:
    python scripts/run_enrichment.py

Environment Variables:
    GOOGLE_PLACES_API_KEY: Required. Your Google Places API key.
    META_ACCESS_TOKEN: Optional. If set, augments leads with Meta Ads Library
        (confirms runs_paid_ads / paid_ads_channels from API when ads found).

Input:
    Reads from output/leads_*.json (most recent file)

Output:
    Writes to output/enriched_*.json
"""

import os
import sys
import json
import glob
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env from project root so META_ACCESS_TOKEN, GOOGLE_PLACES_API_KEY work without exporting
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
except ImportError:
    pass

from pipeline.enrich import PlaceDetailsEnricher
from pipeline.signals import (
    extract_signals,
    extract_signals_batch,
    merge_signals_into_lead
)
from pipeline.meta_ads import get_meta_access_token, augment_lead_with_meta_ads
from pipeline.semantic_signals import build_semantic_signals
from pipeline.decision_agent import DecisionAgent
from pipeline.objective_intelligence import (
    build_objective_intelligence,
    build_objective_intelligence_summary,
)
from pipeline.db import (
    init_db,
    create_run,
    insert_lead,
    insert_lead_signals,
    insert_decision,
    insert_lead_doc_v1,
    insert_lead_intel_v1,
    update_lead_dentist_data,
    update_run_completed,
    update_run_failed,
)
from pipeline.doc_builder import build_typed_docs_for_lead, build_llm_brief_summary_doc
from pipeline.embedding_store import store_lead_embedding_if_eligible
from pipeline.embeddings import get_embedding
from pipeline.rag.hybrid_retriever import build_rag_context, build_retrieval_criteria
from pipeline.validation import check_lead_signals
from pipeline.context import build_context
from pipeline.dentist_profile import (
    is_dental_practice,
    build_dentist_profile_v1,
    fetch_website_html_for_trust,
)
from pipeline.dentist_llm_reasoning import dentist_llm_reasoning_layer
from pipeline.sales_intervention import build_sales_intervention_intelligence
from pipeline.objective_decision_layer import compute_objective_decision_layer
from pipeline.service_depth import build_service_intelligence
from pipeline.competitor_sampling import fetch_competitors_nearby, build_competitive_snapshot
from pipeline.revenue_intelligence import build_revenue_intelligence
from pipeline.agency_decision import build_agency_decision_v1
from pipeline.llm_structured_extraction import extract_structured
from pipeline.llm_executive_compression import build_executive_summary_and_outreach
from pipeline.service_depth import get_page_texts_for_llm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f'enrichment_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    "input_dir": "output",
    "output_dir": "output",
    "max_leads": None,  # None = process all, or set a number for testing
    "progress_interval": 10,
    "agency_type": os.getenv("AGENCY_TYPE", "marketing").lower() or "marketing",  # "seo" | "marketing"
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def find_latest_leads_file(input_dir: str) -> str:
    """Find the most recent leads JSON file."""
    pattern = os.path.join(input_dir, "leads_*.json")
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No leads files found matching {pattern}")
    
    # Sort by modification time, newest first
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def _compute_run_stats(signals: List[Dict]) -> Dict:
    """Health/coverage metrics for a run (stored in runs.run_stats)."""
    total = len(signals)
    if total == 0:
        return {"total": 0}
    keys = ("has_website", "website_accessible", "has_contact_form", "has_phone", "has_email", "has_automated_scheduling")
    counts = {f"{k}_true": sum(1 for s in signals if s.get(k) is True) for k in keys}
    counts.update({f"{k}_false": sum(1 for s in signals if s.get(k) is False) for k in keys})
    counts.update({f"{k}_unknown": sum(1 for s in signals if s.get(k) is None) for k in keys})
    known = sum(1 for s in signals if any(s.get(k) is not None for k in keys))
    counts["total"] = total
    counts["signal_coverage_pct"] = round(100 * known / total, 1) if total else 0
    return counts


def _store_lead_embedding(lead_id: int, lead: Dict, force_embed: bool = False) -> None:
    """Store canonical embedding for dental lead with objective_intelligence. Delegates to shared pipeline helper."""
    store_lead_embedding_if_eligible(lead_id, lead, force_embed=force_embed)


def _store_decision(lead: Dict, decision, agency_type: str) -> None:
    """Write Decision Agent output to lead: decision_agent_v1 and top-level fields."""
    lead["decision_agent_v1"] = {
        "verdict": decision.verdict,
        "confidence": decision.confidence,
        "reasoning": decision.reasoning,
        "primary_risks": decision.primary_risks or [],
        "what_would_change": decision.what_would_change or [],
        "agency_type": agency_type,
    }
    lead["verdict"] = decision.verdict
    lead["confidence"] = decision.confidence
    lead["reasoning"] = decision.reasoning
    lead["primary_risks"] = decision.primary_risks or []
    lead["what_would_change"] = decision.what_would_change or []
    lead["agency_type"] = agency_type


def load_place_ids(filepath: str) -> List[str]:
    """Load place_ids from file: one per line or JSON array."""
    with open(filepath, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    if raw.startswith("["):
        return json.loads(raw)
    return [line.strip() for line in raw.splitlines() if line.strip()]


def load_leads(filepath: str) -> List[Dict]:
    """Load leads from JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle both wrapped and unwrapped formats
    if isinstance(data, dict) and "leads" in data:
        return data["leads"]
    elif isinstance(data, list):
        return data
    else:
        raise ValueError(f"Unexpected data format in {filepath}")


def save_enriched_leads(
    leads: List[Dict],
    signals: List[Dict],
    output_dir: str,
    source_file: str
) -> str:
    """Save enriched leads with signals to JSON."""
    os.makedirs(output_dir, exist_ok=True)
    
    use_meta_ads = get_meta_access_token() is not None
    if use_meta_ads:
        logger.info("META_ACCESS_TOKEN set — augmenting leads with Meta Ads Library")
    
    # Merge signals into leads, optionally augment with Meta Ads Library
    enriched_leads = []
    for lead, signal in zip(leads, signals):
        merged = merge_signals_into_lead(lead, signal)
        if use_meta_ads:
            augment_lead_with_meta_ads(merged)
        enriched_leads.append(merged)
    
    # Generate output filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"enriched_leads_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    # Build output with metadata
    output_data = {
        "metadata": {
            "source_file": os.path.basename(source_file),
            "enriched_at": datetime.utcnow().isoformat(),
            "total_leads": len(enriched_leads),
            "leads_with_website": sum(1 for s in signals if s.get("has_website")),
            "leads_with_phone": sum(1 for s in signals if s.get("has_phone")),
        },
        "leads": enriched_leads
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Saved enriched leads to: {filepath}")
    return filepath


def generate_signal_summary(signals: List[Dict]) -> Dict:
    """
    Generate summary statistics for extracted signals.
    
    Tri-State Signal Semantics:
    - true  = Confidently observed
    - false = Confidently absent
    - null  = Unknown / not determinable
    
    HVAC Signal Interpretation:
    - Phone = PRIMARY booking mechanism
    - Contact form = Inbound readiness
    - Automated scheduling = Ops maturity (not booking requirement)
    - Reviews = Business activity indicator
    """
    total = len(signals)
    if total == 0:
        return {}
    
    # Helper to count tri-state values
    def count_true(key):
        return sum(1 for s in signals if s.get(key) is True)
    
    def count_false(key):
        return sum(1 for s in signals if s.get(key) is False)
    
    def count_null(key):
        return sum(1 for s in signals if s.get(key) is None)
    
    # Phone signals - PRIMARY booking mechanism for HVAC
    has_phone = count_true("has_phone")
    
    # Website signals (tri-state aware)
    has_website = count_true("has_website")
    website_accessible_true = count_true("website_accessible")
    website_accessible_null = count_null("website_accessible")
    has_ssl = count_true("has_ssl")
    mobile_friendly = count_true("mobile_friendly")
    has_trust_badges = count_true("has_trust_badges")
    
    # Inbound readiness - Contact Form (AGENCY-SAFE: false is rare)
    has_contact_form_true = count_true("has_contact_form")
    has_contact_form_false = count_false("has_contact_form")
    has_contact_form_null = count_null("has_contact_form")
    
    # Email (NEVER false - may exist elsewhere)
    has_email_true = count_true("has_email")
    has_email_null = count_null("has_email")
    
    # Operational maturity (tri-state - false is OK for scheduling)
    has_automated_scheduling_true = count_true("has_automated_scheduling")
    has_automated_scheduling_false = count_false("has_automated_scheduling")
    has_automated_scheduling_null = count_null("has_automated_scheduling")
    
    # Review signals - business activity indicator
    has_reviews = sum(1 for s in signals if s.get("review_count", 0) > 0)
    ratings = [s["rating"] for s in signals if s.get("rating") is not None]
    review_counts = [s["review_count"] for s in signals if s.get("review_count")]
    
    # Days since last review
    days_since_review = [
        s["last_review_days_ago"] 
        for s in signals 
        if s.get("last_review_days_ago") is not None
    ]
    
    # Active businesses: >5 reviews AND last review <365 days ago
    active_businesses = sum(
        1 for s in signals 
        if s.get("review_count", 0) > 5 
        and s.get("last_review_days_ago") is not None 
        and s.get("last_review_days_ago") < 365
    )
    
    # Booking capable: has phone (primary HVAC booking mechanism)
    booking_capable = has_phone
    
    # Manual ops (opportunity): explicitly false (analyzed, no scheduling found)
    # null means unknown, so we don't count those as opportunities
    manual_ops_confirmed = has_automated_scheduling_false
    
    return {
        "total_leads": total,
        "booking": {
            "has_phone": has_phone,
            "has_phone_pct": round(has_phone / total * 100, 1),
            "booking_capable": booking_capable,
            "booking_capable_pct": round(booking_capable / total * 100, 1),
        },
        "inbound_readiness": {
            "contact_form_true": has_contact_form_true,
            "contact_form_true_pct": round(has_contact_form_true / total * 100, 1),
            "contact_form_false": has_contact_form_false,
            "contact_form_unknown": has_contact_form_null,
            "email_found": has_email_true,
            "email_found_pct": round(has_email_true / total * 100, 1),
            "email_unknown": has_email_null,
        },
        "ops_maturity": {
            "automated": has_automated_scheduling_true,
            "automated_pct": round(has_automated_scheduling_true / total * 100, 1),
            "manual_confirmed": manual_ops_confirmed,
            "manual_confirmed_pct": round(manual_ops_confirmed / total * 100, 1),
            "unknown": has_automated_scheduling_null,
        },
        "website": {
            "has_website": has_website,
            "has_website_pct": round(has_website / total * 100, 1),
            "accessible": website_accessible_true,
            "not_accessible": count_false("website_accessible"),
            "unknown": website_accessible_null,
            "has_ssl": has_ssl,
            "mobile_friendly": mobile_friendly,
            "has_trust_badges": has_trust_badges,
        },
        "activity": {
            "has_reviews": has_reviews,
            "active_businesses": active_businesses,
            "active_pct": round(active_businesses / total * 100, 1),
            "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else None,
            "avg_review_count": round(sum(review_counts) / len(review_counts), 1) if review_counts else None,
            "avg_days_since_review": round(sum(days_since_review) / len(days_since_review), 1) if days_since_review else None,
        }
    }


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_enrichment_pipeline(
    input_file: str = None,
    max_leads: int = None,
    place_ids_file: str = None,
    force_embed: bool = False,
) -> List[Dict]:
    """
    Run the complete enrichment and signal extraction pipeline.
    
    Args:
        input_file: Path to leads JSON file (default: find latest)
        max_leads: Maximum leads to process (for testing)
    
    Returns:
        List of signal dictionaries
    """
    logger.info("=" * 60)
    logger.info("Lead Enrichment & Signal Extraction Pipeline")
    logger.info("=" * 60)
    
    # Step 1: Load leads
    if input_file is None:
        input_file = find_latest_leads_file(CONFIG["input_dir"])
    
    logger.info(f"Loading leads from: {input_file}")
    leads = load_leads(input_file)
    logger.info(f"Loaded {len(leads)} leads")
    
    # Optionally limit for testing
    if max_leads:
        leads = leads[:max_leads]
        logger.info(f"Limited to {max_leads} leads for processing")

    # Optional: only enrich leads whose place_id is in the given file (re-enrichment)
    if place_ids_file:
        place_ids = set(load_place_ids(place_ids_file))
        before = len(leads)
        leads = [l for l in leads if l.get("place_id") in place_ids]
        logger.info(f"Filtered to {len(leads)} leads (place_ids file: {place_ids_file}, had {before})")
        if not leads:
            logger.warning("No leads left after place_ids filter; exiting")
            return []

    # Step 2: Enrich with Place Details
    logger.info("\nStep 1: Fetching Place Details (website, phone, reviews)...")
    try:
        enricher = PlaceDetailsEnricher()
    except ValueError as e:
        logger.error(f"Cannot initialize enricher: {e}")
        return []
    
    enriched_leads = enricher.enrich_leads_batch(
        leads,
        progress_interval=CONFIG["progress_interval"]
    )
    
    enricher_stats = enricher.get_stats()
    logger.info(f"Place Details API calls: {enricher_stats['total_requests']}")
    logger.info(f"Estimated cost: ${enricher_stats['estimated_cost_usd']:.4f}")
    logger.info(f"Cost optimization: {enricher_stats['savings_vs_all_fields']}")
    
    # Step 3: Extract signals
    logger.info("\nStep 2: Extracting signals (website analysis, phone, reviews)...")
    signals = extract_signals_batch(
        enriched_leads,
        progress_interval=CONFIG["progress_interval"]
    )
    
    # Step 3b: Decision Agent (single owner of judgment); no embeddings/RAG in v1
    agency_type = CONFIG.get("agency_type", "marketing")
    if agency_type not in ("seo", "marketing"):
        agency_type = "marketing"
    logger.info("Decision Agent: agency_type=%s", agency_type)
    run_id = create_run({
        "max_leads": CONFIG.get("max_leads"),
        "agency_type": agency_type,
        "source": "run_enrichment",
    })
    use_meta_ads = get_meta_access_token() is not None
    agent = DecisionAgent(agency_type=agency_type)
    rag_enabled = os.getenv("USE_HYBRID_RAG", "true").strip().lower() in ("1", "true", "yes")
    embeddings_enabled = bool(os.getenv("OPENAI_API_KEY"))
    try:
        for idx, (lead, signal) in enumerate(zip(enriched_leads, signals)):
            merged = merge_signals_into_lead(lead, signal)
            if use_meta_ads:
                augment_lead_with_meta_ads(merged)
            lead_id = insert_lead(run_id, merged)
            merged["lead_id"] = lead_id
            insert_lead_signals(lead_id, signal)

            if is_dental_practice(merged):
                # Dental: Competitors -> Objective decision layer -> Revenue intel -> Objective intelligence -> Decision Agent
                url = merged.get("signal_website_url")
                website_html = fetch_website_html_for_trust(url) if url else None
                dentist_profile_v1 = build_dentist_profile_v1(merged, website_html=website_html)
                obj_layer = None
                llm_reasoning_layer = {}
                sales_intel = None
                if dentist_profile_v1:
                    merged["dentist_profile_v1"] = dentist_profile_v1
                    procedure_mentions = (dentist_profile_v1.get("review_intent_analysis") or {}).get("procedure_mentions") or []
                    service_intel = build_service_intelligence(url, website_html, procedure_mentions)
                    competitors = []
                    search_radius_used_miles = 2
                    lat, lng = merged.get("latitude"), merged.get("longitude")
                    if lat is not None and lng is not None:
                        competitors, search_radius_used_miles = fetch_competitors_nearby(lat, lng, merged.get("place_id"))
                    competitive_snap = build_competitive_snapshot(merged, competitors, search_radius_used_miles) if competitors else {}
                    merged["competitive_snapshot"] = competitive_snap
                    merged["service_intelligence"] = service_intel
                    obj_layer = compute_objective_decision_layer(
                        merged,
                        service_intelligence=service_intel,
                        competitive_snapshot=competitive_snap if competitors else None,
                        revenue_leverage=None,
                    )
                    merged["objective_decision_layer"] = obj_layer if obj_layer else {}
                    pricing_page_detected = False
                    if os.getenv("USE_LLM_STRUCTURED_EXTRACTION", "").strip().lower() in ("1", "true", "yes"):
                        page_texts = get_page_texts_for_llm(merged.get("signal_website_url"), website_html)
                        pricing_page_detected = bool(page_texts and page_texts.get("pricing_page_text"))
                    rev_intel = build_revenue_intelligence(
                        merged,
                        dentist_profile_v1,
                        obj_layer or {},
                        pricing_page_detected=pricing_page_detected,
                        paid_intelligence=merged.get("paid_intelligence"),
                    )
                    merged["revenue_intelligence"] = rev_intel
                # Objective intelligence (deterministic) -> Decision Agent (all dental leads)
                oi = build_objective_intelligence(merged)
                merged["objective_intelligence"] = oi
                oi_summary = build_objective_intelligence_summary(oi)
                decision = agent.decide_from_objective_summary(oi_summary, lead_name=merged.get("name") or "")
                _store_decision(merged, decision, agency_type)
                insert_decision(
                    lead_id=lead_id,
                    agency_type=agency_type,
                    signals_snapshot={"objective_intelligence_summary": oi_summary},
                    verdict=decision.verdict,
                    confidence=decision.confidence,
                    reasoning=decision.reasoning,
                    primary_risks=decision.primary_risks,
                    what_would_change=decision.what_would_change,
                    prompt_version=agent.prompt_version,
                )
                if dentist_profile_v1:
                    context = build_context(merged)
                    lead_score = round((merged.get("confidence") or 0) * 100)
                    # Build and persist deterministic typed docs (pre-LLM).
                    typed_docs = build_typed_docs_for_lead(merged, signal, {"vertical": "dentist"})
                    for doc in typed_docs:
                        emb = None
                        if embeddings_enabled and rag_enabled:
                            emb = get_embedding(doc.get("content_text", ""))
                        insert_lead_doc_v1(
                            lead_id=lead_id,
                            doc_type=doc.get("doc_type", ""),
                            content_text=doc.get("content_text", ""),
                            metadata=doc.get("metadata_json") or {},
                            embedding=emb,
                        )

                    rag_context = {}
                    if rag_enabled:
                        criteria = build_retrieval_criteria(
                            {
                                **merged,
                                "lead_id": lead_id,
                                "city": merged.get("city"),
                                "state": merged.get("state"),
                            },
                            query_docs=typed_docs,
                        )
                        rag_context = build_rag_context(
                            current_lead={**merged, "lead_id": lead_id},
                            criteria=criteria,
                            k_similar=6,
                        )

                    llm_reasoning_layer = dentist_llm_reasoning_layer(
                        business_snapshot=merged,
                        dentist_profile_v1=dentist_profile_v1,
                        context_dimensions=context.get("context_dimensions", []),
                        lead_score=lead_score,
                        priority=merged.get("verdict"),
                        confidence=merged.get("confidence"),
                        rag_context=rag_context if rag_context else None,
                    )
                    # Persist structured intel output for downstream retrieval/analytics.
                    insert_lead_intel_v1(
                        lead_id=lead_id,
                        vertical="dentist",
                        primary_constraint=llm_reasoning_layer.get("primary_constraint"),
                        primary_leverage=llm_reasoning_layer.get("primary_leverage"),
                        contact_priority=llm_reasoning_layer.get("contact_priority"),
                        outreach_angle=llm_reasoning_layer.get("outreach_angle") or llm_reasoning_layer.get("recommended_outreach_angle"),
                        confidence=llm_reasoning_layer.get("confidence"),
                        risks=llm_reasoning_layer.get("risks") or llm_reasoning_layer.get("risk_objections"),
                        evidence=llm_reasoning_layer.get("evidence"),
                    )
                    # Persist post-LLM summary doc for future retrieval.
                    llm_doc = build_llm_brief_summary_doc(
                        merged,
                        signal,
                        {"vertical": "dentist", "llm_reasoning_layer": llm_reasoning_layer},
                    )
                    if llm_doc:
                        llm_emb = get_embedding(llm_doc.get("content_text", "")) if (embeddings_enabled and rag_enabled) else None
                        insert_lead_doc_v1(
                            lead_id=lead_id,
                            doc_type=llm_doc.get("doc_type", "llm_brief_summary"),
                            content_text=llm_doc.get("content_text", ""),
                            metadata=llm_doc.get("metadata_json") or {},
                            embedding=llm_emb,
                        )
                    # UI/product proof layer.
                    merged["cohort_count"] = llm_reasoning_layer.get("cohort_count")
                    merged["cohort_close_rate"] = llm_reasoning_layer.get("cohort_close_rate")
                    merged["top_constraints"] = llm_reasoning_layer.get("top_constraints")
                    merged["top_outreach_angles"] = llm_reasoning_layer.get("top_outreach_angles")
                    merged["similar_leads_count"] = llm_reasoning_layer.get("similar_leads_count")
                    merged["rag_used"] = llm_reasoning_layer.get("rag_used")
                    merged["retrieval_time_ms"] = llm_reasoning_layer.get("retrieval_time_ms")
                    merged["num_similar_docs"] = llm_reasoning_layer.get("num_similar_docs")
                    merged["extraction_method"] = merged.get("signal_extraction_method")
                    sales_intel = build_sales_intervention_intelligence(
                        business_snapshot=merged,
                        dentist_profile_v1=dentist_profile_v1,
                        context_dimensions=context.get("context_dimensions", []),
                        verdict=merged.get("verdict"),
                        confidence=merged.get("confidence"),
                        llm_reasoning_layer=llm_reasoning_layer,
                    )
                    llm_extraction = None
                    if os.getenv("USE_LLM_STRUCTURED_EXTRACTION", "").strip().lower() in ("1", "true", "yes"):
                        page_texts = get_page_texts_for_llm(merged.get("signal_website_url"), website_html)
                        llm_extraction = extract_structured(
                            page_texts.get("homepage_text") or "",
                            page_texts.get("services_page_text"),
                            page_texts.get("pricing_page_text"),
                        )
                        merged["llm_structured_extraction"] = llm_extraction
                    executive_summary = None
                    outreach_angle = None
                    rev_intel = merged.get("revenue_intelligence") or {}
                    if os.getenv("USE_LLM_EXECUTIVE_COMPRESSION", "").strip().lower() in ("1", "true", "yes"):
                        root = (obj_layer or {}).get("root_bottleneck_classification") or {}
                        comp = build_executive_summary_and_outreach(
                            primary_constraint=root.get("why_root_cause") or root.get("bottleneck") or "",
                            revenue_gap=rev_intel.get("organic_revenue_gap_estimate"),
                            cost_leakage_signals=rev_intel.get("cost_leakage_signals"),
                            service_focus=(merged.get("llm_structured_extraction") or {}).get("service_focus"),
                        )
                        executive_summary = comp.get("executive_summary")
                        outreach_angle = comp.get("outreach_angle")
                    merged["agency_decision_v1"] = build_agency_decision_v1(
                        merged,
                        dentist_profile_v1,
                        obj_layer or {},
                        rev_intel,
                        llm_extraction=llm_extraction,
                        executive_summary=executive_summary,
                        outreach_angle=outreach_angle,
                    )
                update_lead_dentist_data(
                    lead_id,
                    dentist_profile_v1=dentist_profile_v1,
                    llm_reasoning_layer=llm_reasoning_layer if llm_reasoning_layer else None,
                    sales_intervention_intelligence=sales_intel if sales_intel else None,
                    objective_decision_layer=obj_layer if obj_layer else None,
                )
                # Store embedding for dental leads with objective_intelligence
                if merged.get("objective_intelligence"):
                    _store_lead_embedding(lead_id, merged, force_embed=force_embed)
            else:
                # Non-dental: semantic signals -> Decision Agent
                semantic = build_semantic_signals(merged)
                decision = agent.decide(semantic, lead_name=merged.get("name") or "")
                _store_decision(merged, decision, agency_type)
                insert_decision(
                    lead_id=lead_id,
                    agency_type=agency_type,
                    signals_snapshot=semantic,
                    verdict=decision.verdict,
                    confidence=decision.confidence,
                    reasoning=decision.reasoning,
                    primary_risks=decision.primary_risks,
                    what_would_change=decision.what_would_change,
                    prompt_version=agent.prompt_version,
                )

            enriched_leads[idx] = merged
            if (idx + 1) % CONFIG["progress_interval"] == 0:
                logger.info(f"  Decision + DB: {idx + 1}/{len(enriched_leads)} leads")
        run_stats = _compute_run_stats(signals)
        update_run_completed(run_id, len(enriched_leads), run_stats=run_stats)
        logger.info(f"Run {run_id[:8]}... completed; {len(enriched_leads)} leads persisted to DB")
    except Exception:
        update_run_failed(run_id)
        raise
    
    # Step 4: Generate summary
    summary = generate_signal_summary(signals)
    
    logger.info("\n" + "=" * 60)
    logger.info("SIGNAL EXTRACTION SUMMARY (HVAC Model + Tri-State)")
    logger.info("=" * 60)
    logger.info(f"Total leads processed: {summary['total_leads']}")
    logger.info("  (true=observed, false=absent, null=unknown)")
    
    logger.info("\n📞 Booking Capability (Phone = Primary for HVAC):")
    logger.info(f"  Has phone: {summary['booking']['has_phone']} ({summary['booking']['has_phone_pct']}%)")
    logger.info(f"  Booking capable: {summary['booking']['booking_capable']} ({summary['booking']['booking_capable_pct']}%)")
    
    logger.info("\n📝 Inbound Readiness (AGENCY-SAFE):")
    logger.info(f"  Contact Form:")
    logger.info(f"    ✓ True (found): {summary['inbound_readiness']['contact_form_true']} ({summary['inbound_readiness']['contact_form_true_pct']}%)")
    logger.info(f"    ✗ False (explicit absence): {summary['inbound_readiness']['contact_form_false']}")
    logger.info(f"    ? Unknown: {summary['inbound_readiness']['contact_form_unknown']}")
    logger.info(f"  Email:")
    logger.info(f"    ✓ Found: {summary['inbound_readiness']['email_found']} ({summary['inbound_readiness']['email_found_pct']}%)")
    logger.info(f"    ? Unknown: {summary['inbound_readiness']['email_unknown']} (never false)")
    
    logger.info("\n⚙️ Ops Maturity (Automated Scheduling):")
    logger.info(f"  ✓ Automated: {summary['ops_maturity']['automated']} ({summary['ops_maturity']['automated_pct']}%)")
    logger.info(f"  ✗ Manual (confirmed opportunity): {summary['ops_maturity']['manual_confirmed']} ({summary['ops_maturity']['manual_confirmed_pct']}%)")
    logger.info(f"  ? Unknown: {summary['ops_maturity']['unknown']}")
    
    logger.info("\n🌐 Website:")
    logger.info(f"  Has website: {summary['website']['has_website']} ({summary['website']['has_website_pct']}%)")
    logger.info(f"  Accessible: {summary['website']['accessible']} | Not accessible: {summary['website']['not_accessible']} | Unknown: {summary['website']['unknown']}")
    logger.info(f"  Has SSL: {summary['website']['has_ssl']}")
    logger.info(f"  Mobile friendly: {summary['website']['mobile_friendly']}")
    logger.info(f"  Has trust badges: {summary['website']['has_trust_badges']}")
    
    logger.info("\n⭐ Business Activity:")
    logger.info(f"  Has reviews: {summary['activity']['has_reviews']}")
    logger.info(f"  Active businesses: {summary['activity']['active_businesses']} ({summary['activity']['active_pct']}%)")
    logger.info(f"  Avg rating: {summary['activity']['avg_rating']}")
    logger.info(f"  Avg review count: {summary['activity']['avg_review_count']}")
    logger.info(f"  Avg days since review: {summary['activity']['avg_days_since_review']}")
    
    # Step 5: Save results
    output_path = save_enriched_leads(
        enriched_leads,
        signals,
        CONFIG["output_dir"],
        input_file
    )
    
    # Print sample
    logger.info("\n" + "=" * 60)
    logger.info("SAMPLE SIGNALS (first 3 leads)")
    logger.info("=" * 60)
    for signal in signals[:3]:
        logger.info(f"\n{signal.get('place_id', 'N/A')[:20]}...")
        logger.info(f"  Website: {signal.get('website_url', 'None')}")
        logger.info(f"  SSL: {signal.get('has_ssl')} | Mobile: {signal.get('mobile_friendly')} | Form: {signal.get('has_contact_form')}")
        logger.info(f"  Phone: {signal.get('phone_number', 'None')}")
        logger.info(f"  Rating: {signal.get('rating')} | Reviews: {signal.get('review_count')} | Last review: {signal.get('last_review_days_ago')} days ago")
    
    return signals


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Lead enrichment and signal extraction with context-first DB")
    parser.add_argument("--max-leads", type=int, default=None, help="Max leads to process (default: all)")
    parser.add_argument("--agency-type", choices=("seo", "marketing"), default=None, help="Agency context for Decision Agent (default: AGENCY_TYPE env or 'marketing')")
    parser.add_argument("--input", "-i", help="Input leads JSON (default: latest output/leads_*.json)")
    parser.add_argument("--place-ids", help="Path to file with place_ids (one per line or JSON array); only enrich these leads")
    parser.add_argument("--force-embed", action="store_true", help="Re-embed leads even if embedding already exists")
    args = parser.parse_args()
    
    CONFIG["max_leads"] = args.max_leads
    if args.agency_type is not None:
        CONFIG["agency_type"] = args.agency_type
    
    logger.info(f"Started at: {datetime.now().isoformat()}")
    
    # Check API key
    if not os.getenv("GOOGLE_PLACES_API_KEY"):
        logger.error(
            "GOOGLE_PLACES_API_KEY environment variable not set. "
            "Please set it before running."
        )
        sys.exit(1)
    
    # Run pipeline
    signals = run_enrichment_pipeline(
        input_file=args.input,
        max_leads=CONFIG["max_leads"],
        place_ids_file=args.place_ids,
        force_embed=args.force_embed,
    )
    
    logger.info(f"\nCompleted at: {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
