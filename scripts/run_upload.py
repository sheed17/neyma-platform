#!/usr/bin/env python3
"""
Enrich team-uploaded leads (CSV or JSON) with the same pipeline as sourced leads.

Teams can upload existing leads (name, optional website, phone, address, place_id)
and get: website signals, Meta Ads check, Decision Agent verdict + reasoning, and DB persistence.
Export and list_runs work the same; runs are tagged source="upload".

Usage:
    python scripts/run_upload.py --upload path/to/leads.csv
    python scripts/run_upload.py --upload path/to/leads.json --max-leads 50 --agency-type seo

Input format:
    CSV or JSON. Required column: name.
    Optional: website, phone, address, place_id (and common variants).
    With place_id: we fetch Place Details (Google API). Without: we use your website/phone only.
"""

import os
import sys
import uuid
import logging
import argparse
from datetime import datetime
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env from project root so META_ACCESS_TOKEN, GOOGLE_PLACES_API_KEY work without exporting
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
except ImportError:
    pass

from pipeline.upload import (
    load_uploaded_leads,
    build_synthetic_place_details,
)
from pipeline.enrich import PlaceDetailsEnricher
from pipeline.signals import extract_signals_batch, merge_signals_into_lead
from pipeline.meta_ads import get_meta_access_token, augment_lead_with_meta_ads
from pipeline.semantic_signals import build_semantic_signals
from pipeline.decision_agent import DecisionAgent
from pipeline.db import (
    init_db,
    create_run,
    insert_lead,
    insert_lead_signals,
    insert_decision,
    update_run_completed,
    update_run_failed,
)
from pipeline.embedding_store import store_lead_embedding_if_eligible

def _compute_run_stats(signals: List[Dict]) -> Dict:
    """Run stats for DB (same as run_enrichment)."""
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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _ensure_place_id(lead: Dict) -> None:
    """Ensure lead has a unique place_id for DB (required)."""
    if lead.get("place_id"):
        return
    lead["place_id"] = "upload:" + uuid.uuid4().hex


def _enrich_uploaded_leads(leads: list, fetch_place_details: bool) -> list:
    """
    Enrich uploaded leads: fetch Place Details when place_id present (and fetch_place_details),
    otherwise set synthetic _place_details from row. Add rating/user_ratings_total for signals.
    """
    # Assign upload place_ids before checking who has real place_id
    for lead in leads:
        _ensure_place_id(lead)
    has_real_place_id = any(
        lead.get("place_id") and not str(lead.get("place_id", "")).startswith("upload:")
        for lead in leads
    )
    enricher = None
    if fetch_place_details and has_real_place_id:
        try:
            enricher = PlaceDetailsEnricher()
        except ValueError:
            logger.warning("GOOGLE_PLACES_API_KEY not set; skipping Place Details for uploaded leads with place_id")

    enriched = []
    for lead in leads:
        place_id = lead.get("place_id", "")
        if place_id and not place_id.startswith("upload:") and enricher:
            enricher.enrich_lead(lead)
        if "_place_details" not in lead:
            lead["_place_details"] = build_synthetic_place_details(lead)
        if lead.get("rating") is None:
            lead["rating"] = None
        if lead.get("user_ratings_total") is None:
            lead["user_ratings_total"] = 0
        if lead.get("address") is None and lead.get("formatted_address"):
            lead["address"] = lead["formatted_address"]
        enriched.append(lead)
    return enriched


def run_upload_pipeline(
    upload_path: str,
    max_leads: int = None,
    agency_type: str = "marketing",
    fetch_place_details: bool = True,
    progress_interval: int = 10,
) -> int:
    """Load uploaded leads, enrich, extract signals, score, persist to DB. Returns number of leads processed."""
    logger.info("=" * 60)
    logger.info("Uploaded Leads Enrichment")
    logger.info("=" * 60)

    leads = load_uploaded_leads(upload_path)
    if not leads:
        logger.warning("No leads found in %s", upload_path)
        return 0
    if max_leads:
        leads = leads[:max_leads]
        logger.info("Limited to %d leads", max_leads)

    logger.info("Step 1: Enrich (Place Details where place_id present, else use uploaded fields)...")
    enriched_leads = _enrich_uploaded_leads(leads, fetch_place_details=fetch_place_details)

    logger.info("Step 2: Extract signals (website analysis, phone, reviews)...")
    signals = extract_signals_batch(enriched_leads, progress_interval=progress_interval)

    if agency_type not in ("seo", "marketing"):
        agency_type = "marketing"
    use_meta_ads = get_meta_access_token() is not None
    if use_meta_ads:
        logger.info("META_ACCESS_TOKEN set — augmenting with Meta Ads Library")
    logger.info("Decision Agent: agency_type=%s", agency_type)

    run_id = create_run({
        "source": "upload",
        "upload_file": upload_path,
        "max_leads": max_leads,
        "agency_type": agency_type,
        "fetch_place_details": fetch_place_details,
    })
    agent = DecisionAgent(agency_type=agency_type)

    try:
        for idx, (lead, signal) in enumerate(zip(enriched_leads, signals)):
            merged = merge_signals_into_lead(lead, signal)
            if use_meta_ads:
                augment_lead_with_meta_ads(merged)
            lead_id = insert_lead(run_id, merged)
            insert_lead_signals(lead_id, signal)
            semantic = build_semantic_signals(merged)
            decision = agent.decide(semantic, lead_name=lead.get("name") or "")
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
            store_lead_embedding_if_eligible(lead_id, merged, force_embed=False)
            if (idx + 1) % progress_interval == 0:
                logger.info("  Processed %d/%d leads", idx + 1, len(enriched_leads))
        run_stats = _compute_run_stats(signals)
        update_run_completed(run_id, len(enriched_leads), run_stats=run_stats)
        logger.info("Run %s completed; %d leads persisted to DB", run_id[:8], len(enriched_leads))
    except Exception:
        update_run_failed(run_id)
        raise

    logger.info("Export with: python scripts/export_leads.py")
    return len(enriched_leads)


def main():
    parser = argparse.ArgumentParser(
        description="Enrich team-uploaded leads (CSV/JSON) with signals, Meta Ads, Decision Agent; persist to DB"
    )
    parser.add_argument("--upload", "-u", required=True, help="Path to CSV or JSON file (required: name column)")
    parser.add_argument("--max-leads", type=int, default=None, help="Max leads to process")
    parser.add_argument("--agency-type", choices=("seo", "marketing"), default=None, help="Agency context for Decision Agent (default: AGENCY_TYPE env or 'marketing')")
    parser.add_argument(
        "--no-place-details",
        action="store_true",
        help="Do not call Google Place Details API (use only uploaded website/phone)",
    )
    args = parser.parse_args()

    if not os.path.isfile(args.upload):
        logger.error("File not found: %s", args.upload)
        sys.exit(1)

    # GOOGLE_PLACES_API_KEY only required if we fetch Place Details for rows with place_id
    if not args.no_place_details and not os.getenv("GOOGLE_PLACES_API_KEY"):
        logger.warning("GOOGLE_PLACES_API_KEY not set; use --no-place-details to run without Place Details")

    agency_type = os.getenv("AGENCY_TYPE", "marketing").lower() or "marketing"
    if args.agency_type is not None:
        agency_type = args.agency_type
    n = run_upload_pipeline(
        upload_path=args.upload,
        max_leads=args.max_leads,
        agency_type=agency_type,
        fetch_place_details=not args.no_place_details,
    )
    logger.info("Done. %d leads processed.", n)


if __name__ == "__main__":
    main()
