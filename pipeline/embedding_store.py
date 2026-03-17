"""
Shared embedding storage: store lead embedding when eligible (objective_intelligence present).
Used by run_enrichment, diagnostic API, and optionally run_upload.
Safe operation: never raises; logs and continues on failure.
"""

from typing import Dict, Any
import logging

from pipeline.db import (
    get_lead_embedding_v2,
    insert_lead_embedding_v2,
)
from pipeline.embedding_snapshot import build_embedding_snapshot_v1
from pipeline.embeddings import get_embedding

logger = logging.getLogger(__name__)


def store_lead_embedding_if_eligible(
    lead_id: int,
    lead: Dict[str, Any],
    force_embed: bool = False,
) -> None:
    """
    Store embedding for dental leads that have objective_intelligence.

    Safe operation:
    • Never raises exceptions upward
    • Skips if embedding already exists unless force_embed=True
    • Skips if objective_intelligence missing
    """
    try:
        if not lead:
            return

        if not lead.get("objective_intelligence"):
            return

        existing = get_lead_embedding_v2(
            lead_id,
            embedding_version="v1_structural",
            embedding_type="objective_state",
        )

        if existing and not force_embed:
            return

        snapshot = build_embedding_snapshot_v1(lead)

        if not snapshot:
            logger.warning("Embedding snapshot empty for lead_id=%s", lead_id)
            return

        embedding = get_embedding(snapshot)

        if not embedding:
            logger.warning("Embedding generation failed for lead_id=%s", lead_id)
            return

        insert_lead_embedding_v2(
            lead_id=lead_id,
            embedding=embedding,
            text=snapshot,
            embedding_version="v1_structural",
            embedding_type="objective_state",
        )

        logger.info("Stored embedding for lead_id=%s", lead_id)

    except Exception as e:
        logger.exception("Embedding storage failed for lead_id=%s: %s", lead_id, e)
