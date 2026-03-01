"""
OpenAI Moderation API integration for user-generated content.

Used to reject harmful or policy-violating input (e.g. Ask Neyma queries)
without echoing the content back. Requires OPENAI_API_KEY. If moderation
infrastructure is unavailable, requests are rejected (fail closed).
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Tuple

logger = logging.getLogger(__name__)

# Generic message shown when content is rejected; do not echo user input.
REJECTED_QUERY_MESSAGE = (
    "We can't process this query. Please use a location (city and state) and "
    "supported filters (e.g. review gap, website, missing service page)."
)

# Fast local guardrail for explicitly abusive/violent phrasing.
# This is not a full moderation system; it catches obvious high-risk inputs.
LOCAL_BLOCK_PATTERNS = [
    r"\bkill\b",
    r"\bmurder\b",
    r"\bmolest(?:or|ers?|ation)?\b",
    r"\bcsam\b",
    r"\bchild\s+porn(?:ography)?\b",
    r"\bchild\s+sexual\s+abuse\s+material\b",
    r"\bsexual\s+content\s+involving\s+minors?\b",
    r"\bminor\s+sexual\s+content\b",
    r"\bchild\s+exploitation\b",
    r"\bnigg(?:er|a|ers|as)\b",
    r"\bfag(?:got|gots)?\b",
    r"\bkike\b",
]
LOCAL_BLOCK_RE = re.compile("|".join(LOCAL_BLOCK_PATTERNS), re.IGNORECASE)


def _load_openai_key_from_env_file() -> str | None:
    """Best-effort .env loader fallback when python-dotenv isn't available."""
    try:
        project_root = Path(__file__).resolve().parents[2]
        env_path = project_root / ".env"
        if not env_path.exists():
            return None
        for line in env_path.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            if key.strip() != "OPENAI_API_KEY":
                continue
            val = value.strip().strip('"').strip("'")
            if val:
                os.environ.setdefault("OPENAI_API_KEY", val)
                return val
        return None
    except Exception:
        return None


def moderate_text(text: str) -> Tuple[bool, str | None]:
    """
    Run OpenAI Moderation on the given text.

    Returns:
        (is_safe, error_message): If is_safe is True, the text passed.
        If is_safe is False, error_message is the generic message to return to the user.
    """
    cleaned = str(text or "").strip()
    if not cleaned:
        return True, None

    if LOCAL_BLOCK_RE.search(cleaned):
        return False, REJECTED_QUERY_MESSAGE

    api_key = os.getenv("OPENAI_API_KEY") or _load_openai_key_from_env_file()
    if not api_key:
        logger.warning("OPENAI_API_KEY not set; moderation unavailable, rejecting")
        return False, "Safety checks are temporarily unavailable. Please try again."

    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("openai package not installed; moderation unavailable, rejecting")
        return False, "Safety checks are temporarily unavailable. Please try again."

    try:
        client = OpenAI()
        # Current moderation model family; can be overridden via env.
        moderation_model = (os.getenv("OPENAI_MODERATION_MODEL") or "omni-moderation-latest").strip()
        try:
            response = client.moderations.create(
                input=cleaned[:32_768],
                model=moderation_model,
            )
        except Exception as model_exc:
            # Compatibility fallback for SDK/API versions that reject the explicit model.
            if "Invalid value for 'model'" in str(model_exc):
                response = client.moderations.create(input=cleaned[:32_768])
            else:
                raise
    except Exception as e:
        logger.warning("Moderation API call failed: %s", e)
        return False, "Safety checks are temporarily unavailable. Please try again."

    results = getattr(response, "results", None)
    if not results:
        logger.warning("Moderation response missing results; rejecting")
        return False, "Safety checks are temporarily unavailable. Please try again."

    first = results[0] if isinstance(results, list) else results
    flagged = getattr(first, "flagged", False)
    if flagged:
        return False, REJECTED_QUERY_MESSAGE
    return True, None
