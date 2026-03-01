"""
Deterministic High-Value Service Intelligence configuration.
All thresholds and service definitions are centralized here.
"""

from __future__ import annotations

import os
from typing import Dict, List, Any


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


SERVICE_PAGE_MIN_WORDS_EXISTS = _env_int("NEYMA_SERVICE_PAGE_MIN_WORDS_EXISTS", 300)
DEPTH_WEAK_MAX_WORDS = _env_int("NEYMA_SERVICE_DEPTH_WEAK_MAX_WORDS", 499)
DEPTH_MODERATE_MIN_WORDS = _env_int("NEYMA_SERVICE_DEPTH_MODERATE_MIN_WORDS", 500)
DEPTH_STRONG_MIN_WORDS = _env_int("NEYMA_SERVICE_DEPTH_STRONG_MIN_WORDS", 900)
INTERNAL_LINKS_MIN_STRONG = _env_int("NEYMA_SERVICE_INTERNAL_LINKS_MIN_STRONG", 3)

# Early-stop limits for per-run service SERP validation requests.
SERVICE_SERP_MAX_QUERIES = _env_int("NEYMA_SERVICE_SERP_MAX_QUERIES", 16)
SERVICE_SERP_MAX_CONSECUTIVE_EMPTY = _env_int("NEYMA_SERVICE_SERP_MAX_CONSECUTIVE_EMPTY", 8)

# Summary/leverage thresholds.
STRONG_COVERAGE_RATIO_FOR_LOW_LEVERAGE = float(os.getenv("NEYMA_STRONG_COVERAGE_RATIO_FOR_LOW_LEVERAGE", "0.8"))
STRONG_SERP_RATIO_FOR_LOW_LEVERAGE = float(os.getenv("NEYMA_STRONG_SERP_RATIO_FOR_LOW_LEVERAGE", "0.6"))

SERVICE_PAGE_CONFIG: Dict[str, Any] = {
    "min_word_count": _env_int("NEYMA_SERVICE_PAGE_MIN_WORD_COUNT", 500),
    "min_keyword_density": _env_float("NEYMA_SERVICE_PAGE_MIN_KEYWORD_DENSITY", 0.02),
    "min_h2_sections": _env_int("NEYMA_SERVICE_PAGE_MIN_H2_SECTIONS", 2),
    "umbrella_service_threshold": _env_int("NEYMA_SERVICE_PAGE_UMBRELLA_SERVICE_THRESHOLD", 5),
    "min_internal_links": _env_int("NEYMA_SERVICE_PAGE_MIN_INTERNAL_LINKS", 1),
    "stub_word_count_max": _env_int("NEYMA_SERVICE_PAGE_STUB_WORD_COUNT_MAX", 300),
    "homepage_single_service_ratio": _env_float("NEYMA_SERVICE_PAGE_HOMEPAGE_SINGLE_SERVICE_RATIO", 0.70),
    "umbrella_meaningful_mentions": _env_int("NEYMA_SERVICE_PAGE_UMBRELLA_MEANINGFUL_MENTIONS", 2),
    "umbrella_top_similarity_ratio": _env_float("NEYMA_SERVICE_PAGE_UMBRELLA_TOP_SIMILARITY_RATIO", 0.60),
}


def _svc(
    slug: str,
    display_name: str,
    revenue_weight: int,
    aliases: List[str],
    min_word_threshold: int = DEPTH_STRONG_MIN_WORDS,
    min_internal_links: int = INTERNAL_LINKS_MIN_STRONG,
) -> Dict[str, Any]:
    return {
        "slug": slug,
        "display_name": display_name,
        "revenue_weight": int(revenue_weight),
        "min_word_threshold": int(min_word_threshold),
        "min_internal_links": int(min_internal_links),
        "aliases": aliases,
    }


HIGH_VALUE_SERVICES: Dict[str, List[Dict[str, Any]]] = {
    "dentist": [
        _svc("implants", "Implants", 5, ["implants", "implant", "dental implants"]),
        _svc("invisalign", "Invisalign", 4, ["invisalign", "clear aligners", "aligners"]),
        _svc("orthodontics", "Orthodontics", 4, ["orthodontics", "orthodontic", "braces"]),
        _svc("veneers", "Veneers", 4, ["veneers", "veneer", "porcelain veneers"]),
        _svc("cosmetic", "Cosmetic Dentistry", 3, ["cosmetic", "cosmetic dentistry", "smile makeover"]),
        _svc("all_on_4", "All-on-4", 5, ["all-on-4", "all on 4", "all on four"]),
        _svc(
            "full_mouth_reconstruction",
            "Full Mouth Reconstruction",
            5,
            ["full mouth reconstruction", "full-mouth reconstruction", "full mouth rehab"],
        ),
        _svc("emergency", "Emergency Dentistry", 3, ["emergency", "emergency dentist", "urgent dental"]),
        _svc("crowns", "Crowns", 3, ["crowns", "crown", "dental crowns"]),
        _svc("root_canal", "Root Canal", 3, ["root canal", "root canals", "endodontics"]),
        _svc("pediatric", "Pediatric Dentistry", 2, ["pediatric", "kids dentist", "children dentist"]),
        _svc("whitening", "Teeth Whitening", 2, ["whitening", "teeth whitening"]),
    ],
    "orthodontist": [
        _svc("invisalign", "Invisalign", 5, ["invisalign", "clear aligners", "aligners"]),
        _svc("orthodontics", "Orthodontics", 5, ["orthodontics", "braces"]),
        _svc("veneers", "Veneers", 2, ["veneers", "veneer"]),
    ],
    "med_spa": [
        _svc("botox", "Botox", 4, ["botox"]),
        _svc("fillers", "Dermal Fillers", 4, ["fillers", "dermal filler", "lip filler"]),
        _svc("iv_therapy", "IV Therapy", 3, ["iv therapy", "iv drip"]),
    ],
    "chiropractor": [
        _svc("acupuncture", "Acupuncture", 3, ["acupuncture"]),
    ],
}
