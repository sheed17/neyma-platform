#!/usr/bin/env python3
"""
Lightweight local lead sourcing pipeline.

Purpose:
- Search a region for local businesses using Google Places Nearby Search
- Deduplicate and optionally filter results
- Enrich only the fields needed for outreach sourcing:
  rating, review count, phone number, website
- Export a lightweight CSV + JSON without invoking the full app pipeline

Examples:
    python scripts/source_local_leads.py --region "San Jose, CA"
    python scripts/source_local_leads.py --region "San Jose, CA" --niche dentist
    python scripts/source_local_leads.py --region "San Jose, CA" --radius-km 18 --search-radius-km 2

Environment:
    GOOGLE_PLACES_API_KEY is required.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
except ImportError:
    pass

from pipeline.enrich import PlaceDetailsEnricher
from pipeline.export import export_to_csv, export_to_json
from pipeline.fetch import PlacesFetcher, get_keywords_for_niche
from pipeline.geo import estimate_api_calls, generate_geo_grid
from pipeline.normalize import deduplicate_places, filter_places, filter_practices_only, normalize_place


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
MINIMAL_PLACE_DETAILS_FIELDS = [
    "place_id",
    "website",
    "formatted_phone_number",
    "international_phone_number",
    "user_ratings_total",
    "rating",
    "url",
]


def geocode_region(region: str, api_key: str) -> Tuple[float, float, str]:
    """Resolve a human-readable region into lat/lng using Google Geocoding."""
    response = requests.get(
        GEOCODE_URL,
        params={"address": region, "key": api_key},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    status = data.get("status")
    if status != "OK" or not data.get("results"):
        raise ValueError(f"Could not geocode region '{region}' (status={status})")

    result = data["results"][0]
    location = result["geometry"]["location"]
    formatted_address = result.get("formatted_address") or region
    return float(location["lat"]), float(location["lng"]), formatted_address


def _slugify(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in value.strip())
    return "_".join(part for part in cleaned.split("_") if part)


def fetch_base_leads(
    region_label: str,
    center_lat: float,
    center_lng: float,
    city_radius_km: float,
    niche: str,
    search_radius_km: float,
    max_pages: int,
    use_keyword_expansion: bool,
    min_rating: Optional[float],
    min_reviews: Optional[int],
) -> List[Dict]:
    keywords = get_keywords_for_niche(niche) if use_keyword_expansion else [niche]
    grid_points = generate_geo_grid(center_lat, center_lng, city_radius_km, search_radius_km)
    estimate = estimate_api_calls(city_radius_km, search_radius_km, len(keywords), max_pages)

    logger.info("Region: %s", region_label)
    logger.info("Center: %.5f, %.5f", center_lat, center_lng)
    logger.info("Grid points: %s", len(grid_points))
    logger.info("Keywords: %s", ", ".join(keywords))
    logger.info(
        "Estimated max calls: %s | Estimated max raw results: %s",
        estimate["max_api_calls"],
        estimate["max_results_theoretical"],
    )

    fetcher = PlacesFetcher()
    raw_places: List[Dict] = []

    total_queries = len(grid_points) * len(keywords)
    query_count = 0
    for lat, lng, radius_m in grid_points:
        for keyword in keywords:
            query_count += 1
            if query_count % 10 == 0 or query_count == total_queries:
                logger.info("Fetch progress: %s/%s queries", query_count, total_queries)
            for place in fetcher.fetch_all_pages_for_query(lat, lng, radius_m, keyword, max_pages):
                raw_places.append(place)

    normalized = []
    for place in raw_places:
        try:
            normalized.append(normalize_place(place))
        except Exception as exc:
            logger.warning("Skipping place that failed normalization: %s", exc)

    deduped = deduplicate_places(normalized)
    if "dent" in niche.lower():
        deduped = filter_practices_only(deduped)

    deduped = filter_places(
        deduped,
        min_rating=min_rating,
        min_reviews=min_reviews,
        exclude_closed=True,
    )
    logger.info("Unique leads after cleanup: %s", len(deduped))
    return deduped


def enrich_minimal_fields(leads: List[Dict], progress_interval: int = 20) -> List[Dict]:
    """Enrich leads with only website, phone, rating, and review count."""
    enricher = PlaceDetailsEnricher()
    enriched_rows: List[Dict] = []

    for index, lead in enumerate(leads, start=1):
        details = enricher.get_place_details(
            lead["place_id"],
            fields=MINIMAL_PLACE_DETAILS_FIELDS,
        ) or {}
        row = {
            "place_id": lead.get("place_id"),
            "name": lead.get("name"),
            "address": lead.get("address"),
            "latitude": lead.get("latitude"),
            "longitude": lead.get("longitude"),
            "rating": details.get("rating", lead.get("rating")),
            "review_count": details.get("user_ratings_total", lead.get("user_ratings_total", 0)),
            "phone": details.get("formatted_phone_number") or details.get("international_phone_number"),
            "website": details.get("website"),
            "google_maps_url": details.get("url"),
            "business_status": lead.get("business_status"),
            "source": lead.get("source"),
        }
        enriched_rows.append(row)
        if index % progress_interval == 0 or index == len(leads):
            logger.info("Enrichment progress: %s/%s leads", index, len(leads))

    stats = enricher.get_stats()
    logger.info(
        "Place Details complete: %s calls | estimated cost $%s",
        stats["total_requests"],
        stats["estimated_cost_usd"],
    )
    return enriched_rows


def save_outputs(rows: List[Dict], output_dir: str, prefix: str, region_label: str, niche: str) -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    region_slug = _slugify(region_label)
    niche_slug = _slugify(niche)
    base_name = f"{prefix}_{niche_slug}_{region_slug}_{timestamp}"

    csv_path = os.path.join(output_dir, f"{base_name}.csv")
    json_path = os.path.join(output_dir, f"{base_name}.json")

    export_to_csv(
        rows,
        csv_path,
        fields=[
            "name",
            "address",
            "rating",
            "review_count",
            "phone",
            "website",
            "google_maps_url",
            "place_id",
            "latitude",
            "longitude",
            "business_status",
        ],
    )
    export_to_json(
        rows,
        json_path,
        include_metadata=True,
        metadata={
            "region": region_label,
            "niche": niche,
            "fields": ["name", "address", "rating", "review_count", "phone", "website"],
        },
    )
    return {"csv": csv_path, "json": json_path}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Source lightweight local leads for outbound.")
    parser.add_argument("--region", default="San Jose, CA", help='Region to geocode, e.g. "San Jose, CA"')
    parser.add_argument("--niche", default="dentist", help="Business niche to search")
    parser.add_argument("--radius-km", type=float, default=15.0, help="Radius around region center to cover")
    parser.add_argument("--search-radius-km", type=float, default=2.0, help="Radius for each tiled Nearby Search query")
    parser.add_argument("--max-pages", type=int, default=3, choices=[1, 2, 3], help="Google Nearby Search pages per query")
    parser.add_argument("--min-rating", type=float, default=None, help="Optional minimum Google rating")
    parser.add_argument("--min-reviews", type=int, default=None, help="Optional minimum review count")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap after cleanup, before enrichment")
    parser.add_argument("--no-keyword-expansion", action="store_true", help="Use only the exact niche term")
    parser.add_argument("--output-dir", default="output/sourcing", help="Directory for lightweight exports")
    parser.add_argument("--filename-prefix", default="local_leads", help="Output filename prefix")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        logger.error("GOOGLE_PLACES_API_KEY environment variable not set.")
        return 1

    center_lat, center_lng, region_label = geocode_region(args.region, api_key)
    leads = fetch_base_leads(
        region_label=region_label,
        center_lat=center_lat,
        center_lng=center_lng,
        city_radius_km=args.radius_km,
        niche=args.niche,
        search_radius_km=args.search_radius_km,
        max_pages=args.max_pages,
        use_keyword_expansion=not args.no_keyword_expansion,
        min_rating=args.min_rating,
        min_reviews=args.min_reviews,
    )

    if args.limit is not None:
        leads = leads[: args.limit]
        logger.info("Applied limit: %s leads", len(leads))

    if not leads:
        logger.warning("No leads found for the requested region.")
        return 0

    rows = enrich_minimal_fields(leads)
    rows.sort(key=lambda row: ((row.get("review_count") or 0), (row.get("rating") or 0)), reverse=True)
    outputs = save_outputs(rows, args.output_dir, args.filename_prefix, region_label, args.niche)

    logger.info("Finished. Exported %s leads.", len(rows))
    logger.info("CSV: %s", outputs["csv"])
    logger.info("JSON: %s", outputs["json"])
    logger.info("Sample:")
    for row in rows[:5]:
        logger.info(
            "  %s | rating=%s | reviews=%s | phone=%s | website=%s",
            row.get("name"),
            row.get("rating"),
            row.get("review_count"),
            row.get("phone"),
            row.get("website"),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
