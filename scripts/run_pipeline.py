#!/usr/bin/env python3
"""
Google Places Lead Extraction Agent

A production-ready pipeline for extracting business leads from Google Places API.
Implements geographic tiling, keyword expansion, pagination, and deduplication.

Usage:
    python scripts/run_pipeline.py

Environment Variables:
    GOOGLE_PLACES_API_KEY: Required. Your Google Places API key.

Example:
    export GOOGLE_PLACES_API_KEY="your-api-key"
    python scripts/run_pipeline.py
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env from project root so GOOGLE_PLACES_API_KEY (and others) work without exporting
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
except ImportError:
    pass

from pipeline.geo import generate_geo_grid, estimate_api_calls
from pipeline.fetch import PlacesFetcher, get_keywords_for_niche
from pipeline.normalize import (
    normalize_place,
    deduplicate_places,
    filter_practices_only,
    filter_places,
    get_place_summary
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

# San Jose, CA city center coordinates
CITY_CONFIG = {
    "name": "San Jose, CA",
    "center_lat": 37.3382,
    "center_lng": -121.8863,
    "radius_km": 15.0  # Cover ~15km radius from downtown
}

# Search configuration (dentist vertical for SEO agency opportunity intelligence)
SEARCH_CONFIG = {
    "niche": "dentist",           # Business niche: dentist | dental | hvac | plumber | etc.
    "search_radius_km": 2.0,      # Radius for each grid search (km)
    "max_pages_per_query": 3,     # Max pagination depth (1-3)
    "use_keyword_expansion": True  # Use multiple keywords per niche
}

# Output configuration
OUTPUT_CONFIG = {
    "output_dir": "output",
    "filename_prefix": "leads"
}


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def run_extraction(
    city_name: str,
    city_lat: float,
    city_lng: float,
    city_radius_km: float,
    niche: str,
    search_radius_km: float = 2.0,
    max_pages: int = 3,
    use_keyword_expansion: bool = True,
    min_rating: Optional[float] = None,
    min_reviews: Optional[int] = None
) -> List[Dict]:
    """
    Run the complete lead extraction pipeline.
    
    Args:
        city_name: Human-readable city name (for logging)
        city_lat: City center latitude
        city_lng: City center longitude
        city_radius_km: Radius of city area to cover
        niche: Business niche (e.g., "hvac", "plumber")
        search_radius_km: Radius for each grid search point
        max_pages: Maximum pages per query (1-3)
        use_keyword_expansion: Whether to use multiple keywords
        min_rating: Optional minimum rating filter
        min_reviews: Optional minimum review count filter
    
    Returns:
        List of normalized, deduplicated place dictionaries
    """
    logger.info("=" * 60)
    logger.info(f"Starting lead extraction for '{niche}' in {city_name}")
    logger.info("=" * 60)
    
    # Step 1: Generate geographic grid
    logger.info("Step 1: Generating geographic search grid...")
    grid_points = generate_geo_grid(
        city_lat, city_lng, city_radius_km, search_radius_km
    )
    logger.info(f"Generated {len(grid_points)} grid points")
    
    # Step 2: Get keywords for niche
    if use_keyword_expansion:
        keywords = get_keywords_for_niche(niche)
    else:
        keywords = [niche]
    logger.info(f"Using {len(keywords)} keywords: {keywords}")
    
    # Step 3: Estimate API calls
    estimate = estimate_api_calls(
        city_radius_km, search_radius_km, len(keywords), max_pages
    )
    logger.info(f"Estimated max API calls: {estimate['max_api_calls']}")
    logger.info(f"Estimated max results: {estimate['max_results_theoretical']}")
    
    # Step 4: Initialize fetcher
    try:
        fetcher = PlacesFetcher()
    except ValueError as e:
        logger.error(f"Failed to initialize fetcher: {e}")
        return []
    
    # Step 5: Fetch places from all grid points
    logger.info("Step 2: Fetching places from Google Places API...")
    all_places = []
    
    total_queries = len(grid_points) * len(keywords)
    query_count = 0
    
    for lat, lng, radius_m in grid_points:
        for keyword in keywords:
            query_count += 1
            
            if query_count % 10 == 0:
                stats = fetcher.get_stats()
                logger.info(
                    f"Progress: {query_count}/{total_queries} queries, "
                    f"{stats['total_requests']} API calls, "
                    f"{len(all_places)} places collected"
                )
            
            # Fetch all pages for this query
            for place in fetcher.fetch_all_pages_for_query(
                lat, lng, radius_m, keyword, max_pages
            ):
                all_places.append(place)
    
    stats = fetcher.get_stats()
    logger.info(f"Fetching complete: {stats['total_requests']} total API calls")
    logger.info(f"Raw places collected: {len(all_places)}")
    
    # Step 6: Normalize places
    logger.info("Step 3: Normalizing place data...")
    normalized_places = []
    for place in all_places:
        try:
            normalized_places.append(normalize_place(place))
        except Exception as e:
            logger.warning(f"Failed to normalize place: {e}")
    
    logger.info(f"Normalized {len(normalized_places)} places")
    
    # Step 7: Deduplicate
    logger.info("Step 4: Deduplicating places...")
    unique_places = deduplicate_places(normalized_places)
    logger.info(f"Unique places after deduplication: {len(unique_places)}")

    # Step 7b: Dental-only practice filter (exclude individual practitioner listings)
    if "dent" in (niche or "").strip().lower():
        logger.info("Step 4b: Filtering to dental practice listings only...")
        unique_places = filter_practices_only(unique_places)
        logger.info(f"Practice listings remaining: {len(unique_places)}")

    # Step 8: Optional filtering
    if min_rating is not None or min_reviews is not None:
        logger.info("Step 5: Applying filters...")
        unique_places = filter_places(
            unique_places,
            min_rating=min_rating,
            min_reviews=min_reviews,
            exclude_closed=True
        )
        logger.info(f"Places after filtering: {len(unique_places)}")
    
    # Step 9: Generate summary
    summary = get_place_summary(unique_places)
    logger.info("=" * 60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total unique leads: {summary['total']}")
    logger.info(f"With ratings: {summary['with_rating']}")
    if summary['avg_rating']:
        logger.info(f"Average rating: {summary['avg_rating']:.2f}")
    logger.info(f"Total reviews across all: {summary['total_reviews']}")
    logger.info(f"Status breakdown: {summary['by_status']}")
    
    return unique_places


def save_results(
    places: List[Dict],
    output_dir: str,
    filename_prefix: str,
    city_name: str,
    niche: str
) -> str:
    """
    Save extraction results to JSON file.
    
    Args:
        places: List of place dictionaries
        output_dir: Output directory path
        filename_prefix: Prefix for output filename
        city_name: City name (for filename)
        niche: Business niche (for filename)
    
    Returns:
        Path to saved file
    """
    # Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    city_slug = city_name.lower().replace(",", "").replace(" ", "_")
    filename = f"{filename_prefix}_{niche}_{city_slug}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    # Save to JSON
    output_data = {
        "metadata": {
            "city": city_name,
            "niche": niche,
            "extracted_at": datetime.utcnow().isoformat(),
            "total_leads": len(places)
        },
        "leads": places
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"Results saved to: {filepath}")
    return filepath


def main():
    """Main entry point for the lead extraction pipeline."""
    logger.info("Google Places Lead Extraction Agent")
    logger.info(f"Started at: {datetime.now().isoformat()}")
    
    # Check for API key
    if not os.getenv("GOOGLE_PLACES_API_KEY"):
        logger.error(
            "GOOGLE_PLACES_API_KEY environment variable not set. "
            "Please set it before running."
        )
        sys.exit(1)
    
    # Run extraction
    places = run_extraction(
        city_name=CITY_CONFIG["name"],
        city_lat=CITY_CONFIG["center_lat"],
        city_lng=CITY_CONFIG["center_lng"],
        city_radius_km=CITY_CONFIG["radius_km"],
        niche=SEARCH_CONFIG["niche"],
        search_radius_km=SEARCH_CONFIG["search_radius_km"],
        max_pages=SEARCH_CONFIG["max_pages_per_query"],
        use_keyword_expansion=SEARCH_CONFIG["use_keyword_expansion"]
    )
    
    if places:
        # Save results
        filepath = save_results(
            places,
            output_dir=OUTPUT_CONFIG["output_dir"],
            filename_prefix=OUTPUT_CONFIG["filename_prefix"],
            city_name=CITY_CONFIG["name"],
            niche=SEARCH_CONFIG["niche"]
        )
        
        # Print sample output
        logger.info("\n" + "=" * 60)
        logger.info("SAMPLE LEADS (first 5)")
        logger.info("=" * 60)
        for place in places[:5]:
            logger.info(
                f"  - {place['name']} | "
                f"Rating: {place.get('rating', 'N/A')} | "
                f"Reviews: {place.get('user_ratings_total', 0)} | "
                f"{place.get('address', 'No address')}"
            )
    else:
        logger.warning("No places extracted. Check API key and search parameters.")
    
    logger.info(f"\nCompleted at: {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
