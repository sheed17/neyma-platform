"""
Place data normalization and deduplication module.

Transforms raw Google Places API responses into clean, DB-ready records.
"""

from typing import List, Dict, Optional, Set
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

PRACTICE_KEYWORDS = (
    "dental",
    "dentistry",
    "group",
    "family",
    "smile",
    "clinic",
    "center",
    "associates",
    "care",
    "&",
)


def normalize_place(place_json: Dict) -> Dict:
    """
    Transform a raw Google Places API response into a clean, structured record.
    
    Extracts and normalizes key fields into a consistent schema suitable
    for database storage or further processing.
    
    Args:
        place_json: Raw place dictionary from Google Places API
    
    Returns:
        Normalized place dictionary with consistent schema
    """
    # Extract location coordinates
    geometry = place_json.get("geometry", {})
    location = geometry.get("location", {})
    
    # Extract opening hours if available
    opening_hours = place_json.get("opening_hours", {})
    
    # Build normalized record
    normalized = {
        # Primary identifier
        "place_id": place_json.get("place_id"),
        
        # Business information
        "name": place_json.get("name"),
        "address": place_json.get("vicinity") or place_json.get("formatted_address"),
        "types": place_json.get("types", []),
        
        # Location
        "latitude": location.get("lat"),
        "longitude": location.get("lng"),
        
        # Ratings and reviews
        "rating": place_json.get("rating"),
        "user_ratings_total": place_json.get("user_ratings_total", 0),
        
        # Business status
        "business_status": place_json.get("business_status"),
        "is_open_now": opening_hours.get("open_now"),
        
        # Price level (0-4, where 0 is free and 4 is very expensive)
        "price_level": place_json.get("price_level"),
        
        # Photos (store reference for later retrieval if needed)
        "photo_reference": None,
        "photo_count": 0,
        
        # Metadata
        "fetched_at": datetime.utcnow().isoformat(),
        "source": "google_places_nearby"
    }
    
    # Extract photo reference if available
    photos = place_json.get("photos", [])
    if photos:
        normalized["photo_reference"] = photos[0].get("photo_reference")
        normalized["photo_count"] = len(photos)
    
    return normalized


def normalize_places(places: List[Dict]) -> List[Dict]:
    """
    Normalize a list of places.
    
    Args:
        places: List of raw place dictionaries
    
    Returns:
        List of normalized place dictionaries
    """
    normalized = []
    for place in places:
        try:
            normalized.append(normalize_place(place))
        except Exception as e:
            logger.warning(f"Failed to normalize place: {e}")
            continue
    return normalized


def deduplicate_places(places: List[Dict]) -> List[Dict]:
    """
    Remove duplicate places based on place_id.
    
    Keeps the first occurrence of each unique place_id.
    
    Args:
        places: List of place dictionaries (raw or normalized)
    
    Returns:
        Deduplicated list of places
    """
    seen_ids: Set[str] = set()
    unique_places = []
    
    for place in places:
        place_id = place.get("place_id")
        if place_id and place_id not in seen_ids:
            seen_ids.add(place_id)
            unique_places.append(place)
    
    duplicates_removed = len(places) - len(unique_places)
    if duplicates_removed > 0:
        logger.info(f"Removed {duplicates_removed} duplicate places")
    
    return unique_places


def is_likely_practice(place: Dict) -> bool:
    """
    Heuristic classifier for dental listings.

    Returns True for practice-level listings, False for likely individual
    practitioner listings (e.g., "Dr. Jane Smith, DDS").
    """
    name = str(place.get("name") or "").strip()
    if not name:
        return True

    lower_name = name.lower()
    has_practice_keyword = any(keyword in lower_name for keyword in PRACTICE_KEYWORDS)
    starts_with_dr = lower_name.startswith("dr.") or lower_name.startswith("dr ")

    if starts_with_dr and not has_practice_keyword:
        return False
    return True


def filter_practices_only(places: List[Dict]) -> List[Dict]:
    """Keep only listings classified as practice-level."""
    kept: List[Dict] = []
    removed = 0
    for place in places:
        if is_likely_practice(place):
            kept.append(place)
        else:
            removed += 1

    if removed > 0:
        logger.info(
            "Filtered out %s individual practitioner listings; %s practices remaining",
            removed,
            len(kept),
        )
    return kept


def filter_places(
    places: List[Dict],
    min_rating: Optional[float] = None,
    min_reviews: Optional[int] = None,
    exclude_closed: bool = False,
    required_types: Optional[List[str]] = None
) -> List[Dict]:
    """
    Filter places based on various criteria.
    
    Args:
        places: List of normalized place dictionaries
        min_rating: Minimum rating threshold (1.0-5.0)
        min_reviews: Minimum number of reviews
        exclude_closed: If True, exclude permanently closed businesses
        required_types: List of required place types (any match passes)
    
    Returns:
        Filtered list of places
    """
    filtered = []
    
    for place in places:
        # Check rating threshold
        if min_rating is not None:
            rating = place.get("rating")
            if rating is None or rating < min_rating:
                continue
        
        # Check review count threshold
        if min_reviews is not None:
            reviews = place.get("user_ratings_total", 0)
            if reviews < min_reviews:
                continue
        
        # Check business status
        if exclude_closed:
            status = place.get("business_status")
            if status in ("CLOSED_PERMANENTLY", "CLOSED_TEMPORARILY"):
                continue
        
        # Check required types
        if required_types:
            place_types = set(place.get("types", []))
            if not place_types.intersection(required_types):
                continue
        
        filtered.append(place)
    
    removed = len(places) - len(filtered)
    if removed > 0:
        logger.info(f"Filtered out {removed} places based on criteria")
    
    return filtered


def enrich_place(place: Dict, additional_data: Dict) -> Dict:
    """
    Enrich a place record with additional data.
    
    Useful for adding data from Place Details API or other sources.
    
    Args:
        place: Normalized place dictionary
        additional_data: Additional fields to merge
    
    Returns:
        Enriched place dictionary
    """
    enriched = place.copy()
    enriched.update(additional_data)
    enriched["enriched_at"] = datetime.utcnow().isoformat()
    return enriched


def get_place_summary(places: List[Dict]) -> Dict:
    """
    Generate summary statistics for a list of places.
    
    Args:
        places: List of normalized place dictionaries
    
    Returns:
        Summary statistics dictionary
    """
    if not places:
        return {"total": 0}
    
    ratings = [p.get("rating") for p in places if p.get("rating") is not None]
    reviews = [p.get("user_ratings_total", 0) for p in places]
    
    # Count by business status
    status_counts = {}
    for place in places:
        status = place.get("business_status", "UNKNOWN")
        status_counts[status] = status_counts.get(status, 0) + 1
    
    return {
        "total": len(places),
        "with_rating": len(ratings),
        "avg_rating": sum(ratings) / len(ratings) if ratings else None,
        "min_rating": min(ratings) if ratings else None,
        "max_rating": max(ratings) if ratings else None,
        "total_reviews": sum(reviews),
        "avg_reviews": sum(reviews) / len(reviews) if reviews else 0,
        "by_status": status_counts
    }
