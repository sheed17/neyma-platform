"""
Resolve business to place_id from website URL or business_name + city.
Uses Google Places Find Place (Legacy), Text Search (Legacy), Geocoding, and Nearby Search.
"""

import os
import logging
from typing import Dict, Optional, Tuple
import requests

logger = logging.getLogger(__name__)

FIND_PLACE_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def _normalize_domain(website: str) -> str:
    """Extract and normalize domain from website URL."""
    url = (website or "").strip().lower()
    if not url:
        return ""
    # Remove protocol
    for prefix in ("https://", "http://", "www."):
        if url.startswith(prefix):
            url = url[len(prefix) :]
            break
    if url.startswith("www."):
        url = url[4:]
    # Remove path and query
    url = url.split("/")[0].split("?")[0]
    return url.strip()


def _get_api_key() -> str:
    key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not key:
        raise ValueError("GOOGLE_PLACES_API_KEY environment variable is required")
    return key


def _place_to_lead(place: Dict) -> Dict:
    """Convert raw Places API result to lead-like dict."""
    geometry = place.get("geometry") or {}
    location = geometry.get("location") or {}
    return {
        "place_id": place.get("place_id"),
        "name": place.get("name"),
        "address": place.get("vicinity") or place.get("formatted_address"),
        "formatted_address": place.get("formatted_address"),
        "latitude": location.get("lat"),
        "longitude": location.get("lng"),
        "rating": place.get("rating"),
        "user_ratings_total": place.get("user_ratings_total", 0),
    }


def resolve_from_website(website: str) -> Optional[Dict]:
    """
    Resolve place from website URL.

    Strategy:
    1. Text Search with domain — often succeeds when domain is indexed with the place.
    2. Find Place from Text with domain — fallback (designed for name/address/phone, not URL).

    Returns lead-like dict with place_id, name, latitude, longitude, formatted_address, etc.
    Returns None if not found.
    """
    domain = _normalize_domain(website)
    if not domain:
        return None
    key = _get_api_key()

    # 1) Text Search with domain — more forgiving for domain-like queries
    try:
        params = {"query": domain, "key": key}
        resp = requests.get(TEXT_SEARCH_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")
        if status == "OK":
            results = data.get("results") or []
            if results:
                return _place_to_lead(results[0])
        if status == "REQUEST_DENIED":
            logger.error("Text Search denied: %s", data.get("error_message", ""))
    except Exception as e:
        logger.warning("Text Search for website failed: %s", e)

    # 2) Fallback: Find Place from Text
    try:
        params = {
            "input": domain,
            "inputtype": "textquery",
            "fields": "place_id,name,formatted_address,geometry,vicinity",
            "key": key,
        }
        resp = requests.get(FIND_PLACE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")
        if status == "OK":
            candidates = data.get("candidates") or []
            if candidates:
                return _place_to_lead(candidates[0])
    except Exception as e:
        logger.warning("Find Place for website failed: %s", e)

    return None


def _geocode_city(city: str, state: Optional[str] = None) -> Optional[Tuple[float, float]]:
    """Geocode city name to (lat, lng). Prefer US if ambiguous."""
    key = _get_api_key()
    if state:
        address = f"{city}, {state}, USA"
    elif "," not in city:
        address = f"{city}, USA"
    else:
        address = city
    params = {"address": address, "key": key}
    try:
        resp = requests.get(GEOCODE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") != "OK":
            return None
        results = data.get("results") or []
        if not results:
            return None
        loc = results[0].get("geometry", {}).get("location", {})
        lat = loc.get("lat")
        lng = loc.get("lng")
        if lat is not None and lng is not None:
            return (float(lat), float(lng))
        return None
    except Exception as e:
        logger.exception("Geocode failed: %s", e)
        raise


def resolve_from_name_city(business_name: str, city: str, state: Optional[str] = None) -> Optional[Dict]:
    """
    Resolve place from business_name + city + optional state using Geocode + Nearby Search.
    Returns lead-like dict with place_id, name, etc. Returns best match.
    """
    coords = _geocode_city(city, state=state)
    if not coords:
        return None
    lat, lng = coords
    key = _get_api_key()
    params = {
        "location": f"{lat},{lng}",
        "radius": 50000,
        "keyword": business_name.strip(),
        "key": key,
    }
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")
        if status in ("ZERO_RESULTS", "REQUEST_DENIED"):
            return None
        if status != "OK":
            return None
        results = data.get("results") or []
        if not results:
            return None
        place = results[0]
        return _place_to_lead(place)
    except Exception as e:
        logger.exception("resolve_from_name_city failed: %s", e)
        raise
