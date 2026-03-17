"""
Google Places Place Details enrichment module.

Fetches additional business details while minimizing API costs
by requesting only the fields we need.

Cost Optimization:
- Google charges based on field categories requested
- Basic fields: Free (included with any request)
- Contact fields (website, phone): $3 per 1,000
- Atmosphere fields (reviews): $5 per 1,000
- Requesting all fields: $17 per 1,000

By using field masks, we pay ~$8 per 1,000 instead of $17.
That's a 53% cost reduction.
"""

import os
import time
import logging
from typing import Dict, Optional, List
import requests

logger = logging.getLogger(__name__)

# API Configuration
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# Fields to request (cost-optimized selection)
# Contact fields ($3/1000): website, formatted_phone_number, international_phone_number
# Atmosphere fields ($5/1000): reviews
# Total: $8/1000 vs $17/1000 for all fields
REQUIRED_FIELDS = [
    "place_id",
    "website",
    "formatted_phone_number",
    "international_phone_number",
    "reviews",
    "user_ratings_total",
    "rating",
    "url",  # Google Maps URL (basic, free)
]

# Rate limiting
REQUEST_DELAY = 0.1  # 100ms between requests
MAX_RETRIES = 3
BACKOFF_FACTOR = 2


class PlaceDetailsEnricher:
    """
    Fetches Place Details with cost-optimized field selection.
    
    Attributes:
        api_key: Google Places API key
        request_count: Total API requests made
        total_cost_estimate: Running cost estimate in dollars
    """
    
    # Cost per 1000 requests by field category
    COST_PER_1000 = {
        "contact": 3.0,   # website, phone
        "atmosphere": 5.0  # reviews
    }
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the enricher with an API key.
        
        Args:
            api_key: Google Places API key. If None, reads from 
                     GOOGLE_PLACES_API_KEY environment variable.
        """
        self.api_key = api_key or os.getenv("GOOGLE_PLACES_API_KEY")
        if not self.api_key:
            raise ValueError(
                "No API key provided. Set GOOGLE_PLACES_API_KEY environment "
                "variable or pass api_key parameter."
            )
        
        self.request_count = 0
        self.session = requests.Session()
    
    def _make_request(
        self,
        place_id: str,
        fields: List[str],
        retry_count: int = 0
    ) -> Optional[Dict]:
        """
        Make a single Place Details API request with retry logic.
        
        Args:
            place_id: Google Places place_id
            fields: List of fields to request
            retry_count: Current retry attempt
        
        Returns:
            API response dict or None on failure
        """
        params = {
            "place_id": place_id,
            "fields": ",".join(fields),
            "key": self.api_key
        }
        
        try:
            time.sleep(REQUEST_DELAY)
            
            response = self.session.get(
                PLACE_DETAILS_URL,
                params=params,
                timeout=30
            )
            self.request_count += 1
            
            response.raise_for_status()
            data = response.json()
            
            status = data.get("status")
            
            if status == "OK":
                return data.get("result", {})
            elif status == "ZERO_RESULTS":
                logger.debug(f"No details found for place_id: {place_id}")
                return {}
            elif status == "OVER_QUERY_LIMIT":
                if retry_count < MAX_RETRIES:
                    wait_time = BACKOFF_FACTOR ** retry_count * 5
                    logger.warning(f"Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    return self._make_request(place_id, fields, retry_count + 1)
                logger.error("Max retries exceeded for rate limit")
                return None
            elif status == "REQUEST_DENIED":
                logger.error(f"Request denied: {data.get('error_message')}")
                return None
            elif status == "INVALID_REQUEST":
                logger.error(f"Invalid request: {data.get('error_message')}")
                return None
            elif status == "NOT_FOUND":
                logger.warning(f"Place not found: {place_id}")
                return None
            else:
                logger.warning(f"Unexpected status: {status}")
                return data.get("result")
                
        except requests.exceptions.Timeout:
            if retry_count < MAX_RETRIES:
                wait_time = BACKOFF_FACTOR ** retry_count
                logger.warning(f"Timeout. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                return self._make_request(place_id, fields, retry_count + 1)
            logger.error("Max retries exceeded for timeout")
            return None
            
        except requests.exceptions.RequestException as e:
            if retry_count < MAX_RETRIES:
                wait_time = BACKOFF_FACTOR ** retry_count
                logger.warning(f"Request error: {e}. Retrying...")
                time.sleep(wait_time)
                return self._make_request(place_id, fields, retry_count + 1)
            logger.error(f"Max retries exceeded. Last error: {e}")
            return None
    
    def get_place_details(
        self,
        place_id: str,
        fields: Optional[List[str]] = None
    ) -> Optional[Dict]:
        """
        Fetch Place Details for a single place.
        
        Args:
            place_id: Google Places place_id
            fields: Optional custom field list (default: REQUIRED_FIELDS)
        
        Returns:
            Place details dict or None on failure
        """
        fields = fields or REQUIRED_FIELDS
        return self._make_request(place_id, fields)
    
    def enrich_lead(self, lead: Dict) -> Dict:
        """
        Enrich a lead with Place Details data.
        
        Merges Place Details into the existing lead dict.
        
        Args:
            lead: Lead dictionary with at least 'place_id'
        
        Returns:
            Enriched lead dictionary
        """
        place_id = lead.get("place_id")
        if not place_id:
            logger.warning("Lead missing place_id, skipping enrichment")
            return lead
        
        details = self.get_place_details(place_id)
        
        if details:
            enriched = lead.copy()
            enriched["_place_details"] = {
                "website": details.get("website"),
                "formatted_phone_number": details.get("formatted_phone_number"),
                "international_phone_number": details.get("international_phone_number"),
                "reviews": details.get("reviews", []),
                "google_maps_url": details.get("url"),
            }
            if details.get("user_ratings_total") is not None:
                enriched["user_ratings_total"] = details["user_ratings_total"]
            if details.get("rating") is not None:
                enriched["rating"] = details["rating"]
            return enriched
        
        return lead
    
    def enrich_leads_batch(
        self,
        leads: List[Dict],
        progress_interval: int = 10
    ) -> List[Dict]:
        """
        Enrich multiple leads with Place Details.
        
        Args:
            leads: List of lead dictionaries
            progress_interval: Log progress every N leads
        
        Returns:
            List of enriched lead dictionaries
        """
        enriched_leads = []
        total = len(leads)
        
        for i, lead in enumerate(leads, 1):
            enriched = self.enrich_lead(lead)
            enriched_leads.append(enriched)
            
            if i % progress_interval == 0:
                logger.info(f"Enriched {i}/{total} leads ({self.request_count} API calls)")
        
        logger.info(f"Enrichment complete: {self.request_count} API calls")
        return enriched_leads
    
    def get_stats(self) -> Dict:
        """Return enrichment statistics and cost estimate."""
        # Cost estimate: $8 per 1000 (Contact + Atmosphere fields)
        cost_per_request = 8.0 / 1000
        estimated_cost = self.request_count * cost_per_request
        
        return {
            "total_requests": self.request_count,
            "estimated_cost_usd": round(estimated_cost, 4),
            "cost_per_1000": 8.0,
            "savings_vs_all_fields": "53% ($8 vs $17 per 1000)"
        }
