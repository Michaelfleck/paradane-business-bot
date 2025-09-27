import os
import time
import requests
import logging
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
import googlemaps

# Load environment variables from .env
load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_API_KEY")

logger = logging.getLogger("project.libs.google_client")


class GoogleClient:
    """Google Maps Places API client for fetching business data."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or GOOGLE_API_KEY
        if not self.api_key:
            raise ValueError("GOOGLE_API_KEY is missing. Please add it to your environment variables.")
        self.client = googlemaps.Client(key=self.api_key)

    def search_place(self, query: str, location: str) -> Optional[Dict[str, Any]]:
        """
        Search for a business by text query and location.
        :param query: Business name (string)
        :param location: Location string (city, state)
        :return: First matching business dict or None
        """
        results = self.client.places(query=f"{query}, {location}")
        candidates = results.get("results", [])
        return candidates[0] if candidates else None

    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        """
        Fetch detailed information about a business by Place ID using both old and new Places APIs.
        :param place_id: Google Maps Place ID
        :return: Merged place details as a dictionary
        """
        merged_details = {}

        # Get details from old API
        old_details = {}
        for attempt in range(3):
            try:
                results = self.client.place(
                    place_id=place_id,
                    fields=[
                        "place_id",
                        "name",
                        "business_status",
                        "formatted_address",
                        "address_component",
                        "adr_address",
                        "vicinity",
                        "plus_code",
                        "utc_offset",
                        "formatted_phone_number",
                        "international_phone_number",
                        "website",
                        "url",
                        "geometry",
                        "opening_hours",
                        "current_opening_hours",
                        "secondary_opening_hours",
                        "rating",
                        "user_ratings_total",
                        "price_level",
                        "reviews",
                        "photo",
                        "icon",
                        "editorial_summary",
                        "reservable",
                        "curbside_pickup",
                        "delivery",
                        "dine_in",
                        "takeout",
                        "wheelchair_accessible_entrance",
                        "serves_breakfast",
                        "serves_lunch",
                        "serves_dinner",
                        "serves_beer",
                        "serves_wine",
                        "serves_brunch",
                        "permanently_closed",
                        "types"
                    ]
                )
                old_details = results.get("result", {})
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)

        # Get details from new API
        new_details = {}
        for attempt in range(3):
            try:
                url = f"https://places.googleapis.com/v1/places/{place_id}"
                params = {
                    "fields": "types,primaryTypeDisplayName,displayName,shortFormattedAddress,googleMapsUri,parkingOptions,paymentOptions,accessibilityOptions,amenities",
                    "key": self.api_key
                }
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    new_details = response.json()
                    break
                else:
                    pass
            except Exception as e:
                pass

        # Merge details: prefer old API for most fields, add new API fields
        merged_details.update(old_details)
        if new_details:
            merged_details['types'] = new_details.get('types', [])
            merged_details['primaryTypeDisplayName'] = new_details.get('primaryTypeDisplayName', {})

        return merged_details
    
    def enrich_with_google(self, yelp_business: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a Yelp business dictionary with Google Places API data.
        Preserves Yelp fields, fills missing ones from Google, and stores
        extras under `google_enrichment`.
        """
        name = yelp_business.get("name")
        location = yelp_business.get("location", {})
        coords = yelp_business.get("coordinates", {})
        city = location.get("city")
        state = location.get("state")

        query = f"{name}, {city}, {state}" if city and state else name
        lat, lng = coords.get("latitude"), coords.get("longitude")

        google_place = None
        try:
            if lat and lng:
                results = self.client.places(query=query, location=(lat, lng))
            else:
                results = self.client.places(query=query)
            candidates = results.get("results", [])
            google_place = candidates[0] if candidates else None
        except Exception:
            google_place = None

        if not google_place:
            return yelp_business

        place_id = google_place.get("place_id")
        details = self.get_place_details(place_id) if place_id else {}

        enriched = yelp_business.copy()

        # Define generic types to filter out
        generic_types = {
            "establishment", "point_of_interest", "food", "drink", "store", "health",
            "place_of_worship", "locality", "political", "geocode", "premise",
            "street_address", "intersection", "postal_code", "country",
            "administrative_area_level_1", "administrative_area_level_2",
            "administrative_area_level_3", "colloquial_area", "sublocality",
            "neighborhood", "route", "street_number", "floor", "room"
        }

        # Get types and type from new API
        if details.get('types'):
            actual_types = [t for t in details['types'] if t not in generic_types]
            enriched['types'] = actual_types
            primary_type_display = details.get('primaryTypeDisplayName', {}).get('text', '')
            if primary_type_display:
                enriched['type'] = primary_type_display
            else:
                # Fallback to first actual type
                enriched['type'] = actual_types[0] if actual_types else None

        # Promote a curated subset to top-level only if missing from Yelp,
        # but preserve the full Google payload under google_enrichment.
        promote_fields = [
            "formatted_address",
            "formatted_phone_number",
            "international_phone_number",
            "geometry",
            "opening_hours",
            "user_ratings_total",
            "rating",
            "website",
            "business_status",
        ]

        for field in promote_fields:
            if field in details and not enriched.get(field):
                enriched[field] = details[field]

        for field in promote_fields:
            if field in details and not enriched.get(field):
                enriched[field] = details[field]

        # Always store the entire Google details payload for full fidelity
        # so we "capture all fields".
        # To avoid accidental mutation, store a shallow copy.
        enriched["google_enrichment"] = dict(details) if isinstance(details, dict) else details

        return enriched

    def enrich_batch(self, businesses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enrich a batch of Yelp businesses using Google Places API."""
        return [self.enrich_with_google(business) for business in businesses]

    def search_competitors_in_category(self, category: str, lat: float, lng: float, search_type: str = 'text') -> List[Dict[str, Any]]:
        """
        Search for businesses in a specific category within a radius around a location.
        :param category: Category name (e.g., "restaurant")
        :param lat: Latitude of center point
        :param lng: Longitude of center point
        :param search_type: 'text' for Text Search, 'nearby' for Nearby Search
        :return: List of competitor businesses
        """
        try:
            logger.debug(f"Searching competitors for category='{category}', lat={lat}, lng={lng}, search_type={search_type}")
            if search_type == 'nearby':
                results = self.client.places_nearby(location=(lat, lng), type='restaurant', rank_by='prominence', radius=1000)
            else:  # 'text' or default
                results = self.client.places(query=category, location=(lat, lng), radius=1000)
            competitors = results.get("results", [])
            place_ids = [comp.get("place_id") for comp in competitors[:5]]  # Log first 5 place_ids
            logger.debug(f"Found {len(competitors)} competitors, first 5 place_ids: {place_ids}")
            return competitors
        except Exception as e:
            logger.error(f"Error searching competitors for {category}: {e}")
            return []
    
    @staticmethod
    def extract_business_website_from_google(google_enrichment: Dict[str, Any]) -> Optional[str]:
        """
        Extract the website field from Google enrichment JSON.
        """
        from project.helpers.crawler import normalize_homepage_url

        if google_enrichment and isinstance(google_enrichment, dict):
            url = google_enrichment.get("website")
            if url:
                return normalize_homepage_url(url)
        return None


# Example usage (to be removed or placed in tests)
if __name__ == "__main__":
    client = GoogleClient()
    place = client.search_place("The Fig Tree Restaurant", "Charlotte, NC")
    if place:
        details = client.get_place_details(place["place_id"])
        print(details)