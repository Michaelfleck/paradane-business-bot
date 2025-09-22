import os
import time
import logging
import requests
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

YELP_API_KEY = os.getenv("YELP_API_KEY")
YELP_API_BASE_URL = "https://api.yelp.com/v3"


logging.basicConfig(level=logging.INFO)

class YelpClient:
    """Yelp Fusion API client for fetching business data."""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or YELP_API_KEY
        if not self.api_key:
            raise ValueError("YELP_API_KEY is missing. Please add it to your environment variables.")
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def search_businesses(self, location: str, category: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for businesses in a given location and category.
        :param location: Location string (e.g., "Charlotte, NC")
        :param category: Business category (e.g., "restaurants")
        :param limit: Number of results to fetch
        :return: List of business objects
        """
        url = f"{YELP_API_BASE_URL}/businesses/search"
        params = {
            "location": location,
            "categories": category,
            "limit": limit
        }
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json().get("businesses", [])

    def get_business_details(self, business_id: str) -> Dict[str, Any]:
        """
        Fetch detailed information about a business by its Yelp ID.
        :param business_id: The Yelp business ID
        :return: Business details as a dictionary
        """
        url = f"{YELP_API_BASE_URL}/businesses/{business_id}"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()


# Example usage (to be removed or placed in tests)
if __name__ == "__main__":
    client = YelpClient()
    results = client.search_businesses(location="Charlotte, NC", category="restaurants", limit=5)
    for biz in results:
        print(f"{biz['name']} - {biz['rating']} stars")