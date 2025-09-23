from typing import Dict, Any, List, Optional
import logging
from project.libs.yelp_client import YelpClient
from project.libs.google_client import GoogleClient
from project.libs.supabase_client import get_client, ensure_table_exists, _businesses_table_schema
from datetime import datetime, timedelta, timezone
from project.helpers.storage import StorageClient


class BusinessIntegrator:
    """
    Integrates Yelp business info with Google Places info.
    """

    def __init__(self):
        self.yelp_client = YelpClient()
        self.google_client = GoogleClient()
        self.storage = StorageClient()
        # Table creation should be handled via migrations; removed runtime creation attempt

    def get_restaurants_in_charlotte(self, limit: int = 10, min_pages: int = 1, max_pages: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch restaurants in Charlotte, NC from Yelp and enrich with Google data.
        :param limit: Number of restaurants to fetch
        :param min_pages: Minimum number of pages to fetch
        :param max_pages: Maximum number of pages to fetch
        :return: List of restaurant info dicts
        """
        yelp_results = []
        for page in range(min_pages, max_pages + 1):
            results = self.yelp_client.search_businesses(
                location="Charlotte, NC", category="restaurants", limit=limit
            )
            if not results:
                break
            yelp_results.extend(results)

        enriched_results = []
        for biz in yelp_results:
            logging.info(f"Scraped business: {biz.get('name')} (id={biz.get('id')})")

            # Guard: if business enrichment done within last 24h, skip details/enrichment
            biz_id = biz.get("id")
            if biz_id and self.storage.business_recently_updated(biz_id):
                logging.info(f"Skipping enrichment for {biz_id}: updated within last 24 hours")
                enriched_results.append({"yelp": biz, "google": {}})
                continue

            # Fetch full Yelp business details
            try:
                yelp_details = self.yelp_client.get_business_details(biz["id"])
            except Exception as e:
                logging.error(f"Failed to fetch details for {biz.get('id')}: {e}")
                yelp_details = biz  # fallback to minimal search result

            google_info = self.google_client.search_place(biz["name"], "Charlotte, NC")
            google_details = {}
            if google_info:
                google_details = self.google_client.get_place_details(google_info["place_id"])

            # Touch businesses.updated_at since we just reprocessed enrichment for this biz_id
            if biz_id:
                try:
                    self.storage.touch_business_updated_at(biz_id)
                except Exception:
                    pass

            enriched_results.append({
                "yelp": yelp_details,
                "google": google_details
            })

        return enriched_results


def merge_business_data(yelp_business: dict, google_business: Optional[dict] = None) -> dict:
    """
    Merge Yelp business dict with optional Google enrichment dict.
    Yelp fields take priority. Missing values are filled in from Google.
    Google-only fields are nested under 'google_enrichment'.
    """
    if not yelp_business or "id" not in yelp_business:
        logging.warning("Critical field 'id' missing in Yelp business data")
        return {}

    merged = dict(yelp_business)  # copy Yelp as base
    google_enrichment = {}

    if google_business:
        for k, v in google_business.items():
            if k in merged and merged[k] in (None, "", [], {}):
                merged[k] = v
            elif k not in merged:
                google_enrichment[k] = v

    if google_enrichment:
        merged["google_enrichment"] = google_enrichment

    return merged


def normalize_for_supabase(business: dict) -> dict:
    """
    Normalize merged business dict for Supabase schema compliance.
    Ensures required schema fields exist, sets None for missing keys.
    """
    schema_fields = [
        "id", "alias", "name", "image_url", "is_claimed", "is_closed",
        "url", "phone", "display_phone", "review_count", "categories",
        "rating", "location", "coordinates", "photos", "price",
        "hours", "transactions", "messaging", "attributes",
        "special_hours", "photo_details", "popularity_score", "rapc",
        "google_enrichment"
    ]

    normalized = {}
    for field in schema_fields:
        if field == "hours":
            normalized[field] = (
                business.get("hours")
                if "hours" in business
                else business.get("business_hours")
            )
        else:
            normalized[field] = business.get(field) if field in business else None

    return normalized


def upsert_business(business: dict) -> None:
    """
    Upsert a single normalized business dict into Supabase.
    Uses 'id' as the conflict resolution key.
    """
    if not business or "id" not in business:
        logging.error("Cannot upsert business without 'id'")
        return

    client = get_client()
    try:
        response = (
            client.table("businesses")
            .upsert(business, on_conflict="id")
            .execute()
        )
        if getattr(response, "error", None):
            logging.error(f"Failed to upsert business {business.get('id')}: {response.error}")
    except Exception as e:
        logging.exception(f"Exception during upsert_business for {business.get('id')}: {e}")


def upsert_businesses(businesses: List[dict]) -> None:
    """
    Batch upsert multiple businesses efficiently into Supabase.
    """
    if not businesses:
        logging.warning("No businesses provided for batch upsert")
        return

    client = get_client()
    try:
        response = (
            client.table("businesses")
            .upsert(businesses, on_conflict="id")
            .execute()
        )
        if getattr(response, "error", None):
            logging.error(f"Batch upsert error: {response.error}")
    except Exception as e:
        logging.exception(f"Exception during batch upsert of businesses: {e}")