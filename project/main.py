import sys
import os

# Ensure the parent directory is on sys.path so "project" can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging

from project.helpers.integration import (
    merge_business_data,
    normalize_for_supabase,
    upsert_businesses,
)
from project.libs.yelp_client import YelpClient
from project.libs.google_client import GoogleClient


def get_paging_config():
    """
    Reads MIN_PAGES and MAX_PAGES from env vars, applies defaults and validation.
    Returns (min_pages, max_pages).
    """
    try:
        min_pages = int(os.getenv("MIN_PAGES", 1))
    except ValueError:
        min_pages = 1
    try:
        max_pages = int(os.getenv("MAX_PAGES", 10))
    except ValueError:
        max_pages = 10

    if min_pages < 1:
        min_pages = 1
    if max_pages < min_pages:
        max_pages = min_pages
    return min_pages, max_pages


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("Starting business data integration pipeline")

    from project.helpers.integration import BusinessIntegrator

    min_pages, max_pages = get_paging_config()
    integrator = BusinessIntegrator()

    try:
        restaurants = integrator.get_restaurants_in_charlotte(limit=10, min_pages=min_pages, max_pages=max_pages)
        logging.info(f"Fetched {len(restaurants)} businesses from Yelp + Google")

        merged_businesses = []
        for biz in restaurants:
            merged = merge_business_data(biz.get("yelp", {}), biz.get("google", {}))
            if merged:
                normalized = normalize_for_supabase(merged)
                merged_businesses.append(normalized)

        if merged_businesses:
            upsert_businesses(merged_businesses)
            logging.info("Upserted businesses into Supabase successfully")
        else:
            logging.warning("No merged businesses to upsert")

    except Exception as e:
        logging.exception(f"Pipeline execution failed: {e}")


if __name__ == "__main__":
    main()
