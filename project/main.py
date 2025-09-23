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

    from project.libs.yelp_client import YelpClient
    from project.libs.google_client import GoogleClient
    from project.helpers.pipeline import BusinessPipeline

    import asyncio
    import os

    yelp_client = YelpClient()
    google_client = GoogleClient()
    # Supabase upserts are handled via integration.py

    # Fetch from Yelp + Google
    businesses = yelp_client.search_businesses("Charlotte, NC", "restaurants", limit=10)
    enriched = google_client.enrich_batch(businesses)

    logging.info(f"Fetched {len(enriched)} businesses from Yelp + Google")
    upsert_businesses(enriched)
    logging.info("Upserted businesses into Supabase successfully")

    # Website scraping and pipeline
    openrouter_api_key = os.getenv("OPENROUTER_API_KEY")
    pagespeed_api_key = os.getenv("PAGESPEED_API_KEY")
    db_url = os.getenv("DATABASE_URL")

    for biz in enriched:
        biz_id = biz.get("id")
        yelp_site = yelp_client.extract_business_website(biz, biz.get("google_enrichment"))
        google_site = google_client.extract_business_website_from_google(biz.get("google_enrichment"))
        website = yelp_site or google_site

        if website:
            logging.info(f"Running pipeline for business {biz.get('name')} ({biz_id}) url={website}")
            pipeline = BusinessPipeline(
                openrouter_api_key=openrouter_api_key,
                pagespeed_api_key=pagespeed_api_key,
                db_url=db_url,
                business_id=biz_id,
                business_url=website
            )
            try:
                asyncio.run(pipeline.run())
            except Exception as e:
                logging.error(f"Pipeline failed for {biz_id}: {e}")


if __name__ == "__main__":
    main()
