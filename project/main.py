import sys
import os

# Ensure the parent directory is on sys.path so "project" can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import logging
import argparse

from project.helpers.integration import (
    merge_business_data,
    normalize_for_supabase,
    upsert_businesses,
)
from project.helpers.zoho_integration import create_zoho_lead_for_business
from project.libs.yelp_client import YelpClient
# from project.libs.google_client import GoogleClient
from project.reporting.config import get_report_config
from project.reporting.business_report import generateBusinessReport, generateBusinessReportPdf, generateBusinessRankLocalReport, generateBusinessRankLocalReportPdf
from project.reporting.website_report import generateWebsiteReport, generateWebsiteReportPdf


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
    # CLI to choose between pipeline demo and report rendering

    parser = argparse.ArgumentParser(description="Paradane Business Bot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Pipeline demo command
    pipeline_parser = subparsers.add_parser("pipeline", help="Run the data pipeline demo")
    pipeline_parser.add_argument("--location", default="Charlotte, NC", help="Yelp search location")
    pipeline_parser.add_argument("--term", default="restaurants", help="Yelp search term")
    pipeline_parser.add_argument("--limit", type=int, default=10, help="Yelp result limit")

    # Report rendering command
    report_parser = subparsers.add_parser("report", help="Render report HTML or PDF")
    report_parser.add_argument("--business-id", required=True, help="Business ID")
    report_parser.add_argument("--pdf", action="store_true", help="Output PDF instead of HTML")
    report_parser.add_argument("--out", required=False, help="Output path for PDF or HTML file")
    report_parser.add_argument("--no-upload", action="store_true", help="Do not upload PDF to Storage even if enabled in config")

    args = parser.parse_args()

    if args.command == "pipeline":
        logging.info("Starting business data integration pipeline")
        from project.helpers.pipeline import BusinessPipeline
        yelp_client = YelpClient()
        businesses = yelp_client.search_businesses(args.location, args.term, limit=args.limit)
        logging.info(f"Fetched {len(businesses)} businesses from Yelp")
        upsert_businesses(businesses)
        logging.info("Upserted businesses into Supabase successfully")

        # Create Zoho CRM leads for each business
        for biz in businesses:
            try:
                create_zoho_lead_for_business(biz)
            except Exception as e:
                logging.error(f"Failed to create Zoho lead for business {biz.get('id')}: {e}")
        logging.info("Created Zoho CRM leads for businesses")

        # Run business_pages pipeline for each business that has a website
        import asyncio
        from urllib.parse import urlparse

        async def run_pipelines():
            tasks = []
            for biz in businesses:
                # biz is a Yelp dict
                biz_id = biz.get("id")
                website = biz.get("website") or biz.get("attributes", {}).get("menu_url")
                if not biz_id or not website:
                    continue
                try:
                    parsed = urlparse(website)
                    if parsed.scheme not in ("http", "https"):
                        continue
                except Exception:
                    continue

                try:
                    openrouter_api_key = os.getenv("OPENROUTER_API_KEY", "")
                    google_api_key = os.getenv("GOOGLE_API_KEY", "")
                    db_url = os.getenv("SUPABASE_URL", "")
                    pipeline = BusinessPipeline(
                        openrouter_api_key=openrouter_api_key,
                        google_api_key=google_api_key,
                        db_url=db_url,
                        business_id=biz_id,
                        business_url=website,
                    )
                    tasks.append(asyncio.create_task(pipeline.run()))
                except Exception as e:
                    logging.exception(f"Failed to schedule pipeline for business {biz_id}: {e}")

            if tasks:
                await asyncio.gather(*tasks)

        logging.info("Starting business_pages processing for enriched businesses")
        asyncio.run(run_pipelines())
        logging.info("Completed business_pages processing")
    elif args.command == "report":
        _ = get_report_config()  # ensure config loads
        if args.pdf:
            if args.type == "business":
                result = generateBusinessReportPdf(args.business_id, to_path=args.out, upload=(False if args.no_upload else None))
            else:
                result = generateWebsiteReportPdf(args.business_id, to_path=args.out, upload=(False if args.no_upload else None))
            print(result)
        else:
            if args.type == "business":
                html = generateBusinessReport(args.business_id)
            else:
                html = generateWebsiteReport(args.business_id)
            if args.out:
                os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
                with open(args.out, "w", encoding="utf-8") as f:
                    f.write(html)
                print(args.out)
            else:
                print(html[:20000])


if __name__ == "__main__":
    main()
