import asyncio
from typing import Dict, Any, List, Optional

from project.helpers.crawler import WebsiteCrawler
from project.helpers.page_processor import PageProcessor
from project.helpers.pagespeed import PageSpeedClient
from project.helpers.storage import StorageClient
from project.helpers.seo_analyzer import analyze_html
from project.libs.openrouter_client import summarize_page as or_summarize_page, classify_page as or_classify_page


class BusinessPipeline:
    """
    Orchestrates the full pipeline for a business website:
    - Crawl up to 20 internal links
    - Classify + Summarize each page
    - Extract emails
    - Run PageSpeed Insights
    - Save into DB
    """

    def __init__(self, openrouter_api_key: str, pagespeed_api_key: str, db_url: str, business_id: str, business_url: str):
        self.business_id = business_id
        self.business_url = business_url
        self.crawler = WebsiteCrawler(max_links=20)
        self.processor = PageProcessor(openrouter_api_key=openrouter_api_key, business_domain=self.extract_domain(business_url))
        self.pagespeed = PageSpeedClient(api_key=pagespeed_api_key)
        self.storage = StorageClient()

    def extract_domain(self, url: str) -> str:
        from urllib.parse import urlparse
        return urlparse(url).netloc

    async def run(self):
        link_results: List[Dict] = await self.crawler.crawl(self.business_url)
        async def process_link(link_data: Dict):
            try:
                url = link_data.get("url") if isinstance(link_data, dict) else str(link_data)
                print(f"[DEBUG] Processing URL: {url}")

                # Render page content
                from playwright.async_api import async_playwright
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context()
                    content = ""
                    try:
                        page = await context.new_page()
                        import logging as _logging, asyncio as _asyncio

                        async def safe_goto(target_url: str) -> Optional[str]:
                            for attempt in range(3):
                                try:
                                    await page.goto(target_url, wait_until="domcontentloaded", timeout=360000)
                                    # Return full HTML (head + body) so SEO analyzer can see meta tags, title, etc.
                                    return await page.content()
                                except Exception as e:
                                    _logging.error(f"Error navigating to {target_url} (attempt {attempt+1}/3): {e}")
                                    if target_url.startswith("https://") and "ERR_CONNECTION_RESET" in str(e):
                                        fallback = target_url.replace("https://", "http://", 1)
                                        _logging.warning(f"Retrying with HTTP fallback: {fallback}")
                                        target_url = fallback
                                    else:
                                        if attempt == 2:
                                            return None
                                    await _asyncio.sleep(2 * (attempt+1))
                            return None

                        content = await safe_goto(url) or ""
                    finally:
                        await page.close()
                        await context.close()
                        await browser.close()

                # Process page (extraction only)
                # extract_emails is synchronous; do not await
                email = self.processor.extract_emails(content)
                print("[DEBUG] Finished PageProcessor")

                # Content enrichment stage
                summary = or_summarize_page(url, content)
                page_type = or_classify_page(url, summary)
                print("[DEBUG] Finished Content Enrichment")

                # Run SEO Analyzer separately (synchronous)
                seo = analyze_html(content)
                print("[DEBUG] Finished SEO Analyzer")

                # Run PageSpeed (synchronous call, no await needed)
                metrics = self.pagespeed.analyze_page(url)
                print("[DEBUG] Finished PageSpeed")

                # Merge results
                page_record = {
                    "business_id": self.business_id,
                    "url": url,
                    "page_type": page_type,
                    "summary": summary,
                    "email": email,
                    "page_speed_score": metrics["page_speed_score"],
                    "time_to_interactive_ms": metrics["time_to_interactive_ms"],
                    "seo_score": seo.get("score"),
                    "seo_explanation": seo.get("explanation"),
                }

                # Save only after all are ready
                self.storage.insert_business_page(page_record)
                print(f"[INFO] Upserted business_pages for {url}")
            except Exception as e:
                print(f"Error processing URL={url}, link_data={link_data}: {e}")

        # Run all links sequentially to avoid Playwright crashes
        for link_data in link_results:
            # Each element from crawler.crawl(...) is a dict with at least {"url": ..., "links": [...]}
            await process_link(link_data if isinstance(link_data, dict) else {"url": str(link_data), "links": []})


if __name__ == "__main__":
    # Example run (replace values)
    pipeline = BusinessPipeline(
        openrouter_api_key="OPENROUTER_KEY",
        pagespeed_api_key="PAGESPEED_KEY",
        db_url="postgres://user:pass@localhost:5432/dbname",
        business_id="00000000-0000-0000-0000-000000000000",
        business_url="https://example.com"
    )
    asyncio.run(pipeline.run())