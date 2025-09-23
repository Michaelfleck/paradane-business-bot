import asyncio
from typing import Dict, Any, List

from project.helpers.crawler import WebsiteCrawler
from project.helpers.page_processor import PageProcessor
from project.helpers.pagespeed import PageSpeedClient
from project.helpers.storage import StorageClient


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
        links: List[str] = await self.crawler.crawl(self.business_url)
        async def process_link(link: str):
            try:
                # Render page content
                from playwright.async_api import async_playwright
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(link, wait_until="domcontentloaded", timeout=60000)
                    content = await page.inner_text("body")
                    await browser.close()

                # Process page
                page_type = self.processor.classify_page(link, content)
                summary = self.processor.summarize_page(link, content)
                email = self.processor.extract_emails(content)
                metrics = self.pagespeed.analyze_page(link)

                # Save
                self.storage.insert_business_page({
                    "business_id": self.business_id,
                    "url": link,
                    "page_type": page_type,
                    "summary": summary,
                    "email": email,
                    "page_speed_score": metrics["page_speed_score"],
                    "time_to_interactive_ms": metrics["time_to_interactive_ms"],
                })
            except Exception as e:
                print(f"Error processing {link}: {e}")

        # Run all links concurrently
        tasks = [process_link(link) for link in links]
        await asyncio.gather(*tasks, return_exceptions=True)


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