import asyncio
import re
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, urljoin

from playwright.async_api import async_playwright


def normalize_homepage_url(url: str) -> str:
    """
    Given a URL possibly pointing to a sub-page, return only the homepage.
    Example:
        https://thecrunkleton.com/locations/charlotte/menus -> https://thecrunkleton.com
    """
    if not url:
        return url
    try:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
        return url
    except Exception:
        return url


class WebsiteCrawler:
    def __init__(self, max_links: int = 20):
        self.max_links = max_links
        self.visited = set()

    async def fetch_links(self, url: str) -> List[str]:
        """Render the page with Playwright and extract internal links."""
        domain = urlparse(url).netloc
        links = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)

            hrefs = await page.eval_on_selector_all("a", "elements => elements.map(e => e.href)")

            for href in hrefs:
                if href and urlparse(href).netloc == domain:
                    cleaned = href.split("#")[0]
                    # Skip non-HTML static assets (images, docs, styles, scripts, archives, etc.)
                    if any(cleaned.lower().endswith(ext) for ext in [
                        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg",
                        ".webp", ".ico", ".tiff", ".pdf", ".doc", ".docx",
                        ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".rar",
                        ".css", ".js", ".json", ".xml"
                    ]):
                        continue
                    if cleaned not in self.visited:
                        self.visited.add(cleaned)
                        links.append(cleaned)
                        if len(links) >= self.max_links:
                            break

            await browser.close()

        return links

    async def crawl(self, start_url: str) -> List[str]:
        """Crawl depth=1 within the same domain starting from start_url, with max_links limit."""
        self.visited = set()
        all_links: List[str] = []

        # Always include root url
        self.visited.add(start_url)
        all_links.append(start_url)

        # Fetch subpage links only from the root (depth=1)
        sub_links = await self.fetch_links(start_url)

        # Enforce 20-page cap (including root)
        sub_links = sub_links[: max(0, self.max_links - 1)]

        # Thread-safe collection of results
        results: List[str] = []
        lock = asyncio.Lock()

        async def fetch_and_store(url: str):
            try:
                # Fetching but not following further links (depth=1 constraint)
                _ = await self.fetch_links(url)
                async with lock:
                    if len(all_links) + len(results) < self.max_links:
                        results.append(url)
            except Exception:
                # Ignore failures gracefully
                pass

        # Run concurrent fetching
        await asyncio.gather(*[fetch_and_store(url) for url in sub_links])

        # Return combined (root + successfully fetched)
        return all_links + results