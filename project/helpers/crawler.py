import asyncio
import re
from typing import List, Dict, Tuple, Optional
from urllib.parse import urlparse, urlunparse, urljoin

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


def normalize_url(url: str) -> str:
    """
    Normalize URLs to avoid duplicates like https://site.com and https://site.com/
    Rules:
    - Strip trailing slash unless it's the root
    - Lowercase the scheme and hostname
    - Remove fragments
    """
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path or "/"

        # Remove default ports
        if netloc.endswith(":80") and scheme == "http":
            netloc = netloc[:-3]
        if netloc.endswith(":443") and scheme == "https":
            netloc = netloc[:-4]

        # Normalize root vs trailing slash
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        normalized = urlunparse((scheme, netloc, path, "", "", ""))
        return normalized
    except Exception:
        return url


class WebsiteCrawler:
    def __init__(self, max_links: int = 20):
        self.max_links = max_links
        self.visited = set()

    async def fetch_links(self, url: str) -> Dict:
        """Render the page with Playwright and extract internal links (no SEO analysis here)."""
        domain = urlparse(url).netloc
        links = []

        # Ensure url is string
        if isinstance(url, dict):
            url = url.get("url", "")
        if not isinstance(url, str):
            raise ValueError(f"Expected string for URL, got {type(url)}: {url}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            import asyncio as _asyncio, logging as _logging

            async def safe_goto(target_url: str) -> bool:
                """Try to navigate with retries and fallback to http if https fails."""
                for attempt in range(3):
                    try:
                        await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                        return True
                    except Exception as e:
                        _logging.error(f"Error navigating to {target_url} (attempt {attempt+1}/3): {e}")
                        # If https failed due to connection reset, attempt http fallback once
                        if target_url.startswith("https://") and "ERR_CONNECTION_RESET" in str(e):
                            fallback = target_url.replace("https://", "http://", 1)
                            _logging.warning(f"Retrying with HTTP fallback: {fallback}")
                            target_url = fallback
                        else:
                            if attempt == 2:
                                return False
                        await _asyncio.sleep(2 * (attempt+1))
                return False

            success = await safe_goto(url)
            if not success:
                await browser.close()
                raise RuntimeError(f"Failed to navigate to {url} after retries")
 
            html = await page.content()

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

                    normalized = normalize_url(cleaned)

                    if normalized not in self.visited:
                        self.visited.add(normalized)
                        links.append(normalized)
                        if len(links) >= self.max_links:
                            break

            await browser.close()
 
        return {
            "url": url,
            "links": links,
        }

    async def crawl(self, start_url: str) -> List[Dict]:
        """Crawl depth=1 within the same domain starting from start_url, with max_links limit.
        
        Returns:
            List[Dict]: A list of dicts like {"url": <str>, "links": [<str>, ...]} including the root result.
        """
        self.visited = set()
        all_links: List[str] = []

        # Always include root url
        root = normalize_url(start_url)
        self.visited.add(root)
        all_links.append(root)

        # Fetch subpage links only from the root (depth=1)
        root_result = await self.fetch_links(start_url)
        sub_links = root_result["links"]

        # Enforce 20-page cap (including root)
        sub_links = sub_links[: max(0, self.max_links - 1)]

        # Thread-safe collection of results
        results: List[Dict] = []
        lock = asyncio.Lock()
 
        async def fetch_and_store(url: str):
            try:
                # Fetching but not following further links (depth=1 constraint)
                page_result = await self.fetch_links(url)
                async with lock:
                    if len(all_links) + len(results) < self.max_links:
                        results.append(page_result)
            except Exception:
                # Ignore failures gracefully
                pass

        # Run concurrent fetching
        await asyncio.gather(*[fetch_and_store(url) for url in sub_links])
 
        # Return combined (root + successfully fetched)
        return [root_result] + results