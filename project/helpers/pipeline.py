import asyncio
import os
import time
import concurrent.futures
import re
import html as _html
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta

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

    # Shared per-process PageSpeed thread pool executor (bounded)
    _PAGESPEED_POOL: Optional[concurrent.futures.ThreadPoolExecutor] = None

    def __init__(self, openrouter_api_key: str, pagespeed_api_key: str, db_url: str, business_id: str, business_url: str):
        self.business_id = business_id
        self.business_url = business_url
        self.crawler = WebsiteCrawler(max_links=20)
        self.processor = PageProcessor(openrouter_api_key=openrouter_api_key, business_domain=self.extract_domain(business_url))
        self.pagespeed = PageSpeedClient(api_key=pagespeed_api_key)
        self.storage = StorageClient()

        # Concurrency controls (configurable via env)
        # LINK_CONCURRENCY_PER_DOMAIN: max concurrent Playwright renders per BusinessPipeline (default 2)
        # PAGESPEED_CONCURRENCY_PER_PROCESS: max concurrent PageSpeed calls per process (default 4)
        link_cc_default = 2
        ps_cc_default = 4
        try:
            self.link_concurrency = max(1, int(os.getenv("LINK_CONCURRENCY_PER_DOMAIN", link_cc_default)))
        except Exception:
            self.link_concurrency = link_cc_default
        try:
            ps_workers = max(1, int(os.getenv("PAGESPEED_CONCURRENCY_PER_PROCESS", ps_cc_default)))
        except Exception:
            ps_workers = ps_cc_default

        if BusinessPipeline._PAGESPEED_POOL is None:
            # Singleton per-process executor
            BusinessPipeline._PAGESPEED_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=ps_workers, thread_name_prefix="pagespeed")

        self._render_semaphore = asyncio.Semaphore(self.link_concurrency)

        # Cache to deduplicate PageSpeed calls within a run
        # Stores url -> asyncio.Task that yields metrics dict
        self._pagespeed_tasks: Dict[str, asyncio.Task] = {}

    def extract_domain(self, url: str) -> str:
        from urllib.parse import urlparse
        return urlparse(url).netloc

    def _schedule_pagespeed(self, url: str) -> asyncio.Task:
        """
        Schedule a PageSpeed analyze_page call in the bounded thread pool and return an asyncio Task.
        Deduplicates per URL within this pipeline run.
        """
        if url in self._pagespeed_tasks:
            return self._pagespeed_tasks[url]

        async def run_in_pool() -> Dict[str, Any]:
            loop = asyncio.get_running_loop()
            # Await the future returned by run_in_executor inside a coroutine to avoid "a coroutine was expected" errors
            return await loop.run_in_executor(BusinessPipeline._PAGESPEED_POOL, self.pagespeed.analyze_page, url)

        task: asyncio.Task = asyncio.create_task(run_in_pool())
        self._pagespeed_tasks[url] = task
        return task

    async def run(self):
        # Skip processing if we've processed pages for this business within last 24 hours
        try:
            if self.storage.business_pages_recently_updated(self.business_id):
                print(f"[INFO] Skipping page processing for {self.business_id}: processed within last 24 hours")
                return
        except Exception:
            # Non-fatal; continue best-effort
            pass

        link_results: List[Dict] = await self.crawler.crawl(self.business_url)

        async def process_link(link_data: Dict):
            try:
                url = link_data.get("url") if isinstance(link_data, dict) else str(link_data)
                print(f"[DEBUG] Processing URL: {url}")

                # Weekly gating: fetch existing row
                existing = self.storage.get_business_page(self.business_id, url)
                now = datetime.now(timezone.utc)
                seven_days = timedelta(days=7)

                # Render page content with bounded concurrency to avoid browser crashes
                from playwright.async_api import async_playwright
                async with self._render_semaphore:
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
                email = self.processor.extract_emails(content)
                print("[DEBUG] Finished PageProcessor")

                # Prepare words-only content for model enrichment (strictly inner <body> content only)
                def _html_to_words_only(doc_html: str) -> str:
                    """
                    Extract ONLY the inner body text using regex-only (no BeautifulSoup on full doc):
                    - Strictly extract inner <body>…</body> if present
                    - Remove scripts/styles/noscript/template/meta/link within that fragment
                    - Decode entities and normalize whitespace
                    """
                    import re as _re
                    # Strictly extract inner body; if no <body>, use full doc as last resort
                    m = _re.search(r"<body[^>]*>([\\s\\S]*?)</body>", doc_html, _re.IGNORECASE)
                    fragment = m.group(1) if m else doc_html

                    # Strip disallowed blocks and tags from the fragment only
                    fragment = _re.sub(r"<(script|style|noscript|template|meta|link)[\\s\\S]*?</\\1>", " ", fragment, flags=_re.IGNORECASE)
                    # Remove any remaining tags
                    fragment = _re.sub(r"<[^>]+>", " ", fragment)

                    # Decode entities and normalize whitespace
                    text = _html.unescape(fragment)
                    text = _re.sub(r"[ \\t\\f\\v\\r\\n]+", " ", text).strip()

                    # Cap characters to protect context window
                    MAX_CHARS = 60000
                    if len(text) > MAX_CHARS:
                        text = text[:MAX_CHARS]
                    return text

                words_only = _html_to_words_only(content)

                # Decide whether to recompute AI fields (summary, page_type) based on weekly gating using updated_at
                recompute_ai = True
                if existing and existing.get("updated_at"):
                    try:
                        last_ts = str(existing["updated_at"]).replace("Z", "+00:00")
                        last_updated = datetime.fromisoformat(last_ts)
                        if last_updated.tzinfo is None:
                            last_updated = last_updated.replace(tzinfo=timezone.utc)
                        else:
                            last_updated = last_updated.astimezone(timezone.utc)
                        recompute_ai = (now - last_updated) >= seven_days
                    except Exception:
                        # If parsing fails, default to recompute to stay safe
                        recompute_ai = True

                if recompute_ai:
                    summary = or_summarize_page(url, words_only)
                    page_type = or_classify_page(url, summary)
                    print("[DEBUG] Finished Content Enrichment (AI recomputed)")
                else:
                    summary = existing.get("summary") if existing else None
                    page_type = existing.get("page_type") if existing else None
                    print("[DEBUG] Skipped AI enrichment (preserved within 7 days)")

                # Run SEO Analyzer separately (synchronous)
                seo = analyze_html(content)
                print("[DEBUG] Finished SEO Analyzer")

                # Schedule PageSpeed in background (bounded by thread pool)
                ps_task = self._schedule_pagespeed(url)
                metrics = await ps_task  # ps_task is an asyncio.Task; awaiting yields dict metrics
                print("[DEBUG] Finished PageSpeed")

                # Merge results. Only include AI fields when recomputed; otherwise preserve by passing existing values.
                page_record: Dict[str, Any] = {
                    "business_id": self.business_id,
                    "url": url,
                    "email": email,
                    "page_speed_score": metrics.get("page_speed_score"),
                    "time_to_interactive_ms": metrics.get("time_to_interactive_ms"),
                    "seo_score": seo.get("score"),
                    "seo_explanation": seo.get("explanation"),
                }
                if recompute_ai:
                    page_record["summary"] = summary
                    page_record["page_type"] = page_type
                else:
                    # Preserve existing values by explicitly setting them if present; if None, do not include to avoid overwriting existing with null
                    if summary is not None:
                        page_record["summary"] = summary
                    if page_type is not None:
                        page_record["page_type"] = page_type

                # Save only after all are ready
                self.storage.insert_business_page(page_record)
                print(f"[INFO] Upserted business_pages for {url} (AI {'updated' if recompute_ai else 'preserved'})")
            except Exception as e:
                print(f"Error processing URL={url}, link_data={link_data}: {e}")

        # Launch per-link tasks concurrently while bounding Playwright via semaphore
        tasks = []
        for link_data in link_results:
            task = asyncio.create_task(process_link(link_data if isinstance(link_data, dict) else {"url": str(link_data), "links": []}))
            tasks.append(task)

        # Await all
        await asyncio.gather(*tasks)