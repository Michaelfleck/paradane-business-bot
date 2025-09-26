import re
from typing import Dict, Any, Optional, List, Tuple
import logging
# Removed OpenRouter client from PageProcessor to enforce separation of concerns

logger = logging.getLogger(__name__)


class PageProcessor:
    """
    Processes a crawled page:
    - Classify page type (Homepage, About, Contact, Menu, Other)
    - Summarize page content in one line
    - Extract contact emails with domain validation
    - Extract social media account/profile links
    """

    def __init__(self, openrouter_api_key: str, business_domain: str):
        # OpenRouter client is now centralized in openrouter_client.py
        self.business_domain = business_domain

    # The following methods have been removed from PageProcessor:
    # - classify_page
    # - summarize_page
    # These now live in the enrichment stage of the pipeline.

    def extract_emails(self, content: str) -> Optional[str]:
        """Extract all email addresses, prioritizing same-domain, returned as comma-separated string."""
        emails = re.findall(r"[\w._%+-]+@[\w.-]+\.[a-zA-Z]{2,}", content)
        if not emails:
            return None

        # Deduplicate while preserving order
        unique_emails = list(dict.fromkeys(emails))

        # Prioritize same domain by placing them first
        same_domain = [email for email in unique_emails if self.business_domain and self.business_domain in email]
        other_emails = [email for email in unique_emails if email not in same_domain]

        return ",".join(same_domain + other_emails)

    # --- Social Media Extraction ---

    _SOCIAL_PATTERNS: List[Tuple[str, re.Pattern]] = [
        # Facebook: pages, profile, business, short urls
        ("facebook", re.compile(r"https?://(?:www\.)?(?:m\.)?facebook\.com/(?:pages/)?[A-Za-z0-9_.-]+/?", re.IGNORECASE)),
        # Instagram
        ("instagram", re.compile(r"https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.-]+/?", re.IGNORECASE)),
        # X/Twitter
        ("twitter", re.compile(r"https?://(?:www\.)?(?:x|twitter)\.com/[A-Za-z0-9_]{1,15}/?", re.IGNORECASE)),
        # LinkedIn company or profile
        ("linkedin", re.compile(r"https?://(?:www\.)?linkedin\.com/(?:company|in|school)/[A-Za-z0-9_.-]+/?", re.IGNORECASE)),
        # TikTok
        ("tiktok", re.compile(r"https?://(?:www\.)?tiktok\.com/@?[A-Za-z0-9_.-]+/?", re.IGNORECASE)),
        # YouTube channel or user or handle
        ("youtube", re.compile(r"https?://(?:www\.)?youtube\.com/(?:@?[A-Za-z0-9_.-]+|channel/[A-Za-z0-9_-]+|c/[A-Za-z0-9_.-]+)/?", re.IGNORECASE)),
        ("youtube", re.compile(r"https?://(?:www\.)?youtu\.be/[A-Za-z0-9_-]+/?", re.IGNORECASE)),
        # Pinterest
        ("pinterest", re.compile(r"https?://(?:www\.)?pinterest\.[a-z.]+/[A-Za-z0-9_.-]+/?", re.IGNORECASE)),
        # WhatsApp (click-to-chat)
        ("whatsapp", re.compile(r"https?://(?:api\.|wa\.)?whatsapp\.com/send\?[^\"'<>\\s]+", re.IGNORECASE)),
        # Threads
        ("threads", re.compile(r"https?://(?:www\.)?threads\.net/@[A-Za-z0-9_.-]+/?", re.IGNORECASE)),
        # Snapchat
        ("snapchat", re.compile(r"https?://(?:www\.)?snapchat\.com/add/[A-Za-z0-9_.-]+/?", re.IGNORECASE)),
    ]

    def _normalize_social_url(self, url: str) -> str:
        """Normalize minor variations (strip trailing punctuation/spaces, unify scheme to https)."""
        s = (url or "").strip().strip('\'"()[]{}.,;')
        if not s:
            return s
        # Force https where possible
        s = re.sub(r"^http://", "https://", s, flags=re.IGNORECASE)
        # Remove query fragments for determinism on common platforms except WhatsApp which uses query for phone/message
        if "whatsapp.com/send" not in s:
            s = s.split("#", 1)[0]
            s = s.rstrip("?&")
        return s

    def extract_social_links(self, content: str) -> Optional[str]:
        """
        Extract social media profile links from the HTML/text content.
        Returns a comma-separated list in a stable order by platform then URL.
        """
        if not content:
            return None
        found: List[Tuple[str, str]] = []

        # Heuristic: search entire content string; covers hrefs, iframes, visible text.
        for platform, pattern in self._SOCIAL_PATTERNS:
            for m in pattern.finditer(content):
                raw = m.group(0)
                norm = self._normalize_social_url(raw)
                if norm:
                    found.append((platform, norm))

        if not found:
            return None

        # Deduplicate preserving first occurrence per URL
        seen_urls = set()
        deduped: List[Tuple[str, str]] = []
        for plat, url in found:
            if url not in seen_urls:
                seen_urls.add(url)
                deduped.append((plat, url))

        # Sort deterministic: by platform name, then by URL
        deduped.sort(key=lambda x: (x[0], x[1]))

        # Return as "platform:url" entries comma-separated to keep label
        parts = [f"{plat}:{url}" for plat, url in deduped]
        return ",".join(parts) if parts else None