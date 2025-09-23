import re
from typing import Dict, Any, Optional
import logging
# Removed OpenRouter client from PageProcessor to enforce separation of concerns

logger = logging.getLogger(__name__)


class PageProcessor:
    """
    Processes a crawled page:
    - Classify page type (Homepage, About, Contact, Menu, Other)
    - Summarize page content in one line
    - Extract contact emails with domain validation
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