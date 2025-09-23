import re
import requests
from typing import Dict, Any, Optional
from openai import OpenAI
import logging

logger = logging.getLogger(__name__)


class PageProcessor:
    """
    Processes a crawled page:
    - Classify page type (Homepage, About, Contact, Menu, Other)
    - Summarize page content in one line
    - Extract contact emails with domain validation
    """

    def __init__(self, openrouter_api_key: str, business_domain: str):
        self.client = OpenAI(api_key=openrouter_api_key, base_url="https://openrouter.ai/api/v1")
        self.business_domain = business_domain

    def classify_page(self, url: str, content: str) -> str:
        """Classify page type using OpenRouter."""
        system_instruction = (
            "You are a strict page classifier. "
            "Your task is to classify a webpage into exactly one of the following categories: "
            "Homepage, About, Contact, Menu, Press, Blog, Article, Product, etc. "
            "Rules: "
            "1. Respond with ONLY one word, no explanation. "
        )
        user_prompt = f"URL: {url}\nContent excerpt: {content[:500]}"
        try:
            resp = self.client.chat.completions.create(
                model="meta-llama/llama-3.3-70b-instruct",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=5,
            )
            classification = resp.choices[0].message.content.strip()
            # Ensure classification is strictly one word; otherwise, fallback to "Other"
            if " " in classification or "\n" in classification or not classification.isalpha():
                return "Other"
            return classification
        except Exception as e:
            logger.error(f"Error classifying page {url}: {e}", exc_info=True)
            return "Other"

    def summarize_page(self, url: str, content: str) -> str:
        """Summarize page in one line using OpenRouter."""
        system_instruction = (
            "You are a summarizer that produces concise one-line summaries stating what a webpage is about. "
            "Avoid flowery language, marketing-style descriptions, or redundant details. "
            "Focus on the main subject or purpose of the page (e.g., 'About us page for a XYZ restaurant', "
            "'E-commerce product page for sneakers', 'News article about AI regulations')."
        )
        user_prompt = f"URL: {url}\nContent excerpt: {content[:1000]}"
        try:
            resp = self.client.chat.completions.create(
                model="meta-llama/llama-3.3-70b-instruct",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=50,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error summarizing page {url}: {e}", exc_info=True)
            return ""

    def extract_emails(self, content: str) -> Optional[str]:
        """Extract an email address, prioritizing same-domain."""
        emails = re.findall(r"[\\w._%+-]+@[\\w.-]+\\.[a-zA-Z]{2,}", content)
        if not emails:
            return None

        # Prioritize same domain
        for email in emails:
            if self.business_domain in email:
                return email

        # If external email(s), we could use OpenRouter to check ownership, but here we just return the first
        return emails[0]