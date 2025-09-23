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

    def classify_page(self, url: str, summary: str) -> str:
        """Classify page type using OpenRouter.
        Note: Pass the summary from summarize_page externally."""
        system_instruction = (
            "You are a strict page classifier. "
            "Your task is to classify a webpage into exactly one of the following categories: "
            "Homepage, About, Contact, Menu, Press, Blog, Article, Product, etc. "
            "Rules: "
            "1. Respond with ONLY one word, no explanation. "
        )
        user_prompt = f"URL: {url}\nSummary: {summary}"
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
        # Clean content: keep only words
        words = re.findall(r"\w+", content)
        cleaned_content = " ".join(words)
        user_prompt = f"URL: {url}\nContent: {cleaned_content}"
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