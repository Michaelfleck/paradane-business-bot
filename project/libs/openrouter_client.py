import os
import logging
from openai import OpenAI

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Create a reusable OpenRouter client
client = None
if OPENROUTER_API_KEY:
    try:
        client = OpenAI(api_key=OPENROUTER_API_KEY, base_url="https://openrouter.ai/api/v1")
    except Exception as e:
        logger.error(f"Failed to create OpenRouter client: {e}")
        client = None


def classify_page(url: str, summary: str) -> str:
    """Classify page type using OpenRouter"""
    if not client:
        return "Other"

    system_instruction = (
        "You are a strict page classifier. "
        "Your task is to classify a webpage into exactly one of the following categories: "
        "Homepage, About, Contact, Menu, Press, Blog, Article, Product, etc. "
        "Rules: Respond with ONLY one word, no explanation."
    )
    user_prompt = f"URL: {url}\nSummary: {summary}"

    try:
        resp = client.chat.completions.create(
            model="meta-llama/llama-3.3-70b-instruct",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=5,
        )
        classification = resp.choices[0].message.content.strip()
        if " " in classification or "\n" in classification or not classification.isalpha():
            return "Other"
        return classification
    except Exception as e:
        logger.error(f"Error classifying page {url}: {e}", exc_info=True)
        return "Other"


def summarize_page(url: str, content: str) -> str:
    """Summarize page in one line using OpenRouter"""
    if not client:
        return ""

    system_instruction = (
        "You are a summarizer that produces concise one-line summaries stating what a webpage is about. "
        "Avoid marketing speak; focus only on the main subject or purpose of the page."
    )
    import re
    words = re.findall(r"\w+", content)
    cleaned_content = " ".join(words)
    user_prompt = f"URL: {url}\nContent: {cleaned_content}"

    try:
        resp = client.chat.completions.create(
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


def refine_explanation(raw_text: str) -> str:
    """Refine SEO explanation with OpenRouter into concise professional summary"""
    if not client:
        return raw_text

    system_instruction = (
        "You are an SEO audit assistant. "
        "Your job is to refine raw SEO audit findings into a concise, professional summary. "
        "Avoid verbose phrasing. Focus on clarity and accuracy."
    )
    user_prompt = f"SEO issues found:\n{raw_text}"

    try:
        resp = client.chat.completions.create(
            model="meta-llama/llama-3.3-70b-instruct",
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=150,
        )
        refined = resp.choices[0].message.content.strip()
        return refined if refined else raw_text
    except Exception as e:
        logger.error(f"Error refining explanation via OpenRouter: {e}", exc_info=True)
        return raw_text