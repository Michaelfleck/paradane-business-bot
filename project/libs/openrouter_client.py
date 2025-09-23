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
        "You are a page classifier for sites. "
        "Classify the page into exactly one canonical category term. "
        "For example: Homepage, About, Contact, Menu, Press, Blog, Article, Product, Services, Gallery, Events, Reservations, Careers, FAQ, Reviews, Location, Legal, etc. "
        "Rules: Output only the single category word from the set. No punctuation, no sentences, no explanations. "
        "If uncertain, output Other."
    )
    user_prompt = f"URL: {url}\nSummary: {summary}"

    import time
    for attempt in range(3):
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
            logger.error(f"Error classifying page {url} (attempt {attempt+1}/3): {e}", exc_info=True)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    return "Other"


def summarize_page(url: str, content: str) -> str:
    """Summarize page in one line using OpenRouter"""
    if not client:
        return ""

    system_instruction = (
        "You write one-sentence summaries of webpages stating what it is about. "
        "Focus only on the main subject or purpose of the page. "
        "Prefer concrete details over fluff. "
        "Avoid marketing language and avoid lists. "
        "Output exactly one sentence without quotes."
    )
    import re
    words = re.findall(r"\w+", content)
    cleaned_content = " ".join(words)
    user_prompt = (
        "Summarize the following based on the URL and content below.\n\n"
        f"URL: {url}\n"
        f"Content: {cleaned_content}"
    )

    import time
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model="meta-llama/llama-3.3-70b-instruct",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=100,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error summarizing page {url} (attempt {attempt+1}/3): {e}", exc_info=True)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    return ""