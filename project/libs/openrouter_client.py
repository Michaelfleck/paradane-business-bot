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


def generate_rank_summary(data: dict) -> str:
    """Generate comprehensive summary for business rank local report using OpenRouter"""
    if not client:
        return "Summary generation unavailable: OpenRouter client not configured."

    system_instruction = (
        "You are a business intelligence analyst generating professional summaries for local business ranking reports. "
        "Create a comprehensive, readable summary based on the provided data. "
        "Include key insights on visibility, competitors, and strategic recommendations. "
        "Keep it professional, factual, and suitable for business reports. "
        "Structure it in one paragraph with clear section. "
        "No markdown formatting, no section titles, just one paragraph."
    )

    user_prompt = f"""
Generate a summary for the business ranking in the category: {data.get('category', 'N/A')}

Key Data:
- Grid Size: {data.get('grid_size', 36)} points
- Gap Distance: {data.get('gap_miles', 'N/A')} miles between points
- Ranks at each point: {data.get('ranks', [])}
- Low visibility points (rank > 10): {', '.join(data.get('low_visibility_points', []))}
- Top 5 competitors (by average rank):
{chr(10).join([f"  - {comp['name']} (avg rank: {comp['avg_rank']:.2f}, categories: {', '.join(comp['categories'])}, Google reviews: {comp['user_ratings_total']})" for comp in data.get('top_5_competitors', [])])}
- Current business reviews: Yelp {data.get('current_reviews', {}).get('yelp', 'N/A')}, Google {data.get('current_reviews', {}).get('google', 'N/A')}

Focus on:
- Overall visibility and ranking performance
- Key competitors and their strengths
- Areas with low visibility
- Review volume comparison
- Strategic insights
"""
    
    import time
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model="meta-llama/llama-3.3-70b-instruct",
                messages=[
                    {"role": "system", "content": system_instruction},
                    {"role": "user", "content": user_prompt},
                ],
                max_tokens=500,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"Error generating rank summary (attempt {attempt+1}/3): {e}", exc_info=True)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    return "Summary generation failed due to API error."