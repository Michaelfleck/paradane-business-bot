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
            if not classification:
                raise ValueError("Empty response from OpenRouter")
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
            content = resp.choices[0].message.content.strip()
            if not content:
                raise ValueError("Empty response from OpenRouter")
            return content
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
        "Include key insights on visibility, competitors, and no strategic recommendations, just facts. "
        "Keep it brief, professional, factual and straight to the point, suitable for business reports. "
        "Structure it in one paragraph with clear section (150 words maximum). "
        "No markdown formatting, no section titles, just one paragraph."
    )

    user_prompt = f"""
Generate a summary for the business ranking in the category: {data.get('category', 'N/A')}

Key Data:
- Grid Size: {data.get('grid_size', 56)} points
- Gap Distance: {data.get('gap_miles', 'N/A')} miles between points
- Average Rank: {data.get('average_rank', 'N/A'):.2f}
- Visibility Coverage: {data.get('visibility_coverage', 0):.1f}% of grid points have rankings
- Top Positions (#1 ranks): {data.get('top_positions', 0)} points
- Best performing direction: {data.get('best_direction', 'N/A')} (avg rank: {data.get('best_direction_rank', 'N/A'):.2f})
- Worst performing direction: {data.get('worst_direction', 'N/A')} (avg rank: {data.get('worst_direction_rank', 'N/A'):.2f})
- Direction averages: {', '.join([f"{dir}: {avg:.2f}" for dir, avg in data.get('direction_averages', {}).items()])}
- Low visibility directions (rank > 10): {', '.join(set(data.get('low_visibility_points', [])))}
- Top 10 competitors (by average rank):
{chr(10).join([f"  - {comp['name']} (avg rank: {comp['avg_rank']:.2f}, categories: {', '.join(comp['categories'])}, Google reviews: {comp['user_ratings_total']})" for comp in data.get('top_10_competitors', [])])}
- Current business reviews: Google reviews {data.get('current_reviews', {}).get('google', 'N/A')}

Focus on:
- Overall visibility and ranking performance with specific metrics
- Geographic patterns and directional performance variations
- Key competitors and their strengths
- Areas with low visibility and strategic implications
- Review volume comparison
- Actionable strategic insights based on geographic data
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
                max_tokens=200,
            )
            content = resp.choices[0].message.content.strip()
            if not content:
                raise ValueError("Empty response from OpenRouter")
            return content
        except Exception as e:
            logger.error(f"Error generating rank summary (attempt {attempt+1}/3): {e}", exc_info=True)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    return "Summary generation failed due to API error."