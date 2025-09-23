from bs4 import BeautifulSoup
from project.libs.openrouter_client import refine_explanation


def analyze_html(html: str) -> dict:
    """
    Analyze HTML content for basic SEO elements and return a score with explanation.

    Args:
        html (str): The HTML content as a string.

    Returns:
        dict: {
            "score": int (0–100),
            "explanation": str (summary of issues/warnings)
        }
    """
    soup = BeautifulSoup(html, "html.parser")
    score = 100
    issues = []

    # Check title
    title_tag = soup.find("title")
    if not title_tag or not title_tag.text.strip():
        score -= 10
        issues.append("Missing <title> tag.")
    else:
        title_length = len(title_tag.text.strip())
        if title_length < 50 or title_length > 60:
            score -= 5
            issues.append(f"<title> length is {title_length} chars (ideal 50-60).")

    # Meta description
    description = soup.find("meta", attrs={"name": "description"})
    if not description or not description.get("content"):
        score -= 10
        issues.append("Missing meta description.")
    else:
        desc_length = len(description.get("content", "").strip())
        if desc_length < 50 or desc_length > 160:
            score -= 5
            issues.append(f"Meta description length is {desc_length} chars (ideal 50-160).")

    # Canonical link
    canonical = soup.find("link", rel="canonical")
    if not canonical or not canonical.get("href"):
        score -= 5
        issues.append("Missing canonical link.")

    # H1 tags
    h1_tags = soup.find_all("h1")
    if not h1_tags:
        score -= 10
        issues.append("Missing <h1> tag.")
    elif len(h1_tags) > 1:
        score -= 5
        issues.append("Multiple <h1> tags found (only one preferred).")

    # Images alt attributes
    img_tags = soup.find_all("img")
    for img in img_tags:
        if not img.get("alt"):
            score -= 2
            issues.append("Image missing alt attribute.")

    # Meta charset
    charset = soup.find("meta", attrs={"charset": True})
    if not charset:
        # Check for http-equiv as fallback
        meta_content_type = soup.find("meta", attrs={"http-equiv": "Content-Type"})
        if not meta_content_type:
            score -= 5
            issues.append("Missing meta charset tag.")
        elif "utf-8" not in meta_content_type.get("content", "").lower():
            score -= 3
            issues.append("Meta charset not set to UTF-8.")
    else:
        if charset.get("charset", "").lower() != "utf-8":
            score -= 3
            issues.append("Charset is not UTF-8.")

    # Viewport
    viewport = soup.find("meta", attrs={"name": "viewport"})
    if not viewport:
        score -= 5
        issues.append("Missing viewport meta tag for responsiveness.")

    # OpenGraph tags
    og_tags = ["og:title", "og:description", "og:image"]
    for og in og_tags:
        if not soup.find("meta", property=og):
            score -= 3
            issues.append(f"Missing OpenGraph tag: {og}.")

    # Robots meta tag
    robots = soup.find("meta", attrs={"name": "robots"})
    if not robots:
        score -= 5
        issues.append("Missing robots meta tag (recommended: index, follow).")
    else:
        content = robots.get("content", "").lower()
        if "index" not in content or "follow" not in content:
            score -= 3
            issues.append(f"Robots meta not best practice: '{content}'.")

    # Clamp score between 0 and 100
    score = max(0, min(100, score))

    explanation = "; ".join(issues) if issues else "All key SEO checks passed."
    refined = refine_explanation(explanation)

    return {
        "score": score,
        "explanation": refined,
    }