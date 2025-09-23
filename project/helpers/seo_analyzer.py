from bs4 import BeautifulSoup


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
    # Guard against non-string or empty HTML to avoid false "all missing" results
    if not isinstance(html, str):
        try:
            html = html.decode("utf-8", errors="ignore")  # handle bytes-like input
        except Exception:
            html = ""
    if not html.strip():
        # Explicitly return minimal score and single issue for clarity
        return {
            "score": 0,
            "explanation": "Empty or invalid HTML input.",
        }

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

    # Extra checks for more detailed analysis
    # Word count in body
    body = soup.find("body")
    if body:
        words = body.get_text(separator=" ").split()
        word_count = len(words)
        if word_count < 300:
            score -= 5
            issues.append(f"Low word count ({word_count}, recommended 300+).")

    # Check H2 presence
    if not soup.find("h2"):
        score -= 3
        issues.append("Missing <h2> tags for content structure.")

    # Favicon
    favicon = soup.find("link", rel=lambda v: v and "icon" in v.lower())
    if not favicon:
        score -= 2
        issues.append("Missing favicon link.")

    # Meta keywords
    meta_keywords = soup.find("meta", attrs={"name": "keywords"})
    if meta_keywords:
        issues.append("Meta keywords tag found (deprecated, should be removed).")

    # Structured data (JSON-LD)
    ld_json = soup.find("script", type="application/ld+json")
    if not ld_json:
        score -= 3
        issues.append("Missing structured data (JSON-LD).")

    # Text-to-HTML ratio
    text_len = len(body.get_text(" ", strip=True)) if body else 0
    html_len = len(html)
    if html_len > 0:
        ratio = (text_len / html_len) * 100
        if ratio < 10:
            score -= 5
            issues.append(f"Low text-to-HTML ratio ({ratio:.1f}%).")

    # Clamp score between 0 and 100
    score = max(0, min(100, score))

    # Keep explanation concise (no external refinement)
    explanation = "; ".join(issues) if issues else "All key SEO checks passed."

    return {
        "score": score,
        "explanation": explanation,
    }