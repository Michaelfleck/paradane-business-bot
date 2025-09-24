from __future__ import annotations

"""
Website Report generator.

Expands BUSINESS_PAGE[INDEX]_* placeholders in project/template/website-report.html
using rows from business_pages for a given business_id.

Per row placeholders:
- {BUSINESS_PAGE[INDEX]_URL}
- {BUSINESS_PAGE[INDEX]_URL_SUMMARY}
- {BUSINESS_PAGE[INDEX]_URL_TYPE}
- {BUSINESS_PAGE[INDEX]_URL_SCORE}
- {BUSINESS_PAGE[INDEX]_URL_LOAD_TIME}
- {BUSINESS_PAGE[INDEX]_URL_SEO_SCORE}
- {BUSINESS_PAGE[INDEX]_URL_SEO_EXPLANATION}
"""

import os
from typing import Any, Dict, List
import html

from project.libs.supabase_client import get_client
from project.reporting.renderer import (
    render_template,
    render_indexed_line_block,
    render_indexed_block_between,
)


def _fetch_pages(business_id: str) -> List[Dict[str, Any]]:
    client = get_client()
    # Attempt ordering by updated_at desc; fallback to id asc if not available
    try:
        resp = (
            client.table("business_pages")
            .select(
                "url,summary,page_type,page_speed_score,time_to_interactive_ms,seo_score,seo_explanation,updated_at,id"
            )
            .eq("business_id", business_id)
            .order("updated_at", desc=True)
            .execute()
        )
    except Exception:
        resp = (
            client.table("business_pages")
            .select(
                "url,summary,page_type,page_speed_score,time_to_interactive_ms,seo_score,seo_explanation,id"
            )
            .eq("business_id", business_id)
            .order("id")
            .execute()
        )
    data = getattr(resp, "data", None)
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    return data or []


def _s(val: Any) -> str:
    if val is None:
        return "N/A"
    s = str(val).strip()
    return s if s else "N/A"

def _escape_html(val: Any) -> str:
    """Return a safe, HTML-escaped string. 'N/A' when missing/blank."""
    if val is None:
        return "N/A"
    s = str(val)
    if not s.strip():
        return "N/A"
    # Escape special chars and quotes so HTML-like text is shown literally
    return html.escape(s, quote=True)






def _ms_to_seconds_str(ms: Any) -> str:
    """Convert milliseconds to a human-friendly seconds string (e.g., 2072 -> '2.07').
    Returns 'N/A' when value is missing or blank; falls back to _s on parsing errors.
    """
    if ms is None or (isinstance(ms, str) and ms.strip() == ""):
        return "N/A"
    try:
        sec = float(ms) / 1000.0
        # Format to 2 decimals, then strip trailing zeros and dot
        formatted = f"{sec:.2f}".rstrip("0").rstrip(".")
        return formatted
    except Exception:
        return _s(ms)


def generateWebsiteReport(business_id: str) -> str:
    template_path = os.path.join("project", "template", "website-report.html")
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template_html = f.read()
    except Exception:
        template_html = ""

    pages = _fetch_pages(business_id)

    # Sort pages by URL depth (shallower first), then by URL alphabetically.
    # Depth = number of non-empty path segments, e.g.:
    #   https://supper.land           -> depth 0
    #   https://supper.land/about     -> depth 1
    #   https://supper.land/about/1   -> depth 2
    from urllib.parse import urlparse

    def _url_depth(u: Any) -> int:
        try:
            if not u:
                return 0
            parsed = urlparse(str(u))
            # Normalize path: strip leading/trailing slashes and split
            path = (parsed.path or "").strip("/")
            if not path:
                return 0
            # Filter out empty segments to be safe
            return len([seg for seg in path.split("/") if seg])
        except Exception:
            return 0

    pages = sorted(
        pages,
        key=lambda p: (_url_depth(p.get("url")), str(p.get("url") or "")),
    )

    # If the template has other global placeholders, provide them here.
    html = render_template(template_html, {})

    def render_row(index: int, row_template: str) -> str:
        p = pages[index]
        return (
            row_template.replace("{BUSINESS_PAGE[INDEX]_URL}", _s(p.get("url")))
            .replace("{BUSINESS_PAGE[INDEX]_URL_SUMMARY}", _escape_html(p.get("summary")))
            .replace("{BUSINESS_PAGE[INDEX]_URL_TYPE}", _s(p.get("page_type")))
            .replace("{BUSINESS_PAGE[INDEX]_URL_SCORE}", _s(p.get("page_speed_score")))
            .replace(
                "{BUSINESS_PAGE[INDEX]_URL_LOAD_TIME}", _ms_to_seconds_str(p.get("time_to_interactive_ms"))
            )
            .replace("{BUSINESS_PAGE[INDEX]_URL_SEO_SCORE}", _s(p.get("seo_score")))
            .replace(
                "{BUSINESS_PAGE[INDEX]_URL_SEO_EXPLANATION}",
                _escape_html(p.get("seo_explanation")),
            )
        )

    # Prefer duplicating the entire <tr> row for PageSpeed table if markers exist; else fallback to single-line duplication.
    # We assume website-report.html wraps the pagespeed row between <!--PAGESPEED_ROW_START--> and <!--PAGESPEED_ROW_END--> markers.
    if "<!--PAGESPEED_ROW_START-->" in html and "<!--PAGESPEED_ROW_END-->" in html:
        def render_ps_row(index: int, block_template: str) -> str:
            p = pages[index]
            return (
                block_template
                .replace("{BUSINESS_PAGE[INDEX]_URL}", _s(p.get("url")))
                .replace("{BUSINESS_PAGE[INDEX]_URL_TYPE}", _s(p.get("page_type")))
                .replace("{BUSINESS_PAGE[INDEX]_URL_SCORE}", _s(p.get("page_speed_score")))
                .replace("{BUSINESS_PAGE[INDEX]_URL_LOAD_TIME}", _ms_to_seconds_str(p.get("time_to_interactive_ms")))
                .replace("{BUSINESS_PAGE[INDEX]_URL_SUMMARY}", _escape_html(p.get("summary")))
            )
        html = render_indexed_block_between(
            html,
            start_marker="<!--PAGESPEED_ROW_START-->",
            end_marker="<!--PAGESPEED_ROW_END-->",
            item_count=len(pages),
            render_for_index=render_ps_row,
        )
    else:
        # Fallback: duplicate a single line containing URL placeholder (legacy behavior)
        html = render_indexed_line_block(
            html,
            match_placeholder="{BUSINESS_PAGE[INDEX]_URL}",
            item_count=len(pages),
            render_for_index=render_row,
        )

    # Duplicate the entire <tr> row for SEO table if markers exist.
    if "<!--SEO_ROW_START-->" in html and "<!--SEO_ROW_END-->" in html:
        def render_seo_row(index: int, block_template: str) -> str:
            p = pages[index]
            return (
                block_template
                .replace("{BUSINESS_PAGE[INDEX]_URL}", _s(p.get("url")))
                .replace("{BUSINESS_PAGE[INDEX]_URL_SEO_SCORE}", _s(p.get("seo_score")))
                .replace("{BUSINESS_PAGE[INDEX]_URL_SEO_EXPLANATION}", _escape_html(p.get("seo_explanation")))
            )
        html = render_indexed_block_between(
            html,
            start_marker="<!--SEO_ROW_START-->",
            end_marker="<!--SEO_ROW_END-->",
            item_count=len(pages),
            render_for_index=render_seo_row,
        )

    return html


if __name__ == "__main__":
    print(generateWebsiteReport("e0sLN8eLzpqhRb4wNNbbhg"))