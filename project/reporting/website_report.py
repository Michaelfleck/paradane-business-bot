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
from datetime import datetime
import pathlib

from project.libs.supabase_client import get_client
from project.reporting.renderer import (
    render_template,
    render_indexed_line_block,
)
from project.reporting.config import get_report_config
from project.reporting.pdf_service import html_to_pdf_file, upload_to_supabase_storage, _project_root_abs, _inject_report_styles, _config_to_options


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

    # Compute aggregates
    def _avg(nums):
        nums = [float(x) for x in nums if x is not None and str(x).strip() != ""]
        if not nums:
            return None
        return sum(nums) / len(nums)

    avg_speed_ms = _avg([p.get("time_to_interactive_ms") for p in pages])
    avg_speed_str = _ms_to_seconds_str(avg_speed_ms) if avg_speed_ms is not None else "N/A"

    avg_ps_score = _avg([p.get("page_speed_score") for p in pages])
    avg_ps_score_str = "N/A" if avg_ps_score is None else str(int(round(avg_ps_score)))

    avg_seo_score = _avg([p.get("seo_score") for p in pages])
    avg_seo_score_str = "N/A" if avg_seo_score is None else str(int(round(avg_seo_score)))

    # If the template has other global placeholders, provide them here.
    html = render_template(
        template_html,
        {
            "BUSINESS_PAGE_TOTAL_AVERAGE_SPEED": avg_speed_str,
            "BUSINESS_PAGE_TOTAL_AVERAGE_SCORE": avg_ps_score_str,
            "BUSINESS_PAGE_TOTAL_AVERAGE_SEO_SCORE": avg_seo_score_str,
        },
    )

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
        # NOTE: render_indexed_block_between was removed from renderer in favor of render_indexed_block.
        # Keep compatibility by calling the new function if available.
        from project.reporting.renderer import render_indexed_block as _rib
        html = _rib(
            html,
            row_start_marker="<!--PAGESPEED_ROW_START-->",
            row_end_marker="<!--PAGESPEED_ROW_END-->",
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
        from project.reporting.renderer import render_indexed_block as _rib
        html = _rib(
            html,
            row_start_marker="<!--SEO_ROW_START-->",
            row_end_marker="<!--SEO_ROW_END-->",
            item_count=len(pages),
            render_for_index=render_seo_row,
        )

    return html


def generateWebsiteReportPdf(business_id: str, to_path: str | None = None, upload: bool | None = None) -> str:
    """
    Render the Website Report PDF for a given business_id.

    Returns:
        str: Local file path if upload is False, otherwise the public URL from Supabase Storage.
    """
    cfg = get_report_config()
    html = generateWebsiteReport(business_id)
    html_with_styles = _inject_report_styles(html)

    out_dir = cfg.REPORTS_OUTPUT_DIR or "./tmp/reports"
    os.makedirs(out_dir, exist_ok=True)
    if to_path is None:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        to_path = os.path.join(out_dir, f"website-{business_id}-{ts}.pdf")

    base_url = pathlib.Path(_project_root_abs()).as_uri()
    html_to_pdf_file(html_with_styles, to_path, base_url=base_url, options=_config_to_options())

    do_upload = cfg.PDF_UPLOAD_ENABLED if upload is None else upload
    if do_upload:
        return upload_to_supabase_storage(to_path, bucket=cfg.STORAGE_BUCKET_REPORTS)

    return to_path


if __name__ == "__main__":
    print(generateWebsiteReport("e0sLN8eLzpqhRb4wNNbbhg"))