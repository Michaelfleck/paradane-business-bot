from __future__ import annotations

"""
Business Report orchestration.

Generates a fully rendered HTML report by binding data from Supabase to the
project/template/business-report.html template using the renderer and utils.
"""

from typing import Any, Dict, List, Optional, Tuple
import base64
import logging
import os
from urllib.parse import urlencode, quote_plus
import pathlib
from datetime import datetime
import concurrent.futures

from project.reporting.config import get_report_config
from project.reporting.renderer import render_template, render_list_block, render_indexed_block
from project.reporting.utils.address import parseAddressFromDisplay, geocodeAddressToCoords
from project.reporting.utils.hours import formatBusinessHours
from project.reporting.utils.web import toRootDomain, buildGooglePlaceUrl, collectBusinessEmails, collectContactPages, collectBusinessSocials
from project.reporting.utils.phone import normalizePhone
from project.libs.supabase_client import get_client
from project.libs.openrouter_client import generate_rank_summary
from project.libs.openrouter_client import summarize_page as or_summarize_page
from project.libs.openrouter_client import generate_business_summary
from project.reporting.pdf_service import html_to_pdf_file, upload_to_supabase_storage, _project_root_abs, _inject_report_styles, _config_to_options
from project.helpers.zoho_integration import attach_pdf_to_lead, get_lead_id_by_business_id

# Logger
logger = logging.getLogger("project.reporting.business_report")

# 1x1 transparent GIF data URL
TRANSPARENT_GIF_DATA_URL = "data:image/gif;base64," + base64.b64encode(
    base64.b64decode(
        "R0lGODlhAQABAPAAAP///wAAACH5BAAAAAAALAAAAAABAAEAAAICRAEAOw=="
    )
).decode("ascii")


def _safe_get(obj: Any, path: str, default: Any = None) -> Any:
    """
    Safely get a nested property by dotted path from dict-like objects.
    """
    try:
        cur = obj
        for part in path.split("."):
            if cur is None:
                return default
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                # object attribute or key access fallback
                cur = getattr(cur, part, None)
        return cur if cur is not None else default
    except Exception:
        return default


def _fetch_business(business_id: str) -> Dict[str, Any]:
    client = get_client()
    # Select wide columns we may need; Supabase will ignore unknowns
    # Columns based on actual schema (see tmp/businesses_rows.csv)
    fields = ",".join(
        [
            "id",
            "name",
            "url",
            "phone",
            "display_phone",
            "price",
            "review_count",  # present
            # "reviews_count",  # not present in schema; remove to avoid 42703
            "rating",
            "is_closed",
            "categories",
            "type",
            "types",
            "hours",
            "business_hours",
            "attributes",
            "website",
            "yelp_menu_url",
            "coordinates",
            "geometry",
            "location",
            "display_address",
            "formatted_address",
            "google_enrichment",
            "user_ratings_total",
            "image_url",
        ]
    )
    # Important: execute() is required to materialize the query; .single() returns a builder.
    resp = (
        client.table("businesses")
        .select(fields)
        .eq("id", business_id)
        .single()
        .execute()
    )
    data = getattr(resp, "data", None)
    # Normalize possible shapes to a dict
    if isinstance(data, list):
        data = data[0] if data else None
    if isinstance(data, dict):
        return data
    return {}


def _fetch_business_pages(business_id: str) -> List[Dict[str, Any]]:
    client = get_client()
    resp = client.table("business_pages").select("url,email,page_type").eq("business_id", business_id).execute()
    data = getattr(resp, "data", resp)
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    return data or []


def _resolve_status(biz: Dict[str, Any]) -> str:
    """
    Status priority:
      Temporary Closed supersedes Closed supersedes Open.
      - attributes.business_temp_closed -> Temporary Closed if True
      - is_closed -> Closed if True
      - google_enrichment.business_status:
          "OPERATIONAL" => Open else Closed
    """
    temp_closed = _safe_get(biz, "attributes.business_temp_closed", False)
    if temp_closed:
        return "Temporary Closed"
    if biz.get("is_closed"):
        return "Closed"
    ge_status = _safe_get(biz, "google_enrichment.business_status")
    if isinstance(ge_status, str):
        return "Open" if ge_status.upper() == "OPERATIONAL" else "Closed"
    return "Open"


def _resolve_categories(biz: Dict[str, Any]) -> str:
    """
    Join category titles and types preserving order and de-duplicating identical titles.
    Handles both array of strings and array of objects with 'title' key for categories.
    Converts types from snake_case to title case.
    """
    def _title_case(s: str) -> str:
        return ' '.join(word.capitalize() for word in s.split('_'))

    categories = biz.get("categories") or []
    type_text = biz.get("type") or ""
    types_json = biz.get("types") or []
    seen = set()
    titles: List[str] = []

    # Process categories
    for c in categories:
        if isinstance(c, str):
            title = c.strip()
        else:
            title = (c or {}).get("title")
        if not title:
            continue
        if title not in seen:
            seen.add(title)
            titles.append(title)

    # Process type or types
    if type_text:
        title = _title_case(type_text) if '_' in type_text else type_text
        if title not in seen:
            seen.add(title)
            titles.append(title)
    else:
        for t in types_json:
            if isinstance(t, str):
                title = _title_case(t.strip())
            else:
                continue
            if not title:
                continue
            if title not in seen:
                seen.add(title)
                titles.append(title)

    return ", ".join(titles) if titles else "N/A"


def _resolve_website(biz: Dict[str, Any]) -> Optional[str]:
    """
    Website precedence:
      - attributes.menu_url
      - website
      - google_enrichment.website
    Normalize to root domain.
    """
    from project.helpers.crawler import normalize_homepage_url  # prefer if available

    candidates = [
        _safe_get(biz, "attributes.menu_url"),
        biz.get("website"),
        _safe_get(biz, "google_enrichment.website"),
    ]
    for url in candidates:
        if url:
            try:
                # normalize_homepage_url returns scheme+host of given url
                root = normalize_homepage_url(url)
                # Also reduce to registrable root domain per spec
                root2 = toRootDomain(root)
                if root2:
                    return root2
            except Exception:
                rd = toRootDomain(str(url))
                if rd:
                    return rd
    return None


def _resolve_hours(biz: Dict[str, Any]) -> str:
    """
    Business hours precedence:
      - businesses.business_hours first
      - else opening_hours (Google)
      - else Yelp businesses.hours
    """
    bh = biz.get("business_hours")
    if bh:
        return formatBusinessHours(bh)
    opening_hours = _safe_get(biz, "google_enrichment.opening_hours") or _safe_get(biz, "opening_hours")
    if opening_hours:
        return formatBusinessHours(opening_hours)
    yelp_hours = biz.get("hours")
    if yelp_hours:
        return formatBusinessHours(yelp_hours)
    return "N/A"


def _resolve_address(biz: Dict[str, Any]) -> Tuple[str, str, str, str]:
    """
    Address resolution using parsing from display_address/formatted_address as fallback.
    """
    # Try structured location first
    addr_obj: Dict[str, Any] = {}
    location = biz.get("location") or {}
    if isinstance(location, dict):
        addr_obj = {
            "address1": location.get("address1"),
            "city": location.get("city"),
            "state": location.get("state"),
            "country": location.get("country"),
        }
    # If missing pieces, attempt fallback parsing
    if not all([addr_obj.get("address1"), addr_obj.get("city"), addr_obj.get("state")]):
        fallback = {
            "display_address": biz.get("display_address"),
            "formatted_address": biz.get("formatted_address") or _safe_get(biz, "google_enrichment.formatted_address"),
            "location": location,
        }
        parsed = parseAddressFromDisplay(fallback)
        for k in ["address1", "city", "state", "country"]:
            if not addr_obj.get(k) and parsed.get(k):
                addr_obj[k] = parsed[k]
    # Normalize "N/A"
    return (
        addr_obj.get("address1") or "N/A",
        addr_obj.get("city") or "N/A",
        addr_obj.get("state") or "N/A",
        addr_obj.get("country") or "N/A",
    )


def _resolve_coords(biz: Dict[str, Any], addr: Dict[str, Optional[str]]) -> Tuple[Optional[float], Optional[float]]:
    """
    Coordinates resolution using businesses.coordinates or geometry; else geocode.
    """
    lat = None
    lng = None
    coords = biz.get("coordinates") or {}
    lat = coords.get("latitude")
    lng = coords.get("longitude")
    if lat is None or lng is None:
        geom = biz.get("geometry") or _safe_get(biz, "google_enrichment.geometry") or {}
        loc = _safe_get(geom, "location", {})
        lat = lat if lat is not None else (loc.get("lat") if isinstance(loc, dict) else None)
        lng = lng if lng is not None else (loc.get("lng") if isinstance(loc, dict) else None)
    if lat is None or lng is None:
        # geocode
        ge = geocodeAddressToCoords(addr)
        lat = lat if lat is not None else ge.get("lat")
        lng = lng if lng is not None else ge.get("lng")
    try:
        lat_f = float(lat) if lat is not None else None
        lng_f = float(lng) if lng is not None else None
    except Exception:
        lat_f, lng_f = None, None
    return lat_f, lng_f


def _build_static_map_url(lat: Optional[float], lng: Optional[float]) -> Optional[str]:
    """
    Build Geoapify Static Map URL if coords present, else None.
    """
    if lat is None or lng is None:
        return None
    cfg = get_report_config()
    if not cfg.GEOAPIFY_API_KEY:
        return None
    zoom_level = cfg.MAP_DEFAULT_ZOOM
    size = cfg.MAP_DEFAULT_SIZE or "1000x800"
    width, height = size.split('x')
    params = {
        "style": "osm-bright",
        "width": width,
        "height": height,
        "center": f"lonlat:{lng},{lat}",
        "zoom": str(zoom_level),
        "marker": f"lonlat:{lng},{lat};color:#ff0000;size:medium",
        "apiKey": cfg.GEOAPIFY_API_KEY,
    }
    return "https://maps.geoapify.com/v1/staticmap?" + urlencode(params)


def _calculate_grid_positions(center_lat: float, center_lng: float, grid_rows: int = 5, grid_cols: int = 5, spacing_km: float = 1.60934) -> List[Tuple[float, float]]:
    """
    Calculate lat/lng positions for a grid_rows x grid_cols grid centered on center_lat, center_lng.
    Uses fixed spacing_km between adjacent bubbles in both axes (default 1.60934 km).
    Returns list of (lat, lng) tuples in row-major order.
    """
    import math
    positions: List[Tuple[float, float]] = []
    # Approximate conversion: 1 km ≈ 1/110.574 deg latitude
    lat_km_to_deg = 1.0 / 110.574  # ~0.00904371733
    # Longitude degrees per km varies with latitude
    lng_km_to_deg = 1.0 / (111.320 * math.cos(math.radians(center_lat)))
    lat_spacing = spacing_km * lat_km_to_deg
    lng_spacing = spacing_km * lng_km_to_deg
    # Offset to center the grid
    row_offset = (grid_rows - 1) / 2.0
    col_offset = (grid_cols - 1) / 2.0
    # Build grid centered on the business
    for row in range(grid_rows):
        for col in range(grid_cols):
            lat = center_lat + (row - row_offset) * lat_spacing  # negative row moves north for top rows
            lng = center_lng + (col - col_offset) * lng_spacing
            positions.append((lat, lng))
    return positions

def _build_heatmap_map_url(center_lat: float, center_lng: float, category: str, target_place_id: str) -> Tuple[Optional[str], float, List[Optional[int]], List[Tuple[float, float]], List[List[Dict[str, Any]]]]:
    """
    Build a heatmap-like image:
      - Base: Google Static Map with dynamic zoom to fit the 7x7 grid
      - Overlay: 7x7 bubbles spaced exactly approximately 1.666 miles apart
      - Each bubble shows the rank at that location; ranks up to 20
      - Bubble color: green 1-15, yellow 16-30, orange 31-45, red 46+
    Returns a tuple of (data URL of the composed PNG, average rank, ranks list, grid_positions, competitors_per_point).
    """
    import math
    import requests
    from io import BytesIO
    from PIL import Image, ImageDraw, ImageFont

    cfg = get_report_config()
    if not cfg.GOOGLE_API_KEY:
        return TRANSPARENT_GIF_DATA_URL, None

    # Parse desired size WxH (e.g., "600x400")
    try:
        w_str, h_str = (cfg.MAP_DEFAULT_SIZE or "1000x1000").split("x")
        width, height = int(w_str), int(h_str)
    except Exception:
        width, height = 1000, 1000

    # Build fixed approximately 1.666 miles grid in geographic coords
    spacing_km = 1.666 * 1.60934
    grid_positions = _calculate_grid_positions(center_lat, center_lng, grid_rows=5, grid_cols=5, spacing_km=spacing_km)

    zoom = 12

    # Request a clean static map without markers; we'll draw overlays ourselves
    params = {
        "center": f"{center_lat},{center_lng}",
        "zoom": str(zoom),
        "size": f"{width}x{height}",
        "key": cfg.GOOGLE_API_KEY,
        "scale": "2",
    }
    base_url = "https://maps.googleapis.com/maps/api/staticmap?" + urlencode(params)

    try:
        resp = requests.get(base_url, timeout=30)
        resp.raise_for_status()
        img = Image.open(BytesIO(resp.content)).convert("RGBA")
    except Exception as e:
        logger.warning(f"Failed to fetch base map: {e}")
        return TRANSPARENT_GIF_DATA_URL, None, [], [], []

    draw = ImageDraw.Draw(img)

    # Utility: Web Mercator projection helpers for pixel coordinate mapping at given zoom
    def _latlng_to_pixel_xy(lat: float, lng: float, z: int) -> Tuple[float, float]:
        siny = math.sin(lat * math.pi / 180.0)
        siny = min(max(siny, -0.9999), 0.9999)
        tile_size = 256
        scale = (1 << z) * tile_size
        x = (lng + 180.0) / 360.0 * scale
        y = (0.5 - math.log((1 + siny) / (1 - siny)) / (4 * math.pi)) * scale
        return x, y

    def _pixel_xy_to_point(px: float, py: float, center_px: float, center_py: float, img_w: int, img_h: int) -> Tuple[int, int]:
        dx = px - center_px
        dy = py - center_py
        x_img = int(img_w / 2 + dx)
        y_img = int(img_h / 2 + dy)
        return x_img, y_img

    center_px, center_py = _latlng_to_pixel_xy(center_lat, center_lng, zoom)

    # For each grid position, search for competitors and find rank
    from project.libs.google_client import GoogleClient
    client = GoogleClient()
    ranks: List[Optional[int]] = []
    competitors_per_point: List[List[Dict[str, Any]]] = []

    def search_position(lat_lng):
        lat, lng = lat_lng
        try:
            # Text search
            comps_text = client.search_competitors_in_category(category, lat, lng)
            rank_text = 0
            for i, comp in enumerate(comps_text):
                if comp.get("place_id") == target_place_id:
                    rank_text = i + 1
                    break
            rank = rank_text if rank_text != 0 else 20

            logger.debug(f"Category '{category}' at ({lat:.6f},{lng:.6f}): text_rank={rank_text}, final_rank={rank}")
            return comps_text, rank
        except Exception as e:
            logger.warning(f"Error getting rank at {lat},{lng}: {e}")
            return [], 20

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(search_position, grid_positions))

    competitors_per_point = [r[0] for r in results]
    ranks = [r[1] for r in results]
    logger.info(f"Category '{category}': ranks = {ranks}")

    # Style constants - same size for all bubbles
    RADIUS = 25
    # Colors with 90% opacity
    GREEN = (20, 132, 50, 230)
    YELLOW = (244, 180, 0, 220)
    ORANGE = (255, 140, 0, 220)
    RED = (210, 43, 43, 200)
    WHITE = (255, 255, 255, 255)
    STROKE = (255, 255, 255, 255)

    # Font: use bold if available, else default
    try:
        font = ImageFont.truetype("arialbd.ttf", 22)
    except Exception:
        try:
            font = ImageFont.truetype("arial.ttf", 22)
        except Exception:
            font = ImageFont.load_default()

    def _color_for_rank(r: Optional[int]) -> Tuple[Tuple[int, int, int, int], str]:
        label = str(r)
        if r <= 5:
            return (GREEN, label)
        elif r <= 10:
            return (YELLOW, label)
        elif r <= 20:
            return (ORANGE, label)
        else:
            return (RED, "")

    # Draw bubbles at each grid point
    for (lat, lng), r in zip(grid_positions, ranks):
        px, py = _latlng_to_pixel_xy(lat, lng, zoom)
        x_img, y_img = _pixel_xy_to_point(px, py, center_px, center_py, img.width, img.height)
        color, label = _color_for_rank(r)
        # Outline circle for better contrast
        draw.ellipse([(x_img - RADIUS - 4, y_img - RADIUS - 4), (x_img + RADIUS + 4, y_img + RADIUS + 4)], fill=STROKE)
        draw.ellipse([(x_img - RADIUS, y_img - RADIUS), (x_img + RADIUS, y_img + RADIUS)], fill=color)
        # Center text precisely using bbox offsets (accounting for ascenders/descenders)
        bbox = draw.textbbox((0, 0), label, font=font)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        offset_x = -bbox[0]
        offset_y = -bbox[1]
        draw.text((x_img - tw / 2 + offset_x, y_img - th / 2.5 + offset_y), label, fill=WHITE, font=font)

    # Encode to base64 data URL
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    data_url = f"data:image/png;base64,{encoded}"
    valid_ranks_for_avg = [r for r in ranks if r is not None]
    average_rank = sum(valid_ranks_for_avg) / len(valid_ranks_for_avg) if valid_ranks_for_avg else None
    return data_url, average_rank, ranks, grid_positions, competitors_per_point


def _reorder_emails_by_domain(emails: List[str], website_root: Optional[str]) -> List[str]:
    """
    If website domain is known, bring emails whose domain matches to the front.
    """
    if not website_root:
        return emails
    host = website_root.split("://", 1)[-1]
    def _score(e: str) -> Tuple[int, int, str]:
        # Higher priority (0) for matching domain, then by len and lexicographic
        domain = e.split("@")[-1]
        match = 0 if domain.endswith(host) else 1
        return (match, len(e), e)
    return sorted(emails, key=_score)


def generateBusinessReport(business_id: str) -> str:
    """
    Generate the Business Report HTML for a given business_id.

    Steps:
      1) Load template file project/template/business-report.html.
      2) Fetch business row and business_pages via Supabase.
      3) Resolve fields per spec.
      4) Render placeholders and list block; return final HTML string.
    """
    # 1) Load template
    template_path = os.path.join("project", "template", "business-report.html")
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template_html = f.read()
    except Exception as e:
        logger.warning("Failed to read template: %s", e)
        template_html = ""

    # 2) Fetch data
    biz = _fetch_business(business_id)
    pages = _fetch_business_pages(business_id)

    # 3) Resolve fields
    name = biz.get("name") or "N/A"
    price = biz.get("price") or "N/A"
    # Yelp total reviews: use column present in schema (review_count)
    yelp_total_reviews = biz.get("review_count")
    if yelp_total_reviews is None:
        yelp_total_reviews = "N/A"
    yelp_rating = biz.get("rating") or "N/A"
    status = _resolve_status(biz)
    categories = _resolve_categories(biz)

    # Yelp URL (strip query params)
    raw_yelp = biz.get("url") or ""
    if raw_yelp and isinstance(raw_yelp, str):
        yelp_url = raw_yelp.split("?", 1)[0]
    else:
        yelp_url = "N/A"

    # Website
    website_root = _resolve_website(biz)
    website_url = website_root or "N/A"

    # Hours
    open_days = _resolve_hours(biz)

    # Address
    addr1, city, state, country = _resolve_address(biz)
    structured_addr = {"address1": None if addr1 == "N/A" else addr1, "city": None if city == "N/A" else city, "state": None if state == "N/A" else state, "country": None if country == "N/A" else country}

    # Coords
    lat, lng = _resolve_coords(biz, structured_addr)
    if lat is None or lng is None:
        logger.warning("Missing coordinates; map will show placeholder for business_id=%s", business_id)

    business_image_url = biz.get("image_url")

    if not business_image_url:
        # Use 1x1 transparent GIF per spec
        business_image_url = TRANSPARENT_GIF_DATA_URL

    # Static Map
    map_url = _build_static_map_url(lat, lng)

    if not map_url:
        # Use 1x1 transparent GIF per spec
        map_url = TRANSPARENT_GIF_DATA_URL

    # Ensure config is available
    cfg = get_report_config()

    # Emails
    emails = collectBusinessEmails(business_id)
    emails = _reorder_emails_by_domain(emails, website_root)
    emails_str = ", ".join(emails) if emails else "N/A"

    # Social links collected across pages
    socials = collectBusinessSocials(business_id)

    # Build socials <li> HTML list from collected platforms
    # Requirement:
    # - Display clickable links but show only @handle text (lowercase)
    # - Strip extra query/hash params from hrefs (canonicalize to scheme+host+path only)
    def _build_social_list_html(socials_dict: Dict[str, List[str]]) -> str:
        from urllib.parse import urlparse, urlunparse

        platform_items: Dict[str, List[str]] = {}
        seen: set = set()

        def _canonicalize_url(u: str) -> str | None:
            try:
                p = urlparse(u.strip())
                if not p.scheme or not p.netloc:
                    return None
                # remove query and fragment
                canon = urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
                # remove trailing slash (but keep root-only like "https://domain/" -> "https://domain/")
                if canon.endswith("/") and p.path not in ("", "/"):
                    canon = canon[:-1]
                return canon
            except Exception:
                return None

        def _last_segment(path: str) -> str:
            seg = path.strip("/").split("/")[-1] if path else ""
            return seg

        def _extract_handle(label_lower: str, href: str) -> str | None:
            try:
                p = urlparse(href)
                host = (p.netloc or "").lower()
                path = p.path or ""

                # platform-specific extraction
                if "instagram.com" in host:
                    h = _last_segment(path)  # /<handle>[/]
                elif "twitter.com" in host or "x.com" in host:
                    h = _last_segment(path)
                elif "facebook.com" in host:
                    # For pages, last segment is usually the page name; for profiles may be 'profile.php' -> no handle
                    last = _last_segment(path)
                    h = "" if last in ("profile.php", "") else last
                elif "tiktok.com" in host:
                    last = _last_segment(path)
                    h = last[1:] if last.startswith("@") else last
                elif "linkedin.com" in host:
                    # /company/<handle> or /in/<handle> etc.
                    parts = [seg for seg in path.split("/") if seg]
                    h = parts[-1] if parts else ""
                elif "youtube.com" in host:
                    # /@handle or /channel/<id> -> prefer @handle if present
                    parts = [seg for seg in path.split("/") if seg]
                    if parts and parts[0].startswith("@"):
                        h = parts[0][1:]
                    else:
                        h = _last_segment(path)
                elif "pinterest.com" in host or "threads.net" in host or "snapchat.com" in host or "whatsapp.com" in host:
                    h = _last_segment(path)
                else:
                    h = _last_segment(path)

                h = (h or "").strip()
                if not h:
                    return None
                # normalize to lowercase and ensure single leading @
                h = h.lstrip("@").lower()
                return f"@{h}"
            except Exception:
                return None

        platform_labels = {
            "facebook": "Facebook",
            "instagram": "Instagram",
            "twitter": "Twitter",
            "x": "Twitter",
            "linkedin": "LinkedIn",
            "tiktok": "TikTok",
            "youtube": "YouTube",
            "pinterest": "Pinterest",
            "whatsapp": "WhatsApp",
            "threads": "Threads",
            "snapchat": "Snapchat",
        }


        for plat, urls in (socials_dict or {}).items():
            label = platform_labels.get(str(plat).lower(), str(plat).title())
            for u in urls or []:
                if not isinstance(u, str):
                    continue
                if not (u.startswith("http://") or u.startswith("https://")):
                    continue
                href = _canonicalize_url(u)
                if not href:
                    continue
                handle = _extract_handle(str(plat).lower(), href)
                if not handle or len(handle.lstrip('@')) < 3:
                    continue
                key = (label, handle)
                if key in seen:
                    continue
                seen.add(key)
                if label not in platform_items:
                    platform_items[label] = []
                platform_items[label].append(f'<a href="{href}" target="_blank" rel="noopener noreferrer">{handle}</a>')

        items = []
        for label, links in platform_items.items():
            links_str = ", ".join(links)
            items.append(f'<li><b>{label}:</b> {links_str}</li>')

        return "\n        ".join(items) if items else ""
    
    social_list_html = _build_social_list_html(socials)

    # Contact pages
    contact_pages = collectContactPages(business_id)

    # Phone normalization
    cfg = get_report_config()
    raw_phone = biz.get("phone") or biz.get("display_phone")
    normalized_phone = normalizePhone(raw_phone, cfg.DEFAULT_PHONE_COUNTRY) or "N/A"

    # 9) Build context with "N/A" defaults
    ge: Dict[str, Any] = _safe_get(biz, "google_enrichment", {}) or {}

    # Editorial summary: Generate from business information using OpenRouter
    business_info = f"Name: {name}\nCategories: {categories}\nAddress: {addr1}, {city}, {state}, {country}\nPhone: {normalized_phone}\nWebsite: {website_url}\nRating: {yelp_rating} (Yelp)\nReviews: {yelp_total_reviews} (Yelp)\nStatus: {status}\nHours: {open_days}\nPrice: {price}\nYelp URL: {yelp_url}\nEmails: {emails_str}"
    editorial_summary = "—"
    if business_info.strip():
        try:
            editorial_summary = generate_business_summary(business_info)
        except Exception as e:
            logger.warning(f"Failed to generate editorial summary via OpenRouter: {e}")
            editorial_summary = "—"

    # Plus codes (global_code, compound_code) live under google_enrichment.plus_code per Google Places schema
    plus_code = ge.get("plus_code") if isinstance(ge, dict) else None
    if not isinstance(plus_code, dict):
        plus_code = {}

    context: Dict[str, Any] = {
        "BUSINESS_NAME": name,
        "BUSINESS_ADDRESS": addr1,
        "BUSINESS_CITY": city,
        "BUSINESS_STATE": state,
        "BUSINESS_COUNTRY": country,
        "BUSINESS_COORDS_LAT": f"{lat:.6f}" if lat is not None else "N/A",
        "BUSINESS_COORDS_LONG": f"{lng:.6f}" if lng is not None else "N/A",
        "BUSINESS_PRICE": price,
        "BUSINESS_YELP_TOTAL_REVIEWS": yelp_total_reviews,
        "BUSINESS_YELP_RATING": yelp_rating,
        "BUSINESS_STATUS": status,
        "BUSINESS_CATEGORIES": categories,
        "BUSINESS_OPEN_DAYS": open_days if open_days else "N/A",
        "BUSINESS_IMAGE_URL": business_image_url,
        "BUSINESS_MAP_IMAGE": map_url,
        "BUSINESS_WEBSITE_URL": website_url,
        "BUSINESS_YELP_URL": yelp_url,
        "BUSINESS_EMAILS": emails_str,
        "BUSINESS_PHONE": normalized_phone,
        "BUSINESS_SOCIALS_LIST": social_list_html,
        "BUSINESS_EDITORIAL_SUMMARY": editorial_summary,
    }

    # 10a) Expand Reviews block using google_enrichment.reviews
    # We render reviews into the block between the opening <div class="flex items-start gap-5"> and its closing sibling.
    try:
        reviews = []
        revs = ge.get("reviews")
        if isinstance(revs, list):
            # Sort most recent first if 'time' exists
            try:
                revs = sorted([r for r in revs if isinstance(r, dict)], key=lambda r: r.get("time", 0), reverse=True)
            except Exception:
                revs = [r for r in revs if isinstance(r, dict)]
            for r in revs:
                # Normalize fields with safe defaults
                profile_photo_url = r.get("profile_photo_url") or ""
                author_name = r.get("author_name") or "Anonymous"
                rating = r.get("rating") if r.get("rating") is not None else "N/A"
                text = r.get("text") or ""
                time_ago = r.get("relative_time_description") or ""
                # Clean text newlines to avoid layout issues
                if isinstance(text, str):
                    text = text.strip().replace("\r", " ").replace("\n", " ")
                reviews.append({
                    "BUSINESS_REVIEW[INDEX]_PROFILE_PHOTO_URL": profile_photo_url or TRANSPARENT_GIF_DATA_URL,
                    "BUSINESS_REVIEW[INDEX]_AUTHOR_NAME": author_name,
                    "BUSINESS_REVIEW[INDEX]_RATING": rating,
                    "BUSINESS_REVIEW[INDEX]_TEXT": text,
                    "BUSINESS_REVIEW[INDEX]_TIME_AGO": time_ago,
                })
        # Render the indexed block if marker lines exist
        if reviews:
            logger.debug("Reviews: prepared %d review rows", len(reviews))

            def _render_review(i: int, block_template: str) -> str:
                """
                Replace any {BUSINESS_REVIEW[INDEX]_...} placeholders in the provided block_template
                with the concrete values from reviews[i], by converting [INDEX] -> [i] dynamically.
                This uses a regex so we replace all review placeholders present in the block without
                having to enumerate each key explicitly.
                """
                import re
                row = reviews[i] if i < len(reviews) else {}
                out = block_template
                # 1) Replace all {BUSINESS_REVIEW[INDEX]_XYZ} with {BUSINESS_REVIEW[i]_XYZ} literally (no capture groups)
                pattern = r"\{BUSINESS_REVIEW\[INDEX\]_([A-Z0-9_]+)\}"
                replacement = "{" + f"BUSINESS_REVIEW[{i}]_\\1" + "}"
                out = re.sub(pattern, replacement, out)
                # 2) Replace concrete placeholders with actual values
                for k, v in row.items():
                    concrete = "{" + k.replace("[INDEX]", f"[{i}]") + "}"
                    out = out.replace(concrete, str(v))
                # 3) Fallback: clear any unresolved BUSINESS_REVIEW[i]_ placeholders
                out = re.sub(r"\{BUSINESS_REVIEW\[" + str(i) + r"\]_[A-Z0-9_]+\}", "", out)
                # Clean any stray markers if present within captured block
                out = out.replace("<!--REVIEWS_ROW_START-->", "").replace("<!--REVIEWS_ROW_END-->", "")
                return out

            # First expand the reviews block before global placeholder rendering, because
            # the reviews contain [INDEX]-scoped placeholders that won't be present in context.
            has_markers = ("<!--REVIEWS_ROW_START-->" in template_html) and ("<!--REVIEWS_ROW_END-->" in template_html)
            logger.debug("Review markers present: %s", has_markers)
            html = render_indexed_block(
                template_html,
                row_start_marker="<!--REVIEWS_ROW_START-->",
                row_end_marker="<!--REVIEWS_ROW_END-->",
                item_count=len(reviews),
                render_for_index=_render_review,
            )
            # Now perform the global replacements and contact pages list
            html = render_template(html, context)
            html = render_list_block(html, "BUSINESS_CONTACT_PAGE", contact_pages)
            logger.debug("Final HTML around Reviews section (snippet): %s", html.split("<!--REVIEWS_ROW_START-->")[0][-300:] if "<!--REVIEWS_ROW_START-->" in template_html else html[:300])
            return html
    except Exception:
        # Fall through to standard rendering if anything goes wrong
        pass

    # 10) Render placeholders and list duplication
    html = render_template(template_html, context)
    html = render_list_block(html, "BUSINESS_CONTACT_PAGE", contact_pages)

    return html

def generateBusinessRankLocalReport(business_id: str) -> str:
    """
    Generate the Business Rank Local Report HTML for a given business_id.

    Shows heatmap for each category with 6x6 grid overlay.
    """
    # Load template
    template_path = os.path.join("project", "template", "business-visibility.html")
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            template_html = f.read()
    except Exception as e:
        logger.warning("Failed to read template: %s", e)
        template_html = ""

    # Fetch business
    biz = _fetch_business(business_id)
    name = biz.get("name") or "N/A"
    categories = _resolve_categories(biz)

    # Select only one category for visibility report
    selected_category = None
    # First priority: use businesses.type (string)
    if biz.get("type"):
        selected_category = biz["type"]
    # Second priority: take first category from businesses.categories array
    elif biz.get("categories") and isinstance(biz["categories"], list) and len(biz["categories"]) > 0:
        first_cat = biz["categories"][0]
        if isinstance(first_cat, dict) and "title" in first_cat:
            selected_category = first_cat["title"]
        elif isinstance(first_cat, str):
            selected_category = first_cat

    category_list = [selected_category] if selected_category else []

    lat, lng = _resolve_coords(biz, {})
    target_place_id = _safe_get(biz, "google_enrichment.place_id")

    # Get current business reviews
    yelp_total_reviews = biz.get("review_count") or "N/A"
    google_place_total_reviews = _safe_get(biz, "google_enrichment.user_ratings_total") or _safe_get(biz, "user_ratings_total") or "N/A"
    current_reviews = {"yelp": yelp_total_reviews, "google": google_place_total_reviews}

    # Define spacing for grid
    spacing_km = 1.666 * 1.60934

    # Calculate gap distance in miles
    gap_km = spacing_km
    gap_miles = gap_km / 1.60934
    gap_miles_str = f"{gap_miles:.6f}"

    # Prepare data for each category - multi-threaded
    import concurrent.futures
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from collections import Counter
    from io import BytesIO
    def process_category(category):
        if lat and lng and target_place_id:
            map_image, avg_rank, ranks, grid_positions, competitors_per_point = _build_heatmap_map_url(lat, lng, category, target_place_id)
        else:
            map_image = TRANSPARENT_GIF_DATA_URL
            avg_rank = None
            ranks = []
            grid_positions = []
            competitors_per_point = []
        return {
            "BUSINESS_TYPE[INDEX]_NAME": category,
            "BUSINESS_TYPE[INDEX]_NAME_MAP_IMAGE": map_image,
            "BUSINESS_TYPE[INDEX]_GAP_DISTANCE_MILES": gap_miles_str,
            "average_rank": avg_rank,
            "ranks": ranks,
            "grid_positions": grid_positions,
            "competitors_per_point": competitors_per_point,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        type_data = list(executor.map(process_category, category_list))
        # Filter out categories where all balls have no rank (avg_rank is None)
        type_data = [d for d in type_data if d["average_rank"] is not None]
        # Filter out categories where average rank is greater than 45
        type_data = [d for d in type_data if d["average_rank"] < 20]
        type_data.sort(key=lambda x: x["average_rank"])

        # Collect overall top-5 competitors across all map points
        competitor_stats = {}  # place_id -> dict
        for item in type_data:
            category = item["BUSINESS_TYPE[INDEX]_NAME"]
            ranks = item["ranks"]
            competitors_per_point = item["competitors_per_point"]
            for i, comps in enumerate(competitors_per_point):
                rank_at_point = ranks[i] if i < len(ranks) else None
                for j, comp in enumerate(comps):
                    pid = comp.get("place_id")
                    if pid and pid != target_place_id:  # exclude the target business
                        comp_rank = j + 1  # rank at this point
                        if pid not in competitor_stats:
                            competitor_stats[pid] = {
                                "name": comp.get("name", "Unknown"),
                                "total_rank": 0,
                                "count": 0,
                                "categories": set(),
                                "user_ratings_total": comp.get("user_ratings_total", 0),
                            }
                        competitor_stats[pid]["total_rank"] += comp_rank
                        competitor_stats[pid]["count"] += 1
                        competitor_stats[pid]["categories"].add(category)

        # Compute average ranks and sort
        competitors_list = []
        for pid, stats in competitor_stats.items():
            if stats["count"] > 0:
                avg_rank = stats["total_rank"] / stats["count"]
                competitors_list.append({
                    "place_id": pid,
                    "name": stats["name"],
                    "avg_rank": avg_rank,
                    "categories": list(stats["categories"]),
                    "user_ratings_total": stats["user_ratings_total"],
                })
        competitors_list.sort(key=lambda x: x["avg_rank"])
        top_10_competitors = competitors_list[:10]

        # Helper function for directions
        def get_direction(lat, lng, center_lat, center_lng):
            dlat = lat - center_lat
            dlng = lng - center_lng
            ns = "north" if dlat > 1e-6 else "south" if dlat < -1e-6 else ""
            ew = "east" if dlng > 1e-6 else "west" if dlng < -1e-6 else ""
            if ns and ew:
                return f"{ns}-{ew}"
            elif ns:
                return ns
            elif ew:
                return ew
            else:
                return "center"

        # Generate summaries for each category - multi-threaded
        def generate_summary_for_item(item):
            category = item["BUSINESS_TYPE[INDEX]_NAME"]
            ranks = item["ranks"]
            grid_positions = item["grid_positions"]

            # Calculate enhanced geographic insights
            valid_ranks = [r for r in ranks if r is not None and r != 20]
            average_rank = sum(valid_ranks) / len(valid_ranks) if valid_ranks else None
            visibility_coverage = len(valid_ranks) / len(ranks) * 100 if ranks else 0
            top_positions = sum(1 for r in ranks if r == 1)

            # Direction-based analysis
            direction_ranks = {}
            low_visibility_points = []
            for i, rank in enumerate(ranks):
                grid_lat, grid_lng = grid_positions[i]
                direction = get_direction(grid_lat, grid_lng, lat, lng)
                if direction not in direction_ranks:
                    direction_ranks[direction] = []
                direction_ranks[direction].append(rank)
                if rank > 20:
                    low_visibility_points.append(direction)

            # Calculate average rank by direction
            direction_averages = {}
            for dir_name, dir_ranks in direction_ranks.items():
                if dir_ranks:
                    direction_averages[dir_name] = sum(dir_ranks) / len(dir_ranks)

            # Minimum threshold for valid directions (configurable, default 5)
            min_valid_ranks = 5

            # Create list of (direction, average, count)
            direction_stats = [(dir_name, avg, len(direction_ranks[dir_name])) for dir_name, avg in direction_averages.items()]

            # Filter directions with enough data
            valid_directions = [stat for stat in direction_stats if stat[2] >= min_valid_ranks]

            if valid_directions:
                # Best: lowest average, then highest count
                best_stat = min(valid_directions, key=lambda x: (x[1], -x[2]))
                best_direction = (best_stat[0], best_stat[1])
                # Worst: highest average, then highest count
                worst_stat = max(valid_directions, key=lambda x: (x[1], -x[2]))
                worst_direction = (worst_stat[0], worst_stat[1])
            else:
                best_direction = ("N/A", 'N/A')
                worst_direction = ("N/A", 'N/A')

            summary_data = {
                "category": category,
                "current_business_categories": categories,
                "grid_size": len(grid_positions),
                "gap_miles": gap_miles_str,
                "ranks": ",".join(str(r) if r is not None else "" for r in ranks),
                "average_rank": average_rank,
                "visibility_coverage": visibility_coverage,
                "valid_rankings_count": len(valid_ranks),
                "top_positions": top_positions,
                "direction_averages": direction_averages,
                "best_direction": best_direction[0],
                "best_direction_rank": best_direction[1],
                "worst_direction": worst_direction[0],
                "worst_direction_rank": worst_direction[1],
                "low_visibility_points": low_visibility_points,
                "top_10_competitors": top_10_competitors,
                "current_reviews": current_reviews,
            }
            summary = generate_rank_summary(summary_data)
            return summary

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            summaries = list(executor.map(generate_summary_for_item, type_data))

        for item, summary in zip(type_data, summaries):
            item["BUSINESS_TYPE[INDEX]_SUMMARY"] = summary

        # Generate rank distribution charts
        for item in type_data:
            ranks = item["ranks"]
            valid_ranks = [r for r in ranks if r is not None]
            if valid_ranks:
                rank_counts = Counter(valid_ranks)
                rank_counts = {k: v for k, v in rank_counts.items() if k < 20}
                ranks_sorted = sorted(rank_counts.keys())
                counts = [rank_counts[r] for r in ranks_sorted]
                labels = [str(r) for r in ranks_sorted]
                plt.figure(figsize=(6, 2))
                plt.rcParams['font.family'] = 'Arial'
                plt.rcParams['font.size'] = 6
                x_positions = list(range(len(labels)))
                plt.bar(x_positions, counts, color='#00489c')
                for i, count in enumerate(counts):
                    plt.text(x_positions[i], count + 0.1, str(count), ha='center', va='bottom', fontsize=6)
                plt.xticks(x_positions, labels, fontsize=6)
                max_count = max(counts) if counts else 0
                plt.yticks(list(range(0, max_count + 1)))
                plt.ylim(0, max_count + 1)
                plt.subplots_adjust(left=0.001)
                buf = BytesIO()
                plt.savefig(buf, format='png', bbox_inches='tight', dpi=300)
                buf.seek(0)
                encoded = base64.b64encode(buf.getvalue()).decode('ascii')
                data_url = f"data:image/png;base64,{encoded}"
                plt.close()
                item["BUSINESS_TYPE[INDEX]_CHART_IMAGE"] = data_url
            else:
                item["BUSINESS_TYPE[INDEX]_CHART_IMAGE"] = TRANSPARENT_GIF_DATA_URL

    # Render indexed block
    def _render_type(i: int, block_template: str) -> str:
        import re
        row = type_data[i] if i < len(type_data) else {}
        out = block_template
        pattern = r"\{BUSINESS_TYPE\[INDEX\]_([A-Z0-9_]+)\}"
        replacement = "{" + f"BUSINESS_TYPE[{i}]_\\1" + "}"
        out = re.sub(pattern, replacement, out)
        for k, v in row.items():
            concrete = "{" + k.replace("[INDEX]", f"[{i}]") + "}"
            out = out.replace(concrete, str(v))
        out = re.sub(r"\{BUSINESS_TYPE\[" + str(i) + r"\]_[A-Z0-9_]+\}", "", out)
        out = out.replace("<!--TYPE_ROW_START-->", "").replace("<!--TYPE_ROW_END-->", "")
        return out

    html = render_indexed_block(
        template_html,
        row_start_marker="<!--TYPE_ROW_START-->",
        row_end_marker="<!--TYPE_ROW_END-->",
        item_count=len(type_data),
        render_for_index=_render_type,
    )

    # Global replacements
    context = {"BUSINESS_NAME": name}
    html = render_template(html, context)

    return html


def generateBusinessReportPdf(business_id: str, to_path: Optional[str] = None, upload: Optional[bool] = None) -> str:
    """
    Render the Business Report PDF for a given business_id.

    Returns:
        str: Local file path if upload is False, otherwise the public URL from Supabase Storage.
    """
    cfg = get_report_config()
    html = generateBusinessReport(business_id)
    # Inject precompiled Tailwind and print CSS
    html_with_styles = _inject_report_styles(html)

    # Determine output path
    out_dir = cfg.REPORTS_OUTPUT_DIR or "./tmp/reports"
    os.makedirs(out_dir, exist_ok=True)
    if to_path is None:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        to_path = os.path.join(out_dir, f"business-{business_id}-{ts}.pdf")

    # Get business name for PDF title
    biz = _fetch_business(business_id)
    business_name = biz.get("name") or "Business"
    pdf_title = f"{business_name} - Business Report"

    # Create custom PDF options with business-specific title
    from project.reporting.pdf_service import PDFOptions
    pdf_options = _config_to_options()
    pdf_options.header_title = pdf_title

    # Render to local file
    base_url = pathlib.Path(_project_root_abs()).as_uri()  # resolve local assets
    html_to_pdf_file(html_with_styles, to_path, base_url=base_url, options=pdf_options)

    # Upload if requested (default to config)
    do_upload = cfg.PDF_UPLOAD_ENABLED if upload is None else upload
    if do_upload:
        uploaded_url = upload_to_supabase_storage(to_path, bucket=cfg.STORAGE_BUCKET_REPORTS)
    else:
        uploaded_url = to_path

    # Attach PDF to Zoho CRM lead
    try:
        lead_id = get_lead_id_by_business_id(business_id)
        if lead_id:
            attach_pdf_to_lead(lead_id, to_path, "Business Report")
    except Exception as e:
        logger.warning(f"Failed to attach Business Report PDF to Zoho lead for business {business_id}: {e}")

    return uploaded_url


def generateBusinessRankLocalReportPdf(business_id: str, to_path: Optional[str] = None, upload: Optional[bool] = None) -> str:
    """
    Render the Business Rank Local Report PDF for a given business_id.

    Returns:
        str: Local file path if upload is False, otherwise the public URL from Supabase Storage.
    """
    cfg = get_report_config()
    html = generateBusinessRankLocalReport(business_id)
    # Inject precompiled Tailwind and print CSS
    html_with_styles = _inject_report_styles(html)

    # Determine output path
    out_dir = cfg.REPORTS_OUTPUT_DIR or "./tmp/reports"
    os.makedirs(out_dir, exist_ok=True)
    if to_path is None:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        to_path = os.path.join(out_dir, f"business-visibility-{business_id}-{ts}.pdf")

    # Get business name for PDF title
    biz = _fetch_business(business_id)
    business_name = biz.get("name") or "Business"
    pdf_title = f"{business_name} - Visibility Report"

    # Create custom PDF options with business-specific title
    from project.reporting.pdf_service import PDFOptions
    pdf_options = _config_to_options()
    pdf_options.header_title = pdf_title

    # Render to local file
    base_url = pathlib.Path(_project_root_abs()).as_uri()  # resolve local assets
    html_to_pdf_file(html_with_styles, to_path, base_url=base_url, options=pdf_options)

    # Upload if requested (default to config)
    do_upload = cfg.PDF_UPLOAD_ENABLED if upload is None else upload
    if do_upload:
        uploaded_url = upload_to_supabase_storage(to_path, bucket=cfg.STORAGE_BUCKET_REPORTS)
    else:
        uploaded_url = to_path

    # Attach PDF to Zoho CRM lead
    try:
        lead_id = get_lead_id_by_business_id(business_id)
        if lead_id:
            attach_pdf_to_lead(lead_id, to_path, "Visibility Report")
    except Exception as e:
        logger.warning(f"Failed to attach Visibility Report PDF to Zoho lead for business {business_id}: {e}")

    return uploaded_url


def main():
    # Produce output when run as a module for quick verification
    html = generateBusinessReport('RVQE2Z2uky4c0-njFQO66g')
    try:
        # Print a preview to avoid flooding terminal
        print(html[:10000])
    except Exception:
        # Fallback print
        print(str(html)[:10000])

if __name__ == "__main__":
    main()
