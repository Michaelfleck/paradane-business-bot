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

from project.reporting.config import get_report_config
from project.reporting.renderer import render_template, render_list_block, render_indexed_block
from project.reporting.utils.address import parseAddressFromDisplay, geocodeAddressToCoords
from project.reporting.utils.hours import formatBusinessHours
from project.reporting.utils.web import toRootDomain, buildGooglePlaceUrl, collectBusinessEmails, collectContactPages
from project.reporting.utils.phone import normalizePhone
from project.libs.supabase_client import get_client
from project.reporting.pdf_service import html_to_pdf_file, upload_to_supabase_storage, _project_root_abs, _inject_report_styles, _config_to_options

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
    Join category titles preserving order and de-duplicating identical titles.
    """
    categories = biz.get("categories") or []
    seen = set()
    titles: List[str] = []
    for c in categories:
        title = (c or {}).get("title")
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
    Build Google Static Map URL if key and coords present, else None.
    """
    if lat is None or lng is None:
        return None
    cfg = get_report_config()
    if not cfg.GOOGLE_API_KEY:
        return None
    params = {
        "center": f"{lat},{lng}",
        "zoom": str(cfg.MAP_DEFAULT_ZOOM),
        "size": cfg.MAP_DEFAULT_SIZE,
        "markers": f"color:red|{lat},{lng}",
        "key": cfg.GOOGLE_API_KEY,
        "maptype": "roadmap",
        "scale": "2",
    }
    return "https://maps.googleapis.com/maps/api/staticmap?" + urlencode(params)


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

    # Google Place URL and total reviews (simple, direct)
    place_id = _safe_get(biz, "google_enrichment.place_id")
    google_place_url = buildGooglePlaceUrl(place_id) or "N/A"
    google_place_total_reviews = (
        (biz.get("user_ratings_total") if isinstance(biz, dict) else None)
        or _safe_get(biz, "user_ratings_total")
        or "N/A"
    )
    google_place_rating = _safe_get(biz, "google_enrichment.rating") or "N/A"

    # Hours
    open_days = _resolve_hours(biz)

    # Address
    addr1, city, state, country = _resolve_address(biz)
    structured_addr = {"address1": None if addr1 == "N/A" else addr1, "city": None if city == "N/A" else city, "state": None if state == "N/A" else state, "country": None if country == "N/A" else country}

    # Coords
    lat, lng = _resolve_coords(biz, structured_addr)
    if lat is None or lng is None:
        logger.warning("Missing coordinates; map will show placeholder for business_id=%s", business_id)

    # Static Map
    map_url = _build_static_map_url(lat, lng)
    if not map_url:
        # Use 1x1 transparent GIF per spec
        map_url = TRANSPARENT_GIF_DATA_URL

    # Ensure config is available before using it for gallery image
    cfg = get_report_config()

    # Business Gallery Image via Google Places Photo API, enhanced by HF classifier
    business_gallery_image = ""
    try:
        from project.libs.image_classifier import select_best_photo  # local import to avoid hard dep if unused
        cfg_key = cfg.GOOGLE_API_KEY
        ge = _safe_get(biz, "google_enrichment", {})
        photos = []
        if isinstance(ge, dict):
            photos = ge.get("photos") or ge.get("photo") or []
        # Normalize to list
        if isinstance(photos, dict):
            photos = [photos]
        photo_refs = []
        for p in photos:
            if not isinstance(p, dict):
                continue
            pref = p.get("photo_reference")
            if pref:
                photo_refs.append(str(pref))
        candidate_urls: list[str] = []
        if cfg_key:
            for ref in photo_refs:
                # Build candidate URLs; width from config
                maxw = get_report_config().GOOGLE_PHOTO_MAXWIDTH
                candidate_urls.append(
                    "https://maps.googleapis.com/maps/api/place/photo"
                    + f"?maxwidth={maxw}&photo_reference={quote_plus(ref)}&key={cfg_key}"
                )
        # Use classifier to select the best candidate
        selected = None
        if candidate_urls:
            try:
                selected = select_best_photo(
                    candidate_urls,
                    timeout_s=get_report_config().CLASSIFIER_TIMEOUT_S,
                    topk=get_report_config().CLASSIFIER_TOPK,
                    business_name=name,
                )
            except Exception:
                selected = None
        if not selected and candidate_urls:
            selected = candidate_urls[0]
        business_gallery_image = selected or ""
    except Exception:
        business_gallery_image = ""

    # Fallback to transparent GIF if still empty (keeps layout consistent)
    if not business_gallery_image:
        business_gallery_image = TRANSPARENT_GIF_DATA_URL

    # Emails
    emails = collectBusinessEmails(business_id)
    emails = _reorder_emails_by_domain(emails, website_root)
    emails_str = ", ".join(emails) if emails else "N/A"

    # Contact pages
    contact_pages = collectContactPages(business_id)

    # Phone normalization
    cfg = get_report_config()
    raw_phone = biz.get("phone") or biz.get("display_phone")
    normalized_phone = normalizePhone(raw_phone, cfg.DEFAULT_PHONE_COUNTRY) or "N/A"

    # 9) Build context with "N/A" defaults
    ge: Dict[str, Any] = _safe_get(biz, "google_enrichment", {}) or {}

    def _amenity_label(flag: Optional[bool], yes_text: str, no_text: str) -> str:
        if flag is None:
            return "—"
        return yes_text if bool(flag) else no_text

    # Editorial summary:
    # - Prefer google_enrichment.editorial_summary.overview if available.
    # - Else synthesize from reviews (up to 2 highlights) and core facts.
    editorial_summary = "—"
    try:
        overview = None
        es = ge.get("editorial_summary")
        if isinstance(es, dict):
            overview = es.get("overview")
        if isinstance(overview, str) and overview.strip():
            editorial_summary = overview.strip()
        else:
            # Fallback synthesis
            revs = ge.get("reviews")
            lines: List[str] = []
            if isinstance(revs, list) and revs:
                # Sort by time desc if 'time' present
                try:
                    revs_sorted = sorted(
                        [r for r in revs if isinstance(r, dict)],
                        key=lambda r: r.get("time", 0),
                        reverse=True,
                    )
                except Exception:
                    revs_sorted = [r for r in revs if isinstance(r, dict)]
                for r in revs_sorted[:2]:
                    txt = r.get("text")
                    if isinstance(txt, str):
                        txt = txt.strip().replace("\n", " ")
                        if txt:
                            # clamp length ~220 chars
                            if len(txt) > 220:
                                txt = txt[:217].rstrip() + "..."
                            lines.append(f"“{txt}”")
            # Add one meta line
            meta_parts: List[str] = []
            if categories and categories != "N/A":
                meta_parts.append(categories)
            if isinstance(google_place_rating, (int, float)) or (isinstance(google_place_rating, str) and google_place_rating != "N/A"):
                meta_parts.append(f"Google {google_place_rating}/5")
            if isinstance(yelp_rating, (int, float)) or (isinstance(yelp_rating, str) and yelp_rating != "N/A"):
                meta_parts.append(f"Yelp {yelp_rating}/5")
            meta = ""
            if meta_parts:
                meta = f"{name} — " + ", ".join(meta_parts) + "."
            # Combine
            if lines or meta:
                editorial_summary = " ".join(([meta] if meta else []) + lines) or "—"
    except Exception:
        editorial_summary = "—"

    # Amenity booleans from Google enrichment with explicit phrasing
    amenity_dine_in = _amenity_label(ge.get("dine_in"), "Dine-in Available", "No Dine-in")
    amenity_take_out = _amenity_label(ge.get("takeout"), "Takeout Available", "No Takeout")
    amenity_reservable = _amenity_label(ge.get("reservable"), "Reservations Accepted", "No Reservations")
    amenity_serves_beer = _amenity_label(ge.get("serves_beer"), "Serves Beer", "Doesn't Serve Beer")
    amenity_serves_wine = _amenity_label(ge.get("serves_wine"), "Serves Wine", "Doesn't Serve Wine")
    amenity_serves_dinner = _amenity_label(ge.get("serves_dinner"), "Serves Dinner", "Doesn't Serve Dinner")
    amenity_curbside_pickup = _amenity_label(ge.get("curbside_pickup"), "Curbside Pickup", "No Curbside Pickup")
    amenity_wheelchair_entrance = _amenity_label(ge.get("wheelchair_accessible_entrance"), "Wheelchair Entrance", "No Wheelchair Entrance")

    # Plus codes (global_code, compound_code) live under google_enrichment.plus_code per Google Places schema
    plus_code = ge.get("plus_code") if isinstance(ge, dict) else None
    if not isinstance(plus_code, dict):
        plus_code = {}

    business_global_code = plus_code.get("global_code") or "N/A"
    business_compound_code = plus_code.get("compound_code") or "N/A"

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
        "BUSINESS_GOOGLE_PLACE_TOTAL_REVIEWS": google_place_total_reviews,
        "BUSINESS_YELP_RATING": yelp_rating,
        "BUSINESS_GOOGLE_PLACE_RATING": google_place_rating,
        "BUSINESS_STATUS": status,
        "BUSINESS_CATEGORIES": categories,
        "BUSINESS_OPEN_DAYS": open_days if open_days else "N/A",
        "BUSINESS_MAP_IMAGE": map_url,
        "BUSINESS_GALLERY_IMAGE": business_gallery_image,
        "BUSINESS_WEBSITE_URL": website_url,
        "BUSINESS_YELP_URL": yelp_url,
        "BUSINESS_GOOGLE_PLACE_URL": google_place_url,
        "BUSINESS_EMAILS": emails_str,
        "BUSINESS_PHONE": normalized_phone,
        # Newly implemented placeholders:
        "BUSINESS_EDITORIAL_SUMMARY": editorial_summary,
        "BUSINESS_AMENITY_DINE_IN": amenity_dine_in,
        "BUSINESS_AMENITY_TAKE_OUT": amenity_take_out,
        "BUSINESS_AMENITY_RESERVABLE": amenity_reservable,
        "BUSINESS_AMENITY_SERVES_BEER": amenity_serves_beer,
        "BUSINESS_AMENITY_SERVES_WINE": amenity_serves_wine,
        "BUSINESS_AMENITY_SERVES_DINNER": amenity_serves_dinner,
        "BUSINESS_AMENITY_CURBSIDE_PICKUP": amenity_curbside_pickup,
        "BUSINESS_AMENITY_WHEERCHAIR_ACCESSIBLE_ENTRANCE": amenity_wheelchair_entrance,
        # Plus code placeholders:
        "BUSINESS_GLOBAL_CODE": business_global_code,
        "BUSINESS_COMPOUND_CODE": business_compound_code,
    }

    # 10a) Expand Reviews block using google_enrichment.reviews
    # We render up to 3 reviews into the block between the opening <div class="flex items-start gap-5"> and its closing sibling.
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

    # Render to local file
    base_url = pathlib.Path(_project_root_abs()).as_uri()  # resolve local assets
    html_to_pdf_file(html_with_styles, to_path, base_url=base_url, options=_config_to_options())

    # Upload if requested (default to config)
    do_upload = cfg.PDF_UPLOAD_ENABLED if upload is None else upload
    if do_upload:
        return upload_to_supabase_storage(to_path, bucket=cfg.STORAGE_BUCKET_REPORTS)

    return to_path


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

    # Render to local file
    base_url = pathlib.Path(_project_root_abs()).as_uri()  # resolve local assets
    html_to_pdf_file(html_with_styles, to_path, base_url=base_url, options=_config_to_options())

    # Upload if requested (default to config)
    do_upload = cfg.PDF_UPLOAD_ENABLED if upload is None else upload
    if do_upload:
        return upload_to_supabase_storage(to_path, bucket=cfg.STORAGE_BUCKET_REPORTS)

    return to_path


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
