from __future__ import annotations

"""
Web utilities for Business Reporting.

- toRootDomain(url): Normalize to scheme + registrable domain; prefer https if scheme missing.
- buildGooglePlaceUrl(place_id): Construct Google Maps Place URL.
- collectBusinessEmails(business_id): Fetch and normalize emails from business_pages.
- collectContactPages(business_id): Fetch and order likely contact page URLs from business_pages.
"""

import re
from typing import List, Optional, Dict, Any, Tuple
from urllib.parse import urlparse

from project.libs.supabase_client import get_client


_EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)


def _public_suffix_domain(netloc: str) -> str:
    """
    Best-effort 'registrable domain' extraction without external deps.
    Heuristic: Keep last two labels, unless TLD is known multi-part like 'co.uk' -> keep last three.
    """
    host = netloc.split("@")[-1].split(":")[0].strip().lower()
    parts = [p for p in host.split(".") if p]
    if len(parts) <= 2:
        return host
    last2 = ".".join(parts[-2:])
    # Multi-part TLD heuristics
    last3 = ".".join(parts[-3:])
    if last2 in {"co.uk", "org.uk", "ac.uk", "gov.uk", "com.au", "net.au", "org.au"} and len(parts) >= 3:
        return last3
    return last2


def toRootDomain(url: str) -> Optional[str]:
    """
    Normalize to scheme + registrable domain. Prefer https if scheme missing.

    Examples:
        http://sub.example.com/path -> https://example.com
        example.com -> https://example.com
    """
    if not url or not isinstance(url, str):
        return None
    s = url.strip()
    if not s:
        return None
    # Prepend scheme if missing to allow urlparse
    if "://" not in s:
        s = "https://" + s
    try:
        parsed = urlparse(s)
        if not parsed.netloc:
            return None
        domain = _public_suffix_domain(parsed.netloc)
        scheme = "https"  # prefer https
        return f"{scheme}://{domain}"
    except Exception:
        return None


def buildGooglePlaceUrl(place_id: Optional[str]) -> Optional[str]:
    """
    Build Google Maps Place URL from place_id.
    """
    if not place_id or not isinstance(place_id, str) or not place_id.strip():
        return None
    return f"https://www.google.com/maps/place/?q=place_id:{place_id.strip()}"


def _fetch_business_pages(business_id: str) -> List[Dict[str, Any]]:
    client = get_client()
    # Select minimal fields used here (include social_links if present)
    try:
        resp = client.table("business_pages").select("url,email,social_links,page_type").eq("business_id", business_id).execute()
    except Exception:
        # Fallback if social_links column not present
        resp = client.table("business_pages").select("url,email,page_type").eq("business_id", business_id).execute()
    data = getattr(resp, "data", resp)  # some clients return dict-like
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    return data or []


def collectBusinessSocials(business_id: str) -> Dict[str, List[str]]:
    """
    Collect social links from business_pages rows.
    Returns dict platform -> list of urls (deduped, sorted deterministically).
    Expects 'social_links' column containing comma-separated "platform:url" entries.
    """
    pages = _fetch_business_pages(business_id)
    items: List[Tuple[str, str]] = []
    for row in pages:
        s = (row or {}).get("social_links")
        if not s:
            continue
        # Split by commas; tolerate spaces
        parts = [p.strip() for p in str(s).split(",") if p.strip()]
        for part in parts:
            if ":" in part:
                plat, url = part.split(":", 1)
                plat = plat.strip().lower()
                url = url.strip()
                if plat and url:
                    items.append((plat, url))
    # Dedup per platform/url
    out: Dict[str, List[str]] = {}
    seen: set[Tuple[str, str]] = set()
    for plat, url in items:
        key = (plat, url)
        if key in seen:
            continue
        seen.add(key)
        out.setdefault(plat, []).append(url)
    # Sort each list deterministically
    for plat in out:
        out[plat] = sorted(out[plat])
    return out


def collectBusinessEmails(business_id: str) -> List[str]:
    """
    Collect emails from business_pages rows.

    Accepts that business_pages.email may contain one or more comma-separated
    email addresses. For each row:
      - Split on commas
      - Trim and lowercase each candidate
      - Validate with basic regex
      - Drop obvious placeholders (e.g., email@example.com, user@domain.com)
    Then:
      - Dedupe while preserving first-seen order
      - Sort deterministically by length then lexicographically

    Returns list of valid emails.
    """
    pages = _fetch_business_pages(business_id)
    raw_emails: List[str] = []

    placeholder_set = {
        "email@example.com",
        "user@domain.com",
        "example@mysite.com",
    }

    for row in pages:
        val = (row or {}).get("email")
        if not val:
            continue
        # Split comma-separated lists from the DB
        parts = [p.strip().lower() for p in str(val).split(",") if p and p.strip()]
        for e in parts:
            # Ignore obvious placeholders
            if e in placeholder_set:
                continue
            if _EMAIL_RE.match(e):
                raw_emails.append(e)

    # Dedupe preserving order
    seen = set()
    deduped: List[str] = []
    for e in raw_emails:
        if e not in seen:
            seen.add(e)
            deduped.append(e)

    # Deterministic ordering (after preserving original order for de-dupe)
    deduped.sort(key=lambda x: (len(x), x))
    return deduped


def _is_contact_type(page_type: Optional[str]) -> bool:
    return bool(page_type) and str(page_type).strip().lower() == "contact"


def _looks_like_contact_url(url: str) -> bool:
    u = url.lower()
    return any(tok in u for tok in ["/contact", "/contact-us", "/contacts"])


def collectContactPages(business_id: str) -> List[str]:
    """
    Collect only Contact pages where page_type == "Contact" (exact match).
    If none exist, return empty list (renderer will remove the line).
    """
    pages = _fetch_business_pages(business_id)
    contacts: List[str] = []
    for row in pages:
        if str((row or {}).get("page_type") or "") == "Contact":
            url = (row or {}).get("url")
            if isinstance(url, str):
                u = url.strip()
                if u:
                    contacts.append(u)
    # Deduplicate and sort ascending for determinism
    seen = set()
    out: List[str] = []
    for u in contacts:
        if u not in seen:
            seen.add(u)
            out.append(u)
    out.sort()
    return out