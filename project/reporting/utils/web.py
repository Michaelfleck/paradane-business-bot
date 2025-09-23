from __future__ import annotations

"""
Web utilities for Business Reporting.

- toRootDomain(url): Normalize to scheme + registrable domain; prefer https if scheme missing.
- buildGooglePlaceUrl(place_id): Construct Google Maps Place URL.
- collectBusinessEmails(business_id): Fetch and normalize emails from business_pages.
- collectContactPages(business_id): Fetch and order likely contact page URLs from business_pages.
"""

import re
from typing import List, Optional, Dict, Any
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
    # Select minimal fields used here
    resp = client.table("business_pages").select("url,email,page_type").eq("business_id", business_id).execute()
    data = getattr(resp, "data", resp)  # some clients return dict-like
    if isinstance(data, dict) and "data" in data:
        data = data["data"]
    return data or []


def collectBusinessEmails(business_id: str) -> List[str]:
    """
    Collect emails from business_pages rows.
    - lowercase, trim
    - basic regex validation
    - dedupe
    - ordering heuristic: by length then lexicographic for determinism
    """
    pages = _fetch_business_pages(business_id)
    emails: List[str] = []
    for row in pages:
        email = (row or {}).get("email")
        if not email:
            continue
        e = str(email).strip().lower()
        if _EMAIL_RE.match(e):
            emails.append(e)
    # Dedupe preserving first occurrence
    seen = set()
    deduped: List[str] = []
    for e in emails:
        if e not in seen:
            seen.add(e)
            deduped.append(e)
    # Order by len then lexicographic to be deterministic
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