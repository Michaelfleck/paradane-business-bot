from __future__ import annotations

"""
Address utilities for Business Reporting.

- parseAddressFromDisplay: Parse various display formats into a structured dict.
- geocodeAddressToCoords: Geocode a structured address into latitude/longitude.
"""

from typing import Any, Dict, Optional, Union

from project.reporting.config import get_report_config

try:
    # Optional import; tests can mock GoogleClient usage
    from project.libs.google_client import GoogleClient  # type: ignore
except Exception:  # pragma: no cover - fallback if client import fails
    GoogleClient = None  # type: ignore


AddressDict = Dict[str, Optional[str]]


def _coalesce(*values: Optional[str]) -> Optional[str]:
    for v in values:
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def parseAddressFromDisplay(display: Union[str, Dict[str, Any], list[str]]) -> AddressDict:
    """
    Attempt to parse a display address into a structured dict:
        { "address1": str|None, "city": str|None, "state": str|None, "country": str|None }

    Supports:
    - list[str]: e.g., ["123 Main St", "Charlotte, NC 28202", "USA"]
    - str: comma-separated string
    - dict: may contain keys address1/city/state/country or location/formatted_address/display_address

    Best-effort parsing; missing parts are None.
    """
    result: AddressDict = {"address1": None, "city": None, "state": None, "country": None}

    if isinstance(display, dict):
        # Direct keys
        address1 = display.get("address1") or display.get("line1") or display.get("street")
        city = display.get("city")
        state = display.get("state")
        country = display.get("country")
        # Nested common forms
        loc = display.get("location") or {}
        if isinstance(loc, dict):
            address1 = _coalesce(address1, loc.get("address1"), loc.get("line1"), loc.get("street"))
            city = _coalesce(city, loc.get("city"))
            state = _coalesce(state, loc.get("state"))
            country = _coalesce(country, loc.get("country"))
        # Google-style formatted_address
        formatted = display.get("formatted_address")
        if formatted and not all([address1, city, state, country]):
            parsed = parseAddressFromDisplay(str(formatted))
            for k in result:
                if result[k] is None and parsed.get(k):
                    result[k] = parsed[k]
        # Yelp-style display_address
        disp = display.get("display_address")
        if isinstance(disp, list):
            parsed = parseAddressFromDisplay(disp)
            for k in result:
                if result[k] is None and parsed.get(k):
                    result[k] = parsed[k]
        else:
            # fill from direct keys
            result["address1"] = address1 or result["address1"]
            result["city"] = city or result["city"]
            result["state"] = state or result["state"]
            result["country"] = country or result["country"]
        return result

    if isinstance(display, list):
        parts = [p for p in (str(x).strip() for x in display) if p]
        # Heuristics:
        # - First line: address1
        # - Next containing comma with state: "City, ST ..." extract city/state
        # - Any line equal to known country-like tokens becomes country
        if parts:
            result["address1"] = parts[0]
        # Find city/state line
        for p in parts[1:]:
            if "," in p:
                left, right = p.split(",", 1)
                left = left.strip()
                right = right.strip()
                # right may start with state code; take first token as state
                state_token = right.split()[0] if right else ""
                if left and state_token:
                    result["city"] = result["city"] or left
                    result["state"] = result["state"] or state_token
            # naive country detection
            if p.upper() in {"USA", "UNITED STATES", "UNITED STATES OF AMERICA", "CANADA"}:
                result["country"] = result["country"] or p
        return result

    if isinstance(display, str):
        # Simple CSV split
        tokens = [t.strip() for t in display.split(",") if t.strip()]
        if tokens:
            # Heuristic assignment:
            # address1 = first token
            result["address1"] = tokens[0]
        if len(tokens) >= 2:
            # token may be "City" or "City ST" or "City ST ZIP"
            # Try to split the second token to find state code
            second = tokens[1]
            chunks = second.split()
            if len(chunks) >= 2:
                result["city"] = " ".join(chunks[:-1])
                result["state"] = chunks[-1]
            else:
                result["city"] = second
        if len(tokens) >= 3:
            # Treat last token as country in many cases
            result["country"] = tokens[-1]
        return result

    return result


def geocodeAddressToCoords(address: AddressDict) -> Dict[str, Optional[float]]:
    """
    Geocode the given structured address to coordinates using GoogleClient if available.

    Args:
        address: Dict with address1, city, state, country (any may be None)

    Returns:
        {"lat": float|None, "lng": float|None}
    """
    # Build single-line address
    line = ", ".join([v for v in [address.get("address1"), address.get("city"), address.get("state"), address.get("country")] if v])
    if not line:
        return {"lat": None, "lng": None}

    cfg = get_report_config()
    api_key = cfg.GOOGLE_API_KEY
    if GoogleClient and api_key:
        try:
            gc = GoogleClient(api_key)
            results = gc.client.geocode(line)
            if results:
                # Prefer 'geometry.location'
                geom = results[0].get("geometry", {})
                loc = geom.get("location", {})
                lat = loc.get("lat")
                lng = loc.get("lng")
                return {"lat": float(lat) if lat is not None else None, "lng": float(lng) if lng is not None else None}
        except Exception:
            # Allow tests to mock failures
            return {"lat": None, "lng": None}
    # If no client or no key, return empty; tests can mock this path
    return {"lat": None, "lng": None}