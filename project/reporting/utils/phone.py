from __future__ import annotations

"""
Phone utilities for Business Reporting.

- normalizePhone(phone, country): Use phonenumbers to parse and format.
"""

from typing import Optional

try:
    import phonenumbers
    from phonenumbers import PhoneNumberFormat
except Exception:  # pragma: no cover - tests can mock behavior
    phonenumbers = None  # type: ignore
    PhoneNumberFormat = None  # type: ignore


def normalizePhone(phone: Optional[str], country: str = "US") -> Optional[str]:
    """
    Normalize a phone number using the phonenumbers library.

    Behavior:
    - Parse with provided default region (country).
    - If valid:
        - For US, return national readable format like "(###) ###-####".
        - Otherwise prefer E164 if possible; fall back to national if E164 unavailable.
    - If parsing or validation fails, return None.

    Args:
        phone: Raw phone string.
        country: Default region for parsing (e.g., "US").

    Returns:
        Normalized phone string or None.
    """
    if not phone or not isinstance(phone, str) or not phone.strip():
        return None
    if phonenumbers is None:
        # Library unavailable in runtime; caller should handle None.
        return None
    try:
        num = phonenumbers.parse(phone, country or "US")
        if not phonenumbers.is_possible_number(num) or not phonenumbers.is_valid_number(num):
            return None
        # US: pretty national format
        if (country or "").upper() == "US":
            return phonenumbers.format_number(num, PhoneNumberFormat.NATIONAL)
        # Others: E164 preferred
        return phonenumbers.format_number(num, PhoneNumberFormat.E164)
    except Exception:
        return None