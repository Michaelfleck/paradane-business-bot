from __future__ import annotations

"""
Configuration for Business Reporting.

Provides a dataclass ReportConfig and a helper to read environment variables.
"""

from dataclasses import dataclass
import os
from typing import Optional


@dataclass(frozen=True)
class ReportConfig:
    """
    Configuration values used by the Business Reporting module.

    Attributes:
        GOOGLE_API_KEY: Optional Google Maps API key to enable geocoding or static maps.
        MAP_DEFAULT_SIZE: Size for static map images, e.g., "600x400".
        MAP_DEFAULT_ZOOM: Default zoom level for static maps.
        DEFAULT_PHONE_COUNTRY: Default phone country code (e.g., "US") for normalization.
    """
    GOOGLE_API_KEY: Optional[str]
    MAP_DEFAULT_SIZE: str
    MAP_DEFAULT_ZOOM: int
    DEFAULT_PHONE_COUNTRY: str


def get_report_config() -> ReportConfig:
    """
    Read configuration from environment variables.

    Supported environment variables:
        - GOOGLE_MAPS_API_KEY or GOOGLE_API_KEY
        - MAP_DEFAULT_SIZE (default "600x400")
        - MAP_DEFAULT_ZOOM (default "15")
        - DEFAULT_PHONE_COUNTRY (default "US")

    Returns:
        ReportConfig: Parsed configuration.
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_API_KEY")
    size = os.getenv("MAP_DEFAULT_SIZE", "600x400")
    zoom_str = os.getenv("MAP_DEFAULT_ZOOM", "15")
    country = os.getenv("DEFAULT_PHONE_COUNTRY", "US")
    try:
        zoom = int(zoom_str)
    except ValueError:
        zoom = 15
    return ReportConfig(
        GOOGLE_API_KEY=api_key,
        MAP_DEFAULT_SIZE=size,
        MAP_DEFAULT_ZOOM=zoom,
        DEFAULT_PHONE_COUNTRY=country,
    )