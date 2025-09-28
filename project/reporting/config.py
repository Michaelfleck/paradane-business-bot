from __future__ import annotations

"""
Configuration for Business Reporting.

Provides a dataclass ReportConfig and a helper to read environment variables.
"""

from dataclasses import dataclass
import os
from typing import Optional, Literal


@dataclass(frozen=True)
class ReportConfig:
    """
    Configuration values used by the Business Reporting module.

    Attributes:
        GOOGLE_API_KEY: Optional Google Maps API key to enable geocoding or static maps.
        GEOAPIFY_API_KEY: Optional Geoapify API key for static maps.
        MAP_DEFAULT_SIZE: Size for static map images, e.g., "600x400".
        MAP_DEFAULT_ZOOM: Default zoom level for static maps.
        DEFAULT_PHONE_COUNTRY: Default phone country code (e.g., "US") for normalization.

        PDF_ENGINE: Which engine to use for HTML-to-PDF. Currently supports "playwright".
        PDF_FORMAT: Page format, e.g., "A4" or "Letter".
        PDF_MARGINS_MM: Integer margin in millimeters for all sides.
        PDF_LANDSCAPE: Whether to render in landscape orientation.
        PDF_PRINT_BACKGROUND: Whether to print background colors and images.
        PDF_HEADER_ENABLED: Show header with logo/title.
        PDF_FOOTER_ENABLED: Show footer with page numbers.
        PDF_HEADER_LOGO_URL: Absolute URL to logo used in header.
        PDF_HEADER_TITLE_PREFIX: Title prefix in the header.
        REPORTS_OUTPUT_DIR: Directory where PDFs are written locally.
        STORAGE_BUCKET_REPORTS: Supabase Storage bucket for report uploads.
        PDF_UPLOAD_ENABLED: If true, upload generated PDFs to Storage by default.
    """
    GOOGLE_API_KEY: Optional[str]
    GEOAPIFY_API_KEY: Optional[str]
    MAP_DEFAULT_SIZE: str
    MAP_DEFAULT_ZOOM: int
    DEFAULT_PHONE_COUNTRY: str

    # Classifier / HF configuration
    HF_MODEL_ID: str
    CLASSIFIER_ENABLED: bool
    CLASSIFIER_TIMEOUT_S: float
    CLASSIFIER_TOPK: int
    CLASSIFIER_CONFIDENCE_MARGIN: float
    CLASSIFIER_CACHE_SIZE: int
    GOOGLE_PHOTO_MAXWIDTH: int

    # PDF-related configuration
    PDF_ENGINE: Literal["playwright"]
    PDF_FORMAT: str
    PDF_MARGINS_MM: int
    PDF_LANDSCAPE: bool
    PDF_PRINT_BACKGROUND: bool
    PDF_HEADER_ENABLED: bool
    PDF_FOOTER_ENABLED: bool
    PDF_HEADER_LOGO_URL: str
    PDF_HEADER_TITLE_PREFIX: str
    REPORTS_OUTPUT_DIR: str
    STORAGE_BUCKET_REPORTS: str
    PDF_UPLOAD_ENABLED: bool


def get_report_config() -> ReportConfig:
    """
    Read configuration from environment variables.

    Supported environment variables:
        - GOOGLE_MAPS_API_KEY or GOOGLE_API_KEY
        - GEOAPIFY_API_KEY
        - MAP_DEFAULT_SIZE (default "600x400")
        - MAP_DEFAULT_ZOOM (default "15")
        - DEFAULT_PHONE_COUNTRY (default "US")
        - HF_MODEL_ID (default "laion/CLIP-ViT-B-32-laion2B-s34B-b79K")
        - CLASSIFIER_ENABLED (default "true")
        - CLASSIFIER_TIMEOUT_S (default "5.0")
        - CLASSIFIER_TOPK (default "2")
        - CLASSIFIER_CONFIDENCE_MARGIN (default "0.10")
        - CLASSIFIER_CACHE_SIZE (default "256")
        - CLASSIFIER_STRICT (default "false")  # if false, auto-disable classifier on init failure
        - GOOGLE_PHOTO_MAXWIDTH (default "800")
        - PDF_ENGINE (default "playwright")
        - PDF_FORMAT (default "A4")
        - PDF_MARGINS_MM (default "10")
        - PDF_LANDSCAPE (default "false")
        - PDF_PRINT_BACKGROUND (default "true")
        - PDF_HEADER_ENABLED (default "true")
        - PDF_FOOTER_ENABLED (default "true")
        - PDF_HEADER_LOGO_URL (default Paradane CDN logo)
        - PDF_HEADER_TITLE_PREFIX (default "Paradane Report")
        - REPORTS_OUTPUT_DIR (default "./tmp/reports")
        - STORAGE_BUCKET_REPORTS (default "reports")
        - PDF_UPLOAD_ENABLED (default "false")

    Returns:
        ReportConfig: Parsed configuration.
    """
    api_key = os.getenv("GOOGLE_MAPS_API_KEY") or os.getenv("GOOGLE_API_KEY")
    geoapify_api_key = os.getenv("GEOAPIFY_API_KEY")
    size = os.getenv("MAP_DEFAULT_SIZE", "1000x800")
    zoom_str = os.getenv("MAP_DEFAULT_ZOOM", "18")
    country = os.getenv("DEFAULT_PHONE_COUNTRY", "US")

    # HF / classifier config
    # Prefer an open, locally-cached friendly model. Keep old default as fallback.
    hf_model_id = os.getenv("HF_MODEL_ID", "openai/clip-vit-base-patch32")
    def _to_bool(val: str | None, default: bool) -> bool:
        if val is None:
            return default
        return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}
    classifier_enabled = _to_bool(os.getenv("CLASSIFIER_ENABLED"), True)
    try:
        classifier_timeout_s = float(os.getenv("CLASSIFIER_TIMEOUT_S", "5.0"))
    except ValueError:
        classifier_timeout_s = 5.0
    try:
        classifier_topk = int(os.getenv("CLASSIFIER_TOPK", "2"))
    except ValueError:
        classifier_topk = 2
    try:
        classifier_conf_margin = float(os.getenv("CLASSIFIER_CONFIDENCE_MARGIN", "0.10"))
    except ValueError:
        classifier_conf_margin = 0.10
    try:
        classifier_cache_size = int(os.getenv("CLASSIFIER_CACHE_SIZE", "256"))
    except ValueError:
        classifier_cache_size = 256
    try:
        google_photo_maxwidth = int(os.getenv("GOOGLE_PHOTO_MAXWIDTH", "800"))
    except ValueError:
        google_photo_maxwidth = 800

    def _to_bool(val: str, default: bool) -> bool:
        if val is None:
            return default
        return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

    try:
        zoom = int(zoom_str)
    except ValueError:
        zoom = 15

    # PDF defaults per approved plan
    pdf_engine = os.getenv("PDF_ENGINE", "playwright")
    pdf_format = os.getenv("PDF_FORMAT", "A4")
    try:
        pdf_margins = int(os.getenv("PDF_MARGINS_MM", "10"))
    except ValueError:
        pdf_margins = 10
    pdf_landscape = _to_bool(os.getenv("PDF_LANDSCAPE"), False)
    pdf_print_bg = _to_bool(os.getenv("PDF_PRINT_BACKGROUND"), True)
    pdf_header_enabled = _to_bool(os.getenv("PDF_HEADER_ENABLED"), True)
    pdf_footer_enabled = _to_bool(os.getenv("PDF_FOOTER_ENABLED"), True)
    pdf_header_logo = os.getenv("PDF_HEADER_LOGO_URL", "https://cdn.paradane.com/images/logo.svg")
    pdf_header_title_prefix = os.getenv("PDF_HEADER_TITLE_PREFIX", "Paradane Report")
    reports_output_dir = os.getenv("REPORTS_OUTPUT_DIR", "./tmp/reports")
    storage_bucket_reports = os.getenv("STORAGE_BUCKET_REPORTS", "reports")
    pdf_upload_enabled = _to_bool(os.getenv("PDF_UPLOAD_ENABLED"), False)

    return ReportConfig(
        GOOGLE_API_KEY=api_key,
        GEOAPIFY_API_KEY=geoapify_api_key,
        MAP_DEFAULT_SIZE=size,
        MAP_DEFAULT_ZOOM=zoom,
        DEFAULT_PHONE_COUNTRY=country,
        # HF/classifier settings (explicit to avoid missing-args TypeError)
        HF_MODEL_ID=hf_model_id,
        CLASSIFIER_ENABLED=classifier_enabled,
        CLASSIFIER_TIMEOUT_S=classifier_timeout_s,
        CLASSIFIER_TOPK=classifier_topk,
        CLASSIFIER_CONFIDENCE_MARGIN=classifier_conf_margin,
        CLASSIFIER_CACHE_SIZE=classifier_cache_size,
        GOOGLE_PHOTO_MAXWIDTH=google_photo_maxwidth,
        # PDF and storage
        PDF_ENGINE=pdf_engine,  # type: ignore[arg-type]
        PDF_FORMAT=pdf_format,
        PDF_MARGINS_MM=pdf_margins,
        PDF_LANDSCAPE=pdf_landscape,
        PDF_PRINT_BACKGROUND=pdf_print_bg,
        PDF_HEADER_ENABLED=pdf_header_enabled,
        PDF_FOOTER_ENABLED=pdf_footer_enabled,
        PDF_HEADER_LOGO_URL=pdf_header_logo,
        PDF_HEADER_TITLE_PREFIX=pdf_header_title_prefix,
        REPORTS_OUTPUT_DIR=reports_output_dir,
        STORAGE_BUCKET_REPORTS=storage_bucket_reports,
        PDF_UPLOAD_ENABLED=pdf_upload_enabled,
    )