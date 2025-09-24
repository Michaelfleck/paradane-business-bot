from __future__ import annotations

"""
Playwright-based HTML to PDF service with Tailwind precompiled CSS injection and optional Supabase upload.
"""

from dataclasses import dataclass
from typing import Optional, List
import os
import pathlib
import time

from project.reporting.config import get_report_config
from project.libs.supabase_client import get_client


@dataclass
class PDFOptions:
    format: str = "A4"
    margins_mm: int = 10
    landscape: bool = False
    print_background: bool = True
    header_enabled: bool = True
    footer_enabled: bool = True
    header_logo_url: str = "https://cdn.paradane.com/images/logo.svg"
    header_title: str = "Paradane Report"
    prefer_css_page_size: bool = True
    scale: float = 1.0


def _bool_env(val: Optional[str], default: bool) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}


def _ensure_dir(path: str) -> None:
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)


def _project_root_abs() -> str:
    return str(pathlib.Path(__file__).resolve().parents[2])  # project/ is two levels up from this file


def _head_injection(html: str, tags: List[str]) -> str:
    """
    Inject provided tags into <head>. Always return a complete HTML document.
    If the input is a fragment (no <html> or <head>), wrap it in a proper shell.
    """
    insertion = "\n".join(tags) + "\n"

    lower = html.lower()
    has_html = "<html" in lower
    has_head = "<head" in lower

    if not has_html or not has_head:
        # Normalize to a full document
        body = html if has_html else html
        return f"<!doctype html><html><head>{insertion}</head><body>{body}</body></html>"

    # Insert right after the first <head> tag
    head_start = lower.find("<head")
    # find the end of the opening head tag '>'
    head_open_end = html.find(">", head_start)
    if head_open_end == -1:
        return f"<!doctype html><html><head>{insertion}</head><body>{html}</body></html>"
    insert_pos = head_open_end + 1
    return html[:insert_pos] + insertion + html[insert_pos:]


def _build_header_template(opts: PDFOptions) -> str:
    # Playwright header/footer must be valid HTML fragment with minimal styles
    # Use CSS variables to keep it compact and ensure consistent sizing.
    return f"""
<div style='font-size:10px; width:100%; padding:0 10mm; display:flex; align-items:center; justify-content:space-between; color:#4b5563;'>
  <div style='display:flex; align-items:center; gap:8px'>
    <span style='font-weight:600; font-size:10px; color:#00489c'>{opts.header_title}</span>
  </div>
  <div><span class='pageNumber'></span> / <span class='totalPages'></span></div>
</div>
""".strip()


def _build_footer_template(opts: PDFOptions) -> str:
    return """
<div style='font-size:10px; width:100%; padding:0 10mm; display:flex; align-items:center; justify-content:flex-end; color:#6b7280'>
  Page <span class="pageNumber"></span> of <span class="totalPages"></span>
</div>
""".strip()


def _resolve_file_url(relative_path_from_repo_root: str) -> str:
    """
    Turn a repo-relative path into a file:// URL for Playwright to load local CSS.
    """
    root = _project_root_abs()
    abs_path = os.path.join(root, relative_path_from_repo_root)
    abs_path = os.path.abspath(abs_path)
    return pathlib.Path(abs_path).as_uri()  # file:///... URL


def _inject_report_styles(html: str) -> str:
    """
    Inject styles into the HTML head with maximum reliability for headless Chromium:
    - Inline CSS contents to avoid file:// loading restrictions during page.set_content.
    - Precedence: tailwind.build.css (if exists) -> reports.css -> print.css.
    """
    root = _project_root_abs()
    tailwind_path = os.path.join(root, "project/template/tailwind.build.css")
    reports_path = os.path.join(root, "project/template/reports.css")
    print_path = os.path.join(root, "project/template/print.css")

    def _read_or_empty(path: str) -> str:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return ""

    tailwind_css = _read_or_empty(tailwind_path)
    reports_css = _read_or_empty(reports_path)
    print_css = _read_or_empty(print_path)

    styles = []
    if tailwind_css:
        styles.append(f"<style>{tailwind_css}</style>")
    if reports_css:
        styles.append(f"<style>{reports_css}</style>")
    if print_css:
        styles.append(f"<style>{print_css}</style>")

    # If all inlining failed (unexpected), fall back to link tags
    if not styles:
        tags = [
            f'<link rel="stylesheet" href="{pathlib.Path(reports_path).as_uri()}" />',
            f'<link rel="stylesheet" href="{pathlib.Path(print_path).as_uri()}" />',
        ]
    else:
        tags = styles

    # Helpful meta to improve print fidelity
    tags.extend([
        '<meta charset="utf-8" />',
        '<meta name="viewport" content="width=device-width, initial-scale=1" />',
    ])

    return _head_injection(html, tags)


def _config_to_options() -> PDFOptions:
    cfg = get_report_config()
    return PDFOptions(
        format=cfg.PDF_FORMAT,
        margins_mm=cfg.PDF_MARGINS_MM,
        landscape=cfg.PDF_LANDSCAPE,
        print_background=cfg.PDF_PRINT_BACKGROUND,
        header_enabled=cfg.PDF_HEADER_ENABLED,
        footer_enabled=cfg.PDF_FOOTER_ENABLED,
        header_logo_url=cfg.PDF_HEADER_LOGO_URL,
        header_title=cfg.PDF_HEADER_TITLE_PREFIX,
        prefer_css_page_size=True,
        scale=1.0,
    )


def html_to_pdf_bytes(html: str, base_url: Optional[str] = None, options: Optional[PDFOptions] = None) -> bytes:
    """
    Convert HTML to PDF bytes using Playwright Chromium.
    """
    cfg = get_report_config()
    if cfg.PDF_ENGINE != "playwright":
        raise RuntimeError("Only Playwright engine is supported by pdf_service currently")

    # Lazy import to avoid hard dependency when not used
    from playwright.sync_api import sync_playwright

    injected = _inject_report_styles(html)
    opts = options or _config_to_options()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context()
            page = context.new_page()

            # Ensure print emulation so @page and @media print rules are honored
            try:
                page.emulate_media(media="print")
            except Exception:
                pass

            # Load HTML via data URL and set a base tag for resolving relative paths.
            # Playwright's set_content does not support base_url in Python API.
            base = base_url or pathlib.Path(_project_root_abs()).as_uri()
            # Inject a <base> tag into head for proper relative URL resolution
            base_tag = f'<base href="{base}" />'
            injected_with_base = _head_injection(injected, [base_tag])
            page.set_content(injected_with_base, wait_until="networkidle")

            # Build Playwright PDF options
            mm = f"{opts.margins_mm}mm"
            pdf_kwargs = {
                "format": opts.format,
                "margin": {"top": mm, "bottom": mm, "left": mm, "right": mm},
                "landscape": opts.landscape,
                "print_background": opts.print_background,
                "prefer_css_page_size": opts.prefer_css_page_size,
                "scale": opts.scale,
            }

            if opts.header_enabled or opts.footer_enabled:
                pdf_kwargs["display_header_footer"] = False
                # pdf_kwargs["header_template"] = _build_header_template(opts) if opts.header_enabled else "<div></div>"
                # pdf_kwargs["footer_template"] = _build_footer_template(opts) if opts.footer_enabled else "<div></div>"

            pdf_bytes: bytes = page.pdf(**pdf_kwargs)  # type: ignore[arg-type]
            return pdf_bytes
        finally:
            browser.close()


def html_to_pdf_file(html: str, out_path: str, base_url: Optional[str] = None, options: Optional[PDFOptions] = None) -> str:
    """
    Convert HTML to PDF and write to out_path. Returns out_path.
    """
    _ensure_dir(out_path)
    data = html_to_pdf_bytes(html, base_url=base_url, options=options)
    with open(out_path, "wb") as f:
        f.write(data)
    return out_path


def upload_to_supabase_storage(local_path: str, bucket: Optional[str] = None, storage_path: Optional[str] = None) -> str:
    """
    Upload a local file to Supabase Storage, returning a public URL.
    If storage_path is None, a timestamped path is used.
    """
    cfg = get_report_config()
    bucket_name = bucket or cfg.STORAGE_BUCKET_REPORTS
    ts = int(time.time())
    filename = os.path.basename(local_path)
    if storage_path is None:
        # e.g. reports/2025/09/filename
        y = time.strftime("%Y")
        m = time.strftime("%m")
        storage_path = f"{y}/{m}/{ts}-{filename}"

    client = get_client()
    storage = client.storage.from_(bucket_name)
    with open(local_path, "rb") as f:
        storage.upload(storage_path, f)  # type: ignore[attr-defined]

    # Try to get a public URL; if bucket is private, callers can use signed URLs instead.
    public = storage.get_public_url(storage_path)  # type: ignore[attr-defined]
    return public.get("publicURL") if isinstance(public, dict) else public  # type: ignore[return-value]