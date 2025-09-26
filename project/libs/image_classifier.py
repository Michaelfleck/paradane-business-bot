from __future__ import annotations

"""
Hugging Face-based image classifier utilities.

Public API:
- classify_exterior_interior(image_url: str, timeout_s: Optional[float] = None) -> dict[str, float]
- select_best_photo(photo_urls: list[str], timeout_s: Optional[float] = None, topk: Optional[int] = None) -> str | None

Behavior:
- Zero-shot image classification with candidate labels:
  - "exterior of building"
  - "interior of building"
- Prioritize exterior, then interior, else fallback to first URL.
- Respects config flags/timeouts; logs decisions and failures.
"""

from typing import Optional, Dict, List, Tuple
import io
import logging
import time
import os
import string
from difflib import SequenceMatcher
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from PIL import Image  # Pillow input for HF pipelines that expect PIL.Image

import torch

try:
    from transformers import CLIPProcessor, CLIPModel, Pipeline  # type: ignore
except Exception:  # pragma: no cover
    CLIPProcessor = None
    CLIPModel = None
    Pipeline = object  # type: ignore

from project.reporting.config import get_report_config

logger = logging.getLogger("project.libs.image_classifier")

_labels_verbose = ["exterior of building", "interior of building", "food item"]

_model_singleton: Optional["CLIPModel"] = None
_processor_singleton: Optional["CLIPProcessor"] = None


def _normalize_text(s: str) -> str:
    try:
        s = s.lower().strip()
        # remove punctuation
        s = s.translate(str.maketrans("", "", string.punctuation))
        # collapse whitespace
        s = " ".join(s.split())
        return s
    except Exception:
        return s.lower().strip()


def _similarity(a: str, b: str) -> float:
    """
    Lightweight similarity using difflib.SequenceMatcher on normalized text.
    Returns a float in [0,1].
    """
    try:
        a_n = _normalize_text(a)
        b_n = _normalize_text(b)
        if not a_n or not b_n:
            return 0.0
        return float(SequenceMatcher(None, a_n, b_n).ratio())
    except Exception:
        return 0.0


def _extract_image_text(pil_img: "Image.Image") -> str:
    """
    Best-effort OCR using pytesseract if available.
    - Accepts PIL.Image and returns lowercased detected text.
    - Gracefully handles ImportError and runtime errors by returning "".
    - Simple preprocessing only via Pillow (grayscale, basic threshold).
    """
    try:
        import pytesseract  # type: ignore
    except Exception:
        logger.info("OCR unavailable; skipping text match")
        return ""
    try:
        img = pil_img.convert("L")  # grayscale
        # simple threshold to bump contrast a bit
        try:
            img = img.point(lambda p: 255 if p > 160 else 0)
        except Exception:
            # if point fails for some mode, ignore and use grayscale
            pass
        text = pytesseract.image_to_string(img) or ""
        return _normalize_text(text)
    except Exception:
        # Any OCR failure should not break selection logic
        logger.info("OCR failed at runtime; skipping text match")
        return ""


class ClassifierError(Exception):
    """Raised when classification fails or is unavailable."""
    pass


def _get_model_and_processor():
    """
    Lazy-init and memoize the CLIP model and processor for zero-shot image classification.

    This loads models locally via transformers. No external HF Hub token is required.
    """
    global _model_singleton, _processor_singleton
    if _model_singleton is not None and _processor_singleton is not None:
        return _model_singleton, _processor_singleton

    cfg = get_report_config()
    if CLIPModel is None or CLIPProcessor is None:
        raise ClassifierError("CLIPModel or CLIPProcessor not available")

    model_id = cfg.HF_MODEL_ID

    try:
        _model_singleton = CLIPModel.from_pretrained(model_id, torch_dtype=torch.float32, device_map="cpu")
        _model_singleton = _model_singleton.eval()
        _processor_singleton = CLIPProcessor.from_pretrained(model_id)
    except Exception as e:
        logger.exception("Failed to initialize CLIP model and processor: %s", e)
        if not cfg.CLASSIFIER_ENABLED:
            raise ClassifierError("Classifier disabled by config") from e
        os.environ["CLASSIFIER_ENABLED"] = "false"
        raise ClassifierError("Failed to initialize CLIP model") from e

    logger.info("Initialized CLIP model and processor model_id=%s", model_id)
    return _model_singleton, _processor_singleton


def _download_image_bytes(url: str, timeout_s: float) -> bytes:
    """
    Download bytes for an image URL with small retry/backoff.
    Special-case Google Places Photo API redirect URLs by allowing redirects and
    ensuring we ultimately fetch the binary content the HF pipeline expects.
    """
    last_exc: Exception | None = None
    # 3 attempts with incremental backoff: 0s, 0.5s, 1.0s
    for attempt in range(3):
        try:
            # Allow redirects; requests will follow the Google Photo API redirect to the actual CDN image.
            r = requests.get(url, stream=True, timeout=timeout_s, allow_redirects=True)
            r.raise_for_status()
            # Some endpoints may respond with HTML if key/params are wrong; add a simple guard
            content_type = r.headers.get("Content-Type", "").lower()
            if "text/html" in content_type and not url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
                # Try to resolve final URL and re-fetch as binary without stream to simplify edge servers
                final_url = r.url
                rr = requests.get(final_url, timeout=timeout_s)
                rr.raise_for_status()
                return rr.content
            return r.content
        except Exception as e:
            last_exc = e
            # brief backoff then retry
            try:
                time.sleep(0.5 * attempt)
            except Exception:
                pass
            logger.warning("Image download attempt %d failed for url=%s err=%s", attempt + 1, url, e)
    # After retries, re-raise
    assert last_exc is not None
    raise last_exc


def _classify_with_retry(url: str, timeout_s: Optional[float] = None) -> Dict[str, float]:
    """Classify with retry logic for multi-threading."""
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            return classify_exterior_interior(url, timeout_s=timeout_s)
        except Exception as e:
            last_exc = e
            try:
                time.sleep(0.5 * attempt)
            except Exception:
                pass
    assert last_exc is not None
    raise last_exc


def classify_exterior_interior(image_url: str, timeout_s: Optional[float] = None) -> Dict[str, float]:
    """
    Classify an image URL into exterior, interior, or food item.

    Returns:
        Dict with scores for keys: 'exterior', 'interior', 'food'.
        Example: {"exterior": 0.82, "interior": 0.15, "food": 0.03}
    Raises:
        ClassifierError if classifier disabled or inference fails.
    """
    cfg = get_report_config()
    if not cfg.CLASSIFIER_ENABLED:
        raise ClassifierError("Classifier disabled by config")

    t0 = time.time()
    timeout = timeout_s if timeout_s is not None else cfg.CLASSIFIER_TIMEOUT_S

    try:
        model, processor = _get_model_and_processor()
    except ClassifierError as e:
        raise

    # Download first to enforce our timeout deterministically with retries inside helper
    img_bytes = _download_image_bytes(image_url, timeout)
    img_buf = io.BytesIO(img_bytes)

    # Load PIL image
    try:
        pil_img = Image.open(img_buf).convert("RGB")
    except Exception as e:
        logger.exception("Failed to load PIL image for url=%s", image_url)
        raise ClassifierError("Failed to load image")

    # Perform CLIP zero-shot classification
    try:
        inputs = processor(text=_labels_verbose, images=pil_img, return_tensors="pt", padding=True)
        outputs = model(**inputs)
        logits = outputs.logits_per_image  # shape (1, num_labels)
        probs = logits.softmax(dim=1).squeeze(0).tolist()
        result = [{"label": label, "score": score} for label, score in zip(_labels_verbose, probs)]
    except Exception as e:
        logger.exception("CLIP classification failed for url=%s", image_url)
        raise ClassifierError("CLIP classification failed")

    scores_map: Dict[str, float] = {"exterior": 0.0, "interior": 0.0, "food": 0.0}
    try:
        # result can be list of dicts: [{'label': '...', 'score': ...}, ...]
        for item in result:
            label = str(item.get("label", "")).strip().lower()
            score = float(item.get("score", 0.0))
            if "exterior" in label:
                scores_map["exterior"] = max(scores_map["exterior"], score)
            elif "interior" in label:
                scores_map["interior"] = max(scores_map["interior"], score)
            elif "food" in label:
                scores_map["food"] = max(scores_map["food"], score)
    except Exception as e:
        logger.exception("Classifier parsing error: %s", e)
        raise ClassifierError("Classifier output parse failed")

    elapsed = (time.time() - t0) * 1000.0
    logger.info("Classified image ext=%.3f int=%.3f food=%.3f ms=%.1f url=%s",
                scores_map["exterior"], scores_map["interior"], scores_map["food"], elapsed, image_url)
    return scores_map


def select_best_photo(photo_urls: List[str], timeout_s: Optional[float] = None, topk: Optional[int] = None, business_name: Optional[str] = None) -> Optional[str]:
    """
    Select best photo by prioritizing exterior, then interior, with optional business name boosting.
    Returns selected URL or None if no candidates.

    Behavior update:
    - As soon as we encounter the first image that BOTH:
        1) contains/matches the business name with a positive match score (OCR or URL heuristic), and
        2) is classified as exterior with at least the interior margin advantage,
      we immediately return that image without evaluating remaining images.

    - If classifier disabled: returns first URL (if any).
    - On any classification error: continues evaluating others; if none succeed, returns first URL.
    - Also retains previous short-circuit on decisive exterior using boosted scores.
    """
    if not photo_urls:
        return None
    cfg = get_report_config()
    if not cfg.CLASSIFIER_ENABLED:
        return photo_urls[0]

    margin = cfg.CLASSIFIER_CONFIDENCE_MARGIN
    # Track raw best as well as boosted best
    best_ext: Tuple[float, Optional[str]] = (-1.0, None)
    best_int: Tuple[float, Optional[str]] = (-1.0, None)
    best_ext_boosted: Tuple[float, Optional[str]] = (-1.0, None)
    best_int_boosted: Tuple[float, Optional[str]] = (-1.0, None)

    any_success = False
    # Helper to compute url_text heuristic
    def _heuristic_url_text(u: str) -> str:
        try:
            parsed = urlparse(u)
            path = parsed.path or ""
            # take last segment
            filename = path.split("/")[-1]
            # strip extension
            name = filename.rsplit(".", 1)[0]
            # replace separators with spaces
            name = name.replace("-", " ").replace("_", " ").replace("%20", " ")
            return _normalize_text(name)
        except Exception:
            return ""

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_url = {executor.submit(_classify_with_retry, url, timeout_s): url for url in photo_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                scores = future.result()
                any_success = True
            except Exception as e:
                logger.warning("Classifier failed for url=%s err=%s; skipping", url, e)
                continue

            ext, inte = scores.get("exterior", 0.0), scores.get("interior", 0.0)
            food = scores.get("food", 0.0)

            # Skip images where food item score is the highest
            if food > ext and food > inte:
                continue

            # Compute name match score if business_name provided
            if business_name:
                name_match_score = 0.0
                ocr_text = ""
                try:
                    timeout = timeout_s if timeout_s is not None else cfg.CLASSIFIER_TIMEOUT_S
                    img_bytes = _download_image_bytes(url, timeout)
                    try:
                        pil_img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
                    except Exception:
                        pil_img = None  # type: ignore
                    if pil_img is not None:
                        ocr_text = _extract_image_text(pil_img)
                except Exception as ne:
                    # Concise warning; OCR helper already logs when unavailable internally
                    logger.warning("OCR error for url=%s err=%s; proceeding without text match", url, ne)
                    ocr_text = ""

                url_text = _heuristic_url_text(url)
                try:
                    sim_ocr = _similarity(ocr_text, business_name)
                except Exception:
                    sim_ocr = 0.0
                try:
                    sim_url = _similarity(url_text, business_name)
                except Exception:
                    sim_url = 0.0
                name_match_score = max(sim_ocr, sim_url)
            else:
                name_match_score = 0.0

            # Apply boosting - prioritize exterior over name match
            boosted_ext = min(1.0, ext + 0.15 * name_match_score)
            boosted_int = min(1.0, inte + 0.03 * name_match_score)

            # Update raw best
            if ext > best_ext[0]:
                best_ext = (ext, url)
            if inte > best_int[0]:
                best_int = (inte, url)

            # Update boosted best
            if boosted_ext > best_ext_boosted[0]:
                best_ext_boosted = (boosted_ext, url)
            if boosted_int > best_int_boosted[0]:
                best_int_boosted = (boosted_int, url)

            # New short-circuit: require stronger evidence before immediate selection to avoid false positives
            # Conditions (ALL must hold):
            #  - Non-trivial name match (>= 0.35) via OCR/URL heuristic
            #  - Exterior substantially exceeds interior by (margin + 0.10)
            #  - Raw exterior confidence itself is at least 0.70
            if business_name and name_match_score >= 0.35 and (ext - inte) >= (margin + 0.10) and ext >= 0.70:
                logger.info("Short-circuit on strong exterior-with-name match url=%s name_score=%.2f ext=%.3f int=%.3f food=%.3f", url, name_match_score, ext, inte, food)
                return url

            # Short-circuit using boosted scores for decisive exterior (tighten threshold slightly)
            if (boosted_ext - boosted_int) >= (margin + 0.05) and boosted_ext >= 0.85 and ext >= 0.60:
                logger.info("Short-circuit exterior selection (tight) url=%s ext=%.3f int=%.3f food=%.3f boosted_ext=%.3f boosted_int=%.3f", url, ext, inte, food, boosted_ext, boosted_int)
                return url

            # Per-URL log when name provided
            if business_name:
                logger.info("Name match score=%.2f boosted_ext=%.2f boosted_int=%.2f food=%.3f url=%s", name_match_score, boosted_ext, boosted_int, food, url)

    # Post-loop selection with safer preference:
    # 1) Prefer boosted exterior if reasonably confident and advantaged
    if best_ext_boosted[1] is not None and best_ext_boosted[0] >= 0.80:
        return best_ext_boosted[1]
    # 2) Otherwise prefer boosted interior only if exterior wasn't confident at all
    if best_int_boosted[1] is not None and (best_ext_boosted[0] < 0.60):
        return best_int_boosted[1]
    # 3) Then raw exterior if available
    if best_ext[1] is not None and best_ext[0] >= 0.70:
        return best_ext[1]
    # 4) Then raw interior if exterior confidence is poor
    if best_int[1] is not None and best_ext[0] < 0.55:
        return best_int[1]
    # 5) Fallback to first URL only if classifier never succeeded; otherwise None to signal no confident pick
    return photo_urls[0] if not any_success else None