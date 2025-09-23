import os
import time
import requests
from typing import Dict, Any, Optional


class PageSpeedClient:
    """
    Wrapper for Google PageSpeed Insights API.
    Requires an environment variable: PAGESPEED_API_KEY
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("PAGESPEED_API_KEY")
        if not self.api_key:
            raise ValueError("PAGESPEED_API_KEY is missing. Please add it to your environment variables.")
        self.base_url = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"

    def analyze_page(self, url: str, strategy: str = "desktop") -> Dict[str, Any]:
        """
        Get PageSpeed analysis of a URL.
        Returns score and time-to-interactive in ms.
        Retries the request up to 3 times on transient failures (network issues or 5xx).
        """
        params = {
            "url": url,
            "strategy": strategy,
            "key": self.api_key,
        }

        last_err: Optional[Exception] = None
        # Exponential backoff: 0.5s, 1s, 2s between attempts
        for attempt in range(3):
            try:
                response = requests.get(self.base_url, params=params, timeout=360)
                # Only retry on 5xx. For 4xx, raise immediately.
                if 500 <= response.status_code < 600:
                    raise requests.HTTPError(f"{response.status_code} Server Error", response=response)
                response.raise_for_status()
                data = response.json()
                break
            except requests.HTTPError as http_err:
                # If it's a 4xx, don't retry.
                status = getattr(getattr(http_err, "response", None), "status_code", None)
                if status is not None and 400 <= status < 500 and status != 429:
                    # Surface client errors immediately (invalid key, bad request, etc.), except 429 which is retryable.
                    raise
                last_err = http_err
            except (requests.ConnectionError, requests.Timeout) as net_err:
                last_err = net_err
            except Exception as e:
                # Unknown error; keep for last but retry as it may be transient.
                last_err = e

            if attempt < 2:
                delay = 0.5 * (2 ** attempt)
                time.sleep(delay)
            else:
                # Exhausted retries
                if last_err:
                    raise last_err
                raise RuntimeError("Unknown error calling PageSpeed API with no exception captured.")

        lighthouse = data.get("lighthouseResult", {})
        categories = lighthouse.get("categories", {})
        performance = categories.get("performance", {})
        score = int(performance.get("score", 0) * 100) if performance.get("score") is not None else None

        audits = lighthouse.get("audits", {})
        tti = audits.get("interactive", {}).get("numericValue")

        return {
            "page_speed_score": score,
            "time_to_interactive_ms": int(tti) if tti is not None else None,
        }